import time
import os
import subprocess
import copy
import io
import bz2
from datetime import datetime
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
import github
import requests
import rapidjson as json
import click
import tenacity

from conda_build.conda_interface import VersionOrder
from conda._vendor.toolz.itertoolz import groupby

from .shards import read_subdir_shards, get_shard_path
from .metadata import CONDA_FORGE_SUBIDRS
from .utils import timer
from .releases import get_latest_links

CHANNELDATA_VERSION = 1
WORKDIR = "repodata_products"
MIN_UPDATE_TIME = 30
HEAD = "REPO WORKER: "

GH = github.Github(os.environ["GITHUB_TOKEN"])
REPODATA = GH.get_repo("regro/repodata")


def _load_shard_channeldata(subdir, fn, repodata):
    pth = os.path.join("repodata-shards", get_shard_path(subdir, fn))
    if not os.path.exists(pth):
        assert False, repodata["packages"][fn]
    with open(pth, "r") as fp:
        shard = json.load(fp)
    return shard["channeldata"], shard["channeldata_version"]


def _make_seconds(timestamp):
    timestamp = int(timestamp)
    if timestamp > 253402300799:  # 9999-12-31
        timestamp //= 1000
        # convert milliseconds to seconds; see conda/conda-build#1988
    return timestamp


def update_channeldata_for_subdir(channel_data, repodata, subdir):
    legacy_packages = repodata["packages"]
    conda_packages = repodata["packages.conda"]

    use_these_legacy_keys = (
        set(legacy_packages.keys())
        - set(k[:-6] + '.tar.bz2' for k in conda_packages.keys())
    )
    all_repodata_packages = conda_packages.copy()
    all_repodata_packages.update({k: legacy_packages[k] for k in use_these_legacy_keys})
    package_data = channel_data.get('packages', {})

    for fn, x in all_repodata_packages.items():
        assert "subdir" not in x or x["subdir"] == subdir, x

    def _append_group(groups, candidate):
        pkg_dict = candidate[1]
        pkg_name = pkg_dict['name']

        run_exports = package_data.get(pkg_name, {}).get('run_exports', {})
        if (
            pkg_name not in package_data
            or subdir not in package_data.get(pkg_name, {}).get('subdirs', [])
            or (
                package_data.get(pkg_name, {}).get('timestamp', 0)
                < _make_seconds(pkg_dict.get('timestamp', 0))
            )
            or run_exports and pkg_dict['version'] not in run_exports
        ):
            groups.append(candidate)

    groups = []
    package_groups = groupby(lambda x: x[1]['name'], all_repodata_packages.items())
    for groupname, group in package_groups.items():
        if (
            groupname not in package_data
            or package_data[groupname].get('run_exports')
        ):
            # pay special attention to groups that have run_exports
            #   - we need to process each version
            # group by version; take newest per version group.  We handle groups that
            #    are not in the index t all yet similarly, because we can't check
            #    if they have any run_exports
            for vgroup in groupby(lambda x: x[1]['version'], group).values():
                candidate = next(iter(sorted(
                    vgroup, key=lambda x: x[1].get('timestamp', 0), reverse=True))
                )
                _append_group(groups, candidate)
        else:
            # take newest per group
            candidate = next(iter(sorted(
                group, key=lambda x: x[1].get('timestamp', 0), reverse=True))
            )
            _append_group(groups, candidate)

    def _replace_if_newer_and_present(pd, data, erec, data_newer, k):
        if data.get(k) and (data_newer or not erec.get(k)):
            pd[k] = data[k]
        else:
            pd[k] = erec.get(k)

    # unzipping
    fns, fn_dicts = [], []
    if groups:
        fns, fn_dicts = zip(*groups)

    for fn_dict, fn in zip(fn_dicts, fns):
        data, _cdver = _load_shard_channeldata(subdir, fn, repodata)
        assert _cdver == CHANNELDATA_VERSION
        if data:
            data.update(fn_dict)
            name = data['name']
            # existing record
            erec = package_data.get(name, {})
            data_v = data.get('version', '0')
            erec_v = erec.get('version', '0')
            data_newer = VersionOrder(data_v) > VersionOrder(erec_v)

            package_data[name] = package_data.get(name, {})
            # keep newer value for these
            for k in (
                'description', 'dev_url', 'doc_url', 'doc_source_url', 'home',
                'license',
                'source_url', 'source_git_url', 'summary', 'icon_url', 'icon_hash',
                'tags',
                'identifiers', 'keywords', 'recipe_origin', 'version'
            ):
                _replace_if_newer_and_present(
                    package_data[name], data, erec, data_newer, k
                )

            # keep any true value for these, since we don't distinguish subdirs
            for k in (
                "binary_prefix", "text_prefix", "activate.d", "deactivate.d",
                "pre_link", "post_link", "pre_unlink"
            ):
                package_data[name][k] = any((data.get(k), erec.get(k)))

            package_data[name]['subdirs'] = sorted(list(set(
                erec.get('subdirs', []) + [subdir]
            )))
            # keep one run_exports entry per version of the package, since these
            # vary by version
            run_exports = erec.get('run_exports', {})
            exports_from_this_version = data.get('run_exports')
            if exports_from_this_version:
                run_exports[data_v] = data.get('run_exports')
            package_data[name]['run_exports'] = run_exports
            package_data[name]['timestamp'] = _make_seconds(max(
                data.get('timestamp', 0),
                channel_data.get(name, {}).get('timestamp', 0)
            ))

    channel_data.update({
        'channeldata_version': CHANNELDATA_VERSION,
        'subdirs': sorted(list(set(channel_data.get('subdirs', []) + [subdir]))),
        'packages': package_data,
    })


def build_or_update_links_and_repodata_from_packages(
    repodata,
    links,
    subdir,
    override_labels=None,
    removed=None,
    new_shards=None,
):
    if subdir not in repodata:
        repodata[subdir] = {}

    override_labels = override_labels or {}
    removed = removed or []
    init_repodata = {
        'info': {'subdir': subdir},
        'packages': {},
        'packages.conda': {},
        'removed': [],
        'repodata_version': 1
    }

    all_shards = {}
    read_subdir_shards("repodata-shards", subdir, all_shards, shard_paths=new_shards)
    print(
        f"{HEAD}    found {len(all_shards)} repodata shards for subdir {subdir}",
        flush=True,
    )
    if new_shards is not None:
        assert len(all_shards) == len(new_shards)

    updated_data = set()

    for subdir_pkg, shard in all_shards.items():
        shard["labels"] = override_labels.get(subdir_pkg, shard["labels"])
        for label in shard["labels"]:
            if label not in repodata[subdir]:
                repodata[subdir][label] = copy.deepcopy(init_repodata)
            if label not in links["packages"]:
                links["packages"][label] = {}
            repodata[subdir][label]["packages"][shard["package"]] \
                = shard["repodata"]
            links["packages"][label][subdir_pkg] = shard["url"]
            updated_data.add((subdir, label))

    if (
        "main" in repodata[subdir]
        and sorted(repodata[subdir]["main"]["removed"]) != sorted(removed)
    ):
        updated_data.add((subdir, "main"))
        repodata[subdir]["main"]["removed"] = removed
        for fn in removed:
            if fn in repodata[subdir]["main"]["packages"]:
                repodata[subdir]["main"]["packages"].pop(fn, None)

    return updated_data


@tenacity.retry(
    wait=tenacity.wait_random_exponential(multiplier=1, max=60),
    stop=tenacity.stop_after_attempt(10),
    reraise=True,
)
def _get_broken_packages(subdir):
    r = requests.get(
        "https://conda.anaconda.org/conda-forge/label/broken"
        f"/{subdir}/repodata.json.bz2"
    )
    rd_broken = json.load(
        io.StringIO(bz2.decompress(r.content).decode("utf-8")))
    return rd_broken


def build_or_update_links_and_repodata_subdir(repodata, links, new_shards, subdir):
    if new_shards is not None:
        _new_shards = [
            k
            for k in new_shards
            if k.startswith(f"repodata-shards/shards/{subdir}/")
        ]
    else:
        _new_shards = None

    rd_broken = _get_broken_packages(subdir)

    return build_or_update_links_and_repodata_from_packages(
        repodata,
        links,
        subdir,
        removed=list(rd_broken["packages"]),
        new_shards=_new_shards,
    )


def get_new_shards_from_repo(old_sha):
    if old_sha is None:
        old_sha = subprocess.run(
            "cd repodata-shards && git rev-parse --verify HEAD",
            shell=True,
            capture_output=True,
        ).stdout.decode("utf-8").strip()
    subprocess.run(
        "cd repodata-shards && git pull",
        shell=True,
    )
    new_sha = subprocess.run(
        "cd repodata-shards && git rev-parse --verify HEAD",
        shell=True,
        capture_output=True,
    ).stdout.decode("utf-8").strip()
    print(f"{HEAD}old shards sha={old_sha}", flush=True)
    print(f"{HEAD}new shards sha={new_sha}", flush=True)
    new_shards = subprocess.run(
        "cd repodata-shards && git diff --name-only %s %s" % (old_sha, new_sha),
        shell=True,
        capture_output=True,
    ).stdout.decode("utf-8")
    new_shards = [
        os.path.join("repodata-shards", line.strip())
        for line in new_shards.splitlines()
    ]
    print(f"{HEAD}found {len(new_shards)} new shards", flush=True)

    return old_sha, new_sha, new_shards


def get_new_shards(current_shas):
    if not current_shas:
        print(f"{HEAD}doing a full rebuild of repo data products", flush=True)
        new_shards = None
        new_sha = subprocess.run(
            "cd repodata-shards && git rev-parse --verify HEAD",
            shell=True,
            capture_output=True,
        ).stdout.decode("utf-8").strip()
        old_sha = None
    else:
        with timer(HEAD, "pulling new shards"):
            old_sha, new_sha, new_shards = get_new_shards_from_repo(
                current_shas.get("repodata-shards-sha", None)
            )

        if (
            "repodata-shards-sha" in current_shas
            and old_sha != current_shas["repodata-shards-sha"]
        ):
            # the internal stats is inconsistent, rebuild it all
            print(
                f"{HEAD}internal state of repodata-shard SHAs is "
                "inconsistent! rebuilding the full data!",
                flush=True,
            )
            new_shards = None
            new_sha = subprocess.run(
                "cd repodata-shards && git rev-parse --verify HEAD",
                shell=True,
                capture_output=True,
            ).stdout.decode("utf-8").strip()
            old_sha = None

    return old_sha, new_sha, new_shards


def load_current_data(make_releases):
    load_links = False
    rel = REPODATA.get_latest_release()
    for ast in rel.get_assets():
        if "links.json.bz2" in ast.name:
            load_links = True
            break

    if not load_links:
        if not make_releases:
            all_links = {
                "packages": {},
                "repodata": {},
            }
        else:
            raise RuntimeError("Cannot find current links! This is not safe! Aborting!")
    else:
        all_links = get_latest_links()

    if (
        os.path.exists(f"{WORKDIR}/current_shas.json")
        and os.path.exists(f"{WORKDIR}/all_repodata.json")
        and os.path.exists(f"{WORKDIR}/all_channeldata.json")
    ):
        with open(f"{WORKDIR}/current_shas.json", "r") as fp:
            current_shas = json.load(fp)

        with open(f"{WORKDIR}/all_repodata.json", "r") as fp:
            all_repodata = json.load(fp)

        with open(f"{WORKDIR}/all_channeldata.json", "r") as fp:
            all_channeldata = json.load(fp)

        return current_shas, all_repodata, all_channeldata, all_links
    else:
        return {}, {}, {}, all_links


def _init_git():
    # configure git
    subprocess.run(
        "git config --global user.email "
        "'64793534+conda-forge-daemon@users.noreply.github.com'",
        shell=True,
        check=True,
    )
    subprocess.run(
        "git config --global user.name 'conda-forge-daemon'",
        shell=True,
        check=True,
    )
    subprocess.run(
        "git config --global pull.rebase false",
        shell=True,
        check=True,
    )


def _clone_repodata_shards():
    subprocess.run(
        "git clone --depth=1 https://github.com/"
        "regro/repodata-shards.git",
        shell=True,
        check=True,
    )


def _clone_repodata():
    subprocess.run(
        "git clone --depth=1 https://github.com/"
        "regro/repodata.git",
        shell=True,
        check=True,
    )


def _get_repodata_sha():
    repo_sha = subprocess.run(
        "cd repodata && git rev-parse --verify HEAD",
        shell=True,
        capture_output=True,
    ).stdout.decode("utf-8").strip()
    return repo_sha


@tenacity.retry(
    wait=tenacity.wait_random_exponential(multiplier=1, max=60),
    stop=tenacity.stop_after_attempt(10),
    reraise=True,
)
def _upload_asset(rel, pth, content_type):
    rel.upload_asset(pth, content_type=content_type)
    tag = rel.tag_name
    fn = os.path.basename(pth)
    return (
        fn,
        f"https://github.com/regro/repodata/releases/download/{tag}/{fn}",
    )


@tenacity.retry(
    wait=tenacity.wait_random_exponential(multiplier=1, max=60),
    stop=tenacity.stop_after_attempt(10),
    reraise=True,
)
def _delete_release(rel):
    for ast in rel.get_assets():
        ast.delete_asset()
    if not rel.draft:
        tag = REPODATA.get_git_ref(f"tags/{rel.tag_name}")
    else:
        tag = None
    rel.delete_release()
    if tag is not None:
        tag.delete()


@tenacity.retry(
    wait=tenacity.wait_random_exponential(multiplier=1, max=60),
    stop=tenacity.stop_after_attempt(10),
    reraise=True,
)
def _delete_old_releases(all_links):
    releases_to_delete = []
    for rel in REPODATA.get_releases():
        tag = rel.tag_name
        has_tag = any(
            any(tag in url for url in urls)
            for _, urls in all_links["repodata"].items()
        )
        if not has_tag:
            releases_to_delete.append(rel)

    for rel in releases_to_delete:
        print(f"{HEAD}deleting release {rel.tag_name}", flush=True)
        _delete_release(rel)


@click.command()
@click.argument("time_limit", type=int)
@click.option(
    "--make-releases", is_flag=True, help="make github releases of the repo data")
@click.option(
    "--main-only", is_flag=True, help="only release the main channel")
@click.option(
    "--debug", is_flag=True, help="write data locally for debugging")
def main(time_limit, make_releases, main_only, debug):
    """Worker process for continuously building repodata for a maximum
    number of TIME_LIMIT seconds.
    """
    start_time = time.time()

    with timer(HEAD, "initializing git and pulling repodata shards"):
        os.makedirs(WORKDIR, exist_ok=True)
        _init_git()
        if not os.path.exists("repodata-shards"):
            _clone_repodata_shards()
        if not os.path.exists("repodata"):
            _clone_repodata()

    with timer(HEAD, "loading local data"):
        (
            current_shas, all_repodata, all_channeldata, all_links
        ) = load_current_data(make_releases)

    while time.time() - start_time < time_limit:
        build_start_time = time.time()

        with timer(HEAD, "doing repodata products rebuild"), ThreadPoolExecutor(max_workers=8) as exec:  # noqa
            old_sha, new_sha, new_shards = get_new_shards(current_shas)
            if old_sha is None and new_shards is None:
                # we are doing a full rebuild
                dump_data = True
            elif old_sha is not None and new_sha is not None and len(new_shards) > 0:
                # partial update but are adding stuff
                dump_data = True
            else:
                dump_data = False

            futures = []
            updated_data = set()
            if dump_data and make_releases:
                tag = datetime.utcnow().strftime("%Y.%m.%d.%H.%M.%S")
                rel = REPODATA.create_git_tag_and_release(
                    tag,
                    "",
                    tag,
                    "",
                    _get_repodata_sha(),
                    "commit",
                    draft=True,
                )

            for subdir in CONDA_FORGE_SUBIDRS:
                if (
                    new_shards is not None
                    and not any(
                        _spth.startswith(f"repodata-shards/shards/{subdir}/")
                        for _spth in new_shards)
                ):
                    rebuild_subdir = False
                else:
                    rebuild_subdir = True

                with timer(HEAD, "processing subdir %s" % subdir):
                    if rebuild_subdir:
                        with timer(HEAD, "making repodata", indent=1):
                            updated_data |= build_or_update_links_and_repodata_subdir(
                                all_repodata,
                                all_links,
                                new_shards,
                                subdir,
                            )

                        with timer(HEAD, "making channel data", indent=1):
                            for label in all_repodata[subdir]:
                                channel_data = all_channeldata.get(label, {})
                                update_channeldata_for_subdir(
                                    channel_data,
                                    all_repodata[subdir][label],
                                    subdir,
                                )
                                all_channeldata[label] = channel_data

                        with timer(HEAD, "writing data", indent=1):
                            for label in all_repodata[subdir]:
                                if (subdir, label) not in updated_data:
                                    continue
                                if main_only and label != "main":
                                    continue
                                with open(
                                    f"{WORKDIR}/repodata_{subdir}_{label}.json", "w"
                                ) as fp:
                                    json.dump(
                                        all_repodata[subdir][label],
                                        fp,
                                        indent=2,
                                        sort_keys=True,
                                    )
                                subprocess.run(
                                    f"cd {WORKDIR} && "
                                    f"rm -f repodata_{subdir}_{label}.json.bz2 && "
                                    f"bzip2 --keep repodata_{subdir}_{label}.json",
                                    shell=True,
                                )

                    if dump_data and make_releases:
                        with timer(
                            HEAD, "uploading any new repodata", indent=1, result=False
                        ):
                            for label in all_repodata[subdir]:
                                if (subdir, label) not in updated_data:
                                    continue
                                if main_only and label != "main":
                                    continue
                                pth = f"{WORKDIR}/repodata_{subdir}_{label}.json"
                                futures.append(exec.submit(
                                    _upload_asset, rel, pth, "application/json"
                                ))
                                pth = f"{WORKDIR}/repodata_{subdir}_{label}.json.bz2"
                                futures.append(exec.submit(
                                    _upload_asset, rel, pth, "application/x-bzip2"
                                ))

            current_shas["repodata-shards-sha"] = new_sha
            with open(f"{WORKDIR}/current_shas.json", "w") as fp:
                json.dump(current_shas, fp)

            if dump_data and make_releases:
                with timer(HEAD, "writing channel data"):
                    for label in all_channeldata:
                        if not any(label == t[1] for t in updated_data):
                            continue
                        if main_only and label != "main":
                            continue
                        with open(f"{WORKDIR}/channeldata_{label}.json", "w") as fp:
                            json.dump(
                                all_channeldata[label],
                                fp,
                                indent=2,
                                sort_keys=True,
                            )

                with timer(HEAD, "uploading channel data", result=False):
                    for label in all_channeldata:
                        if not any(label == t[1] for t in updated_data):
                            continue
                        if main_only and label != "main":
                            continue
                        pth = f"{WORKDIR}/channeldata_{label}.json"
                        futures.append(exec.submit(
                            _upload_asset, rel, pth, "application/json"
                        ))

                with timer(HEAD, "waiting for uploads to finish"):
                    for fut in concurrent.futures.as_completed(futures):
                        fname, url = fut.result()
                        if fname not in all_links["repodata"]:
                            all_links["repodata"][fname] = []
                        all_links["repodata"][fname].append(url)
                        if len(all_links["repodata"][fname]) > 3:
                            all_links["repodata"][fname] = \
                                all_links["repodata"][fname][-3:]
                    futures = []

                with timer(HEAD, "updating and uploading links"):
                    with open(f"{WORKDIR}/links.json", "w") as fp:
                        json.dump(all_links, fp, indent=2, sort_keys=True)
                    subprocess.run(
                        f"cd {WORKDIR} && "
                        "rm -f links.json.bz2 && "
                        "bzip2 links.json",
                        shell=True,
                    )

                    pth = f"{WORKDIR}/links.json.bz2"
                    futures.append(exec.submit(
                        _upload_asset, rel, pth, "application/x-bzip2"
                    ))
                    concurrent.futures.wait(futures)

                with timer(HEAD, "publishing release", result=False):
                    rel.update_release(rel.title, rel.body, draft=False)

                with timer(HEAD, "deleting old releases"):
                    _delete_old_releases(all_links)

        dt = int(time.time() - build_start_time)

        if dt < MIN_UPDATE_TIME:
            print(
                "REPO WORKER: waiting for %s seconds before "
                "next update" % (MIN_UPDATE_TIME - dt),
                flush=True,
            )
            time.sleep(MIN_UPDATE_TIME - dt)

        print(" ", flush=True)

    if debug:
        with timer(HEAD, "dumping all data to JSON"):
            with open(f"{WORKDIR}/all_repodata.json", "w") as fp:
                json.dump(all_repodata, fp, indent=2, sort_keys=True)
            with open(f"{WORKDIR}/all_channeldata.json", "w") as fp:
                json.dump(all_channeldata, fp, indent=2, sort_keys=True)
            with open(f"{WORKDIR}/current_shas.json", "w") as fp:
                json.dump(current_shas, fp)
