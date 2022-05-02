import sys
import time
import os
import io
import bz2
import importlib
import subprocess
import copy
from datetime import datetime
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
import pytz

import github
import tenacity
import requests
import rapidjson as json
import click

from .shards import read_subdir_shards
from .metadata import CONDA_FORGE_SUBIDRS
from .utils import timer

from .links import get_latest_links
from repodata_tools.index import (
    upload_repodata_asset,
    delete_old_repodata_releases,
    build_or_update_channeldata,
    build_or_update_links_and_repodata,
    INIT_REPODATA,
    build_current_repodata,
    REPODATA_NAME,
    REPODATA_REPO,
    refresh_github_token_and_client,
    get_repodata,
)

WORKDIR = "repodata_products"
MIN_UPDATE_TIME = 30
HEAD = "REPO WORKER: "
DEBUG = False


def _write_compress_and_start_upload(
    data, fn, rel, exec, no_compress=False, only_compress=False
):
    pth = os.path.join(WORKDIR, fn)
    with open(pth, "w") as fp:
        json.dump(data, fp, indent=2, sort_keys=True)
    futs = []
    if not only_compress:
        futs.append(exec.submit(upload_repodata_asset, rel, pth, "application/json"))
    if not no_compress:
        subprocess.run(
            f"cd {WORKDIR} && "
            f"rm -f {fn}.bz2 && "
            f"bzip2 --keep {fn}",
            shell=True,
            check=True,
        )
        futs.append(
            exec.submit(
                upload_repodata_asset, rel, pth + ".bz2", "application/x-bzip2"
            )
        )
    return futs


@tenacity.retry(
    wait=tenacity.wait_random_exponential(multiplier=1, max=10),
    stop=tenacity.stop_after_attempt(5),
    reraise=True,
)
def _fetch_repodata(links, subdir, label):
    fn = f"repodata_from_packages_{subdir}_{label}.json"
    if fn in links["serverdata"]:
        url = links["serverdata"][fn][-1]
        if not url.endswith(".bz2"):
            url += ".bz2"
        print(
            f"{HEAD}    fetching {url}",
            flush=True,
        )
        r = requests.get(url)
        return json.load(io.StringIO(bz2.decompress(r.content).decode("utf-8")))
    else:
        rd = copy.deepcopy(INIT_REPODATA)
        rd["info"]["subdir"] = subdir
        return rd


@tenacity.retry(
    wait=tenacity.wait_random_exponential(multiplier=1, max=10),
    stop=tenacity.stop_after_attempt(5),
    reraise=True,
)
def _fetch_patched_repodata(links, subdir, label):
    fn = f"repodata_{subdir}_{label}.json"
    if fn in links["serverdata"]:
        url = links["serverdata"][fn][-1]
        if not url.endswith(".bz2"):
            url += ".bz2"
        print(
            f"{HEAD}    fetching {url}",
            flush=True,
        )
        r = requests.get(url)
        return json.load(io.StringIO(bz2.decompress(r.content).decode("utf-8")))
    else:
        rd = copy.deepcopy(INIT_REPODATA)
        rd["info"]["subdir"] = subdir
        return rd


def _update_repodata_from_shards(repodata, links, new_shards, removed_shards, subdir):
    all_shards = {}
    read_subdir_shards("repodata-shards", subdir, all_shards, shard_paths=new_shards)
    print(
        f"{HEAD}    found {len(all_shards)} repodata shards for subdir {subdir}",
        flush=True,
    )
    if new_shards is not None:
        assert len(all_shards) == len(new_shards)

    return build_or_update_links_and_repodata(
        repodata,
        links,
        subdir,
        all_shards,
        fetch_repodata=None if DEBUG else _fetch_repodata,
        removed_shards=removed_shards,
    )


def _clean_nones(data):
    for k in list(data.keys()):
        if isinstance(data[k], dict):
            _clean_nones(data[k])
        elif data[k] is None:
            del data[k]


def _patch_repodata(repodata, patched_repodata, subdir, patch_fns, do_all=False):
    removed = patch_fns["gen_removals"](subdir)
    if not do_all:
        # compute the new data to patch
        data_to_patch = copy.deepcopy(INIT_REPODATA)
        data_to_patch["info"]["subdir"] = subdir
        add_fn = (
            set(repodata["packages"])
            - set(removed)
            - set(patched_repodata["packages"])
        )
        for fn in add_fn:
            data_to_patch["packages"][fn] = copy.deepcopy(repodata["packages"][fn])

        new_index = patch_fns["gen_new_index"](data_to_patch, subdir)
        _clean_nones(new_index)

        for index_key in ["packages", "packages.conda"]:
            patched_repodata[index_key].update(new_index[index_key])
    else:
        new_index = patch_fns["gen_new_index"](copy.deepcopy(repodata), subdir)
        _clean_nones(new_index)

        for index_key in ["packages", "packages.conda"]:
            patched_repodata[index_key] = new_index[index_key]

        patched_repodata["removed"] = []

    # FIXME: this appears to be buggy - I think the line resetting removed above fixes
    # this, but I want to wait a while for the old buggy versions to be removed
    # from the releases before trying again - MRB 2020/09/21
    # to_remove = set(removed) - set(patched_repodata["removed"])
    for fn in removed:
        if fn in patched_repodata["packages"]:
            del patched_repodata["packages"][fn]

    patched_repodata["removed"] = sorted(removed)

    return patched_repodata


def _build_channel_data(
    all_channeldata,
    all_links,
    all_patched_repodata,
    all_labels,
    updated_data,
    rel,
    exec,
    *,
    make_releases,
    main_only,
):
    futs = []

    for label in all_labels:
        if not any(label == t[1] for t in updated_data):
            continue
        if main_only and label != "main":
            continue

        with timer(HEAD, f"processing label {label}", indent=1):
            # reset since some package in the channel has been updated
            all_channeldata[label] = {}

            for subdir in CONDA_FORGE_SUBIDRS:
                with timer(HEAD, f"processing subdir {subdir}", indent=2):
                    if label not in all_patched_repodata[subdir]:
                        with timer(
                            HEAD,
                            f"fetching patched repodata for {label}/{subdir}",
                            indent=3,
                        ):
                            all_patched_repodata[subdir][label] = \
                                _fetch_patched_repodata(
                                    all_links, subdir, label
                                )

                    channel_data = all_channeldata.get(label, {})
                    build_or_update_channeldata(
                        channel_data,
                        all_patched_repodata[subdir][label],
                        subdir,
                    )
                    all_channeldata[label] = channel_data

            if make_releases:
                futs.extend(_write_compress_and_start_upload(
                    all_channeldata[label],
                    f"channeldata_{label}.json",
                    rel,
                    exec,
                    no_compress=True,
                ))

    return futs


def _get_new_shards_from_repo(old_sha):
    subprocess.run(
        "cd repodata-shards && git pull",
        shell=True,
    )
    new_sha = subprocess.run(
        "cd repodata-shards && git rev-parse --verify HEAD",
        shell=True,
        check=True,
        capture_output=True,
    ).stdout.decode("utf-8").strip()
    print(f"{HEAD}old shards sha={old_sha}", flush=True)
    print(f"{HEAD}new shards sha={new_sha}", flush=True)
    new_shards_output = subprocess.run(
        "cd repodata-shards && git diff --name-status %s %s" % (old_sha, new_sha),
        shell=True,
        check=True,
        capture_output=True,
    ).stdout.decode("utf-8")
    new_shards = [
        os.path.join("repodata-shards", line.strip().split()[1].strip())
        for line in new_shards_output.splitlines()
        if line.strip().split()[0].strip() != "D"
    ]
    print(f"{HEAD}found {len(new_shards)} new or modified shards", flush=True)

    removed_shards = [
        os.path.join("repodata-shards", line.strip().split()[1].strip())
        for line in new_shards_output.splitlines()
        if line.strip().split()[0].strip() == "D"
    ]
    print(f"{HEAD}found {len(removed_shards)} removed shards", flush=True)

    return old_sha, new_sha, new_shards, removed_shards


def _get_new_shards(old_sha):
    if old_sha is None:
        print(f"{HEAD}doing a full rebuild of repodata products", flush=True)
        new_shards = None
        removed_shards = None
        new_sha = subprocess.run(
            "cd repodata-shards && git rev-parse --verify HEAD",
            shell=True,
            check=True,
            capture_output=True,
        ).stdout.decode("utf-8").strip()
    else:
        with timer(HEAD, "pulling new shards"):
            old_sha, new_sha, new_shards, removed_shards = _get_new_shards_from_repo(
                old_sha
            )

    return old_sha, new_sha, new_shards, removed_shards


def _update_and_reimport_patch_fns(old_sha):
    subprocess.run(
        "cd conda-forge-repodata-patches-feedstock && git pull",
        shell=True,
        check=True,
    )
    new_sha = subprocess.run(
        "cd conda-forge-repodata-patches-feedstock && git rev-parse --verify HEAD",
        shell=True,
        capture_output=True,
        check=True,
    ).stdout.decode("utf-8").strip()
    print(f"{HEAD}old patches sha={old_sha}", flush=True)
    print(f"{HEAD}new patches sha={new_sha}", flush=True)
    if old_sha != new_sha:
        print(f"{HEAD}repatching all repodata in all subdirs & labels", flush=True)

    mpath = os.path.abspath("./conda-forge-repodata-patches-feedstock/recipe")
    if mpath not in sys.path:
        sys.path.append(mpath)
    if "get_license_family" in sys.modules:
        importlib.reload(sys.modules["get_license_family"])
    if "gen_patch_json" in sys.modules:
        importlib.reload(sys.modules["gen_patch_json"])
    from gen_patch_json import _add_removals, _gen_new_index

    @tenacity.retry(
        wait=tenacity.wait_random_exponential(multiplier=1, max=10),
        stop=tenacity.stop_after_attempt(5),
        reraise=True,
    )
    def gen_removals(subdir):
        ins = {"remove": []}
        _add_removals(ins, subdir)
        return sorted(ins["remove"])

    return (
        old_sha, new_sha,
        {"gen_new_index": _gen_new_index, "gen_removals": gen_removals},
    )


def _load_current_data(make_releases, allow_unsafe):
    all_links = {
        "packages": {},
        "serverdata": {},
        "current-shas": {},
        "labels": [],
    }

    if DEBUG:
        if (
            os.path.exists(f"{WORKDIR}/all_repodata.json")
            and os.path.exists(f"{WORKDIR}/all_links.json")
        ):
            with open(f"{WORKDIR}/all_repodata.json", "r") as fp:
                all_repodata = json.load(fp)

            with open(f"{WORKDIR}/all_links.json", "r") as fp:
                all_links = json.load(fp)

            return all_repodata, all_links
        else:
            return {}, all_links

    else:
        load_links = False
        try:
            rel = get_repodata().get_latest_release()
        except github.UnknownObjectException as e:
            if not allow_unsafe:
                raise e
            rel = None

        if rel is not None:
            for ast in rel.get_assets():
                if "links.json.bz2" in ast.name:
                    load_links = True
                    break

            if not load_links:
                if make_releases:
                    raise RuntimeError(
                        "Cannot find current links! This is not safe! Aborting!"
                    )
            else:
                all_links = get_latest_links()

        return {}, all_links


def _clone_repodata_shards():
    subprocess.run(
        "git clone https://github.com/conda-forge/repodata-shards.git",
        shell=True,
        check=True,
    )


def _clone_repodata():
    subprocess.run(
        f"git clone https://github.com/{REPODATA_REPO}.git",
        shell=True,
        check=True,
    )


def _get_repodata_sha():
    repo_sha = subprocess.run(
        f"cd {REPODATA_NAME} && git rev-parse --verify HEAD",
        shell=True,
        check=True,
        capture_output=True,
    ).stdout.decode("utf-8").strip()
    return repo_sha


def _clone_and_init_repodata_patches():
    subprocess.run(
        "git clone "
        "https://github.com/conda-forge/conda-forge-repodata-patches-feedstock.git",
        shell=True,
        check=True,
    )


def _rebuild_subdir(
    *, subdir, new_shards, removed_shards, repatch_all_pkgs,
    all_repodata, all_patched_repodata, all_links, updated_data,
    make_releases, main_only, patch_fns, futures, rel,
):
    if new_shards is not None:
        new_subdir_shards = [
            k
            for k in new_shards
            if k.startswith(f"repodata-shards/shards/{subdir}/")
        ]
    else:
        # this is a sentinal that indicates a full rebuild
        new_subdir_shards = None

    if removed_shards is not None:
        removed_subdir_shards = [
            k
            for k in removed_shards
            if k.startswith(f"repodata-shards/shards/{subdir}/")
        ]
    else:
        # this is a sentinal that indicates a full rebuild
        removed_subdir_shards = None

    if subdir not in all_repodata:
        all_repodata[subdir] = {}
    if subdir not in all_patched_repodata:
        all_patched_repodata[subdir] = {}

    subdir_updated_data = set()

    with timer(HEAD, "processing shards for subdir %s" % subdir):
        if (
            new_subdir_shards is None
            or len(new_subdir_shards) > 0
            or removed_subdir_shards is None
            or len(removed_subdir_shards) > 0
        ):
            with timer(HEAD, "making repodata", indent=1):
                subdir_updated_data = _update_repodata_from_shards(
                    all_repodata,
                    all_links,
                    new_subdir_shards,
                    removed_subdir_shards,
                    subdir,
                )
                updated_data |= subdir_updated_data
                all_labels = set(all_links["labels"])
                all_labels |= set(
                    [label for label in all_patched_repodata[subdir]])
                all_labels |= set(
                    [label for label in all_repodata[subdir]])
                all_links["labels"] = sorted(all_labels)

        if make_releases and (subdir_updated_data or repatch_all_pkgs):
            with timer(HEAD, "patching and writing repodata", indent=1):
                for label in all_links["labels"]:
                    if (
                        (subdir, label) not in updated_data
                        and not repatch_all_pkgs
                    ):
                        continue
                    if main_only and label != "main":
                        continue

                    if label not in all_repodata[subdir]:
                        all_repodata[subdir][label] = \
                            _fetch_repodata(all_links, subdir, label)

                    if label not in all_patched_repodata[subdir]:
                        all_patched_repodata[subdir][label] = \
                            _fetch_patched_repodata(
                                all_links, subdir, label
                            )

                    if label == "broken":
                        all_patched_repodata[subdir][label] = copy.deepcopy(
                            all_repodata[subdir][label]
                        )
                    else:
                        _patch_repodata(
                            all_repodata[subdir][label],
                            all_patched_repodata[subdir][label],
                            subdir,
                            patch_fns,
                            do_all=repatch_all_pkgs,
                        )

                    futures.extend(_write_compress_and_start_upload(
                        all_patched_repodata[subdir][label],
                        f"repodata_{subdir}_{label}.json",
                        rel,
                        exec,
                    ))

            with timer(
                HEAD, "building and writing current repodata", indent=1
            ):
                for label in all_links["labels"]:
                    if (
                        (subdir, label) not in updated_data
                        and not repatch_all_pkgs
                    ):
                        continue
                    if main_only and label != "main":
                        continue

                    if label not in all_patched_repodata[subdir]:
                        with timer(
                            HEAD,
                            f"fetching patched repodata for "
                            f"{label}/{subdir}",
                            indent=3,
                        ):
                            all_patched_repodata[subdir][label] = \
                                _fetch_patched_repodata(
                                    all_links, subdir, label
                                )

                    crd = build_current_repodata(
                        subdir,
                        all_patched_repodata[subdir][label],
                        )

                    futures.extend(_write_compress_and_start_upload(
                        crd,
                        f"current_repodata_{subdir}_{label}.json",
                        rel,
                        exec,
                    ))

            with timer(
                HEAD, "writing repodata from packages", indent=1
            ):
                for label in all_links["labels"]:
                    if (subdir, label) not in updated_data:
                        continue
                    if main_only and label != "main":
                        continue

                    futures.extend(_write_compress_and_start_upload(
                        all_repodata[subdir][label],
                        f"repodata_from_packages_{subdir}_{label}.json",
                        rel,
                        exec,
                    ))


@click.command()
@click.argument("time_limit", type=int)
@click.option(
    "--make-releases", is_flag=True, help="make github releases of the repo data")
@click.option(
    "--main-only", is_flag=True, help="only release the main channel")
@click.option(
    "--debug", is_flag=True, help="write data locally for debugging")
@click.option(
    "--allow-unsafe", is_flag=True, help="allow unsafe operation when making releases")
def main(time_limit, make_releases, main_only, debug, allow_unsafe):
    """Worker process for continuously building repodata for a maximum
    number of TIME_LIMIT seconds.
    """
    global DEBUG
    DEBUG = debug

    start_time = time.time()

    # refresh at the start
    refresh_github_token_and_client()

    with timer(HEAD, "pulling repos"):
        os.makedirs(WORKDIR, exist_ok=True)
        if not os.path.exists("repodata-shards"):
            _clone_repodata_shards()
        if not os.path.exists(REPODATA_NAME):
            _clone_repodata()
        if not os.path.exists("conda-forge-repodata-patches-feedstock"):
            _clone_and_init_repodata_patches()

    with timer(HEAD, "loading local data"):
        all_repodata, all_links = _load_current_data(make_releases, allow_unsafe)
        all_channeldata = {}
        all_patched_repodata = {}

    while time.time() - start_time < time_limit:
        __dt = time.time() - start_time
        print("===================================================", flush=True)
        print("===================================================", flush=True)
        print(
            "used %ds of %ds total - %ds remaining" % (
                __dt, time_limit, time_limit - __dt
            ),
            flush=True,
        )
        print("===================================================", flush=True)
        print("===================================================", flush=True)

        build_start_time = time.time()

        refresh_github_token_and_client()

        with timer(HEAD, "doing repodata products rebuild"), ThreadPoolExecutor(max_workers=8) as exec:  # noqa
            old_sha, new_sha, new_shards, removed_shards = _get_new_shards(
                all_links["current-shas"].get("repodata-shards-sha", None)
            )
            # TODO force repatch if local data is inconsistent
            old_patch_sha, new_patch_sha, patch_fns = _update_and_reimport_patch_fns(
                all_links["current-shas"].get("repodata-patches-sha", None)
            )
            repatch_all_pkgs = old_patch_sha != new_patch_sha
            utcnow = datetime.now().astimezone(pytz.UTC)

            updated_data = set()
            if (
                make_releases
                and
                # None is a full rebuild, otherwise len > 0 means we have new ones
                # to add
                # we have to make a release if we need to repatch everything as well
                (
                    new_shards is None
                    or len(new_shards) > 0
                    or removed_shards is None
                    or len(removed_shards) > 0
                    or repatch_all_pkgs
                )
            ):
                tag = utcnow.strftime("%Y.%m.%d.%H.%M.%S")
                rel = get_repodata().create_git_tag_and_release(
                    tag,
                    "",
                    tag,
                    "",
                    _get_repodata_sha(),
                    "commit",
                    draft=True,
                )
                futures = []
            else:
                # do this to catch errors
                futures = None

            for subdir in CONDA_FORGE_SUBIDRS:
                try:
                    _rebuild_subdir(
                        subdir=subdir,
                        new_shards=new_shards,
                        removed_shards=removed_shards,
                        repatch_all_pkgs=repatch_all_pkgs,
                        all_repodata=all_repodata,
                        all_patched_repodata=all_patched_repodata,
                        all_links=all_links,
                        updated_data=updated_data,
                        make_releases=make_releases,
                        main_only=main_only,
                        patch_fns=patch_fns,
                        futures=futures,
                        rel=rel,
                    )
                except Exception:
                    # rebuild it all if we error
                    _rebuild_subdir(
                        subdir=subdir,
                        new_shards=None,
                        removed_shards=None,
                        repatch_all_pkgs=True,
                        all_repodata=all_repodata,
                        all_patched_repodata=all_patched_repodata,
                        all_links=all_links,
                        updated_data=updated_data,
                        make_releases=make_releases,
                        main_only=main_only,
                        patch_fns=patch_fns,
                        futures=futures,
                        rel=rel,
                    )

            all_links["current-shas"]["repodata-shards-sha"] = new_sha
            all_links["current-shas"]["repodata-patches-sha"] = new_patch_sha

            if updated_data and make_releases:
                with timer(HEAD, "(re)building channel data"):
                    futures.extend(_build_channel_data(
                        all_channeldata,
                        all_links,
                        all_patched_repodata,
                        all_links["labels"],
                        updated_data,
                        rel,
                        exec,
                        make_releases=make_releases,
                        main_only=main_only,
                    ))

            if updated_data and make_releases:
                with timer(HEAD, "waiting for repo/channel data uploads to finish"):
                    for fut in concurrent.futures.as_completed(futures):
                        fname, url = fut.result()
                        if fname not in all_links["serverdata"]:
                            all_links["serverdata"][fname] = []
                        all_links["serverdata"][fname].append(url)
                        if len(all_links["serverdata"][fname]) > 3:
                            all_links["serverdata"][fname] = \
                                all_links["serverdata"][fname][-3:]
                    futures = []

                with timer(HEAD, "writing and uploading links"):
                    all_links["updated_at"] = utcnow.strftime("%Y-%m-%d %H:%M:%S %Z%z")
                    futures.extend(
                        _write_compress_and_start_upload(
                            all_links,
                            "links.json",
                            rel,
                            exec,
                            only_compress=True,
                        )
                    )
                    concurrent.futures.wait(futures)

                with timer(HEAD, "publishing release", result=False):
                    rel.update_release(rel.title, rel.body, draft=False)

            if make_releases:
                with timer(HEAD, "deleting old releases"):
                    tags = delete_old_repodata_releases(all_links)
                    for tag in tags:
                        print(f"{HEAD}deleted release {tag}", flush=True)

        dt = int(time.time() - build_start_time)

        if dt < MIN_UPDATE_TIME:
            print(
                "REPO WORKER: waiting for %s seconds before "
                "next update" % (MIN_UPDATE_TIME - dt),
                flush=True,
            )
            time.sleep(MIN_UPDATE_TIME - dt)

    if DEBUG:
        with timer(HEAD, "dumping all data to JSON"):
            with open(f"{WORKDIR}/all_repodata.json", "w") as fp:
                json.dump(all_repodata, fp, indent=2, sort_keys=True)
            with open(f"{WORKDIR}/all_patched_repodata.json", "w") as fp:
                json.dump(all_patched_repodata, fp, indent=2, sort_keys=True)
            with open(f"{WORKDIR}/all_links.json", "w") as fp:
                json.dump(all_links, fp, indent=2, sort_keys=True)
            with open(f"{WORKDIR}/all_channeldata.json", "w") as fp:
                json.dump(all_channeldata, fp, indent=2, sort_keys=True)
