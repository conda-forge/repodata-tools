import os
import tempfile
import subprocess
import time
import hmac
import copy
import sys
import random

from git import Repo
import tenacity
import click
import rapidjson as json
import requests
import tqdm
import github
import joblib
from github import RateLimitExceededException

from .utils import (
    chunk_iterable,
    compute_md5,
    split_pkg,
    print_github_api_limits,
)
from .shards import (
    make_repodata_shard_noretry,
    get_old_shard_path,
    get_shard_path,
    read_subdir_shards,
)
from .releases import (
    get_or_make_release,
    upload_asset
)
from .metadata import CONDA_FORGE_SUBIDRS


def _build_shard(subdir, pkg, label):
    try:
        subdir_pkg = os.path.join(subdir, pkg)
        if label == "main":
            url = (
                "https://conda.anaconda.org/conda-forge"
                f"/{subdir_pkg}"
            )
        else:
            url = (
                "https://conda.anaconda.org/conda-forge"
                f"/label/{label}/{subdir_pkg}"
            )

        with tempfile.TemporaryDirectory() as tmpdir:
            shard = make_repodata_shard_noretry(
                subdir,
                pkg,
                label,
                None,
                url,
                tmpdir,
            )
    except Exception as e:
        print("\n\n\nERROR: %s\n\n\n" % subdir_pkg, flush=True)
        raise e

    return shard


def _write_shards(shards_to_write, all_shards, msg):
    for subdir_pkg in shards_to_write:
        pth = get_shard_path(*os.path.split(subdir_pkg))

        if subdir_pkg in all_shards:
            dir = os.path.dirname(pth)
            os.makedirs(dir, exist_ok=True)

            with open(pth, "w") as fp:
                json.dump(
                    all_shards[subdir_pkg], fp, sort_keys=True, indent=2
                )

        subprocess.run(f"git add {pth}", shell=True)

    subprocess.run(
        f"git commit --allow-empty -m '{msg} [ci skip] [cf admin skip] ***NO_CI***'",
        shell=True,
        check=True,
    )


@tenacity.retry(
    wait=tenacity.wait_random_exponential(multiplier=0.1, max=10),
    stop=tenacity.stop_after_attempt(5),
    reraise=True,
)
def _push_repo():
    subprocess.run("git pull --no-edit", shell=True, check=True)
    subprocess.run("git push", shell=True, check=True)


def update_shards(labels, all_shards, rank, n_ranks, start_time, time_limit=3300):
    cd = requests.get(
            "https://conda.anaconda.org/conda-forge/channeldata.json"
        ).json()

    shards_to_write = set()
    for label in tqdm.tqdm(labels, desc="labels"):

        for loop_index, subdir in enumerate(CONDA_FORGE_SUBIDRS):
            if loop_index % n_ranks != rank:
                continue

            if label == "main":
                r = requests.get(
                    "https://conda.anaconda.org/conda-forge/"
                    f"{subdir}/repodata_from_packages.json"
                )
            else:
                r = requests.get(
                    "https://conda.anaconda.org/conda-forge/label/"
                    f"{label}/{subdir}/repodata.json"
                )

            if r.status_code != 200:
                continue

            rd = r.json()

            os.makedirs(f"shards/{subdir}", exist_ok=True)

            all_pkgs = sorted(list(rd["packages"]))

            total_chunks = len(rd["packages"]) // 64 + 1
            for chunk_index, pkg_chunk in tqdm.tqdm(
                enumerate(chunk_iterable(all_pkgs, 64)),
                desc=f"{label}/{subdir}",
                total=total_chunks,
            ):
                jobs = []
                max_bytes = 0
                for pkg in pkg_chunk:
                    subdir_pkg = os.path.join(subdir, pkg)

                    new_shard_pth = get_shard_path(subdir, pkg)

                    for old_shard_pth in [
                        get_old_shard_path(subdir, pkg),
                        get_shard_path(subdir, pkg, n_dirs=4),
                    ]:
                        if os.path.exists(old_shard_pth):
                            if not os.path.exists(new_shard_pth):
                                os.makedirs(
                                    os.path.dirname(new_shard_pth),
                                    exist_ok=True,
                                )
                                subprocess.run(
                                    "git mv %s %s" % (
                                        old_shard_pth, get_shard_path(subdir, pkg)
                                    ),
                                    shell=True,
                                    check=True,
                                )
                                shards_to_write.add(subdir_pkg)
                                with open(new_shard_pth, "r") as fp:
                                    all_shards[subdir_pkg] = json.load(fp)
                            else:
                                subprocess.run(
                                    "git rm -f %s" % old_shard_pth,
                                    shell=True,
                                    check=True,
                                )

                            break

                    if subdir_pkg not in all_shards:
                        max_bytes = max(max_bytes, rd["packages"][pkg]["size"])
                        jobs.append(joblib.delayed(_build_shard)(
                            subdir, pkg, label
                        ))
                    else:
                        if label not in all_shards[subdir_pkg]["labels"]:
                            all_shards[subdir_pkg]["labels"].append(label)
                            shards_to_write.add(subdir_pkg)

                        main_url = (
                            "https://conda.anaconda.org/conda-forge"
                            f"/{subdir_pkg}"
                        )
                        if (
                            label == "main"
                            and all_shards[subdir_pkg]["url"] != main_url
                            and "conda.anaconda.org" in all_shards[subdir_pkg]["url"]
                        ):
                            all_shards[subdir_pkg]["url"] = main_url
                            shards_to_write.add(subdir_pkg)

                if jobs:
                    max_gb = max_bytes / 1000**3
                    n_jobs = min(max(int(1.0 / max_gb), 1), 16)
                    print(
                        "using %d processes for %d jobs w/ max GB of %s" % (
                            n_jobs, len(jobs), max_gb
                        ),
                        flush=True,
                    )
                    shards = joblib.Parallel(n_jobs=n_jobs, verbose=0)(jobs)
                    for shard in shards:
                        subdir_pkg = os.path.join(shard["subdir"], shard["package"])

                        # sometimes conda index chokes on a package, so we put in the
                        # data we have by hand
                        if (
                            shard["repodata"] is None
                            and shard["package"] in rd["packages"]
                        ):
                            shard["repodata_version"] = rd.get("repodata_version", 1)
                            shard["repodata"] = copy.deepcopy(rd["packages"][pkg])

                        if (
                            shard["channeldata"] is None
                            and shard["repodata"] is not None
                            and shard["repodata"]["name"] in cd["packages"]
                        ):
                            shard["channeldata_version"] = cd["channeldata_version"]
                            shard["channeldata"] = copy.deepcopy(
                                cd["packages"][shard["repodata"]["name"]]
                            )
                            shard["channeldata"]["subdirs"] = [subdir]
                            shard["channeldata"]["version"] = (
                                shard["repodata"]["version"]
                            )

                        all_shards[subdir_pkg] = shard
                        shards_to_write.add(subdir_pkg)

                if len(shards_to_write) >= 64 or time.time() - start_time > time_limit:
                    _write_shards(
                        shards_to_write,
                        all_shards,
                        f"chunk {chunk_index + 1} of {total_chunks} {label}/{subdir}",
                    )
                    shards_to_write = set()

                    try:
                        _push_repo()
                    except Exception:
                        pass

                if time.time() - start_time > time_limit:
                    return True

    if shards_to_write:
        _write_shards(
            shards_to_write,
            all_shards,
            f"chunk {chunk_index + 1} of {total_chunks} {label}/{subdir}",
        )

        try:
            _push_repo()
        except Exception:
            pass

    return False


@tenacity.retry(
    wait=tenacity.wait_random_exponential(multiplier=0.1, max=10),
    stop=tenacity.stop_after_attempt(5),
    reraise=True,
)
def _download_package(tmpdir, subdir, pkg, url, md5_checksum):
    os.makedirs(f"{tmpdir}/{subdir}", exist_ok=True)
    r = requests.head(url)
    if r.status_code != 200:
        _, name, ver, _ = split_pkg(os.path.join(subdir, pkg))
        url = f"https://anaconda.org/conda-forge/{name}/{ver}/download/{subdir}/{pkg}"

    subprocess.run(
        f"curl --no-progress-meter -L {url} > {tmpdir}/{subdir}/{pkg}",
        shell=True,
        check=True,
    )

    if md5_checksum is not None:
        local_md5 = compute_md5(f"{tmpdir}/{subdir}/{pkg}")
        if not hmac.compare_digest(local_md5, md5_checksum):
            raise RuntimeError("md5 chechsum is incorrect! exiting!")


def _make_release(subdir, pkg, shard, repo, repo_pth):
    # make release and upload if shard does not exist
    with tempfile.TemporaryDirectory() as tmpdir:
        _download_package(
            tmpdir, subdir, pkg, shard["url"], shard["repodata"]["md5"]
        )
        rel, curr_asts = get_or_make_release(
            repo,
            subdir,
            pkg,
            repo_pth=repo_pth,
            make_commit=False,
        )

        ast = upload_asset(
            rel,
            curr_asts,
            f"{tmpdir}/{subdir}/{pkg}",
            content_type="application/x-bzip2",
        )

        shard["url"] = ast.browser_download_url
        with open(f"{tmpdir}/repodata_shard.json", "w") as fp:
            json.dump(shard, fp, sort_keys=True, indent=2)

        upload_asset(
            rel,
            curr_asts,
            f"{tmpdir}/repodata_shard.json",
            content_type="application/json",
        )


def upload_packages(
    all_shards, rank, n_ranks, start_time, time_limit, max_write=400
):

    gh = github.Github(os.environ["GITHUB_TOKEN"])
    repo = gh.get_repo("regro/releases")

    with tempfile.TemporaryDirectory() as tmpdir:
        Repo.clone_from("https://github.com/regro/releases.git", tmpdir)
        subprocess.run(
            f"cd {tmpdir} && git remote set-url --push origin "
            "https://${GITHUB_TOKEN}@github.com/regro/releases.git",
            shell=True,
            check=True,
        )
        shards_to_write = set()
        pkgs = sorted([
            subdir_pkg
            for subdir_pkg in all_shards
            if CONDA_FORGE_SUBIDRS.index(os.path.split(subdir_pkg)[0]) % n_ranks == rank
        ])
        for pkg_index, subdir_pkg in tqdm.tqdm(enumerate(pkgs), total=len(pkgs)):
            subdir, pkg = os.path.split(subdir_pkg)
            if CONDA_FORGE_SUBIDRS.index(subdir) % n_ranks != rank:
                continue

            if "conda.anaconda.org" in all_shards[subdir_pkg]["url"]:
                try:
                    print("releasing %s..." % subdir_pkg, flush=True)
                    shard = copy.deepcopy(all_shards[subdir_pkg])
                    _make_release(subdir, pkg, shard, repo, tmpdir)
                except RateLimitExceededException:
                    print(
                        "\n\nGitHub API rate limit exceeded - exiting\n\n",
                        flush=True,
                    )
                    print_github_api_limits(gh)
                    break
                except Exception as e:
                    print("\n\nERROR: %s\n\n" % repr(e), flush=True)
                    pass
                else:
                    all_shards[subdir_pkg] = shard
                    shards_to_write.add(subdir_pkg)
                    print("made %d releases" % len(shards_to_write), flush=True)
                    print_github_api_limits(gh)
                finally:
                    time.sleep(random.uniform(10, 20.0))

            if (
                len(shards_to_write) >= max_write
                or time.time() - start_time > time_limit
            ):
                break

        if len(shards_to_write) > 0:
            _write_shards(
                shards_to_write,
                all_shards,
                f"release {pkg_index+1} of {len(pkgs)} for {subdir}",
            )
            try:
                _push_repo()
            except Exception:
                pass

    print("made %d releases" % len(shards_to_write), flush=True)


@click.command()
@click.option(
    "--rank",
    default=0,
    type=int,
    help="The rank of the process. Should be in tha range [0, n_ranks-1]."
)
@click.option(
    "--n-ranks",
    default=1,
    type=int,
    help="The number of processes to split the sync over."
)
@click.option(
    "--time-limit",
    default=2400,
    type=int,
    help="The maximum time to run in seconds."
)
def main(rank, n_ranks, time_limit):
    """Sync anaconda repodata shards w/ a local copy and upload packages.
    """
    start_time = time.time()

    all_shards = {}
    print("reading all shards", flush=True)
    for subdir in CONDA_FORGE_SUBIDRS:
        read_subdir_shards(".", subdir, all_shards)
    print(" ", flush=True)

    print("getting labels", flush=True)
    label_info = requests.get(
        "https://api.anaconda.org/channels/conda-forge",
        headers={'Authorization': 'token {}'.format(os.environ["BINSTAR_TOKEN"])}
    ).json()

    labels = sorted(
        label
        for label in label_info
        if "/" not in label
    )
    counts = {label: label_info[label]["count"] for label in labels}
    labels = sorted(labels, key=lambda x: counts[x], reverse=True)
    for label in labels:
        print("%-32s %s" % (label, counts[label]), flush=True)
    print(" ", flush=True)

    print("updating shards", flush=True)
    update_shards(
        labels,
        all_shards,
        rank,
        n_ranks,
        start_time,
        time_limit=time_limit,
    )
    print(" ", flush=True)

    if time.time() - start_time > time_limit:
        sys.exit(0)

    print("uploading releases", flush=True)

    try:
        _push_repo()
    except Exception:
        pass

    upload_packages(
        all_shards,
        rank,
        n_ranks,
        start_time,
        time_limit,
        max_write=400,
    )
    print(" ", flush=True)
