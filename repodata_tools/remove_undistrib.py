import os
import tempfile
import subprocess
import time
import copy
import random
import functools

from git import Repo
import tenacity
import click
import rapidjson as json
import requests
import tqdm
import github
from github import RateLimitExceededException

from .utils import (
    split_pkg,
    print_github_api_limits,
    compute_subdir_pkg_index,
)
from .shards import (
    get_shard_path,
    read_subdir_shards,
)
from .releases import (
    get_or_make_release,
)
from .metadata import CONDA_FORGE_SUBIDRS, UNDISTRIBUTABLE, UNDISTRIBUTABLE_HASH


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

    subprocess.run("git status", shell=True)
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


@functools.lru_cache(maxsize=128)
def _get_cached_repodata(subdir, label):
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
        raise RuntimeError(f"Could not download repodata for {label}/{subdir}")

    return r.json()


def _remove_pkg_and_update_shard(subdir, pkg, shard, repo, repo_pth):
    rel, curr_asts = get_or_make_release(
        repo,
        subdir,
        pkg,
        repo_pth=repo_pth,
        make_commit=False,
    )
    for ast in curr_asts:
        print("removing asset %s for %s/%s" % (ast, subdir, pkg), flush=True)
        ast.delete_asset()
    tagname = rel.tag_name
    rel.delete_release()
    try:
        subprocess.run(
            f"cd {repo_pth} && git push --delete origin \"{tagname}\"",
            shell=True,
        )
    except Exception:
        pass

    shard["url"] = "https://conda.anaconda.org/conda-forge/%s/%s" % (subdir, pkg)
    for label in shard["labels"]:
        rd = _get_cached_repodata(subdir, label)
        if pkg in rd["packages"]:
            shard["repodata"]["md5"] = rd["packages"][pkg]["md5"]
            break


def remove_undistributable(
    all_shards, rank, n_ranks, start_time, time_limit, max_write=400
):
    upload_sleep_factor = float(os.environ.get("UPLOAD_SLEEP_FACTOR", "1.0"))

    gh = github.Github(os.environ["GITHUB_TOKEN"])
    repo = gh.get_repo("conda-forge/releases")

    with tempfile.TemporaryDirectory() as tmpdir:
        Repo.clone_from(
            "https://github.com/conda-forge/releases.git",
            tmpdir,
            multi_options=["--no-tags"],
        )
        subprocess.run(
            f"cd {tmpdir} && git remote set-url --push origin "
            "https://${DELTAG_GITHUB_TOKEN}@github.com/conda-forge/releases.git",
            shell=True,
            check=True,
        )
        shards_to_write = set()
        pkgs = sorted([
            subdir_pkg
            for subdir_pkg in all_shards
            if (
                compute_subdir_pkg_index(subdir_pkg) % n_ranks == rank
                and split_pkg(subdir_pkg)[1] in UNDISTRIBUTABLE
                and (
                    all_shards[subdir_pkg].get("undistributable_hash", None)
                    != UNDISTRIBUTABLE_HASH
                )
            )
        ])
        for pkg_index, subdir_pkg in tqdm.tqdm(enumerate(pkgs), total=len(pkgs)):
            subdir, pkg = os.path.split(subdir_pkg)
            _, pkg_name, _, _ = split_pkg(subdir_pkg)

            try:
                shard = copy.deepcopy(all_shards[subdir_pkg])
                _remove_pkg_and_update_shard(subdir, pkg, shard, repo, tmpdir)

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
                shard["undistributable_hash"] = UNDISTRIBUTABLE_HASH
                all_shards[subdir_pkg] = shard
                shards_to_write.add(subdir_pkg)
            finally:
                time.sleep(random.uniform(12.5, 17.5) * upload_sleep_factor)

            if (
                len(shards_to_write) >= max_write
                or time.time() - start_time > time_limit
            ):
                break

        if len(shards_to_write) > 0:
            _write_shards(
                shards_to_write,
                all_shards,
                f"remove undistributable {pkg_index+1} of {len(pkgs)} for rank {rank}",
            )
            try:
                _push_repo()
            except Exception:
                pass

    print_github_api_limits(gh)
    print("removed %d releases" % len(shards_to_write), flush=True)


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
    default=2700,
    type=int,
    help="The maximum time to run in seconds."
)
def main(rank, n_ranks, time_limit):
    """Remove undistributable packages.
    """
    start_time = time.time()

    all_shards = {}
    print("reading all shards", flush=True)
    for subdir in CONDA_FORGE_SUBIDRS:
        old_len = len(all_shards)
        read_subdir_shards(".", subdir, all_shards)
        print(
            "found %d repodata shards for subdir %s" % (
                len(all_shards) - old_len, subdir
            ),
            flush=True,
        )
    print(" ", flush=True)

    print("removing undistributable packages", flush=True)

    remove_undistributable(
        all_shards,
        rank,
        n_ranks,
        start_time,
        time_limit,
        max_write=400,
    )
    print(" ", flush=True)
