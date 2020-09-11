import time
import os
import io
import bz2
import subprocess
import copy
from datetime import datetime
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor

import github
import tenacity
import requests
import rapidjson as json
import click

from .shards import read_subdir_shards
from .metadata import CONDA_FORGE_SUBIDRS
from .utils import timer

from .index import (
    get_latest_links,
    get_broken_packages,
    upload_repodata_asset,
    delete_old_repodata_releases,
    build_or_update_channeldata,
    build_or_update_links_and_repodata,
    REPODATA,
    INIT_REPODATA,
)

WORKDIR = "repodata_products"
MIN_UPDATE_TIME = 30
HEAD = "REPO WORKER: "
DEBUG = False


@tenacity.retry(
    wait=tenacity.wait_random_exponential(multiplier=1, max=60),
    stop=tenacity.stop_after_attempt(10),
    reraise=True,
)
def _fetch_repodata(links, subdir, label):
    fn = f"repodata_{subdir}_{label}.json"
    if fn in links["serverdata"]:
        url = links["serverdata"][fn][-1]
        if not url.endswith(".bz2"):
            url += ".bz2"
        r = requests.get(url)
        return json.load(io.StringIO(bz2.decompress(r.content).decode("utf-8")))
    else:
        rd = copy.deepcopy(INIT_REPODATA)
        rd["info"]["subdir"] = subdir
        return None


def _update_repodata_from_shards(repodata, links, new_shards, subdir):
    all_shards = {}
    read_subdir_shards("repodata-shards", subdir, all_shards, shard_paths=new_shards)
    print(
        f"{HEAD}    found {len(all_shards)} repodata shards for subdir {subdir}",
        flush=True,
    )
    if new_shards is not None:
        assert len(all_shards) == len(new_shards)

    rd_broken = get_broken_packages(subdir)

    return build_or_update_links_and_repodata(
        repodata,
        links,
        subdir,
        all_shards,
        removed=list(rd_broken["packages"]),
        fetch_repodata=None if DEBUG else _fetch_repodata,
    )


def _get_new_shards_from_repo(old_sha):
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


def _get_new_shards(current_shas):
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
            old_sha, new_sha, new_shards = _get_new_shards_from_repo(
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


def _load_current_data(make_releases, allow_unsafe):
    all_links = {
        "packages": {},
        "serverdata": {},
        "current-shas": {},
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
            rel = REPODATA.get_latest_release()
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
        "git clone https://github.com/regro/repodata-shards.git",
        shell=True,
        check=True,
    )


def _clone_repodata():
    subprocess.run(
        "git clone https://github.com/regro/repodata.git",
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

    with timer(HEAD, "initializing git and pulling repodata shards"):
        os.makedirs(WORKDIR, exist_ok=True)
        _init_git()
        if not os.path.exists("repodata-shards"):
            _clone_repodata_shards()
        if not os.path.exists("repodata"):
            _clone_repodata()

    with timer(HEAD, "loading local data"):
        all_repodata, all_links = _load_current_data(make_releases, allow_unsafe)
        all_channeldata = {}

    while time.time() - start_time < time_limit:
        build_start_time = time.time()

        with timer(HEAD, "doing repodata products rebuild"), ThreadPoolExecutor(max_workers=8) as exec:  # noqa
            old_sha, new_sha, new_shards = _get_new_shards(all_links["current-shas"])

            updated_data = set()
            if (
                make_releases
                and
                # None is a full rebuild, otherwise it means we have new ones to add
                (new_shards is None or len(new_shards) > 0)
            ):
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
                futures = []

            for subdir in CONDA_FORGE_SUBIDRS:
                if new_shards is not None:
                    new_subdir_shards = [
                        k
                        for k in new_shards
                        if k.startswith(f"repodata-shards/shards/{subdir}/")
                    ]
                else:
                    # this is a sentinal that indicates a full rebuild
                    new_subdir_shards = None

                with timer(HEAD, "processing subdir %s" % subdir):
                    if new_subdir_shards is None or len(new_subdir_shards) > 0:
                        with timer(HEAD, "making repodata", indent=1):
                            updated_data |= _update_repodata_from_shards(
                                all_repodata,
                                all_links,
                                new_subdir_shards,
                                subdir,
                            )

                        if make_releases:
                            with timer(HEAD, "writing repodata data", indent=1):
                                for label in all_repodata[subdir]:
                                    if (subdir, label) not in updated_data:
                                        continue
                                    if main_only and label != "main":
                                        continue

                                    pth = f"{WORKDIR}/repodata_{subdir}_{label}.json"
                                    with open(pth, "w") as fp:
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

                                    futures.append(exec.submit(
                                        upload_repodata_asset,
                                        rel,
                                        pth,
                                        "application/json",
                                    ))
                                    pth += ".bz2"
                                    futures.append(exec.submit(
                                        upload_repodata_asset,
                                        rel,
                                        pth,
                                        "application/x-bzip2",
                                    ))

            all_links["current-shas"]["repodata-shards-sha"] = new_sha

            with timer(HEAD, "(re)building channel data"):
                # we have to make sure that any repodata for other subdirs not
                # updated on a specific label is present locally
                all_labels = set()
                for subdir in CONDA_FORGE_SUBIDRS:
                    if subdir not in all_repodata:
                        all_repodata[subdir] = {}
                    all_labels |= set([label for label in all_repodata[subdir]])

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
                                if label not in all_repodata[subdir]:
                                    with timer(
                                        HEAD, f"fetching repodata for {label}/{subdir}",
                                        indent=3,
                                    ):
                                        all_repodata[subdir][label] = _fetch_repodata(
                                            all_links, subdir, label
                                        )

                                channel_data = all_channeldata.get(label, {})
                                build_or_update_channeldata(
                                    channel_data,
                                    all_repodata[subdir][label],
                                    subdir,
                                )
                                all_channeldata[label] = channel_data

                        if make_releases:
                            pth = f"{WORKDIR}/channeldata_{label}.json"

                            with open(pth, "w") as fp:
                                json.dump(
                                    all_channeldata[label],
                                    fp,
                                    indent=2,
                                    sort_keys=True,
                                )

                            futures.append(exec.submit(
                                upload_repodata_asset, rel, pth, "application/json"
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

                with timer(HEAD, "writing links"):
                    with open(f"{WORKDIR}/links.json", "w") as fp:
                        json.dump(all_links, fp, indent=2, sort_keys=True)
                    subprocess.run(
                        f"cd {WORKDIR} && "
                        "rm -f links.json.bz2 && "
                        "bzip2 --keep links.json",
                        shell=True,
                    )

                with timer(HEAD, "waiting for links upload to finish"):
                    pth = f"{WORKDIR}/links.json.bz2"
                    futures.append(exec.submit(
                        upload_repodata_asset, rel, pth, "application/x-bzip2"
                    ))
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

        print(" ", flush=True)

    if DEBUG:
        with timer(HEAD, "dumping all data to JSON"):
            with open(f"{WORKDIR}/all_repodata.json", "w") as fp:
                json.dump(all_repodata, fp, indent=2, sort_keys=True)
            with open(f"{WORKDIR}/all_links.json", "w") as fp:
                json.dump(all_links, fp, indent=2, sort_keys=True)
            with open(f"{WORKDIR}/all_channeldata.json", "w") as fp:
                json.dump(all_channeldata, fp, indent=2, sort_keys=True)
