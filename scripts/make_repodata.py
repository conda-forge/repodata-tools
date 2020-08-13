import os
import sys
import copy
import glob
import subprocess

import rapidjson as json
import joblib
import requests


def chunk(iterable, chunk_size):
    """Generate sequences of `chunk_size` elements from `iterable`.

    https://stackoverflow.com/a/12797249/1745538
    """
    iterable = iter(iterable)
    while True:
        chunk = []
        try:
            for _ in range(chunk_size):
                chunk.append(next(iterable))
            yield chunk
        except StopIteration:
            if chunk:
                yield chunk
            break


def gen_shards(shards_repo, subdir, chunksize=1024):
    shards = glob.glob(
        os.path.join(
            shards_repo,
            "shards",
            subdir,
            "*", "*", "*",
            "*", "*", "*",
            "*", "*", "*",
            "*", "*", "*",
            "*.json"
        )
    )
    return shards


def _read_shards(shard_pths):
    shards = []
    for shard_pth in shard_pths:
        with open(shard_pth, "r") as fp:
            shards.append(json.load(fp))
    return shards


def build_links_and_repodata_from_packages(
    shards_repo,
    subdir,
    override_labels=None,
    removed=None,
):
    override_labels = override_labels or {}
    removed = removed or []
    init_repodata = {
        'info': {'subdir': subdir},
        'packages': {},
        'packages.conda': {},
        'removed': [],
        'repodata_version': 1
    }
    repodata = {}
    links = {}

    shard_paths = gen_shards(shards_repo, subdir)
    tot = len(shard_paths)
    print("found %d repodata shards for %s" % (tot, subdir), flush=True)

    n_jobs = 8
    with joblib.Parallel(n_jobs=n_jobs, backend="threading", verbose=100) as p:
        shards_lists = p(
            joblib.delayed(_read_shards)(s)
            for s in chunk(shard_paths, tot // n_jobs)
        )

    assert sum(len(s) for s in shards_lists) == len(shard_paths)

    for shards in shards_lists:
        for shard in shards:
            subdir_pkg = os.path.join(shard["subdir"], shard["package"])
            shard["labels"] = override_labels.get(subdir_pkg, shard["labels"])
            for label in shard["labels"]:
                if label not in repodata:
                    repodata[label] = copy.deepcopy(init_repodata)
                if label not in links:
                    links[label] = {}
                repodata[label]["packages"][shard["package"]] = shard["repodata"]
                links[label][subdir_pkg] = shard["url"]

    if "main" in repodata:
        repodata["main"]["removed"] = removed

    return repodata, links


if __name__ == "__main__":
    tm = sys.argv[1]
    shards_path = sys.argv[2]
    links = {}
    subdirs = ["linux-64", "osx-64", "win-64", "linux-aarch64", "linux-ppc64le"]
    for subdir in subdirs:
        r = requests.get(
            f"https://conda.anaconda.org/conda-forge/{subdir}/repodata.json"
        )
        rd, _links = build_links_and_repodata_from_packages(
            shards_path,
            subdir,
            removed=r.json()["removed"],
        )
        for label in rd:
            with open(f"repodata_{subdir}_{label}.json", "w") as fp:
                json.dump(rd[label], fp)
            subprocess.run(
                f"bzip2 --keep repodata_{subdir}_{label}.json",
                shell=True,
            )
        for label in _links:
            if label not in links:
                links[label] = {}
            links[label].update(_links[label])
    with open("links.json", "w") as fp:
        json.dump(links, fp)
    subprocess.run(
        "bzip2 links.json",
        shell=True,
    )
