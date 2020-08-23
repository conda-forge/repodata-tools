import os
import sys
import copy
import glob
import tempfile
import subprocess
import time
import random
import hashlib

import requests
import rapidjson as json
import tqdm
import joblib


TIME_LIMIT = 55 * 60


def chunk(iterable, chunk_size):
    """Generate sequences of `chunk_size` elements from `iterable`.

    https://stackoverflow.com/a/12797249/1745538
    """
    chunk_size = max(chunk_size, 1)

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


def get_old_shard_path(subdir, pkg, n_dirs=12):
    chars = [c for c in pkg if c.isalnum()]
    while len(chars) < n_dirs:
        chars.append("z")

    pth_parts = (
        ["shards", subdir]
        + [chars[i] for i in range(n_dirs)]
        + [pkg + ".json"]
    )

    return os.path.join(*pth_parts)


def get_shard_path(subdir, pkg, n_dirs=3):
    hex = hashlib.sha1(pkg.encode("utf-8")).hexdigest()[0:n_dirs]

    pth_parts = (
        ["shards", subdir]
        + [hex[i] for i in range(n_dirs)]
        + [pkg + ".json"]
    )

    return os.path.join(*pth_parts)


def gen_shards(shards_repo, subdir, chunksize=1024):
    shards = glob.glob(
        os.path.join(
            shards_repo,
            "shards",
            subdir,
            "*", "*", "*",
            "*.json"
        )
    )
    return shards


def _read_shard_chunk(shard_pths):
    shards = []
    for shard_pth in shard_pths:
        with open(shard_pth, "r") as fp:
            shards.append(json.load(fp))
    return shards


def read_subdir_shards(shards_repo, subdir, all_shards):
    shard_paths = gen_shards(shards_repo, subdir)
    tot = len(shard_paths)
    print("found %d repodata shards for subdir %s" % (tot, subdir), flush=True)

    n_jobs = 8
    with joblib.Parallel(n_jobs=n_jobs, backend="threading", verbose=100) as p:
        shards_lists = p(
            joblib.delayed(_read_shard_chunk)(s)
            for s in chunk(shard_paths, tot // n_jobs)
        )

    assert sum(len(s) for s in shards_lists) == len(shard_paths)

    for shards in shards_lists:
        for shard in shards:
            subdir_pkg = os.path.join(shard["subdir"], shard["package"])
            all_shards[subdir_pkg] = shard


def make_repodata_shard(subdir, pkg, label, feedstock, url, tmpdir):
    os.makedirs(f"{tmpdir}/noarch", exist_ok=True)
    os.makedirs(f"{tmpdir}/{subdir}", exist_ok=True)
    subprocess.run(
        f"curl --no-progress-meter -L {url} > {tmpdir}/{subdir}/{pkg}",
        shell=True
    )
    subprocess.run(
        f"conda index --no-progress {tmpdir}",
        shell=True
    )

    with open(f"{tmpdir}/channeldata.json", "r") as fp:
        cd = json.load(fp)

    with open(f"{tmpdir}/{subdir}/repodata.json", "r") as fp:
        rd = json.load(fp)

    shard = {}
    shard["labels"] = [label]
    shard["repodata_version"] = rd["repodata_version"]
    shard["repodata"] = rd["packages"][pkg]
    shard["subdir"] = subdir
    shard["package"] = pkg
    shard["url"] = url
    shard["feedstock"] = feedstock

    # we are hacking at this
    shard["channeldata_version"] = cd["channeldata_version"]
    shard["channeldata"] = copy.deepcopy(
        cd["packages"][rd["packages"][pkg]["name"]]
    )

    return shard


def _build_shard(subdir, pkg, label):
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
        shard = make_repodata_shard(
            subdir,
            pkg,
            label,
            None,
            url,
            tmpdir,
        )

    return shard


if __name__ == "__main__":
    start_time = time.time()
    rank = int(sys.argv[1])
    n_ranks = 4

    all_shards = {}
    print("reading all shards")
    for subdir in [
        "linux-64", "linux-aarch64", "linux-ppc64le",
        "osx-64", "win-64",
        "noarch", "osx-arm64"
    ]:
        read_subdir_shards(".", subdir, all_shards)
    print(" ")

    print("getting labels")
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
        print("%-32s %s" % (label, counts[label]))
    print(" ")

    print("updating shards")
    shards_to_write = set()
    for label in tqdm.tqdm(labels, desc="labels"):
        count = label_info[label]["count"]

        for loop_index, subdir in enumerate([
            "linux-64", "osx-64", "win-64", "noarch",
            "linux-aarch64", "linux-ppc64le", "osx-arm64"
        ]):
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
            rd = r.json()

            os.makedirs(f"shards/{subdir}", exist_ok=True)

            all_pkgs = list(rd["packages"])
            random.shuffle(all_pkgs)

            total_chunks = len(rd["packages"]) // 64 + 1
            for chunk_index, pkg_chunk in tqdm.tqdm(
                enumerate(chunk(all_pkgs, 64)),
                desc=f"{label}/{subdir}",
                total=total_chunks,
            ):
                jobs = []
                for pkg in pkg_chunk:
                    subdir_pkg = os.path.join(subdir, pkg)

                    new_shard_pth = get_shard_path(subdir, pkg)

                    for old_shard_pth in [
                        get_old_shard_path(subdir, pkg),
                        get_shard_path(subdir, pkg, n_dirs=4),
                    ]:
                        if os.path.exists(old_shard_pth):
                            os.makedirs(os.path.dirname(new_shard_pth), exist_ok=True)
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

                            break

                    if subdir_pkg not in all_shards:
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
                    for n_jobs in [16, 8, 4]:
                        try:
                            shards = joblib.Parallel(n_jobs=n_jobs, verbose=0)(jobs)
                        except Exception:
                            pass
                        else:
                            break
                    for shard in shards:
                        subdir_pkg = os.path.join(shard["subdir"], shard["package"])
                        all_shards[subdir_pkg] = shard
                        shards_to_write.add(subdir_pkg)

                if shards_to_write:
                    subprocess.run("git pull", shell=True)

                    for subdir_pkg in shards_to_write:
                        _, pkg = os.path.split(subdir_pkg)
                        pth = get_shard_path(subdir, pkg)

                        if subdir_pkg in all_shards:
                            dir = os.path.dirname(pth)
                            os.makedirs(dir, exist_ok=True)

                            with open(pth, "w") as fp:
                                json.dump(
                                    all_shards[subdir_pkg], fp, sort_keys=True, indent=2
                                )

                        subprocess.run(f"git add {pth}", shell=True)

                    try:
                        cip1 = chunk_index + 1
                        subprocess.run(
                            "git commit -m '[ci skip]  [cf admin skip] ***NO_CI*** "
                            f"chunk {cip1} of {total_chunks} {label}/{subdir}'",
                            shell=True,
                            check=True,
                        )
                        subprocess.run("git pull", shell=True, check=True)
                        subprocess.run("git push", shell=True, check=True)
                    except Exception:
                        pass
                    else:
                        shards_to_write = set()

                if time.time() - start_time > TIME_LIMIT:
                    sys.exit(0)
