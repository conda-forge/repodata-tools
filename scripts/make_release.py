import os
import rapidjson as json
import tempfile
import subprocess
import copy
import base64
import hashlib
import sys
import hmac

import github
import requests
import tenacity


def compute_md5(pth):
    with open(pth, "rb") as f:
        file_hash = hashlib.md5()
        while True:
            chunk = f.read(8192)
            if not chunk:
                break
            file_hash.update(chunk)
    return file_hash.hexdigest()


def get_shard_path(subdir, pkg, n_dirs=3):
    hex = hashlib.sha1(pkg.encode("utf-8")).hexdigest()[0:n_dirs]

    pth_parts = (
        ["shards", subdir]
        + [hex[i] for i in range(n_dirs)]
        + [pkg + ".json"]
    )

    return os.path.join(*pth_parts)


@tenacity.retry(
    wait=tenacity.wait_random_exponential(multiplier=1, max=10),
    stop=tenacity.stop_after_attempt(5),
    reraise=True,
)
def make_repodata_shard(subdir, pkg, label, feedstock, url, tmpdir, md5_val):
    os.makedirs(f"{tmpdir}/noarch", exist_ok=True)
    os.makedirs(f"{tmpdir}/{subdir}", exist_ok=True)
    subprocess.run(
        f"curl -L {url} > {tmpdir}/{subdir}/{pkg}",
        shell=True,
        check=True,
    )

    local_md5 = compute_md5(f"{tmpdir}/{subdir}/{pkg}")
    if not hmac.compare_digest(local_md5, md5_val):
        print("md5 chechsum is incorrect! exiting!")
        sys.exit(1)

    subprocess.run(
        f"conda index --no-progress {tmpdir}",
        shell=True,
        check=True,
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


@tenacity.retry(
    wait=tenacity.wait_random_exponential(multiplier=1, max=10),
    stop=tenacity.stop_after_attempt(5),
    reraise=True,
)
def shard_exists(shard_pth):
    r = requests.get(
        "https://api.github.com/repos/conda-forge/"
        "repodata-shards/contents/%s" % shard_pth,
        headers={"Authorization": "token %s" % os.environ["GITHUB_TOKEN"]},
    )
    if r.status_code == 200:
        return True
    elif r.status_code == 404:
        return False
    else:
        r.raise_for_status()


@tenacity.retry(
    wait=tenacity.wait_random_exponential(multiplier=1, max=10),
    stop=tenacity.stop_after_attempt(5),
    reraise=True,
)
def get_or_make_release(repo, subdir, pkg):
    tag = f"{subdir}/{pkg}"
    try:
        rel = repo.get_release(tag)
    except github.UnknownObjectException:
        repo_sha = make_or_get_commit(subdir, pkg, make=True)

        rel = repo.create_git_tag_and_release(
            tag,
            "",
            tag,
            "",
            repo_sha,
            "commit",
        )

    return rel


@tenacity.retry(
    wait=tenacity.wait_random_exponential(multiplier=1, max=10),
    stop=tenacity.stop_after_attempt(5),
    reraise=True,
)
def upload_asset(rel, pth, content_type):
    name = os.path.basename(pth)
    ast = None
    for _ast in rel.get_assets():
        if _ast.name == name:
            ast = _ast
            break

    print("found asset %s for %s" % (ast, name), flush=True)

    if ast is None:
        ast = rel.upload_asset(pth, content_type=content_type)

    return ast


@tenacity.retry(
    wait=tenacity.wait_random_exponential(multiplier=1, max=10),
    stop=tenacity.stop_after_attempt(5),
    reraise=True,
)
def push_shard(shard, shard_pth, subdir, pkg):
    if not shard_exists(shard_pth):
        edata = base64.standard_b64encode(
            json.dumps(shard, sort_keys=True, indent=2).encode("utf-8")
        ).decode("ascii")

        data = {
            "message": (
                "[ci skip] [skip ci] [cf admin skip] ***NO_CI*** added "
                "%s/%s" % (subdir, pkg)
            ),
            "content": edata,
            "branch": "master",
        }

        r = requests.put(
            "https://api.github.com/repos/conda-forge/"
            "repodata-shards/contents/%s" % shard_pth,
            headers={"Authorization": "token %s" % os.environ["GITHUB_TOKEN"]},
            json=data
        )

        if r.status_code != 201:
            r.raise_for_status()


@tenacity.retry(
    wait=tenacity.wait_random_exponential(multiplier=1, max=10),
    stop=tenacity.stop_after_attempt(5),
    reraise=True,
)
def make_or_get_commit(subdir, pkg, make=False):
    if make:
        subprocess.run(
            "git pull",
            shell=True,
            check=True,
        )
        subprocess.run(
            "git commit --allow-empty -m "
            "'%s/%s [ci skip] [cf admin skip] ***NO_CI***'" % (subdir, pkg),
            shell=True,
            check=True,
        )

    repo_sha = subprocess.run(
        "git rev-parse --verify HEAD",
        shell=True,
        capture_output=True,
    ).stdout.decode("utf-8").strip()

    if make:
        for i in range(10):
            try:
                subprocess.run(
                    "git pull",
                    shell=True,
                    check=True,
                )
                subprocess.run(
                    "git push",
                    shell=True,
                    check=True,
                )
            except Exception:
                if i == 9:
                    raise
            else:
                break

    return repo_sha


if __name__ == "__main__":
    # configure git

    subprocess.run(
        "git config --global user.email 'conda.forge.daemon@gmail.com'",
        shell=True,
        check=True,
    )
    subprocess.run(
        "git config --global user.name 'conda-forge-daemon'",
        shell=True,
        check=True,
    )
    subprocess.run(
        "git config --global pull.rebase true",
        shell=True,
        check=True,
    )

    # pull event data
    with open(os.environ["GITHUB_EVENT_PATH"], 'r') as fp:
        event_data = json.load(fp)
    event_name = os.environ['GITHUB_EVENT_NAME'].lower()
    assert event_data["action"] == "release"

    # package info
    subdir = event_data['client_payload']["subdir"]
    pkg = event_data['client_payload']["package"]
    url = event_data['client_payload']["url"]
    label = event_data['client_payload']["label"]
    feedstock = event_data['client_payload']["feedstock"]
    add_shard = event_data['client_payload'].get("add_shard", True)
    md5_val = event_data['client_payload']["md5"]
    print("subdir/package: %s/%s" % (subdir, pkg), flush=True)
    print("url:", url, flush=True)
    print("add shard:", add_shard, flush=True)

    # repo info
    gh = github.Github(os.environ["GITHUB_TOKEN"])
    repo = gh.get_repo("conda-forge/releases")

    # make release and upload if shard does not exist
    with tempfile.TemporaryDirectory() as tmpdir:
        shard = make_repodata_shard(subdir, pkg, label, feedstock, url, tmpdir, md5_val)

        rel = get_or_make_release(repo, subdir, pkg)

        ast = upload_asset(
            rel,
            f"{tmpdir}/{subdir}/{pkg}",
            content_type="application/x-bzip2",
        )

        shard["url"] = ast.browser_download_url
        with open(f"{tmpdir}/repodata_shard.json", "w") as fp:
            json.dump(shard, fp, sort_keys=True, indent=2)

        ast = upload_asset(
            rel,
            f"{tmpdir}/repodata_shard.json",
            content_type="application/json",
        )

    # push the repodata shard
    shard_pth = get_shard_path(subdir, pkg)
    if add_shard and not shard_exists(shard_pth):
        push_shard(shard, shard_pth, subdir, pkg)
