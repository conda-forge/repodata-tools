import os
import json
import tempfile
import subprocess
import copy
import base64
import hashlib

import github
import requests
import tenacity


def get_shard_path(subdir, pkg, n_dirs=3):
    hex = hashlib.sha1(pkg.encode("utf-8")).hexdigest()[0:n_dirs]

    pth_parts = (
        ["shards", subdir]
        + [hex[i] for i in range(n_dirs)]
        + [pkg + ".json"]
    )

    return os.path.join(*pth_parts)


@tenacity.retry(
    wait=tenacity.wait_random_exponential(multiplier=1, max=60),
    stop=tenacity.stop_after_attempt(10),
    reraise=True,
)
def make_repodata_shard(subdir, pkg, label, feedstock, url, tmpdir):
    os.makedirs(f"{tmpdir}/noarch", exist_ok=True)
    os.makedirs(f"{tmpdir}/{subdir}", exist_ok=True)
    subprocess.run(
        f"curl -L {url} > {tmpdir}/{subdir}/{pkg}",
        shell=True,
        check=True,
    )
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
    shard["channeldata"]["subdirs"] = [subdir]
    shard["channeldata"]["version"] = rd["packages"][pkg]["version"]

    return shard


@tenacity.retry(
    wait=tenacity.wait_random_exponential(multiplier=1, max=60),
    stop=tenacity.stop_after_attempt(10),
    reraise=True,
)
def shard_exists(shard_pth):
    r = requests.get(
        "https://api.github.com/repos/regro/"
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
    wait=tenacity.wait_random_exponential(multiplier=1, max=60),
    stop=tenacity.stop_after_attempt(10),
    reraise=True,
)
def get_or_make_release(repo, tag, repo_sha):
    try:
        rel = repo.get_release(tag)
    except github.UnknownObjectException:
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
    wait=tenacity.wait_random_exponential(multiplier=1, max=60),
    stop=tenacity.stop_after_attempt(10),
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
    wait=tenacity.wait_random_exponential(multiplier=1, max=60),
    stop=tenacity.stop_after_attempt(10),
    reraise=True,
)
def push_shard(shard, shard_pth, subdir, pkg):
    if not shard_exists(shard_pth):
        edata = base64.standard_b64encode(
            json.dumps(shard).encode("utf-8")).decode("ascii")

        data = {
            "message": (
                "[ci skip] [skip ci] [cf admin skip] ***NO_CI*** added "
                "repodata shard for %s/%s" % (subdir, pkg)),
            "content": edata,
            "branch": "master",
        }

        r = requests.put(
            "https://api.github.com/repos/regro/"
            "repodata-shards/contents/%s" % shard_pth,
            headers={"Authorization": "token %s" % os.environ["GITHUB_TOKEN"]},
            json=data
        )

        if r.status_code != 201:
            r.raise_for_status()


if __name__ == "__main__":
    # pull event data
    with open(os.environ["GITHUB_EVENT_PATH"], 'r') as fp:
        event_data = json.load(fp)
    event_name = os.environ['GITHUB_EVENT_NAME'].lower()
    assert event_data["action"] == "release"

    # repo info
    gh = github.Github(os.environ["GITHUB_TOKEN"])
    repo = gh.get_repo("regro/releases")
    repo_sha = subprocess.run(
        "git rev-parse --verify HEAD",
        shell=True,
        capture_output=True,
    ).stdout.decode("utf-8").strip()

    # package info
    subdir = event_data['client_payload']["subdir"]
    pkg = event_data['client_payload']["package"]
    url = event_data['client_payload']["url"]
    label = event_data['client_payload']["label"]
    feedstock = event_data['client_payload']["feedstock"]
    print("subdir/package: %s/%s" % (subdir, pkg), flush=True)
    print("url:", url, flush=True)

    shard_pth = get_shard_path(subdir, pkg)

    # test if shard exists - if so, dump out
    if shard_exists(shard_pth):
        print("*** release already exists - not uploading again! ***", flush=True)

    # make release and upload if shard does not exist
    with tempfile.TemporaryDirectory() as tmpdir:
        shard = make_repodata_shard(subdir, pkg, label, feedstock, url, tmpdir)
        rel = get_or_make_release(repo, f"{subdir}/{pkg}", repo_sha)

        ast = upload_asset(
            rel,
            f"{tmpdir}/{subdir}/{pkg}",
            content_type="application/x-bzip2",
        )

        shard["url"] = ast.browser_download_url
        with open(f"{tmpdir}/repodata_shard.json", "w") as fp:
            json.dump(shard, fp)

        ast = upload_asset(
            rel,
            f"{tmpdir}/repodata_shard.json",
            content_type="application/json",
        )

    # push the repodata shard
    push_shard(shard, shard_pth, subdir, pkg)
