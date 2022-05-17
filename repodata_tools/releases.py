import os
import subprocess
import tempfile
import sys

import click
import rapidjson as json
import github
import tenacity

from .shards import (
    make_repodata_shard,
    get_shard_path,
    shard_exists,
    push_shard,
)
from .metadata import UNDISTRIBUTABLE
from .utils import split_pkg, print_github_api_limits


@tenacity.retry(
    wait=tenacity.wait_random_exponential(multiplier=1, max=10),
    stop=tenacity.stop_after_attempt(5),
    reraise=True,
)
def get_or_make_release(repo, subdir, pkg, repo_pth=None, make_commit=True):
    tag = f"{subdir}/{pkg}"
    try:
        rel = repo.get_release(tag)
    except github.UnknownObjectException:
        repo_sha = make_or_get_commit(
            subdir,
            pkg,
            make_commit=make_commit,
            repo_pth=repo_pth,
        )

        rel = repo.create_git_tag_and_release(
            tag,
            "",
            tag,
            "",
            repo_sha,
            "commit",
        )

    curr_asts = [ast for ast in rel.get_assets()]

    return rel, curr_asts


@tenacity.retry(
    wait=tenacity.wait_random_exponential(multiplier=1, max=10),
    stop=tenacity.stop_after_attempt(5),
    reraise=True,
)
def upload_asset(rel, curr_asts, pth, content_type):
    name = os.path.basename(pth)
    ast = None
    for _ast in curr_asts:
        if _ast.name == name:
            ast = _ast
            break

    print("found asset %s for %s" % (ast, name), flush=True)

    if ast is None:
        ast = rel.upload_asset(pth, content_type=content_type)
        curr_asts.append(ast)

    return ast


@tenacity.retry(
    wait=tenacity.wait_random_exponential(multiplier=1, max=10),
    stop=tenacity.stop_after_attempt(5),
    reraise=True,
)
def make_or_get_commit(subdir, pkg, make_commit=False, repo_pth=None):
    if repo_pth is None:
        repo_pth = "."
    if make_commit:
        subprocess.run(
            f"cd {repo_pth} && git pull --no-edit",
            shell=True,
            check=True,
        )
        subprocess.run(
            f"cd {repo_pth} && git commit --allow-empty -m "
            f"'{subdir}/{pkg} [ci skip] [cf admin skip] ***NO_CI***'",
            shell=True,
            check=True,
        )

    repo_sha = subprocess.run(
        f"cd {repo_pth} && git rev-parse --verify HEAD",
        shell=True,
        capture_output=True,
    ).stdout.decode("utf-8").strip()

    if make_commit:
        subprocess.run(
            f"cd {repo_pth} && git pull --no-edit",
            shell=True,
            check=True,
        )
        subprocess.run(
            f"cd {repo_pth} && git push",
            shell=True,
            check=True,
        )

    return repo_sha


@click.command()
def main():
    """Make a GitHub release of a package and upload the repodata shard.

    This command is meant to be run inside of GitHub actions, triggered on
    repo dispatch events.
    """
    # pull event data
    with open(os.environ["GITHUB_EVENT_PATH"], 'r') as fp:
        event_data = json.load(fp)
    assert event_data["action"] in ["release", "validate"]

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

    shard_pth = get_shard_path(subdir, pkg)
    shard_pth_exists = shard_exists(shard_pth)
    print("shard exists:", shard_pth_exists, flush=True)
    print("shard path:", shard_pth, flush=True)
    if shard_pth_exists:
        print("shard already exists! not uploading new package!", flush=True)
        sys.exit(0)

    # we are not making github releases anymore
    # if split_pkg(os.path.join(subdir, pkg))[1] in UNDISTRIBUTABLE:
    #     upload_pkg = False
    # else:
    #     upload_pkg = True
    upload_pkg = False

    # repo info
    gh = github.Github(os.environ["GITHUB_TOKEN"])
    print_github_api_limits(gh)

    # make release and upload if shard does not exist
    with tempfile.TemporaryDirectory() as tmpdir:
        shard = make_repodata_shard(
            subdir,
            pkg,
            label,
            feedstock,
            url,
            tmpdir,
            md5_checksum=md5_val,
        )

        if upload_pkg:
            repo = gh.get_repo("conda-forge/releases")
            rel, curr_asts = get_or_make_release(
                repo,
                subdir,
                pkg,
                make_commit=False,
            )

            ast = upload_asset(
                rel,
                curr_asts,
                f"{tmpdir}/{subdir}/{pkg}",
                content_type="application/x-bzip2",
            )
            shard["url"] = ast.browser_download_url

            # we don't upload shards to releases anymore
            # with open(f"{tmpdir}/repodata_shard.json", "w") as fp:
            #     json.dump(shard, fp, sort_keys=True, indent=2)
            # upload_asset(
            #     rel,
            #     curr_asts,
            #     f"{tmpdir}/repodata_shard.json",
            #     content_type="application/json",
            # )

    # push the repodata shard
    if add_shard and not shard_pth_exists:
        push_shard(shard, shard_pth, subdir, pkg)
