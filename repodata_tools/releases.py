import os
import subprocess
import tempfile

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


@tenacity.retry(
    wait=tenacity.wait_random_exponential(multiplier=1, max=60),
    stop=tenacity.stop_after_attempt(10),
    reraise=True,
)
def get_or_make_release(repo, subdir, pkg, repo_pth=None):
    tag = f"{subdir}/{pkg}"
    try:
        rel = repo.get_release(tag)
    except github.UnknownObjectException:
        repo_sha = make_or_get_commit(subdir, pkg, make=True, repo_pth=repo_pth)

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
def make_or_get_commit(subdir, pkg, make=False, repo_pth=None):
    if repo_pth is None:
        repo_pth = "."
    if make:
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

    if make:
        for i in range(10):
            try:
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
            except Exception:
                if i == 9:
                    raise
            else:
                break

    return repo_sha


@click.command()
def main():
    """Make a GitHub release of a package and upload the repodata shard.

    This command is meant to be run inside of GitHub actions, triggered on
    repo dispatch events.
    """
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
        "git config --global pull.rebase false",
        shell=True,
        check=True,
    )

    # pull event data
    with open(os.environ["GITHUB_EVENT_PATH"], 'r') as fp:
        event_data = json.load(fp)
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
    repo = gh.get_repo("regro/releases")

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
