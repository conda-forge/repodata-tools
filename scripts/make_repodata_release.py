import os
import subprocess
import glob
import json
import sys

from github import Github

gh = Github(os.environ["GITHUB_TOKEN"])

repo = gh.get_repo(os.environ["GITHUB_REPOSITORY"])

sha = subprocess.run(
    "git rev-parse --verify HEAD",
    shell=True,
    capture_output=True,
).stdout.decode("utf-8").strip()

rd_sha = subprocess.run(
    "cd %s && git rev-parse --verify HEAD" % sys.argv[2],
    shell=True,
    capture_output=True,
).stdout.decode("utf-8").strip()


tm = sys.argv[1]

r = repo.create_git_tag_and_release(
    tm,
    "",
    tm,
    "repodata release for repodata-shards at SHA %s" % rd_sha,
    sha,
    "commit",
)

info = {"shards_sha": rd_sha}
with open("repodata_info.json", "w") as fp:
    json.dump(info, fp)


fnames = glob.glob("repodata_*") + glob.glob("links_*")
for fname in fnames:
    if fname.endswith("bz2"):
        content_type = "application/x-bzip2"
    else:
        content_type = "application/json"
    r.upload_asset(fname, content_type=content_type)
