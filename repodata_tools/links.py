import tempfile
import subprocess

import tenacity
import rapidjson as json

from .index import REPODATA_REPO


@tenacity.retry(
    wait=tenacity.wait_random_exponential(multiplier=1, max=10),
    stop=tenacity.stop_after_attempt(5),
    reraise=True,
)
def get_latest_links():
    with tempfile.TemporaryDirectory() as tmpdir:
        subprocess.run(
            f"cd {tmpdir} && wget --quiet https://github.com/{REPODATA_REPO}/releases"
            "/latest/download/links.json.bz2 && bunzip2 -q links.json.bz2",
            shell=True,
            check=True,
        )
        with open(f"{tmpdir}/links.json") as fp:
            return json.loads(fp.read())
