import io
import bz2

import tenacity
import rapidjson as json
import requests


@tenacity.retry(
    wait=tenacity.wait_random_exponential(multiplier=1, max=10),
    stop=tenacity.stop_after_attempt(5),
    reraise=True,
)
def get_latest_links():
    return json.load(io.StringIO(bz2.decompress(
        requests.get(
            "https://github.com/regro/repodata/releases/latest/download/links.json.bz2"
        ).content
    ).decode("utf-8")))
