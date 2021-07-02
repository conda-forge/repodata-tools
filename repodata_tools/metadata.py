# this is the list of all subdirs for conda-forge that we mirror
# do not reorder this list
# - we use a mod operator to distribute them over tasks
# - they are ordered so that mod by 4 puts the biggest subdirs on separate tasks
import hashlib
import json
import os

HERE = os.path.dirname(os.path.abspath(__file__))

with open(os.path.join(HERE, "metadata.json"), "r") as f:
    metadata = json.load(f)

CONDA_FORGE_SUBIDRS = CONDA_FORGE_SUBDIRS = metadata["subdirs"]

# these packages cannot be indexed
UNINDEXABLE = metadata["unindexable"]

UNDISTRIBUTABLE = metadata["undistributable"]

UNDISTRIBUTABLE_HASH = hashlib.sha256(
    "".join(sorted(UNDISTRIBUTABLE)).encode("utf-8")
).hexdigest()[:6]
