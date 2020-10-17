# this is the list of all subdirs for conda-forge that we mirror
# do not reorder this list
# - we use a mod operator to distribute them over tasks
# - they are ordered so that mod by 4 puts the biggest subdirs on separate tasks

CONDA_FORGE_SUBIDRS = [
    "linux-64", "osx-64", "win-64", "noarch",
    "linux-aarch64", "linux-ppc64le", "osx-arm64"
]


# these packages cannot be indexed
UNINDEXABLE = [
    "linux-64/pyside2-2.0.0~alpha0-py27_0.tar.bz2",
    "linux-64/pyside2-2.0.0~alpha0-py35_0.tar.bz2",
    "linux-64/pyside2-2.0.0~alpha0-py36_0.tar.bz2",
    "osx-64/pyside2-2.0.0~alpha0-py27_0.tar.bz2",
    "osx-64/pyside2-2.0.0~alpha0-py35_0.tar.bz2",
    "osx-64/pyside2-2.0.0~alpha0-py36_0.tar.bz2",
]

UNDISTRIBUTABLE = [
    "cudatoolkit",
]
