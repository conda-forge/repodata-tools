import sys
import os
import glob
import rapidjson as json
import subprocess

from conda_build.index import _make_subdir_index_html, _make_channeldata_index_html


if __name__ == "__main__":
    dest_pth = sys.argv[1]
    fnames = glob.glob("repodata_*.json")
    fnames = [f for f in fnames if f != "repodata_info.json"]
    labels = set([f.split("_", maxsplit=2)[2][:-5] for f in fnames])
    subdirs = [
        "linux-64", "osx-64", "win-64", "linux-aarch64", "linux-ppc64le", "noarch"
    ]

    for label in labels:
        _dest_pth = os.path.join(dest_pth, "label", label)
        if label == "main":
            channel_name = "conda-forge"
        else:
            channel_name = f"conda-forge/label/{label}"

        os.makedirs(_dest_pth, exist_ok=True)

        for subdir in subdirs:
            _subdir_dest_pth = os.path.join(_dest_pth, subdir)
            os.makedirs(_subdir_dest_pth, exist_ok=True)
            with open(f"repodata_{subdir}_{label}.json", "r") as fp:
                rd_pkg = json.load(fp)["packages"]
            html = _make_subdir_index_html(channel_name, subdir, rd_pkg, {})
            with open(os.path.join(_subdir_dest_pth, "index.html"), "w") as fp:
                fp.write(html)

        with open(f"channeldata_{label}.json", "r") as fp:
            cd = json.load(fp)
        html = _make_channeldata_index_html(channel_name, cd)
        with open(os.path.join(_dest_pth, "index.html"), "w") as fp:
            fp.write(html)

    if "main" in labels:
        subprocess.run(
            f"cp -r {dest_pth}/label/main/* .",
            shell=True,
        )
