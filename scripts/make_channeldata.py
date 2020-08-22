import glob
import os
import sys
import rapidjson as json
import subprocess
import hashlib

from conda_build.conda_interface import VersionOrder
from conda._vendor.toolz.itertoolz import groupby

CHANNELDATA_VERSION = 1


def get_shard_path(subdir, pkg, n_dirs=3):
    hex = hashlib.sha1(pkg.encode("utf-8")).hexdigest()[0:n_dirs]

    pth_parts = (
        ["shards", subdir]
        + [hex[i] for i in range(n_dirs)]
        + [pkg + ".json"]
    )

    return os.path.join(*pth_parts)


def _load_shard_channeldata(shards_repo, subdir, fn):
    pth = os.path.join(shards_repo, get_shard_path(subdir, fn))
    with open(pth, "r") as fp:
        shard = json.load(fp)
    return shard["channeldata"], shard["channeldata_version"]


def _make_seconds(timestamp):
    timestamp = int(timestamp)
    if timestamp > 253402300799:  # 9999-12-31
        timestamp //= 1000
        # convert milliseconds to seconds; see conda/conda-build#1988
    return timestamp


def update_channeldata_for_subdir(channel_data, repodata, subdir, shards_repo):
    legacy_packages = repodata["packages"]
    conda_packages = repodata["packages.conda"]

    use_these_legacy_keys = (
        set(legacy_packages.keys())
        - set(k[:-6] + '.tar.bz2' for k in conda_packages.keys())
    )
    all_repodata_packages = conda_packages.copy()
    all_repodata_packages.update({k: legacy_packages[k] for k in use_these_legacy_keys})
    package_data = channel_data.get('packages', {})

    def _append_group(groups, candidate):
        pkg_dict = candidate[1]
        pkg_name = pkg_dict['name']

        run_exports = package_data.get(pkg_name, {}).get('run_exports', {})
        if (
            pkg_name not in package_data
            or subdir not in package_data.get(pkg_name, {}).get('subdirs', [])
            or (
                package_data.get(pkg_name, {}).get('timestamp', 0)
                < _make_seconds(pkg_dict.get('timestamp', 0))
            )
            or run_exports and pkg_dict['version'] not in run_exports
        ):
            groups.append(candidate)

    groups = []
    package_groups = groupby(lambda x: x[1]['name'], all_repodata_packages.items())
    for groupname, group in package_groups.items():
        if (
            groupname not in package_data
            or package_data[groupname].get('run_exports')
        ):
            # pay special attention to groups that have run_exports
            #   - we need to process each version
            # group by version; take newest per version group.  We handle groups that
            #    are not in the index t all yet similarly, because we can't check
            #    if they have any run_exports
            for vgroup in groupby(lambda x: x[1]['version'], group).values():
                candidate = next(iter(sorted(
                    vgroup, key=lambda x: x[1].get('timestamp', 0), reverse=True))
                )
                _append_group(groups, candidate)
        else:
            # take newest per group
            candidate = next(iter(sorted(
                group, key=lambda x: x[1].get('timestamp', 0), reverse=True))
            )
            _append_group(groups, candidate)

    def _replace_if_newer_and_present(pd, data, erec, data_newer, k):
        if data.get(k) and (data_newer or not erec.get(k)):
            pd[k] = data[k]
        else:
            pd[k] = erec.get(k)

    # unzipping
    fns, fn_dicts = [], []
    if groups:
        fns, fn_dicts = zip(*groups)

    for fn_dict, fn in zip(fn_dicts, fns):
        data, _cdver = _load_shard_channeldata(shards_repo, subdir, fn)
        assert _cdver == CHANNELDATA_VERSION
        if data:
            data.update(fn_dict)
            name = data['name']
            # existing record
            erec = package_data.get(name, {})
            data_v = data.get('version', '0')
            erec_v = erec.get('version', '0')
            data_newer = VersionOrder(data_v) > VersionOrder(erec_v)

            package_data[name] = package_data.get(name, {})
            # keep newer value for these
            for k in (
                'description', 'dev_url', 'doc_url', 'doc_source_url', 'home',
                'license',
                'source_url', 'source_git_url', 'summary', 'icon_url', 'icon_hash',
                'tags',
                'identifiers', 'keywords', 'recipe_origin', 'version'
            ):
                _replace_if_newer_and_present(
                    package_data[name], data, erec, data_newer, k
                )

            # keep any true value for these, since we don't distinguish subdirs
            for k in (
                "binary_prefix", "text_prefix", "activate.d", "deactivate.d",
                "pre_link", "post_link", "pre_unlink"
            ):
                package_data[name][k] = any((data.get(k), erec.get(k)))

            package_data[name]['subdirs'] = sorted(list(set(
                erec.get('subdirs', []) + [subdir]
            )))
            # keep one run_exports entry per version of the package, since these
            # vary by version
            run_exports = erec.get('run_exports', {})
            exports_from_this_version = data.get('run_exports')
            if exports_from_this_version:
                run_exports[data_v] = data.get('run_exports')
            package_data[name]['run_exports'] = run_exports
            package_data[name]['timestamp'] = _make_seconds(max(
                data.get('timestamp', 0),
                channel_data.get(name, {}).get('timestamp', 0)
            ))

    channel_data.update({
        'channeldata_version': CHANNELDATA_VERSION,
        'subdirs': sorted(list(set(channel_data.get('subdirs', []) + [subdir]))),
        'packages': package_data,
    })


if __name__ == "__main__":
    tm = sys.argv[1]
    shards_repo = sys.argv[2]

    fnames = glob.glob("repodata_*.json")
    fnames = [f for f in fnames if f != "repodata_info.json"]
    labels = set([f.split("_", maxsplit=2)[2][:-5] for f in fnames])
    for label in labels:
        channel_data = {}
        subdirs = [
            "linux-64", "osx-64", "win-64", "linux-aarch64", "linux-ppc64le", "noarch"
        ]
        for subdir in subdirs:
            with open(f"repodata_{subdir}_{label}.json", "r") as fp:
                repodata = json.load(fp)
            update_channeldata_for_subdir(channel_data, repodata, subdir, shards_repo)

        with open(f"channeldata_{label}.json", "w") as fp:
            json.dump(channel_data, fp, indent=2, sort_keys=True)
        subprocess.run(
            f"bzip2 --keep channeldata_{label}.json",
            shell=True,
        )
