import os
import io
import bz2
import copy

import github
import tenacity
import rapidjson as json
import requests
from conda_build.conda_interface import VersionOrder
from conda._vendor.toolz.itertoolz import groupby

from .shards import get_shard_path

CHANNELDATA_VERSION = 1
REPODATA_VERSION = 1

GH = github.Github(os.environ["GITHUB_TOKEN"])
REPODATA = GH.get_repo("regro/repodata")

INIT_REPODATA = {
    'info': {},
    'packages': {},
    'packages.conda': {},
    'removed': [],
    'repodata_version': REPODATA_VERSION
}


@tenacity.retry(
    wait=tenacity.wait_random_exponential(multiplier=1, max=60),
    stop=tenacity.stop_after_attempt(10),
    reraise=True,
)
def get_latest_links():
    return json.load(io.StringIO(bz2.decompress(
        requests.get(
            "https://github.com/regro/repodata/releases/latest/download/links.json.bz2"
        ).content
    ).decode("utf-8")))


@tenacity.retry(
    wait=tenacity.wait_random_exponential(multiplier=1, max=60),
    stop=tenacity.stop_after_attempt(10),
    reraise=True,
)
def get_broken_packages(subdir):
    r = requests.get(
        "https://conda.anaconda.org/conda-forge/label/broken"
        f"/{subdir}/repodata.json.bz2"
    )
    rd_broken = json.load(
        io.StringIO(bz2.decompress(r.content).decode("utf-8")))
    return rd_broken


@tenacity.retry(
    wait=tenacity.wait_random_exponential(multiplier=1, max=60),
    stop=tenacity.stop_after_attempt(10),
    reraise=True,
)
def upload_repodata_asset(rel, pth, content_type):
    rel.upload_asset(pth, content_type=content_type)
    tag = rel.tag_name
    fn = os.path.basename(pth)
    return (
        fn,
        f"https://github.com/regro/repodata/releases/download/{tag}/{fn}",
    )


@tenacity.retry(
    wait=tenacity.wait_random_exponential(multiplier=1, max=60),
    stop=tenacity.stop_after_attempt(10),
    reraise=True,
)
def _delete_repodata_release(rel):
    for ast in rel.get_assets():
        ast.delete_asset()
    if not rel.draft:
        tag = REPODATA.get_git_ref(f"tags/{rel.tag_name}")
    else:
        tag = None
    rel.delete_release()
    if tag is not None:
        tag.delete()


@tenacity.retry(
    wait=tenacity.wait_random_exponential(multiplier=1, max=60),
    stop=tenacity.stop_after_attempt(10),
    reraise=True,
)
def delete_old_repodata_releases(all_links):
    releases_to_delete = []
    for rel in REPODATA.get_releases():
        tag = rel.tag_name
        has_tag = any(
            any(tag in url for url in urls)
            for _, urls in all_links["serverdata"].items()
        )
        if not has_tag:
            releases_to_delete.append(rel)

    deleted_tags = []
    for rel in releases_to_delete:
        deleted_tags.append(rel.tag_name)
        _delete_repodata_release(rel)

    return deleted_tags


def _load_shard_channeldata(subdir, fn, repodata):
    pth = os.path.join("repodata-shards", get_shard_path(subdir, fn))
    if not os.path.exists(pth):
        assert False, repodata["packages"][fn]
    with open(pth, "r") as fp:
        shard = json.load(fp)
    return shard["channeldata"], shard["channeldata_version"]


def _make_seconds(timestamp):
    timestamp = int(timestamp)
    if timestamp > 253402300799:  # 9999-12-31
        timestamp //= 1000
        # convert milliseconds to seconds; see conda/conda-build#1988
    return timestamp


def build_or_update_channeldata(channel_data, repodata, subdir):
    legacy_packages = repodata["packages"]
    conda_packages = repodata["packages.conda"]

    use_these_legacy_keys = (
        set(legacy_packages.keys())
        - set(k[:-6] + '.tar.bz2' for k in conda_packages.keys())
    )
    all_repodata_packages = conda_packages.copy()
    all_repodata_packages.update({k: legacy_packages[k] for k in use_these_legacy_keys})
    package_data = channel_data.get('packages', {})

    for fn, x in all_repodata_packages.items():
        assert "subdir" not in x or x["subdir"] == subdir, x

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
        data, _cdver = _load_shard_channeldata(subdir, fn, repodata)
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


def build_or_update_links_and_repodata(
    repodata,
    links,
    subdir,
    shards,
    override_labels=None,
    removed=None,
    fetch_repodata=None,
):
    if subdir not in repodata:
        repodata[subdir] = {}

    override_labels = override_labels or {}
    removed = removed or []

    updated_data = set()

    for subdir_pkg, shard in shards.items():
        shard["labels"] = override_labels.get(subdir_pkg, shard["labels"])
        for label in shard["labels"]:
            if label not in repodata[subdir]:
                if fetch_repodata is not None:
                    repodata[subdir][label] = (
                        fetch_repodata(links, subdir, label)
                        or copy.deepcopy(INIT_REPODATA)
                    )
                else:
                    repodata[subdir][label] = copy.deepcopy(INIT_REPODATA)

                repodata[subdir][label]["info"]["subdir"] = subdir

            repodata[subdir][label]["packages"][shard["package"]] \
                = shard["repodata"]
            links["packages"][subdir_pkg] = shard["url"]
            updated_data.add((subdir, label))

    if (
        "main" in repodata[subdir]
        and sorted(repodata[subdir]["main"]["removed"]) != sorted(removed)
    ):
        updated_data.add((subdir, "main"))
        repodata[subdir]["main"]["removed"] = removed
        for fn in removed:
            if fn in repodata[subdir]["main"]["packages"]:
                repodata[subdir]["main"]["packages"].pop(fn, None)

    return updated_data
