from setuptools import setup, find_packages

setup(
    name='repodata_tools',
    version='0.1.0',
    packages=find_packages(),
    include_package_data=True,
    entry_points="""
        [console_scripts]
        sync-anaconda-data=repodata_tools.anaconda_sync:main
        make-github-release=repodata_tools.releases:main
        run-repodata-worker=repodata_tools.repoworker:main
        remove-undistributable=repodata_tools.remove_undistrib:main
    """,
)
