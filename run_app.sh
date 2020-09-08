#!/usr/bin/env bash

echo " "
echo "==================================================================================================="
echo "==================================================================================================="

conda info

echo " "
echo "==================================================================================================="
echo "==================================================================================================="

whoami

ls -lah /opt/conda/lib/python3.7/site-packages/.wh.conda-4.8.2-py3.7.egg-info
chmod -R 777 /opt/conda
ls -lah /opt/conda/lib/python3.7/site-packages/.wh.conda-4.8.2-py3.7.egg-info

run-repodata-worker &

uvicorn --host=0.0.0.0 --port=${PORT:-5000} repodata_tools.app:app
