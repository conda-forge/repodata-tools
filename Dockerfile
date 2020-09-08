FROM condaforge/miniforge3

# bust the docker cache so that we always rerun the installs below
ADD http://www.randomtext.me/api/gibberish /opt/docker/etc/gibberish

COPY . /opt/app
RUN cd /opt/app && \
    conda install -q -y --file requirements.txt && \
    pip install -e . && \
    chmod -R 777 /opt/conda && \
    conda clean -tipsy && \
    find /opt/conda -follow -type f -name '*.a' -delete && \
    find /opt/conda -follow -type f -name '*.pyc' -delete && \
    conda clean -afy

EXPOSE 5000

CMD ["tini", "-s", "--", "/opt/app/run_app.sh"]
