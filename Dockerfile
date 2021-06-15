FROM condaforge/miniforge3

# bust the docker cache so that we always rerun the installs below
ADD http://worldtimeapi.org/api/timezone/Europe/London.txt /opt/docker/etc/gibberish

COPY . /opt/app
RUN cd /opt/app && \
    chmod a+x /opt/app/run_app.sh && \
    conda install mamba --yes && \
    mamba env create --file environment.yml && \
    /bin/bash -c "source activate test && pip install -e . " && \
    conda clean -tipsy && \
    find /opt/conda -follow -type f -name '*.a' -delete && \
    find /opt/conda -follow -type f -name '*.pyc' -delete && \
    conda clean -afy

CMD /opt/app/run_app.sh
