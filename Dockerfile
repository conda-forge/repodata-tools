FROM condaforge/miniforge3

# bust the docker cache so that we always rerun the installs below
ADD http://www.randomtext.me/api/gibberish /opt/docker/etc/gibberish

COPY . /opt/app
RUN cd /opt/app && \
    conda install -q -y --file requirements.txt && \
    pip install -e . && \
    conda clean --all

EXPOSE 5000

CMD ["tini", "--", "/opt/app/run_app.sh"]
