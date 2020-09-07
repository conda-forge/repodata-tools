FROM condaforge/miniforge3

COPY . /opt/app
RUN cd /opt/app && \
    conda install -q -y --file requirements.txt && \
    pip install -e . && \
    conda clean --all

EXPOSE 5000

CMD ["tini", "--", "/opt/app/run_app.sh"]
