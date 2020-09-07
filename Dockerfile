FROM condaforge/miniforge3

COPY . .
RUN conda install -q -y --file requirements.txt && \
    pip install -e . && \
    conda clean --all

EXPOSE 5000

CMD ["tini", "--", "run_app.sh"]
