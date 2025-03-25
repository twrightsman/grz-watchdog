FROM condaforge/miniforge3:24.11.3-2

WORKDIR /app
COPY environment.yaml ./environment.yaml
SHELL ["/bin/bash", "-c"]
RUN conda env create -f environment.yaml -n grz-watchdog
COPY config ./config
COPY workflow ./workflow
SHELL ["conda", "run", "-n", "grz-watchdog", "/bin/bash", "-c"]
ENTRYPOINT ["conda", "run", "-n", "grz-watchdog", "snakemake", "--cores", "2", "--sdm", "conda", "--verbose"]