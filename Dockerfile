FROM condaforge/miniforge3:24.11.3-2

WORKDIR /app
COPY environment.yaml environment.yaml
SHELL ["/bin/bash", "-c"]
RUN conda env create -f environment.yaml -n grz-watchdog
COPY workflow/ workflow/
SHELL ["conda", "run", "-n", "grz-watchdog", "/bin/bash", "-c"]
COPY pyproject.toml pyproject.toml
COPY uv.lock uv.lock
COPY src/ src/
RUN uv sync --locked
RUN uv pip install .
RUN mkdir /app/workdir
COPY config workdir/config
ENTRYPOINT ["conda", "run", "--no-capture-output", "-n", "grz-watchdog", "snakemake", "--cores", "2", "--sdm", "conda", "--verbose", "--directory", "/app/workdir"]