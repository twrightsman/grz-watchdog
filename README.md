A prototype of a snakemake workflow for the grz-ingest process.
Make use of continuously updated input files.

## Idea
1. Spawn a thread that checks s3 buckets defined in `config/config.yaml` regularly for (new) `metadata.json` files.
(We could employ bucket notifications instead, but that would probably be less portable and more complex to set up.)
2. Push new submissions to the "input" queue (and keep track of their "state" in `state/metadata.jsonl`)
3. Process the submissions in the "input" queue in a snakemake workflow; depending on whether we have to do QC or have research consent, different paths are taken.

## Setup


### Manual
1. Setup conda environment
    ```sh
    conda env create -n grz-watchdog -f environment.yaml
    conda activate grz-watchdog
    ```
2. Install helper package locally:
   ```sh
   uv sync --locked
   uv pip install -e .
   ```

3. Adjust `config/config.yaml` to your needs.

4. (optional) Make a copy of `workflow/profiles/default` and adjust to your needs (e.g. by adding `executor: slurm`) and use `--workflow-profile` to point to that modified copy.

5. Invoke `snakemake`


### Docker compose
Comes with minio and minio-client (mc), sets up testing S3 storage, see `docker-compose.yaml` for more info.
Test config is in `tests/config`. Working directory is in `tests/workflow-workdir`.
```sh
docker compose up --build
```

## TODO
 - [ ] Find out how to gracefully stop the service to avoid snakemake lock issues.
 - [ ] actual implementations of the commands
 - [ ] initial ad-hoc testing
 - [ ] generate `local_case_id` for each submission, i.e. do not use the object key (as that contains the `tanG` atm) for storage in `grz_internal` / `ghga`.
 - [ ] test setup
 - [ ] proper error handling (check for WorkflowError and KeyboardInterruption etc and handle gracefully)
 - [ ] better consistency between branching within a rule (based on params) or between rules (based on requested inputs)
 - [ ] threads + resource definitions for each rule
 - [ ] explain how to configure profiles to optionally make use of [slurm executor](https://snakemake.github.io/snakemake-plugin-catalog/plugins/executor/slurm.html) or [kubernetes executor](https://snakemake.github.io/snakemake-plugin-catalog/plugins/executor/kubernetes.html)
 - [ ] use [s3 storage plugin](https://snakemake.github.io/snakemake-plugin-catalog/plugins/storage/s3.html) where applicable
