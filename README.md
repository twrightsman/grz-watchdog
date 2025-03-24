A prototype of a snakemake workflow for the grz-ingest process.
Make use of continuously updated input files.

Idea:
1. Spawn a thread that checks s3 buckets defined in `config/config.yaml` regularly for (new) `metadata.json` files.
(We could employ bucket notifications instead, but that would probably be less portable and more complex to set up.)
2. Push new submissions to the "input" queue (and keep track of their "state" in `state/metadata.jsonl`)
3. Process the submissions in the "input" queue in a snakemake workflow; depending on whether we have to do QC or have research consent, different paths are taken.

TODO:
 - [ ] actual implementations of the commands
 - [ ] initial ad-hoc testing
 - [ ] generate `local_case_id` for each submission, i.e. do not use the object key as that contains the `tanG` atm.
 - [ ] test setup
 - [ ] proper error handling (check for WorkflowError and KeyboardInterruption etc and handle gracefully)
 - [ ] better consistency between branching within a rule (based on params) or between rules (based on requested inputs)
 - [ ] threads + resource definitions for each rule
