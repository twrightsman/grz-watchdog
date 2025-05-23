# Design of grz-watchdog

`grz-watchdog` should act as an automated data steward for the GRZ.
Most importantly, all steps that `grz-watchdog` manages should also be executable manually by a data steward to aid in debugging and error recovery.

## Pipeline Overview

```mermaid
flowchart TD
    n1["Inbox"] --> n2["grz-cli download"]
    n2 -.-> n12
    n2 --> n3["grz-cli decrypt"]
    n3 -.-> n12
    n3 --> n4["grz-cli validate"]
    n4 -.-> n12["Submission Database + Logs"]
    n4 --> n15["Valid?"]
    n5["Extra QC?"] -- No --> n9["Consent?"]
    n5 -- "Yes" --> n7["QC Pipeline"]
    n7 --> n9
    n7 -.-> n12
    n6 -.-> n12
    n9 -- Yes --> n11["Encrypt"]
    n9 -- No/Partial --> n14["Encrypt"]
    n11 --> n8["Archive<br>(Fully Consented)"]
    n11 -.-> n12
    n12 --> n13["Tätigkeitsbericht<br>(Quarterly)"]
    n14 --> n10["Archive<br>(Other)"]
    n14 -.-> n12
    n15 -- Yes --> n5
    n15 -- No  --> n9
    n10 --> n6["Submit Prüfbericht"]
    n8 --> n6

    n1@{ shape: das}
    n2@{ shape: proc}
    n3@{ shape: proc}
    n4@{ shape: proc}
    n12@{ shape: db}
    n15@{ shape: decision}
    n5@{ shape: decision}
    n6@{ shape: proc}
    n7@{ shape: proc}
    n9@{ shape: decision}
    n11@{ shape: proc}
    n14@{ shape: proc}
    n8@{ shape: das}
    n13@{ shape: doc}
    n10@{ shape: das}
```


## Submission Lifecycle

In case of any errors or explicit termination requests, `grz-watchdog` should be able to gracefully resume from any combination of submission states.
The major exception is any submission in an Error state (see lifecycle below), which would require manual intervention.

```mermaid
flowchart TD
    n1["Uploading"] --> n2["Uploaded"]
    n2 --> n3["Downloading"]
    n3 --> n4["Downloaded"] & n5["Error"]
    n4 --> n7["Decrypting"]
    n7 --> n8["Decrypted"] & n5
    n8 --> n10["Validating"]
    n10 --> n11["Validated"] & n5
    n11 -- 2% of valid --> n13["QCing"]
    n13 --> n15["QCed"] & n5
    n15 --> n18["Encrypting"]
    n18 --> n19["Encrypted"] & n5
    n19 --> n20["Archiving"]
    n20 --> n21["Archived"] & n5
    n11 -- 98% of valid and all invalid  --> n18
    n21 --> n16["Reported"]
    n21 -- "Report Failed" --> n5

    n1@{ shape: rect}
    n2@{ shape: rect}
    n3@{ shape: rect}
    n4@{ shape: rect}
    n5@{ shape: rect}
    n7@{ shape: rect}
    n8@{ shape: rect}
    n10@{ shape: rect}
    n11@{ shape: rect}
    n13@{ shape: rect}
    n15@{ shape: rect}
    n16@{ shape: rect}
    n18@{ shape: rect}
    n19@{ shape: rect}
    n20@{ shape: rect}
    n21@{ shape: rect}
```


## Submission Database

Submissions are tracked within an SQL database at each GRZ.

This database consists of three tables, described in the following sections.

(?) Do we need a schema versioning system or can we let sqlmodel + alembic handle this?

### `submissions`

Columns:

1. `id` (primary key, str)
2. `tanG` (unique nullable str)
    - after phase 0 this column must be null once a submission's test report has been successfully submitted.
3. `pseudonym` (nullable str)
    - during phase 0 this will be the local case ID
    - if tanG is null then we know that this is a real RKI psuedonym instead of a local case ID


### `submission_states`

Columns:

1. `submission_id` (primary key, str, maps to `id` in `submissions`)
2. `timestamp` (str, ISO 8601 format)
3. `state` (enum of lifecycle states)


### `submission_metrics`

Columns:

1. `submission_id` (primary key, str, maps to `id` in `submissions`)

QC metrics are in the following columns: TBD.

Need both metadata/LE-provided numbers and GRZ computed numbers.

## Pipeline Details

### `grz-cli download`

- prevent snakemake from filling up disk space by downloading lots of submissions while a few cores are free
  - idea: subworkflow for each submission with a disk resource that depends on the submission data size (QC pipeline takes up >4x input data size total disk space, including input data)

## Miscellaneous

- Decide on sampling logic for ensuring >=2% and >=1/month of submissions are QCed
- Decide on retry logic for test report API
- Can use [Florian's tool](https://github.com/Hoeze/snakemk_util) to test snakemake rules outside of a workflow
- verify submission ID on our side since it is deterministic
- archive logs (with submission or not?)
  - ensure QC pipeline version + commit hash stored
- track resource consumption for each submission to estimate costs
  - inbox storage / time
  - archive storage
  - CPU hours
  - memory
  - walltime
  - others?
- updating watchdog
    1. Ctrl+C to initiate shutdown of old watchdog and stop accepting new submissions
    2. Start new watchdog instance that starts on unprocessed submissions
- what if we get same submission ID and/or tanG after cleaning from inbox?
