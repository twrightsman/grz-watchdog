# Design of grz-watchdog

## Pipeline Overview

```mermaid
flowchart TD
    n1["Inbox"] --> n2["grz-cli download"]
    n2 --> n3["grz-cli decrypt"]
    n3 --> n4["grz-cli validate"]
    n4 --> n5["QC?"] & n12["Submission Database"]
    n5 -- 98% --> n6["Submit Prüfbericht"]
    n5 -- "&gt;=2% and &gt;=1/month" --> n7["QC pipeline"]
    n7 --> n6 & n12
    n6 --> n9["Consent?"] & n12
    n9 --> n11["Encrypt"]
    n11 --> n8["Archive<br>(Fully Consented)"] & n10["Archive<br>(Other)"] & n12
    n12 --> n13["Tätigkeitsbericht"]

    n1@{ shape: das}
    n2@{ shape: proc}
    n3@{ shape: proc}
    n4@{ shape: proc}
    n5@{ shape: decision}
    n12@{ shape: db}
    n6@{ shape: proc}
    n7@{ shape: proc}
    n9@{ shape: decision}
    n11@{ shape: proc}
    n8@{ shape: das}
    n10@{ shape: das}
    n13@{ shape: doc}
```


## Submission Lifecycle

```mermaid
flowchart TD
    n1["Uploading"] --> n2["Uploaded"] & n6["Upload Aborted"]
    n2 --> n3["Downloading"]
    n3 --> n4["Downloaded"] & n5["Download Failed"]
    n4 --> n7["Decrypting"]
    n7 --> n8["Decrypted"] & n9["Decryption Failed"]
    n8 --> n10["Validating"]
    n10 --> n11["Validated"] & n12["Invalid"]
    n11 -- 2% --> n13["QC Running"]
    n13 --> n14["QC Failed"] & n15["QC Finished"]
    n11 -- 98% --> n15
    n15 --> n16["Prüfbericht Submitted"] & n17["Prüfbericht Failed"]
    n17 --> n16
    n14 --> n13
    n16 --> n18["Encrypting"]
    n18 --> n19["Encrypted"] & n22["Encryption Failed"]
    n19 --> n20["Archiving"]
    n20 --> n21["Archived"]
    n20 --> n23["Archiving Failed"]

    n1@{ shape: rect}
    n2@{ shape: rect}
    n6@{ shape: rect}
    n3@{ shape: rect}
    n4@{ shape: rect}
    n5@{ shape: rect}
    n7@{ shape: rect}
    n8@{ shape: rect}
    n9@{ shape: rect}
    n10@{ shape: rect}
    n11@{ shape: rect}
    n12@{ shape: rect}
    n13@{ shape: rect}
    n14@{ shape: rect}
    n15@{ shape: rect}
    n16@{ shape: rect}
    n17@{ shape: rect}
    n18@{ shape: rect}
    n19@{ shape: rect}
    n22@{ shape: rect}
    n20@{ shape: rect}
    n21@{ shape: rect}
    n23@{ shape: rect}
```


## Submission Database

Submissions are tracked within an SQLite database at each GRZ.

This database consists of three tables, described in the following sections.


### `submissions`

Columns:

1. `id` (primary key, str)
2. `tanG` (str | None)
    - after phase 0 this column must be null once a submission's test report has been successfully submitted.
    - need not be unique if, for example, there was a mistake in the first submission.
3. `pseudonym` (str | None)


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

