storage inbox:
    provider="s3",
    # optionally add custom settings here if needed
    # alternatively they can be passed via command line arguments
    # starting with --storage-s3-..., see below
    # Maximum number of requests per second for this storage provider. If nothing is specified, the default implemented by the storage plugin is used.
    max_requests_per_second=...,
    # S3 endpoint URL (if omitted, AWS S3 is used)
    endpoint_url=...,
    # region constraint for the S3 storage
    region=...,
    # S3 access key (if omitted, credentials are taken from .aws/credentials as e.g. created by aws configure)
    access_key=...,
    # S3 secret key (if omitted, credentials are taken from .aws/credentials as e.g. created by aws configure)
    secret_key=...,
    # S3 token (usually not required)
    token=...,
    # S3 signature version
    signature_version=...,
    # S3 API retries
    retries=...,


storage nonconsented:
    provider="s3",
    max_requests_per_second=...,
    endpoint_url=...,
    region=...,
    access_key=...,
    secret_key=...,
    token=...,
    signature_version=...,
    retries=...,


storage consented:
    provider="s3",
    max_requests_per_second=...,
    endpoint_url=...,
    region=...,
    access_key=...,
    secret_key=...,
    token=...,
    signature_version=...,
    retries=...,
