# Ingest WCS and WF Catch Survey Data

This function handles the automated ingestion of fish catch survey data
from both WCS and WF sources through Kobo Toolbox. It performs the
following operations:

1.  Downloads the survey data from Kobo Toolbox

2.  Processes and formats the data

3.  Uploads the processed files to configured cloud storage locations

## Usage

``` r
ingest_surveys(log_threshold = logger::DEBUG)
```

## Arguments

- log_threshold:

  The logging threshold to use. Default is logger::DEBUG. See
  [`logger::log_levels`](https://daroczig.github.io/logger/reference/log_levels.html)
  for available options.

## Value

None (invisible). The function performs its operations for side effects:

- Creates parquet files locally

- Uploads files to configured cloud storage

- Generates logs of the process

## Details

The function requires specific configuration in the `conf.yml` file with
the following structure:

    surveys:
      wcs_surveys:
        raw_surveys:
          file_prefix: "wcs_raw_data"     # Prefix for output files
          asset_id: "xxxxx"               # Kobo Toolbox asset ID
          username: "user@example.com"    # Kobo Toolbox username
          password: "password123"         # Kobo Toolbox password
      wf_surveys:
        raw_surveys:
          file_prefix: "wf_raw_data"
          asset_id: "yyyyy"
          username: "user2@example.com"
          password: "password456"
    storage:
      gcp:                               # Storage provider name
        key: "google"                    # Storage provider identifier
        options:
          project: "project-id"          # Cloud project ID
          bucket: "bucket-name"          # Storage bucket name
          service_account_key: "path/to/key.json"

The function processes both WCS and WF surveys sequentially, with
separate logging for each step. For each survey:

- Downloads data using the
  [`retrieve_surveys()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/retrieve_surveys.md)
  function

- Converts the data to parquet format

- Uploads the resulting files to all configured storage providers

Error handling is managed through the logger package, with informative
messages at each step of the process.

## See also

- [`retrieve_surveys()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/retrieve_surveys.md)
  for details on the survey retrieval process

- [`upload_cloud_file()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/upload_cloud_file.md)
  for details on the cloud upload process

## Examples

``` r
if (FALSE) { # \dontrun{
# Run with default debug logging
ingest_surveys()

# Run with info-level logging only
ingest_surveys(logger::INFO)
} # }
```
