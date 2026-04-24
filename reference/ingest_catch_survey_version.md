# Core ingestion logic for catch survey data

Downloads survey data from Kobotoolbox, validates submission uniqueness,
flattens the nested JSON into tabular format, and uploads to cloud
storage as a versioned Parquet file.

## Usage

``` r
ingest_catch_survey_version(version, kobo_config, storage_config)
```

## Arguments

- version:

  Version identifier (e.g., "wcs", "wf_v1", "wf_v2")

- kobo_config:

  List with Kobo connection details: `url`, `asset_id`, `username`,
  `password`.

- storage_config:

  List with storage details: `file_prefix`, `provider`, `options`.

## Value

No return value. Processes and uploads data as side effects.
