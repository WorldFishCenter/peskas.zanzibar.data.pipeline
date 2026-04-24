# Ingest WF Catch Survey Data

Retrieves WF catch survey data (v1 and v2) from Kobotoolbox, processes
it, and uploads the raw data as Parquet files to cloud storage.

## Usage

``` r
ingest_wf_surveys(log_threshold = logger::DEBUG)
```

## Arguments

- log_threshold:

  Logging threshold level (default: logger::DEBUG).

## Value

No return value. Downloads data, processes it, and uploads to cloud
storage.

## Details

The function processes both WF v1 and v2 catch surveys from
eu.kobotoolbox.org.

## Examples

``` r
if (FALSE) { # \dontrun{
ingest_wf_surveys()
} # }
```
