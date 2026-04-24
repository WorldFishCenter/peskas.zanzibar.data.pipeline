# Ingest WCS Catch Survey Data

Retrieves WCS catch survey data from Kobotoolbox, processes it, and
uploads the raw data as a Parquet file to cloud storage.

## Usage

``` r
ingest_wcs_surveys(log_threshold = logger::DEBUG)
```

## Arguments

- log_threshold:

  Logging threshold level (default: logger::DEBUG).

## Value

No return value. Downloads data, processes it, and uploads to cloud
storage.

## Details

The function:

1.  Reads configuration settings

2.  Downloads survey data from Kobotoolbox using
    [`coasts::get_kobo_data()`](https://rdrr.io/pkg/coasts/man/get_kobo_data.html)

3.  Checks for uniqueness of submissions

4.  Converts data to tabular format

5.  Uploads raw data as Parquet files to cloud storage

## Examples

``` r
if (FALSE) { # \dontrun{
ingest_wcs_surveys()
} # }
```
