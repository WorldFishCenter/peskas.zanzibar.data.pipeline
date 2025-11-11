# \#' Download Parquet File from Cloud Storage

This function handles the process of downloading a parquet file from
cloud storage and reading it into memory.

## Usage

``` r
download_parquet_from_cloud(prefix, provider, options, version = "latest")
```

## Arguments

- prefix:

  The file prefix path in cloud storage

- provider:

  The cloud storage provider key

- options:

  Cloud storage provider options

- version:

  The version to retrieve ("latest" or specific version string)

## Value

A tibble containing the data from the parquet file

## Examples

``` r
if (FALSE) { # \dontrun{
raw_data <- download_parquet_from_cloud(
  prefix = conf$ingestion$koboform$catch$legacy$raw,
  provider = conf$storage$google$key,
  options = conf$storage$google$options
)
} # }
```
