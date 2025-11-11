# Upload Processed Data to Cloud Storage

This function handles the process of writing data to a parquet file and
uploading it to cloud storage.

## Usage

``` r
upload_parquet_to_cloud(
  data,
  prefix,
  provider,
  options,
  compression = "lz4",
  compression_level = 12
)
```

## Arguments

- data:

  The data frame or tibble to upload

- prefix:

  The file prefix path in cloud storage

- provider:

  The cloud storage provider key

- options:

  Cloud storage provider options

- compression:

  Compression algorithm to use (default: "lz4")

- compression_level:

  Compression level (default: 12)

## Value

Invisible NULL

## Examples

``` r
if (FALSE) { # \dontrun{
upload_parquet_to_cloud(
  data = processed_data,
  prefix = conf$ingestion$koboform$catch$legacy$preprocessed,
  provider = conf$storage$google$key,
  options = conf$storage$google$options
)
} # }
```
