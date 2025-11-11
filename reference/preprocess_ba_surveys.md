# Pre-process Blue Alliance Surveys

Downloads and preprocesses raw structured Blue Alliance survey data from
cloud storage into a binary format. The process includes date
standardization and survey ID generation for unique trip identification.

## Usage

``` r
preprocess_ba_surveys(log_threshold = logger::DEBUG)
```

## Arguments

- log_threshold:

  The logging threshold to use. Default is logger::DEBUG. See
  [`logger::log_levels`](https://daroczig.github.io/logger/reference/log_levels.html)
  for available options.

## Value

None; the function is used for its side effects.

## Details

Configurations are read from `conf.yml` with the following necessary
parameters:

    surveys:
      ba_surveys:
        asset_id:
        username:
        password:
        file_prefix:
      version:
        preprocess:
    storage:
      storage_name:
        key:
        options:
          project:
          bucket:
          service_account_key:

The function uses logging to track progress and creates unique survey
IDs using CRC32 hashing of concatenated trip attributes.
