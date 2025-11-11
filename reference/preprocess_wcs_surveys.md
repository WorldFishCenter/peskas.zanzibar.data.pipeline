# Pre-process Zanzibar WCS Surveys

Downloads and preprocesses raw structured WCS survey data from cloud
storage into a binary format. The process includes nesting multiple
columns related to species information into single columns within a
dataframe, which helps reduce its width and organize data efficiently
for analysis.

## Usage

``` r
preprocess_wcs_surveys(log_threshold = logger::DEBUG)
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
      wcs_surveys:
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

The function uses logging to track progress.

## See also

[`pt_nest_trip`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/pt_nest_trip.md),
[`pt_nest_catch`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/pt_nest_catch.md),
[`pt_nest_length`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/pt_nest_length.md),
[`pt_nest_market`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/pt_nest_market.md),
[`pt_nest_attachments`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/pt_nest_attachments.md)
