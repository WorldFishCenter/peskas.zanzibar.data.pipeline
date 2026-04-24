# Export Validated API-Ready Trip Data

Downloads validated WF and WCS survey data, transforms both into the
canonical API schema, merges them, and uploads a single parquet file to
cloud storage. This is the **validated** stage of the two-stage API
export pipeline.

## Usage

``` r
export_api_validated(log_threshold = logger::DEBUG)
```

## Arguments

- log_threshold:

  Logging level (default
  [`logger::DEBUG`](https://daroczig.github.io/logger/reference/log_levels.html)).

## Value

NULL invisibly. Side effect: uploads merged parquet to cloud storage.

## Details

See
[`export_api_raw()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/export_api_raw.md)
for the full output schema. This function reads from the validated cloud
paths and writes to `conf$api$trips$validated$cloud_path`.

## See also

- [`export_api_raw()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/export_api_raw.md)
  for the raw/preprocessed counterpart

- [`validate_wf_surveys()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/validate_wf_surveys.md)
  and
  [`validate_wcs_surveys()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/validate_wcs_surveys.md)
  for upstream steps
