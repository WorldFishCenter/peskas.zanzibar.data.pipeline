# Export Validated API-Ready Trip Data

Downloads validated WF survey data, transforms it into the canonical API
schema, and uploads a single parquet file to cloud storage. This is the
**validated** stage of the two-stage API export pipeline.

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

**Why WCS is held back**: the export is gated on `api$include_wcs`,
which is `FALSE`. The WF/WCS audit in issue \#4 concluded that the
difference between the two programmes is real rather than a processing
artefact — they sample different vessel platforms in different
proportions — but it left two preconditions for publishing them
together: every row needs a source label, and `catch_price` means
different things in each programme (WCS derives it from market medians,
WF reads a trip-level field). Until those are settled,
`format_api_wcs()` stays in place and unreferenced at runtime rather
than being deleted. Flip the flag to publish both.

## See also

- [`export_api_raw()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/export_api_raw.md)
  for the raw/preprocessed counterpart

- [`validate_wf_surveys()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/validate_wf_surveys.md)
  and
  [`validate_wcs_surveys()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/validate_wcs_surveys.md)
  for upstream steps
