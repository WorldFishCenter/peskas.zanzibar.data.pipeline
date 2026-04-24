# Export Raw API-Ready Trip Data

Downloads preprocessed WF and WCS survey data, transforms both into the
canonical API schema, merges them, and uploads a single parquet file to
cloud storage. This is the **raw/preprocessed** stage of the two-stage
API export pipeline.

## Usage

``` r
export_api_raw(log_threshold = logger::DEBUG)
```

## Arguments

- log_threshold:

  Logging level (default
  [`logger::DEBUG`](https://daroczig.github.io/logger/reference/log_levels.html)).

## Value

NULL invisibly. Side effect: uploads merged parquet to cloud storage.

## Details

**Output Schema**:

- `survey_id`: Kobo asset ID identifying the source survey form

- `trip_id`: Unique identifier (`TRIP_<submission_id>` format)

- `landing_date`: Date of landing

- `gaul_1_code`, `gaul_1_name`: GAUL level 1 region

- `gaul_2_code`, `gaul_2_name`: GAUL level 2 district

- `n_fishers`: Total fishers (men + women + children)

- `trip_duration_hrs`: Trip duration in hours

- `gear`: Standardised gear type

- `vessel_type`: Standardised vessel type

- `catch_habitat`: Habitat where catch occurred

- `catch_outcome`: Outcome of catch

- `n_catch`: Number of catch items

- `catch_taxon`: Species alpha-3 code

- `scientific_name`: Scientific name

- `length_cm`: Length in cm (NA for WCS surveys)

- `catch_kg`: Catch weight in kg

- `catch_price`: Individual-level price (NA — not resolved at this
  stage)

- `tot_catch_kg`: Total catch weight per trip

- `tot_catch_price`: Total catch price per trip

**Cloud Storage Location**: `conf$api$trips$raw$cloud_path` /
`{file_prefix}__{timestamp}_{git_sha}__.parquet`

## See also

- [`export_api_validated()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/export_api_validated.md)
  for the validated-data counterpart

- [`preprocess_wf_surveys()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/preprocess_wf_surveys.md)
  and
  [`preprocess_wcs_surveys()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/preprocess_wcs_surveys.md)
  for upstream steps
