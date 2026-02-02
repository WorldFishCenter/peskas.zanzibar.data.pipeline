# Export Validated API-Ready Trip Data

Processes WorldFish preprocessed survey data into a simplified
API-friendly format and exports it to cloud storage for external
consumption. This function exports the **validated** version of trip
data.

## Usage

``` r
export_api_validated(log_threshold = logger::DEBUG)
```

## Arguments

- log_threshold:

  The logging level threshold for the logger package (e.g., DEBUG,
  INFO). See
  [`logger::log_levels`](https://daroczig.github.io/logger/reference/log_levels.html)
  for available options. Default is logger::DEBUG.

## Value

NULL (invisible). The function uploads data to cloud storage as a side
effect.

## Details

The function performs the following operations:

- Downloads **validated** WF survey data from cloud storage

- Loads form-specific assets (taxa, geography, gear, vessels) from
  Airtable metadata

- Generates unique trip IDs using xxhash64 algorithm

- Transforms nested survey structure to flat API format

- Joins with standardized lookup tables (districts, gear types, vessel
  types)

- Exports to the **validated** cloud storage path (before validation)

**Data Pipeline Context**: This function exports raw preprocessed data
and is part of a two-stage API export pipeline:

1.  [`raw()`](https://rdrr.io/r/base/raw.html) - Exports
    raw/preprocessed data

2.  `export_api_validated()` - Exports validated data (this function)

**Output Schema**: The exported dataset includes the following fields:

- `trip_id`: Unique identifier (TRIP_xxxxxxxxxxxx format)

- `landing_date`: Date of landing

- `gaul_2_name`: Standardized district name (GAUL level 2)

- `n_fishers`: Total number of fishers (men + women + children)

- `trip_duration_hrs`: Duration in hours

- `gear`: Standardized gear type

- `vessel_type`: Standardized vessel type

- `catch_habitat`: Habitat where catch occurred

- `catch_outcome`: Outcome of catch (landed, sold, etc.)

- `n_catch`: Number of individual catch items

- `catch_taxon`: Species or taxonomic group

- `length_cm`: Length measurement in centimeters

- `catch_kg`: Weight in kilograms

- `catch_price`: Price in local currency

**Cloud Storage Location**: Files are uploaded to the path specified in
`conf$api$trips$validated$cloud_path` (e.g., `zanzibar/validated/`) with
versioned filenames following the pattern:
`{file_prefix}__{timestamp}_{git_sha}__.parquet`

## See also

- [`preprocess_wf_surveys()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/preprocess_wf_surveys.md)
  for generating the preprocessed survey data

- [`validate_wf_surveys()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/validate_wf_surveys.md)
  for the validation step that produces validated data

- [`download_parquet_from_cloud()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/download_parquet_from_cloud.md)
  for retrieving data from cloud storage

- [`upload_cloud_file()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/upload_cloud_file.md)
  for uploading data to cloud storage

- [`get_airtable_form_id()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/get_airtable_form_id.md)
  for retrieving form-specific asset metadata

## Examples

``` r
if (FALSE) { # \dontrun{
# Export raw API trip data with default debug logging
export_api_validated()

# Export with info-level logging only
export_api_validated(logger::INFO)
} # }
```
