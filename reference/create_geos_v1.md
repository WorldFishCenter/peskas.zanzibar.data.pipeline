# Generate Geographic Regional Summaries of Fishery Data (Version 1)

Creates geospatial representations of fishery metrics by aggregating
landing site data to regional levels along the Zanzibar coast. This
simplified version uses direct administrative district mappings instead
of spatial proximity calculations, making it more efficient for cases
where district-to-region relationships are already established.

## Usage

``` r
create_geos_v1(monthly_summaries_dat = NULL, pars = NULL)
```

## Arguments

- monthly_summaries_dat:

  A data frame containing monthly fishery metrics by district. Required
  columns:

  - `district`: Character, name of the landing site district

  - `date`: Date, month of the summary

  - `mean_cpue_day`: Numeric, mean catch per unit effort per day

  - `mean_rpue_day`: Numeric, mean revenue per unit effort per day

  - `mean_price_kg`: Numeric, mean price per kilogram

- pars:

  Configuration parameters list containing:

  - `storage$google$key`: Cloud storage provider key

  - `storage$google$options_coasts`: Cloud storage options for coastal
    data

## Value

NULL (invisible). The function uploads data to cloud storage as side
effects:

- Parquet file: "zanzibar_monthly_summaries_map" containing regional
  summaries

- GeoJSON file:
  "ZAN_regions\_[version](https://rdrr.io/r/base/Version.html).geojson"
  containing regional boundaries

## Details

The function performs the following operations:

1.  **District-Region Mapping**: Retrieves pre-defined administrative
    mappings between districts and regions from site metadata

2.  **Data Harmonization**: Standardizes district names (e.g., "Chake
    chake" to "Chake Chake") to ensure proper joining

3.  **Regional Aggregation**: Calculates monthly summary statistics for
    each region by aggregating district-level data

4.  **GeoJSON Export**: Combines regional polygon geometries with
    summary statistics and exports to cloud storage

**Key Differences from create_geos()**:

- Uses administrative mappings (ADM column) instead of spatial distance
  calculations

- More efficient as it avoids complex spatial operations

- Relies on pre-established district-region relationships in metadata

- Handles district name inconsistencies automatically

**Calculated Regional Metrics** (using median values across districts in
each region):

- Mean CPUE (Catch Per Unit Effort, kg per fisher per day)

- Mean RPUE (Revenue Per Unit Effort, TZS per fisher per day)

- Mean Price per kg of catch

**Output Format**:

- Regional summaries are exported as Parquet files to cloud storage

- GeoJSON file containing regional boundaries is uploaded separately

- Both files use standardized naming conventions with version
  information

## Note

**Dependencies**:

- Requires the `sf` package for reading and writing spatial data

- Requires "ZAN_coast_regions.geojson" file in the package inst/
  directory

- Uses
  [`get_metadata()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/get_metadata.md)
  function to retrieve district-region mappings

- Uses
  [`add_version()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/add_version.md)
  to append version information to filenames

- Uses
  [`upload_parquet_to_cloud()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/upload_parquet_to_cloud.md)
  and
  [`upload_cloud_file()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/upload_cloud_file.md)
  for cloud storage

**Data Processing Notes**:

- District names are case-corrected (specifically "Chake chake" → "Chake
  Chake")

- Regions are converted to lowercase in the output for consistency

- Dates are formatted in ISO 8601 format (YYYY-MM-DDTHH:MM:SSZ)

- All outputs include "country" = "zanzibar" for multi-country
  compatibility

## See also

- [`create_geos()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/create_geos.md)
  for the spatial proximity-based version

- [`export_wf_data()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/export_wf_data.md)
  which calls this function as part of the export workflow

- [`get_metadata()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/get_metadata.md)
  for retrieving site and administrative information

## Examples

``` r
if (FALSE) { # \dontrun{
# Load configuration
pars <- read_config()

# Get monthly summaries data
monthly_data <- download_parquet_from_cloud(
  prefix = "wf_monthly_summaries",
  provider = pars$storage$google$key,
  options = pars$storage$google$options
)

# Create regional geospatial summaries
create_geos_v1(
  monthly_summaries_dat = monthly_data,
  pars = pars
)
} # }
```
