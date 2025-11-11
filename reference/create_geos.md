# Generate Geographic Regional Summaries of Fishery Data

This function creates geospatial representations of fishery metrics by
aggregating lnding sites data to regional levels along the Zanzibar
coast. It assigns each site to its nearest coastal region, calculates
regional summaries of fishery performance metrics, and exports the
results as a GeoJSON file for spatial visualization.

## Usage

``` r
create_geos(monthly_summaries_dat = NULL, pars = NULL)
```

## Arguments

- monthly_summaries_dat:

  A data frame containing monthly fishery metrics by site

- pars:

  Congiguration parameters

## Value

This function does not return a value. It writes a GeoJSON file named
"zanzibar_monthly_summaries.geojson" to the inst/ directory of the
package, containing regional polygons with associated monthly fishery
metrics.#'

## Details

The function performs the following operations:

1.  **BMU Coordinate Extraction**: Retrieves geographic coordinates
    (latitude/longitude) for all landing sites

2.  **Spatial Conversion**: Converts sites coordinates to spatial point
    objects.

3.  **Regional Assignment**: Uses spatial analysis to assign each BMU to
    its nearest coastal region.

4.  **Regional Aggregation**: Calculates monthly summary statistics for
    each region by aggregating BMU data.

5.  **GeoJSON Creation**: Combines regional polygon geometries with
    summary statistics and exports as GeoJSON.

**Calculated Regional Metrics** (using median values across BMUs in each
region):

- Mean CPUE (Catch Per Unit Effort, kg per fisher)

- Mean RPUE (Revenue Per Unit Effort, currency per fisher)

- Mean Price per kg of catch

## Note

**Dependencies**:

- Requires the `sf` package for spatial operations.

- Requires a GeoJSON file named "ZAN_coast_regions.geojson" included in
  the peskas.zanzibar.data.pipeline package.

- Uses the
  [`get_metadata()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/get_metadata.md)
  function to retrieve sites location information.

## Examples

``` r
if (FALSE) { # \dontrun{
# First generate monthly summaries
monthly_data <- get_fishery_metrics(validated_data, bmu_size)

# Then create regional geospatial summary
create_geos(monthly_summaries_dat = monthly_data)
} # }
```
