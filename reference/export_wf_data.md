# Export WorldFish Summary Data to MongoDB

Downloads previously summarized WorldFish survey data from cloud
storage, incorporates modeled aggregated estimates, and exports
everything to MongoDB collections for use in data portals. The function
also generates geographic regional summaries.

## Usage

``` r
export_wf_data(log_threshold = logger::DEBUG)
```

## Arguments

- log_threshold:

  The logging level threshold for the logger package (e.g., DEBUG, INFO)
  See
  [`logger::log_levels`](https://daroczig.github.io/logger/reference/log_levels.html)
  for available options.

## Value

NULL (invisible). The function uploads data to MongoDB as a side effect.

## Details

The function performs the following operations:

- Downloads five summary datasets from cloud storage:

  - Monthly summaries: Aggregated catch metrics by district and month

  - Taxa summaries: Species-specific metrics in long format

  - Districts summaries: District-level indicators over time

  - Gear summaries: Performance metrics by gear type

  - Grid summaries: Spatial grid data from vessel tracking

- Downloads aggregated catch estimates from the modeling step

- Creates geographic regional summaries using the monthly data

- Joins aggregated estimates (fishing trips, catch tonnage, revenue) to
  monthly summaries

- Transforms monthly summaries to long format for portal consumption

- Uploads all datasets to specified MongoDB collections

The function expects the summary files to be named with the pattern:
`{file_prefix}_{table_name}.parquet` where table_name is one of:
monthly_summaries, taxa_summaries, districts_summaries, gear_summaries,
grid_summaries

## See also

- [`summarize_data()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/summarize_data.md)
  for generating the summary datasets

- [`download_parquet_from_cloud()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/download_parquet_from_cloud.md)
  for retrieving data from cloud storage

- [`mdb_collection_push()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/mdb_collection_push.md)
  for uploading data to MongoDB

- [`create_geos()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/create_geos.md)
  for generating geographic summaries

## Examples

``` r
if (FALSE) { # \dontrun{
# Export WF summary data with default debug logging
export_wf_data()

# Export with info-level logging only
export_wf_data(logger::INFO)
} # }
```
