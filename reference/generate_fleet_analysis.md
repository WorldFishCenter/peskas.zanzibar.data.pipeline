# Generate Complete Fleet Activity Analysis Pipeline

Executes the complete fleet activity analysis pipeline from data
retrieval through final aggregation and cloud storage upload. This
function orchestrates all steps including metadata retrieval, trip
processing, fleet estimation, and results storage.

## Usage

``` r
generate_fleet_analysis(log_threshold = logger::DEBUG)
```

## Arguments

- log_threshold:

  Logging threshold level (default: logger::DEBUG). Controls the
  verbosity of logging output during pipeline execution.

## Value

Invisibly returns NULL. The function's primary purpose is to:

- Generate and save aggregated analysis results to RDS file

- Upload results to cloud storage

- Log progress and completion status

## Details

This function executes the complete analysis pipeline:

1.  Reads configuration parameters and retrieves metadata

2.  Prepares boat registry from metadata

3.  Processes trip data from PDS API using device IMEIs

4.  Downloads monthly summaries from cloud storage

5.  Calculates monthly trip statistics from GPS data

6.  Estimates fleet-wide activity using boat registry

7.  Calculates district totals using catch/revenue data

8.  Generates annual summary statistics by district

9.  Saves aggregated results to RDS file and uploads to cloud storage

The function creates a comprehensive analysis that scales GPS-tracked
boat data to estimate total fleet activity, catch, and revenue by
district and time period.

## Output Files

The function creates an RDS file containing a list with three
components:

- **fleet_estimates**: Monthly fleet activity estimates by district

- **district_totals**: Monthly district-level catch and revenue totals

- **annual_summary**: Annual summary statistics by district including:

  - months_with_data: Number of months with data

  - avg_sampling_rate: Average sampling rate across months

  - total_estimated_catch_kg: Total estimated annual catch

  - total_estimated_revenue: Total estimated annual revenue

  - avg_monthly_catch_kg: Average monthly catch

  - avg_monthly_revenue: Average monthly revenue

## See also

- [`prepare_boat_registry()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/prepare_boat_registry.md)
  for boat registry preparation

- [`process_trip_data()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/process_trip_data.md)
  for trip data processing

- [`calculate_monthly_trip_stats()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/calculate_monthly_trip_stats.md)
  for monthly statistics

- [`estimate_fleet_activity()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/estimate_fleet_activity.md)
  for fleet estimates

- [`calculate_district_totals()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/calculate_district_totals.md)
  for final totals

## Examples

``` r
if (FALSE) { # \dontrun{
# Run complete fleet analysis pipeline
generate_fleet_analysis()

# Run with different logging level
generate_fleet_analysis(log_threshold = logger::INFO)
} # }
```
