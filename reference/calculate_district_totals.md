# Calculate District-Level Total Catch and Revenue

Combines fleet activity estimates with catch and revenue data to
calculate total catch and revenue by district and month. Uses fleet-wide
trip estimates to scale up from sample-based averages.

## Usage

``` r
calculate_district_totals(fleet_estimates, monthly_summaries)
```

## Arguments

- fleet_estimates:

  Data frame from
  [`estimate_fleet_activity()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/estimate_fleet_activity.md)
  containing:

  - district: District name

  - date_month: Month as date

  - estimated_total_trips: Estimated trips for entire fleet

  - sampling_rate: Proportion of fleet tracked

  - Other fleet statistics

- monthly_summaries:

  Data frame with catch/revenue data containing:

  - district: District name (must match fleet_estimates)

  - date: Month as date (will be matched to date_month)

  - metric: Metric name (filtered for mean_catch_kg and
    mean_catch_price)

  - value: Metric value

## Value

A data frame with district-level totals:

- district: District name

- date_month: Month as date

- sample_total_trips: Trips from tracked boats

- estimated_total_trips: Estimated trips for entire fleet

- sampling_rate: Proportion of fleet tracked

- mean_catch_kg: Average catch per trip

- mean_catch_price: Average revenue per trip

- estimated_total_catch_kg: Estimated total catch for district

- estimated_total_revenue: Estimated total revenue for district

## Details

This function merges fleet activity data with monthly catch/revenue
summaries to estimate total district-level fishing production. The
calculations are:

- Total catch = mean catch per trip × estimated total trips

- Total revenue = mean revenue per trip × estimated total trips

Only districts with catch data are included in the results.

## See also

- [`calculate_monthly_trip_stats()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/calculate_monthly_trip_stats.md)
  for trip statistics

- [`estimate_fleet_activity()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/estimate_fleet_activity.md)
  for fleet-wide estimates

## Examples

``` r
if (FALSE) { # \dontrun{
# Calculate the full pipeline
monthly_stats <- calculate_monthly_trip_stats(trips_data)
fleet_estimates <- estimate_fleet_activity(monthly_stats, boat_registry)
district_totals <- calculate_district_totals(fleet_estimates, monthly_summaries)

# View total catch by district
district_totals |>
  dplyr::group_by(district) |>
  dplyr::summarise(total_annual_catch = sum(estimated_total_catch_kg, na.rm = TRUE))
} # }
```
