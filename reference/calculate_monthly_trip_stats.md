# Calculate Monthly Trip Statistics by District

Calculates monthly fishing activity statistics by district based on
GPS-tracked boats. These are estimates from a sample of the total fleet
and can be used to extrapolate fleet-wide fishing activity when combined
with boat registry data.

## Usage

``` r
calculate_monthly_trip_stats(trips_data)
```

## Arguments

- trips_data:

  A data frame containing trip information with columns:

  - district: District name

  - date_month: Month as date (first day of month)

  - duration_hrs: Trip duration in hours

  - boat: Unique boat identifier

## Value

A data frame with monthly statistics by district containing:

- district: District name

- date_month: Month as date

- sample_total_trips: Total trips recorded from tracked boats

- sample_boats_tracked: Number of unique boats tracked

- avg_trip_duration_hrs: Average trip duration in hours

- avg_trips_per_boat_per_month: Average trips per boat (for scaling to
  fleet)

## Details

The function processes trip data to generate monthly summaries
including:

- Number of trips recorded from GPS-tracked boats

- Number of unique boats tracked per month

- Average trip duration in hours

- Average trips per boat per month (key metric for fleet scaling)

These metrics are particularly useful for estimating total fishing
effort when GPS trackers are only installed on a subset of the fishing
fleet.

## See also

- [`estimate_fleet_activity()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/estimate_fleet_activity.md)
  for scaling to total fleet

- [`calculate_district_totals()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/calculate_district_totals.md)
  for catch and revenue estimates

## Examples

``` r
if (FALSE) { # \dontrun{
# Calculate monthly statistics
monthly_stats <- calculate_monthly_trip_stats(trips_stats)

# View results
print(monthly_stats)
} # }
```
