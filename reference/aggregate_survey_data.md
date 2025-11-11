# Aggregate survey data and calculate metrics

Aggregates catch data to survey level and calculates key fisheries
metrics including total catches, revenues, and effort-based indicators.
The function:

1.  Aggregates catches and revenue by survey

2.  Joins with trip information

3.  Calculates effort-based metrics (CPUE, RPUE)

4.  Adjusts fisher counts for multiple boats when applicable

## Usage

``` r
aggregate_survey_data(catch_price_table, trips_info)
```

## Arguments

- catch_price_table:

  A dataframe containing catch data with calculated prices

- trips_info:

  A dataframe containing trip-level information

## Value

A dataframe of aggregated survey data containing:

- Survey metadata (ID, date, location, habitat)

- Vessel and gear information

- Total catches and revenue

- Effort-based metrics (CPUE, RPUE)

- Nested catch composition data

## Details

Calculated metrics include:

- CPUE (Catch Per Unit Effort): kg/fisher/day

- RPUE (Revenue Per Unit Effort): TZS/fisher/day
