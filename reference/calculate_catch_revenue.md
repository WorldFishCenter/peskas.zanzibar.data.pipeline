# Calculate catch revenue from validated data

Calculates revenue based on catch weights and market prices. The
function handles missing prices through a hierarchical approach:

1.  Uses direct match if available

2.  Falls back to group median price if direct match is missing

3.  Uses overall median price if group median is unavailable

## Usage

``` r
calculate_catch_revenue(validated, market_table)
```

## Arguments

- validated:

  A dataframe of validated catch data containing weights and taxonomic
  information

- market_table:

  A dataframe containing market price information by species group and
  family

## Value

A dataframe with calculated revenues, including:

- Survey and catch identification

- Catch details (date, vessel, gear, fishers)

- Taxonomic information

- Adjusted catch weights

- Calculated revenue in TZS

## Details

The function also:

- Adjusts catch weights based on number of elements when applicable

- Aggregates catches at the survey and catch number level

- Calculates total revenue in TZS (Tanzanian Shillings)
