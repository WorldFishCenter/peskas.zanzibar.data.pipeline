# Nest Market Group Columns

Nests market group columns from structured WCS landings survey data.
This method organizes multiple related market data points into a single
nested 'market' column per row.

## Usage

``` r
pt_nest_market(x)
```

## Arguments

- x:

  Data frame of WCS survey data in tabular format.

## Value

A data frame with market data nested into a 'market' column, containing
a tibble for each row with various market-related attributes.
