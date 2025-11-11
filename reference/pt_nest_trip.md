# Nest Trip Group Columns

Processes and nests trip-related columns from structured WCS landings
survey data into a single 'trip' column. This approach consolidates trip
information into nested tibbles within the dataframe, simplifying the
structure for analysis.

## Usage

``` r
pt_nest_trip(x)
```

## Arguments

- x:

  A data frame containing structured survey data in tabular format.

## Value

A data frame with trip data nested into a single 'trip' column
containing a tibble for each row, corresponding to the various trip
details.
