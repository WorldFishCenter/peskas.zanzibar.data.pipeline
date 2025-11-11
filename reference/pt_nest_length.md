# Nest Length Group Columns

Nests length group columns obtained from the structured data of WCS
landings surveys. This reduces the width of data by converting multiple
related columns into a single nested column.

## Usage

``` r
pt_nest_length(x)
```

## Arguments

- x:

  Data frame of WCS survey data in tabular format.

## Value

A data frame with length data nested into a single 'length' column,
which contains a tibble for each row with multiple measurements.
