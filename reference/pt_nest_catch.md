# Nest Catch Group Columns

Nests catch group columns from WCS structured survey data to organize
multiple related catch data points into a single nested 'catch' column
per row.

## Usage

``` r
pt_nest_catch(x)
```

## Arguments

- x:

  Data frame of WCS survey data in tabular format.

## Value

A data frame with catch data nested into a 'catch' column, containing a
tibble for each row with various catch-related attributes.
