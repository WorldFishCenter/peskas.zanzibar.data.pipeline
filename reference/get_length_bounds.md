# Get length bounds for survey data

Calculates upper bounds for fish lengths by gear type and catch taxon
using robust statistical methods. Similar to get_catch_bounds but for
length measurements. The function:

1.  Filters out invalid fish categories

2.  Groups data by gear and fish category

3.  Calculates upper bounds on log scale and exponentiates results

## Usage

``` r
get_length_bounds(data = NULL, k_param = NULL)
```

## Arguments

- data:

  A dataframe containing survey data with columns for gear, catch_taxon,
  and length_cm

- k_param:

  Numeric parameter for the LocScaleB outlier detection (default: NULL).
  Higher values are more conservative in outlier detection.

## Value

A dataframe containing upper length bounds for each gear and catch taxon
combination
