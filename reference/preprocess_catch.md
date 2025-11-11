# Preprocess Catch Data for Both Survey Versions

Processes catch data from WorldFish surveys, handling both version 1 and
version 2 survey structures. Automatically detects survey version and
applies appropriate species field normalization and length group
processing.

## Usage

``` r
preprocess_catch(data = NULL, version = NULL)
```

## Arguments

- data:

  A data frame containing raw survey data with species groups

- version:

  Character string specifying version ("v1" or "v2"). If NULL, version
  will be auto-detected based on column patterns.

## Value

A data frame with processed catch data including: submission_id,
n_catch, count_method, catch_taxon, n_buckets, weight_bucket,
individuals, length

## Details

The function uses
[`reshape_catch_data_v2()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/reshape_catch_data_v2.md)
internally which automatically handles:

- Version 1: Single species field, nested length groups

- Version 2: Multiple species fields (species_TL, species_RF, etc.),
  separate length group structure for fish \>100cm
