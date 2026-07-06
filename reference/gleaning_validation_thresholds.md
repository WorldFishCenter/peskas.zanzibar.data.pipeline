# Default Thresholds for Gleaning Survey Validation

Returns the bounds used by
[`validate_gleaning_surveys()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/validate_gleaning_surveys.md)
for the Zanzibar pipeline. Calibrated to the observed Zanzibar
distributions, which differ from Kenya: prices are in TZS, catch weight
is reconstructed from the bucket / plastic-bag container fields (so its
errors are larger and need container-level checks), and recall gaps run
longer. Override any value by name, e.g.
`gleaning_validation_thresholds(total_catch_kg_max = 40)`.

## Usage

``` r
gleaning_validation_thresholds(...)
```

## Arguments

- ...:

  Named overrides for any default threshold.

## Value

A named list of thresholds.
