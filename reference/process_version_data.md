# Process Version Data Helper Function

Internal helper. Assembles per-version preprocessed data from
already-reshaped catch/general info using precomputed length-weight
coefficients.

## Usage

``` r
process_version_data(catch_info, general_info, lwcoeffs)
```

## Arguments

- catch_info:

  Processed catch information for one form version

- general_info:

  Processed general information for one form version

- lwcoeffs:

  Precomputed length-weight coefficients (list with `lw` and `ml`
  elements). Computed once in
  [`preprocess_wf_surveys()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/preprocess_wf_surveys.md)
  across the union of taxa from all available versions, then reused for
  every version.

## Value

Combined and processed survey data for one version
