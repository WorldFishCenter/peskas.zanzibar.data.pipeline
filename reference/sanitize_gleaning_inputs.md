# Sanitize Gleaning Catch Inputs

Drops (sets to NA) values that fall outside physically plausible bounds.
Defensive against data-entry slips and unit/decimal-separator confusion
(e.g. a single 10 kg bucket recorded as `bucket_weight = 13000`, or a
full bucket count of `7000`). Bounds are deliberately conservative: they
remove only the unambiguously impossible. Stricter, taxon-aware checks
belong downstream — this only protects the reshaped output from extreme
outliers.

Drops (sets to NA) values that fall outside physically plausible bounds.
Defensive against data-entry slips and unit/decimal-separator confusion
(e.g. a single 10 kg bucket recorded as `bucket_weight = 13000`, or a
full bucket count of `7000`). Bounds are deliberately conservative: they
remove only the unambiguously impossible. Stricter, taxon-aware checks
belong downstream — this only protects the reshaped output from extreme
outliers.

## Usage

``` r
sanitize_gleaning_inputs(df)

sanitize_gleaning_inputs(df)
```

## Arguments

- df:

  A data frame containing any subset of the columns above.

## Value

The input with out-of-range values replaced by NA.

The input with out-of-range values replaced by NA.

## Details

Bounds:

- `n_individuals` in 0-10000 — a bucket of small shells can hold
  thousands; counts above this are almost certainly errors. Recorded
  zeros are kept (a true "none in this size class").

- `unit_weight_kg` in 0-100 — nominal containers are 5–50 kg.

- `n_containers` in 0-200 — hand gleaning rarely exceeds a few full
  containers; very large counts are data slips.

Columns absent from the input are left untouched.

Bounds:

- `n_individuals` in 0-10000 — a bucket of small shells can hold
  thousands; counts above this are almost certainly errors. Recorded
  zeros are kept (a true "none in this size class").

- `unit_weight_kg` in 0-100 — nominal containers are 5–50 kg.

- `n_containers` in 0-200 — hand gleaning rarely exceeds a few full
  containers; very large counts are data slips.

Columns absent from the input are left untouched.
