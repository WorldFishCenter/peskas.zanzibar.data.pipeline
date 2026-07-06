# Validate Preprocessed Zanzibar Gleaning Surveys and Build a Clean Dataset

Flags unreasonable values in the preprocessed Zanzibar gleaning dataset
(the long skeleton from `preprocess_gleaning_surveys()`) and removes
every submission with at least one flag, since a bad value taints the
whole record.

## Usage

``` r
validate_gleaning_surveys(log_threshold = logger::INFO)
```

## Arguments

- log_threshold:

  Logging threshold (default
  [`logger::INFO`](https://daroczig.github.io/logger/reference/log_levels.html)).

## Value

A list with `validated` (input + flag columns + alert fields),
`flagged_submissions` (one row per flagged submission with reasons),
`clean` (original columns, flagged submissions removed), and `summary`
(submissions tripping each check).

## Details

Tailored to the Zanzibar instrument. In addition to the demographic /
effort / economic / temporal range checks, it includes
**container-plausibility** checks that target this pipeline's main
weakness — catch weight is derived as `unit_weight_kg * n_containers`,
so single-container weights exceeding the container's nominal capacity,
or absurd container counts, are the root cause of the heavy-catch tail:

- `flag_unit_weight` - `unit_weight_kg` \> factor x `container_size_kg`

- `flag_n_containers` - implausible number of full containers

Each check writes a `flag_*` logical column (TRUE = problem; NA values
pass). Flags are consolidated per row into `alert_n`, `alert_flag`,
`alert_reasons`, then rolled up to the submission for removal.

## Examples

``` r
if (FALSE) { # \dontrun{
validate_gleaning_surveys()
v$summary
clean <- v$clean
} # }
```
