# Validate worldfish Survey Data

Validates survey data from worldfish activities by applying quality
control checks and flagging potential data issues. The function filters
out submissions that don't meet validation criteria and processes catch
data.

## Usage

``` r
validate_wf_surveys(log_threshold = logger::DEBUG)
```

## Arguments

- log_threshold:

  The logging level threshold for the logger package (e.g., DEBUG, INFO)

## Value

The function processes and uploads two datasets to cloud storage:

1.  Validation flags for each submission

2.  Validated survey data with invalid submissions removed

## Details

The function applies the following validation checks:

1.  Bucket weight validation (max 50 kg per bucket)

2.  Number of buckets validation (max 300 buckets)

3.  Number of individuals validation (max 100 individuals)

4.  Form completeness check for catch details

5.  Catch information completeness check

Alert codes:

- 1: A catch was reported with no taxon

- 2: A taxon with neither a number of fish nor a weight

- 3, 4: Length below the taxon's minimum, or above its maximum

- 5: Bucket weight exceeds maximum

- 6: Number of buckets exceeds maximum

- 7: Number of individuals exceeds maximum

- 8: Price per kg exceeds maximum (`price_kg_max`)

- 9, 10: CPUE or RPUE exceeds maximum

- 11: Landing date is after the submission date

- 12: Trip duration is zero or negative

- 13: Fisher count is zero or negative

- 14: Price per kg is below minimum (`price_kg_min`)

8 to 10 and 14 are checked only on submissions with none of the others.

## Note

- Requires configuration parameters to be set up in config file

- Automatically downloads preprocessed survey data from cloud storage

- Removes submissions that fail validation checks, unless a reviewer
  approved them (read with
  [`coasts::review_decisions()`](https://rdrr.io/pkg/coasts/man/review_decisions.html))

- Sets catch_kg to 0 when catch_outcome is 0

## Data Processing Steps

1.  Downloads preprocessed survey data and reads reviewers' decisions

2.  Applies validation checks and generates alert flags

3.  Filters out submissions with validation alerts, except those a
    reviewer approved

4.  Processes catch data and adjusts catch weights

5.  Uploads validation flags and validated data to cloud storage
