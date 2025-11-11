# Validate Wild Fishing Survey Data

Validates survey data from wild fishing activities by applying quality
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

- 5: Bucket weight exceeds maximum

- 6: Number of buckets exceeds maximum

- 7: Number of individuals exceeds maximum

- 8: Incomplete catch form

- 9: Incomplete catch information

## Note

- Requires configuration parameters to be set up in config file

- Automatically downloads preprocessed survey data from cloud storage

- Removes submissions that fail validation checks

- Sets catch_kg to 0 when catch_outcome is 0

## Data Processing Steps

1.  Downloads preprocessed survey data

2.  Applies validation checks and generates alert flags

3.  Filters out submissions with validation alerts

4.  Processes catch data and adjusts catch weights

5.  Uploads validation flags and validated data to cloud storage
