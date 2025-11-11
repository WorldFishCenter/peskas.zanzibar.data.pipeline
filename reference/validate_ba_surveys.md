# Validate Blue Alliance (BA) Surveys Data

Validates Blue Alliance survey data by performing quality checks and
calculating catch metrics. The function follows these main steps:

1.  Loads and preprocesses survey data

2.  Performs logical checks on key variables

3.  Calculates catch and length bounds

4.  Flags potential data quality issues

5.  Saves and uploads validated data

## Usage

``` r
validate_ba_surveys(log_threshold = logger::DEBUG)
```

## Arguments

- log_threshold:

  The logging level threshold for the logger package (e.g., DEBUG, INFO)

## Value

None. Writes validated data to parquet file and uploads to cloud storage

## Details

The validation includes:

- Logical checks (non-negative catches, valid fisher counts, valid trip
  durations)

- Statistical outlier detection for catch weights and lengths

- Automated flagging system for quality control

Alert flag descriptions:

- 1: Total catch is negative

- 2: Number of fishers is 0 or negative

- 3: Trip duration is 0 or negative

- 4: Catch weight or length exceeds calculated bounds
