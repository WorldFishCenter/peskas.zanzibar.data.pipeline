# Validate WCS Surveys Data

Validates Wildlife Conservation Society (WCS) survey data by performing
quality checks and calculating catch metrics. The function follows these
main steps:

1.  Preprocesses survey data

2.  Validates catches using predefined thresholds for weights, counts
    and prices

3.  Calculates revenue and CPUE metrics

4.  Uploads validated data to cloud storage

## Usage

``` r
validate_wcs_surveys(log_threshold = logger::DEBUG)
```

## Arguments

- log_threshold:

  The logging level threshold for the logger package (e.g., DEBUG, INFO)

## Value

None. Writes validated data to parquet file and uploads to cloud storage

## Details

The validation includes:

- Basic data quality checks (e.g., negative catches, missing values)

- Gear-specific validations (e.g., number of fishers per gear type)

- Weight thresholds by catch type (individual vs bucket measures)

- Market price validations (valid price ranges per kg)
