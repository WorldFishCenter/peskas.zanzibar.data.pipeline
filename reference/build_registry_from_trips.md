# Build Boat Registry from GPS Trips Data

Extracts unique boat identifiers from GPS trips data to create an
implicit device registry. Used when a separate registry table is not
available (e.g., for Zanzibar data).

## Usage

``` r
build_registry_from_trips(trips)
```

## Arguments

- trips:

  Data frame with imei and boat identifier columns

## Value

Data frame with unique combinations of:

- imei: Device identifier

- registration_number: Boat registration (standardized name)

- boat_name: Boat name

- fisher_name: Fisher/captain name (standardized name)

Only rows with at least one non-NA identifier are retained.

## Details

The function:

1.  Standardizes column names (vessel_reg_number -\>
    registration_number, etc.)

2.  Selects only the identifier columns

3.  Finds distinct combinations

4.  Filters out rows where all identifiers (except imei) are NA
