# Standardize Column Names for Matching

Renames variant column names to standard names expected by matching
functions. Creates missing columns as NA if they don't exist.

## Usage

``` r
standardize_column_names(data)
```

## Arguments

- data:

  Data frame with boat identifier columns

## Value

Data frame with standardized column names: registration_number,
boat_name, fisher_name

## Details

The function handles variant column names from different data sources:

- **Registration**: vessel_reg_number, boat_reg_no -\>
  registration_number

- **Fisher**: captain_name, captain -\> fisher_name

- **Boat name**: retained as boat_name

If any of these columns are missing entirely, they are created as
NA_character\_.
