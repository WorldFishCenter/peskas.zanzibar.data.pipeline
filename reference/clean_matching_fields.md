# Clean Matching Fields in a Data Frame

Creates cleaned versions of boat identifiers for fuzzy matching. Removes
boat_name when it duplicates the registration_number.

## Usage

``` r
clean_matching_fields(data)
```

## Arguments

- data:

  Data frame with standardized column names: registration_number,
  boat_name, fisher_name

## Value

Data frame with three additional columns: registration_number_clean,
boat_name_clean, fisher_name_clean

## Details

The function performs the following operations:

- Cleans registration_number using
  [`clean_registration()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/clean_registration.md)

- Cleans boat_name and fisher_name using
  [`clean_text()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/clean_text.md)

- Sets boat_name_clean to NA when it matches registration_number_clean
  (to avoid double-counting the same information)
