# Clean Registration Numbers

Standardizes boat registration numbers by removing spaces and filtering
out invalid or placeholder values.

## Usage

``` r
clean_registration(x)
```

## Arguments

- x:

  Character vector of registration numbers to clean

## Value

Character vector with cleaned registration numbers. Invalid values
(empty strings, "0", "0000", "035") are converted to NA.

## Details

The function performs the following transformations:

- Applies text cleaning via
  [`clean_text()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/clean_text.md)

- Removes all spaces

- Converts invalid placeholders to NA: "", "0", "0000", "035"
