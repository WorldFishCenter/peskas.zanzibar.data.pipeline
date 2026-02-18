# Clean Text for Cross-Dataset Matching

Standardizes text fields by converting to lowercase, removing
punctuation, eliminating common boat prefixes, and normalizing
whitespace.

## Usage

``` r
clean_text(x)
```

## Arguments

- x:

  Character vector to clean

## Value

Character vector with cleaned text. Empty strings are converted to NA.

## Details

The function performs the following transformations:

- Converts to lowercase

- Replaces punctuation (/, \\ -, \_, ,, ., :, ;) with spaces

- Removes common boat prefixes (mv, mv., boat, vessel)

- Removes all non-alphanumeric characters except spaces

- Normalizes whitespace (removes extra spaces and trims)

- Converts empty strings to NA
