# Reshape Catch Data with Length Groupings

This function takes a data frame with species catch information and
reshapes it into a long format while properly handling nested length
group information.

## Usage

``` r
reshape_catch_data(df = NULL)
```

## Arguments

- df:

  A data frame containing catch data with species groups and length
  information

## Value

A data frame in long format with each row representing a species at a
specific length range, or just species data if no length information is
available

## Details

The function first calls
[`reshape_species_groups()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/reshape_species_groups.md)
to convert the species data to long format, then handles the length
group information. For length data, it creates separate rows for each
length category while preserving the original structure for catches
without length measurements. Special handling is provided for fish over
100cm, where the actual length is moved to a separate column.

The function preserves all rows, even those without length data, by
creating consistent column structure across all rows.

## Examples

``` r
if (FALSE) { # \dontrun{
final_data <- reshape_catch_data(catch_info)

# Analyze counts by length range
final_data |>
  filter(!is.na(count)) |>
  group_by(species, length_range) |>
  summarize(total_count = sum(as.numeric(count), na.rm = TRUE))
} # }
```
