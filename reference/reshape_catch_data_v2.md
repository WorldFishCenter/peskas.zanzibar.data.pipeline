# Reshape Catch Data with Length Groupings - Version 2

Enhanced version of reshape_catch_data that handles both survey
versions. This function takes a data frame with species catch
information and reshapes it into a long format while properly handling
nested length group information for both survey versions.

## Usage

``` r
reshape_catch_data_v2(df = NULL)
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

The function auto-detects survey version based on column patterns:

- Version 1: Uses single 'species' field, length data nested within
  species groups

- Version 2: Uses multiple species fields (species_TL, species_RF,
  etc.), fish \>100cm in separate repeated group

For Version 2, species fields are normalized using coalesce, and the
separate length group structure for fish \>100cm is properly handled.

## Examples

``` r
if (FALSE) { # \dontrun{
final_data <- reshape_catch_data_v2(catch_info)

# Analyze counts by length range
final_data |>
  filter(!is.na(count)) |>
  group_by(species, length_range) |>
  summarize(total_count = sum(as.numeric(count), na.rm = TRUE))
} # }
```
