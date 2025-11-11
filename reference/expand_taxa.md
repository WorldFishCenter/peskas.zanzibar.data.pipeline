# Expand Taxonomic Vectors into a Data Frame

Converts a vector of species identifiers into a detailed data frame
containing taxonomic classification. Each identifier should follow the
format 'family_genus_species', which is expanded to include
comprehensive taxonomic details.

## Usage

``` r
expand_taxa(data = NULL)
```

## Arguments

- data:

  A vector of species identifiers formatted as 'family_genus_species'.
  If not provided, the function will return an error.

## Value

A data frame where each row corresponds to a species, enriched with
taxonomic classification information including family, genus, species,
and additional taxonomic ranks.

## Details

This function splits each species identifier into its constituent parts,
replaces underscores with spaces for readability, and retrieves
taxonomic classification from the GBIF database using the `taxize`
package.

## Note

Requires internet access to fetch data from the GBIF database. The
accuracy of results depends on the correct formatting of input data and
the availability of taxonomic data in the GBIF database.

## Examples

``` r
if (FALSE) { # \dontrun{
species_vector <- c("lutjanidae_lutjanus_spp", "scaridae_spp", "acanthuridae_naso_hexacanthus")
expanded_data <- expand_taxa(species_vector)
} # }
```
