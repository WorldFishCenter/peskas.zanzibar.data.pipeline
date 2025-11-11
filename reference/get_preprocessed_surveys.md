# Download Preprocessed Surveys

Retrieves preprocessed survey data from Google Cloud Storage,
specifically configured for WCS (Wildlife Conservation Society)
datasets. This function fetches data stored in Parquet format.

## Usage

``` r
get_preprocessed_surveys(pars, prefix = NULL)
```

## Arguments

- pars:

  A list representing the configuration settings, typically obtained
  from a YAML configuration file.

- prefix:

  A character string specifying the organization prefix to retrieve
  preprocessed surveys, either "wcs" or "ba".

## Value

A dataframe of preprocessed survey landings, loaded from Parquet files.

## Examples

``` r
if (FALSE) { # \dontrun{
config <- peskas.zanzibar.pipeline::read_config()
df_preprocessed <- get_preprocessed_surveys(config, prefix = "ba")
} # }
```
