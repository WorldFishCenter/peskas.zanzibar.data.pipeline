# Download Validated Surveys

Retrieves validated survey data from Google Cloud Storage for multiple
survey sources. This function fetches data stored in Parquet format from
sources defined in the configuration.

## Usage

``` r
get_validated_surveys(pars, sources = NULL)
```

## Arguments

- pars:

  A list representing the configuration settings, typically obtained
  from a YAML configuration file. The configuration should include: -
  `surveys.<source>_surveys.validated_surveys.file_prefix`: File prefix
  for validated surveys -
  `surveys.<source>_surveys.validated_surveys.version`: Version to
  retrieve (optional, defaults to `version.preprocess` or "latest") -
  `storage.google.key`: Storage provider key - `storage.google.options`:
  Storage provider options (bucket, project, service_account_key)

- sources:

  Character vector specifying which survey sources to retrieve. Accepts
  short names ("wcs", "wf", "ba") or full config keys ("wcs_surveys",
  "wf_surveys_v1", etc.). The "wf" alias maps to "wf_surveys_v1" for
  backward compatibility. If NULL (default), retrieves data from all
  available sources with validated_surveys configuration.

## Value

A dataframe of validated survey landings from all requested sources,
loaded from Parquet files. Each row includes a 'source' column
indicating the data origin.

## Examples

``` r
if (FALSE) { # \dontrun{
config <- peskas.zanzibar.pipeline::read_config()
# Get all available validated surveys
all_validated <- get_validated_surveys(config)

# Get only WCS and BA validated surveys (using short names)
some_validated <- get_validated_surveys(config, sources = c("wcs", "ba"))

# Get WF validated surveys (maps to wf_surveys_v1)
wf_validated <- get_validated_surveys(config, sources = "wf")
} # }
```
