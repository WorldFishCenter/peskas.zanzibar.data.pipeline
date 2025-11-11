# Retrieve Surveys from Kobotoolbox

Downloads survey data from Kobotoolbox for a specified project and
uploads the data in Parquet format. File naming can include versioning
details.

## Usage

``` r
retrieve_surveys(
  prefix = NULL,
  append_version = NULL,
  url = NULL,
  project_id = NULL,
  username = NULL,
  psswd = NULL,
  encoding = NULL
)
```

## Arguments

- prefix:

  Filename prefix or path for downloaded files.

- append_version:

  Boolean indicating whether to append versioning info to filenames.

- url:

  URL of the Kobotoolbox instance.

- project_id:

  Project asset ID for data download.

- username:

  Kobotoolbox account username.

- psswd:

  Kobotoolbox account password.

- encoding:

  Character encoding for the downloaded data; defaults to "UTF-8".

## Value

Vector of paths for the downloaded Parquet files.

## Examples

``` r
if (FALSE) { # \dontrun{
file_list <- retrieve_surveys(
  prefix = "my_data",
  append_version = TRUE,
  url = "kf.kobotoolbox.org",
  project_id = "my_project_id",
  username = "admin",
  psswd = "admin",
  encoding = "UTF-8"
)
} # }
```
