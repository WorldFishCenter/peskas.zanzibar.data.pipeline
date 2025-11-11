# Pre-process and Combine WorldFish Surveys - Both Versions

Downloads and preprocesses raw structured WorldFish survey data from
both version 1 and version 2 sources, combines the results, and uploads
a unified dataset. The function automatically handles different survey
structures and merges the processed data.

## Usage

``` r
preprocess_wf_surveys(log_threshold = logger::DEBUG, version = "v2")
```

## Arguments

- log_threshold:

  The logging threshold to use. Default is logger::DEBUG. See
  [`logger::log_levels`](https://daroczig.github.io/logger/reference/log_levels.html)
  for available options.

- version:

  Character string, deprecated. Function now processes both versions
  automatically.

## Value

None; the function is used for its side effects.

## Details

**Processing Workflow:**

1.  Downloads and processes Version 1 surveys (if available)

2.  Downloads and processes Version 2 surveys (if available)

3.  Combines (binds) both datasets into a unified result

4.  Uploads the combined dataset to cloud storage

**Survey Version Differences:**

- **Version 1**: Single species field, nested length groups within
  species groups

- **Version 2**: Multiple species fields (species_TL, species_RF, etc.),
  separate repeated group for fish \>100cm
  (`species_group/no_fish_by_length_group_100/`)

The function uses
[`reshape_catch_data_v2()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/reshape_catch_data_v2.md)
which automatically detects survey version based on column patterns and
applies appropriate processing logic for each dataset.

**Error Handling:** If one version fails to process or is unavailable,
the function continues with the available data and logs warnings. The
function only fails if both versions are unavailable.

Configurations are read from `conf.yml` with the following necessary
parameters:

    surveys:
      wf_surveys:
        raw_surveys:
          file_prefix:
        preprocessed_surveys:
          file_prefix:
      wf_surveys_v2:
        raw_surveys:
          file_prefix:
    storage:
      google:
        key:
        options:
          project:
          bucket:
          service_account_key:

## See also

[`reshape_catch_data_v2`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/reshape_catch_data_v2.md),
[`preprocess_general`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/preprocess_general.md),
[`preprocess_catch`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/preprocess_catch.md),
[`process_version_data`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/process_version_data.md)

## Examples

``` r
if (FALSE) { # \dontrun{
# Process and combine both survey versions
preprocess_wf_surveys()

# With custom logging threshold
preprocess_wf_surveys(log_threshold = logger::INFO)
} # }
```
