# Merge Survey and GPS Trip Data

Matches catch surveys to GPS trips using fuzzy matching on boat
identifiers, then merges all records (matched and unmatched) and uploads
to cloud storage.

## Usage

``` r
merge_trips(site = c("kenya", "zanzibar"), log_threshold = logger::DEBUG)
```

## Arguments

- site:

  Character. Site identifier: "kenya" or "zanzibar"

- log_threshold:

  Logger threshold level. Default is
  [`logger::DEBUG`](https://daroczig.github.io/logger/reference/log_levels.html).

## Value

Invisible NULL. Uploads merged parquet file to cloud storage containing:

- Matched survey-trip pairs (both submission_id and trip are non-NA)

- Unmatched surveys (trip = NA)

- Unmatched trips (submission_id = NA)

- Match quality indicators: n_fields_used, n_fields_ok, match_ok

## Details

The function executes a five-step pipeline:

1.  Load device registry from cloud storage

2.  Load validated surveys

3.  Load preprocessed GPS trips

4.  Match surveys to trips via fuzzy matching (surveys -\> registry -\>
    trips)

5.  Merge matched subset with all unmatched records and upload

Site-specific configuration is handled automatically based on the `site`
parameter.

## Examples

``` r
if (FALSE) { # \dontrun{
# Kenya
merge_trips(site = "kenya")

# Zanzibar
merge_trips(site = "zanzibar")
} # }
```
