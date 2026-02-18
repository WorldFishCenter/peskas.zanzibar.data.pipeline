# Match Surveys to Device Registry via Fuzzy Matching

Uses per-field Levenshtein distances to match surveys to a boat
registry. Each survey is matched to the registry entry with the most
fields within the specified distance thresholds.

## Usage

``` r
match_surveys_to_registry(
  surveys,
  registry,
  reg_threshold = 0.15,
  name_threshold = 0.25
)
```

## Arguments

- surveys:

  Data frame with cleaned matching fields (registration_number_clean,
  boat_name_clean, fisher_name_clean) and submission_id

- registry:

  Data frame with cleaned matching fields (registration_number_clean,
  boat_name_clean, fisher_name_clean) and imei

- reg_threshold:

  Numeric. Maximum normalized Levenshtein distance (0-1) for
  registration number matching. Default is 0.15.

- name_threshold:

  Numeric. Maximum normalized Levenshtein distance (0-1) for boat name
  and fisher name matching. Default is 0.25.

## Value

Data frame with the following columns:

- submission_id: Survey identifier

- imei: Matched device IMEI

- n_fields_used: Number of non-NA fields available for matching (0-3)

- n_fields_ok: Number of fields within threshold

- match_ok: Logical indicating if at least one field matched
  (n_fields_ok \>= 1)

## Details

The matching algorithm:

1.  Computes Levenshtein distance matrices between surveys and registry
    for each field (registration, boat name, fisher name)

2.  Normalizes distances by maximum string length to get values in 0,1

3.  For each survey, counts how many fields match each registry entry
    (within thresholds)

4.  Assigns survey to the registry entry with the most matching fields

5.  Sets match_ok = TRUE if at least one field matched

Surveys with no non-NA fields return imei = NA and match_ok = FALSE.
