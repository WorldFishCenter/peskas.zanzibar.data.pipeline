# Match IMEIs to GPS Trips with One-Trip-Per-Day Constraint

Joins surveys to GPS trips by IMEI and landing_date, but only creates
matches when there is exactly one survey and one trip per IMEI-date
combination. Records with multiple trips/surveys per day remain
unmatched.

## Usage

``` r
match_imei_to_trip(matched, trips)
```

## Arguments

- matched:

  Data frame of surveys with matched IMEIs (from
  match_surveys_to_registry) containing submission_id, landing_date,
  imei, and boat identifiers

- trips:

  Data frame of GPS trips containing trip, imei, ended, and boat
  identifiers

## Value

Data frame combining all surveys and trips with the following structure:

- **Matched records**: Survey-trip pairs where both have
  unique_trip_per_day = TRUE

- **Unmatched surveys**: Surveys with multiple per day
  (unique_trip_per_day = FALSE)

- **Unmatched trips**: Trips with multiple per day (unique_trip_per_day
  = FALSE)

Key columns include:

- submission_id, landing_date, imei

- n_fields_used, n_fields_ok, match_ok (from registry matching)

- trip, started, ended (trip identifiers)

- registration_number_survey, registration_number_trip (for comparison)

- boat_name_survey, boat_name_trip

- fisher_name_survey, fisher_name_trip

- boat, duration_seconds, range_meters, distance_meters (trip metadata)

## Details

The function implements a conservative matching strategy:

1.  Standardizes column names and creates landing_date from trip end
    times

2.  Counts number of surveys and trips per IMEI-date combination

3.  Splits data into unique (n=1) and non-unique (n\>1) groups

4.  Only joins the unique groups (one survey + one trip per day)

5.  Combines matched records with all unmatched records

6.  Removes internal columns (\_clean suffixes, counts, flags)

This approach prevents ambiguous matches when multiple trips occur on
the same day.
