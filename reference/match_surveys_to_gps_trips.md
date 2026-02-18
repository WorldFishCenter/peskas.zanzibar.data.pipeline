# Match Catch Surveys to GPS Trips

Universal two-step matching workflow that links catch survey records to
GPS trip data via device identifiers (IMEI). Works for both Kenya and
Zanzibar by accepting either an explicit device registry or constructing
an implicit one from the trips data.

## Usage

``` r
match_surveys_to_gps_trips(
  surveys,
  trips,
  registry = NULL,
  reg_threshold = 0.15,
  name_threshold = 0.25
)
```

## Arguments

- surveys:

  Data frame containing:

  - submission_id: Unique survey identifier

  - landing_date: Date of landing

  - Boat identifiers: registration_number (or variants), boat_name,
    fisher_name (or captain)

- trips:

  Data frame containing:

  - trip: Unique trip identifier

  - imei: Device identifier

  - ended: Trip end timestamp

  - Boat identifiers: registration_number (or variants), boat_name,
    fisher_name (or captain)

- registry:

  Optional data frame with device-boat mappings containing:

  - imei: Device identifier

  - Boat identifiers: registration_number (or variants), boat_name,
    fisher_name (or captain)

  If NULL, will be constructed from trips data (Zanzibar approach).

- reg_threshold:

  Numeric. Maximum normalized Levenshtein distance (0-1) for
  registration number fuzzy matching. Default is 0.15 (15% difference
  allowed).

- name_threshold:

  Numeric. Maximum normalized Levenshtein distance (0-1) for boat name
  and fisher name fuzzy matching. Default is 0.25 (25% difference
  allowed).

## Value

Data frame combining matched and unmatched records with columns:

- submission_id: Survey identifier (NA for unmatched trips)

- landing_date: Landing date

- imei: Device identifier

- n_fields_used: Number of fields available for matching (0-3)

- n_fields_ok: Number of fields that matched within threshold

- match_ok: Logical indicating successful match (at least 1 field
  matched)

- trip: GPS trip identifier (NA for unmatched surveys)

- started, ended: Trip timestamps

- registration_number_survey, registration_number_trip: For comparison

- boat_name_survey, boat_name_trip: For comparison

- fisher_name_survey, fisher_name_trip: For comparison

- boat, duration_seconds, range_meters, distance_meters: Trip metadata

- Additional trip columns (gear, community, etc.)

## Details

The function implements a two-step matching process:

**Step 1: Survey -\> Registry (IMEI Assignment)**

1.  Standardizes column names across datasets

2.  Cleans text fields (lowercase, remove punctuation, normalize
    whitespace)

3.  Uses fuzzy matching (Levenshtein distance) on registration number,
    boat name, and fisher name

4.  Assigns each survey to the registry entry with the most matching
    fields

5.  Requires at least 1 field to match within threshold (match_ok =
    TRUE)

**Step 2: IMEI -\> Trips (Date Matching)**

1.  Joins surveys to trips by IMEI and landing_date

2.  Only creates matches when there is exactly ONE survey and ONE trip
    per IMEI-date

3.  Records with multiple trips/surveys per day remain unmatched

4.  Preserves all unmatched surveys and trips in the output

## Matching Thresholds

The normalized Levenshtein distance ranges from 0 (exact match) to 1
(completely different):

- reg_threshold = 0.15: Allows ~15% character differences in
  registration numbers

- name_threshold = 0.25: Allows ~25% character differences in names

## Site-Specific Usage

- Kenya:

  Uses explicit device registry from Airtable/PDS

- Zanzibar:

  Builds implicit registry from historical trip data (registry = NULL)

## Examples

``` r
if (FALSE) { # \dontrun{
# Kenya: With explicit device registry
results <- match_surveys_to_gps_trips(
  surveys = kefs_surveys,
  trips = pds_trips,
  registry = devices
)

# Zanzibar: Without explicit registry (builds from trips)
results <- match_surveys_to_gps_trips(
  surveys = wf_surveys,
  trips = pds_trips,
  registry = NULL
)

# With custom thresholds
results <- match_surveys_to_gps_trips(
  surveys = surveys,
  trips = trips,
  registry = devices,
  reg_threshold = 0.10,  # Stricter registration matching
  name_threshold = 0.30  # More lenient name matching
)
} # }
```
