# Process Trip Data with District Information

Retrieves and processes trip data from the PDS API by adding district
information from community metadata. Filters out unrealistic trips
longer than 48 hours to remove outliers and prepares data for monthly
statistics calculation.

## Usage

``` r
process_trip_data(pars = NULL, imei_list = NULL)
```

## Arguments

- pars:

  Configuration parameters containing:

  - pds\$token: API token for PDS access

  - pds\$secret: API secret for PDS access

- imei_list:

  Vector of IMEI numbers for devices to retrieve trip data for

## Value

A processed data frame with:

- district: District name (from community metadata)

- community: Community name

- trip: Trip identifier

- boat: Boat identifier

- landing_date: Date of trip landing

- date_month: First day of landing month

- duration_hrs: Trip duration in hours

## Details

The function:

- Calls
  [`get_trips()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/get_trips.md)
  API to retrieve trip data for specified IMEIs

- Converts timestamps to landing dates and monthly periods

- Calculates trip duration in hours from seconds

- Joins with community metadata to add district information

- Filters out unrealistic trips (\>48 hours)

- Standardizes column names and structure

## See also

- [`calculate_monthly_trip_stats()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/calculate_monthly_trip_stats.md)
  for using processed trip data

- [`get_trips()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/get_trips.md)
  for the underlying API call

- [`get_metadata()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/get_metadata.md)
  for community metadata retrieval

## Examples

``` r
if (FALSE) { # \dontrun{
# Process trip data with parameters and device list
pars <- read_config()
trips_stats <- process_trip_data(
  pars = pars,
  imei_list = imei_list
)

# Check data structure
str(trips_stats)
} # }
```
