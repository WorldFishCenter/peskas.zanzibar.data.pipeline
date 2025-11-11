# Prepare Boat Registry Data from Metadata

Processes boat registry data from metadata table to create a summary of
total boats by district. This data is used for scaling GPS sample data
to fleet-wide estimates.

## Usage

``` r
prepare_boat_registry(boats_table = NULL)
```

## Arguments

- boats_table:

  Data frame containing boat registry information with:

  - District: District name for each boat

  - Additional boat metadata (boat ID, registration details, etc.)

## Value

A data frame with boat counts by district:

- district: District name (standardized from metadata)

- total_boats: Total number of boats registered in the district

## Details

The function takes boat metadata and creates a district-level summary of
boat counts. This is essential for calculating sampling rates and
extrapolating fleet-wide activity from GPS-tracked samples.

## See also

- [`estimate_fleet_activity()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/estimate_fleet_activity.md)
  for using boat registry in fleet estimates

## Examples

``` r
if (FALSE) { # \dontrun{
# Get boat registry data from metadata
metadata <- get_metadata()
boat_registry <- prepare_boat_registry(boats_table = metadata$boats)

# View boat counts by district
print(boat_registry)
} # }
```
