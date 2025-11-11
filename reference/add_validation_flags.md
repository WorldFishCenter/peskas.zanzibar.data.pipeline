# Add validation flags to catch data

Adds quality control flags based on predefined thresholds and rules.
Flag descriptions: 1: Number of fishers is 0 2: Catch is negative 3:
Catch is null despite group_catch being non-NULL 4: Catch is 0 despite
group_catch being non-NULL 5: Number of buckets is too high (\> 150) 6:
Bucket is \> 40kg 7: Small pelagic individual weight \> 10kg 8:
Individual weight \> 250kg 9: Number of fishers is too high (\>100) for
non-ring nets

## Usage

``` r
add_validation_flags(catch_data)
```

## Arguments

- catch_data:

  Processed catch data

## Value

A dataframe with validation flags added
