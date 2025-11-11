# Generate a Kepler.gl map

This function is a R wrapper of `kepler_wcs_mapper_py`, a python script
function aimed to elaborate produce a self-contained map (in html) using
the Kepler.gl python library
<https://docs.kepler.gl/docs/keplergl-jupyter>.

## Usage

``` r
kepler_mapper(data_path = NULL)
```

## Arguments

- data_path:

  Data to add to map.

## Value

A self-contained map in html.
