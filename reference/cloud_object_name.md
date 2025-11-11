# Retrieve Full Name of Versioned Cloud Object

Gets the full name(s) of object(s) in cloud storage matching the
specified prefix, version, and file extension.

## Usage

``` r
cloud_object_name(
  prefix,
  version = "latest",
  extension = "",
  provider,
  exact_match = FALSE,
  options
)
```

## Arguments

- prefix:

  A string indicating the object's prefix.

- version:

  A string specifying the version ("latest" or a specific version
  string).

- extension:

  The file extension to filter by. An empty string ("") includes all
  extensions.

- provider:

  A character string specifying the cloud provider ("gcs" or "aws").

- exact_match:

  A logical indicating whether to match the prefix exactly.

- options:

  A named list of provider-specific options including the bucket and
  authentication details.

## Value

A vector of names of objects matching the criteria.

## Details

For GCS, the options list should include:

- `bucket`: The bucket name.

- `service_account_key`: The authentication JSON contents, if not
  previously authenticated.

## Examples

``` r
if (FALSE) { # \dontrun{
authentication_details <- readLines("path/to/json_file.json")
cloud_object_name(
  "prefix",
  "latest",
  "json",
  "gcs",
  list(service_account_key = authentication_details, bucket = "my-bucket")
)
#'
} # }
```
