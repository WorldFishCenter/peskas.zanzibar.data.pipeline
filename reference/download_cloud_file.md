# Download Object from Cloud Storage

Downloads an object from cloud storage to a local file.

## Usage

``` r
download_cloud_file(name, provider, options, file = name)
```

## Arguments

- name:

  The name of the object in the storage bucket.

- provider:

  A character string specifying the cloud provider ("gcs" or "aws").

- options:

  A named list of provider-specific options including the bucket and
  authentication details.

- file:

  (Optional) The local path to save the downloaded object. If not
  specified, the object name is used.

## Value

The path to the downloaded file.

## Details

For GCS, the options list should include:

- `bucket`: The name of the bucket from which the object is downloaded.

- `service_account_key`: The authentication JSON contents, if not
  previously authenticated.

## Examples

``` r
if (FALSE) { # \dontrun{
authentication_details <- readLines("path/to/json_file.json")
download_cloud_file(
  "object_name.json",
  "gcs",
  list(service_account_key = authentication_details, bucket = "my-bucket"),
  "local_path/to/save/object.json"
)
} # }
```
