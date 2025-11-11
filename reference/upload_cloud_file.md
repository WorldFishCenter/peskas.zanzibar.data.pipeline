# Upload File to Cloud Storage

Uploads a local file to a specified cloud storage bucket, supporting
both single and multiple files.

## Usage

``` r
upload_cloud_file(file, provider, options, name = file)
```

## Arguments

- file:

  A character vector specifying the path(s) of the file(s) to upload.

- provider:

  A character string specifying the cloud provider ("gcs" or "aws").

- options:

  A named list of provider-specific options including the bucket and
  authentication details.

- name:

  (Optional) The name to assign to the file in the cloud. If not
  specified, the local file name is used.

## Value

A list of metadata objects for the uploaded files if successful.

## Details

For GCS, the options list must include:

- `bucket`: The name of the bucket to which files are uploaded.

- `service_account_key`: The authentication JSON contents, if not
  previously authenticated.

This function utilizes
[`googleCloudStorageR::gcs_upload()`](https://cloudyr.github.io/googleCloudStorageR//reference/gcs_upload.html)
for file uploads to GCS.

## Examples

``` r
if (FALSE) { # \dontrun{
authentication_details <- readLines("path/to/json_file.json")
upload_cloud_file(
  "path/to/local_file.csv",
  "gcs",
  list(service_account_key = authentication_details, bucket = "my-bucket")
)
} # }
```
