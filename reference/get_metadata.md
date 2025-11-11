# Get metadata tables

Get Metadata tables from Google sheets. This function downloads the
tables that include information about the fishery. You can specify a
single table to download or get all available tables.

## Usage

``` r
get_metadata(table = NULL, log_threshold = logger::DEBUG)
```

## Arguments

- table:

  Character. Name of the specific table to download. If NULL (default),
  all tables specified in the configuration will be downloaded.

- log_threshold:

  The logging threshold level. Default is logger::DEBUG.

## Value

A named list containing the requested tables as data frames. If a single
table is requested, the list will contain only that table. If no table
is specified, the list will contain all available tables.

## Details

The parameters needed in `conf.yml` are:

    storage:
      storage_name:
        key:
        options:
          project:
          bucket:
          service_account_key:
    metadata:
      google_sheets:
        sheet_id:
        tables:
          - table1
          - table2

## Examples

``` r
if (FALSE) { # \dontrun{
# Ensure you have the necessary configuration in conf.yml

# Download all metadata tables
metadata_tables <- get_metadata()

# Download a specific table
catch_table <- get_metadata(table = "devices")
} # }
```
