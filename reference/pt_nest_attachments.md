# Nest Attachment Columns

Nests attachment-related columns from structured WCS survey data,
organizing multiple attachment entries into a single nested column. This
function addresses the challenge of handling wide data tables by
converting them into more manageable nested data frames.

## Usage

``` r
pt_nest_attachments(x)
```

## Arguments

- x:

  A data frame containing raw survey data, potentially with multiple
  attachments per survey entry.

## Value

A data frame with attachment information nested into a single
'\_attachments' column, containing a tibble for each row.

## Examples

``` r
if (FALSE) { # \dontrun{
dummy_landings <- tidyr::tibble(
  `_id` = "123",
  `_attachments.0.download_url` = "http://url-1.com",
  `_attachments.0.id` = "01",
  `_attachments.1.download_url` = "http://url-2.com",
  `_attachments.1.id` = "02",
  `_attachments.2.download_url` = NA,
  `_attachments.2.id` = NA
)
pt_nest_attachments(dummy_landings)
} # }
```
