#' Ingest WCS Catch Survey Data
#'
#' This function automates the downloading of WCS fish catch survey data collected through Kobo Toolbox and uploads it to cloud storage services. The filenames are versioned to include date-time stamps and, if available, the first 7 digits of the Git commit SHA.
#'
#' Configuration parameters required from `conf.yml` include:
#'
#' ```
#' surveys:
#'   wcs_surveys:
#'     asset_id:
#'     username:
#'     password:
#'     file_prefix:
#' storage:
#'   storage_name:
#'     key:
#'     options:
#'       project:
#'       bucket:
#'       service_account_key:
#' ```
#'
#' Progress through the function is tracked using the package *logger*.
#'
#'
#' Logging progress is managed using the `logger` package.
#'
#' @param log_threshold Log level used as the threshold for logging (see [logger::log_levels]).
#' @return None; the function is used for its side effects.
#' @export
#' @keywords workflow ingestion
#' @examples
#' \dontrun{
#' ingest_wcs_surveys(logger::DEBUG)
#' }
#'
ingest_wcs_surveys <- function(log_threshold = logger::DEBUG) {
  logger::log_threshold(log_threshold)

  pars <- read_config()

  file_list <- retrieve_wcs_surveys(
    prefix = pars$surveys$wcs_surveys$raw_surveys$file_prefix,
    append_version = TRUE,
    url = "kf.kobotoolbox.org",
    project_id = pars$surveys$wcs_surveys$asset_id,
    username = pars$surveys$wcs_surveys$username,
    psswd = pars$surveys$wcs_surveys$password,
    encoding = "UTF-8"
  )

  logger::log_info("Uploading files to cloud...")
  # Iterate over multiple storage providers if there are more than one
  purrr::map(pars$storage, ~ upload_cloud_file(file_list, .$key, .$options))
  logger::log_success("Files upload succeded")
}


#' Retrieve WCS Surveys from Kobotoolbox
#'
#' Downloads survey data from Kobotoolbox for a specified project and uploads the data in Parquet format. File naming can include versioning details.
#'
#' @param prefix Filename prefix or path for downloaded files.
#' @param append_version Boolean indicating whether to append versioning info to filenames.
#' @param url URL of the Kobotoolbox instance.
#' @param project_id Project asset ID for data download.
#' @param username Kobotoolbox account username.
#' @param psswd Kobotoolbox account password.
#' @param encoding Character encoding for the downloaded data; defaults to "UTF-8".
#'
#' @return Vector of paths for the downloaded Parquet files.
#' @export
#' @keywords ingestion
#' @examples
#' \dontrun{
#' file_list <- retrieve_wcs_surveys(
#'   prefix = "my_data",
#'   append_version = TRUE,
#'   url = "kf.kobotoolbox.org",
#'   project_id = "my_project_id",
#'   username = "admin",
#'   psswd = "admin",
#'   encoding = "UTF-8"
#' )
#' }
#'
retrieve_wcs_surveys <- function(
    prefix = NULL,
    append_version = NULL,
    url = NULL,
    project_id = NULL,
    username = NULL,
    psswd = NULL,
    encoding = NULL) {
  logger::log_info("Downloading WCS Fish Catch Survey Kobo data...")
  data_raw <-
    KoboconnectR::kobotools_kpi_data(
      url = url,
      assetid = project_id,
      uname = username,
      pwd = psswd,
      encoding = encoding
    )$results

  # Check that submissions are unique in case there is overlap in the pagination
  if (dplyr::n_distinct(purrr::map_dbl(data_raw, ~ .$`_id`)) != length(data_raw)) {
    stop("Number of submission ids not the same as number of records")
  }

  logger::log_info("Converting WCS Fish Catch Survey Kobo data to tabular format...")
  tabular_data <- purrr::map_dfr(data_raw, flatten_row)
  data_filename <- prefix

  if (isTRUE(append_version)) {
    parquet_filename <- add_version(data_filename, "parquet")
  } else {
    parquet_filename <- paste0(data_filename, ".parquet")
  }

  logger::log_info("Converting json data to Parquet as {parquet_filename}...")

  # Convert tabular_data to Arrow Table
  arrow_table <- arrow::as_arrow_table(tabular_data)

  # Write to Parquet format
  arrow::write_parquet(
    arrow_table,
    sink = parquet_filename,
    compression = "lz4",
    compression_level = 12
  )

  return(parquet_filename)
}

#' Flatten Survey Data Rows
#'
#' Transforms each row of nested survey data into a flat tabular format using a mapping and flattening process.
#'
#' @param x A list representing a row of data, potentially containing nested lists or vectors.
#' @return A tibble with each row representing flattened survey data.
#' @keywords internal
#' @export
flatten_row <- function(x) {
  x %>%
    # Each row is composed of several fields
    purrr::imap(flatten_field) %>%
    rlang::squash() %>%
    tibble::as_tibble()
}

#' Flatten Survey Data Fields
#'
#' Processes each field within a row of survey data, handling both simple vectors and nested lists. For lists with named elements, renames and unlists them for flat structure preparation.
#'
#' @param x A vector or list representing a field in the data.
#' @param p The prefix or name associated with the field, used for naming during the flattening process.
#' @return Modified field, either unchanged, unnested, or appropriately renamed.
#' @keywords internal
#' @export
flatten_field <- function(x, p) {
  # If the field is a simple vector do nothing but if the field is a list we
  # need more logic
  if (inherits(x, "list")) {
    if (length(x) > 0) {
      if (purrr::vec_depth(x) == 2) {
        # If the field-list has named elements is we just need to rename the list
        x <- list(x) %>%
          rlang::set_names(p) %>%
          unlist() %>%
          as.list()
      } else {
        # If the field-list is an "array" we need to iterate over its children
        x <- purrr::imap(x, rename_child, p = p)
      }
    }
  } else {
    if (is.null(x)) x <- NA
  }
  x
}

#' Rename Nested Survey Data Elements
#'
#' Appends a parent name or index to child elements within a nested list, assisting in creating a coherent and traceable data structure during the flattening process.
#'
#' @param x A list element, possibly nested, to be renamed.
#' @param i The index or key of the element within the parent list.
#' @param p The parent name to prepend to the element's existing name for context.
#' @return A renamed list element, structured to maintain contextual relevance in a flattened dataset.
#' @keywords internal
#' @export
rename_child <- function(x, i, p) {
  if (length(x) == 0) {
    if (is.null(x)) x <- NA
    x <- list(x)
    x <- rlang::set_names(x, paste(p, i - 1, sep = "."))
  } else {
    if (inherits(i, "character")) {
      x <- rlang::set_names(x, paste(p, i, sep = "."))
    } else if (inherits(i, "integer")) {
      x <- rlang::set_names(x, paste(p, i - 1, names(x), sep = "."))
    }
  }
  x
}
