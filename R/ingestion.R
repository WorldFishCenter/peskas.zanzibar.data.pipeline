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

  logger::log_info("Downloading WCS Fish Catch Survey Kobo data...")
  file_list <- retrieve_surveys(
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


ingest_wf_surveys <- function(log_threshold = logger::DEBUG) {
  logger::log_threshold(log_threshold)

  pars <- read_config()

  logger::log_info("Downloading WF Fish Catch Survey Kobo data...")

  file_list <- retrieve_surveys(
    prefix = pars$surveys$wf_surveys$raw_surveys$file_prefix,
    append_version = TRUE,
    url = "kf.kobotoolbox.org",
    project_id = pars$surveys$wf_surveys$asset_id,
    username = pars$surveys$wf_surveys$username,
    psswd = pars$surveys$wf_surveys$password,
    encoding = "UTF-8"
  )

  logger::log_info("Uploading files to cloud...")
  # Iterate over multiple storage providers if there are more than one
  purrr::map(pars$storage, ~ upload_cloud_file(file_list, .$key, .$options))
  logger::log_success("Files upload succeded")
}


#' Retrieve Surveys from Kobotoolbox
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
#' file_list <- retrieve_surveys(
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
retrieve_surveys <- function(
    prefix = NULL,
    append_version = NULL,
    url = NULL,
    project_id = NULL,
    username = NULL,
    psswd = NULL,
    encoding = NULL) {
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

#' Retrieve Trip Details from Pelagic Data API
#'
#' This function retrieves trip details from the Pelagic Data API for a specified time range,
#' with options to filter by IMEIs and include additional information.
#'
#' @param token Character string. The API token for authentication.
#' @param secret Character string. The API secret for authentication.
#' @param dateFrom Character string. Start date in 'YYYY-MM-dd' format.
#' @param dateTo Character string. End date in 'YYYY-MM-dd' format.
#' @param imeis Character vector. Optional. Filter by IMEI numbers.
#' @param deviceInfo Logical. If TRUE, include device IMEI and ID fields in the response. Default is FALSE.
#' @param withLastSeen Logical. If TRUE, include device last seen date in the response. Default is FALSE.
#' @param tags Character vector. Optional. Filter by trip tags.
#'
#' @return A data frame containing trip details.
#' @keywords ingestion
#' @examples
#' \dontrun{
#' trips <- get_trips(
#'   token = "your_token",
#'   secret = "your_secret",
#'   dateFrom = "2020-05-01",
#'   dateTo = "2020-05-03",
#'   imeis = c("123456789", "987654321"),
#'   deviceInfo = TRUE,
#'   withLastSeen = TRUE,
#'   tags = c("tag1", "tag2")
#' )
#' }
#'
#' @export
#'
get_trips <- function(
    token = NULL,
  secret = NULL,
  dateFrom = NULL,
  dateTo = NULL,
  imeis = NULL,
  deviceInfo = FALSE,
  withLastSeen = FALSE,
  tags = NULL) {
# Base URL
base_url <- paste0("https://analytics.pelagicdata.com/api/", token, "/v1/trips/", dateFrom, "/", dateTo)

# Build query parameters
query_params <- list()
if (!is.null(imeis)) {
query_params$imeis <- paste(imeis, collapse = ",")
}
if (deviceInfo) {
query_params$deviceInfo <- "true"
}
if (withLastSeen) {
query_params$withLastSeen <- "true"
}
if (!is.null(tags)) {
query_params$tags <- paste(tags, collapse = ",")
}

# Build the request
req <- httr2::request(base_url) %>%
httr2::req_headers(
"X-API-SECRET" = secret,
"Content-Type" = "application/json"
) %>%
httr2::req_url_query(!!!query_params)

# Perform the request
resp <- req %>% httr2::req_perform()

# Check for HTTP errors
if (httr2::resp_status(resp) != 200) {
stop("Request failed with status: ", httr2::resp_status(resp), "\n", httr2::resp_body_string(resp))
}

# Read CSV content
content_text <- httr2::resp_body_string(resp)
trips_data <- readr::read_csv(content_text, show_col_types = FALSE)

return(trips_data)
}



#' Get Trip Points from Pelagic Data Systems API
#'
#' Retrieves trip points data from the Pelagic Data Systems API. The function can either
#' fetch data for a specific trip ID or for a date range. The response can be returned
#' as a data frame or written directly to a file.
#'
#' @param token Character string. Access token for the PDS API.
#' @param secret Character string. Secret key for the PDS API.
#' @param id Numeric or character. Optional trip ID. If provided, retrieves points for
#'   specific trip. If NULL, dateFrom and dateTo must be provided.
#' @param dateFrom Character string. Start date for data retrieval in format "YYYY-MM-DD".
#'   Required if id is NULL.
#' @param dateTo Character string. End date for data retrieval in format "YYYY-MM-DD".
#'   Required if id is NULL.
#' @param path Character string. Optional path where the CSV file should be saved.
#'   If provided, the function returns the path instead of the data frame.
#' @param imeis Vector of character or numeric. Optional IMEI numbers to filter the data.
#' @param deviceInfo Logical. If TRUE, includes device information in the response.
#'   Default is FALSE.
#' @param errant Logical. If TRUE, includes errant points in the response.
#'   Default is FALSE.
#' @param withLastSeen Logical. If TRUE, includes last seen information.
#'   Default is FALSE.
#' @param tags Vector of character. Optional tags to filter the data.
#' @param overwrite Logical. If TRUE, will overwrite existing file when path is provided.
#'   Default is TRUE.
#'
#' @return If path is NULL, returns a tibble containing the trip points data.
#'   If path is provided, returns the file path as a character string.
#'
#' @examples
#' \dontrun{
#' # Get data for a specific trip
#' trip_data <- get_trip_points(
#'   token = "your_token",
#'   secret = "your_secret",
#'   id = "12345",
#'   deviceInfo = TRUE
#' )
#'
#' # Get data for a date range
#' date_data <- get_trip_points(
#'   token = "your_token",
#'   secret = "your_secret",
#'   dateFrom = "2024-01-01",
#'   dateTo = "2024-01-31"
#' )
#'
#' # Save data directly to file
#' file_path <- get_trip_points(
#'   token = "your_token",
#'   secret = "your_secret",
#'   id = "12345",
#'   path = "trip_data.csv"
#' )
#' }
#'
#' @keywords ingestion
#'
#' @export
get_trip_points <- function(token = NULL,
        secret = NULL,
        id = NULL,
        dateFrom = NULL,
        dateTo = NULL,
        path = NULL,
        imeis = NULL,
        deviceInfo = FALSE,
        errant = FALSE,
        withLastSeen = FALSE,
        tags = NULL,
        overwrite = TRUE) {
# Build base URL based on whether ID is provided
if (!is.null(id)) {
base_url <- paste0(
"https://analytics.pelagicdata.com/api/",
token,
"/v1/trips/",
id,
"/points"
)
} else {
if (is.null(dateFrom) || is.null(dateTo)) {
stop("dateFrom and dateTo are required when id is not provided")
}
base_url <- paste0(
"https://analytics.pelagicdata.com/api/",
token,
"/v1/points/",
dateFrom,
"/",
dateTo
)
}

# Build query parameters
query_params <- list()
if (!is.null(imeis)) {
query_params$imeis <- paste(imeis, collapse = ",")
}
if (deviceInfo) {
query_params$deviceInfo <- "true"
}
if (errant) {
query_params$errant <- "true"
}
if (withLastSeen) {
query_params$withLastSeen <- "true"
}
if (!is.null(tags)) {
query_params$tags <- paste(tags, collapse = ",")
}
# Add format=csv if saving to file
if (!is.null(path)) {
query_params$format <- "csv"
}

# Build the request
req <- httr2::request(base_url) %>%
httr2::req_headers(
"X-API-SECRET" = secret,
"Content-Type" = "application/json"
) %>%
httr2::req_url_query(!!!query_params)

# Perform the request
resp <- req %>% httr2::req_perform()

# Check for HTTP errors first
if (httr2::resp_status(resp) != 200) {
stop(
"Request failed with status: ",
httr2::resp_status(resp),
"\n",
httr2::resp_body_string(resp)
)
}

# Handle the response based on whether path is provided
if (!is.null(path)) {
# Write the response content to file
writeBin(httr2::resp_body_raw(resp), path)
result <- path
} else {
# Read CSV content
content_text <- httr2::resp_body_string(resp)
result <- readr::read_csv(content_text, show_col_types = FALSE)
}

return(result)
}