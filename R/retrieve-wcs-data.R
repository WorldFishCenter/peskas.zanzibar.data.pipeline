retrieve_wcs_surveys <- function(prefix = NULL,
                                 file_format = NULL,
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
  data_filename <- paste(prefix, "raw", sep = "_")

  if (isTRUE(append_version)) {
    csv_filename <- add_version(data_filename, "csv")
    rds_filename <- add_version(data_filename, "rds")
  }

  filenames <- character()
  if ("csv" %in% file_format) {
    logger::log_info("Converting json data to CSV as {csv_filename}...")
    readr::write_csv(tabular_data, csv_filename)
    filenames <- c(filenames, csv_filename)
  }

  if ("rds" %in% file_format) {
    logger::log_info("Converting json data to RDS as {rds_filename}...")
    readr::write_rds(tabular_data, rds_filename)
    filenames <- c(filenames, rds_filename)
  }
  filenames
}


flatten_row <- function(x) {
  x %>%
    # Each row is composed of several fields
    purrr::imap(flatten_field) %>%
    rlang::squash() %>%
    tibble::as_tibble()
}

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

# Appends parent name or number to element
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
