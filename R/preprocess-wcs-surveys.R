#' Pre-process Zanzibar WCS surveys
#'
#' Downloads raw structured data from cloud storage services and pre-processes
#' into a binary format that is easier to deal with in R. During the pre-processing
#' phase, multiple columns in the survey data, which can become very wide due to
#' multiple recordings of similar information (e.g., species information), are nested
#' using a set of utility functions (`pt_nest_trip`, `pt_nest_catch`,
#' `pt_nest_length`, `pt_nest_market`, `pt_nest_attachments`).
#'
#' Nesting these columns helps in reducing the width of the dataframe and organizes
#' related columns into a single nested tibble column, thus simplifying subsequent
#' analysis and visualization tasks.#'
#'
#' This function downloads the landings data from a given version (specified in
#' the config file `conf.yml`.The parameters needed are:
#'
#' ```
#' surveys:
#'   wcs_surveys:
#'     asset_id:
#'     username:
#'     password:
#'     file_prefix:
#'   version:
#'     preprocess:
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
#' @inheritParams ingest_wcs_surveys
#' @keywords workflow
#' @return no outputs. This function is used for it's side effects
#' @seealso \code{\link[=pt_nest_trip]{pt_nest_trip}}, \code{\link[=pt_nest_catch]{pt_nest_catch}},
#'   \code{\link[=pt_nest_length]{pt_nest_length}}, \code{\link[=pt_nest_market]{pt_nest_market}},
#'   \code{\link[=pt_nest_attachments]{pt_nest_attachments}}
#' @export
#'
preprocess_wcs_surveys <- function(log_threshold = logger::DEBUG) {
  logger::log_threshold(log_threshold)

  pars <- read_config()

  wcs_surveys_csv <- cloud_object_name(
    prefix = pars$surveys$wcs_surveys$file_prefix,
    provider = pars$storage$google$key,
    extension = "csv",
    version = pars$surveys$wcs_surveys$version$preprocess,
    options = pars$storage$google$options
  )

  logger::log_info("Retrieving {wcs_surveys_csv}")
  download_cloud_file(
    name = wcs_surveys_csv,
    provider = pars$storage$google$key,
    options = pars$storage$google$options
  )

  catch_surveys_raw <- readr::read_csv(
    file = wcs_surveys_csv,
    col_types = readr::cols(.default = readr::col_character())
  )

  other_info <-
    catch_surveys_raw %>%
    dplyr::select(
      .data$`_id`,
      .data$today,
      .data$start,
      .data$end,
      .data$survey_real,
      .data$survey_type,
      .data$landing_site,
      .data$trip_info,
      .data$people,
      .data$boats_landed
    )

  logger::log_info("Nesting survey groups' fields")
  group_surveys <-
    list(
      survey_trip = pt_nest_trip(catch_surveys_raw),
      other_info = other_info,
      survey_catch = pt_nest_catch(catch_surveys_raw),
      survey_length = pt_nest_length(catch_surveys_raw),
      survey_market = pt_nest_market(catch_surveys_raw),
      survey_attachments = pt_nest_attachments(catch_surveys_raw)
    )

  wcs_surveys_nested <- purrr::reduce(
    group_surveys,
    ~ dplyr::left_join(.x, .y, by = "_id")
  )

  preprocessed_filename <- pars$surveys$wcs_surveys$preprocessed_surveys$file_prefix %>%
    add_version(extension = "rds")

  readr::write_rds(
    x = wcs_surveys_nested,
    file = preprocessed_filename,
    compress = "gz"
  )

  logger::log_info("Uploading {preprocessed_filename} to cloud sorage")
  upload_cloud_file(
    file = preprocessed_filename,
    provider = pars$storage$google$key,
    options = pars$storage$google$options
  )
}
