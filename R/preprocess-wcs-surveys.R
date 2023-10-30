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

  logger::log_info("Nesting survey groups' fields")
  group_surveys <-
    list(
      survey_trip = pt_nest_trip(catch_surveys_raw),
      survey_catch = pt_nest_catch(catch_surveys_raw),
      survey_length = pt_nest_length(catch_surveys_raw),
      survey_market = pt_nest_market(catch_surveys_raw),
      survey_attachments = pt_nest_attachments(catch_surveys_raw)
    )

  wcs_surveys_nested <- purrr::reduce(
    group_surveys,
    ~ dplyr::left_join(.x, .y, by = "_id")
  )

  preprocessed_filename <- paste(pars$surveys$wcs_surveys$file_prefix,
    "preprocessed",
    sep = "_"
  ) %>%
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
