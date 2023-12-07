#' Validate WCS Surveys Data
#'
#' This function validates Wildlife Conservation Society (WCS) survey data. It reads
#'  configuration parameters, preprocesses surveys,
#' and performs various validations on survey duration, catches, lengths, and market data.
#'  It also logs the process at specified
#' log thresholds. The function consolidates validated data and saves it as an RDS file,
#'  which is then uploaded to cloud storage.
#'
#' @param log_threshold A log level threshold from the `logger` package, used to
#'  set the minimum level of log messages to be captured.
#' @importFrom logger log_threshold log_info
#' @importFrom dplyr select left_join mutate
#' @importFrom purrr map reduce
#' @importFrom lubridate with_tz
#' @importFrom readr write_rds
#'
#' @return No return value; this function is called for its side effects,
#' including data validation, file creation, and cloud uploading.
#' @export
#'
#' @examples
#' # Assuming necessary configuration and data are available:
#' \dontrun{
#' validate_wcs_surveys(log_threshold = logger::INFO)
#' }
#' @seealso \code{\link{validate_catch}}, \code{\link{validate_length}}, \code{\link{validate_market}}
validate_wcs_surveys <- function(log_threshold = logger::DEBUG) {
  logger::log_threshold(log_threshold)

  pars <- read_config()
  preprocessed_surveys <- get_preprocessed_surveys(pars)

  # define validation parameters
  hrs_min <- pars$surveys$wcs_surveys$validation$duration_min
  hrs_max <- pars$surveys$wcs_surveys$validation$duration_max
  k_max_nb <- pars$surveys$wcs_surveys$validation$K_nb_elements_max
  k_max_weight <- pars$surveys$wcs_surveys$validation$K_weight_max
  k_max_length <- pars$surveys$wcs_surveys$validation$K_length_max
  k_max_price <- pars$surveys$wcs_surveys$validation$K_price_max

  surveys_duration_alerts <- validate_surveys_time(data = preprocessed_surveys, hrs_min = hrs_min, hrs_max = hrs_max)
  logger::log_info("Validating catches groups")
  surveys_catch_alerts <- validate_catch(data = preprocessed_surveys, k_max_nb = k_max_nb, k_max_weight = k_max_weight)
  logger::log_info("Validating lengths group")
  surveys_length_alerts <- validate_length(data = preprocessed_surveys, k_max_length = k_max_length)
  logger::log_info("Validating markets group")
  surveys_market_alerts <- validate_market(data = preprocessed_surveys, k_max_price = k_max_price)

  logger::log_info("Renaming data fields")
  validated_groups <-
    list(
      surveys_duration_alerts,
      surveys_catch_alerts,
      surveys_length_alerts,
      surveys_market_alerts
    ) %>%
    purrr::map(~ dplyr::select(.x, -alert_number)) %>%
    purrr::reduce(dplyr::left_join, by = "submission_id")

  trips_info <-
    preprocessed_surveys %>%
    dplyr::mutate(
      submission_id = as.integer(.data$`_id`),
      date = lubridate::with_tz(.data$today, "Africa/Dar_es_Salaam"),
      date = as.Date(date)
    ) %>%
    dplyr::select(
      .data$submission_id, .data$date, .data$survey_real, .data$survey_type,
      .data$landing_site, .data$fishing_location, .data$fishing_ground_name,
      .data$fishing_ground_type, .data$fishing_ground_depth, .data$gear_type,
      .data$gear_type, .data$boat_type, .data$engine_yn, .data$engine,
      .data$boats_landed
    )

  validated_surveys <-
    dplyr::left_join(trips_info, validated_groups, by = "submission_id")

  validated_filename <-
    pars$surveys$wcs_surveys$validated_surveys$file_prefix %>%
    add_version(extension = "rds")

  readr::write_rds(
    x = validated_surveys,
    file = validated_filename,
    compress = "gz"
  )

  logger::log_info("Uploading {validated_filename} to cloud sorage")
  upload_cloud_file(
    file = validated_filename,
    provider = pars$storage$google$key,
    options = pars$storage$google$options
  )
}
