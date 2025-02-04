#' Validate WCS Surveys Data
#'
#' Validates Wildlife Conservation Society (WCS) survey data by checking for inconsistencies in survey duration, catches, lengths, and market data. The function preprocesses surveys, performs validations, logs the process, and uploads the validated data to cloud storage.
#'
#' @param log_threshold The logging level used as a threshold for the `logger` package, which controls the verbosity of logging output.
#' @return None; the function is used for its side effects, which include data validation and uploading validated data to cloud storage.
#' @keywords workflow validation
#' @export
#' @examples
#' \dontrun{
#' validate_wcs_surveys(log_threshold = logger::INFO)
#' }
#' @seealso \code{\link{validate_catch}}, \code{\link{validate_length}}, \code{\link{validate_market}}
#'
#'
validate_wcs_surveys <- function(log_threshold = logger::DEBUG) {
  logger::log_threshold(log_threshold)

  pars <- read_config()

  preprocessed_surveys <-
    get_preprocessed_surveys(pars, prefix = pars$surveys$wcs_surveys$preprocessed_surveys$file_prefix)

  # define validation parameters
  k_max_nb <- pars$surveys$wcs_surveys$validation$K_nb_elements_max
  k_max_weight <- pars$surveys$wcs_surveys$validation$K_weight_max
  k_max_length <- pars$surveys$wcs_surveys$validation$K_length_max
  k_max_price <- pars$surveys$wcs_surveys$validation$K_price_max

  trips_info <-
    preprocessed_surveys %>%
    dplyr::select(
      "survey_id",
      "survey_type",
      "landing_site",
      "lat",
      "lon",
      "trip_length_days",
      "fishing_location",
      "fishing_ground_name",
      "habitat",
      "fishing_ground_depth",
      "gear",
      "propulsion_gear",
      "boat_engine",
      "engine_hp",
      "n_fishers",
      "n_boats"
    )

  logger::log_info("Validating dates")
  surveys_dates_alerts <- validate_dates(data = preprocessed_surveys)
  logger::log_info("Validating catches groups")
  surveys_catch_alerts <- validate_catch(data = preprocessed_surveys, k_max_nb = k_max_nb, k_max_weight = k_max_weight)
  # logger::log_info("Validating lengths group")
  # surveys_length_alerts <- validate_length(data = preprocessed_surveys, k_max_length = k_max_length)
  logger::log_info("Validating markets group")
  surveys_market_alerts <- validate_market(data = preprocessed_surveys, k_max_price = k_max_price)

  logger::log_info("Renaming data fields")
  validated_groups <-
    list(
      surveys_dates_alerts,
      surveys_catch_alerts,
      # surveys_length_alerts,
      surveys_market_alerts
    ) %>%
    purrr::map(~ dplyr::select(.x, -alert_number)) |>
    purrr::reduce(dplyr::left_join, by = "survey_id")


  validated_surveys <-
    dplyr::left_join(trips_info, validated_groups, by = "survey_id") |>
    dplyr::select("survey_id", "submission_date", dplyr::everything())

  validated_filename <-
    pars$surveys$wcs_surveys$validated_surveys$file_prefix %>%
    add_version(extension = "parquet")

  arrow::write_parquet(
    x = validated_surveys,
    sink = validated_filename,
    compression = "lz4",
    compression_level = 12
  )


  logger::log_info("Uploading {validated_filename} to cloud sorage")
  upload_cloud_file(
    file = validated_filename,
    provider = pars$storage$google$key,
    options = pars$storage$google$options
  )
}


#validate_ba_surveys <- function(){
#  logger::log_threshold(log_threshold)
#  pars <- read_config()
#  preprocessed_surveys <-
#    get_preprocessed_surveys(pars, pars$surveys$ba_surveys$preprocessed_surveys$file_prefix)
#}

#' Generate an alert vector based on the `univOutl::LocScaleB()` function
#'
#' @param x numeric vector where outliers will be checked
#' @param no_alert_value value to put in the output when there is no alert (x is within bounds)
#' @param alert_if_larger alert for when x is above the bounds found by `univOutl::LocScaleB()`
#' @param alert_if_smaller alert for when x is below the bounds found by `univOutl::LocScaleB()`
#' @param ... arguments for `univOutl::LocScaleB()`
#'
#' @return a vector of the same lenght as x
#' @importFrom stats mad
#'
#' @keywords validation
#' @export
#'
alert_outlier <- function(x,
                          no_alert_value = NA_real_,
                          alert_if_larger = no_alert_value,
                          alert_if_smaller = no_alert_value,
                          ...) {
  algo_args <- list(...)

  # Helper function to check if everything is NA or zero
  all_na_or_zero <- function(x) {
    isTRUE(all(is.na(x) | x == 0))
  }

  # If everything is NA or zero there is nothing to compute
  if (all_na_or_zero(x)) {
    return(NA_real_)
  }
  # If the median absolute deviation is zero we shouldn't be using this algo
  if (mad(x, na.rm = T) <= 0) {
    return(NA_real_)
  }
  # If weights are specified and they are all NA or zero
  if (!is.null(algo_args$weights)) {
    if (all_na_or_zero(algo_args$weights)) {
      return(NA_real_)
    }
  }

  bounds <- univOutl::LocScaleB(x, ...) %>%
    magrittr::extract2("bounds")

  if (isTRUE(algo_args$logt)) bounds <- exp(bounds) - 1

  dplyr::case_when(
    x < bounds[1] ~ alert_if_smaller,
    x > bounds[2] ~ alert_if_larger,
    TRUE ~ no_alert_value
  )
}


#' Validate Submission Dates in WCS Surveys
#'
#' This function validates the submission dates in WCS surveys. It flags any submissions with dates earlier than January 1, 2020.
#'
#' @param data Data frame containing preprocessed survey data. If NULL, the function will return NULL.
#' @return A data frame with the following columns:
#'   \item{submission_date}{Date of survey submission. Dates before 2020-01-01 are set to NA.}
#'   \item{alert_number}{Numeric. 1 if the submission date is invalid (NA), NA otherwise.}
#'   \item{survey_id}{Integer. Unique identifier for each survey_id}
#' @details
#' The function performs the following operations:
#' 1. Selects 'survey_id' and 'submission_date' columns from the input data.
#' 2. Sets submission dates before 2020-01-01 to NA.
#' 3. Creates an alert_number column: 1 for invalid dates, NA for valid dates.
#' @keywords validation
#' @export
#' @examples
#' \dontrun{
#' validated_data <- validate_dates(survey_data)
#' }
#'
validate_dates <- function(data = NULL) {
  data %>%
    dplyr::select("survey_id", "submission_date") %>%
    dplyr::transmute(
      submission_date = dplyr::case_when(.data$submission_date < "2020-01-01" ~ NA, TRUE ~ .data$submission_date),
      alert_number = ifelse(is.na(.data$submission_date), 1, NA_real_),
      survey_id = as.integer(.data$survey_id)
    )
}

#' Validate Catch Data and Detect Outliers
#'
#' This function processes catch data to identify outliers in the number of
#' elements and weight of the catch.
#' It utilizes the LocScaleB method with the MAD method for outlier detection,
#'  where `k` determines the bounds extension.
#' The function selects and transforms relevant data, groups it, and applies
#' outlier detection based on the specified `k_max_nb`
#' and `k_max_weight` thresholds.
#'
#' @param data A data frame containing catch information, including columns `_id`,
#'  `gear_type`, and `catch`.
#' @param k_max_nb Nonnegative constant used in the MAD method for determining
#' bounds in outlier detection for the number of elements.
#' Common values are 2, 2.5, and 3.
#' @param k_max_weight Nonnegative constant used in the MAD method for determining
#'  bounds in outlier detection for the weight.
#' Common values are 2, 2.5, and 3.
#'
#' @return A data frame with the original catch data, additional columns for
#' detected outliers, and nested catch data.
#' @export
#'
#' @keywords validation
#' @export
#' @examples
#' \dontrun{
#' # Assuming you have a data frame `catch_data` with the necessary structure:
#' validated_catch <- validate_catch(data = catch_data, k_max_nb = 10, k_max_weight = 100)
#' }
#'
validate_catch <- function(data = NULL, k_max_nb = NULL, k_max_weight = NULL) {
  data %>%
    dplyr::select("survey_id", "gear", "catch") %>%
    tidyr::unnest("catch") %>%
    dplyr::mutate(
      dplyr::across(c("n_elements", "catch_kg"), ~ as.numeric(.x)),
      survey_id = as.integer(.data$survey_id)
    ) %>%
    dplyr::group_by(.data$type_measure, .data$gear, .data$group_catch) %>%
    dplyr::mutate(
      alert_nb = alert_outlier(
        x = .data$n_elements,
        alert_if_larger = 2,
        logt = TRUE,
        k = k_max_nb
      ),
      n_elements = dplyr::case_when(
        is.na(.data$alert_nb) ~ .data$n_elements,
        TRUE ~ NA_real_
      ),
      alert_catch = alert_outlier(
        x = .data$catch_kg,
        alert_if_larger = 3,
        logt = TRUE,
        k = k_max_weight
      ),
      catch_kg = dplyr::case_when(
        is.na(.data$alert_catch) ~ .data$catch_kg,
        TRUE ~ NA_real_
      )
    ) %>%
    dplyr::ungroup() %>%
    dplyr::mutate(alert_number = dplyr::coalesce(.data$alert_catch, .data$alert_nb)) %>%
    tidyr::nest(
      "catch" = c(
        .data$type_measure, .data$group_catch,
        .data$species_catch, .data$n_elements, .data$catch_kg
      ),
      .by = c(.data$survey_id, .data$alert_number)
    )
}

#' Validate Length Data and Detect Outliers
#'
#' This function processes length data to identify outliers in total length
#' using the LocScaleB method with the MAD method for outlier detection.
#' The `k` parameter is used to determine the extension of bounds. The function
#'  selects and transforms data, groups it by gear type and species,
#' and applies the outlier detection using the specified `k_max_length` threshold.
#'
#' @param data A data frame containing length information, including columns `_id`,
#'  `gear_type`, and `length`.
#' @param k_max_length Nonnegative constant used in the MAD method for determining
#'  bounds in outlier detection for total length.
#' Common values are 2, 2.5, and 3.
#'
#' @return A data frame with the original length data, additional columns for
#' detected outliers, and nested length data.
#' @export
#'
#' @keywords validation
#' @export
#' @examples
#' \dontrun{
#' # Assuming you have a data frame `length_data` with the necessary structure:
#' validated_length <- validate_length(data = length_data, k_max_length = 200)
#' }
#'
validate_length <- function(data = NULL, k_max_length = NULL) {
  data %>%
    dplyr::select("survey_id", "gear", "length") %>%
    tidyr::unnest("length") %>%
    dplyr::mutate(
      total_length = as.numeric(.data$total_length),
      survey_id = as.integer(.data$survey_id)
    ) %>%
    dplyr::group_by(.data$gear, .data$species) %>%
    dplyr::mutate(
      alert_number = alert_outlier(
        x = .data$total_length,
        alert_if_larger = 4,
        logt = TRUE,
        k = k_max_length
      ),
      total_length = dplyr::case_when(
        is.na(.data$alert_number) ~ .data$total_length,
        TRUE ~ NA_real_
      )
    ) %>%
    dplyr::ungroup() %>%
    tidyr::nest(
      "length" = c(
        .data$family, .data$species,
        .data$sex, .data$total_length
      ),
      .by = c(.data$survey_id, .data$alert_number)
    )
}

#' Validate Market Data and Detect Outliers in Price Per Kilogram
#'
#' This function processes market data to identify outliers in the price per kilogram.
#'  It selects necessary columns,
#' unnests market data, and calculates the price per kilogram. The data is grouped
#'  by gear type, market group, and
#' species in the market. Outliers are detected using the `alert_outlier` function,
#'  which applies the LocScaleB method
#' for outlier detection based on the Median Absolute Deviation (MAD) method.
#' The `k` parameter is used within the MAD
#' method to determine the extension of bounds for outlier detection.
#'
#' @param data A data frame containing market information. Expected to include
#' columns `_id`, `gear_type`, and `market`.
#' @param k_max_price Nonnegative constant used in the LocScaleB function,
#' based on the MAD method, to determine the
#' extension of bounds for outlier detection in price per kilogram. Common values
#'  are 2, 2.5, and 3.
#'
#' @return A data frame with the original market data, additional columns for
#' detected outliers, and nested market data.
#'
#' @keywords validation
#' @export
#' @examples
#' \dontrun{
#' # Assuming you have a data frame `market_data` with the necessary structure:
#' validated_market <- validate_market(data = market_data, k_max_price = 100)
#' }
validate_market <- function(data = NULL, k_max_price = NULL) {
  data %>%
    dplyr::select("survey_id", "gear", "market") %>%
    tidyr::unnest("market") %>%
    dplyr::mutate(
      dplyr::across(c("catch_kg_market", "catch_price"), ~ as.numeric(.x)),
      price_kg = .data$catch_price / .data$catch_kg_market,
      survey_id = as.integer(.data$survey_id)
    ) %>%
    dplyr::group_by(.data$gear, .data$group_market, .data$species_market) %>%
    dplyr::mutate(
      alert_number = alert_outlier(
        x = .data$price_kg,
        alert_if_larger = 5,
        logt = TRUE,
        k = k_max_price
      ),
      price_kg = dplyr::case_when(
        is.na(.data$alert_number) ~ .data$price_kg,
        TRUE ~ NA_real_
      )
    ) %>%
    dplyr::ungroup() %>%
    tidyr::nest(
      "market" = c(
        .data$group_market, .data$species_market, .data$price_kg
      ),
      .by = c(.data$survey_id, .data$alert_number)
    )
}
