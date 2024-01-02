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


#' Validate surveys' fishing duration
#'
#' This function takes a preprocessed landings' matrix and validate fishing trip
#' duration associated to each survey.
#'
#' @param data A preprocessed data frame
#' @param hrs_max Upper threshold of fishing trip duration.
#' @param hrs_min Lower threshold of fishing trip duration.
#'
#' @return A list containing data frames with validated catch
#'   duration.
#'
#' @importFrom rlang .data
#' @export
#'
#' @examples
#' \dontrun{
#' pars <- read_config()
#' landings <- get_preprocessed_surveys(pars)
#' validate_surveys_time(landings, hrs_max = 72, hrs_min = 1)
#' }
validate_surveys_time <- function(data = NULL, hrs_max = NULL, hrs_min = NULL) {
  data %>%
    dplyr::select(.data$`_id`, .data$fishing_duration) %>%
    dplyr::mutate(fishing_duration = abs(as.numeric(.data$fishing_duration))) %>%
    dplyr::transmute(
      trip_duration = dplyr::case_when(
        .data$fishing_duration > hrs_max |
          .data$fishing_duration < hrs_min ~ NA_real_,
        TRUE ~ .data$fishing_duration
      ), # test if catch duration is longer than hrs_max or minor than hrs_min
      alert_number = dplyr::case_when(
        .data$fishing_duration > hrs_max |
          .data$fishing_duration < hrs_min ~ 1,
        TRUE ~ NA_real_
      ),
      submission_id = as.integer(.data$`_id`)
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
#' @examples
#' \dontrun{
#' # Assuming you have a data frame `catch_data` with the necessary structure:
#' validated_catch <- validate_catch(data = catch_data, k_max_nb = 10, k_max_weight = 100)
#' }
#'
#' @importFrom dplyr select mutate group_by ungroup case_when coalesce
#' @importFrom tidyr unnest nest
#' @importFrom magrittr %>%
validate_catch <- function(data = NULL, k_max_nb = NULL, k_max_weight = NULL) {
  data %>%
    dplyr::select("_id", "gear_type", "catch") %>%
    tidyr::unnest("catch") %>%
    dplyr::mutate(
      dplyr::across(c("nb_elements", "weight_kg"), ~ as.numeric(.x)),
      submission_id = as.integer(.data$`_id`)
    ) %>%
    dplyr::group_by(.data$type_measure, .data$gear_type, .data$group_catch) %>%
    dplyr::mutate(
      alert_nb = alert_outlier(
        x = .data$nb_elements,
        # alert_if_smaller = 1,
        alert_if_larger = 2,
        logt = TRUE,
        k = k_max_nb
      ),
      nb_elements = dplyr::case_when(
        is.na(.data$alert_nb) ~ .data$nb_elements,
        TRUE ~ NA_real_
      ),
      alert_weight = alert_outlier(
        x = .data$weight_kg,
        # alert_if_smaller = 3,
        alert_if_larger = 4,
        logt = TRUE,
        k = k_max_weight
      ),
      weight_kg = dplyr::case_when(
        is.na(.data$alert_weight) ~ .data$weight_kg,
        TRUE ~ NA_real_
      )
    ) %>%
    dplyr::ungroup() %>%
    dplyr::mutate(alert_number = dplyr::coalesce(.data$alert_weight, .data$alert_nb)) %>%
    tidyr::nest(
      "catch" = c(
        .data$type_measure, .data$group_catch,
        .data$species_catch, .data$nb_elements, .data$weight_kg
      ),
      .by = c(.data$submission_id, .data$alert_number)
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
#' @examples
#' \dontrun{
#' # Assuming you have a data frame `length_data` with the necessary structure:
#' validated_length <- validate_length(data = length_data, k_max_length = 200)
#' }
#'
#' @importFrom dplyr select mutate group_by ungroup case_when
#' @importFrom tidyr unnest nest
#' @importFrom magrittr %>%
validate_length <- function(data = NULL, k_max_length = NULL) {
  data %>%
    dplyr::select("_id", "gear_type", "length") %>%
    tidyr::unnest("length") %>%
    dplyr::mutate(
      total_length = as.numeric(.data$total_length),
      submission_id = as.integer(.data$`_id`)
    ) %>%
    dplyr::group_by(.data$gear_type, .data$species) %>%
    dplyr::mutate(
      alert_number = alert_outlier(
        x = .data$total_length,
        # alert_if_smaller = 1,
        alert_if_larger = 2,
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
      .by = c(.data$submission_id, .data$alert_number)
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
#' @export
#'
#' @examples
#' \dontrun{
#' # Assuming you have a data frame `market_data` with the necessary structure:
#' validated_market <- validate_market(data = market_data, k_max_price = 100)
#' }
#'
#' @importFrom dplyr select mutate group_by ungroup case_when
#' @importFrom tidyr unnest nest
#' @importFrom magrittr %>%
validate_market <- function(data = NULL, k_max_price = NULL) {
  data %>%
    dplyr::select("_id", "gear_type", "market") %>%
    tidyr::unnest("market") %>%
    dplyr::mutate(
      dplyr::across(c("weight_market", "price_sold_for"), ~ as.numeric(.x)),
      price_kg = .data$price_sold_for / .data$weight_market,
      submission_id = as.integer(.data$`_id`)
    ) %>%
    dplyr::group_by(.data$gear_type, .data$group_market, .data$species_market) %>%
    dplyr::mutate(
      alert_number = alert_outlier(
        x = .data$price_kg,
        # alert_if_smaller = 1,
        alert_if_larger = 2,
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
      .by = c(.data$submission_id, .data$alert_number)
    )
}
