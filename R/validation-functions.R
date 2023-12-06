#' Validate surveys' temporal parameters
#'
#' This function takes a preprocessed landings' matrix and validate temporal
#' info associated to each survey.
#'
#' @param data A preprocessed data frame
#' @param hrs_max Upper threshold of fishing trip duration.
#' @param hrs_min Lower threshold of fishing trip duration.
#'
#' @return A list containing data frames with validated catch dates and catch
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
validate_surveys_time <- function(data, hrs_max = NULL, hrs_min) {
  validated_time <- list(
    validated_dates = data %>%
      dplyr::select(.data$`_id`, date = .data$today) %>%
      dplyr::mutate(
        date = lubridate::with_tz(.data$date, "Africa/Dar_es_Salaam"),
        date = as.Date(date)
      ),
    validated_duration = data %>%
      dplyr::select(.data$`_id`, .data$fishing_duration) %>%
      dplyr::mutate(fishing_duration = abs(as.numeric(.data$fishing_duration))) %>%
      dplyr::transmute(
        trip_duration = dplyr::case_when(
          .data$fishing_duration > hrs_max |
            .data$fishing_duration < hrs_min ~ NA_real_,
          TRUE ~ .data$fishing_duration
        ), # test if catch duration is longer than n hours or minor than 1 hour
        alert_number = dplyr::case_when(
          .data$fishing_duration > hrs_max |
            .data$fishing_duration < hrs_min ~ 5,
          TRUE ~ NA_real_
        ),
        submission_id = as.integer(.data$`_id`)
      )
  )
  validated_time
}
