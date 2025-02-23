#' Export Processed Fisheries Data to MongoDB
#'
#' @description
#' Processes validated survey data to generate summary metrics and exports them to MongoDB collections.
#' The function calculates three main types of metrics:
#' 1. Monthly CPUE and RPUE metrics by landing site
#' 2. Gear-specific metrics by landing site and habitat
#' 3. Taxa proportions by landing site
#'
#' @details
#' The function performs the following operations:
#' - Filters out landing sites with >75% missing CPUE data
#' - Calculates monthly median CPUE and RPUE values
#' - Processes gear-specific metrics, excluding gears with spaces in names and "other" category
#' - Computes taxa proportions, grouping taxa with <5% representation as "Others"
#' - Uploads processed data to specified MongoDB collections
#'
#' The metrics calculated include:
#' - CPUE (Catch Per Unit Effort)
#' - RPUE (Revenue Per Unit Effort)
#' - Catch proportions by taxa
#'
#' Exported collections:
#' - Monthly metrics: Time series of CPUE and RPUE by landing site
#' - Gear metrics: CPUE and RPUE by gear type, habitat, and landing site
#' - Taxa proportions: Percentage contribution of each taxa to total catch by landing site
#'
#' @param log_threshold The logging level threshold for the logger package (e.g., DEBUG, INFO)
#'   See `logger::log_levels` for available options.
#'
#' @return None (invisible). The function performs its operations for side effects:
#'   - Processes survey data into summary metrics
#'   - Uploads results to MongoDB collections
#'   - Generates logs of the process
#'
#' @examples
#' \dontrun{
#' # Export data with default debug logging
#' export_data()
#'
#' # Export with info-level logging only
#' export_data(logger::INFO)
#' }
#'
#' @seealso
#' * [get_validated_surveys()] for details on the input data format
#' * [mdb_collection_push()] for details on the MongoDB upload process
#' * [expand_taxa()] for details on taxa classification
#'
#' @keywords workflow export
#' @export
export_data <- function(log_threshold = logger::DEBUG) {
  logger::log_threshold(log_threshold)
  pars <- read_config()

  validated_surveys <- get_validated_surveys(pars)

  # Get unique landing sites first for nesting
  landing_sites <- validated_surveys %>%
    dplyr::pull(.data$landing_site) %>%
    unique()

  monthly_metrics <-
    validated_surveys |>
    dplyr::mutate(date = lubridate::floor_date(.data$landing_date, "month")) |>
    dplyr::group_by(.data$date, .data$landing_site) |>
    dplyr::summarise(
      median_cpue = stats::median(.data$cpue, na.rm = TRUE),
      median_rpue = stats::median(.data$rpue, na.rm = TRUE),
      n = dplyr::n(),
      .groups = "drop"
    ) |>
    dplyr::ungroup() |>
    tidyr::complete(
      date,
      landing_site = landing_sites,
      fill = list(median_cpue = NA_real_, median_rpue = NA_real_, n = 0)
    )

  sites_selected <-
    monthly_metrics %>%
    dplyr::group_by(.data$landing_site) %>%
    dplyr::summarise(
      missing_cpue = mean(is.na(.data$median_cpue)) * 100,
      .groups = "drop"
    ) %>%
    dplyr::arrange(dplyr::desc(.data$missing_cpue)) %>%
    dplyr::filter(.data$missing_cpue < 75) %>%
    dplyr::pull(.data$landing_site)

  monthly_metrics_tidy <-
    monthly_metrics |>
    dplyr::filter(.data$landing_site %in% sites_selected) |>
    dplyr::mutate(date = lubridate::as_datetime(.data$date)) |>
    tidyr::pivot_longer(
      -c("date", "landing_site", "n"), 
      names_to = "metric", 
      values_to = "value"
    )

  gear_metrics_tidy <-
    validated_surveys |>
    dplyr::group_by(.data$landing_site, .data$habitat, .data$gear) |>
    dplyr::filter(
      !is.na(.data$gear), 
      !stringr::str_detect(.data$gear, " "), 
      !.data$gear == "other"
    ) |>
    dplyr::summarise(
      median_cpue = stats::median(.data$cpue, na.rm = TRUE),
      median_rpue = stats::median(.data$rpue, na.rm = TRUE),
      n = dplyr::n(),
      .groups = "drop"
    ) |>
    tidyr::pivot_longer(
      -c("landing_site", "habitat", "gear", "n"), 
      names_to = "metric", 
      values_to = "value"
    )

  taxa_proportion <-
    validated_surveys |>
    dplyr::filter(.data$landing_site %in% sites_selected) |>
    dplyr::select("survey_id", "landing_site", "catch_taxa") |>
    tidyr::unnest("catch_taxa") |>
    expand_taxa() |>
    dplyr::select("survey_id", "landing_site", "family", "catch_kg") |>
    dplyr::group_by(.data$survey_id, .data$landing_site) |>
    dplyr::mutate(n_catches = dplyr::n()) |>
    dplyr::rowwise() |>
    dplyr::mutate(taxa_catch_kg = .data$catch_kg / .data$n_catches) |>
    dplyr::select(-c("catch_kg", "n_catches")) |>
    dplyr::group_by(.data$landing_site, .data$family) |>
    dplyr::summarise(
      total_catch_kg = sum(.data$taxa_catch_kg),
      .groups = "drop"
    ) |>
    dplyr::filter(!is.na(.data$family)) |>
    dplyr::group_by(.data$landing_site) |>
    dplyr::mutate(
      overall_catch = sum(.data$total_catch_kg),
      catch_prop = (.data$total_catch_kg / .data$overall_catch) * 100
    ) |>
    dplyr::ungroup() |>
    dplyr::select(-c("total_catch_kg", "overall_catch")) |>
    tidyr::complete(
      landing_site = unique(.data$landing_site),
      family = unique(.data$family),
      fill = list(catch_prop = 0)
    ) |>
    dplyr::group_by(.data$landing_site) |>
    dplyr::mutate(family = ifelse(.data$catch_prop < 5, "Others", .data$family)) |>
    dplyr::group_by(.data$landing_site, .data$family) |>
    dplyr::summarise(
      catch_prop = sum(.data$catch_prop),
      .groups = "drop"
    )

  # Dataframes to upload
  dataframes_to_upload <- list(
    monthly_metrics_tidy = monthly_metrics_tidy,
    gear_metrics_tidy = gear_metrics_tidy,
    taxa_proportion = taxa_proportion
  )
  
  # Collection names
  collection_names <- list(
    monthly_metrics_tidy = pars$storage$mongodb$export$collection$monthly_metrics,
    gear_metrics_tidy = pars$storage$mongodb$export$collection$gear_metrics,
    taxa_proportion = pars$storage$mongodb$export$collection$taxa
  )

  # Upload data
  purrr::walk2(
    .x = dataframes_to_upload,
    .y = collection_names,
    .f = ~ {
      logger::log_info(paste("Uploading", .y, "data to MongoDB"))
      mdb_collection_push(
        data = .x,
        connection_string = pars$storage$mongodb$connection_string,
        collection_name = .y,
        db_name = pars$storage$mongodb$database_name
      )
    }
  )
}