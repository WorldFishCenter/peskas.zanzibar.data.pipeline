#' Pre-process WorldFish Zanzibar Gleaning Surveys
#'
#' Downloads raw structured gleaning survey data from cloud storage and
#' preprocesses it into a single analysis-ready data frame. The function
#' assembles three pieces and joins them on the submission:
#' \enumerate{
#'   \item \strong{General info} -- strips the Kobo group prefixes
#'         (`group_general/`, `group_trip/`, `no_fishers/`, `demographics/`,
#'         `group_gleaning_activity/`, `group_supply_chain/`), selects and
#'         renames the trip, demographic, activity and supply-chain fields,
#'         coalesces the conditional `landing_site` columns into one, and
#'         coerces dates and numeric fields.
#'   \item \strong{Catch info} -- reshapes the wide `group_catch` block into a
#'         tidy long table (one row per submission x shell group x size class)
#'         via \code{\link{reshape_gleaning_catch}}, unifying the parallel
#'         bucket/plastic container fields and applying conservative
#'         sanitisation.
#'   \item \strong{Catch totals} -- per submission, sums individuals across
#'         size classes (`total_individuals`) and reconstructs catch weight as
#'         `unit_weight_kg * n_containers` (`total_catch_kg`); the container
#'         fields are constant within a submission, hence `first()`.
#' }
#'
#' Configurations are read from `config.yml` with the following necessary
#' parameters:
#'
#' ```
#' surveys:
#'   wf_gleaning:
#'     raw:
#'       file_prefix:
#'       version:
#' storage:
#'   google:
#'     key:
#'     options:
#'       project:
#'       bucket:
#'       service_account_key:
#' ```
#'
#' The function uses logging to track progress.
#'
#' @param log_threshold Logging threshold level (default: `logger::DEBUG`).
#' @return A data frame of preprocessed gleaning surveys: one row per
#'   submission x shell group x size class, with general/demographic/activity/
#'   supply-chain fields plus `total_individuals` and `total_catch_kg`.
#' @export
#' @keywords workflow preprocessing
#' @seealso \code{\link{reshape_gleaning_catch}}, \code{\link{sanitize_gleaning_inputs}}
preprocess_wf_gleaning <- function(log_threshold = logger::DEBUG) {
  logger::log_threshold(log_threshold)
  conf <- read_config()

  raw_dat <- coasts::download_parquet_from_cloud(
    prefix = conf$surveys$wf_gleaning$raw$file_prefix,
    provider = conf$storage$google$key,
    options = conf$storage$google$options,
    version = conf$surveys$wf_gleaning$raw$version
  )

  general_info <-
    raw_dat %>%
    dplyr::rename_with(~ stringr::str_remove(., "group_general/")) %>%
    dplyr::rename_with(~ stringr::str_remove(., "group_trip/")) %>%
    dplyr::rename_with(~ stringr::str_remove(., "no_fishers/")) %>%
    dplyr::rename_with(~ stringr::str_remove(., "demographics/")) %>%
    dplyr::rename_with(~ stringr::str_remove(., "group_gleaning_activity/")) %>%
    dplyr::rename_with(~ stringr::str_remove(., "group_supply_chain/")) %>%
    dplyr::select(
      # general
      "submission_id",
      submitted_by = "_submitted_by",
      submission_date = "_submission_time",
      "landing_date",
      "district",
      dplyr::contains("landing_site"),
      "collect_data_today",
      survey_activity = "gleaners_collected",
      # demographics
      fisher_name = "gleaner_name",
      "gender",
      "age",
      "education",
      # activity
      "days_collection_week",
      trip_duration = "hours_collection",
      fishing_ground = "collection_area",
      "habitat",
      "transport",
      vessel_type = "vessel",
      "vessel_status",
      "vessel_cost",
      fuel_L = "fuel",
      propulsion_gear = "propulsion",
      dplyr::ends_with("_n"),
      "gear",
      catch_outcome = "collect_data_today",
      # supply chain
      "conservation",
      "catch_use",
      "market",
      "who_selling",
      "selling_time",
      "home_consumption",
      catch_price = "revenue",
      happiness_rating = "happiness"
    ) %>%
    dplyr::mutate(
      landing_site = dplyr::coalesce(
        !!!dplyr::select(., dplyr::contains("landing_site"))
      )
    ) |>
    dplyr::select(-dplyr::contains("landing_site"), "landing_site") |>
    dplyr::relocate("landing_site", .after = "district") |>
    dplyr::mutate(
      landing_date = lubridate::as_date(.data$landing_date),
      submission_date = lubridate::as_date(.data$submission_date),
      dplyr::across(
        c(
          dplyr::contains("days_collection_week"),
          "trip_duration",
          "catch_price",
          dplyr::ends_with("_n")
        ),
        ~ as.double(.x)
      )
    )

  catch_info <-
    raw_dat %>%
    dplyr::rename_with(~ stringr::str_remove(., "group_general/")) |>
    dplyr::rename_with(~ stringr::str_remove(., "group_trip/")) |>
    dplyr::rename_with(~ stringr::str_remove(., "no_fishers/")) |>
    dplyr::rename_with(~ stringr::str_remove(., "demographics/")) |>
    dplyr::select(
      "submission_id",
      "collect_data_today",
      survey_activity = "gleaners_collected",
      catch_outcome = "collect_data_today",
      dplyr::contains("group_catch")
    ) |>
    reshape_gleaning_catch()

  catch_totals <-
    catch_info |>
    dplyr::group_by(.data$submission_id) |>
    dplyr::summarise(
      total_individuals = sum(.data$n_individuals, na.rm = TRUE),
      total_catch_kg = dplyr::first(.data$unit_weight_kg) *
        dplyr::first(.data$n_containers),
      .groups = "drop"
    )

  gleaning <- general_info |>
    dplyr::left_join(
      catch_info,
      by = c("submission_id", "catch_outcome", "survey_activity")
    ) |>
    dplyr::left_join(catch_totals, by = "submission_id") |>
    #fix fields
    dplyr::mutate(
      size_class = as.character(.data$size_class),
      happiness_rating = as.integer(.data$happiness_rating)
    )

  coasts::upload_parquet_to_cloud(
    data = gleaning,
    prefix = conf$surveys$wf_gleaning$preprocessed$file_prefix,
    provider = conf$storage$google$key,
    options = conf$storage$google$options
  )
}
