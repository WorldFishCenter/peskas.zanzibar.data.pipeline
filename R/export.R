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

  validated_surveys <- get_validated_surveys(pars, sources = "wf")

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
    dplyr::mutate(
      family = ifelse(.data$catch_prop < 5, "Others", .data$family)
    ) |>
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
    monthly_metrics_tidy = pars$storage$mongodb$pipeline$collection$monthly_metrics,
    gear_metrics_tidy = pars$storage$mongodb$pipeline$collection$gear_metrics,
    taxa_proportion = pars$storage$mongodb$pipeline$collection$taxa
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
        db_name = pars$storage$mongodb$pipeline$database_name
      )
    }
  )
}

#' Export WorldFish Survey Data
#'
#' @description
#' Processes validated survey data from WorldFish sources, filtering out flagged submissions
#' and generating two key datasets:
#' 1. Indicators dataset with aggregated catch metrics
#' 2. Taxa dataset with species-specific catch information
#'
#' @details
#' The function performs the following operations:
#' - Retrieves validated WF survey data
#' - Pulls submission flags from MongoDB
#' - Filters out submissions with alert flags
#' - For the indicators dataset:
#'   - Aggregates catch data by submission
#'   - Calculates price per kg, CPUE, and RPUE metrics
#' - For the taxa dataset:
#'   - Preserves taxonomic information
#'   - Calculates catch metrics by species
#'
#' The metrics calculated include:
#' - Total catch weight per submission
#' - Price per kg of catch
#' - CPUE (Catch Per Unit Effort)
#' - RPUE (Revenue Per Unit Effort)
#'
#' @param log_threshold The logging level threshold for the logger package (e.g., DEBUG, INFO)
#'   See `logger::log_levels` for available options.
#'
#' @return Two data frames (invisible):
#'   - indicators_df: Aggregated catch metrics by submission
#'   - taxa_df: Species-specific catch information
#'
#' @examples
#' \dontrun{
#' # Export WF data with default debug logging
#' export_wf_data()
#'
#' # Export with info-level logging only
#' export_wf_data(logger::INFO)
#' }
#'
#' @seealso
#' * [get_validated_surveys()] for details on the input data format
#' * [mdb_collection_pull()] for retrieving flag information
#' * [export_data()] for the more comprehensive export function
#'
#' @keywords workflow export
#' @export
export_wf_data <- function(log_threshold = logger::DEBUG) {
  logger::log_threshold(log_threshold)
  pars <- read_config()
  metadata_tables <- get_metadata()

  validated_surveys <-
    get_validated_surveys(pars, sources = "wf") |>
    dplyr::select(-"source")

  # Choose a parallelization strategy - adjust the number of workers as needed
  future::plan(
    strategy = future::multisession,
    workers = future::availableCores() - 2
  ) # Use 4 parallel workers

  # get validation table
  valid_ids <-
    unique(validated_surveys$submission_id) %>%
    furrr::future_map_dfr(
      get_validation_status,
      asset_id = pars$surveys$wf_surveys$asset_id,
      token = pars$surveys$wf_surveys$token,
      .options = furrr::furrr_options(seed = TRUE)
    ) |>
    dplyr::filter(.data$validation_status == "validation_status_approved") |>
    dplyr::pull(.data$submission_id) |>
    unique()

  clean_data <-
    validated_surveys |>
    dplyr::filter(.data$submission_id %in% valid_ids)

  indicators_df <-
    clean_data |>
    dplyr::filter(.data$collect_data_today == "1") |>
    dplyr::mutate(
      n_fishers = .data$no_men_fishers +
        .data$no_women_fishers +
        .data$no_child_fishers
    ) |>
    dplyr::select(
      "submission_id",
      "landing_date",
      "district",
      "landing_site",
      "habitat",
      "gear",
      "vessel_type",
      "propulsion_gear",
      "fuel_L",
      "trip_duration",
      "vessel_type",
      "n_fishers",
      "catch_taxon",
      "catch_price",
      "catch_kg"
    ) |>
    dplyr::group_by(.data$submission_id) |>
    dplyr::summarise(
      dplyr::across(
        .cols = c(
          "landing_date",
          "district",
          "landing_site",
          "habitat",
          "gear",
          "vessel_type",
          "propulsion_gear",
          "fuel_L",
          "trip_duration",
          "vessel_type",
          "n_fishers",
          "catch_price"
        ),
        ~ dplyr::first(.x)
      ),
      tot_catch_kg = sum(.data$catch_kg),
      catch_taxon = paste(unique(.data$catch_taxon), collapse = "-")
    ) |>
    dplyr::mutate(
      catch_price = .data$catch_price,
      price_kg = .data$catch_price / .data$tot_catch_kg,
      cpue = .data$tot_catch_kg / .data$n_fishers / .data$trip_duration,
      rpue = .data$catch_price / .data$n_fishers / .data$trip_duration
    )

  taxa_df <-
    clean_data |>
    dplyr::filter(.data$collect_data_today == "1") |>
    dplyr::mutate(
      n_fishers = .data$no_men_fishers +
        .data$no_women_fishers +
        .data$no_child_fishers
    ) |>
    dplyr::select(
      "submission_id",
      "landing_date",
      "district",
      "landing_site",
      "habitat",
      "gear",
      "vessel_type",
      "propulsion_gear",
      "fuel_L",
      "trip_duration",
      "vessel_type",
      "n_fishers",
      "catch_taxon",
      "length",
      "catch_price",
      "catch_kg"
    ) |>
    dplyr::group_by(.data$submission_id) |>
    dplyr::mutate(
      n_catch = dplyr::n(),
      .groups = "drop"
    ) |>
    dplyr::group_by(.data$submission_id, .data$catch_taxon) |>
    dplyr::summarise(
      dplyr::across(
        .cols = c(
          "n_catch",
          "landing_date",
          "district",
          "landing_site",
          "habitat",
          "gear",
          "vessel_type",
          "propulsion_gear",
          "fuel_L",
          "trip_duration",
          "vessel_type",
          "n_fishers",
          "catch_price"
        ),
        ~ dplyr::first(.x)
      ),
      lenght = mean(.data$length),
      tot_catch_kg = sum(.data$catch_kg),
      .groups = "drop"
    ) |>
    dplyr::relocate("n_catch", .after = "submission_id")

  monthly_summaries <-
    indicators_df |>
    dplyr::mutate(
      date = lubridate::floor_date(.data$landing_date, "month"),
      date = lubridate::as_datetime(.data$date)
    ) |>
    dplyr::group_by(.data$district, .data$date) |>
    dplyr::summarise(
      dplyr::across(
        .cols = c(
          "cpue",
          "rpue",
          "price_kg"
        ),
        ~ mean(.x, na.rm = TRUE)
      ),
      .groups = "drop"
    ) |>
    dplyr::rename(
      mean_cpue = "cpue",
      mean_rpue = "rpue",
      mean_price_kg = "price_kg"
    ) |>
    tidyr::complete(
      .data$district,
      date = seq(min(.data$date), max(.data$date), by = "month"),
      fill = list(
        mean_cpue = NA,
        mean_rpue = NA,
        mean_price_kg = NA
      )
    ) |>
    tidyr::pivot_longer(
      -c("date", "district"),
      names_to = "metric",
      values_to = "value"
    ) |>
    dplyr::mutate(
      district = stringr::str_to_title(.data$district),
      district = stringr::str_replace(.data$district, "_", " ")
    )

  taxa_summaries <-
    taxa_df |>
    dplyr::group_by(.data$district, .data$catch_taxon) |>
    dplyr::summarise(
      catch_kg = sum(.data$tot_catch_kg, na.rm = T),
      catch_price = sum(.data$catch_price, na.rm = T),
      mean_length = mean(.data$lenght, na.rm = T),
      .groups = "drop"
    ) |>
    dplyr::rename(alpha3_code = "catch_taxon") |>
    dplyr::left_join(metadata_tables$catch_type, by = "alpha3_code") |>
    dplyr::select(
      "district",
      "common_name",
      "catch_kg",
      "catch_price",
      "mean_length"
    ) |>
    tidyr::complete(
      .data$district,
      .data$common_name,
      fill = list(
        catch_kg = 0,
        catch_price = NA,
        length = NA
      )
    ) |>
    dplyr::mutate(price_kg = .data$catch_price / .data$catch_kg) |>
    dplyr::select(-c("catch_price")) |>
    tidyr::pivot_longer(
      -c("district", "common_name"),
      names_to = "metric",
      values_to = "value"
    ) |>
    dplyr::mutate(
      district = stringr::str_to_title(.data$district),
      district = stringr::str_replace(.data$district, "_", " ")
    )

  districts_summaries <-
    indicators_df |>
    dplyr::group_by(.data$district) |>
    dplyr::summarise(
      n_submissions = dplyr::n(),
      n_fishers = mean(.data$n_fishers),
      trip_duration = mean(.data$trip_duration),
      mean_cpue = mean(.data$cpue, na.rm = TRUE),
      mean_rpue = mean(.data$rpue, na.rm = TRUE),
      mean_price_kg = mean(.data$price_kg, na.rm = TRUE),
      .groups = "drop"
    ) |>
    tidyr::pivot_longer(
      -"district",
      names_to = "indicator",
      values_to = "value"
    ) |>
    dplyr::mutate(
      district = stringr::str_to_title(.data$district),
      district = stringr::str_replace(.data$district, "_", " ")
    )

  gear_summaries <-
    indicators_df |>
    dplyr::group_by(.data$district, .data$gear) |>
    dplyr::summarise(
      n_submissions = dplyr::n(),
      cpue = mean(.data$cpue, na.rm = T),
      rpue = mean(.data$rpue),
      .groups = "drop"
    ) |>
    dplyr::mutate(
      gear = dplyr::case_when(
        .data$gear == "HL" ~ "Hand line",
        .data$gear == "BS" ~ "Beach Seine",
        .data$gear == "CS" ~ "Cast Net",
        .data$gear == "GN" ~ "Gill Net",
        .data$gear == "LL" ~ "Long line",
        .data$gear == "SP" ~ "Spear gun",
        .data$gear == "SR" ~ "Stick Rod",
        .data$gear == "PS" ~ "Purse Seine",
        .data$gear == "RN" ~ "Ring Net",
        .data$gear == "SN" ~ "Shark Net",
        .data$gear == "TR" ~ "Trap",
        TRUE ~ .data$gear # Keep original value if no match
      )
    ) |>
    tidyr::pivot_longer(
      -c("district", "gear"),
      names_to = "indicator",
      values_to = "value"
    ) |>
    dplyr::mutate(
      district = stringr::str_to_title(.data$district),
      district = stringr::str_replace(.data$district, "_", " ")
    )

  # Dataframes to upload
  dataframes_to_upload <- list(
    monthly_summaries = monthly_summaries,
    taxa_summaries = taxa_summaries,
    districts_summaries = districts_summaries,
    gear_summaries = gear_summaries
  )

  # Collection names
  collection_names <- list(
    monthly_summaries = pars$storage$mongodb$portal$collection$monthly_summaries,
    taxa_summaries = pars$storage$mongodb$portal$collection$taxa_summaries,
    districts_summaries = pars$storage$mongodb$portal$collection$districts_summaries,
    gear_summaries = pars$storage$mongodb$portal$collection$gear_summaries
  )

  # Iterate over the dataframes and upload them
  purrr::walk2(
    .x = dataframes_to_upload,
    .y = collection_names,
    .f = ~ {
      logger::log_info(paste("Uploading", .y, "data to MongoDB"))
      mdb_collection_push(
        data = .x,
        connection_string = pars$storage$mongodb$connection_string,
        collection_name = .y,
        db_name = pars$storage$mongodb$portal$database_name
      )
    }
  )
}
