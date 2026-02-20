#' Summarize WorldFish Survey Data
#'
#' @description
#' Processes validated survey data from WorldFish sources, filtering out flagged submissions
#' and generating summary datasets for various dimensions:
#' - Monthly summaries with aggregated catch metrics
#' - Taxa summaries with species-specific information
#' - District summaries with submission and effort metrics
#' - Gear summaries with gear-specific performance metrics
#' - Grid summaries from vessel tracking data
#'
#' @details
#' The function performs the following operations:
#' - Retrieves validated WF survey data
#' - Filters for approved validation status
#' - Creates multiple summary datasets:
#'   - Monthly summaries: Average catch, price, CPUE, and RPUE by district and month
#'   - Taxa summaries: Catch metrics by species, district, and month
#'   - District summaries: Submission counts and effort metrics by district
#'   - Gear summaries: Performance metrics by gear type
#'   - Grid summaries: Downloaded from cloud storage
#' - Uploads all summaries to cloud storage as versioned parquet files
#'
#' The metrics calculated include:
#' - Total and mean catch weight
#' - Price per kg of catch
#' - CPUE (Catch Per Unit Effort) - both hourly and daily
#' - RPUE (Revenue Per Unit Effort) - both hourly and daily
#' - Number of submissions and fishers
#' - Trip duration
#'
#' @param log_threshold The logging level threshold for the logger package (e.g., DEBUG, INFO)
#'   See `logger::log_levels` for available options.
#'
#' @return NULL (invisible). The function uploads summary files to cloud storage as a side effect.
#'
#' @examples
#' \dontrun{
#' # Summarize WF data with default debug logging
#' summarize_data()
#'
#' # Summarize with info-level logging only
#' summarize_data(logger::INFO)
#' }
#'
#' @seealso
#' * [get_validated_surveys()] for details on the input data format
#' * [get_validation_status()] for retrieving validation information
#' * [upload_cloud_file()] for uploading results to cloud storage
#' * [download_parquet_from_cloud()] for retrieving grid summaries
#'
#' @keywords workflow pipeline mining
#' @export
summarize_data <- function(log_threshold = logger::DEBUG) {
  logger::log_threshold(log_threshold)
  pars <- read_config()

  # Input: validated trip data (catch-level rows, one row per catch item per trip)
  clean_data <- download_parquet_from_cloud(
    prefix = file.path(
      pars$api$trips$validated$cloud_path,
      pars$api$trips$validated$file_prefix
    ),
    provider = pars$storage$google$key,
    options = pars$storage$google$options_api
  )

  f_metrics <- calculate_fishery_metrics(data = clean_data)

  # Trip-level intermediate: collapse to one row per trip, add effort metrics.
  # clean_data has multiple rows per trip (one per catch item); trip-level columns
  # (tot_catch_kg, tot_catch_price, n_fishers, etc.) are identical within a trip,
  # so slice(1) is safe and explicit.
  indicators_df <-
    clean_data |>
    dplyr::group_by(.data$trip_id) |>
    dplyr::slice(1) |>
    dplyr::ungroup() |>
    dplyr::mutate(
      price_kg = .data$tot_catch_price / .data$tot_catch_kg,
      cpue = .data$tot_catch_kg / .data$n_fishers / .data$trip_duration_hrs,
      rpue = .data$tot_catch_price / .data$n_fishers / .data$trip_duration_hrs,
      cpue_day = .data$tot_catch_kg / .data$n_fishers,
      rpue_day = .data$tot_catch_price / .data$n_fishers
    )

  # Taxon-level intermediate: collapse to one row per trip x taxon,
  # computing per-taxon catch weight and mean length.
  taxa_df <-
    clean_data |>
    dplyr::group_by(.data$trip_id, .data$catch_taxon) |>
    dplyr::summarise(
      dplyr::across(dplyr::everything(), ~ dplyr::first(.x)),
      length_cm = mean(.data$length_cm, na.rm = TRUE),
      taxon_catch_kg = sum(.data$catch_kg, na.rm = TRUE),
      .groups = "drop"
    ) |>
    dplyr::relocate("n_catch", .after = "trip_id")

  # --- Summaries (all stored wide until export_wf_data pivots for MongoDB) ---

  monthly_summaries <-
    indicators_df |>
    dplyr::mutate(
      date = lubridate::floor_date(.data$landing_date, "month"),
      date = lubridate::as_datetime(.data$date)
    ) |>
    dplyr::group_by(.data$gaul_2_name, .data$date) |>
    dplyr::summarise(
      dplyr::across(
        .cols = c(
          "tot_catch_kg",
          "tot_catch_price",
          "cpue",
          "cpue_day",
          "rpue",
          "rpue_day",
          "price_kg"
        ),
        ~ mean(.x, na.rm = TRUE)
      ),
      .groups = "drop"
    ) |>
    dplyr::rename(
      mean_catch_kg = "tot_catch_kg",
      mean_catch_price = "tot_catch_price",
      mean_cpue = "cpue",
      mean_cpue_day = "cpue_day",
      mean_rpue = "rpue",
      mean_rpue_day = "rpue_day",
      mean_price_kg = "price_kg"
    ) |>
    tidyr::complete(
      .data$gaul_2_name,
      date = seq(min(.data$date), max(.data$date), by = "month"),
      fill = list(
        mean_catch_kg = NA,
        mean_catch_price = NA,
        mean_cpue = NA,
        mean_cpue_day = NA,
        mean_rpue = NA,
        mean_rpue_day = NA,
        mean_price_kg = NA
      )
    )

  # Taxa summaries: total catch and mean length per species x district x month.
  # Stored in long format (metric/value) for portal consumption.
  taxa_summaries <-
    taxa_df |>
    dplyr::mutate(date = lubridate::floor_date(.data$landing_date, "month")) |>
    dplyr::group_by(.data$gaul_2_name, .data$date, .data$catch_taxon) |>
    dplyr::summarise(
      catch_kg = sum(.data$taxon_catch_kg, na.rm = TRUE),
      catch_price = sum(.data$tot_catch_price, na.rm = TRUE),
      mean_length = mean(.data$length_cm, na.rm = TRUE),
      .groups = "drop"
    ) |>
    tidyr::complete(
      .data$gaul_2_name,
      date = seq(min(.data$date), max(.data$date), by = "month"),
      .data$catch_taxon
    ) |>
    dplyr::mutate(price_kg = .data$catch_price / .data$catch_kg) |>
    dplyr::select(-c("catch_price")) |>
    tidyr::pivot_longer(
      -c("gaul_2_name", "date", "catch_taxon"),
      names_to = "metric",
      values_to = "value"
    ) |>
    dplyr::mutate(
      date = lubridate::as_datetime(.data$date)
    ) |>
    dplyr::distinct()

  # Districts summaries: submission counts and effort metrics per district x month.
  # Stored wide; export_wf_data() joins modeled estimates then pivots to long.
  districts_summaries <-
    indicators_df |>
    dplyr::mutate(date = lubridate::floor_date(.data$landing_date, "month")) |>
    dplyr::group_by(.data$gaul_2_name, .data$date) |>
    dplyr::summarise(
      n_submissions = dplyr::n(),
      n_fishers = mean(.data$n_fishers),
      trip_duration_hrs = mean(.data$trip_duration_hrs),
      mean_cpue = mean(.data$cpue, na.rm = TRUE),
      mean_rpue = mean(.data$rpue, na.rm = TRUE),
      mean_price_kg = mean(.data$price_kg, na.rm = TRUE),
      .groups = "drop"
    ) |>
    tidyr::complete(
      .data$gaul_2_name,
      date = seq(min(.data$date), max(.data$date), by = "month")
    ) |>
    dplyr::mutate(date = lubridate::as_datetime(.data$date))

  # Gear summaries: CPUE/RPUE by gear type x district x month, in long format.
  gear_summaries <-
    indicators_df |>
    dplyr::mutate(date = lubridate::floor_date(.data$landing_date, "month")) |>
    dplyr::group_by(.data$gaul_2_name, .data$date, .data$gear) |>
    dplyr::summarise(
      n_submissions = dplyr::n(),
      cpue = mean(.data$cpue, na.rm = TRUE),
      rpue = mean(.data$rpue, na.rm = TRUE),
      .groups = "drop"
    ) |>
    tidyr::complete(
      .data$gaul_2_name,
      date = seq(min(.data$date), max(.data$date), by = "month"),
      .data$gear
    ) |>
    dplyr::mutate(date = lubridate::as_datetime(.data$date)) |>
    tidyr::pivot_longer(
      -c("gaul_2_name", "date", "gear"),
      names_to = "indicator",
      values_to = "value"
    )

  # Grid summaries: pre-computed spatial grid from PDS tracks, passed through as-is.
  grid_summaries <-
    download_parquet_from_cloud(
      prefix = paste0(pars$pds$pds_tracks$file_prefix, "-grid_summaries"),
      provider = pars$storage$google$key,
      options = pars$storage$google$options
    )

  # Upload all summaries to cloud storage (versioned parquet files)
  dataframes_to_upload <- list(
    monthly_summaries = monthly_summaries,
    taxa_summaries = taxa_summaries,
    districts_summaries = districts_summaries,
    gear_summaries = gear_summaries,
    grid_summaries = grid_summaries
  )

  # Write each data frame to its own parquet file with versioning and upload
  for (name in names(dataframes_to_upload)) {
    filename <- pars$surveys$wf_v1$summaries$file_prefix %>%
      paste0("_", name) %>% # Add the table name to distinguish files
      add_version(extension = "parquet")

    arrow::write_parquet(
      x = dataframes_to_upload[[name]],
      sink = filename,
      compression = "lz4",
      compression_level = 12
    )

    logger::log_info("Uploading {filename} to cloud storage")
    upload_cloud_file(
      file = filename,
      provider = pars$storage$google$key,
      options = pars$storage$google$options
    )
  }

  upload_parquet_to_cloud(
    data = f_metrics,
    prefix = "zanzibar_fishery_metrics",
    provider = pars$storage$google$key,
    options = pars$storage$google$options_coasts
  )
}
