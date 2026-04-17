# ── Internal format helpers ────────────────────────────────────────────────────

#' Transform WF survey data into the canonical API schema
#'
#' @param surveys_df Data frame of WF preprocessed or validated survey records.
#' @param conf Configuration list from [read_config()].
#' @return A tibble in the canonical API schema.
#' @noRd
format_api_wf <- function(surveys_df, conf) {
  surveys_df |>
    dplyr::rowwise() |>
    dplyr::mutate(
      trip_id = paste0("TRIP_", .data$submission_id),
      survey_id = dplyr::case_when(
        .data$survey_version == "1" ~ conf$ingestion$wf_v1$asset_id,
        .data$survey_version == "2" ~ conf$ingestion$wf_v2$asset_id
      )
    ) |>
    dplyr::ungroup() |>
    dplyr::mutate(
      n_catch = as.integer(.data$n_catch),
      n_fishers = .data$no_men_fishers +
        .data$no_women_fishers +
        .data$no_child_fishers
    ) |>
    dplyr::select(
      "survey_id",
      "trip_id",
      "landing_date",
      "gaul_1_code",
      "gaul_1_name",
      "gaul_2_code",
      "gaul_2_name",
      "n_fishers",
      trip_duration_hrs = "trip_duration",
      "gear",
      "vessel_type",
      catch_habitat = "habitat",
      "catch_outcome",
      "n_catch",
      catch_taxon = "alpha3_code",
      "scientific_name",
      length_cm = "length",
      "catch_kg",
      tot_catch_price = "catch_price"
    ) |>
    dplyr::group_by(.data$trip_id) |>
    dplyr::mutate(
      catch_price = NA_real_,
      tot_catch_kg = sum(.data$catch_kg)
    ) |>
    dplyr::ungroup() |>
    dplyr::relocate(
      c("catch_price", "tot_catch_kg", "tot_catch_price"),
      .after = "catch_kg"
    ) |>
    dplyr::distinct()
}


#' Transform WCS survey data into the canonical API schema
#'
#' @param surveys_df Data frame of WCS preprocessed or validated survey records.
#' @param conf Configuration list from [read_config()].
#' @return A tibble in the canonical API schema.
#' @noRd
format_api_wcs <- function(surveys_df, conf) {
  surveys_df |>
    # preprocessed data uses alpha3_code; validated data already renames it to catch_taxon
    dplyr::rename(dplyr::any_of(c(catch_taxon = "alpha3_code"))) |>
    dplyr::mutate(
      trip_id = paste0("TRIP_", .data$submission_id),
      survey_id = conf$ingestion$wcs$asset_id,
      n_catch = as.integer(.data$n_catch),
      length_cm = NA_real_
    ) |>
    dplyr::select(
      "survey_id",
      "trip_id",
      landing_date = "submission_date",
      "gaul_1_code",
      "gaul_1_name",
      "gaul_2_code",
      "gaul_2_name",
      "n_fishers",
      trip_duration_hrs = "trip_length_hrs",
      "gear",
      "vessel_type",
      catch_habitat = "habitat",
      "catch_outcome",
      "n_catch",
      "catch_taxon",
      "scientific_name",
      "length_cm",
      "catch_kg",
      "catch_price"
    ) |>
    dplyr::group_by(.data$trip_id) |>
    dplyr::mutate(
      tot_catch_kg = sum(.data$catch_kg),
      tot_catch_price = sum(.data$catch_price)
    ) |>
    dplyr::ungroup() |>
    dplyr::relocate(
      c("catch_price", "tot_catch_kg", "tot_catch_price"),
      .after = "catch_kg"
    ) |>
    dplyr::distinct()
}


#' Write a parquet file locally and upload it to cloud storage
#'
#' @param data Data frame to export.
#' @param file_prefix File prefix string (versioned filename will be derived).
#' @param cloud_path Cloud directory path.
#' @param conf Configuration list from [read_config()].
#' @return NULL invisibly.
#' @noRd
upload_api_parquet <- function(data, file_prefix, cloud_path, conf) {
  filename <- add_version(file_prefix, extension = "parquet")
  logger::log_info("Writing parquet file locally: {filename}")
  arrow::write_parquet(
    data,
    sink = filename,
    compression = "lz4",
    compression_level = 12
  )
  full_cloud_path <- file.path(cloud_path, filename)
  logger::log_info("Uploading to cloud storage: {full_cloud_path}")
  coasts::upload_cloud_file(
    file = filename,
    provider = conf$storage$google$key,
    options = conf$storage$google$options_api,
    name = full_cloud_path
  )
  file.remove(filename)
  invisible(NULL)
}


# ── Exported workflow functions ────────────────────────────────────────────────

#' Export Raw API-Ready Trip Data
#'
#' @description
#' Downloads preprocessed WF and WCS survey data, transforms both into the
#' canonical API schema, merges them, and uploads a single parquet file to
#' cloud storage. This is the **raw/preprocessed** stage of the two-stage API
#' export pipeline.
#'
#' @details
#' **Output Schema**:
#' - `survey_id`: Kobo asset ID identifying the source survey form
#' - `trip_id`: Unique identifier (`TRIP_<submission_id>` format)
#' - `landing_date`: Date of landing
#' - `gaul_1_code`, `gaul_1_name`: GAUL level 1 region
#' - `gaul_2_code`, `gaul_2_name`: GAUL level 2 district
#' - `n_fishers`: Total fishers (men + women + children)
#' - `trip_duration_hrs`: Trip duration in hours
#' - `gear`: Standardised gear type
#' - `vessel_type`: Standardised vessel type
#' - `catch_habitat`: Habitat where catch occurred
#' - `catch_outcome`: Outcome of catch
#' - `n_catch`: Number of catch items
#' - `catch_taxon`: Species alpha-3 code
#' - `scientific_name`: Scientific name
#' - `length_cm`: Length in cm (NA for WCS surveys)
#' - `catch_kg`: Catch weight in kg
#' - `catch_price`: Individual-level price (NA — not resolved at this stage)
#' - `tot_catch_kg`: Total catch weight per trip
#' - `tot_catch_price`: Total catch price per trip
#'
#' **Cloud Storage Location**:
#' `conf$api$trips$raw$cloud_path` /
#' `{file_prefix}__{timestamp}_{git_sha}__.parquet`
#'
#' @param log_threshold Logging level (default `logger::DEBUG`).
#' @return NULL invisibly. Side effect: uploads merged parquet to cloud storage.
#'
#' @seealso
#' * [export_api_validated()] for the validated-data counterpart
#' * [preprocess_wf_surveys()] and [preprocess_wcs_surveys()] for upstream steps
#'
#' @keywords workflow export
#' @export
export_api_raw <- function(log_threshold = logger::DEBUG) {
  logger::log_threshold(log_threshold)
  conf <- read_config()

  logger::log_info("Downloading WF preprocessed survey data...")
  wf_preprocessed <- coasts::download_parquet_from_cloud(
    prefix = conf$surveys$wf_v1$preprocessed$file_prefix,
    provider = conf$storage$google$key,
    options = conf$storage$google$options
  )

  logger::log_info("Downloading WCS preprocessed survey data...")
  wcs_preprocessed <- coasts::download_parquet_from_cloud(
    prefix = conf$surveys$wcs$preprocessed$file_prefix,
    provider = conf$storage$google$key,
    options = conf$storage$google$options
  )

  logger::log_info("Transforming surveys to API format...")
  api_data <- dplyr::bind_rows(
    format_api_wf(wf_preprocessed, conf),
    format_api_wcs(wcs_preprocessed, conf)
  ) |>
    dplyr::filter(!.data$survey_id == conf$ingestion$wcs$asset_id)

  logger::log_info(
    "Processed {nrow(api_data)} records from {length(unique(api_data$trip_id))} unique trips"
  )

  upload_api_parquet(
    data = api_data,
    file_prefix = conf$api$trips$raw$file_prefix,
    cloud_path = conf$api$trips$raw$cloud_path,
    conf = conf
  )

  logger::log_success("Raw API trip data export completed successfully")
  invisible(NULL)
}


#' Export Validated API-Ready Trip Data
#'
#' @description
#' Downloads validated WF and WCS survey data, transforms both into the
#' canonical API schema, merges them, and uploads a single parquet file to
#' cloud storage. This is the **validated** stage of the two-stage API export
#' pipeline.
#'
#' @details
#' See [export_api_raw()] for the full output schema. This function reads from
#' the validated cloud paths and writes to
#' `conf$api$trips$validated$cloud_path`.
#'
#' @param log_threshold Logging level (default `logger::DEBUG`).
#' @return NULL invisibly. Side effect: uploads merged parquet to cloud storage.
#'
#' @seealso
#' * [export_api_raw()] for the raw/preprocessed counterpart
#' * [validate_wf_surveys()] and [validate_wcs_surveys()] for upstream steps
#'
#' @keywords workflow export
#' @export
export_api_validated <- function(log_threshold = logger::DEBUG) {
  logger::log_threshold(log_threshold)
  conf <- read_config()

  logger::log_info("Downloading WF validated survey data...")
  wf_validated <- coasts::download_parquet_from_cloud(
    prefix = conf$surveys$wf_v1$validated$file_prefix,
    provider = conf$storage$google$key,
    options = conf$storage$google$options
  )

  logger::log_info("Downloading WCS validated survey data...")
  wcs_validated <- coasts::download_parquet_from_cloud(
    prefix = conf$surveys$wcs$validated$file_prefix,
    provider = conf$storage$google$key,
    options = conf$storage$google$options
  )

  logger::log_info("Transforming surveys to API format...")
  api_data <- dplyr::bind_rows(
    format_api_wf(wf_validated, conf),
    format_api_wcs(wcs_validated, conf)
  ) |>
    dplyr::filter(!.data$survey_id == conf$ingestion$wcs$asset_id)

  logger::log_info(
    "Processed {nrow(api_data)} records from {length(unique(api_data$trip_id))} unique trips"
  )

  upload_api_parquet(
    data = api_data,
    file_prefix = conf$api$trips$validated$file_prefix,
    cloud_path = conf$api$trips$validated$cloud_path,
    conf = conf
  )

  logger::log_success("Validated API trip data export completed successfully")
  invisible(NULL)
}
