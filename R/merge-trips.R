#' Clean Text for Cross-Dataset Matching
#'
#' Standardizes text fields by converting to lowercase, removing punctuation,
#' eliminating common boat prefixes, and normalizing whitespace.
#'
#' @param x Character vector to clean
#'
#' @return Character vector with cleaned text. Empty strings are converted to NA.
#'
#' @details
#' The function performs the following transformations:
#' \itemize{
#'   \item Converts to lowercase
#'   \item Replaces punctuation (/, \, -, _, ,, ., :, ;) with spaces
#'   \item Removes common boat prefixes (mv, mv., boat, vessel)
#'   \item Removes all non-alphanumeric characters except spaces
#'   \item Normalizes whitespace (removes extra spaces and trims)
#'   \item Converts empty strings to NA
#' }
#'
#' @keywords internal
clean_text <- function(x) {
  x |>
    tolower() |>
    stringr::str_replace_all("[/\\-_,.:;]+", " ") |>
    stringr::str_replace_all("\\b(mv|mv\\.|boat|vessel)\\b", " ") |>
    stringr::str_replace_all("[^a-z0-9 ]", " ") |>
    stringr::str_squish() |>
    dplyr::na_if("")
}

#' Clean Registration Numbers
#'
#' Standardizes boat registration numbers by removing spaces and filtering out
#' invalid or placeholder values.
#'
#' @param x Character vector of registration numbers to clean
#'
#' @return Character vector with cleaned registration numbers. Invalid values
#'   (empty strings, "0", "0000", "035") are converted to NA.
#'
#' @details
#' The function performs the following transformations:
#' \itemize{
#'   \item Applies text cleaning via `clean_text()`
#'   \item Removes all spaces
#'   \item Converts invalid placeholders to NA: "", "0", "0000", "035"
#' }
#'
#' @keywords internal
clean_registration <- function(x) {
  x |>
    clean_text() |>
    stringr::str_replace_all(" ", "") |>
    dplyr::na_if("") |>
    dplyr::na_if("0") |>
    dplyr::na_if("0000") |>
    dplyr::na_if("035")
}

#' Clean Matching Fields in a Data Frame
#'
#' Creates cleaned versions of boat identifiers for fuzzy matching. Removes
#' boat_name when it duplicates the registration_number.
#'
#' @param data Data frame with standardized column names: registration_number,
#'   boat_name, fisher_name
#'
#' @return Data frame with three additional columns: registration_number_clean,
#'   boat_name_clean, fisher_name_clean
#'
#' @details
#' The function performs the following operations:
#' \itemize{
#'   \item Cleans registration_number using `clean_registration()`
#'   \item Cleans boat_name and fisher_name using `clean_text()`
#'   \item Sets boat_name_clean to NA when it matches registration_number_clean
#'     (to avoid double-counting the same information)
#' }
#'
#' @keywords internal
clean_matching_fields <- function(data) {
  data |>
    dplyr::mutate(
      registration_number_clean = clean_registration(.data$registration_number),
      boat_name_clean = clean_text(.data$boat_name),
      fisher_name_clean = clean_text(.data$fisher_name)
    ) |>
    dplyr::mutate(
      boat_name_clean = dplyr::if_else(
        !is.na(.data$boat_name_clean) &
          !is.na(.data$registration_number_clean) &
          .data$boat_name_clean == .data$registration_number_clean,
        NA_character_,
        .data$boat_name_clean
      )
    )
}

#' Standardize Column Names for Matching
#'
#' Renames variant column names to standard names expected by matching functions.
#' Creates missing columns as NA if they don't exist.
#'
#' @param data Data frame with boat identifier columns
#'
#' @return Data frame with standardized column names: registration_number,
#'   boat_name, fisher_name
#'
#' @details
#' The function handles variant column names from different data sources:
#' \itemize{
#'   \item \strong{Registration}: vessel_reg_number, boat_reg_no -> registration_number
#'   \item \strong{Fisher}: captain_name, captain -> fisher_name
#'   \item \strong{Boat name}: retained as boat_name
#' }
#'
#' If any of these columns are missing entirely, they are created as NA_character_.
#'
#' @keywords internal
standardize_column_names <- function(data) {
  # Detect and rename registration number column
  reg_cols <- c("registration_number", "vessel_reg_number", "boat_reg_no")
  reg_col <- intersect(reg_cols, names(data))[1]
  if (!is.na(reg_col) && reg_col != "registration_number") {
    data <- data |>
      dplyr::rename(registration_number = dplyr::all_of(reg_col))
  } else if (is.na(reg_col)) {
    data <- data |> dplyr::mutate(registration_number = NA_character_)
  }

  # Detect and rename fisher/captain column
  fisher_cols <- c("fisher_name", "captain_name", "captain")
  fisher_col <- intersect(fisher_cols, names(data))[1]
  if (!is.na(fisher_col) && fisher_col != "fisher_name") {
    data <- data |>
      dplyr::rename(fisher_name = dplyr::all_of(fisher_col))
  } else if (is.na(fisher_col)) {
    data <- data |> dplyr::mutate(fisher_name = NA_character_)
  }

  # boat_name should already exist, if not create it
  if (!"boat_name" %in% names(data)) {
    data <- data |> dplyr::mutate(boat_name = NA_character_)
  }

  data
}

# ---- Core Matching Functions ----

#' Match Surveys to Device Registry via Fuzzy Matching
#'
#' Uses per-field Levenshtein distances to match surveys to a boat registry.
#' Each survey is matched to the registry entry with the most fields within
#' the specified distance thresholds.
#'
#' @param surveys Data frame with cleaned matching fields (registration_number_clean,
#'   boat_name_clean, fisher_name_clean) and submission_id
#' @param registry Data frame with cleaned matching fields (registration_number_clean,
#'   boat_name_clean, fisher_name_clean) and imei
#' @param reg_threshold Numeric. Maximum normalized Levenshtein distance (0-1)
#'   for registration number matching. Default is 0.15.
#' @param name_threshold Numeric. Maximum normalized Levenshtein distance (0-1)
#'   for boat name and fisher name matching. Default is 0.25.
#'
#' @return Data frame with the following columns:
#'   \itemize{
#'     \item submission_id: Survey identifier
#'     \item imei: Matched device IMEI
#'     \item n_fields_used: Number of non-NA fields available for matching (0-3)
#'     \item n_fields_ok: Number of fields within threshold
#'     \item match_ok: Logical indicating if at least one field matched (n_fields_ok >= 1)
#'   }
#'
#' @details
#' The matching algorithm:
#' \enumerate{
#'   \item Computes Levenshtein distance matrices between surveys and registry
#'     for each field (registration, boat name, fisher name)
#'   \item Normalizes distances by maximum string length to get values in \[0,1\]
#'   \item For each survey, counts how many fields match each registry entry
#'     (within thresholds)
#'   \item Assigns survey to the registry entry with the most matching fields
#'   \item Sets match_ok = TRUE if at least one field matched
#' }
#'
#' Surveys with no non-NA fields return imei = NA and match_ok = FALSE.
#'
#' @keywords internal
match_surveys_to_registry <- function(
  surveys,
  registry,
  reg_threshold = 0.15,
  name_threshold = 0.25
) {
  s_reg <- ifelse(
    is.na(surveys$registration_number_clean),
    "",
    surveys$registration_number_clean
  )
  s_boat <- ifelse(is.na(surveys$boat_name_clean), "", surveys$boat_name_clean)
  s_cap <- ifelse(
    is.na(surveys$fisher_name_clean),
    "",
    surveys$fisher_name_clean
  )

  r_reg <- ifelse(
    is.na(registry$registration_number_clean),
    "",
    registry$registration_number_clean
  )
  r_boat <- ifelse(
    is.na(registry$boat_name_clean),
    "",
    registry$boat_name_clean
  )
  r_cap <- ifelse(
    is.na(registry$fisher_name_clean),
    "",
    registry$fisher_name_clean
  )

  reg_dists <- stringdist::stringdistmatrix(s_reg, r_reg, method = "lv")
  boat_dists <- stringdist::stringdistmatrix(s_boat, r_boat, method = "lv")
  cap_dists <- stringdist::stringdistmatrix(s_cap, r_cap, method = "lv")

  normalize <- function(dmat, a, b) {
    max_len <- outer(nchar(a), nchar(b), pmax, 1)
    dmat / max_len
  }

  reg_norm <- normalize(reg_dists, s_reg, r_reg)
  boat_norm <- normalize(boat_dists, s_boat, r_boat)
  cap_norm <- normalize(cap_dists, s_cap, r_cap)

  purrr::map_dfr(seq_len(nrow(surveys)), function(i) {
    has_reg <- !is.na(surveys$registration_number_clean[i])
    has_boat <- !is.na(surveys$boat_name_clean[i])
    has_cap <- !is.na(surveys$fisher_name_clean[i])

    n_fields <- sum(has_reg, has_boat, has_cap)

    if (n_fields == 0L) {
      return(tibble::tibble(
        submission_id = surveys$submission_id[i],
        imei = NA_character_,
        n_fields_used = 0L,
        n_fields_ok = 0L,
        match_ok = FALSE
      ))
    }

    n_ok <- rep(0L, nrow(registry))
    if (has_reg) {
      n_ok <- n_ok + as.integer(reg_norm[i, ] <= reg_threshold)
    }
    if (has_boat) {
      n_ok <- n_ok + as.integer(boat_norm[i, ] <= name_threshold)
    }
    if (has_cap) {
      n_ok <- n_ok + as.integer(cap_norm[i, ] <= name_threshold)
    }

    best_idx <- which.max(n_ok)

    tibble::tibble(
      submission_id = surveys$submission_id[i],
      imei = registry$imei[best_idx],
      n_fields_used = as.integer(n_fields),
      n_fields_ok = as.integer(n_ok[best_idx]),
      match_ok = n_ok[best_idx] >= 1L
    )
  })
}

#' Match IMEIs to GPS Trips with One-Trip-Per-Day Constraint
#'
#' Joins surveys to GPS trips by IMEI and landing_date, but only creates matches
#' when there is exactly one survey and one trip per IMEI-date combination.
#' Records with multiple trips/surveys per day remain unmatched.
#'
#' @param matched Data frame of surveys with matched IMEIs (from match_surveys_to_registry)
#'   containing submission_id, landing_date, imei, and boat identifiers
#' @param trips Data frame of GPS trips containing trip, imei, ended, and boat identifiers
#'
#' @return Data frame combining all surveys and trips with the following structure:
#'   \itemize{
#'     \item \strong{Matched records}: Survey-trip pairs where both have unique_trip_per_day = TRUE
#'     \item \strong{Unmatched surveys}: Surveys with multiple per day (unique_trip_per_day = FALSE)
#'     \item \strong{Unmatched trips}: Trips with multiple per day (unique_trip_per_day = FALSE)
#'   }
#'
#'   Key columns include:
#'   \itemize{
#'     \item submission_id, landing_date, imei
#'     \item n_fields_used, n_fields_ok, match_ok (from registry matching)
#'     \item trip, started, ended (trip identifiers)
#'     \item registration_number_survey, registration_number_trip (for comparison)
#'     \item boat_name_survey, boat_name_trip
#'     \item fisher_name_survey, fisher_name_trip
#'     \item boat, duration_seconds, range_meters, distance_meters (trip metadata)
#'   }
#'
#' @details
#' The function implements a conservative matching strategy:
#' \enumerate{
#'   \item Standardizes column names and creates landing_date from trip end times
#'   \item Counts number of surveys and trips per IMEI-date combination
#'   \item Splits data into unique (n=1) and non-unique (n>1) groups
#'   \item Only joins the unique groups (one survey + one trip per day)
#'   \item Combines matched records with all unmatched records
#'   \item Removes internal columns (_clean suffixes, counts, flags)
#' }
#'
#' This approach prevents ambiguous matches when multiple trips occur on the same day.
#'
#' @keywords internal
match_imei_to_trip <- function(matched, trips) {
  # Prepare trips with explicit naming
  trips_clean <- trips |>
    dplyr::mutate(
      imei = as.character(.data$imei),
      landing_date = lubridate::as_date(.data$ended)
    ) |>
    standardize_column_names() |>
    dplyr::rename(
      registration_number_trip = "registration_number",
      boat_name_trip = "boat_name",
      fisher_name_trip = "fisher_name"
    )

  # Counts
  survey_counts <- matched |>
    dplyr::count(.data$landing_date, .data$imei, name = "n_surveys")

  trip_counts <- trips_clean |>
    dplyr::count(.data$landing_date, .data$imei, name = "n_trips")

  # Surveys already have _survey suffix from match_surveys_to_gps_trips
  surveys_with_counts <- matched |>
    dplyr::left_join(survey_counts, by = c("landing_date", "imei")) |>
    dplyr::mutate(unique_trip_per_day = .data$n_surveys == 1)

  trips_with_counts <- trips_clean |>
    dplyr::left_join(trip_counts, by = c("landing_date", "imei")) |>
    dplyr::mutate(unique_trip_per_day = .data$n_trips == 1)

  # Split by unique_trip_per_day
  surveys_split <- surveys_with_counts |>
    split(surveys_with_counts$unique_trip_per_day)

  trips_split <- trips_with_counts |>
    split(trips_with_counts$unique_trip_per_day)

  # Full join only the unique ones
  matched_records <- dplyr::full_join(
    surveys_split$`TRUE`,
    trips_split$`TRUE`,
    by = c("landing_date", "imei", "unique_trip_per_day"),
    relationship = "many-to-many"
  )

  # Combine all
  result <- dplyr::bind_rows(
    matched_records,
    surveys_split$`FALSE`,
    trips_split$`FALSE`
  ) |>
    dplyr::select(
      # Core
      "submission_id",
      "landing_date",
      "imei",

      # Match quality
      dplyr::any_of(c("n_fields_used", "n_fields_ok", "match_ok")),

      # Trip
      "trip",
      "started",
      "ended",

      # Identifiers comparison
      dplyr::any_of(c(
        "registration_number_survey",
        "registration_number_trip",
        "boat_name_survey",
        "boat_name_trip",
        "fisher_name_survey",
        "fisher_name_trip"
      )),

      # Everything else
      dplyr::everything()
    ) |>
    dplyr::select(
      -dplyr::ends_with("_clean"),
      -dplyr::any_of(c("n_surveys", "n_trips", "unique_trip_per_day"))
    )

  result
}

#' Build Boat Registry from GPS Trips Data
#'
#' Extracts unique boat identifiers from GPS trips data to create an implicit
#' device registry. Used when a separate registry table is not available
#' (e.g., for Zanzibar data).
#'
#' @param trips Data frame with imei and boat identifier columns
#'
#' @return Data frame with unique combinations of:
#'   \itemize{
#'     \item imei: Device identifier
#'     \item registration_number: Boat registration (standardized name)
#'     \item boat_name: Boat name
#'     \item fisher_name: Fisher/captain name (standardized name)
#'   }
#'
#'   Only rows with at least one non-NA identifier are retained.
#'
#' @details
#' The function:
#' \enumerate{
#'   \item Standardizes column names (vessel_reg_number -> registration_number, etc.)
#'   \item Selects only the identifier columns
#'   \item Finds distinct combinations
#'   \item Filters out rows where all identifiers (except imei) are NA
#' }
#'
#' @keywords internal
build_registry_from_trips <- function(trips) {
  trips |>
    standardize_column_names() |>
    dplyr::select("imei", "registration_number", "boat_name", "fisher_name") |>
    dplyr::distinct() |>
    dplyr::filter(
      !is.na(.data$registration_number) |
        !is.na(.data$boat_name) |
        !is.na(.data$fisher_name)
    )
}

# ---- Main Workflow Function ----

#' Match Catch Surveys to GPS Trips
#'
#' Universal two-step matching workflow that links catch survey records to GPS
#' trip data via device identifiers (IMEI). Works for both Kenya and Zanzibar
#' by accepting either an explicit device registry or constructing an implicit
#' one from the trips data.
#'
#' @param surveys Data frame containing:
#'   \itemize{
#'     \item submission_id: Unique survey identifier
#'     \item landing_date: Date of landing
#'     \item Boat identifiers: registration_number (or variants), boat_name, fisher_name (or captain)
#'   }
#' @param trips Data frame containing:
#'   \itemize{
#'     \item trip: Unique trip identifier
#'     \item imei: Device identifier
#'     \item ended: Trip end timestamp
#'     \item Boat identifiers: registration_number (or variants), boat_name, fisher_name (or captain)
#'   }
#' @param registry Optional data frame with device-boat mappings containing:
#'   \itemize{
#'     \item imei: Device identifier
#'     \item Boat identifiers: registration_number (or variants), boat_name, fisher_name (or captain)
#'   }
#'   If NULL, will be constructed from trips data (Zanzibar approach).
#' @param reg_threshold Numeric. Maximum normalized Levenshtein distance (0-1)
#'   for registration number fuzzy matching. Default is 0.15 (15% difference allowed).
#' @param name_threshold Numeric. Maximum normalized Levenshtein distance (0-1)
#'   for boat name and fisher name fuzzy matching. Default is 0.25 (25% difference allowed).
#'
#' @return Data frame combining matched and unmatched records with columns:
#'   \itemize{
#'     \item submission_id: Survey identifier (NA for unmatched trips)
#'     \item landing_date: Landing date
#'     \item imei: Device identifier
#'     \item n_fields_used: Number of fields available for matching (0-3)
#'     \item n_fields_ok: Number of fields that matched within threshold
#'     \item match_ok: Logical indicating successful match (at least 1 field matched)
#'     \item trip: GPS trip identifier (NA for unmatched surveys)
#'     \item started, ended: Trip timestamps
#'     \item registration_number_survey, registration_number_trip: For comparison
#'     \item boat_name_survey, boat_name_trip: For comparison
#'     \item fisher_name_survey, fisher_name_trip: For comparison
#'     \item boat, duration_seconds, range_meters, distance_meters: Trip metadata
#'     \item Additional trip columns (gear, community, etc.)
#'   }
#'
#' @details
#' The function implements a two-step matching process:
#'
#' \strong{Step 1: Survey -> Registry (IMEI Assignment)}
#' \enumerate{
#'   \item Standardizes column names across datasets
#'   \item Cleans text fields (lowercase, remove punctuation, normalize whitespace)
#'   \item Uses fuzzy matching (Levenshtein distance) on registration number, boat name, and fisher name
#'   \item Assigns each survey to the registry entry with the most matching fields
#'   \item Requires at least 1 field to match within threshold (match_ok = TRUE)
#' }
#'
#' \strong{Step 2: IMEI -> Trips (Date Matching)}
#' \enumerate{
#'   \item Joins surveys to trips by IMEI and landing_date
#'   \item Only creates matches when there is exactly ONE survey and ONE trip per IMEI-date
#'   \item Records with multiple trips/surveys per day remain unmatched
#'   \item Preserves all unmatched surveys and trips in the output
#' }
#'
#' @section Matching Thresholds:
#' The normalized Levenshtein distance ranges from 0 (exact match) to 1 (completely different):
#' \itemize{
#'   \item reg_threshold = 0.15: Allows ~15% character differences in registration numbers
#'   \item name_threshold = 0.25: Allows ~25% character differences in names
#' }
#'
#' @section Site-Specific Usage:
#' \describe{
#'   \item{Kenya}{Uses explicit device registry from Airtable/PDS}
#'   \item{Zanzibar}{Builds implicit registry from historical trip data (registry = NULL)}
#' }
#'
#' @keywords matching workflow
#' @examples
#' \dontrun{
#' # Kenya: With explicit device registry
#' results <- match_surveys_to_gps_trips(
#'   surveys = kefs_surveys,
#'   trips = pds_trips,
#'   registry = devices
#' )
#'
#' # Zanzibar: Without explicit registry (builds from trips)
#' results <- match_surveys_to_gps_trips(
#'   surveys = wf_surveys,
#'   trips = pds_trips,
#'   registry = NULL
#' )
#'
#' # With custom thresholds
#' results <- match_surveys_to_gps_trips(
#'   surveys = surveys,
#'   trips = trips,
#'   registry = devices,
#'   reg_threshold = 0.10,  # Stricter registration matching
#'   name_threshold = 0.30  # More lenient name matching
#' )
#' }
#'
#' @export
match_surveys_to_gps_trips <- function(
  surveys,
  trips,
  registry = NULL,
  reg_threshold = 0.15,
  name_threshold = 0.25
) {
  surveys_clean <- surveys |>
    standardize_column_names() |>
    clean_matching_fields()

  if (is.null(registry)) {
    logger::log_info(
      "No registry provided - building implicit registry from trips data"
    )
    registry <- build_registry_from_trips(trips)
  } else {
    registry <- registry |> standardize_column_names()
  }

  registry_clean <- registry |> clean_matching_fields()

  logger::log_info("Step 1: Matching surveys to boat registry...")
  survey_matched <- match_surveys_to_registry(
    surveys = surveys_clean,
    registry = registry_clean,
    reg_threshold = reg_threshold,
    name_threshold = name_threshold
  )

  good_matches <- surveys_clean |>
    dplyr::inner_join(survey_matched, by = "submission_id") |>
    dplyr::filter(.data$match_ok == TRUE) |>
    # Explicitly preserve these for the trip join
    dplyr::mutate(
      registration_number_survey = .data$registration_number,
      boat_name_survey = .data$boat_name,
      fisher_name_survey = .data$fisher_name
    )

  logger::log_info(
    "Registry matching: {nrow(good_matches)} surveys -> {length(unique(good_matches$imei))} IMEIs"
  )

  logger::log_info("Step 2: Matching IMEIs to trips...")
  result <- match_imei_to_trip(good_matches, trips)

  logger::log_info("Done: {nrow(result)} survey-trip pairs matched")
  result
}


#' Merge Survey and GPS Trip Data
#'
#' Matches catch surveys to GPS trips using fuzzy matching on boat identifiers,
#' then merges all records (matched and unmatched) and uploads to cloud storage.
#'
#' @param site Character. Site identifier: "kenya" or "zanzibar"
#' @param log_threshold Logger threshold level. Default is `logger::DEBUG`.
#'
#' @return Invisible NULL. Uploads merged parquet file to cloud storage containing:
#'   \itemize{
#'     \item Matched survey-trip pairs (both submission_id and trip are non-NA)
#'     \item Unmatched surveys (trip = NA)
#'     \item Unmatched trips (submission_id = NA)
#'     \item Match quality indicators: n_fields_used, n_fields_ok, match_ok
#'   }
#'
#' @details
#' The function executes a five-step pipeline:
#' \enumerate{
#'   \item Load device registry from cloud storage
#'   \item Load validated surveys
#'   \item Load preprocessed GPS trips
#'   \item Match surveys to trips via fuzzy matching (surveys -> registry -> trips)
#'   \item Merge matched subset with all unmatched records and upload
#' }
#'
#' Site-specific configuration is handled automatically based on the `site` parameter.
#'
#' @examples
#' \dontrun{
#' # Kenya
#' merge_trips(site = "kenya")
#'
#' # Zanzibar
#' merge_trips(site = "zanzibar")
#' }
#'
#' @keywords workflow
#' @export
merge_trips <- function(
  site = c("kenya", "zanzibar"),
  log_threshold = logger::DEBUG
) {
  site <- match.arg(site)
  conf <- read_config()

  # Site-specific configuration
  site_config <- if (site == "kenya") {
    list(
      device_filter = c("WorldFish - Kenya", "Kenya", "Kenya AABS"),
      survey_prefix = conf$surveys$kefs$v2$validated$file_prefix,
      output_prefix = conf$surveys$kefs$v2$merged,
      reg_col = "vessel_reg_number",
      fisher_col = "captain_name",
      pds_flag_col = "pds",
      pds_flag_values = c("yes", "pds")
    )
  } else {
    # zanzibar
    list(
      device_filter = c("WorldFish - Tanzania AP", "WorldFish - Zanzibar"),
      survey_prefix = conf$surveys$wf_v1$validated$file_prefix,
      output_prefix = conf$surveys$wf_v1$merged$file_prefix,
      reg_col = "boat_reg_no",
      fisher_col = "fisher_name",
      pds_flag_col = "has_PDS",
      pds_flag_values = "1"
    )
  }

  # Load device registry
  logger::log_info("Loading {site} device registry...")
  registry <- cloud_object_name(
    prefix = conf$metadata$airtable$assets,
    provider = conf$storage$google$key,
    version = "latest",
    extension = "rds",
    options = conf$storage$google$options_coasts
  ) |>
    download_cloud_file(
      provider = conf$storage$google$key,
      options = conf$storage$google$options_coasts
    ) |>
    readr::read_rds() |>
    purrr::pluck("devices") |>
    dplyr::filter(.data$customer_name %in% site_config$device_filter) |>
    dplyr::select(
      "imei",
      "boat_name",
      "registration_number",
      fisher_name = "captain"
    )

  # Load ALL validated surveys (no filtering)
  logger::log_info("Loading ALL validated surveys...")
  all_surveys <- download_parquet_from_cloud(
    prefix = site_config$survey_prefix,
    provider = conf$storage$google$key,
    options = conf$storage$google$options
  ) |>
    # Add flag for whether survey claimed to have PDS
    dplyr::mutate(
      survey_claimed_pds = !!rlang::sym(site_config$pds_flag_col) %in%
        site_config$pds_flag_values
    )

  # Load GPS trips
  logger::log_info("Loading GPS trips from cloud storage...")
  pds_trips_parquet <- cloud_object_name(
    prefix = conf$pds$pds_trips$file_prefix,
    provider = conf$storage$google$key,
    extension = "parquet",
    version = conf$pds$pds_trips$version,
    options = conf$storage$google$options
  )

  all_trips <- download_cloud_file(
    name = pds_trips_parquet,
    provider = conf$storage$google$key,
    options = conf$storage$google$options
  ) |>
    arrow::read_parquet() |>
    janitor::clean_names() |>
    dplyr::mutate(
      imei = as.character(.data$imei),
      landing_date = lubridate::as_date(.data$ended)
    )

  # Prepare surveys for matching (ALL of them, not filtered)
  surveys_for_matching <- all_surveys |>
    dplyr::select(
      "submission_id",
      "landing_date",
      registration_number = dplyr::all_of(site_config$reg_col),
      "boat_name",
      fisher_name = dplyr::all_of(site_config$fisher_col),
      "survey_claimed_pds" # Keep this flag
    ) |>
    dplyr::distinct()

  logger::log_info("Matching {nrow(surveys_for_matching)} surveys to trips...")

  # Fuzzy match surveys to trips
  matched_subset <- match_surveys_to_gps_trips(
    surveys = surveys_for_matching,
    trips = all_trips,
    registry = registry
  )

  logger::log_info("Merging with full datasets...")

  # Identify matched IDs
  matched_submission_ids <- matched_subset |>
    dplyr::filter(!is.na(.data$submission_id)) |>
    dplyr::pull(.data$submission_id)

  matched_trip_ids <- matched_subset |>
    dplyr::filter(!is.na(.data$trip)) |>
    dplyr::pull(.data$trip)

  # Enrich matched records with full survey data
  matched_with_full_data <- matched_subset |>
    dplyr::left_join(
      all_surveys,
      by = "submission_id",
      suffix = c("_match", ""),
      relationship = "many-to-many"
    ) |>
    dplyr::select(-dplyr::ends_with("_match"))

  # Get unmatched records
  unmatched_surveys <- all_surveys |>
    dplyr::filter(!.data$submission_id %in% matched_submission_ids)

  unmatched_trips <- all_trips |>
    dplyr::filter(!.data$trip %in% matched_trip_ids)

  # Combine all records
  merged_trips <- dplyr::bind_rows(
    matched_with_full_data,
    unmatched_surveys,
    unmatched_trips
  )

  n_matched <- sum(
    !is.na(merged_trips$submission_id) & !is.na(merged_trips$trip)
  )
  n_matched_claimed_pds <- sum(
    !is.na(merged_trips$submission_id) &
      !is.na(merged_trips$trip) &
      merged_trips$survey_claimed_pds,
    na.rm = TRUE
  )
  n_matched_no_claim <- n_matched - n_matched_claimed_pds

  logger::log_info(
    "Done: {nrow(merged_trips)} total records ({n_matched} matched)"
  )
  logger::log_info(
    "  - {n_matched_claimed_pds} matched where survey claimed PDS"
  )
  logger::log_info(
    "  - {n_matched_no_claim} matched where survey did NOT claim PDS (data quality issue?)"
  )

  # Upload to cloud storage
  logger::log_info("Uploading merged data to cloud storage...")
  upload_parquet_to_cloud(
    data = merged_trips,
    prefix = site_config$output_prefix,
    provider = conf$storage$google$key,
    options = conf$storage$google$options
  )

  invisible(NULL)
}
