#' Extract trip information from preprocessed surveys
#'
#' Extracts relevant trip-level information from the preprocessed survey data,
#' including location, vessel, and fishing effort details.
#'
#' @param preprocessed_surveys A preprocessed survey dataframe containing trip information
#' @return A dataframe with trip-level information
#' @keywords validation
#' @export
extract_trips_info <- function(preprocessed_surveys) {
  preprocessed_surveys %>%
    dplyr::select(
      "survey_id",
      "survey_type",
      landing_date = "submission_date",
      "landing_site",
      "lat",
      "lon",
      "trip_length_days",
      "fishing_location",
      "fishing_ground_name",
      "habitat",
      "fishing_ground_depth",
      "gear",
      "vessel_type",
      "boat_engine",
      "engine_hp",
      "n_fishers",
      "boats_landed"
    )
}

#' Process catch data from surveys
#'
#' Processes and structures catch data from surveys, including catch numbers
#' and associated vessel/gear information.
#'
#' @param preprocessed_surveys A preprocessed survey dataframe
#' @param trips_info Trip information dataframe from extract_trips_info
#' @return A dataframe containing processed catch data
#' @keywords validation
#' @export
process_catch_data <- function(preprocessed_surveys, trips_info) {
  preprocessed_surveys |>
    dplyr::filter(.data$survey_type == "catch") |>
    dplyr::select("survey_id", "catch") |>
    tidyr::unnest(.data$catch, keep_empty = T) |>
    dplyr::select(-"all_catch_in_boat") |>
    dplyr::group_by(.data$survey_id) |>
    dplyr::mutate(catch_number = dplyr::row_number()) |>
    dplyr::ungroup() |>
    dplyr::relocate("catch_number", .after = "survey_id") |>
    dplyr::left_join(
      trips_info |>
        dplyr::select(
          "survey_id",
          "landing_date",
          "vessel_type",
          "gear",
          "n_fishers"
        ),
      by = "survey_id"
    ) |>
    dplyr::relocate(
      c("vessel_type", "gear", "n_fishers"),
      .after = "catch_number"
    )
}

#' Add validation flags to catch data
#'
#' Adds quality control flags based on predefined thresholds and rules.
#' Flag descriptions:
#' 1: Number of fishers is 0
#' 2: Catch is negative
#' 3: Catch is null despite group_catch being non-NULL
#' 4: Catch is 0 despite group_catch being non-NULL
#' 5: Number of buckets is too high (> 150)
#' 6: Bucket is > 40kg
#' 7: Small pelagic individual weight > 10kg
#' 8: Individual weight > 250kg
#' 9: Number of fishers is too high (>100) for non-ring nets
#'
#' @param catch_data Processed catch data
#' @return A dataframe with validation flags added
#' @keywords validation
#' @export
add_validation_flags <- function(catch_data) {
  catch_data |>
    dplyr::mutate(
      alert_flag = dplyr::case_when(
        .data$n_fishers < 1 ~ "1",
        .data$catch_kg < 0 ~ "2",
        is.na(.data$catch_kg) & !is.na(.data$group_catch) ~ "3",
        .data$catch_kg == 0 & !is.na(.data$group_catch) ~ "4",
        .data$type_measure == "bucket" & .data$n_elements > 150 ~ "5",
        .data$type_measure == "bucket" & .data$catch_kg > 40 ~ "6",
        .data$type_measure == "individual" &
          .data$group_catch == "small_pelagic" &
          .data$catch_kg > 20 ~
          "7",
        .data$type_measure == "individual" & .data$catch_kg > 250 ~ "8",
        .data$n_fishers > 100 & !.data$gear == "ring_nets" ~ "9",
        .data$landing_date < "2020-01-01" ~ "10",
        TRUE ~ NA_character_
      )
    )
}

#' Validate catches using quality flags
#'
#' Applies validation flags and filters out problematic catches based on quality checks.
#' Propagates flags to survey level to ensure consistency.
#'
#' @param catch_data Processed catch data
#' @return A dataframe of validated catches
#' @keywords validation
#' @export
validate_catches <- function(catch_data) {
  flags_df <- add_validation_flags(catch_data)

  flags_df |>
    dplyr::group_by(.data$survey_id) %>%
    dplyr::mutate(
      alert_catch_survey = max(as.numeric(.data$alert_flag), na.rm = TRUE),
      alert_catch_survey = ifelse(
        .data$alert_catch_survey == -Inf,
        NA,
        .data$alert_catch_survey
      )
    ) %>%
    dplyr::ungroup() |>
    dplyr::filter(is.na(.data$alert_catch_survey)) |>
    dplyr::select(-c("alert_flag", "alert_catch_survey")) |>
    expand_taxa()
}

#' Validate market prices
#'
#' Processes and validates market price data, filtering for reasonable price ranges
#' and calculating median prices by species group and family.
#'
#' @param preprocessed_data Preprocessed survey data containing market information
#' @return A dataframe with validated market prices
#' @keywords validation
#' @export
validate_prices <- function(preprocessed_data) {
  preprocessed_data |>
    dplyr::filter(.data$survey_type == "market") |>
    dplyr::select("survey_id", "market") |>
    tidyr::unnest(.data$market, keep_empty = T) |>
    dplyr::mutate(
      catch_price_kg = .data$catch_price / .data$catch_kg_market
    ) |>
    dplyr::filter(.data$catch_price_kg > 777 & .data$catch_price_kg < 51800) |>
    dplyr::rename(species_catch = "species_market") |>
    expand_taxa() |>
    dplyr::group_by(.data$group_market, .data$family) |>
    dplyr::summarise(
      catch_price_kg = stats::median(.data$catch_price_kg)
    ) |>
    dplyr::ungroup() |>
    dplyr::rename(group_catch = "group_market")
}

#' Calculate catch revenue from validated data
#'
#' Calculates revenue based on catch weights and market prices. The function handles
#' missing prices through a hierarchical approach:
#' 1. Uses direct match if available
#' 2. Falls back to group median price if direct match is missing
#' 3. Uses overall median price if group median is unavailable
#'
#' The function also:
#' - Adjusts catch weights based on number of elements when applicable
#' - Aggregates catches at the survey and catch number level
#' - Calculates total revenue in TZS (Tanzanian Shillings)
#'
#' @param validated A dataframe of validated catch data containing weights and taxonomic information
#' @param market_table A dataframe containing market price information by species group and family
#' @return A dataframe with calculated revenues, including:
#'   - Survey and catch identification
#'   - Catch details (date, vessel, gear, fishers)
#'   - Taxonomic information
#'   - Adjusted catch weights
#'   - Calculated revenue in TZS
#' @keywords economics fisheries
#' @keywords validation
#' @export
calculate_catch_revenue <- function(validated, market_table) {
  validated |>
    dplyr::filter(!is.na(.data$catch_kg)) |>
    dplyr::left_join(market_table, by = c("family", "group_catch")) |>
    dplyr::group_by(.data$survey_id, .data$catch_number) |>
    dplyr::summarise(
      dplyr::across(
        c(
          "landing_date",
          "vessel_type",
          "gear",
          "n_fishers",
          "group_catch",
          "species_catch",
          "n_elements"
        ),
        ~ dplyr::first(.x)
      ),
      catch_kg = dplyr::first(.data$catch_kg),
      catch_price_kg = stats::median(.data$catch_price_kg),
      .groups = "drop"
    ) |>
    dplyr::mutate(
      catch_kg = ifelse(
        !is.na(.data$n_elements),
        .data$catch_kg * .data$n_elements,
        .data$catch_kg
      )
    ) |>
    dplyr::select(-"n_elements") |>
    dplyr::group_by(.data$group_catch) %>%
    dplyr::mutate(
      median_price = stats::median(.data$catch_price_kg, na.rm = TRUE)
    ) %>%
    dplyr::ungroup() %>%
    dplyr::mutate(
      overall_median_price = stats::median(.data$catch_price_kg, na.rm = TRUE),
      catch_price_kg = ifelse(
        is.na(.data$catch_price_kg),
        ifelse(
          is.na(.data$median_price),
          .data$overall_median_price,
          .data$median_price
        ),
        .data$catch_price_kg
      ),
      revenue_TZS = .data$catch_price_kg * .data$catch_kg
    ) %>%
    dplyr::select(-c("catch_price_kg", "median_price", "overall_median_price"))
}


#' Aggregate survey data and calculate metrics
#'
#' Aggregates catch data to survey level and calculates key fisheries metrics including
#' total catches, revenues, and effort-based indicators. The function:
#' 1. Aggregates catches and revenue by survey
#' 2. Joins with trip information
#' 3. Calculates effort-based metrics (CPUE, RPUE)
#' 4. Adjusts fisher counts for multiple boats when applicable
#'
#' Calculated metrics include:
#' - CPUE (Catch Per Unit Effort): kg/fisher/day
#' - RPUE (Revenue Per Unit Effort): TZS/fisher/day
#'
#' @param catch_price_table A dataframe containing catch data with calculated prices
#' @param trips_info A dataframe containing trip-level information
#' @return A dataframe of aggregated survey data containing:
#'   - Survey metadata (ID, date, location, habitat)
#'   - Vessel and gear information
#'   - Total catches and revenue
#'   - Effort-based metrics (CPUE, RPUE)
#'   - Nested catch composition data
#' @keywords validation
#' @export
aggregate_survey_data <- function(catch_price_table, trips_info) {
  validated_price_catch <-
    catch_price_table |>
    dplyr::select(-"catch_number") |>
    dplyr::group_by(.data$survey_id, .data$landing_date) |>
    dplyr::summarise(
      vessel_type = dplyr::first(.data$vessel_type),
      gear = dplyr::first(.data$gear),
      n_fishers = dplyr::first(.data$n_fishers),
      total_catch_kg = sum(.data$catch_kg),
      total_revenue_TZS = sum(.data$revenue_TZS),
      catch_taxa = list(tibble::tibble(
        group_catch = .data$group_catch,
        species_catch = .data$species_catch,
        catch_kg = .data$catch_kg
      )),
      .groups = "drop"
    ) |>
    dplyr::ungroup()

  validated_price_catch |>
    dplyr::left_join(
      trips_info,
      by = c("survey_id", "landing_date", "vessel_type", "gear", "n_fishers")
    ) |>
    dplyr::select(
      "survey_id",
      "landing_date",
      "landing_site",
      "trip_length_days",
      "habitat",
      "vessel_type",
      "boats_landed",
      "gear",
      "n_fishers",
      "total_catch_kg",
      "total_revenue_TZS",
      "catch_taxa"
    ) |>
    dplyr::mutate(
      boats_landed = ifelse(is.na(.data$boats_landed), 0, .data$boats_landed),
      n_fishers = ifelse(
        .data$boats_landed > 0,
        .data$n_fishers * .data$boats_landed,
        .data$n_fishers
      ),
      rpue = (.data$total_revenue_TZS / .data$n_fishers) /
        .data$trip_length_days,
      cpue = (.data$total_catch_kg / .data$n_fishers) / .data$trip_length_days
    ) |>
    dplyr::select(-c("boats_landed", "trip_length_days")) |>
    dplyr::relocate("catch_taxa", .after = "cpue")
}
#' Validate market prices
#'
#' Processes and validates market price data, filtering for reasonable price ranges
#' and calculating median prices by species group and family.
#'
#' @param preprocessed_data Preprocessed survey data containing market information
#' @return A dataframe with validated market prices
#' @keywords validation
#' @export
validate_prices <- function(preprocessed_data) {
  preprocessed_data |>
    dplyr::filter(.data$survey_type == "market") |>
    dplyr::select("survey_id", "market") |>
    tidyr::unnest(.data$market, keep_empty = T) |>
    dplyr::mutate(
      catch_price_kg = .data$catch_price / .data$catch_kg_market
    ) |>
    dplyr::filter(.data$catch_price_kg > 777 & .data$catch_price_kg < 51800) |>
    dplyr::rename(species_catch = "species_market") |>
    expand_taxa() |>
    dplyr::group_by(.data$group_market, .data$family) |>
    dplyr::summarise(
      catch_price_kg = stats::median(.data$catch_price_kg)
    ) |>
    dplyr::ungroup() |>
    dplyr::rename(group_catch = "group_market")
}


#' Get catch bounds for survey data
#'
#' Calculates upper bounds for catch weights by gear type and catch taxon using
#' robust statistical methods. The function performs the following steps:
#' 1. Filters out invalid fish categories
#' 2. Groups data by gear and fish category
#' 3. Calculates upper bounds on log scale and exponentiates results
#'
#' @param data A dataframe containing survey data with columns for gear, catch_taxon, and catch_kg
#' @param k_param Numeric parameter for the LocScaleB outlier detection (default: NULL).
#'               Higher values are more conservative in outlier detection.
#' @return A dataframe containing upper catch bounds for each gear and catch taxon combination
#' @keywords validation
#' @export
get_catch_bounds <- function(data = NULL, k_param = NULL) {
  # 1) Filter out non-valid fish categories
  # 2) Split by gear + fish_category
  # 3) Calculate upper bounds (on log scale, then exponentiate)

  data %>%
    dplyr::select("gear", "catch_taxon", "catch_kg") %>%
    dplyr::filter(!is.na(.data$catch_taxon)) %>%
    split(interaction(.$gear, .$catch_taxon)) %>%
    purrr::map(~ na.omit(.)) %>%
    purrr::discard(~ nrow(.) == 0) %>%
    purrr::map(
      ~ {
        univOutl::LocScaleB(.x[["catch_kg"]], logt = TRUE, k = k_param) %>%
          magrittr::extract2("bounds")
      }
    ) %>%
    dplyr::bind_rows(.id = "gear_catch") %>%
    dplyr::mutate(upper_catch = exp(.data$upper.up)) %>%
    tidyr::separate(
      col = "gear_catch",
      into = c("gear", "catch_taxon"),
      sep = "\\."
    ) %>%
    dplyr::select(-c("lower.low", "upper.up"))
}

#' Get length bounds for survey data
#'
#' Calculates upper bounds for fish lengths by gear type and catch taxon using
#' robust statistical methods. Similar to get_catch_bounds but for length measurements.
#' The function:
#' 1. Filters out invalid fish categories
#' 2. Groups data by gear and fish category
#' 3. Calculates upper bounds on log scale and exponentiates results
#'
#' @param data A dataframe containing survey data with columns for gear, catch_taxon, and length_cm
#' @param k_param Numeric parameter for the LocScaleB outlier detection (default: NULL).
#'               Higher values are more conservative in outlier detection.
#' @return A dataframe containing upper length bounds for each gear and catch taxon combination
#' @keywords validation
#' @export
get_length_bounds <- function(data = NULL, k_param = NULL) {
  data %>%
    dplyr::select("gear", "catch_taxon", "length_cm") %>%
    dplyr::filter(!is.na(.data$catch_taxon)) %>%
    split(interaction(.$gear, .$catch_taxon)) %>%
    purrr::map(~ na.omit(.)) %>%
    purrr::discard(~ nrow(.) == 0) %>%
    purrr::map(
      ~ {
        univOutl::LocScaleB(.x[["length_cm"]], logt = TRUE, k = k_param) %>%
          magrittr::extract2("bounds")
      }
    ) %>%
    dplyr::bind_rows(.id = "gear_length") %>%
    dplyr::mutate(upper_length = exp(.data$upper.up)) %>%
    tidyr::separate(
      col = "gear_length",
      into = c("gear", "catch_taxon"),
      sep = "\\."
    ) %>%
    dplyr::select(-c("lower.low", "upper.up"))
}


#' Get Validation Status from KoboToolbox
#'
#' Retrieves the validation status for a specific submission in KoboToolbox.
#' The function handles NULL responses and returns a consistent tibble structure
#' regardless of the API response.
#'
#' @param submission_id Character string. The ID of the submission to check.
#' @param asset_id Character string. The asset ID from KoboToolbox.
#' @param token Character string. The authorization token for KoboToolbox API.
#' @param debug Logical. If TRUE, prints the request object. Default is FALSE.
#'
#' @return A tibble with one row containing:
#'   \item{submission_id}{The ID of the checked submission}
#'   \item{validation_status}{The validation status (e.g., "validation_status_approved" or "not_validated")}
#'   \item{validated_at}{Timestamp of validation as POSIXct}
#'   \item{validated_by}{Username of the validator}
#'
#' @keywords validation
#' @examples
#' \dontrun{
#' # Single submission
#' get_validation_status(
#'   submission_id = "1234567",
#'   asset_id = "your asset id",
#'   token = "Token YOUR_TOKEN_HERE"
#' )
#'
#' # Multiple submissions using purrr
#' submission_ids <- c("1234567", "154267")
#' submission_ids %>%
#'   purrr::map_dfr(get_validation_status,
#'     asset_id = "your asset id",
#'     token = "Token YOUR_TOKEN_HERE"
#'   )
#' }
#'
#' @keywords workflow validation
#' @export
get_validation_status <- function(
  submission_id = NULL,
  asset_id = NULL,
  token = NULL,
  debug = FALSE
) {
  base_url <- paste0(
    "https://eu.kobotoolbox.org/api/v2/assets/",
    asset_id,
    "/data/"
  )
  url <- paste0(base_url, submission_id, "/validation_status/")

  # Remove "Token " prefix if it exists
  if (grepl("^Token ", token)) {
    token <- gsub("^Token ", "", token)
  }

  req <- httr2::request(url) %>%
    httr2::req_headers(
      Authorization = token
    ) %>%
    httr2::req_method("GET")

  if (debug) {
    print(req)
  }

  tryCatch(
    {
      response <- httr2::req_perform(req)
      if (httr2::resp_status(response) == 200) {
        validation_data <- httr2::resp_body_json(response)

        # Handle NULL validation data
        timestamp <- if (
          !is.null(validation_data) && !is.null(validation_data$timestamp)
        ) {
          lubridate::as_datetime(validation_data$timestamp)
        } else {
          lubridate::as_datetime(NA)
        }

        status <- if (
          !is.null(validation_data) && !is.null(validation_data$uid)
        ) {
          validation_data$uid
        } else {
          "not_validated"
        }

        validator <- if (
          !is.null(validation_data) && !is.null(validation_data$by_whom)
        ) {
          validation_data$by_whom
        } else {
          NA_character_
        }

        dplyr::tibble(
          submission_id = submission_id,
          validation_status = status,
          validated_at = timestamp,
          validated_by = validator
        )
      } else {
        dplyr::tibble(
          submission_id = submission_id,
          validation_status = "not_validated",
          validated_at = lubridate::as_datetime(NA),
          validated_by = NA_character_
        )
      }
    },
    error = function(e) {
      if (debug) {
        cat("Error:", as.character(e), "\n")
      }

      dplyr::tibble(
        submission_id = submission_id,
        validation_status = "not_validated",
        validated_at = lubridate::as_datetime(NA),
        validated_by = NA_character_
      )
    }
  )
}
#' Update Validation Status in KoboToolbox
#'
#' Updates the validation status for a specific submission in KoboToolbox.
#' The function allows setting the status to approved, not approved, or on hold.
#'
#' @param submission_id Character string. The ID of the submission to update.
#' @param asset_id Character string. The asset ID from KoboToolbox.
#' @param token Character string. The authorization token for KoboToolbox API.
#' @param status Character string. The validation status to set. Must be one of:
#'        "validation_status_approved", "validation_status_not_approved", or
#'        "validation_status_on_hold".
#' @param debug Logical. If TRUE, prints the request object and response. Default is FALSE.
#'
#' @return A tibble with one row containing:
#'   \item{submission_id}{The ID of the updated submission}
#'   \item{validation_status}{The new validation status}
#'   \item{validated_at}{Timestamp of validation as POSIXct}
#'   \item{validated_by}{Username of the validator}
#'   \item{update_success}{Logical indicating if the update was successful}
#'
#' @keywords validation
#' @examples
#' \dontrun{
#' # Update a single submission
#' update_validation_status(
#'   submission_id = "1234567",
#'   asset_id = "your asset id",
#'   token = "Token YOUR_TOKEN_HERE",
#'   status = "validation_status_approved"
#' )
#'
#' # Update multiple submissions using purrr
#' submission_ids <- c("1234567", "154267")
#' submission_ids %>%
#'   purrr::map_dfr(update_validation_status,
#'     asset_id = "your asset id",
#'     token = "Token YOUR_TOKEN_HERE",
#'     status = "validation_status_approved"
#'   )
#' }
#'
#' @keywords workflow validation
#' @export
update_validation_status <- function(
  submission_id = NULL,
  asset_id = NULL,
  token = NULL,
  status = "validation_status_approved",
  debug = FALSE
) {
  # Validate status
  valid_statuses <- c(
    "validation_status_approved",
    "validation_status_not_approved",
    "validation_status_on_hold"
  )

  if (!status %in% valid_statuses) {
    stop("Status must be one of: ", paste(valid_statuses, collapse = ", "))
  }

  # Construct the URL
  base_url <- paste0(
    "https://eu.kobotoolbox.org/api/v2/assets/",
    asset_id,
    "/data/"
  )
  url <- paste0(base_url, submission_id, "/validation_status/")

  # Set up request body
  body <- list(
    "validation_status.uid" = status
  )

  # Add "Token " prefix to token if it doesn't already have it
  if (!grepl("^Token ", token)) {
    token <- paste("Token", token)
  }

  # Set up request
  req <- httr2::request(url) %>%
    httr2::req_headers(
      Authorization = token,
      "Content-Type" = "application/json"
    ) %>%
    httr2::req_method("PATCH") %>%
    httr2::req_body_json(body)

  if (debug) {
    print(req)
    print(body)
  }

  tryCatch(
    {
      response <- httr2::req_perform(req)

      if (debug) {
        cat("Response status:", httr2::resp_status(response), "\n")
        cat("Response body:", httr2::resp_body_string(response), "\n")
      }

      if (httr2::resp_status(response) %in% c(200, 201, 204)) {
        # If update was successful, get the current validation status
        # Make sure to add the Token prefix for get_validation_status too
        original_token <- token
        if (grepl("^Token ", token)) {
          token <- gsub("^Token ", "", token)
        }

        updated_data <- get_validation_status(
          submission_id = submission_id,
          asset_id = asset_id,
          token = token,
          debug = debug
        )

        # Add success indicator
        updated_data %>%
          dplyr::mutate(update_success = TRUE)
      } else {
        dplyr::tibble(
          submission_id = submission_id,
          validation_status = NA_character_,
          validated_at = lubridate::as_datetime(NA),
          validated_by = NA_character_,
          update_success = FALSE
        )
      }
    },
    error = function(e) {
      if (debug) {
        cat("Error:", as.character(e), "\n")
      }

      dplyr::tibble(
        submission_id = submission_id,
        validation_status = NA_character_,
        validated_at = lubridate::as_datetime(NA),
        validated_by = NA_character_,
        update_success = FALSE
      )
    }
  )
}

#' Process Submissions with Rate-Limited Parallel API Calls
#'
#' @description
#' Helper function to process multiple submissions in parallel with rate limiting,
#' progress tracking, and error logging. This function is designed to handle
#' KoboToolbox API calls efficiently while respecting rate limits.
#'
#' @param submission_ids Character vector of submission IDs to process
#' @param process_fn Function to apply to each submission ID. Should return a data frame.
#' @param description Character string describing what's being processed (for logging)
#' @param rate_limit Numeric delay in seconds between requests (default: 0.2)
#'
#' @return Data frame with results from process_fn for all submissions
#'
#' @keywords internal
#' @export
process_submissions_parallel <- function(
  submission_ids,
  process_fn,
  description = "submissions",
  rate_limit = 0.2
) {
  logger::log_info("Processing {length(submission_ids)} {description}")

  progressr::with_progress({
    p <- progressr::progressor(along = submission_ids)

    results <- furrr::future_map_dfr(
      submission_ids,
      function(id) {
        # Add delay to respect rate limits
        Sys.sleep(rate_limit)

        result <- process_fn(id)
        p()
        result
      },
      .options = furrr::furrr_options(seed = TRUE)
    )

    # Log failures if update_success column exists
    if ("update_success" %in% names(results)) {
      failures <- results %>% dplyr::filter(!.data$update_success)
      if (nrow(failures) > 0) {
        logger::log_warn(
          "Failed to update {nrow(failures)} {description}: {paste(failures$submission_id, collapse = ', ')}"
        )
      } else {
        logger::log_info(
          "Successfully processed all {length(submission_ids)} {description}"
        )
      }
    }

    results
  })
}

#' Default Thresholds for Gleaning Survey Validation
#'
#' Returns the bounds used by [validate_gleaning_surveys()] for the Zanzibar
#' pipeline. Calibrated to the observed Zanzibar distributions, which differ from
#' Kenya: prices are in TZS, catch weight is reconstructed from the bucket /
#' plastic-bag container fields (so its errors are larger and need container-level
#' checks), and recall gaps run longer. Override any value by name, e.g.
#' `gleaning_validation_thresholds(total_catch_kg_max = 40)`.
#'
#' @param ... Named overrides for any default threshold.
#' @return A named list of thresholds.
#' @keywords validation
#' @export
gleaning_validation_thresholds <- function(...) {
  defaults <- list(
    age_min = 8, # years
    age_max = 90,
    trip_hours_max = 12, # gleaning trip length (h)
    days_week_max = 7,
    revenue_max = 200000, # daily revenue (TZS)
    total_catch_kg_max = 50, # per-submission catch weight (hand gleaning, shells only)
    total_individuals_max = 10000, # per-submission count
    n_individuals_max = 5000, # per size-class count
    unit_weight_capacity_factor = 1.5, # max unit_weight_kg / container_size_kg
    n_containers_max = 20, # full containers in one trip
    fuel_L_max = 100, # litres
    vessel_cost_max = 100000, # TZS
    recall_days_max = 45, # submission - landing gap
    project_start = as.Date("2025-01-01")
  )
  utils::modifyList(defaults, list(...))
}


#' Validate Preprocessed Zanzibar Gleaning Surveys and Build a Clean Dataset
#'
#' Flags unreasonable values in the preprocessed Zanzibar gleaning dataset (the
#' long skeleton from `preprocess_gleaning_surveys()`) and removes every
#' submission with at least one flag, since a bad value taints the whole record.
#'
#' Tailored to the Zanzibar instrument. In addition to the demographic / effort /
#' economic / temporal range checks, it includes \strong{container-plausibility}
#' checks that target this pipeline's main weakness — catch weight is derived as
#' `unit_weight_kg * n_containers`, so single-container weights exceeding the
#' container's nominal capacity, or absurd container counts, are the root cause of
#' the heavy-catch tail:
#' \itemize{
#'   \item `flag_unit_weight`  - `unit_weight_kg` > factor x `container_size_kg`
#'   \item `flag_n_containers` - implausible number of full containers
#' }
#'
#' Each check writes a `flag_*` logical column (TRUE = problem; NA values pass).
#' Flags are consolidated per row into `alert_n`, `alert_flag`, `alert_reasons`,
#' then rolled up to the submission for removal.
#'
#' @param log_threshold Logging threshold (default `logger::INFO`).
#'
#' @return A list with `validated` (input + flag columns + alert fields),
#'   `flagged_submissions` (one row per flagged submission with reasons),
#'   `clean` (original columns, flagged submissions removed), and `summary`
#'   (submissions tripping each check).
#' @export
#' @keywords validation workflow
#' @examples
#' \dontrun{
#' validate_gleaning_surveys()
#' v$summary
#' clean <- v$clean
#' }
validate_gleaning_surveys <- function(
  log_threshold = logger::INFO
) {
  logger::log_threshold(log_threshold)

  conf <- read_config()

  data <-
    coasts::download_parquet_from_cloud(
      prefix = conf$surveys$wf_gleaning$preprocessed$file_prefix,
      provider = conf$storage$google$key,
      options = conf$storage$google$options
    )

  th <- gleaning_validation_thresholds()
  n <- nrow(data)

  get_num <- function(name) {
    if (name %in% names(data)) as.double(data[[name]]) else rep(NA_real_, n)
  }
  get_date <- function(name) {
    if (name %in% names(data)) {
      lubridate::as_date(data[[name]])
    } else {
      as.Date(rep(NA, n))
    }
  }

  age <- get_num("age")
  trip <- get_num("trip_duration")
  days <- get_num("days_collection_week")
  rev <- get_num("catch_price")
  tck <- get_num("total_catch_kg")
  tind <- get_num("total_individuals")
  ni <- get_num("n_individuals")
  uw <- get_num("unit_weight_kg")
  cap <- get_num("container_size_kg")
  ncon <- get_num("n_containers")
  fuel <- get_num("fuel_L")
  vcost <- get_num("vessel_cost")
  land <- get_date("landing_date")
  subd <- get_date("submission_date")

  ff <- function(cond) dplyr::coalesce(cond, FALSE) # NA condition -> not flagged

  flags <- tibble::tibble(
    flag_age = ff(age < th$age_min | age > th$age_max),
    flag_trip_duration = ff(trip <= 0 | trip > th$trip_hours_max),
    flag_days_week = ff(days < 0 | days > th$days_week_max),
    flag_revenue = ff(rev < 0 | rev > th$revenue_max),
    flag_total_catch = ff(tck > th$total_catch_kg_max),
    flag_total_individuals = ff(tind > th$total_individuals_max),
    flag_n_individuals = ff(ni < 0 | ni > th$n_individuals_max),
    # --- Zanzibar container-plausibility checks ---
    flag_unit_weight = ff(uw > th$unit_weight_capacity_factor * cap),
    flag_n_containers = ff(ncon > th$n_containers_max),
    flag_fuel = ff(fuel > th$fuel_L_max),
    flag_vessel_cost = ff(vcost > th$vessel_cost_max),
    # --- temporal ---
    flag_date_order = ff(land > subd),
    flag_date_future = ff(land > Sys.Date() | subd > Sys.Date()),
    flag_date_recall = ff(as.numeric(subd - land) > th$recall_days_max),
    flag_date_range = ff(land < th$project_start)
  )
  flag_names <- names(flags)

  flags <- flags |>
    dplyr::mutate(
      alert_n = rowSums(dplyr::across(dplyr::all_of(flag_names))),
      alert_flag = .data$alert_n > 0,
      alert_reasons = apply(
        dplyr::across(dplyr::all_of(flag_names)),
        1,
        function(r) paste(sub("^flag_", "", flag_names)[r], collapse = ", ")
      )
    )

  validated <- dplyr::bind_cols(data, flags)

  flagged_submissions <- validated |>
    dplyr::filter(.data$alert_flag) |>
    dplyr::group_by(.data$submission_id) |>
    dplyr::summarise(
      n_flagged_rows = dplyr::n(),
      reasons = paste(
        sort(unique(unlist(
          stringr::str_split(.data$alert_reasons, ", ")
        ))),
        collapse = ", "
      ),
      .groups = "drop"
    )

  summary <- tibble::tibble(
    check = sub("^flag_", "", flag_names),
    submissions_flagged = purrr::map_int(flag_names, function(fn) {
      dplyr::n_distinct(validated$submission_id[validated[[fn]]])
    })
  ) |>
    dplyr::arrange(dplyr::desc(.data$submissions_flagged))

  clean <- validated |>
    dplyr::filter(
      !.data$submission_id %in% flagged_submissions$submission_id
    ) |>
    dplyr::select(
      -dplyr::all_of(c(flag_names, "alert_n", "alert_flag", "alert_reasons"))
    )

  logger::log_info(
    "Validation: flagged {nrow(flagged_submissions)} of {dplyr::n_distinct(data$submission_id)} submissions; clean set retains {dplyr::n_distinct(clean$submission_id)}."
  )

  validated_list <- list(
    validated = validated,
    flagged_submissions = flagged_submissions,
    clean = clean,
    summary = summary
  )

  coasts::upload_parquet_to_cloud(
    data = validated_list$clean,
    prefix = conf$surveys$wf_gleaning$validated$file_prefix,
    provider = conf$storage$google$key,
    options = conf$storage$google$options
  )
}
