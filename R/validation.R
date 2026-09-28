#' Validate WCS Surveys Data
#'
#' Validates Wildlife Conservation Society (WCS) survey data by performing quality checks and calculating catch metrics.
#' The function follows these main steps:
#' 1. Preprocesses survey data
#' 2. Validates catches using predefined thresholds for weights, counts and prices
#' 3. Calculates revenue and CPUE metrics
#' 4. Uploads validated data to cloud storage
#'
#' The validation includes:
#' - Basic data quality checks (e.g., negative catches, missing values)
#' - Gear-specific validations (e.g., number of fishers per gear type)
#' - Weight thresholds by catch type (individual vs bucket measures)
#' - Market price validations (valid price ranges per kg)
#'
#' @param log_threshold The logging level threshold for the logger package (e.g., DEBUG, INFO)
#' @return None. Writes validated data to parquet file and uploads to cloud storage
#' @keywords workflow validation
#' @export
validate_wcs_surveys <- function(log_threshold = logger::DEBUG) {
  logger::log_threshold(log_threshold)
  conf <- read_config()

  # Load and preprocess survey data
  preprocessed_surveys <-
    coasts::download_parquet_from_cloud(
      prefix = conf$surveys$wcs$preprocessed$file_prefix,
      provider = conf$storage$google$key,
      options = conf$storage$google$options
    ) |>
    dplyr::filter(.data$submission_date > "2020-01-01")

  max_bucket_weight_kg <- 50
  max_n_buckets <- 300
  max_n_individuals <- 200
  price_kg_max <- 78225 # Tanzanian Shilling -> 30 eur
  cpue_max <- 30
  rpue_max <- 78225

  catch_df <-
    preprocessed_surveys |>
    dplyr::select(
      "submission_id",
      "n_catch",
      "submission_date",
      "catch_price",
      catch_taxon = "alpha3_code",
      "individuals",
      "n_buckets",
      "weight_bucket",
      "catch_kg",
      "catch_outcome"
    )
  # dplyr::mutate(n_fishers = rowSums(across(c("no_men_fishers", "no_women_fishers", "no_child_fishers")),
  #                                 na.rm = TRUE)) |>
  # dplyr::select(-c("no_men_fishers", "no_women_fishers", "no_child_fishers")) |>
  # dplyr::relocate("n_fishers", .after = "has_boat")

  catch_flags <-
    catch_df |>
    dplyr::mutate(
      alert_form_incomplete = dplyr::case_when(
        !is.na(.data$catch_kg) & is.na(.data$catch_taxon) ~ "1",
        TRUE ~ NA_character_
      ),
      alert_catch_info_incomplete = dplyr::case_when(
        !is.na(.data$catch_taxon) &
          is.na(.data$n_buckets) &
          is.na(.data$catch_kg) &
          is.na(.data$individuals) ~ "2",
        TRUE ~ NA_character_
      ),
      # NEW: bucket count vs weight contradiction
      alert_bucket_contradiction = dplyr::case_when(
        !is.na(.data$n_buckets) &
          .data$n_buckets == 0 &
          !is.na(.data$weight_bucket) &
          .data$weight_bucket > 0 ~ "3",
        !is.na(.data$n_buckets) &
          .data$n_buckets > 0 &
          (is.na(.data$weight_bucket) | .data$weight_bucket == 0) ~ "3",
        TRUE ~ NA_character_
      ),
      # NEW: negative values in fields that must be non-negative
      # (zero catch_kg is handled separately by flag 11)
      alert_implausible_values = dplyr::case_when(
        (!is.na(.data$catch_kg) & .data$catch_kg < 0) |
          (!is.na(.data$weight_bucket) & .data$weight_bucket < 0) |
          (!is.na(.data$n_buckets) & .data$n_buckets < 0) |
          (!is.na(.data$individuals) & .data$individuals < 0) ~ "4",
        TRUE ~ NA_character_
      ),
      alert_bucket_weight = dplyr::case_when(
        !is.na(.data$weight_bucket) &
          .data$weight_bucket > max_bucket_weight_kg ~ "5",
        TRUE ~ NA_character_
      ),
      alert_n_buckets = dplyr::case_when(
        !is.na(.data$n_buckets) & .data$n_buckets > max_n_buckets ~ "6",
        TRUE ~ NA_character_
      ),
      alert_n_individuals = dplyr::case_when(
        !is.na(.data$individuals) & .data$individuals > max_n_individuals ~ "7",
        TRUE ~ NA_character_
      ),
      # NEW: taxon recorded but no weight — only meaningful when the trip has catch
      # (if catch_outcome = 0, all rows having NA/zero catch_kg is expected)
      alert_zero_or_missing_catch = dplyr::case_when(
        .data$catch_outcome == 1L &
          !is.na(.data$catch_taxon) &
          (is.na(.data$catch_kg) | .data$catch_kg == 0) ~ "11",
        TRUE ~ NA_character_
      )
    )

  flags_id <-
    catch_flags |>
    dplyr::select(
      "submission_id",
      "n_catch",
      "submission_date",
      dplyr::contains("alert_")
    ) |>
    dplyr::mutate(
      alert_flag = paste(
        .data$alert_bucket_weight,
        .data$alert_n_buckets,
        .data$alert_n_individuals,
        .data$alert_form_incomplete,
        .data$alert_catch_info_incomplete,
        .data$alert_bucket_contradiction,
        .data$alert_implausible_values,
        .data$alert_zero_or_missing_catch,
        sep = ","
      ) |>
        stringr::str_remove_all("NA,") |>
        stringr::str_remove_all(",NA") |>
        stringr::str_remove_all("^NA$")
    ) |>
    dplyr::mutate(
      alert_flag = ifelse(
        .data$alert_flag == "",
        NA_character_,
        .data$alert_flag
      ),
      submission_date = lubridate::as_datetime(.data$submission_date)
    ) |>
    dplyr::select(
      "submission_id",
      "n_catch",
      "submission_date",
      "alert_flag"
    ) |>
    dplyr::group_by(.data$submission_id) %>%
    # Summarize to get values
    dplyr::summarise(
      submission_date = dplyr::first(.data$submission_date),
      alert_flag = if (all(is.na(.data$alert_flag))) {
        NA_character_
      } else {
        paste(.data$alert_flag[!is.na(.data$alert_flag)], collapse = ", ")
      }
    ) %>%
    # Clean up empty strings
    dplyr::mutate(
      alert_flag = ifelse(
        .data$alert_flag == "",
        NA_character_,
        .data$alert_flag
      )
    )

  catch_df_validated <-
    catch_df |>
    dplyr::left_join(flags_id, by = c("submission_id", "submission_date")) |>
    dplyr::group_by(.data$submission_id) |>
    dplyr::mutate(
      submission_alerts = paste(
        unique(.data$alert_flag[!is.na(.data$alert_flag)]),
        collapse = ","
      )
    ) |>
    dplyr::mutate(
      submission_alerts = ifelse(
        .data$submission_alerts == "",
        NA_character_,
        .data$submission_alerts
      )
    ) |>
    dplyr::ungroup() |>
    dplyr::filter(is.na(.data$submission_alerts))

  surveys_basic_validated <-
    preprocessed_surveys |>
    dplyr::select(
      -c("catch_price", "individuals", "n_buckets", "weight_bucket", "catch_kg")
    ) |>
    dplyr::left_join(
      catch_df_validated,
      by = c(
        "submission_id",
        "submission_date",
        "n_catch",
        "alpha3_code" = "catch_taxon",
        "catch_outcome"
      )
    ) |>
    dplyr::rename(catch_taxon = "alpha3_code") |>
    dplyr::select(
      -c("alert_flag", "submission_alerts")
    )

  ### get flags for composite indicators ###
  no_flag_ids <-
    flags_id |>
    dplyr::filter(is.na(.data$alert_flag)) |>
    dplyr::select("submission_id") |>
    dplyr::distinct()

  indicators <-
    surveys_basic_validated |>
    dplyr::filter(.data$submission_id %in% no_flag_ids$submission_id) |>
    dplyr::select(
      "submission_id",
      "n_catch",
      "submission_date",
      "landing_site",
      "gear",
      "trip_length_hrs",
      "vessel_type",
      "n_fishers",
      "catch_taxon",
      "catch_price",
      "catch_kg",
      "catch_outcome"
    ) |>
    dplyr::group_by(.data$submission_id) |>
    dplyr::summarise(
      dplyr::across(
        .cols = c(
          "submission_date",
          "landing_site",
          "gear",
          "trip_length_hrs",
          "vessel_type",
          "n_fishers",
          "catch_outcome" # <-- add
        ),
        ~ dplyr::first(.x)
      ),
      catch_kg = sum(.data$catch_kg, na.rm = TRUE),
      catch_price = sum(.data$catch_price, na.rm = TRUE)
    ) |>
    dplyr::distinct() |>
    dplyr::transmute(
      submission_id = .data$submission_id,
      catch_outcome = .data$catch_outcome,
      trip_length_hrs = .data$trip_length_hrs,
      n_fishers = .data$n_fishers,
      # price_kg is meaningless (NaN) for zero-catch trips — suppress it
      price_kg = dplyr::if_else(
        .data$catch_outcome == 1L,
        .data$catch_price / .data$catch_kg,
        NA_real_
      ),
      cpue_day = (.data$catch_kg / .data$n_fishers) /
        (.data$trip_length_hrs / 24),
      cpue = (.data$catch_kg / .data$n_fishers) / .data$trip_length_hrs,
      rpue_day = (.data$catch_price / .data$n_fishers) /
        (.data$trip_length_hrs / 24),
      rpue = (.data$catch_price / .data$n_fishers) / .data$trip_length_hrs
    )

  composite_flags <-
    indicators |>
    dplyr::mutate(
      alert_price_kg = dplyr::case_when(
        !is.na(.data$price_kg) & .data$price_kg > price_kg_max ~ "8",
        TRUE ~ NA_character_
      ),
      alert_cpue = dplyr::case_when(
        !is.infinite(.data$cpue_day) & .data$cpue_day > cpue_max ~ "9",
        TRUE ~ NA_character_
      ),
      alert_rpue = dplyr::case_when(
        !is.infinite(.data$rpue_day) & .data$rpue_day > rpue_max ~ "10",
        TRUE ~ NA_character_
      ),
      # NEW: Inf indicators signal n_fishers = 0 or trip_length_hrs = 0
      alert_inf_indicators = dplyr::case_when(
        is.infinite(.data$cpue_day) | is.infinite(.data$rpue_day) ~ "12",
        TRUE ~ NA_character_
      ),
      # NEW: zero or negative fisher count makes all effort-based indicators invalid
      alert_zero_fishers = dplyr::case_when(
        !is.na(.data$n_fishers) & .data$n_fishers <= 0 ~ "13",
        TRUE ~ NA_character_
      )
    ) |>
    dplyr::mutate(
      alert_flag_composite = paste(
        .data$alert_price_kg,
        .data$alert_cpue,
        .data$alert_rpue,
        .data$alert_inf_indicators,
        .data$alert_zero_fishers,
        sep = ","
      ) |>
        stringr::str_remove_all("NA,") |>
        stringr::str_remove_all(",NA") |>
        stringr::str_remove_all("^NA$")
    ) |>
    dplyr::mutate(
      alert_flag_composite = ifelse(
        .data$alert_flag_composite == "",
        NA_character_,
        .data$alert_flag_composite
      )
    ) |>
    dplyr::select("submission_id", "alert_flag_composite")

  # bind new flags to flags dataframe
  flags_combined <-
    flags_id |>
    dplyr::full_join(composite_flags, by = "submission_id") |>
    dplyr::mutate(
      alert_flag = dplyr::case_when(
        # If both are non-NA, combine them
        !is.na(.data$alert_flag) & !is.na(.data$alert_flag_composite) ~
          paste(.data$alert_flag, .data$alert_flag_composite, sep = ", "),
        # If only one is non-NA, use that one
        is.na(.data$alert_flag) ~ .data$alert_flag_composite,
        is.na(.data$alert_flag_composite) ~ .data$alert_flag,
        # If both are NA, keep it NA
        TRUE ~ NA_character_
      )
    ) |>
    # Remove the now redundant alert_flag_composite column
    dplyr::select(-"alert_flag_composite") |>
    dplyr::left_join(
      surveys_basic_validated |>
        dplyr::select("submission_id", "submitted_by") |>
        dplyr::distinct(),
      by = "submission_id"
    ) |>
    dplyr::relocate("submitted_by", .after = "submission_id") |>
    dplyr::distinct()

  flags_ids <-
    flags_combined |>
    dplyr::filter(!is.na(.data$alert_flag)) |>
    dplyr::pull(.data$submission_id) |>
    unique()

  clean_landings <-
    surveys_basic_validated |>
    dplyr::filter(!.data$submission_id %in% flags_ids)

  coasts::upload_parquet_to_cloud(
    data = clean_landings,
    prefix = conf$surveys$wcs$validated$file_prefix,
    provider = conf$storage$google$key,
    options = conf$storage$google$options
  )
  # export_validation_flags(
  #   conf = conf,
  #   asset_id = conf$ingestion$wcs$asset_id,
  #   all_flags = flags_combined,
  #   validation_statuses = validation_statuses
  # )

  invisible(NULL)
}


#' Validate worldfish Survey Data
#'
#' @description
#' Validates survey data from worldfish activities by applying quality control checks
#' and flagging potential data issues. The function filters out submissions that don't
#' meet validation criteria and processes catch data.
#'
#' @details
#' The function applies the following validation checks:
#' 1. Bucket weight validation (max 50 kg per bucket)
#' 2. Number of buckets validation (max 300 buckets)
#' 3. Number of individuals validation (max 100 individuals)
#' 4. Form completeness check for catch details
#' 5. Catch information completeness check
#'
#' Alert codes:
#' - 5: Bucket weight exceeds maximum
#' - 6: Number of buckets exceeds maximum
#' - 7: Number of individuals exceeds maximum
#' - 8: Incomplete catch form
#' - 9: Incomplete catch information
#' - 12: Effort indicator is infinite (`n_fishers` or `trip_duration` is zero)
#' - 13: Fisher count is zero or negative
#'
#' @param log_threshold The logging level threshold for the logger package (e.g., DEBUG, INFO)
#' @return
#' The function processes and uploads two datasets to cloud storage:
#' 1. Validation flags for each submission
#' 2. Validated survey data with invalid submissions removed
#'
#' @note
#' - Requires configuration parameters to be set up in config file
#' - Automatically downloads preprocessed survey data from cloud storage
#' - Removes submissions that fail validation checks, unless a reviewer approved
#'   them (read with [coasts::review_decisions()])
#' - Sets catch_kg to 0 when catch_outcome is 0
#'
#' @section Data Processing Steps:
#' 1. Downloads preprocessed survey data and reads reviewers' decisions
#' 2. Applies validation checks and generates alert flags
#' 3. Filters out submissions with validation alerts, except those a reviewer
#'    approved
#' 4. Processes catch data and adjusts catch weights
#' 5. Uploads validation flags and validated data to cloud storage
#'
#' @importFrom logger log_threshold
#' @importFrom dplyr filter select mutate group_by ungroup left_join
#' @importFrom stringr str_remove_all
#'
#' @keywords workflow validation
#' @export
validate_wf_surveys <- function(log_threshold = logger::DEBUG) {
  logger::log_threshold(log_threshold)
  conf <- read_config()

  # 1. Load and preprocess survey data
  preprocessed_surveys <-
    coasts::download_parquet_from_cloud(
      prefix = conf$surveys$wf_v1$preprocessed$file_prefix,
      provider = conf$storage$google$key,
      options = conf$storage$google$options
    )

  # Reviewers' decisions for each form version, read before this run rewrites
  # the flags collections.
  # Named v1..v3, as export_validation_flags() looks them up below.
  wf <- stats::setNames(
    conf$ingestion[c("wf_v1", "wf_v2", "wf_v3")],
    c("v1", "v2", "v3")
  )
  pipeline_users <- unique(purrr::map_chr(wf, "username"))
  validation_statuses <-
    purrr::map(wf, function(ingestion) {
      coasts::review_decisions(
        flags = coasts::mdb_collection_pull(
          connection_string = conf$storage$mongodb$connection_strings$validation,
          db_name = conf$storage$mongodb$databases$validation$database_name,
          collection_name = paste(
            conf$storage$mongodb$databases$validation$collections$flags,
            ingestion$asset_id,
            sep = "-"
          )
        ),
        pipeline_users = pipeline_users,
        asset_id = ingestion$asset_id,
        token = ingestion$token
      )
    })
  decisions <- dplyr::bind_rows(validation_statuses)
  approved_ids <- decisions$submission_id[
    decisions$validation_status == "validation_status_approved"
  ]
  rejected_ids <- decisions$submission_id[
    decisions$validation_status == "validation_status_not_approved"
  ]

  max_bucket_weight_kg <- 50
  max_n_buckets <- 250
  max_n_individuals <- 500
  price_kg_max <- 81420 # 30 eur
  cpue_max <- 30
  rpue_max <- 81420
  max_length_cm <- 500

  catch_df <-
    preprocessed_surveys |>
    dplyr::filter(
      .data$survey_activity == "1" &
        .data$collect_data_today == "1" |
        .data$collect_data_today == "yes"
    ) |>
    dplyr::select(
      "submission_id",
      "n_catch",
      "landing_date",
      "submission_date",
      # dplyr::ends_with("fishers"),
      "catch_outcome",
      "catch_price",
      "fish_group",
      catch_taxon = "alpha3_code",
      "length",
      "min_length",
      "max_length_75",
      "individuals",
      "n_buckets",
      "weight_bucket",
      "catch_kg"
    )

  # dplyr::mutate(n_fishers = rowSums(across(c("no_men_fishers", "no_women_fishers", "no_child_fishers")),
  #                                 na.rm = TRUE)) |>
  # dplyr::select(-c("no_men_fishers", "no_women_fishers", "no_child_fishers")) |>
  # dplyr::relocate("n_fishers", .after = "has_boat")

  catch_flags <-
    catch_df |>
    dplyr::mutate(
      alert_form_incomplete = dplyr::case_when(
        .data$catch_outcome == "1" & is.na(.data$catch_taxon) ~ "1",
        TRUE ~ NA_character_
      ),
      alert_catch_info_incomplete = dplyr::case_when(
        !is.na(.data$catch_taxon) &
          is.na(.data$n_buckets) &
          is.na(.data$individuals) ~
          "2",
        TRUE ~ NA_character_
      ),
      alert_min_length = dplyr::case_when(
        .data$length < .data$min_length ~ "3",
        TRUE ~ NA_character_
      ),
      alert_max_length = dplyr::case_when(
        .data$length > .data$max_length_75 ~ "4",
        .data$length > max_length_cm ~ "4",
        TRUE ~ NA_character_
      ),
      alert_bucket_weight = dplyr::case_when(
        !is.na(.data$weight_bucket) &
          .data$weight_bucket > max_bucket_weight_kg ~
          "5",
        TRUE ~ NA_character_
      ),
      alert_n_buckets = dplyr::case_when(
        !is.na(.data$n_buckets) & .data$n_buckets > max_n_buckets ~ "6",
        TRUE ~ NA_character_
      ),
      alert_n_individuals = dplyr::case_when(
        !is.na(.data$individuals) & .data$individuals > max_n_individuals ~ "7",
        TRUE ~ NA_character_
      ),
      alert_n_date = dplyr::case_when(
        .data$landing_date > .data$submission_date ~ "11",
        TRUE ~ NA_character_
      )
    )

  flags_id <-
    catch_flags |>
    dplyr::select(
      "submission_id",
      "n_catch",
      "submission_date",
      dplyr::contains("alert_")
    ) |>
    dplyr::mutate(
      alert_flag = paste(
        .data$alert_min_length,
        .data$alert_max_length,
        .data$alert_bucket_weight,
        .data$alert_n_buckets,
        .data$alert_n_individuals,
        .data$alert_form_incomplete,
        .data$alert_catch_info_incomplete,
        .data$alert_n_date,
        sep = ","
      ) |>
        stringr::str_remove_all("NA,") |>
        stringr::str_remove_all(",NA") |>
        stringr::str_remove_all("^NA$")
    ) |>
    dplyr::mutate(
      alert_flag = ifelse(
        .data$alert_flag == "",
        NA_character_,
        .data$alert_flag
      ),
      submission_date = lubridate::as_datetime(.data$submission_date)
    ) |>
    dplyr::select(
      "submission_id",
      "n_catch",
      "submission_date",
      "alert_flag"
    ) |>
    dplyr::group_by(.data$submission_id) %>%
    # Summarize to get values
    dplyr::summarise(
      submission_date = dplyr::first(.data$submission_date),
      alert_flag = if (all(is.na(.data$alert_flag))) {
        NA_character_
      } else {
        paste(.data$alert_flag[!is.na(.data$alert_flag)], collapse = ", ")
      }
    ) %>%
    # Clean up empty strings
    dplyr::mutate(
      alert_flag = ifelse(
        .data$alert_flag == "",
        NA_character_,
        .data$alert_flag
      )
    )

  catch_df_validated <-
    catch_df |>
    dplyr::left_join(
      flags_id,
      by = c("submission_id", "submission_date")
    ) |>
    dplyr::group_by(.data$submission_id) |>
    dplyr::mutate(
      submission_alerts = paste(
        unique(.data$alert_flag[!is.na(.data$alert_flag)]),
        collapse = ","
      )
    ) |>
    dplyr::mutate(
      submission_alerts = ifelse(
        .data$submission_alerts == "",
        NA_character_,
        .data$submission_alerts
      )
    ) |>
    dplyr::ungroup() |>
    dplyr::filter(is.na(.data$submission_alerts))

  validated_data <-
    preprocessed_surveys |>
    dplyr::left_join(catch_df_validated) |>
    dplyr::select(
      -c("alert_flag", "submission_alerts", "min_length", "max_length_75", "n")
    ) |>
    # if catch outcome is 0 catch kg must be set to 0
    dplyr::mutate(
      catch_kg = dplyr::if_else(.data$catch_outcome == "0", 0, .data$catch_kg),
      catch_price = dplyr::if_else(
        .data$catch_outcome == "0",
        0,
        .data$catch_price
      )
    )

  ### get flags for composite indicators ###
  no_flag_ids <-
    flags_id |>
    dplyr::filter(is.na(.data$alert_flag)) |>
    dplyr::select("submission_id") |>
    dplyr::distinct()

  indicators <-
    validated_data |>
    dplyr::filter(.data$submission_id %in% no_flag_ids$submission_id) |>
    dplyr::mutate(
      n_fishers = .data$no_men_fishers +
        .data$no_women_fishers +
        .data$no_child_fishers
    ) |>
    dplyr::select(
      "submission_id",
      "catch_outcome",
      "landing_date",
      "district",
      "landing_site",
      "gear",
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
          "catch_outcome",
          "landing_date",
          "district",
          "landing_site",
          "gear",
          "trip_duration",
          "vessel_type",
          "n_fishers",
          "catch_price"
        ),
        ~ dplyr::first(.x)
      ),
      catch_kg = sum(.data$catch_kg)
    ) |>
    dplyr::transmute(
      submission_id = .data$submission_id,
      catch_outcome = .data$catch_outcome,
      n_fishers = .data$n_fishers,
      price_kg = .data$catch_price / .data$catch_kg,
      price_kg_USD = .data$price_kg * 0.00037,
      cpue = .data$catch_kg / .data$n_fishers / .data$trip_duration,
      rpue = .data$catch_price / .data$n_fishers / .data$trip_duration,
      rpue_USD = .data$rpue * 0.00037
    )

  composite_flags <-
    indicators |>
    dplyr::mutate(
      alert_price_kg = dplyr::case_when(
        .data$price_kg > price_kg_max ~ "8",
        TRUE ~ NA_character_
      ),
      alert_cpue = dplyr::case_when(
        !is.infinite(.data$cpue) & .data$cpue > cpue_max ~ "9",
        TRUE ~ NA_character_
      ),
      alert_rpue = dplyr::case_when(
        !is.infinite(.data$rpue) & .data$rpue > rpue_max ~ "10",
        TRUE ~ NA_character_
      ),
      # NEW: Inf indicators signal n_fishers = 0 or trip_duration = 0
      alert_inf_indicators = dplyr::case_when(
        is.infinite(.data$cpue) | is.infinite(.data$rpue) ~ "12",
        TRUE ~ NA_character_
      ),
      # NEW: zero or negative fisher count makes all effort-based indicators
      # invalid. A trip with nobody on it did not happen: the zero is the
      # enumerator's untouched default, not a count. This is deliberately not
      # narrowed by catch_outcome — that narrowing is what let the same defect
      # through on no-catch trips and published n_fishers = 0 against a schema
      # whose minimum is 1, with every per-fisher metric dividing into Inf.
      alert_zero_fishers = dplyr::case_when(
        !is.na(.data$n_fishers) & .data$n_fishers <= 0 ~ "13",
        TRUE ~ NA_character_
      )
    ) |>
    dplyr::mutate(
      alert_flag_composite = paste(
        .data$alert_price_kg,
        .data$alert_cpue,
        .data$alert_rpue,
        .data$alert_inf_indicators,
        .data$alert_zero_fishers,
        sep = ","
      ) |>
        stringr::str_remove_all("NA,") |>
        stringr::str_remove_all(",NA") |>
        stringr::str_remove_all("^NA$")
    ) |>
    dplyr::mutate(
      alert_flag_composite = ifelse(
        .data$alert_flag_composite == "",
        NA_character_,
        .data$alert_flag_composite
      )
    ) |>
    dplyr::select("submission_id", "alert_flag_composite")

  # bind new flags to flags dataframe
  flags_combined <-
    flags_id |>
    dplyr::full_join(composite_flags, by = "submission_id") |>
    dplyr::mutate(
      alert_flag = dplyr::case_when(
        # If both are non-NA, combine them
        !is.na(.data$alert_flag) & !is.na(.data$alert_flag_composite) ~
          paste(.data$alert_flag, .data$alert_flag_composite, sep = ", "),
        # If only one is non-NA, use that one
        is.na(.data$alert_flag) ~ .data$alert_flag_composite,
        is.na(.data$alert_flag_composite) ~ .data$alert_flag,
        # If both are NA, keep it NA
        TRUE ~ NA_character_
      )
    ) |>
    # Remove the now redundant alert_flag_composite column
    dplyr::select(-"alert_flag_composite") |>
    dplyr::left_join(
      validated_data |>
        dplyr::select("submission_id", "submitted_by") |>
        dplyr::distinct(),
      by = "submission_id"
    ) |>
    dplyr::relocate("submitted_by", .after = "submission_id") |>
    dplyr::distinct()

  # A reviewer's decision outranks the automatic flags, either way.
  flags_ids <-
    flags_combined |>
    dplyr::filter(
      (!is.na(.data$alert_flag) & !.data$submission_id %in% approved_ids) |
        .data$submission_id %in% rejected_ids
    ) |>
    dplyr::select("submission_id") |>
    dplyr::distinct()

  clean_data <-
    validated_data |>
    dplyr::filter(!.data$submission_id %in% flags_ids$submission_id)

  coasts::upload_parquet_to_cloud(
    data = clean_data,
    prefix = conf$surveys$wf_v1$validated$file_prefix,
    provider = conf$storage$google$key,
    options = conf$storage$google$options
  )

  flags_combined_versions <-
    preprocessed_surveys |>
    dplyr::select("submission_id", "survey_version") |>
    dplyr::right_join(flags_combined, by = "submission_id") %>%
    dplyr::distinct() %>%
    split(.$survey_version)

  purrr::walk(
    .x = names(flags_combined_versions),
    .f = ~ export_validation_flags(
      conf = conf,
      asset_id = paste0("v", .x),
      all_flags = flags_combined_versions[[.x]],
      validation_statuses = validation_statuses[[paste0("v", .x)]]
    )
  )

  invisible(NULL)
}


#' Validate Blue Alliance (BA) Surveys Data
#'
#' Validates Blue Alliance survey data by performing quality checks and calculating catch metrics.
#' The function follows these main steps:
#' 1. Loads and preprocesses survey data
#' 2. Performs logical checks on key variables
#' 3. Calculates catch and length bounds
#' 4. Flags potential data quality issues
#' 5. Saves and uploads validated data
#'
#' The validation includes:
#' - Logical checks (non-negative catches, valid fisher counts, valid trip durations)
#' - Statistical outlier detection for catch weights and lengths
#' - Automated flagging system for quality control
#'
#' Alert flag descriptions:
#' - 1: Total catch is negative
#' - 2: Number of fishers is 0 or negative
#' - 3: Trip duration is 0 or negative
#' - 4: Catch weight or length exceeds calculated bounds
#'
#' @param log_threshold The logging level threshold for the logger package (e.g., DEBUG, INFO)
#' @return None. Writes validated data to parquet file and uploads to cloud storage
#' @keywords workflow validation
#' @export
validate_ba_surveys <- function(log_threshold = logger::DEBUG) {
  conf <- read_config()

  preprocessed_surveys <-
    coasts::download_parquet_from_cloud(
      prefix = conf$surveys$ba$preprocessed$file_prefix,
      provider = conf$storage$google$key,
      options = conf$storage$google$options
    ) |>
    dplyr::arrange(.data$survey_id)

  logical_check_flags <-
    preprocessed_surveys |>
    dplyr::group_by(.data$survey_id) |>
    dplyr::mutate(total_catch_kg = sum(.data$catch_kg)) |>
    dplyr::ungroup() |>
    dplyr::select(
      "survey_id",
      "n_fishers",
      "trip_duration",
      "total_catch_kg"
    ) |>
    dplyr::distinct() |>
    dplyr::mutate(
      alert_flag = dplyr::case_when(
        # Condition 1: Total Catch cannot be negative
        .data$total_catch_kg < 0 ~ "1",
        # Condition 2: No. of Fishers cannot be 0 or negative
        .data$n_fishers <= 0 ~ "2",
        # Condition 2: trip_duration cannot be 0 or negative
        .data$trip_duration <= 0 ~ "3",
        TRUE ~ NA_character_
      )
    ) |>
    dplyr::select("survey_id", "alert_flag")

  clean_logic <-
    preprocessed_surveys |>
    dplyr::left_join(logical_check_flags, by = "survey_id") |>
    dplyr::filter(is.na(.data$alert_flag))

  catch_bounds <- get_catch_bounds(data = clean_logic, k_param = 5)
  length_bounds <- get_length_bounds(data = clean_logic, k_param = 5)

  bounds <- dplyr::full_join(
    catch_bounds,
    length_bounds,
    by = c("gear", "catch_taxon")
  )

  catch_clean <-
    clean_logic |>
    dplyr::left_join(bounds, by = c("gear", "catch_taxon")) |>
    dplyr::rowwise() |>
    dplyr::mutate(
      alert_catch = ifelse(
        .data$catch_kg > .data$upper_catch,
        "4",
        NA_character_
      ),
      alert_length = ifelse(
        .data$length_cm > .data$upper_length,
        "4",
        NA_character_
      )
    ) |>
    dplyr::group_by(.data$survey_id) %>%
    dplyr::mutate(
      alert_catch_survey = max(as.numeric(.data$alert_catch), na.rm = TRUE),
      alert_catch_survey = ifelse(
        .data$alert_catch_survey == -Inf,
        NA,
        .data$alert_catch_survey
      ),
      alert_length_survey = max(as.numeric(.data$alert_length), na.rm = TRUE),
      alert_length_survey = ifelse(
        .data$alert_length_survey == -Inf,
        NA,
        .data$alert_length_survey
      )
    ) |>
    dplyr::ungroup()

  flags_df <-
    catch_clean |>
    dplyr::select(
      "survey_id",
      "alert_flag",
      "alert_catch_survey",
      "alert_length_survey"
    ) |>
    dplyr::mutate(
      alert_catch_survey = as.character(.data$alert_catch_survey),
      alert_length_survey = as.character(.data$alert_length_survey),
      alert_flag = dplyr::coalesce(
        .data$alert_flag,
        .data$alert_catch_survey,
        .data$alert_length_survey
      )
    ) |>
    dplyr::select(-c("alert_catch_survey", "alert_length_survey")) |>
    dplyr::ungroup() |>
    dplyr::distinct()

  validated_surveys <-
    catch_clean |>
    dplyr::select(
      -c(
        "fisher_id",
        "local_name",
        "alert_flag",
        "upper_catch",
        "upper_length",
        "alert_catch",
        "alert_catch_survey",
        "alert_length",
        "alert_length_survey"
      )
    )

  validated_filename <- conf$surveys$ba$validated$file_prefix %>%
    add_version(extension = "parquet")

  arrow::write_parquet(
    x = validated_surveys,
    sink = validated_filename,
    compression = "lz4",
    compression_level = 12
  )

  logger::log_info("Uploading {validated_filename} to cloud storage")
  coasts::upload_cloud_file(
    file = validated_filename,
    provider = conf$storage$google$key,
    options = conf$storage$google$options
  )
}

#' Export Validation Flags to MongoDB
#'
#' @description
#' Exports validation flags to MongoDB, keeping the decisions reviewers made in
#' the Peskas Management Platform or in KoboToolbox (read beforehand with
#' `coasts::review_decisions()`).
#'
#' @details
#' The function performs the following steps:
#' \enumerate{
#'   \item Joins validation flags with KoboToolbox validation statuses
#'   \item Identifies manual human approvals (excluding system username)
#'   \item Preserves manual human decisions while updating system-generated statuses
#'   \item Creates both wide and long format datasets for different reporting needs
#'   \item Pushes results directly to MongoDB collections
#' }
#'
#' \strong{Validation Status Logic:}
#' \itemize{
#'   \item If submission has flags AND validated_by is system username: set to "not_approved"
#'   \item If submission has no flags AND validated_by is system username: set to "approved"
#'   \item If validated_by is NOT system username: preserve existing status (a reviewer's approval or rejection)
#' }
#'
#' @param conf Configuration object from `read_config()` containing MongoDB connection
#'   parameters and survey-specific settings
#' @param asset_id Character string specifying which WF survey version to process.
#'   Must be one of "v1", "v2" or "v3". Determines which configuration to use from
#'   `conf$ingestion$wf_{asset_id}`. Default is "v1".
#' @param all_flags Data frame containing all validation flags with columns:
#'   `submission_id`, `submitted_by`, `submission_date`, `alert_flag`
#' @param validation_statuses Reviewers' decisions from `coasts::review_decisions()`,
#'   with columns `submission_id`, `validation_status`, `validated_at`, `validated_by`
#'
#' @return Invisible NULL. The function pushes data to MongoDB as a side effect.
#'
#' @section MongoDB Collections:
#' The function pushes to two MongoDB collections, named with the survey's KoBo
#' asset id:
#' \describe{
#'   \item{surveys_flags-{asset_id}}{Wide format with one row per submission including
#'     validation status and flags}
#'   \item{enumerators_stats-{asset_id}}{Long format with one row per flag per
#'     submission for enumerator statistics}
#' }
#'
#' @note
#' This function is called internally by `validate_wf_surveys()` and should not
#' typically be called directly. It requires:
#' \itemize{
#'   \item Valid configuration with MongoDB connection string
#'   \item Survey-specific configuration under `conf$ingestion$wf_{asset_id}`
#'   \item System username configured to identify automated vs. manual validations
#' }
#'
#' @examples
#' \dontrun{
#' # Called internally by validate_wf_surveys()
#' export_validation_flags(
#'   conf = conf,
#'   asset_id = "v1",
#'   all_flags = flags_combined,
#'   validation_statuses = validation_statuses
#' )
#' }
#'
#'
#' @keywords validation workflow
#' @export
export_validation_flags <- function(
  conf = NULL,
  asset_id = c("v1", "v2", "v3"),
  all_flags = NULL,
  validation_statuses = NULL
) {
  asset_id <- match.arg(asset_id)
  config_key <- paste0("wf_", asset_id)

  # Get the survey-specific config
  survey_conf <- conf$ingestion[[config_key]]

  validation_flags_with_kobo_status <-
    all_flags |>
    dplyr::full_join(validation_statuses, by = "submission_id") |>
    dplyr::mutate(
      # The pipeline signs an unflagged submission, unless a reviewer decided it.
      validated_by = dplyr::if_else(
        is.na(.data$alert_flag) & is.na(.data$validated_by),
        conf$ingestion$wf_v1$username,
        .data$validated_by
      ),
      validation_status = dplyr::case_when(
        # Preserve existing status if validated by someone else (not pipeline account user and not NA)
        !is.na(.data$validated_by) &
          .data$validated_by !=
            conf$ingestion$wf_v1$username ~ .data$validation_status,
        # Apply new status only if validated_by is NA or matches kobo user
        !is.na(.data$alert_flag) ~ "validation_status_not_approved",
        is.na(.data$alert_flag) ~ "validation_status_approved",
        TRUE ~ .data$validation_status
      )
    ) |>
    dplyr::filter(!is.na(.data$submitted_by))

  validation_flags_long <- validation_flags_with_kobo_status |>
    dplyr::mutate(alert_flag = as.character(.data$alert_flag)) %>%
    tidyr::separate_rows("alert_flag", sep = ",\\s*") |>
    dplyr::select(-c(dplyr::starts_with("valid")))

  asset_id <- survey_conf$asset_id

  coasts::mdb_collection_push(
    data = validation_flags_with_kobo_status,
    connection_string = conf$storage$mongodb$connection_strings$validation,
    db_name = conf$storage$mongodb$databases$validation$database_name,
    collection_name = paste(
      conf$storage$mongodb$databases$validation$collections$flags,
      asset_id,
      sep = "-"
    )
  )

  coasts::mdb_collection_push(
    data = validation_flags_long,
    connection_string = conf$storage$mongodb$connection_strings$validation,
    db_name = conf$storage$mongodb$databases$validation$database_name,
    collection_name = paste(
      conf$storage$mongodb$databases$validation$collections$enumerators_stats,
      asset_id,
      sep = "-"
    )
  )

  logger::log_info("Validation synchronization completed successfully")
  invisible(NULL)
}
