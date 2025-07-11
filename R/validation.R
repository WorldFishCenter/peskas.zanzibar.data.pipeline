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
  pars <- read_config()

  # 1. Load and preprocess survey data
  preprocessed_surveys <- get_preprocessed_surveys(
    pars,
    prefix = pars$surveys$wcs_surveys$preprocessed_surveys$file_prefix
  ) |>
    dplyr::filter(!.data$trip_info == "no")

  # 2. Extract and process data
  trips_info <- extract_trips_info(preprocessed_surveys)
  catch_data <- process_catch_data(preprocessed_surveys, trips_info)
  validated <- validate_catches(catch_data)

  # 3. Calculate prices and revenue
  market_table <- validate_prices(preprocessed_surveys)
  catch_price_table <- calculate_catch_revenue(validated, market_table)

  # 4. Aggregate catches and calculate final metrics
  validated_surveys <- aggregate_survey_data(catch_price_table, trips_info)

  # 5. Save and upload results
  validated_filename <- pars$surveys$wcs_surveys$validated_surveys$file_prefix %>%
    add_version(extension = "parquet")

  arrow::write_parquet(
    x = validated_surveys,
    sink = validated_filename,
    compression = "lz4",
    compression_level = 12
  )

  logger::log_info("Uploading {validated_filename} to cloud storage")
  upload_cloud_file(
    file = validated_filename,
    provider = pars$storage$google$key,
    options = pars$storage$google$options
  )
}


#' Validate Wild Fishing Survey Data
#'
#' @description
#' Validates survey data from wild fishing activities by applying quality control checks
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
#' - Removes submissions that fail validation checks
#' - Sets catch_kg to 0 when catch_outcome is 0
#'
#' @section Data Processing Steps:
#' 1. Downloads preprocessed survey data
#' 2. Applies validation checks and generates alert flags
#' 3. Filters out submissions with validation alerts
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
  pars <- read_config()

  # 1. Load and preprocess survey data
  preprocessed_surveys <-
    download_parquet_from_cloud(
      prefix = pars$surveys$wf_surveys$preprocessed_surveys$file_prefix,
      provider = pars$storage$google$key,
      options = pars$storage$google$options
    )

  future::plan(
    strategy = future::multisession,
    workers = future::availableCores() - 2
  )

  # get validation table and store approved ids
  approved_ids <-
    unique(preprocessed_surveys$submission_id) %>%
    furrr::future_map_dfr(
      get_validation_status,
      asset_id = pars$surveys$wf_surveys$asset_id,
      token = pars$surveys$wf_surveys$token,
      .options = furrr::furrr_options(seed = TRUE)
    ) |>
    dplyr::filter(.data$validation_status == "validation_status_approved") |>
    dplyr::pull(.data$submission_id) |>
    unique()

  max_bucket_weight_kg <- 50
  max_n_buckets <- 300
  max_n_individuals <- 200
  price_kg_max <- 81420 # 30 eur
  cpue_max <- 30
  rpue_max <- 81420

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
      "submission_date",
      # dplyr::ends_with("fishers"),
      "catch_outcome",
      "catch_price",
      "catch_taxon",
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
        .data$cpue > cpue_max ~ "9",
        TRUE ~ NA_character_
      ),
      alert_rpue = dplyr::case_when(
        .data$rpue > rpue_max ~ "10",
        TRUE ~ NA_character_
      )
    ) |>
    dplyr::mutate(
      alert_flag_composite = paste(
        .data$alert_price_kg,
        .data$alert_cpue,
        .data$alert_rpue,
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
    dplyr::distinct() |>
    # keep approved submissions untouched
    dplyr::mutate(
      alert_flag = dplyr::if_else(
        .data$submission_id %in% approved_ids,
        NA_character_,
        .data$alert_flag
      )
    )

  upload_parquet_to_cloud(
    data = flags_combined,
    prefix = pars$surveys$wf_surveys$validation$flags$file_prefix,
    provider = pars$storage$google$key,
    options = pars$storage$google$options
  )

  upload_parquet_to_cloud(
    data = validated_data,
    prefix = pars$surveys$wf_surveys$validated_surveys$file_prefix,
    provider = pars$storage$google$key,
    options = pars$storage$google$options
  )
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
  pars <- read_config()

  preprocessed_surveys <-
    get_preprocessed_surveys(
      pars,
      prefix = pars$surveys$ba_surveys$preprocessed_surveys$file_prefix
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

  validated_filename <- pars$surveys$ba_surveys$validated_surveys$file_prefix %>%
    add_version(extension = "parquet")

  arrow::write_parquet(
    x = validated_surveys,
    sink = validated_filename,
    compression = "lz4",
    compression_level = 12
  )

  logger::log_info("Uploading {validated_filename} to cloud storage")
  upload_cloud_file(
    file = validated_filename,
    provider = pars$storage$google$key,
    options = pars$storage$google$options
  )
}

#' Synchronize Validation Statuses with KoboToolbox
#'
#' @description
#' Synchronizes validation statuses between the local system and KoboToolbox by processing
#' validation flags and updating submission statuses accordingly. This function handles
#' both flagged (not approved) and clean (approved) submissions in parallel.
#'
#' @details
#' The function follows these steps:
#' 1. Downloads the current validation flags from cloud storage
#' 2. Sets up parallel processing using the future package
#' 3. Processes submissions with alert flags (marking them as not approved in KoboToolbox)
#' 4. Processes submissions without alert flags (marking them as approved in KoboToolbox)
#' 5. Pushes all validation flags to MongoDB for record-keeping
#'
#' Progress reporting is enabled to track the status of submissions being processed.
#'
#' @param log_threshold The logging level threshold for the logger package (e.g., DEBUG, INFO).
#'        Default is logger::DEBUG.
#'
#' @return None. The function performs status updates and database operations as side effects.
#'
#' @section Parallel Processing:
#' The function uses the future and furrr packages for parallel processing, with the number
#' of workers set to system cores minus 2 to prevent resource exhaustion.
#'
#' @note
#' This function requires proper configuration in the config file, including:
#' - MongoDB connection parameters
#' - KoboToolbox asset ID and token
#' - Google cloud storage parameters
#'
#' @examples
#' \dontrun{
#' # Run with default DEBUG logging
#' sync_validation_submissions()
#'
#' # Run with INFO level logging
#' sync_validation_submissions(log_threshold = logger::INFO)
#' }
#'
#' @importFrom logger log_threshold log_info
#' @importFrom dplyr filter pull
#' @importFrom future plan multicore availableCores
#' @importFrom progressr handlers handler_progress with_progress progressor
#' @importFrom furrr future_walk
#'
#' @keywords workflow validation
#' @export
sync_validation_submissions <- function(log_threshold = logger::DEBUG) {
  logger::log_threshold(log_threshold)

  pars <- read_config()

  validation_flags <-
    download_parquet_from_cloud(
      prefix = pars$surveys$wf_surveys$validation$flags$file_prefix,
      provider = pars$storage$google$key,
      options = pars$storage$google$options
    )

  validation_flags_long <-
    validation_flags |>
    dplyr::mutate(alert_flag = as.character(.data$alert_flag)) %>%
    tidyr::separate_rows("alert_flag", sep = ",\\s*")

  # 1. First handle submissions with alert flags (mark as not approved)
  flagged_submissions <-
    validation_flags %>%
    dplyr::filter(!is.na(.data$alert_flag)) %>%
    dplyr::pull(.data$submission_id) %>%
    unique()

  future::plan(
    strategy = future::multisession,
    workers = future::availableCores() - 2
  )

  # Enable progress reporting globally
  progressr::handlers(progressr::handler_progress(
    format = "[:bar] :current/:total (:percent) eta: :eta"
  ))

  logger::log_info(
    "Processing {} submissions with alert flags",
    length(flagged_submissions)
  )

  progressr::with_progress({
    p_flagged <- progressr::progressor(along = flagged_submissions)

    flagged_submissions %>%
      furrr::future_walk(
        function(id) {
          update_validation_status(
            submission_id = id,
            asset_id = pars$surveys$wf_surveys$asset_id,
            token = pars$surveys$wf_surveys$token,
            status = "validation_status_not_approved"
          )
          p_flagged(message = paste("Marked not approved:", id))
        },
        .options = furrr::furrr_options(seed = TRUE)
      )
  })

  # 2. Now handle submissions without alert flags (mark as approved)
  clean_submissions <-
    validation_flags %>%
    dplyr::filter(is.na(.data$alert_flag)) %>%
    dplyr::pull(.data$submission_id) %>%
    unique()

  logger::log_info(
    "Processing {} submissions without alert flags",
    length(clean_submissions)
  )

  progressr::with_progress({
    p_clean <- progressr::progressor(along = clean_submissions)

    clean_submissions %>%
      furrr::future_walk(
        function(id) {
          update_validation_status(
            submission_id = id,
            asset_id = pars$surveys$wf_surveys$asset_id,
            token = pars$surveys$wf_surveys$token,
            status = "validation_status_approved"
          )
          p_clean(message = paste("Marked approved:", id))
        },
        .options = furrr::furrr_options(seed = TRUE)
      )
  })

  # Push the validation flags to MongoDB
  mdb_collection_push(
    data = validation_flags,
    connection_string = pars$storage$mongodb$connection_string,
    collection_name = pars$storage$mongodb$validation$collection$flags,
    db_name = pars$storage$mongodb$validation$database_name
  )

  # Push enumerators statistics to MongoDB
  mdb_collection_push(
    data = validation_flags_long,
    connection_string = pars$storage$mongodb$connection_string,
    collection_name = pars$storage$mongodb$validation$collection$enumerators_stats,
    db_name = pars$storage$mongodb$validation$database_name
  )

  logger::log_info("Validation synchronization completed successfully")
}
