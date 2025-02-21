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
  preprocessed_surveys <- get_preprocessed_surveys(pars,
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

  max_bucket_weight_kg <- 50
  max_n_buckets <- 300
  max_n_individuals <- 100
  price_kg_max <- 81420 # 30 eur

  catch_df <-
    preprocessed_surveys |>
    dplyr::filter(.data$survey_activity == "1" & .data$collect_data_today == "1") |>
    dplyr::select(
      "submission_id", "n_catch",
      # dplyr::ends_with("fishers"),
      "catch_outcome",
      "catch_taxon", "length", "individuals", "n_buckets", "weight_bucket", "catch_kg"
    )
  # dplyr::mutate(n_fishers = rowSums(across(c("no_men_fishers", "no_women_fishers", "no_child_fishers")),
  #                                 na.rm = TRUE)) |>
  # dplyr::select(-c("no_men_fishers", "no_women_fishers", "no_child_fishers")) |>
  # dplyr::relocate("n_fishers", .after = "has_boat")

  catch_flags <-
    catch_df |>
    dplyr::mutate(
      alert_bucket_weight = dplyr::case_when(
        !is.na(.data$weight_bucket) & .data$weight_bucket > max_bucket_weight_kg ~ "5",
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
      alert_form_incomplete = dplyr::case_when(
        .data$catch_outcome == "1" & is.na(.data$catch_taxon) ~ "8",
        TRUE ~ NA_character_
      ),
      alert_catch_info_incomplete = dplyr::case_when(
        !is.na(.data$catch_taxon) & is.na(.data$n_buckets) & is.na(.data$individuals) ~ "9",
        TRUE ~ NA_character_
      )
    )

  flags_id <-
    catch_flags |>
    dplyr::select("submission_id", "n_catch", dplyr::contains("alert_")) |>
    dplyr::mutate(
      alert_flag = paste(
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
    dplyr::mutate(alert_flag = ifelse(.data$alert_flag == "", NA_character_, .data$alert_flag)) |>
    dplyr::select("submission_id", "n_catch", "alert_flag")

  catch_df_validated <-
    catch_df |>
    dplyr::left_join(flags_id, by = c("submission_id", "n_catch")) |>
    dplyr::group_by(.data$submission_id) |>
    dplyr::mutate(
      submission_alerts = paste(unique(.data$alert_flag[!is.na(.data$alert_flag)]), collapse = ",")
    ) |>
    dplyr::mutate(
      submission_alerts = ifelse(.data$submission_alerts == "", NA_character_, .data$submission_alerts)
    ) |>
    dplyr::ungroup() |>
    dplyr::filter(is.na(.data$submission_alerts)) |>
    dplyr::select(-c("alert_flag", "submission_alerts")) |>
    # if catch outcome is 0 catch kg must be set to 0
    dplyr::mutate(catch_kg = ifelse(.data$catch_outcome == "0", 0, .data$catch_kg))
  
  validated_data <- 
    preprocessed_surveys |> 
    dplyr::left_join(catch_df_validated)
  
    upload_parquet_to_cloud(
      data = flags_id,
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
    get_preprocessed_surveys(pars,
      prefix = pars$surveys$ba_surveys$preprocessed_surveys$file_prefix
    ) |>
    dplyr::arrange(.data$survey_id)

  logical_check_flags <-
    preprocessed_surveys |>
    dplyr::group_by(.data$survey_id) |>
    dplyr::mutate(total_catch_kg = sum(.data$catch_kg)) |>
    dplyr::ungroup() |>
    dplyr::select("survey_id", "n_fishers", "trip_duration", "total_catch_kg") |>
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

  bounds <- dplyr::full_join(catch_bounds, length_bounds, by = c("gear", "catch_taxon"))

  catch_clean <-
    clean_logic |>
    dplyr::left_join(bounds, by = c("gear", "catch_taxon")) |>
    dplyr::rowwise() |>
    dplyr::mutate(
      alert_catch = ifelse(.data$catch_kg > .data$upper_catch, "4", NA_character_),
      alert_length = ifelse(.data$length_cm > .data$upper_length, "4", NA_character_)
    ) |>
    dplyr::group_by(.data$survey_id) %>%
    dplyr::mutate(
      alert_catch_survey = max(as.numeric(.data$alert_catch), na.rm = TRUE),
      alert_catch_survey = ifelse(.data$alert_catch_survey == -Inf, NA, .data$alert_catch_survey),
      alert_length_survey = max(as.numeric(.data$alert_length), na.rm = TRUE),
      alert_length_survey = ifelse(.data$alert_length_survey == -Inf, NA, .data$alert_length_survey)
    ) |>
    dplyr::ungroup()


  flags_df <-
    catch_clean |>
    dplyr::select("survey_id", "alert_flag", "alert_catch_survey", "alert_length_survey") |>
    dplyr::mutate(
      alert_catch_survey = as.character(.data$alert_catch_survey),
      alert_length_survey = as.character(.data$alert_length_survey),
      alert_flag = dplyr::coalesce(.data$alert_flag, .data$alert_catch_survey, .data$alert_length_survey)
    ) |>
    dplyr::select(-c("alert_catch_survey", "alert_length_survey")) |>
    dplyr::ungroup() |>
    dplyr::distinct()

  validated_surveys <-
    catch_clean |>
    dplyr::select(-c(
      "fisher_id", "local_name", "alert_flag", "upper_catch", "upper_length",
      "alert_catch", "alert_catch_survey", "alert_length", "alert_length_survey"
    ))

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
