#' Pre-process Zanzibar WCS Surveys
#'
#' Downloads and preprocesses raw structured WCS survey data from cloud storage into a binary format. The process includes nesting multiple columns related to species information into single columns within a dataframe, which helps reduce its width and organize data efficiently for analysis.
#'
#' Configurations are read from `conf.yml` with the following necessary parameters:
#'
#' ```
#' surveys:
#'   wcs_surveys:
#'     asset_id:
#'     username:
#'     password:
#'     file_prefix:
#'   version:
#'     preprocess:
#' storage:
#'   storage_name:
#'     key:
#'     options:
#'       project:
#'       bucket:
#'       service_account_key:
#' ```
#'
#' The function uses logging to track progress.
#'
#' @inheritParams ingest_surveys
#' @return None; the function is used for its side effects.
#' @export
#' @keywords workflow preprocessing
#' @seealso \code{\link[=pt_nest_trip]{pt_nest_trip}}, \code{\link[=pt_nest_catch]{pt_nest_catch}},
#' \code{\link[=pt_nest_length]{pt_nest_length}}, \code{\link[=pt_nest_market]{pt_nest_market}},
#' \code{\link[=pt_nest_attachments]{pt_nest_attachments}}
#'
preprocess_wcs_surveys <- function(log_threshold = logger::DEBUG) {
  logger::log_threshold(log_threshold)

  pars <- read_config()

  catch_surveys_raw <-
    download_parquet_from_cloud(
      prefix = pars$surveys$wcs_surveys$raw_surveys$file_prefix,
      provider = pars$storage$google$key,
      options = pars$storage$google$options
    ) |>
    dplyr::filter(.data$survey_real == "real")

  other_info <-
    catch_surveys_raw %>%
    tidyr::separate(
      .data$gps,
      into = c("lat", "lon", "drop1", "drop2"),
      sep = " "
    ) %>%
    dplyr::select(
      "survey_id" = "_id",
      submission_date = "today",
      "survey_type",
      "landing_site",
      "lat",
      "lon",
      "trip_info",
      n_fishers = "people",
      "boats_landed"
    ) |>
    dplyr::mutate(
      submission_date = lubridate::with_tz(
        .data$submission_date,
        "Africa/Dar_es_Salaam"
      ),
      submission_date = as.Date(.data$submission_date),
      dplyr::across(
        .cols = c(
          "lat",
          "lon",
          "n_fishers",
          "boats_landed"
        ),
        ~ as.numeric(.x)
      )
    )

  logger::log_info("Nesting survey groups' fields")
  group_surveys <-
    list(
      survey_trip = pt_nest_trip(catch_surveys_raw),
      other_info = other_info,
      survey_catch = pt_nest_catch(catch_surveys_raw),
      # survey_length = pt_nest_length(catch_surveys_raw),
      survey_market = pt_nest_market(catch_surveys_raw),
      survey_attachments = pt_nest_attachments(catch_surveys_raw)
    )

  wcs_surveys_nested <- purrr::reduce(
    group_surveys,
    ~ dplyr::full_join(.x, .y, by = "survey_id")
  )

  upload_parquet_to_cloud(
    data = wcs_surveys_nested,
    prefix = pars$surveys$wcs_surveys$preprocessed_surveys$file_prefix,
    provider = pars$storage$google$key,
    options = pars$storage$google$options
  )
}

#' Pre-process and Combine WorldFish Surveys - Both Versions
#'
#' Downloads and preprocesses raw structured WorldFish survey data from both version 1 and
#' version 2 sources, combines the results, and uploads a unified dataset. The function
#' automatically handles different survey structures and merges the processed data.
#'
#' @details
#' **Processing Workflow:**
#' 1. Downloads and processes Version 1 surveys (if available)
#' 2. Downloads and processes Version 2 surveys (if available)
#' 3. Combines (binds) both datasets into a unified result
#' 4. Uploads the combined dataset to cloud storage
#'
#' **Survey Version Differences:**
#' - **Version 1**: Single species field, nested length groups within species groups
#' - **Version 2**: Multiple species fields (species_TL, species_RF, etc.), separate
#'   repeated group for fish >100cm (`species_group/no_fish_by_length_group_100/`)
#'
#' The function uses `reshape_catch_data_v2()` which automatically detects survey version
#' based on column patterns and applies appropriate processing logic for each dataset.
#'
#' **Error Handling:**
#' If one version fails to process or is unavailable, the function continues with the
#' available data and logs warnings. The function only fails if both versions are unavailable.
#'
#' Configurations are read from `conf.yml` with the following necessary parameters:
#'
#' ```
#' surveys:
#'   wf_surveys:
#'     raw_surveys:
#'       file_prefix:
#'     preprocessed_surveys:
#'       file_prefix:
#'   wf_surveys_v2:
#'     raw_surveys:
#'       file_prefix:
#' storage:
#'   google:
#'     key:
#'     options:
#'       project:
#'       bucket:
#'       service_account_key:
#' ```
#'
#' @inheritParams ingest_surveys
#' @param version Character string, deprecated. Function now processes both versions automatically.
#' @return None; the function is used for its side effects.
#' @export
#' @keywords workflow preprocessing
#' @seealso \code{\link{reshape_catch_data_v2}}, \code{\link{preprocess_general}}, \code{\link{preprocess_catch}}, \code{\link{process_version_data}}
#'
#' @examples
#' \dontrun{
#' # Process and combine both survey versions
#' preprocess_wf_surveys()
#'
#' # With custom logging threshold
#' preprocess_wf_surveys(log_threshold = logger::INFO)
#' }
#'
preprocess_wf_surveys <- function(
  log_threshold = logger::DEBUG,
  version = "v2"
) {
  logger::log_threshold(log_threshold)

  pars <- read_config()
  asfis <- download_parquet_from_cloud(
    prefix = "asfis",
    provider = pars$storage$google$key,
    options = pars$storage$google$options
  )

  # metadata <- get_metadata()

  # Process both versions and combine results
  logger::log_info("Processing both survey versions and combining results")

  combined_data <- list()

  # Process Version 1 if available
  v1_data <- tryCatch(
    {
      logger::log_info("Processing Version 1 surveys...")

      catch_surveys_raw_v1 <-
        download_parquet_from_cloud(
          prefix = pars$surveys$wf_surveys_v1$raw_surveys$file_prefix,
          provider = pars$storage$google$key,
          options = pars$storage$google$options
        ) |>
        dplyr::select(-dplyr::starts_with("_att")) |>
        dplyr::rename(submission_id = "_id")

      general_info_v1 <- preprocess_general(data = catch_surveys_raw_v1)
      catch_info_v1 <- preprocess_catch(
        data = catch_surveys_raw_v1,
        version = "v1"
      )

      # Calculate catch data for v1
      process_version_data(catch_info_v1, general_info_v1, asfis)
    },
    error = function(e) {
      logger::log_warn(
        "Version 1 data not available or failed to process: {e$message}"
      )
      NULL
    }
  )

  # Process Version 2 if available
  v2_data <- tryCatch(
    {
      logger::log_info("Processing Version 2 surveys...")

      catch_surveys_raw_v2 <-
        download_parquet_from_cloud(
          prefix = pars$surveys$wf_surveys_v2$raw_surveys$file_prefix,
          provider = pars$storage$google$key,
          options = pars$storage$google$options
        ) |>
        dplyr::select(-dplyr::starts_with("_att")) |>
        dplyr::rename(submission_id = "_id")

      general_info_v2 <- preprocess_general(data = catch_surveys_raw_v2)
      catch_info_v2 <- preprocess_catch(
        data = catch_surveys_raw_v2,
        version = "v2"
      )

      # Calculate catch data for v2
      process_version_data(catch_info_v2, general_info_v2, asfis)
    },
    error = function(e) {
      logger::log_warn(
        "Version 2 data not available or failed to process: {e$message}"
      )
      NULL
    }
  )

  # Combine available datasets
  if (!is.null(v1_data) && !is.null(v2_data)) {
    logger::log_info("Combining Version 1 and Version 2 datasets")
    preprocessed_data <- dplyr::bind_rows(v1_data, v2_data)
  } else if (!is.null(v1_data)) {
    logger::log_info("Using only Version 1 data")
    preprocessed_data <- v1_data
  } else if (!is.null(v2_data)) {
    logger::log_info("Using only Version 2 data")
    preprocessed_data <- v2_data
  } else {
    stop("No survey data could be processed from either version")
  }

  # Sort final combined data
  preprocessed_data <- preprocessed_data |>
    dplyr::arrange(.data$submission_id, .data$n_catch)

  logger::log_info(
    "Combined dataset has {nrow(preprocessed_data)} rows from {length(unique(preprocessed_data$submission_id))} submissions"
  )

  upload_parquet_to_cloud(
    data = preprocessed_data,
    prefix = pars$surveys$wf_surveys_v1$preprocessed_surveys$file_prefix,
    provider = pars$storage$google$key,
    options = pars$storage$google$options
  )
}

#' Process Version Data Helper Function
#'
#' Internal helper function to process catch and general info for a specific survey version
#'
#' @param catch_info Processed catch information
#' @param general_info Processed general information
#' @param asfis ASFIS species data
#'
#' @return Combined and processed survey data
#' @keywords internal
process_version_data <- function(catch_info, general_info, asfis) {
  # Try to get length-weight coefficients from Rfishbase
  lwcoeffs <- tryCatch(
    {
      getLWCoeffs(
        taxa_list = unique(catch_info$catch_taxon),
        asfis_list = asfis
      )
    },
    error = function(e) {
      message("Error in getLWCoeffs, using local fallback: ", e$message)
      # Fallback to local data
      readr::read_rds(system.file(
        "length_weight_params.rds",
        package = "peskas.zanzibar.data.pipeline"
      ))
    }
  )

  # add flying fish estimates
  fly_lwcoeffs <- dplyr::tibble(
    catch_taxon = "FLY",
    n = 0,
    a_6 = 0.00631,
    b_6 = 3.05
  )
  lwcoeffs$lw <- dplyr::bind_rows(lwcoeffs$lw, fly_lwcoeffs)

  catch_df <-
    calculate_catch(catch_data = catch_info, lwcoeffs = lwcoeffs$lw) |>
    dplyr::left_join(lwcoeffs$ml, by = "catch_taxon") |>
    dplyr::select(-"max_weightkg_75")

  preprocessed_data <-
    dplyr::left_join(general_info, catch_df, by = "submission_id") |>
    dplyr::arrange(.data$submission_id, .data$n_catch)

  return(preprocessed_data)
}

#' Pre-process Blue Alliance Surveys
#'
#' Downloads and preprocesses raw structured Blue Alliance survey data from cloud storage into a binary format.
#' The process includes date standardization and survey ID generation for unique trip identification.
#'
#' Configurations are read from `conf.yml` with the following necessary parameters:
#'
#' ```
#' surveys:
#'   ba_surveys:
#'     asset_id:
#'     username:
#'     password:
#'     file_prefix:
#'   version:
#'     preprocess:
#' storage:
#'   storage_name:
#'     key:
#'     options:
#'       project:
#'       bucket:
#'       service_account_key:
#' ```
#'
#' The function uses logging to track progress and creates unique survey IDs using CRC32 hashing
#' of concatenated trip attributes.
#'
#' @inheritParams ingest_surveys
#' @return None; the function is used for its side effects.
#' @export
#' @keywords workflow preprocessing
#'
preprocess_ba_surveys <- function(log_threshold = logger::DEBUG) {
  logger::log_threshold(log_threshold)

  pars <- read_config()

  ba_surveys_csv <- cloud_object_name(
    prefix = pars$surveys$ba_surveys$raw_surveys$file_prefix,
    provider = pars$storage$google$key,
    extension = "csv",
    version = pars$surveys$ba_surveys$version$preprocess,
    options = pars$storage$google$options
  )

  logger::log_info("Retrieving {ba_surveys_csv}")
  download_cloud_file(
    name = ba_surveys_csv,
    provider = pars$storage$google$key,
    options = pars$storage$google$options
  )

  catch_surveys_preprocessed <-
    readr::read_csv2(ba_surveys_csv, show_col_types = FALSE) |>
    dplyr::mutate(date = paste(.data$date, .data$year)) %>% # Combine date and year
    dplyr::mutate(date = lubridate::dmy(.data$date)) %>% # Convert to Date format
    dplyr::rename(
      landing_date = "date",
      catch_taxon = "Taxon"
    ) |>
    dplyr::select(-c("month", "year")) |>
    dplyr::rowwise() %>%
    dplyr::mutate(
      survey_id = digest::digest(
        paste(
          .data$landing_date,
          .data$fisher_id,
          .data$landing_site,
          .data$n_fishers,
          .data$gear,
          .data$trip_duration,
          .data$n_fishers,
          sep = "_"
        ),
        algo = "crc32"
      )
    ) |>
    dplyr::ungroup() |>
    dplyr::select("survey_id", dplyr::everything()) |>
    dplyr::mutate(dplyr::across(dplyr::where(is.character), ~ tolower(.x))) |>
    # fix taxa names
    dplyr::mutate(
      catch_taxon = dplyr::case_when(
        .data$catch_taxon == "cheilunus trilobatus" ~ "cheilinus trilobatus",
        .data$catch_taxon == "pufflamen crysopterum" ~ "sufflamen chrysopterum",
        .data$catch_taxon == "sufflamen crysopterum" ~ "sufflamen chrysopterum",
        .data$catch_taxon == "perupeneus fraserorum" ~ "parupeneus fraserorum",
        .data$catch_taxon == "perupeneus macronemus" ~ "parupeneus macronemus",
        .data$catch_taxon == "octopus mucronatus" ~ "octopus",
        .data$catch_taxon == "parupeneus signutus" ~ "parupeneus",
        .data$catch_taxon == "perupeneus" ~ "parupeneus",
        .data$catch_taxon == "acanhocybium solandri" ~ "acanthocybium solandri",
        .data$catch_taxon == "parupeneus signutu" ~ "parupeneus",
        .data$catch_taxon == "gymnothorax crobroris" ~ "gymnothorax cribroris",
        .data$catch_taxon == "perupeneus signatus" ~ "parupeneus signatus",
        .data$catch_taxon == "gymonthorax favagineus" ~
          "gymnothorax favagineus",
        .data$catch_taxon == "colotomus carolinus" ~ "calotomus carolinus",
        .data$catch_taxon == "pegullus naatalensis" ~ "pagellus natalensis",
        .data$catch_taxon == "scomberomorus plurineatus" ~
          "scomberomorus plurilineatus",
        .data$catch_taxon == "lethrinus fulvus" ~ "lethrinus",
        .data$catch_taxon == "neotrygon caeruieopunctata" ~
          "neotrygon caeruleopunctata",
        .data$catch_taxon == "myuripristis berndti" ~ "myripristis berndti",
        TRUE ~ .data$catch_taxon
      )
    )

  upload_parquet_to_cloud(
    data = catch_surveys_preprocessed,
    prefix = pars$surveys$ba_surveys$preprocessed_surveys$file_prefix,
    provider = pars$storage$google$key,
    options = pars$storage$google$options
  )
}

#' Preprocess Pelagic Data Systems (PDS) Track Data
#'
#' @description
#' Downloads raw GPS tracks and creates a gridded summary of fishing activity.
#'
#' @param log_threshold The logging threshold to use. Default is logger::DEBUG.
#' @param grid_size Numeric. Size of grid cells in meters (100, 250, 500, or 1000).
#'
#' @return None (invisible). Creates and uploads preprocessed files.
#'
#' @keywords workflow preprocessing
#' @export
preprocess_pds_tracks <- function(
  log_threshold = logger::DEBUG,
  grid_size = 500
) {
  logger::log_threshold(log_threshold)
  pars <- read_config()

  # Get already preprocessed tracks
  logger::log_info("Checking existing preprocessed tracks...")
  preprocessed_filename <- cloud_object_name(
    prefix = paste0(pars$pds$pds_tracks$file_prefix, "-preprocessed"),
    provider = pars$storage$google$key,
    extension = "parquet",
    version = pars$pds$pds_tracks$version,
    options = pars$storage$google$options
  )

  # Get preprocessed trip IDs if file exists
  preprocessed_trips <- tryCatch(
    {
      download_cloud_file(
        name = preprocessed_filename,
        provider = pars$storage$google$key,
        options = pars$storage$google$options
      )
      preprocessed_data <- arrow::read_parquet(preprocessed_filename)
      unique(preprocessed_data$Trip)
    },
    error = function(e) {
      logger::log_info("No existing preprocessed tracks file found")
      character(0)
    }
  )

  # List raw tracks
  logger::log_info("Listing raw tracks...")
  raw_tracks <- googleCloudStorageR::gcs_list_objects(
    bucket = pars$pds_storage$google$options$bucket,
    prefix = pars$pds$pds_tracks$file_prefix
  )$name

  raw_trip_ids <- extract_trip_ids_from_filenames(raw_tracks)
  new_trip_ids <- setdiff(raw_trip_ids, preprocessed_trips)

  if (length(new_trip_ids) == 0) {
    logger::log_info("No new tracks to preprocess")
    return(invisible())
  }

  # Get raw tracks that need preprocessing
  new_tracks <- raw_tracks[raw_trip_ids %in% new_trip_ids]

  workers <- parallel::detectCores() - 1
  logger::log_info("Setting up parallel processing with {workers} workers...")
  future::plan(future::multisession, workers = workers)

  logger::log_info("Processing {length(new_tracks)} tracks in parallel...")
  new_processed_data <- furrr::future_map_dfr(
    new_tracks,
    function(track_file) {
      download_cloud_file(
        name = track_file,
        provider = pars$pds_storage$google$key,
        options = pars$pds_storage$google$options
      )

      track_data <- arrow::read_parquet(track_file) %>%
        preprocess_track_data(grid_size = grid_size)

      unlink(track_file)
      track_data
    },
    .options = furrr::furrr_options(seed = TRUE),
    .progress = TRUE
  )

  future::plan(future::sequential)

  # Combine with existing preprocessed data if it exists
  final_data <- if (length(preprocessed_trips) > 0) {
    dplyr::bind_rows(preprocessed_data, new_processed_data)
  } else {
    new_processed_data
  }

  output_filename <-
    paste0(pars$pds$pds_tracks$file_prefix, "-preprocessed") |>
    add_version(extension = "parquet")

  arrow::write_parquet(
    final_data,
    sink = output_filename,
    compression = "lz4",
    compression_level = 12
  )

  logger::log_info("Uploading preprocessed tracks...")
  upload_cloud_file(
    file = output_filename,
    provider = pars$storage$google$key,
    options = pars$storage$google$options
  )

  unlink(output_filename)
  if (exists("preprocessed_filename")) {
    unlink(preprocessed_filename)
  }

  logger::log_success("Track preprocessing complete")

  grid_summaries <- generate_track_summaries(final_data)

  output_filename <-
    paste0(pars$pds$pds_tracks$file_prefix, "-grid_summaries") |>
    add_version(extension = "parquet")

  arrow::write_parquet(
    grid_summaries,
    sink = output_filename,
    compression = "lz4",
    compression_level = 12
  )

  logger::log_info("Uploading preprocessed tracks...")
  upload_cloud_file(
    file = output_filename,
    provider = pars$storage$google$key,
    options = pars$storage$google$options
  )
}
