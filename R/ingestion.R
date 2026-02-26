#' Core ingestion logic for catch survey data
#'
#' Downloads survey data from Kobotoolbox, validates submission uniqueness,
#' flattens the nested JSON into tabular format, and uploads to cloud storage
#' as a versioned Parquet file.
#'
#' @param version Version identifier (e.g., "wcs", "wf_v1", "wf_v2")
#' @param kobo_config List with Kobo connection details: `url`, `asset_id`,
#'   `username`, `password`.
#' @param storage_config List with storage details: `file_prefix`, `provider`,
#'   `options`.
#' @return No return value. Processes and uploads data as side effects.
#' @keywords internal
ingest_catch_survey_version <- function(version, kobo_config, storage_config) {
  logger::log_info(glue::glue(
    "Downloading Fish Catch Survey Kobo data ({version})..."
  ))

  data_raw <- coasts::get_kobo_data(
    url = kobo_config$url,
    assetid = kobo_config$asset_id,
    uname = kobo_config$username,
    pwd = kobo_config$password,
    encoding = "UTF-8",
    format = "json"
  )
  logger::log_info(
    paste0(
      "Checking uniqueness of ",
      length(data_raw),
      " submissions"
    )
  )

  unique_ids <- dplyr::n_distinct(purrr::map_dbl(data_raw, ~ .$`_id`))
  if (unique_ids != length(data_raw)) {
    stop(glue::glue(
      "Number of submission ids ({unique_ids}) not the same as ",
      "number of records ({length(data_raw)}) in {version} data"
    ))
  }

  logger::log_info(glue::glue(
    "Converting Kobo data to tabular format..."
  ))

  raw_survey <- data_raw %>%
    purrr::map(flatten_row) %>%
    dplyr::bind_rows() %>%
    dplyr::rename(submission_id = "_id")

  logger::log_info(glue::glue(
    "Converted {nrow(raw_survey)} rows with {ncol(raw_survey)} columns"
  ))

  upload_parquet_to_cloud(
    data = raw_survey,
    prefix = storage_config$file_prefix,
    provider = storage_config$provider,
    options = storage_config$options
  )

  logger::log_info(glue::glue(
    "Successfully completed ingestion for {version}"
  ))
}


#' Ingest WCS Catch Survey Data
#'
#' Retrieves WCS catch survey data from Kobotoolbox, processes it, and uploads
#' the raw data as a Parquet file to cloud storage.
#'
#' @param log_threshold Logging threshold level (default: logger::DEBUG).
#'
#' @return No return value. Downloads data, processes it, and uploads to cloud storage.
#'
#' @details
#' The function:
#' 1. Reads configuration settings
#' 2. Downloads survey data from Kobotoolbox using `coasts::get_kobo_data()`
#' 3. Checks for uniqueness of submissions
#' 4. Converts data to tabular format
#' 5. Uploads raw data as Parquet files to cloud storage
#'
#' @keywords workflow ingestion
#' @export
#'
#' @examples
#' \dontrun{
#' ingest_wcs_surveys()
#' }
ingest_wcs_surveys <- function(log_threshold = logger::DEBUG) {
  logger::log_threshold(log_threshold)
  conf <- read_config()

  version_configs <- list(
    wcs = list(
      kobo = list(
        url = "kf.kobotoolbox.org",
        asset_id = conf$ingestion$wcs$asset_id,
        username = conf$ingestion$wcs$username,
        password = conf$ingestion$wcs$password
      ),
      storage = list(
        file_prefix = conf$surveys$wcs$raw$file_prefix,
        provider = conf$storage$google$key,
        options = conf$storage$google$options
      )
    )
  )

  purrr::iwalk(
    version_configs,
    ~ ingest_catch_survey_version(
      version = .y,
      kobo_config = .x$kobo,
      storage_config = .x$storage
    )
  )
}


#' Ingest WF Catch Survey Data
#'
#' Retrieves WF catch survey data (v1 and v2) from Kobotoolbox, processes it,
#' and uploads the raw data as Parquet files to cloud storage.
#'
#' @param log_threshold Logging threshold level (default: logger::DEBUG).
#'
#' @return No return value. Downloads data, processes it, and uploads to cloud storage.
#'
#' @details
#' The function processes both WF v1 and v2 catch surveys from eu.kobotoolbox.org.
#'
#' @keywords workflow ingestion
#' @export
#'
#' @examples
#' \dontrun{
#' ingest_wf_surveys()
#' }
ingest_wf_surveys <- function(log_threshold = logger::DEBUG) {
  logger::log_threshold(log_threshold)
  conf <- read_config()

  version_configs <- list(
    wf_v1 = list(
      kobo = list(
        url = "eu.kobotoolbox.org",
        asset_id = conf$ingestion$wf_v1$asset_id,
        username = conf$ingestion$wf_v1$username,
        password = conf$ingestion$wf_v1$password
      ),
      storage = list(
        file_prefix = conf$surveys$wf_v1$raw$file_prefix,
        provider = conf$storage$google$key,
        options = conf$storage$google$options
      )
    ),
    wf_v2 = list(
      kobo = list(
        url = "eu.kobotoolbox.org",
        asset_id = conf$ingestion$wf_v2$asset_id,
        username = conf$ingestion$wf_v2$username,
        password = conf$ingestion$wf_v2$password
      ),
      storage = list(
        file_prefix = conf$surveys$wf_v2$raw$file_prefix,
        provider = conf$storage$google$key,
        options = conf$storage$google$options
      )
    )
  )

  purrr::iwalk(
    version_configs,
    ~ ingest_catch_survey_version(
      version = .y,
      kobo_config = .x$kobo,
      storage_config = .x$storage
    )
  )
}


# ---- Flattening helpers (Kobo JSON → tabular) --------------------------------

#' Flatten a Single Row of Kobotoolbox Data
#'
#' Transforms each row of nested survey data into a flat tabular format.
#'
#' @param x A list representing a single row of Kobotoolbox data.
#' @return A flattened tibble representing the input row.
#' @keywords internal
#' @export
flatten_row <- function(x) {
  x %>%
    purrr::imap(flatten_field) %>%
    rlang::squash() %>%
    purrr::compact() %>%
    tibble::as_tibble(.name_repair = "unique")
}


#' Flatten a Single Field of Kobotoolbox Data
#'
#' Processes each field within a row of survey data, handling both simple
#' vectors and nested lists.
#'
#' @param x A vector or list representing a field in the data.
#' @param p The prefix or name associated with the field.
#' @return Modified field, either unchanged, unnested, or appropriately renamed.
#' @keywords internal
#' @export
flatten_field <- function(x, p) {
  if (inherits(x, "list")) {
    if (length(x) > 0) {
      if (purrr::vec_depth(x) == 2) {
        x <- list(x) %>%
          rlang::set_names(p) %>%
          unlist() %>%
          as.list()
      } else {
        x <- purrr::imap(x, rename_child, p = p)
      }
    } else {
      return(NULL)
    }
  } else {
    if (is.null(x)) x <- NA
  }
  x
}


#' Rename Nested Survey Data Elements
#'
#' Appends a parent name or index to child elements within a nested list.
#'
#' @param x A list element, possibly nested, to be renamed.
#' @param i The index or key of the element within the parent list.
#' @param p The parent name to prepend.
#' @return A renamed list element.
#' @keywords internal
#' @export
rename_child <- function(x, i, p) {
  if (length(x) == 0) {
    if (is.null(x)) {
      x <- NA
    }
    x <- list(x)
    x <- rlang::set_names(x, paste(p, i - 1, sep = "."))
  } else {
    if (inherits(i, "character")) {
      x <- rlang::set_names(x, paste(p, i, sep = "."))
    } else if (inherits(i, "integer")) {
      x <- rlang::set_names(x, paste(p, i - 1, names(x), sep = "."))
    }
  }
  x
}


# ---- PDS ingestion -----------------------------------------------------------

#' Ingest Pelagic Data Systems (PDS) Trip Data
#'
#' @description
#' Handles the automated ingestion of GPS boat trip data from Pelagic Data
#' Systems (PDS). Retrieves device metadata, downloads trip data, converts to
#' Parquet format, and uploads to cloud storage.
#'
#' @param log_threshold Logging threshold level (default: logger::DEBUG).
#'
#' @return None (invisible). Creates a Parquet file and uploads it to cloud storage.
#'
#' @seealso [get_trips()], [upload_cloud_file()]
#' @keywords workflow ingestion
#' @export
#'
#' @examples
#' \dontrun{
#' ingest_pds_trips()
#' }
ingest_pds_trips <- function(log_threshold = logger::DEBUG) {
  logger::log_threshold(log_threshold)
  conf <- read_config()

  logger::log_info("Loading device registry...")
  devices <- cloud_object_name(
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
    dplyr::filter(
      .data$customer_name %in%
        c("WorldFish - Tanzania AP", "WorldFish - Zanzibar")
    )

  boats_trips <- get_trips(
    token = conf$pds$token,
    secret = conf$pds$secret,
    dateFrom = "2023-01-01",
    dateTo = Sys.Date(),
    deviceInfo = TRUE,
    imeis = unique(devices$imei)
  )

  filename <- conf$pds$pds_trips$file_prefix %>%
    add_version(extension = "parquet")

  arrow::write_parquet(
    x = boats_trips,
    sink = filename,
    compression = "lz4",
    compression_level = 12
  )

  logger::log_info("Uploading {filename} to cloud storage")
  upload_cloud_file(
    file = filename,
    provider = conf$storage$google$key,
    options = conf$storage$google$options
  )
}


#' Ingest Pelagic Data Systems (PDS) Track Data
#'
#' @description
#' Handles the automated ingestion of GPS boat track data from PDS.
#' Downloads and stores only new tracks that haven't been previously uploaded.
#' Uses parallel processing for improved performance.
#'
#' @param log_threshold Logging threshold level (default: logger::DEBUG).
#' @param batch_size Optional number of tracks to process. If NULL, processes all.
#'
#' @return None (invisible). Processes and uploads track data.
#' @keywords workflow ingestion
#' @export
ingest_pds_tracks <- function(
  log_threshold = logger::DEBUG,
  batch_size = NULL
) {
  logger::log_threshold(log_threshold)
  conf <- read_config()

  # Get trips file from cloud storage
  logger::log_info("Getting trips file from cloud storage...")
  pds_trips_parquet <- cloud_object_name(
    prefix = conf$pds$pds_trips$file_prefix,
    provider = conf$storage$google$key,
    extension = "parquet",
    version = conf$pds$pds_trips$version,
    options = conf$storage$google$options
  )

  logger::log_info("Downloading {pds_trips_parquet}")
  download_cloud_file(
    name = pds_trips_parquet,
    provider = conf$storage$google$key,
    options = conf$storage$google$options
  )

  # Read trip IDs
  logger::log_info("Reading trip IDs...")
  trips_data <- arrow::read_parquet(file = pds_trips_parquet) %>%
    dplyr::pull("Trip") %>%
    unique()

  unlink(pds_trips_parquet)

  # List existing files in GCS bucket
  logger::log_info("Checking existing tracks in cloud storage...")
  existing_tracks <-
    googleCloudStorageR::gcs_list_objects(
      bucket = conf$pds_storage$google$options$bucket,
      prefix = conf$pds$pds_tracks$file_prefix
    )$name

  # Get new trip IDs
  existing_trip_ids <- extract_trip_ids_from_filenames(existing_tracks)
  new_trip_ids <- setdiff(trips_data, existing_trip_ids)

  if (length(new_trip_ids) == 0) {
    logger::log_info("No new tracks to download")
    return(invisible())
  }

  # Setup parallel processing
  workers <- parallel::detectCores() - 1
  logger::log_info("Setting up parallel processing with {workers} workers...")
  future::plan(future::multisession, workers = workers)

  process_ids <- if (!is.null(batch_size)) {
    new_trip_ids[seq_len(min(batch_size, length(new_trip_ids)))]
  } else {
    new_trip_ids
  }
  logger::log_info("Processing {length(process_ids)} new tracks in parallel...")

  results <- furrr::future_map(
    process_ids,
    ~ process_single_track(.x, conf),
    .options = furrr::furrr_options(seed = TRUE),
    .progress = TRUE
  )

  future::plan(future::sequential)

  # Summarize results
  statuses <- purrr::map_chr(results, "status")
  successes <- sum(statuses == "success")
  failures <- sum(statuses == "error")

  logger::log_info(
    "Processing complete. Successfully processed {successes} tracks."
  )
  if (failures > 0) {
    logger::log_warn("Failed to process {failures} tracks.")
    failed_results <- results[statuses == "error"]
    purrr::walk(
      failed_results,
      ~ logger::log_warn("Trip {.x$trip_id}: {.x$message}")
    )
  }
}


#' Extract Trip IDs from Track Filenames
#'
#' @param filenames Character vector of track filenames.
#' @return Character vector of trip IDs.
#' @keywords internal
extract_trip_ids_from_filenames <- function(filenames) {
  if (length(filenames) == 0) {
    return(character(0))
  }
  gsub(".*_([0-9]+)\\.parquet$", "\\1", filenames)
}


#' Process Single PDS Track
#'
#' @param trip_id Character. The ID of the trip to process.
#' @param conf List. Configuration parameters.
#' @return List with processing status and details.
#' @keywords internal
process_single_track <- function(trip_id, conf) {
  tryCatch(
    {
      track_filename <- sprintf(
        "%s_%s.parquet",
        conf$pds$pds_tracks$file_prefix,
        trip_id
      )

      track_data <- get_trip_points(
        token = conf$pds$token,
        secret = conf$pds$secret,
        id = as.character(trip_id),
        deviceInfo = TRUE
      )

      arrow::write_parquet(
        x = track_data,
        sink = track_filename,
        compression = "lz4",
        compression_level = 12
      )

      logger::log_info("Uploading track for trip {trip_id}")
      upload_cloud_file(
        file = track_filename,
        provider = conf$pds_storage$google$key,
        options = conf$pds_storage$google$options
      )

      unlink(track_filename)

      list(
        status = "success",
        trip_id = trip_id,
        message = "Successfully processed"
      )
    },
    error = function(e) {
      list(status = "error", trip_id = trip_id, message = e$message)
    }
  )
}
