#' Ingest WCS and WF Catch Survey Data
#'
#' @description
#' This function handles the automated ingestion of fish catch survey data from both WCS
#' and WF sources through Kobo Toolbox. It performs the following operations:
#' 1. Downloads the survey data from Kobo Toolbox
#' 2. Processes and formats the data
#' 3. Uploads the processed files to configured cloud storage locations
#'
#' @details
#' The function requires specific configuration in the `conf.yml` file with the following structure:
#'
#' ```yaml
#' surveys:
#'   wcs_surveys:
#'     raw_surveys:
#'       file_prefix: "wcs_raw_data"     # Prefix for output files
#'       asset_id: "xxxxx"               # Kobo Toolbox asset ID
#'       username: "user@example.com"    # Kobo Toolbox username
#'       password: "password123"         # Kobo Toolbox password
#'   wf_surveys:
#'     raw_surveys:
#'       file_prefix: "wf_raw_data"
#'       asset_id: "yyyyy"
#'       username: "user2@example.com"
#'       password: "password456"
#' storage:
#'   gcp:                               # Storage provider name
#'     key: "google"                    # Storage provider identifier
#'     options:
#'       project: "project-id"          # Cloud project ID
#'       bucket: "bucket-name"          # Storage bucket name
#'       service_account_key: "path/to/key.json"
#' ```
#'
#' The function processes both WCS and WF surveys sequentially, with separate logging
#' for each step. For each survey:
#' - Downloads data using the `retrieve_surveys()` function
#' - Converts the data to parquet format
#' - Uploads the resulting files to all configured storage providers
#'
#' Error handling is managed through the logger package, with informative messages
#' at each step of the process.
#'
#' @param log_threshold The logging threshold to use. Default is logger::DEBUG.
#'   See `logger::log_levels` for available options.
#'
#' @return None (invisible). The function performs its operations for side effects:
#'   - Creates parquet files locally
#'   - Uploads files to configured cloud storage
#'   - Generates logs of the process
#'
#' @examples
#' \dontrun{
#' # Run with default debug logging
#' ingest_surveys()
#'
#' # Run with info-level logging only
#' ingest_surveys(logger::INFO)
#' }
#'
#' @seealso
#' * [retrieve_surveys()] for details on the survey retrieval process
#' * [upload_cloud_file()] for details on the cloud upload process
#'
#' @keywords workflow ingestion
#' @export
ingest_surveys <- function(log_threshold = logger::DEBUG) {
  logger::log_threshold(log_threshold)
  pars <- read_config()

  # WCS Survey
  #logger::log_info("Downloading WCS Fish Catch Survey Kobo data...")
  #wcs_files <- retrieve_surveys(
  #  prefix = pars$surveys$wcs_surveys$raw_surveys$file_prefix,
  #  append_version = TRUE,
  #  url = "kf.kobotoolbox.org",
  #  project_id = pars$surveys$wcs_surveys$asset_id,
  #  username = pars$surveys$wcs_surveys$username,
  #  psswd = pars$surveys$wcs_surveys$password,
  #  encoding = "UTF-8"
  #)

  #logger::log_info("Uploading WCS files to cloud...")
  #purrr::map(pars$storage, ~ upload_cloud_file(wcs_files, .$key, .$options))
  #logger::log_success("WCS files upload succeeded")

  # WF Survey
  logger::log_info("Downloading WF Fish Catch Survey Kobo data...")
  wf_files <- retrieve_surveys(
    prefix = pars$surveys$wf_surveys$raw_surveys$file_prefix,
    append_version = TRUE,
    url = "eu.kobotoolbox.org",
    project_id = pars$surveys$wf_surveys$asset_id,
    username = pars$surveys$wf_surveys$username,
    psswd = pars$surveys$wf_surveys$password,
    encoding = "UTF-8"
  )

  logger::log_info("Uploading WF files to cloud...")
  purrr::map(pars$storage, ~ upload_cloud_file(wf_files, .$key, .$options))
  logger::log_success("WF files upload succeeded")
}

#' Retrieve Surveys from Kobotoolbox
#'
#' Downloads survey data from Kobotoolbox for a specified project and uploads the data in Parquet format. File naming can include versioning details.
#'
#' @param prefix Filename prefix or path for downloaded files.
#' @param append_version Boolean indicating whether to append versioning info to filenames.
#' @param url URL of the Kobotoolbox instance.
#' @param project_id Project asset ID for data download.
#' @param username Kobotoolbox account username.
#' @param psswd Kobotoolbox account password.
#' @param encoding Character encoding for the downloaded data; defaults to "UTF-8".
#'
#' @return Vector of paths for the downloaded Parquet files.
#' @export
#' @keywords ingestion
#' @examples
#' \dontrun{
#' file_list <- retrieve_surveys(
#'   prefix = "my_data",
#'   append_version = TRUE,
#'   url = "kf.kobotoolbox.org",
#'   project_id = "my_project_id",
#'   username = "admin",
#'   psswd = "admin",
#'   encoding = "UTF-8"
#' )
#' }
#'
retrieve_surveys <- function(
  prefix = NULL,
  append_version = NULL,
  url = NULL,
  project_id = NULL,
  username = NULL,
  psswd = NULL,
  encoding = NULL
) {
  data_raw <-
    KoboconnectR::kobotools_kpi_data(
      url = url,
      assetid = project_id,
      uname = username,
      pwd = psswd,
      encoding = encoding
    )$results

  # Check that submissions are unique in case there is overlap in the pagination
  if (
    dplyr::n_distinct(purrr::map_dbl(data_raw, ~ .$`_id`)) != length(data_raw)
  ) {
    stop("Number of submission ids not the same as number of records")
  }

  logger::log_info(
    "Converting WCS Fish Catch Survey Kobo data to tabular format..."
  )
  tabular_data <- purrr::map_dfr(data_raw, flatten_row)
  data_filename <- prefix

  if (isTRUE(append_version)) {
    parquet_filename <- add_version(data_filename, "parquet")
  } else {
    parquet_filename <- paste0(data_filename, ".parquet")
  }

  logger::log_info("Converting json data to Parquet as {parquet_filename}...")

  # Convert tabular_data to Arrow Table
  arrow_table <- arrow::as_arrow_table(tabular_data)

  # Write to Parquet format
  arrow::write_parquet(
    arrow_table,
    sink = parquet_filename,
    compression = "lz4",
    compression_level = 12
  )

  return(parquet_filename)
}

#' Flatten Survey Data Rows
#'
#' Transforms each row of nested survey data into a flat tabular format using a mapping and flattening process.
#'
#' @param x A list representing a row of data, potentially containing nested lists or vectors.
#' @return A tibble with each row representing flattened survey data.
#' @keywords internal
#' @export
flatten_row <- function(x) {
  x %>%
    # Each row is composed of several fields
    purrr::imap(flatten_field) %>%
    rlang::squash() %>%
    tibble::as_tibble()
}

#' Flatten Survey Data Fields
#'
#' Processes each field within a row of survey data, handling both simple vectors and nested lists. For lists with named elements, renames and unlists them for flat structure preparation.
#'
#' @param x A vector or list representing a field in the data.
#' @param p The prefix or name associated with the field, used for naming during the flattening process.
#' @return Modified field, either unchanged, unnested, or appropriately renamed.
#' @keywords internal
#' @export
flatten_field <- function(x, p) {
  # If the field is a simple vector do nothing but if the field is a list we
  # need more logic
  if (inherits(x, "list")) {
    if (length(x) > 0) {
      if (purrr::vec_depth(x) == 2) {
        # If the field-list has named elements is we just need to rename the list
        x <- list(x) %>%
          rlang::set_names(p) %>%
          unlist() %>%
          as.list()
      } else {
        # If the field-list is an "array" we need to iterate over its children
        x <- purrr::imap(x, rename_child, p = p)
      }
    }
  } else {
    if (is.null(x)) x <- NA
  }
  x
}

#' Rename Nested Survey Data Elements
#'
#' Appends a parent name or index to child elements within a nested list, assisting in creating a coherent and traceable data structure during the flattening process.
#'
#' @param x A list element, possibly nested, to be renamed.
#' @param i The index or key of the element within the parent list.
#' @param p The parent name to prepend to the element's existing name for context.
#' @return A renamed list element, structured to maintain contextual relevance in a flattened dataset.
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

#' Ingest Pelagic Data Systems (PDS) Trip Data
#'
#' @description
#' This function handles the automated ingestion of GPS boat trip data from Pelagic Data Systems (PDS).
#' It performs the following operations:
#' 1. Retrieves device metadata from the configured source
#' 2. Downloads trip data from PDS API using device IMEIs
#' 3. Converts the data to parquet format
#' 4. Uploads the processed file to configured cloud storage
#'
#' @details
#' The function requires specific configuration in the `conf.yml` file with the following structure:
#'
#' ```yaml
#' pds:
#'   token: "your_pds_token"               # PDS API token
#'   secret: "your_pds_secret"             # PDS API secret
#'   pds_trips:
#'     file_prefix: "pds_trips"            # Prefix for output files
#' storage:
#'   google:                               # Storage provider name
#'     key: "google"                       # Storage provider identifier
#'     options:
#'       project: "project-id"             # Cloud project ID
#'       bucket: "bucket-name"             # Storage bucket name
#'       service_account_key: "path/to/key.json"
#' ```
#'
#' The function processes trips sequentially:
#' - Retrieves device metadata using `get_metadata()`
#' - Downloads trip data using the `get_trips()` function
#' - Converts the data to parquet format
#' - Uploads the resulting file to configured storage provider
#'
#' @param log_threshold The logging threshold to use. Default is logger::DEBUG.
#'   See `logger::log_levels` for available options.
#'
#' @return None (invisible). The function performs its operations for side effects:
#'   - Creates a parquet file locally with trip data
#'   - Uploads file to configured cloud storage
#'   - Generates logs of the process
#'
#' @examples
#' \dontrun{
#' # Run with default debug logging
#' ingest_pds_trips()
#'
#' # Run with info-level logging only
#' ingest_pds_trips(logger::INFO)
#' }
#'
#' @seealso
#' * [get_trips()] for details on the PDS trip data retrieval process
#' * [get_metadata()] for details on the device metadata retrieval
#' * [upload_cloud_file()] for details on the cloud upload process
#'
#' @keywords workflow ingestion
#' @export
ingest_pds_trips <- function(log_threshold = logger::DEBUG) {
  logger::log_threshold(log_threshold)
  pars <- read_config()

  devices_table <- get_metadata(table = "devices")

  boats_trips <- get_trips(
    token = pars$pds$token,
    secret = pars$pds$secret,
    dateFrom = "2023-01-01",
    dateTo = Sys.Date(),
    imeis = devices_table$devices$IMEI
  )

  filename <- pars$pds$pds_trips$file_prefix %>%
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
    provider = pars$storage$google$key,
    options = pars$storage$google$options
  )
}
#' Ingest Pelagic Data Systems (PDS) Track Data
#'
#' @description
#' This function handles the automated ingestion of GPS boat track data from Pelagic Data Systems (PDS).
#' It downloads and stores only new tracks that haven't been previously uploaded to Google Cloud Storage.
#' Uses parallel processing for improved performance.
#'
#' @param log_threshold The logging threshold to use. Default is logger::DEBUG.
#' @param batch_size Optional number of tracks to process. If NULL, processes all new tracks.
#'
#' @return None (invisible). The function performs its operations for side effects.
#'
#' @keywords workflow ingestion
#' @export
ingest_pds_tracks <- function(
  log_threshold = logger::DEBUG,
  batch_size = NULL
) {
  logger::log_threshold(log_threshold)
  pars <- read_config()

  # Get trips file from cloud storage
  logger::log_info("Getting trips file from cloud storage...")
  pds_trips_parquet <- cloud_object_name(
    prefix = pars$pds$pds_trips$file_prefix,
    provider = pars$storage$google$key,
    extension = "parquet",
    version = pars$pds$pds_trips$version,
    options = pars$storage$google$options
  )

  logger::log_info("Downloading {pds_trips_parquet}")
  download_cloud_file(
    name = pds_trips_parquet,
    provider = pars$storage$google$key,
    options = pars$storage$google$options
  )

  # Read trip IDs
  logger::log_info("Reading trip IDs...")
  trips_data <- arrow::read_parquet(file = pds_trips_parquet) %>%
    dplyr::pull("Trip") %>%
    unique()

  # Clean up downloaded file
  unlink(pds_trips_parquet)

  # List existing files in GCS bucket
  logger::log_info("Checking existing tracks in cloud storage...")
  existing_tracks <-
    googleCloudStorageR::gcs_list_objects(
      bucket = pars$pds_storage$google$options$bucket,
      prefix = pars$pds$pds_tracks$file_prefix
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

  # Select tracks to process
  process_ids <- if (!is.null(batch_size)) {
    new_trip_ids[1:batch_size]
  } else {
    new_trip_ids
  }
  logger::log_info("Processing {length(process_ids)} new tracks in parallel...")

  # Process tracks in parallel with progress bar
  results <- furrr::future_map(
    process_ids,
    function(trip_id) {
      tryCatch(
        {
          # Create filename for this track
          track_filename <- sprintf(
            "%s_%s.parquet",
            pars$pds$pds_tracks$file_prefix,
            trip_id
          )

          # Get track data
          track_data <- get_trip_points(
            token = pars$pds$token,
            secret = pars$pds$secret,
            id = as.character(trip_id),
            deviceInfo = TRUE
          )

          # Save to parquet
          arrow::write_parquet(
            x = track_data,
            sink = track_filename,
            compression = "lz4",
            compression_level = 12
          )

          # Upload to cloud
          logger::log_info("Uploading track for trip {trip_id}")
          upload_cloud_file(
            file = track_filename,
            provider = pars$pds_storage$google$key,
            options = pars$pds_storage$google$options
          )

          # Clean up local file
          unlink(track_filename)

          list(
            status = "success",
            trip_id = trip_id,
            message = "Successfully processed"
          )
        },
        error = function(e) {
          list(
            status = "error",
            trip_id = trip_id,
            message = e$message
          )
        }
      )
    },
    .options = furrr::furrr_options(seed = TRUE),
    .progress = TRUE
  )

  # Clean up parallel processing
  future::plan(future::sequential)

  # Summarize results
  successes <- sum(purrr::map_chr(results, "status") == "success")
  failures <- sum(purrr::map_chr(results, "status") == "error")

  logger::log_info(
    "Processing complete. Successfully processed {successes} tracks."
  )
  if (failures > 0) {
    logger::log_warn("Failed to process {failures} tracks.")
    failed_results <- results[purrr::map_chr(results, "status") == "error"]
    failed_trips <- purrr::map_chr(failed_results, "trip_id")
    failed_messages <- purrr::map_chr(failed_results, "message")

    logger::log_warn("Failed trip IDs and reasons:")
    purrr::walk2(
      failed_trips,
      failed_messages,
      ~ logger::log_warn("Trip {.x}: {.y}")
    )
  }
}

#' Extract Trip IDs from Track Filenames
#'
#' @param filenames Character vector of track filenames
#' @return Character vector of trip IDs
#' @keywords internal
extract_trip_ids_from_filenames <- function(filenames) {
  if (length(filenames) == 0) {
    return(character(0))
  }
  # Assuming filenames are in format: pds-tracks_TRIPID.parquet
  gsub(".*_([0-9]+)\\.parquet$", "\\1", filenames)
}

#' Process Single PDS Track
#'
#' @param trip_id Character. The ID of the trip to process.
#' @param pars List. Configuration parameters.
#' @return List with processing status and details.
#' @keywords internal
process_single_track <- function(trip_id, pars) {
  tryCatch(
    {
      # Create filename for this track
      track_filename <- sprintf(
        "%s_%s.parquet",
        pars$pds$pds_tracks$file_prefix,
        trip_id
      )

      # Get track data
      track_data <- get_trip_points(
        token = pars$pds$token,
        secret = pars$pds$secret,
        id = trip_id,
        deviceInfo = TRUE
      )

      # Save to parquet
      arrow::write_parquet(
        x = track_data,
        sink = track_filename,
        compression = "lz4",
        compression_level = 12
      )

      # Upload to cloud
      logger::log_info("Uploading track for trip {trip_id}")
      upload_cloud_file(
        file = track_filename,
        provider = pars$pds_storage$google$key,
        options = pars$pds_storage$google$options
      )

      # Clean up local file
      unlink(track_filename)

      list(
        status = "success",
        trip_id = trip_id,
        message = "Successfully processed"
      )
    },
    error = function(e) {
      list(
        status = "error",
        trip_id = trip_id,
        message = e$message
      )
    }
  )
}
