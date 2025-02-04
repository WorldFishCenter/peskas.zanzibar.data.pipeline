#' Authenticate to a Cloud Storage Provider
#'
#' This function is primarily used internally by other functions to establish authentication
#' with specified cloud providers such as Google Cloud Services (GCS) or Amazon Web Services (AWS).
#'
#' @param provider A character string specifying the cloud provider ("gcs" or "aws").
#' @param options A named list of options specific to the cloud provider (see details).
#'
#' @details For GCS, the options list must include:
#' - `service_account_key`: The contents of the authentication JSON file from your Google Project.
#'
#' This function wraps [googleCloudStorageR::gcs_auth()] to handle GCS authentication.
#'
#' @export
#' @keywords storage
#' @examples
#' \dontrun{
#' authentication_details <- readLines("path/to/json_file.json")
#' cloud_storage_authenticate("gcs", list(service_account_key = authentication_details))
#' #'
#' }
cloud_storage_authenticate <- function(provider, options) {
  if ("gcs" %in% provider) {
    # Only need to authenticate if there is no token for downstream requests
    if (isFALSE(googleAuthR::gar_has_token())) {
      service_account_key <- options$service_account_key
      temp_auth_file <- tempfile(fileext = "json")
      writeLines(service_account_key, temp_auth_file)
      googleCloudStorageR::gcs_auth(json_file = temp_auth_file)
    }
  }
}

#' Upload File to Cloud Storage
#'
#' Uploads a local file to a specified cloud storage bucket, supporting both single and multiple files.
#'
#' @param file A character vector specifying the path(s) of the file(s) to upload.
#' @param provider A character string specifying the cloud provider ("gcs" or "aws").
#' @param options A named list of provider-specific options including the bucket and authentication details.
#' @param name (Optional) The name to assign to the file in the cloud. If not specified, the local file name is used.
#'
#' @details For GCS, the options list must include:
#' - `bucket`: The name of the bucket to which files are uploaded.
#' - `service_account_key`: The authentication JSON contents, if not previously authenticated.
#'
#' This function utilizes [googleCloudStorageR::gcs_upload()] for file uploads to GCS.
#'
#' @return A list of metadata objects for the uploaded files if successful.
#' @export
#' @keywords storage
#' @examples
#' \dontrun{
#' authentication_details <- readLines("path/to/json_file.json")
#' upload_cloud_file(
#'   "path/to/local_file.csv",
#'   "gcs",
#'   list(service_account_key = authentication_details, bucket = "my-bucket")
#' )
#' }
#'
upload_cloud_file <- function(file, provider, options, name = file) {
  cloud_storage_authenticate(provider, options)

  out <- list()
  if ("gcs" %in% provider) {
    # Iterate over multiple files (and names)
    google_output <- purrr::map2(
      file, name,
      ~ googleCloudStorageR::gcs_upload(
        file = .x,
        bucket = options$bucket,
        name = .y,
        predefinedAcl = "bucketLevel"
      )
    )

    out <- c(out, google_output)
  }

  out
}

#' Retrieve Full Name of Versioned Cloud Object
#'
#' Gets the full name(s) of object(s) in cloud storage matching the specified prefix, version, and file extension.
#'
#' @param prefix A string indicating the object's prefix.
#' @param version A string specifying the version ("latest" or a specific version string).
#' @param extension The file extension to filter by. An empty string ("") includes all extensions.
#' @param provider A character string specifying the cloud provider ("gcs" or "aws").
#' @param exact_match A logical indicating whether to match the prefix exactly.
#' @param options A named list of provider-specific options including the bucket and authentication details.
#'
#' @details For GCS, the options list should include:
#' - `bucket`: The bucket name.
#' - `service_account_key`: The authentication JSON contents, if not previously authenticated.
#'
#' @return A vector of names of objects matching the criteria.
#' @export
#' @keywords storage
#' @examples
#' \dontrun{
#' authentication_details <- readLines("path/to/json_file.json")
#' cloud_object_name(
#'   "prefix",
#'   "latest",
#'   "json",
#'   "gcs",
#'   list(service_account_key = authentication_details, bucket = "my-bucket")
#' )
#' #'
#' }
cloud_object_name <- function(prefix, version = "latest", extension = "",
                              provider, exact_match = FALSE, options) {
  cloud_storage_authenticate(provider, options)

  if ("gcs" %in% provider) {
    gcs_files <- googleCloudStorageR::gcs_list_objects(
      bucket = options$bucket,
      prefix = prefix
    )

    if (nrow(gcs_files) == 0) {
      return(character(0))
    }

    gcs_files_formatted <- gcs_files %>%
      tidyr::separate(
        col = .data$name,
        into = c("base_name", "version", "ext"),
        # Version is separated with the "__" string
        sep = "__",
        remove = FALSE
      ) %>%
      dplyr::filter(stringr::str_detect(.data$ext, paste0(extension, "$"))) %>%
      dplyr::group_by(.data$base_name, .data$ext)

    if (isTRUE(exact_match)) {
      selected_rows <- gcs_files_formatted %>%
        dplyr::filter(.data$base_name == prefix)
    } else {
      selected_rows <- gcs_files_formatted
    }

    if (version == "latest") {
      selected_rows <- selected_rows %>%
        dplyr::filter(max(.data$updated) == .data$updated)
    } else {
      this_version <- version
      selected_rows <- selected_rows %>%
        dplyr::filter(.data$version == this_version)
    }

    selected_rows$name
  }
}


#' Download Object from Cloud Storage
#'
#' Downloads an object from cloud storage to a local file.
#'
#' @param name The name of the object in the storage bucket.
#' @param provider A character string specifying the cloud provider ("gcs" or "aws").
#' @param options A named list of provider-specific options including the bucket and authentication details.
#' @param file (Optional) The local path to save the downloaded object. If not specified, the object name is used.
#'
#' @details For GCS, the options list should include:
#' - `bucket`: The name of the bucket from which the object is downloaded.
#' - `service_account_key`: The authentication JSON contents, if not previously authenticated.
#'
#' @return The path to the downloaded file.
#' @export
#' @keywords storage
#' @examples
#' \dontrun{
#' authentication_details <- readLines("path/to/json_file.json")
#' download_cloud_file(
#'   "object_name.json",
#'   "gcs",
#'   list(service_account_key = authentication_details, bucket = "my-bucket"),
#'   "local_path/to/save/object.json"
#' )
#' }
#'
download_cloud_file <- function(name, provider, options, file = name) {
  cloud_storage_authenticate(provider, options)

  if ("gcs" %in% provider) {
    purrr::map2(
      name, file,
      ~ googleCloudStorageR::gcs_get_object(
        object_name = .x,
        bucket = options$bucket,
        saveToDisk = .y,
        overwrite = ifelse(is.null(options$overwrite), TRUE, options$overwrite)
      )
    )
  }

  file
}

#' Download Preprocessed Surveys
#'
#' Retrieves preprocessed survey data from Google Cloud Storage, specifically configured for WCS (Wildlife Conservation Society) datasets. This function fetches data stored in Parquet format.
#'
#' @param pars A list representing the configuration settings, typically obtained from a YAML configuration file.
#' @param prefix A character string specifying the organization prefix to retrieve preprocessed surveys, either "wcs" or "ba".
#' @return A dataframe of preprocessed survey landings, loaded from Parquet files.
#' @keywords storage
#' @export
#' @examples
#' \dontrun{
#' config <- peskas.zanzibar.pipeline::read_config()
#' df_preprocessed <- get_preprocessed_surveys(config, prefix = "ba")
#' }
#'
get_preprocessed_surveys <- function(pars, prefix = NULL) {
  wcs_preprocessed_surveys <-
    cloud_object_name(
      prefix = prefix,
      provider = pars$storage$google$key,
      extension = "parquet",
      version = pars$surveys$wcs_surveys$version$preprocess,
      options = pars$storage$google$options
    )

  logger::log_info("Retrieving {wcs_preprocessed_surveys}")
  download_cloud_file(
    name = wcs_preprocessed_surveys,
    provider = pars$storage$google$key,
    options = pars$storage$google$options
  )

  arrow::read_parquet(wcs_preprocessed_surveys)
}


#' Download WCS Validated Surveys
#'
#' Retrieves validated survey data from Google Cloud Storage, tailored for WCS (Wildlife Conservation Society) datasets. This function fetches data stored in Parquet format.
#'
#' @param pars A list representing the configuration settings, typically obtained from a YAML configuration file.
#'
#' @return A dataframe of validated survey landings, loaded from Parquet files.
#' @keywords storage
#' @export
#' @examples
#' \dontrun{
#' config <- peskas.zanzibar.pipeline::read_config()
#' df_validated <- get_validated_surveys(config)
#' #'
#' }
#'
get_validated_surveys <- function(pars) {
  wcs_validated_surveys <-
    cloud_object_name(
      prefix = pars$surveys$wcs_surveys$validated_surveys$file_prefix,
      provider = pars$storage$google$key,
      extension = "parquet",
      version = pars$surveys$wcs_surveys$version$preprocess,
      options = pars$storage$google$options
    )

  logger::log_info("Retrieving {wcs_validated_surveys}")
  download_cloud_file(
    name = wcs_validated_surveys,
    provider = pars$storage$google$key,
    options = pars$storage$google$options
  )

  arrow::read_parquet(wcs_validated_surveys)
}


#' Get metadata tables
#'
#' Get Metadata tables from Google sheets. This function downloads
#' the tables include information about the fishery.
#'
#' The parameters needed in `conf.yml` are:
#'
#' ```
#' storage:
#'   storage_name:
#'     key:
#'     options:
#'       project:
#'       bucket:
#'       service_account_key:
#' ```
#'
#' @param log_threshold The logging threshold level. Default is logger::DEBUG.
#'
#' @export
#' @keywords storage
#'
#' @examples
#' \dontrun{
#' # Ensure you have the necessary configuration in conf.yml
#' metadata_tables <- get_metadata()
#' }
get_metadata <- function(log_threshold = logger::DEBUG) {
  logger::log_threshold(log_threshold)
  conf <- read_config()

  logger::log_info("Authenticating for google drive")
  googlesheets4::gs4_auth(
    path = conf$storage$google$options$service_account_key,
    use_oob = TRUE
  )
  logger::log_info("Downloading metadata tables")

  tables <-
    conf$metadata$google_sheets$tables %>%
    rlang::set_names() %>%
    purrr::map(~ googlesheets4::range_read(
      ss = conf$metadata$google_sheets$sheet_id,
      sheet = .x,
      col_types = "c"
    ))

  tables
}
