#' Ingest WCS catch sruvey data
#'
#' Downloads landings information that has been collected using Kobo Toolbox and
#' uploads it to cloud storage services.
#'
#' This function downloads the survey data and uploads this information to cloud
#' services. File names used contain a
#' versioning string that includes the date-time and, if available, the first 7
#' digits of the git commit sha. This is acomplished using [add_version()]
#'
#' The parameters needed in `conf.yml` are:
#'
#' ```
#' surveys:
#'   wcs_surveys:
#'     asset_id:
#'     username:
#'     password:
#'     file_prefix:
#' storage:
#'   storage_name:
#'     key:
#'     options:
#'       project:
#'       bucket:
#'       service_account_key:
#' ```
#'
#' Progress through the function is tracked using the package *logger*.
#'
#'
#' @param log_threshold The (standard Apache logj4) log level used as a
#'   threshold for the logging infrastructure. See [logger::log_levels] for more
#'   details
#'
#' @keywords workflow
#'
#' @return No output. This function is used for it's side effects
#' @export
#'
ingest_wcs_surveys <- function(log_threshold = logger::DEBUG) {
  logger::log_threshold(log_threshold)

  pars <- read_config()

  file_list <- retrieve_wcs_surveys(
    prefix = pars$surveys$wcs_surveys$file_prefix,
    file_format = "csv",
    append_version = TRUE,
    url = "kf.kobotoolbox.org",
    project_id = pars$surveys$wcs_surveys$asset_id,
    username = pars$surveys$wcs_surveys$username,
    psswd = pars$surveys$wcs_surveys$password,
    encoding = "UTF-8"
  )

  pars$
  logger::log_info("Uploading files to cloud...")
  # Iterate over multiple storage providers if there are more than one
  purrr::map(pars$storage, ~ upload_cloud_file(file_list, .$key, .$options))
  logger::log_success("Files upload succeded")
}
