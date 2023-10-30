ingest_wcs_surveys <- function(log_threshold = logger::DEBUG) {
  logger::log_threshold(log_threshold)

  pars <- read_config()

  file_list <- retrieve_wcs_surveys(
    prefix = pars$surveys$wcs_catch_survey$file_prefix,
    file_format = "csv",
    append_version = TRUE,
    url = "kf.kobotoolbox.org",
    project_id = pars$surveys$wcs_catch_survey$asset_id,
    username = pars$surveys$wcs_catch_survey$username,
    psswd = pars$surveys$wcs_catch_survey$password,
    encoding = "UTF-8"
  )

  logger::log_info("Uploading files to cloud...")
  # Iterate over multiple storage providers if there are more than one
  purrr::map(pars$storage, ~ upload_cloud_file(file_list, .$key, .$options))
  logger::log_success("Files upload succeded")
}
