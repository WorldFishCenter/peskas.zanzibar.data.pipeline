#' Download WCS preprocessed surveys
#'
#' Download preprocessed WCS data from Google Cloud.
#'
#' @param pars The configuration file.
#'
#' @return A rds dataframe of preprocessed survey landings.
#' @export
#'
get_preprocessed_surveys <- function(pars) {
  wcs_preprocessed_surveys <-
    cloud_object_name(
      prefix = pars$surveys$wcs_surveys$preprocessed_surveys$file_prefix,
      provider = pars$storage$google$key,
      extension = "rds",
      version = pars$surveys$wcs_surveys$version$preprocess,
      options = pars$storage$google$options
    )

  logger::log_info("Retrieving {wcs_preprocessed_surveys}")
  download_cloud_file(
    name = wcs_preprocessed_surveys,
    provider = pars$storage$google$key,
    options = pars$storage$google$options
  )

  readr::read_rds(wcs_preprocessed_surveys)
}
