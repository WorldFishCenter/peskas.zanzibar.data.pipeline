% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/retrieve-wcs-data.R
\name{retrieve_wcs_surveys}
\alias{retrieve_wcs_surveys}
\title{Download WCS Surveys from Kobotoolbox}
\usage{
retrieve_wcs_surveys(
  prefix = NULL,
  file_format = NULL,
  append_version = NULL,
  url = NULL,
  project_id = NULL,
  username = NULL,
  psswd = NULL,
  encoding = NULL
)
}
\arguments{
\item{prefix}{Name to be used as the prefix of the file names to be
downloaded. Can be a path.}

\item{file_format}{Either "csv" or "rds",}

\item{append_version}{Whether to append versioning information to the
filename using \link{add_version}..}

\item{url}{The URL of kobotoolbox (often referred to as 'kpi-url').}

\item{project_id}{Is the asset id of the asset for which the data is
to be downloaded.}

\item{username}{Username of your kobotoolbox account.}

\item{psswd}{Password of the account.}

\item{encoding}{Encoding to be used. Default is "UTF-8".}
}
\value{
A character vector with paths of the downloaded files.
}
\description{
This function retrieves survey data from Kobotoolbox for a specific project.
It allows users to customize the filename using a prefix, choose between CSV or RDS formats,
and decide whether to append versioning information to the filename.
The resulting files are downloaded to the working directory or specified path,
with paths returned as a character vector.
}
\examples{
\dontrun{
file_list <- retrieve_wcs_surveys(
  prefix = "my_data",
  file_format = "csv",
  append_version = TRUE,
  url = "kf.kobotoolbox.org",
  project_id = "my_project_id",
  username = "admin",
  psswd = "admin",
  encoding = "UTF-8"
)
}
}
