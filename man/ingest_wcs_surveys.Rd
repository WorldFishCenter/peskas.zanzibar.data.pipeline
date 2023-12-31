% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/ingest-wcs-surveys.R
\name{ingest_wcs_surveys}
\alias{ingest_wcs_surveys}
\title{Ingest WCS catch sruvey data}
\usage{
ingest_wcs_surveys(log_threshold = logger::DEBUG)
}
\arguments{
\item{log_threshold}{The (standard Apache logj4) log level used as a
threshold for the logging infrastructure. See \link[logger:log_levels]{logger::log_levels} for more
details}
}
\value{
No output. This function is used for it's side effects
}
\description{
Downloads landings information that has been collected using Kobo Toolbox and
uploads it to cloud storage services.
}
\details{
This function downloads the survey data and uploads this information to cloud
services. File names used contain a
versioning string that includes the date-time and, if available, the first 7
digits of the git commit sha. This is acomplished using \code{\link[=add_version]{add_version()}}

The parameters needed in \code{conf.yml} are:

\if{html}{\out{<div class="sourceCode">}}\preformatted{surveys:
  wcs_surveys:
    asset_id:
    username:
    password:
    file_prefix:
storage:
  storage_name:
    key:
    options:
      project:
      bucket:
      service_account_key:
}\if{html}{\out{</div>}}

Progress through the function is tracked using the package \emph{logger}.
}
\keyword{workflow}
