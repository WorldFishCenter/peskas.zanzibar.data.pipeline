% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/model-taxa.R
\name{getLWCoeffs}
\alias{getLWCoeffs}
\title{Get Length-Weight Coefficients for Species}
\usage{
getLWCoeffs(taxa_list = NULL, asfis_list = NULL)
}
\arguments{
\item{taxa_list}{Character vector of FAO 3-alpha codes}

\item{asfis_list}{ASFIS list data frame}
}
\value{
A data frame with columns:
\itemize{
\item catch_taxon - FAO 3-alpha code
\item n - Number of measurements
\item a_75 - 75th percentile of parameter 'a'
\item b_75 - 75th percentile of parameter 'b'
}
}
\description{
Retrieves and summarizes length-weight relationship coefficients by handling both
FishBase and SeaLifeBase data in a single workflow.
}
\keyword{mining}
\keyword{preprocessing}
