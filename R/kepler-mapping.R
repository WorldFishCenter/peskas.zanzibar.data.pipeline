#' Generate a Kepler.gl map
#'
#' This function is a R wrapper of `kepler_wcs_mapper_py`, a python script function
#' aimed to elaborate produce a self-contained map (in html) using the
#' Kepler.gl python library \url{https://docs.kepler.gl/docs/keplergl-jupyter}.
#'
#' @param data_path Data to add to map.
#'
#' @keywords visualization
#' @return A self-contained map in html.
#' @export
#'
kepler_mapper <- function(data_path = NULL) {
  python_path <- system.file(package = "peskas.zanzibar.data.pipeline")
  kepler_wcs_mapper_py <- reticulate::import_from_path(
    module = "kepler_wcs_surveys",
    path = python_path
  )
  py_function <- kepler_wcs_mapper_py$kepler_map
  py_function(data_path)
}
