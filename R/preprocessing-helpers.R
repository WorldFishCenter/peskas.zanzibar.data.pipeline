#' Preprocess Track Data into Spatial Grid Summary
#'
#' @description
#' This function processes GPS track data into a spatial grid summary, calculating time spent
#' and other metrics for each grid cell. The grid size can be specified to analyze spatial
#' patterns at different scales.
#'
#' @param data A data frame containing GPS track data with columns:
#'   - Trip: Unique trip identifier
#'   - Time: Timestamp of the GPS point
#'   - Lat: Latitude
#'   - Lng: Longitude
#'   - Speed (M/S): Speed in meters per second
#'   - Range (Meters): Range in meters
#'   - Heading: Heading in degrees
#'
#' @param grid_size Numeric. Size of grid cells in meters. Must be one of:
#'   - 100: ~100m grid cells
#'   - 250: ~250m grid cells
#'   - 500: ~500m grid cells (default)
#'   - 1000: ~1km grid cells
#'
#' @return A tibble with the following columns:
#'   - Trip: Trip identifier
#'   - lat_grid: Latitude of grid cell center
#'   - lng_grid: Longitude of grid cell center
#'   - time_spent_mins: Total time spent in grid cell in minutes
#'   - mean_speed: Average speed in grid cell (M/S)
#'   - mean_range: Average range in grid cell (Meters)
#'   - first_seen: First timestamp in grid cell
#'   - last_seen: Last timestamp in grid cell
#'   - n_points: Number of GPS points in grid cell
#'
#' @details
#' The function creates a grid by rounding coordinates based on the specified grid size.
#' Grid sizes are approximate due to the conversion from meters to degrees, with calculations
#' based on 1 degree ≈ 111km at the equator. Time spent is calculated using the time
#' differences between consecutive points.
#'
#' @keywords preprocessing
#'
#' @examples
#' \dontrun{
#' # Process tracks with 500m grid (default)
#' result_500m <- preprocess_track_data(tracks_data)
#'
#' # Use 100m grid for finer resolution
#' result_100m <- preprocess_track_data(tracks_data, grid_size = 100)
#'
#' # Use 1km grid for broader patterns
#' result_1km <- preprocess_track_data(tracks_data, grid_size = 1000)
#' }
#'
#' @keywords preprocessing
#' @export
preprocess_track_data <- function(data, grid_size = 500) {
  # Define grid size in meters to degrees (approximately)
  # 1 degree = 111km at equator
  grid_degrees <- switch(
    as.character(grid_size),
    "100" = 0.001, # ~100m
    "250" = 0.0025, # ~250m
    "500" = 0.005, # ~500m
    "1000" = 0.01, # ~1km
    stop("grid_size must be one of: 100, 250, 500, 1000")
  )

  data %>%
    dplyr::select(
      "Trip",
      "Time",
      "Lat",
      "Lng",
      "Speed (M/S)",
      "Range (Meters)",
      "Heading"
    ) %>%
    dplyr::group_by(.data$Trip) %>%
    dplyr::arrange(.data$Time) %>%
    dplyr::mutate(
      # Create grid cells based on selected size
      lat_grid = round(.data$Lat / grid_degrees, 0) * grid_degrees,
      lng_grid = round(.data$Lng / grid_degrees, 0) * grid_degrees,

      # Calculate time spent (difference with next point)
      time_diff = as.numeric(difftime(
        dplyr::lead(.data$Time),
        .data$Time,
        units = "mins"
      )),
      # For last point in series, use difference with previous point
      time_diff = dplyr::if_else(
        is.na(.data$time_diff),
        as.numeric(difftime(
          .data$Time,
          dplyr::lag(.data$Time),
          units = "mins"
        )),
        .data$time_diff
      )
    ) %>%
    # Group by trip and grid cell
    dplyr::group_by(.data$Trip, .data$lat_grid, .data$lng_grid) %>%
    dplyr::summarise(
      time_spent_mins = sum(.data$time_diff, na.rm = TRUE),
      mean_speed = mean(.data$`Speed (M/S)`, na.rm = TRUE),
      mean_range = mean(.data$`Range (Meters)`, na.rm = TRUE),
      first_seen = min(.data$Time),
      last_seen = max(.data$Time),
      n_points = dplyr::n(),
      .groups = "drop"
    ) %>%
    dplyr::filter(.data$time_spent_mins > 0) %>%
    dplyr::group_by(.data$Trip) %>%
    dplyr::arrange(.data$first_seen) %>%
    dplyr::filter(
      !(dplyr::row_number() %in% c(1, 2, dplyr::n() - 1, dplyr::n()))
    ) %>%
    dplyr::ungroup()
}

#' Generate Grid Summaries for Track Data
#'
#' @description
#' Processes GPS track data into 1km grid summaries for visualization and analysis.
#'
#' @param data Preprocessed track data
#' @param min_hours Minimum hours threshold for filtering (default: 0.15)
#' @param max_hours Maximum hours threshold for filtering (default: 10)
#'
#' @return A dataframe with grid summary statistics
#'
#' @keywords preprocessing
#' @export
generate_track_summaries <- function(data, min_hours = 0.15, max_hours = 15) {
  data %>%
    # First summarize by current grid (500m)
    dplyr::group_by(.data$lat_grid, .data$lng_grid) %>%
    dplyr::summarise(
      avg_time_mins = mean(.data$time_spent_mins),
      avg_speed = mean(.data$mean_speed),
      avg_range = mean(.data$mean_range),
      visits = dplyr::n_distinct(.data$Trip),
      total_points = sum(.data$n_points),
      .groups = "drop"
    ) %>%
    # Then regrid to 1km
    dplyr::mutate(
      lat_grid_1km = round(.data$lat_grid / 0.01) * 0.01,
      lng_grid_1km = round(.data$lng_grid / 0.01) * 0.01
    ) %>%
    dplyr::group_by(.data$lat_grid_1km, .data$lng_grid_1km) %>%
    dplyr::summarise(
      avg_time_mins = mean(.data$avg_time_mins),
      avg_speed = mean(.data$avg_speed),
      avg_range = mean(.data$avg_range),
      total_visits = sum(.data$visits),
      original_cells = dplyr::n(),
      total_points = sum(.data$total_points),
      .groups = "drop"
    ) %>%
    dplyr::mutate(
      avg_time_hours = .data$avg_time_mins / 60
    ) %>%
    dplyr::filter(
      .data$avg_time_hours >= min_hours,
      .data$avg_time_hours <= max_hours
    ) |>
    dplyr::select(-"avg_time_mins")
}

#' Calculate Fishery Metrics
#'
#' Transforms catch-level data into normalized fishery performance indicators.
#' Calculates site-level, gear-specific, and species-specific metrics.
#'
#' @param data A data frame with catch records containing required columns:
#'   submission_id, landing_date, district, gear, catch_outcome, no_men_fishers,
#'   no_women_fishers, no_child_fishers, catch_taxon, catch_price, catch_kg
#'
#' @return A data frame in normalized long format with columns: landing_site,
#'   year_month, metric_type, metric_value, gear_type, species, rank
#'
#' @keywords preprocessing
#' @export
calculate_fishery_metrics <- function(data = NULL) {
  catch_data <- data |>
    dplyr::filter(.data$catch_outcome == "1") |>
    dplyr::select(
      "submission_id",
      "landing_date",
      "district",
      "gear",
      "trip_duration",
      dplyr::starts_with("no_"),
      "catch_taxon",
      "catch_price",
      "catch_kg"
    ) |>
    dplyr::mutate(
      n_fishers = .data$no_men_fishers +
        .data$no_women_fishers +
        .data$no_child_fishers,
      year_month = lubridate::floor_date(.data$landing_date, "month")
    ) |>
    dplyr::select(
      -c("no_men_fishers", "no_women_fishers", "no_child_fishers")
    ) |>
    dplyr::rename(
      landing_site = .data$district,
      species = .data$catch_taxon
    )

  trip_level_data <- catch_data |>
    dplyr::group_by(
      .data$submission_id,
      .data$landing_date,
      .data$landing_site,
      .data$gear,
      .data$n_fishers,
      .data$year_month
    ) |>
    dplyr::summarise(
      trip_total_catch_kg = sum(.data$catch_kg, na.rm = TRUE),
      trip_total_revenue = sum(.data$catch_price, na.rm = TRUE),
      .groups = "drop"
    )

  site_level_metrics <- trip_level_data |>
    dplyr::group_by(.data$landing_site, .data$year_month) |>
    dplyr::summarise(
      avg_fishers_per_trip = mean(.data$n_fishers, na.rm = TRUE),
      avg_catch_per_trip = mean(.data$trip_total_catch_kg, na.rm = TRUE),
      .groups = "drop"
    ) |>
    tidyr::pivot_longer(
      cols = c(.data$avg_fishers_per_trip, .data$avg_catch_per_trip),
      names_to = "metric_type",
      values_to = "metric_value"
    ) |>
    dplyr::mutate(
      gear_type = NA_character_,
      species = NA_character_,
      rank = NA_integer_
    )

  gear_metrics <- trip_level_data |>
    dplyr::group_by(.data$landing_site, .data$year_month) |>
    dplyr::mutate(total_trips = dplyr::n()) |>
    dplyr::add_count(.data$gear, name = "gear_count") |>
    dplyr::slice_max(.data$gear_count, n = 1, with_ties = FALSE) |>
    dplyr::distinct(.data$landing_site, .data$year_month, .keep_all = TRUE) |>
    dplyr::mutate(
      pct_main_gear = (.data$gear_count / .data$total_trips) * 100
    ) |>
    dplyr::select(
      .data$landing_site,
      .data$year_month,
      .data$gear,
      .data$pct_main_gear
    ) |>
    dplyr::ungroup()

  predominant_gear_metrics <- gear_metrics |>
    dplyr::transmute(
      landing_site = .data$landing_site,
      year_month = .data$year_month,
      metric_type = "predominant_gear",
      metric_value = NA_real_,
      gear_type = .data$gear,
      species = NA_character_,
      rank = NA_integer_
    )

  pct_main_gear_metrics <- gear_metrics |>
    dplyr::transmute(
      landing_site = .data$landing_site,
      year_month = .data$year_month,
      metric_type = "pct_main_gear",
      metric_value = .data$pct_main_gear,
      gear_type = NA_character_,
      species = NA_character_,
      rank = NA_integer_
    )

  cpue_metrics <- trip_level_data |>
    dplyr::mutate(cpue = .data$trip_total_catch_kg / .data$n_fishers) |>
    dplyr::group_by(.data$landing_site, .data$year_month, .data$gear) |>
    dplyr::summarise(
      avg_cpue = mean(.data$cpue, na.rm = TRUE),
      .groups = "drop"
    ) |>
    dplyr::transmute(
      landing_site = .data$landing_site,
      year_month = .data$year_month,
      metric_type = "cpue",
      metric_value = .data$avg_cpue,
      gear_type = .data$gear,
      species = NA_character_,
      rank = NA_integer_
    )

  rpue_metrics <- trip_level_data |>
    dplyr::mutate(rpue = .data$trip_total_revenue / .data$n_fishers) |>
    dplyr::group_by(.data$landing_site, .data$year_month, .data$gear) |>
    dplyr::summarise(
      avg_rpue = mean(.data$rpue, na.rm = TRUE),
      .groups = "drop"
    ) |>
    dplyr::transmute(
      landing_site = .data$landing_site,
      year_month = .data$year_month,
      metric_type = "rpue",
      metric_value = .data$avg_rpue,
      gear_type = .data$gear,
      species = NA_character_,
      rank = NA_integer_
    )

  species_metrics <- catch_data |>
    dplyr::group_by(.data$landing_site, .data$year_month, .data$species) |>
    dplyr::summarise(
      total_species_catch = sum(.data$catch_kg, na.rm = TRUE),
      .groups = "drop"
    ) |>
    dplyr::group_by(.data$landing_site, .data$year_month) |>
    dplyr::mutate(
      total_site_catch = sum(.data$total_species_catch),
      species_pct = (.data$total_species_catch / .data$total_site_catch) * 100
    ) |>
    dplyr::arrange(
      .data$landing_site,
      .data$year_month,
      dplyr::desc(.data$species_pct)
    ) |>
    dplyr::mutate(rank = dplyr::row_number()) |>
    dplyr::filter(.data$rank <= 2) |>
    dplyr::transmute(
      landing_site = .data$landing_site,
      year_month = .data$year_month,
      metric_type = "species_pct",
      metric_value = .data$species_pct,
      gear_type = NA_character_,
      species = .data$species,
      rank = .data$rank
    ) |>
    dplyr::ungroup()

  fishery_metrics <- dplyr::bind_rows(
    site_level_metrics,
    predominant_gear_metrics,
    pct_main_gear_metrics,
    cpue_metrics,
    rpue_metrics,
    species_metrics
  ) |>
    dplyr::arrange(.data$landing_site, .data$year_month, .data$metric_type)

  return(fishery_metrics)
}
