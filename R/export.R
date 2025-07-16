#' Export WorldFish Summary Data to MongoDB
#'
#' @description
#' Downloads previously summarized WorldFish survey data from cloud storage, incorporates
#' modeled aggregated estimates, and exports everything to MongoDB collections for use
#' in data portals. The function also generates geographic regional summaries.
#'
#' @details
#' The function performs the following operations:
#' - Downloads five summary datasets from cloud storage:
#'   - Monthly summaries: Aggregated catch metrics by district and month
#'   - Taxa summaries: Species-specific metrics in long format
#'   - Districts summaries: District-level indicators over time
#'   - Gear summaries: Performance metrics by gear type
#'   - Grid summaries: Spatial grid data from vessel tracking
#' - Downloads aggregated catch estimates from the modeling step
#' - Creates geographic regional summaries using the monthly data
#' - Joins aggregated estimates (fishing trips, catch tonnage, revenue) to monthly summaries
#' - Transforms monthly summaries to long format for portal consumption
#' - Uploads all datasets to specified MongoDB collections
#'
#' The function expects the summary files to be named with the pattern:
#' `{file_prefix}_{table_name}.parquet` where table_name is one of:
#' monthly_summaries, taxa_summaries, districts_summaries, gear_summaries, grid_summaries
#'
#' @param log_threshold The logging level threshold for the logger package (e.g., DEBUG, INFO)
#'   See `logger::log_levels` for available options.
#'
#' @return NULL (invisible). The function uploads data to MongoDB as a side effect.
#'
#' @examples
#' \dontrun{
#' # Export WF summary data with default debug logging
#' export_wf_data()
#'
#' # Export with info-level logging only
#' export_wf_data(logger::INFO)
#' }
#'
#' @seealso
#' * [summarize_data()] for generating the summary datasets
#' * [download_parquet_from_cloud()] for retrieving data from cloud storage
#' * [mdb_collection_push()] for uploading data to MongoDB
#' * [create_geos()] for generating geographic summaries
#'
#' @keywords workflow export
#' @export
export_wf_data <- function(log_threshold = logger::DEBUG) {
  logger::log_threshold(log_threshold)
  pars <- read_config()

  # Download each parquet file
  data_summaries <- list()

  table_names <- c(
    "monthly_summaries",
    "taxa_summaries",
    "districts_summaries",
    "gear_summaries",
    "grid_summaries"
  )

  for (name in table_names) {
    prefix <- pars$surveys$wf_surveys$summaries$file_prefix %>%
      paste0("_", name)

    data_summaries[[name]] <- download_parquet_from_cloud(
      prefix = prefix,
      provider = pars$storage$google$key,
      options = pars$storage$google$options
    )
  }

  # Create geographic summaries
  create_geos_v1(
    monthly_summaries_dat = data_summaries$monthly_summaries,
    pars = pars
  )

  logger::log_info("Downloading aggregated catch data from cloud storage...")
  aggregated_filename <- cloud_object_name(
    prefix = pars$surveys$wf_surveys$aggregated$file_prefix,
    provider = pars$storage$google$key,
    extension = "rds",
    version = pars$pds$pds_tracks$version,
    options = pars$storage$google$options
  )

  download_cloud_file(
    name = aggregated_filename,
    provider = pars$storage$google$key,
    options = pars$storage$google$options
  )

  aggregated_data <- readr::read_rds(aggregated_filename)

  monthly_aggregated <-
    aggregated_data$district_totals |>
    dplyr::mutate(estimated_catch_tn = .data$estimated_total_catch_kg / 1000) |>
    dplyr::select(
      "district",
      "date" = "date_month",
      "estimated_fishing_trips" = "estimated_total_trips",
      "estimated_catch_tn",
      "estimated_revenue_TZS" = "estimated_total_revenue"
    )

  # Transform monthly summaries to long format for portal
  monthly_summaries <-
    data_summaries$monthly_summaries |>
    dplyr::left_join(monthly_aggregated, by = c("district", "date")) |>
    dplyr::relocate(
      "estimated_fishing_trips",
      .after = "date"
    ) |>
    dplyr::select(-c("mean_cpue_day", "mean_rpue_day")) |> # Drop map-specific metrics
    tidyr::pivot_longer(
      -c("date", "district"),
      names_to = "metric",
      values_to = "value"
    )

  # Dataframes to upload
  dataframes_to_upload <- list(
    monthly_summaries = monthly_summaries,
    taxa_summaries = data_summaries$taxa_summaries,
    districts_summaries = data_summaries$districts_summaries,
    gear_summaries = data_summaries$gear_summaries,
    grid_summaries = data_summaries$grid_summaries
  )

  # Collection names
  collection_names <- list(
    monthly_summaries = pars$storage$mongodb$portal$collection$monthly_summaries,
    taxa_summaries = pars$storage$mongodb$portal$collection$taxa_summaries,
    districts_summaries = pars$storage$mongodb$portal$collection$districts_summaries,
    gear_summaries = pars$storage$mongodb$portal$collection$gear_summaries,
    grid_summaries = pars$storage$mongodb$portal$collection$grid_summaries
  )

  # Iterate over the dataframes and upload them
  purrr::walk2(
    .x = dataframes_to_upload,
    .y = collection_names,
    .f = ~ {
      logger::log_info(paste("Uploading", .y, "data to MongoDB"))
      mdb_collection_push(
        data = .x,
        connection_string = pars$storage$mongodb$connection_string,
        collection_name = .y,
        db_name = pars$storage$mongodb$portal$database_name
      )
    }
  )
}

#' Generate Geographic Regional Summaries of Fishery Data
#'
#' @description
#' This function creates geospatial representations of fishery metrics by aggregating
#' lnding sites data to regional levels along the Zanzibar coast. It assigns
#' each site to its nearest coastal region, calculates regional summaries of fishery
#' performance metrics, and exports the results as a GeoJSON file for spatial visualization.
#'
#' @details
#' The function performs the following operations:
#' 1. **BMU Coordinate Extraction**: Retrieves geographic coordinates (latitude/longitude) for all landing sites
#' 2. **Spatial Conversion**: Converts sites coordinates to spatial point objects.
#' 3. **Regional Assignment**: Uses spatial analysis to assign each BMU to its nearest coastal region.
#' 4. **Regional Aggregation**: Calculates monthly summary statistics for each region by aggregating BMU data.
#' 5. **GeoJSON Creation**: Combines regional polygon geometries with summary statistics and exports as GeoJSON.
#'
#' **Calculated Regional Metrics** (using median values across BMUs in each region):
#' - Mean CPUE (Catch Per Unit Effort, kg per fisher)
#' - Mean RPUE (Revenue Per Unit Effort, currency per fisher)
#' - Mean Price per kg of catch

#'
#' @param monthly_summaries_dat A data frame containing monthly fishery metrics by site
#' @param pars Congiguration parameters
#'
#' @return This function does not return a value. It writes a GeoJSON file named
#'         "zanzibar_monthly_summaries.geojson" to the inst/ directory of the package,
#'         containing regional polygons with associated monthly fishery metrics.#'
#' @note
#' **Dependencies**:
#' - Requires the `sf` package for spatial operations.
#' - Requires a GeoJSON file named "ZAN_coast_regions.geojson" included in the
#'   peskas.zanzibar.data.pipeline package.
#' - Uses the `get_metadata()` function to retrieve sites location information.
#'
#' @importFrom sf st_as_sf st_read st_boundary st_distance st_write
#' @importFrom dplyr transmute left_join select group_by summarise mutate
#' @importFrom stats median
#' @importFrom stringr str_to_title
#'
#' @keywords export spatial
#' @examples
#' \dontrun{
#' # First generate monthly summaries
#' monthly_data <- get_fishery_metrics(validated_data, bmu_size)
#'
#' # Then create regional geospatial summary
#' create_geos(monthly_summaries_dat = monthly_data)
#' }
create_geos <- function(monthly_summaries_dat = NULL, pars = NULL) {
  zan_coords <-
    get_metadata("sites")$sites |>
    dplyr::transmute(
      .data$district,
      lat = as.numeric(.data$lat),
      lon = as.numeric(.data$lon)
    )

  sites_points <- sf::st_as_sf(
    zan_coords,
    coords = c("lon", "lat"),
    crs = 4326,
    na.fail = FALSE
  )

  zan_coast <- sf::st_read(system.file(
    "ZAN_coast_regions.geojson",
    package = "peskas.zanzibar.data.pipeline"
  ))

  # ASSIGN EACH site TO THE NEAREST REGION
  # Calculate boundaries of regions
  region_boundaries <- sf::st_boundary(zan_coast)

  # Calculate distances between sites points and region boundaries
  distances <- sf::st_distance(sites_points, region_boundaries)

  # Assign each site to the nearest region
  nearest_region <- as.numeric(apply(distances, 1, which.min))

  # Add the nearest region to the sites_points data frame
  sites_points$nearest_region <- zan_coast$region[nearest_region]

  geos_df <-
    sites_points |>
    dplyr::select("district", region = "nearest_region")

  region_monthly_summaries <-
    monthly_summaries_dat |>
    dplyr::left_join(geos_df, by = "district") |>
    dplyr::group_by(.data$region, .data$date) |>
    dplyr::summarise(
      mean_cpue = stats::median(.data$mean_cpue_day, na.rm = TRUE),
      mean_rpue = stats::median(.data$mean_rpue_day, na.rm = TRUE),
      mean_price_kg = stats::median(.data$mean_price_kg, na.rm = TRUE),
      .groups = "drop"
    ) |>
    dplyr::distinct() |>
    dplyr::mutate(
      date = format(.data$date, "%Y-%m-%dT%H:%M:%SZ"),
      country = "zanzibar",
      region = tolower(.data$region)
    ) |>
    dplyr::relocate("country", .before = "region")

  zan_coast <-
    zan_coast |>
    dplyr::mutate(
      country = "zanzibar",
      region = tolower(.data$region)
    ) |>
    dplyr::relocate("country", .before = "region")

  upload_parquet_to_cloud(
    data = region_monthly_summaries,
    prefix = "zanzibar_monthly_summaries_map",
    provider = pars$storage$google$key,
    options = pars$storage$google$options_coasts
  )

  filename_geo <-
    "ZAN_regions" %>%
    add_version(extension = "geojson")

  sf::st_write(
    zan_coast,
    filename_geo,
    driver = "GeoJSON",
    delete_dsn = TRUE
  )

  upload_cloud_file(
    file = filename_geo,
    provider = pars$storage$google$key,
    options = pars$storage$google$options_coasts
  )

  file.remove(filename_geo)
}


#' Generate Geographic Regional Summaries of Fishery Data (Version 1)
#'
#' @description
#' Creates geospatial representations of fishery metrics by aggregating landing
#' site data to regional levels along the Zanzibar coast. This simplified version
#' uses direct administrative district mappings instead of spatial proximity calculations,
#' making it more efficient for cases where district-to-region relationships are
#' already established.
#'
#' @details
#' The function performs the following operations:
#' 1. **District-Region Mapping**: Retrieves pre-defined administrative mappings
#'    between districts and regions from site metadata
#' 2. **Data Harmonization**: Standardizes district names (e.g., "Chake chake" to
#'    "Chake Chake") to ensure proper joining
#' 3. **Regional Aggregation**: Calculates monthly summary statistics for each
#'    region by aggregating district-level data
#' 4. **GeoJSON Export**: Combines regional polygon geometries with summary
#'    statistics and exports to cloud storage
#'
#' **Key Differences from create_geos()**:
#' - Uses administrative mappings (ADM column) instead of spatial distance calculations
#' - More efficient as it avoids complex spatial operations
#' - Relies on pre-established district-region relationships in metadata
#' - Handles district name inconsistencies automatically
#'
#' **Calculated Regional Metrics** (using median values across districts in each region):
#' - Mean CPUE (Catch Per Unit Effort, kg per fisher per day)
#' - Mean RPUE (Revenue Per Unit Effort, TZS per fisher per day)
#' - Mean Price per kg of catch
#'
#' **Output Format**:
#' - Regional summaries are exported as Parquet files to cloud storage
#' - GeoJSON file containing regional boundaries is uploaded separately
#' - Both files use standardized naming conventions with version information
#'
#' @param monthly_summaries_dat A data frame containing monthly fishery metrics by district.
#'   Required columns:
#'   - `district`: Character, name of the landing site district
#'   - `date`: Date, month of the summary
#'   - `mean_cpue_day`: Numeric, mean catch per unit effort per day
#'   - `mean_rpue_day`: Numeric, mean revenue per unit effort per day
#'   - `mean_price_kg`: Numeric, mean price per kilogram
#'
#' @param pars Configuration parameters list containing:
#'   - `storage$google$key`: Cloud storage provider key
#'   - `storage$google$options_coasts`: Cloud storage options for coastal data
#'
#' @return NULL (invisible). The function uploads data to cloud storage as side effects:
#'   - Parquet file: "zanzibar_monthly_summaries_map" containing regional summaries
#'   - GeoJSON file: "ZAN_regions_[version].geojson" containing regional boundaries
#'
#' @note
#' **Dependencies**:
#' - Requires the `sf` package for reading and writing spatial data
#' - Requires "ZAN_coast_regions.geojson" file in the package inst/ directory
#' - Uses `get_metadata()` function to retrieve district-region mappings
#' - Uses `add_version()` to append version information to filenames
#' - Uses `upload_parquet_to_cloud()` and `upload_cloud_file()` for cloud storage
#'
#' **Data Processing Notes**:
#' - District names are case-corrected (specifically "Chake chake" → "Chake Chake")
#' - Regions are converted to lowercase in the output for consistency
#' - Dates are formatted in ISO 8601 format (YYYY-MM-DDTHH:MM:SSZ)
#' - All outputs include "country" = "zanzibar" for multi-country compatibility
#'
#' @seealso
#' * [create_geos()] for the spatial proximity-based version
#' * [export_wf_data()] which calls this function as part of the export workflow
#' * [get_metadata()] for retrieving site and administrative information
#'
#' @examples
#' \dontrun{
#' # Load configuration
#' pars <- read_config()
#'
#' # Get monthly summaries data
#' monthly_data <- download_parquet_from_cloud(
#'   prefix = "wf_monthly_summaries",
#'   provider = pars$storage$google$key,
#'   options = pars$storage$google$options
#' )
#'
#' # Create regional geospatial summaries
#' create_geos_v1(
#'   monthly_summaries_dat = monthly_data,
#'   pars = pars
#' )
#' }
#'
#' @keywords export spatial
#' @importFrom sf st_read st_write
#' @importFrom dplyr select distinct left_join rename mutate if_else group_by summarise relocate
#' @importFrom stats median
create_geos_v1 <- function(monthly_summaries_dat = NULL, pars = NULL) {
  zan_coords <-
    get_metadata("sites")$sites |>
    dplyr::select("ADM", "district") |>
    dplyr::distinct()

  zan_coast <- sf::st_read(system.file(
    "ZAN_coast_regions.geojson",
    package = "peskas.zanzibar.data.pipeline"
  ))

  geos_df <-
    dplyr::left_join(
      zan_coords,
      zan_coast,
      by = c("ADM" = "region")
    ) |>
    dplyr::rename(region = "ADM")

  region_monthly_summaries <-
    monthly_summaries_dat |>
    dplyr::mutate(
      district = dplyr::if_else(
        .data$district == "Chake chake",
        "Chake Chake",
        .data$district
      )
    ) |>
    dplyr::left_join(geos_df, by = "district") |>
    dplyr::group_by(.data$region, .data$date) |>
    dplyr::summarise(
      mean_cpue = stats::median(.data$mean_cpue_day, na.rm = TRUE),
      mean_rpue = stats::median(.data$mean_rpue_day, na.rm = TRUE),
      mean_price_kg = stats::median(.data$mean_price_kg, na.rm = TRUE),
      .groups = "drop"
    ) |>
    dplyr::distinct() |>
    dplyr::mutate(
      date = format(.data$date, "%Y-%m-%dT%H:%M:%SZ"),
      country = "zanzibar",
      region = tolower(.data$region)
    ) |>
    dplyr::relocate("country", .before = "region")

  zan_coast <-
    zan_coast |>
    dplyr::mutate(
      country = "zanzibar",
      region = tolower(.data$region)
    ) |>
    dplyr::relocate("country", .before = "region")

  upload_parquet_to_cloud(
    data = region_monthly_summaries,
    prefix = "zanzibar_monthly_summaries_map",
    provider = pars$storage$google$key,
    options = pars$storage$google$options_coasts
  )

  filename_geo <-
    "ZAN_regions" %>%
    add_version(extension = "geojson")

  sf::st_write(
    zan_coast,
    filename_geo,
    driver = "GeoJSON",
    delete_dsn = TRUE
  )

  upload_cloud_file(
    file = filename_geo,
    provider = pars$storage$google$key,
    options = pars$storage$google$options_coasts
  )

  file.remove(filename_geo)
}
