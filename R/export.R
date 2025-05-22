#' Export Processed Fisheries Data to MongoDB
#'
#' @description
#' Processes validated survey data to generate summary metrics and exports them to MongoDB collections.
#' The function calculates three main types of metrics:
#' 1. Monthly CPUE and RPUE metrics by landing site
#' 2. Gear-specific metrics by landing site and habitat
#' 3. Taxa proportions by landing site
#'
#' @details
#' The function performs the following operations:
#' - Filters out landing sites with >75% missing CPUE data
#' - Calculates monthly median CPUE and RPUE values
#' - Processes gear-specific metrics, excluding gears with spaces in names and "other" category
#' - Computes taxa proportions, grouping taxa with <5% representation as "Others"
#' - Uploads processed data to specified MongoDB collections
#'
#' The metrics calculated include:
#' - CPUE (Catch Per Unit Effort)
#' - RPUE (Revenue Per Unit Effort)
#' - Catch proportions by taxa
#'
#' Exported collections:
#' - Monthly metrics: Time series of CPUE and RPUE by landing site
#' - Gear metrics: CPUE and RPUE by gear type, habitat, and landing site
#' - Taxa proportions: Percentage contribution of each taxa to total catch by landing site
#'
#' @param log_threshold The logging level threshold for the logger package (e.g., DEBUG, INFO)
#'   See `logger::log_levels` for available options.
#'
#' @return None (invisible). The function performs its operations for side effects:
#'   - Processes survey data into summary metrics
#'   - Uploads results to MongoDB collections
#'   - Generates logs of the process
#'
#' @examples
#' \dontrun{
#' # Export data with default debug logging
#' export_data()
#'
#' # Export with info-level logging only
#' export_data(logger::INFO)
#' }
#'
#' @seealso
#' * [get_validated_surveys()] for details on the input data format
#' * [mdb_collection_push()] for details on the MongoDB upload process
#' * [expand_taxa()] for details on taxa classification
#'
#' @keywords workflow export
#' @export
export_data <- function(log_threshold = logger::DEBUG) {
  logger::log_threshold(log_threshold)
  pars <- read_config()

  validated_surveys <- get_validated_surveys(pars, sources = "wf")

  # Get unique landing sites first for nesting
  landing_sites <- validated_surveys %>%
    dplyr::pull(.data$landing_site) %>%
    unique()

  monthly_metrics <-
    validated_surveys |>
    dplyr::mutate(date = lubridate::floor_date(.data$landing_date, "month")) |>
    dplyr::group_by(.data$date, .data$landing_site) |>
    dplyr::summarise(
      median_cpue = stats::median(.data$cpue, na.rm = TRUE),
      median_rpue = stats::median(.data$rpue, na.rm = TRUE),
      n = dplyr::n(),
      .groups = "drop"
    ) |>
    dplyr::ungroup() |>
    tidyr::complete(
      date,
      landing_site = landing_sites,
      fill = list(median_cpue = NA_real_, median_rpue = NA_real_, n = 0)
    )

  sites_selected <-
    monthly_metrics %>%
    dplyr::group_by(.data$landing_site) %>%
    dplyr::summarise(
      missing_cpue = mean(is.na(.data$median_cpue)) * 100,
      .groups = "drop"
    ) %>%
    dplyr::arrange(dplyr::desc(.data$missing_cpue)) %>%
    dplyr::filter(.data$missing_cpue < 75) %>%
    dplyr::pull(.data$landing_site)

  monthly_metrics_tidy <-
    monthly_metrics |>
    dplyr::filter(.data$landing_site %in% sites_selected) |>
    dplyr::mutate(date = lubridate::as_datetime(.data$date)) |>
    tidyr::pivot_longer(
      -c("date", "landing_site", "n"),
      names_to = "metric",
      values_to = "value"
    )

  gear_metrics_tidy <-
    validated_surveys |>
    dplyr::group_by(.data$landing_site, .data$habitat, .data$gear) |>
    dplyr::filter(
      !is.na(.data$gear),
      !stringr::str_detect(.data$gear, " "),
      !.data$gear == "other"
    ) |>
    dplyr::summarise(
      median_cpue = stats::median(.data$cpue, na.rm = TRUE),
      median_rpue = stats::median(.data$rpue, na.rm = TRUE),
      n = dplyr::n(),
      .groups = "drop"
    ) |>
    tidyr::pivot_longer(
      -c("landing_site", "habitat", "gear", "n"),
      names_to = "metric",
      values_to = "value"
    )

  taxa_proportion <-
    validated_surveys |>
    dplyr::filter(.data$landing_site %in% sites_selected) |>
    dplyr::select("survey_id", "landing_site", "catch_taxa") |>
    tidyr::unnest("catch_taxa") |>
    expand_taxa() |>
    dplyr::select("survey_id", "landing_site", "family", "catch_kg") |>
    dplyr::group_by(.data$survey_id, .data$landing_site) |>
    dplyr::mutate(n_catches = dplyr::n()) |>
    dplyr::rowwise() |>
    dplyr::mutate(taxa_catch_kg = .data$catch_kg / .data$n_catches) |>
    dplyr::select(-c("catch_kg", "n_catches")) |>
    dplyr::group_by(.data$landing_site, .data$family) |>
    dplyr::summarise(
      total_catch_kg = sum(.data$taxa_catch_kg),
      .groups = "drop"
    ) |>
    dplyr::filter(!is.na(.data$family)) |>
    dplyr::group_by(.data$landing_site) |>
    dplyr::mutate(
      overall_catch = sum(.data$total_catch_kg),
      catch_prop = (.data$total_catch_kg / .data$overall_catch) * 100
    ) |>
    dplyr::ungroup() |>
    dplyr::select(-c("total_catch_kg", "overall_catch")) |>
    tidyr::complete(
      landing_site = unique(.data$landing_site),
      family = unique(.data$family),
      fill = list(catch_prop = 0)
    ) |>
    dplyr::group_by(.data$landing_site) |>
    dplyr::mutate(
      family = ifelse(.data$catch_prop < 5, "Others", .data$family)
    ) |>
    dplyr::group_by(.data$landing_site, .data$family) |>
    dplyr::summarise(
      catch_prop = sum(.data$catch_prop),
      .groups = "drop"
    )

  # Dataframes to upload
  dataframes_to_upload <- list(
    monthly_metrics_tidy = monthly_metrics_tidy,
    gear_metrics_tidy = gear_metrics_tidy,
    taxa_proportion = taxa_proportion
  )

  # Collection names
  collection_names <- list(
    monthly_metrics_tidy = pars$storage$mongodb$pipeline$collection$monthly_metrics,
    gear_metrics_tidy = pars$storage$mongodb$pipeline$collection$gear_metrics,
    taxa_proportion = pars$storage$mongodb$pipeline$collection$taxa
  )

  # Upload data
  purrr::walk2(
    .x = dataframes_to_upload,
    .y = collection_names,
    .f = ~ {
      logger::log_info(paste("Uploading", .y, "data to MongoDB"))
      mdb_collection_push(
        data = .x,
        connection_string = pars$storage$mongodb$connection_string,
        collection_name = .y,
        db_name = pars$storage$mongodb$pipeline$database_name
      )
    }
  )
}

#' Export WorldFish Survey Data
#'
#' @description
#' Processes validated survey data from WorldFish sources, filtering out flagged submissions
#' and generating two key datasets:
#' 1. Indicators dataset with aggregated catch metrics
#' 2. Taxa dataset with species-specific catch information
#'
#' @details
#' The function performs the following operations:
#' - Retrieves validated WF survey data
#' - Pulls submission flags from MongoDB
#' - Filters out submissions with alert flags
#' - For the indicators dataset:
#'   - Aggregates catch data by submission
#'   - Calculates price per kg, CPUE, and RPUE metrics
#' - For the taxa dataset:
#'   - Preserves taxonomic information
#'   - Calculates catch metrics by species
#'
#' The metrics calculated include:
#' - Total catch weight per submission
#' - Price per kg of catch
#' - CPUE (Catch Per Unit Effort)
#' - RPUE (Revenue Per Unit Effort)
#'
#' @param log_threshold The logging level threshold for the logger package (e.g., DEBUG, INFO)
#'   See `logger::log_levels` for available options.
#'
#' @return Two data frames (invisible):
#'   - indicators_df: Aggregated catch metrics by submission
#'   - taxa_df: Species-specific catch information
#'
#' @examples
#' \dontrun{
#' # Export WF data with default debug logging
#' export_wf_data()
#'
#' # Export with info-level logging only
#' export_wf_data(logger::INFO)
#' }
#'
#' @seealso
#' * [get_validated_surveys()] for details on the input data format
#' * [mdb_collection_pull()] for retrieving flag information
#' * [export_data()] for the more comprehensive export function
#'
#' @keywords workflow export
#' @export
export_wf_data <- function(log_threshold = logger::DEBUG) {
  logger::log_threshold(log_threshold)
  pars <- read_config()
  metadata_tables <- get_metadata()

  validated_surveys <-
    get_validated_surveys(pars, sources = "wf") |>
    dplyr::select(-"source")

  # Choose a parallelization strategy - adjust the number of workers as needed
  future::plan(
    strategy = future::multisession,
    workers = future::availableCores() - 2
  ) # Use 4 parallel workers

  # get validation table
  valid_ids <-
    unique(validated_surveys$submission_id) %>%
    furrr::future_map_dfr(
      get_validation_status,
      asset_id = pars$surveys$wf_surveys$asset_id,
      token = pars$surveys$wf_surveys$token,
      .options = furrr::furrr_options(seed = TRUE)
    ) |>
    dplyr::filter(.data$validation_status == "validation_status_approved") |>
    dplyr::pull(.data$submission_id) |>
    unique()

  clean_data <-
    validated_surveys |>
    dplyr::filter(.data$submission_id %in% valid_ids)

  indicators_df <-
    clean_data |>
    dplyr::filter(.data$collect_data_today == "1") |>
    dplyr::mutate(
      n_fishers = .data$no_men_fishers +
        .data$no_women_fishers +
        .data$no_child_fishers
    ) |>
    dplyr::select(
      "submission_id",
      "landing_date",
      "district",
      "landing_site",
      "habitat",
      "gear",
      "vessel_type",
      "propulsion_gear",
      "fuel_L",
      "trip_duration",
      "vessel_type",
      "n_fishers",
      "catch_taxon",
      "catch_price",
      "catch_kg"
    ) |>
    dplyr::group_by(.data$submission_id) |>
    dplyr::summarise(
      dplyr::across(
        .cols = c(
          "landing_date",
          "district",
          "landing_site",
          "habitat",
          "gear",
          "vessel_type",
          "propulsion_gear",
          "fuel_L",
          "trip_duration",
          "vessel_type",
          "n_fishers",
          "catch_price"
        ),
        ~ dplyr::first(.x)
      ),
      tot_catch_kg = sum(.data$catch_kg),
      catch_taxon = paste(unique(.data$catch_taxon), collapse = "-")
    ) |>
    dplyr::mutate(
      catch_price = .data$catch_price,
      price_kg = .data$catch_price / .data$tot_catch_kg,
      cpue = .data$tot_catch_kg / .data$n_fishers / .data$trip_duration,
      rpue = .data$catch_price / .data$n_fishers / .data$trip_duration,
      cpue_day = .data$tot_catch_kg / .data$n_fishers, # CPUE per day for map data (assuming 1 trip per day)
      rpue_day = .data$catch_price / .data$n_fishers # RPUE per day for map data (assuming 1 trip per day)
    )

  taxa_df <-
    clean_data |>
    dplyr::filter(.data$collect_data_today == "1") |>
    dplyr::mutate(
      n_fishers = .data$no_men_fishers +
        .data$no_women_fishers +
        .data$no_child_fishers
    ) |>
    dplyr::select(
      "submission_id",
      "landing_date",
      "district",
      "landing_site",
      "habitat",
      "gear",
      "vessel_type",
      "propulsion_gear",
      "fuel_L",
      "trip_duration",
      "vessel_type",
      "n_fishers",
      "catch_taxon",
      "length",
      "catch_price",
      "catch_kg"
    ) |>
    dplyr::group_by(.data$submission_id) |>
    dplyr::mutate(
      n_catch = dplyr::n(),
      .groups = "drop"
    ) |>
    dplyr::group_by(.data$submission_id, .data$catch_taxon) |>
    dplyr::summarise(
      dplyr::across(
        .cols = c(
          "n_catch",
          "landing_date",
          "district",
          "landing_site",
          "habitat",
          "gear",
          "vessel_type",
          "propulsion_gear",
          "fuel_L",
          "trip_duration",
          "vessel_type",
          "n_fishers",
          "catch_price"
        ),
        ~ dplyr::first(.x)
      ),
      lenght = mean(.data$length),
      tot_catch_kg = sum(.data$catch_kg),
      .groups = "drop"
    ) |>
    dplyr::relocate("n_catch", .after = "submission_id")

  monthly_summaries <-
    indicators_df |>
    dplyr::mutate(
      date = lubridate::floor_date(.data$landing_date, "month"),
      date = lubridate::as_datetime(.data$date)
    ) |>
    dplyr::group_by(.data$district, .data$date) |>
    dplyr::summarise(
      dplyr::across(
        .cols = c(
          "cpue",
          "cpue_day",
          "rpue",
          "rpue_day",
          "price_kg"
        ),
        ~ mean(.x, na.rm = TRUE)
      ),
      .groups = "drop"
    ) |>
    dplyr::rename(
      mean_cpue = "cpue",
      mean_cpue_day = "cpue_day",
      mean_rpue = "rpue",
      mean_rpue_day = "rpue_day",
      mean_price_kg = "price_kg"
    ) |>
    tidyr::complete(
      .data$district,
      date = seq(min(.data$date), max(.data$date), by = "month"),
      fill = list(
        mean_cpue = NA,
        mean_cpue_day = NA,
        mean_rpue = NA,
        mean_rpue_day = NA,
        mean_price_kg = NA
      )
    ) |>
    dplyr::mutate(
      district = stringr::str_to_title(.data$district),
      district = stringr::str_replace(.data$district, "_", " ")
    )

  create_geos(monthly_summaries_dat = monthly_summaries, pars = pars)

  monthly_summaries <-
    monthly_summaries |>
    dplyr::select(-c("mean_cpue_day", "mean_rpue_day")) |> # we can drop these as these metrics were only used for mapping
    tidyr::pivot_longer(
      -c("date", "district"),
      names_to = "metric",
      values_to = "value"
    )

  taxa_summaries <-
    taxa_df |>
    dplyr::group_by(.data$district, .data$catch_taxon) |>
    dplyr::summarise(
      catch_kg = sum(.data$tot_catch_kg, na.rm = T),
      catch_price = sum(.data$catch_price, na.rm = T),
      mean_length = mean(.data$lenght, na.rm = T),
      .groups = "drop"
    ) |>
    dplyr::rename(alpha3_code = "catch_taxon") |>
    dplyr::left_join(metadata_tables$catch_type, by = "alpha3_code") |>
    dplyr::select(
      "district",
      "common_name",
      "catch_kg",
      "catch_price",
      "mean_length"
    ) |>
    tidyr::complete(
      .data$district,
      .data$common_name,
      fill = list(
        catch_kg = 0,
        catch_price = NA,
        length = NA
      )
    ) |>
    dplyr::mutate(price_kg = .data$catch_price / .data$catch_kg) |>
    dplyr::select(-c("catch_price")) |>
    tidyr::pivot_longer(
      -c("district", "common_name"),
      names_to = "metric",
      values_to = "value"
    ) |>
    dplyr::mutate(
      district = stringr::str_to_title(.data$district),
      district = stringr::str_replace(.data$district, "_", " ")
    )

  districts_summaries <-
    indicators_df |>
    dplyr::group_by(.data$district) |>
    dplyr::summarise(
      n_submissions = dplyr::n(),
      n_fishers = mean(.data$n_fishers),
      trip_duration = mean(.data$trip_duration),
      mean_cpue = mean(.data$cpue, na.rm = TRUE),
      mean_rpue = mean(.data$rpue, na.rm = TRUE),
      mean_price_kg = mean(.data$price_kg, na.rm = TRUE),
      .groups = "drop"
    ) |>
    tidyr::pivot_longer(
      -"district",
      names_to = "indicator",
      values_to = "value"
    ) |>
    dplyr::mutate(
      district = stringr::str_to_title(.data$district),
      district = stringr::str_replace(.data$district, "_", " ")
    )

  gear_summaries <-
    indicators_df |>
    dplyr::group_by(.data$district, .data$gear) |>
    dplyr::summarise(
      n_submissions = dplyr::n(),
      cpue = mean(.data$cpue, na.rm = T),
      rpue = mean(.data$rpue),
      .groups = "drop"
    ) |>
    dplyr::mutate(
      gear = dplyr::case_when(
        .data$gear == "HL" ~ "Hand line",
        .data$gear == "BS" ~ "Beach Seine",
        .data$gear == "CS" ~ "Cast Net",
        .data$gear == "GN" ~ "Gill Net",
        .data$gear == "LL" ~ "Long line",
        .data$gear == "SP" ~ "Spear gun",
        .data$gear == "SR" ~ "Stick Rod",
        .data$gear == "PS" ~ "Purse Seine",
        .data$gear == "RN" ~ "Ring Net",
        .data$gear == "SN" ~ "Shark Net",
        .data$gear == "TR" ~ "Trap",
        TRUE ~ .data$gear # Keep original value if no match
      )
    ) |>
    tidyr::pivot_longer(
      -c("district", "gear"),
      names_to = "indicator",
      values_to = "value"
    ) |>
    dplyr::mutate(
      district = stringr::str_to_title(.data$district),
      district = stringr::str_replace(.data$district, "_", " ")
    )

  grid_summaries <-
    download_parquet_from_cloud(
      prefix = paste0(pars$pds$pds_tracks$file_prefix, "-grid_summaries"),
      provider = pars$storage$google$key,
      options = pars$storage$google$options
    )

  # Dataframes to upload
  dataframes_to_upload <- list(
    monthly_summaries = monthly_summaries,
    taxa_summaries = taxa_summaries,
    districts_summaries = districts_summaries,
    gear_summaries = gear_summaries,
    grid_summaries = grid_summaries
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
#' @keywords export
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
    get_metadata()$sites |>
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
      mean_cpue = stats::median(.data$mean_cpue, na.rm = TRUE),
      mean_rpue = stats::median(.data$mean_rpue, na.rm = TRUE),
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
