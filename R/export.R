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
#'
#' @keywords workflow export
#' @export
export_wf_data <- function(log_threshold = logger::DEBUG) {
  logger::log_threshold(log_threshold)
  conf <- read_config()

  map <- cloud_object_name(
    prefix = conf$metadata$map_boundaries$prefix,
    provider = conf$storage$google$key,
    options = conf$storage$google$options_coasts,
    version = "latest",
    extension = "geojson"
  ) |>
    download_cloud_file(
      provider = conf$storage$google$key,
      options = conf$storage$google$options_coasts
    ) |>
    sf::st_read() |>
    dplyr::select(
      -c("map_code", "gaul0_code", "gaul0_name", "continent", "disp_en")
    )

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
    prefix <- conf$surveys$summaries$file_prefix %>%
      paste0("_", name)

    data_summaries[[name]] <- download_parquet_from_cloud(
      prefix = prefix,
      provider = conf$storage$google$key,
      options = conf$storage$google$options
    )
  }

  # Create geographic summaries
  region_monthly_summaries <-
    data_summaries$monthly_summaries |>
    dplyr::left_join(map, by = c("gaul_2_name" = "gaul2_name")) |>
    dplyr::group_by(.data$gaul_2_name, .data$date) |>
    dplyr::summarise(
      gaul_1_name = dplyr::first(.data$gaul1_name),
      mean_cpue = stats::median(.data$mean_cpue_day, na.rm = TRUE),
      mean_rpue = stats::median(.data$mean_rpue_day, na.rm = TRUE),
      mean_price_kg = stats::median(.data$mean_price_kg, na.rm = TRUE),
      .groups = "drop"
    ) |>
    dplyr::distinct() |>
    dplyr::mutate(
      date = format(.data$date, "%Y-%m-%dT%H:%M:%SZ"),
      country = conf$country
    ) |>
    dplyr::select(
      "country",
      gaul1_name = "gaul_1_name",
      "gaul_2_name",
      "date",
      "mean_cpue",
      "mean_rpue",
      "mean_price_kg"
    )

  upload_parquet_to_cloud(
    data = region_monthly_summaries,
    prefix = paste0(conf$country, "_monthly_summaries_map"),
    provider = conf$storage$google$key,
    options = conf$storage$google$options_coasts
  )

  logger::log_info("Downloading aggregated catch data from cloud storage...")
  aggregated_filename <- cloud_object_name(
    prefix = conf$surveys$aggregated$file_prefix,
    provider = conf$storage$google$key,
    extension = "rds",
    options = conf$storage$google$options
  )

  download_cloud_file(
    name = aggregated_filename,
    provider = conf$storage$google$key,
    options = conf$storage$google$options
  )

  aggregated_data <- readr::read_rds(aggregated_filename)

  monthly_aggregated <-
    aggregated_data$district_totals |>
    dplyr::mutate(estimated_catch_tn = .data$estimated_total_catch_kg / 1000) |>
    dplyr::select(
      "gaul_2_name",
      "date" = "date_month",
      "estimated_fishing_trips" = "estimated_total_trips",
      "estimated_catch_tn",
      "estimated_revenue_TZS" = "estimated_total_revenue"
    )

  # Transform monthly summaries to long format for portal
  monthly_summaries <-
    data_summaries$monthly_summaries |>
    dplyr::left_join(monthly_aggregated, by = c("gaul_2_name", "date")) |>
    dplyr::relocate(
      "estimated_fishing_trips",
      .after = "date"
    ) |>
    dplyr::select(-c("mean_cpue_day", "mean_rpue_day")) |> # Drop map-specific metrics
    tidyr::pivot_longer(
      -c("date", "gaul_2_name"),
      names_to = "metric",
      values_to = "value"
    )

  districts_summaries <-
    data_summaries$districts_summaries |>
    dplyr::full_join(monthly_aggregated, by = c("gaul_2_name", "date")) |>
    dplyr::select(dplyr::where(~ !all(is.na(.))), -"estimated_fishing_trips") |>
    tidyr::pivot_longer(
      -c("date", "gaul_2_name"),
      names_to = "indicator",
      values_to = "value"
    )

  # Dataframes to upload
  dataframes_to_upload <- list(
    monthly_summaries = monthly_summaries,
    taxa_summaries = data_summaries$taxa_summaries,
    districts_summaries = districts_summaries,
    gear_summaries = data_summaries$gear_summaries,
    grid_summaries = data_summaries$grid_summaries
  )

  # Collection names
  collection_names <- list(
    monthly_summaries = conf$storage$mongodb$databases$dashboard$collections$monthly_summaries,
    taxa_summaries = conf$storage$mongodb$databases$dashboard$collections$taxa_summaries,
    districts_summaries = conf$storage$mongodb$databases$dashboard$collections$districts_summaries,
    gear_summaries = conf$storage$mongodb$databases$dashboard$collections$gear_summaries,
    grid_summaries = conf$storage$mongodb$databases$dashboard$collections$grid_summaries
  )

  # Iterate over the dataframes and upload them
  purrr::walk2(
    .x = dataframes_to_upload,
    .y = collection_names,
    .f = ~ {
      logger::log_info(paste("Uploading", .y, "data to MongoDB"))
      mdb_collection_push(
        data = .x,
        connection_string = conf$storage$mongodb$connection_strings$main,
        collection_name = .y,
        db_name = conf$storage$mongodb$databases$dashboard$database_name
      )
    }
  )
}
