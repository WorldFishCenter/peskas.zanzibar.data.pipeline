#' Pre-process Zanzibar WCS Surveys
#'
#' Downloads and preprocesses raw structured WCS survey data from cloud storage into a binary format. The process includes nesting multiple columns related to species information into single columns within a dataframe, which helps reduce its width and organize data efficiently for analysis.
#'
#' Configurations are read from `conf.yml` with the following necessary parameters:
#'
#' ```
#' surveys:
#'   wcs_surveys:
#'     asset_id:
#'     username:
#'     password:
#'     file_prefix:
#'   version:
#'     preprocess:
#' storage:
#'   storage_name:
#'     key:
#'     options:
#'       project:
#'       bucket:
#'       service_account_key:
#' ```
#'
#' The function uses logging to track progress.
#'
#' @inheritParams ingest_surveys
#' @return None; the function is used for its side effects.
#' @export
#' @keywords workflow preprocessing
#' @seealso \code{\link[=pt_nest_trip]{pt_nest_trip}}, \code{\link[=pt_nest_catch]{pt_nest_catch}},
#' \code{\link[=pt_nest_length]{pt_nest_length}}, \code{\link[=pt_nest_market]{pt_nest_market}},
#' \code{\link[=pt_nest_attachments]{pt_nest_attachments}}
#'
preprocess_wcs_surveys <- function(log_threshold = logger::DEBUG) {
  logger::log_threshold(log_threshold)

  pars <- read_config()

  wcs_surveys_parquet <- cloud_object_name(
    prefix = pars$surveys$wcs_surveys$raw_surveys$file_prefix,
    provider = pars$storage$google$key,
    extension = "parquet",
    version = pars$surveys$wcs_surveys$version$preprocess,
    options = pars$storage$google$options
  )

  logger::log_info("Retrieving {wcs_surveys_parquet}")
  download_cloud_file(
    name = wcs_surveys_parquet,
    provider = pars$storage$google$key,
    options = pars$storage$google$options
  )

  catch_surveys_raw <-
    arrow::read_parquet(
      file = wcs_surveys_parquet
    ) |>
    # filter for real surveys
    dplyr::filter(.data$survey_real == "real")


  other_info <-
    catch_surveys_raw %>%
    tidyr::separate(.data$gps,
      into = c("lat", "lon", "drop1", "drop2"),
      sep = " "
    ) %>%
    dplyr::select(
      "survey_id" = "_id",
      submission_date = "today",
      "survey_type",
      "landing_site",
      "lat",
      "lon",
      "trip_info",
      n_fishers = "people",
      "boats_landed"
    ) |>
    dplyr::mutate(
      submission_date = lubridate::with_tz(.data$submission_date, "Africa/Dar_es_Salaam"),
      submission_date = as.Date(.data$submission_date),
      dplyr::across(.cols = c(
        "lat",
        "lon",
        "n_fishers",
        "boats_landed"
      ), ~ as.numeric(.x))
    )


  logger::log_info("Nesting survey groups' fields")
  group_surveys <-
    list(
      survey_trip = pt_nest_trip(catch_surveys_raw),
      other_info = other_info,
      survey_catch = pt_nest_catch(catch_surveys_raw),
      # survey_length = pt_nest_length(catch_surveys_raw),
      survey_market = pt_nest_market(catch_surveys_raw),
      survey_attachments = pt_nest_attachments(catch_surveys_raw)
    )

  wcs_surveys_nested <- purrr::reduce(
    group_surveys,
    ~ dplyr::full_join(.x, .y, by = "survey_id")
  )

  unique(group_surveys$survey_trip$gear) |>
    sort()

  preprocessed_filename <- pars$surveys$wcs_surveys$preprocessed_surveys$file_prefix %>%
    add_version(extension = "parquet")

  arrow::write_parquet(
    x = wcs_surveys_nested,
    sink = preprocessed_filename,
    compression = "lz4",
    compression_level = 12
  )

  logger::log_info("Uploading {preprocessed_filename} to cloud storage")
  upload_cloud_file(
    file = preprocessed_filename,
    provider = pars$storage$google$key,
    options = pars$storage$google$options
  )
}

#' Pre-process WorldFish Surveys
#'
#' Downloads and preprocesses raw structured WorldFish survey data from cloud storage into a binary format.
#' The process includes organizing data from general and trip information into a structured format
#' optimized for analysis.
#'
#' Configurations are read from `conf.yml` with the following necessary parameters:
#'
#' ```
#' surveys:
#'   wf_surveys:
#'     asset_id:
#'     username:
#'     password:
#'     file_prefix:
#'   version:
#'     preprocess:
#' storage:
#'   storage_name:
#'     key:
#'     options:
#'       project:
#'       bucket:
#'       service_account_key:
#' ```
#'
#' The function uses logging to track progress.
#'
#' @inheritParams ingest_surveys
#' @return None; the function is used for its side effects.
#' @export
#' @keywords workflow preprocessing
#'
preprocess_wf_surveys <- function(log_threshold = logger::DEBUG) {
  logger::log_threshold(log_threshold)

  pars <- read_config()
  metadata <- get_metadata()

  wf_surveys_parquet <- cloud_object_name(
    prefix = pars$surveys$wf_surveys$raw_surveys$file_prefix,
    provider = pars$storage$google$key,
    extension = "parquet",
    version = pars$surveys$wf_surveys$version$preprocess,
    options = pars$storage$google$options
  )

  logger::log_info("Retrieving {wf_surveys_parquet}")
  download_cloud_file(
    name = wf_surveys_parquet,
    provider = pars$storage$google$key,
    options = pars$storage$google$options
  )

  catch_surveys_raw <- arrow::read_parquet(
    file = wf_surveys_parquet
  )

  general_info <-
    catch_surveys_raw %>%
    dplyr::rename_with(~ stringr::str_remove(., "group_general/")) %>%
    dplyr::select(
      "submission_id" = "_id",
      "landing_date",
      "landing_site",
      "survey_activity",
      "survey_activity_whynot",
      "catch_outcome",
      submission_date = "today"
    ) %>%
    # dplyr::mutate(landing_code = dplyr::coalesce(.data$landing_site_palma, .data$landing_site_mocimboa)) %>%
    # tidyr::separate(.data$gps,
    #  into = c("lat", "lon", "drop1", "drop2"),
    #  sep = " "
    # ) %>%
    # dplyr::relocate("landing_code", .after = "district") %>%
    dplyr::mutate(
      landing_date = lubridate::as_datetime(.data$landing_date),
      submission_date = lubridate::as_datetime(.data$submission_date)
    ) %>%
    # dplyr::left_join(metadata$landing_site, by = c("district", "landing_code")) %>%
    dplyr::relocate("submission_date", .after = "landing_date")

  trip_info <-
    catch_surveys_raw %>%
    dplyr::rename_with(~ stringr::str_remove(., "group_trip/")) %>%
    dplyr::select(
      "submission_id" = "_id",
      "has_boat",
      # "boat_reg_no",
      "has_PDS",
      tracker_imei = "PDS_IMEI",
      "vessel_type",
      "propulsion_gear",
      "trip_duration",
      "habitat",
      male_fishers = "no_fishers/no_men_fishers",
      female_fishers = "no_fishers/no_women_fishers",
      child_fishers = "no_fishers/no_child_fishers",
      "gear",
      "mesh_size",
      "hook_size"
    ) %>%
    dplyr::mutate(dplyr::across(dplyr::ends_with("fishers"), ~ stringr::str_remove(.x, "_")), # Remove the suffix but need to be fixed
      dplyr::across(c("trip_duration", "mesh_size", "hook_size", dplyr::ends_with("fishers")), as.numeric),
      tot_fishers = rowSums(dplyr::across(dplyr::ends_with("fishers")), na.rm = TRUE)
    ) %>%
    dplyr::relocate("habitat", .after = "tracker_imei") %>%
    dplyr::relocate("vessel_type", .after = "habitat") %>%
    dplyr::relocate("gear", .after = "vessel_type")



  preprocessed_filename <- pars$surveys$wf_surveys$preprocessed_surveys$file_prefix %>%
    add_version(extension = "parquet")

  # arrow::write_parquet(
  #  x = wf_surveys_nested,
  #  sink = preprocessed_filename,
  #  compression = "lz4",
  #  compression_level = 12
  # )

  # logger::log_info("Uploading {preprocessed_filename} to cloud storage")
  # upload_cloud_file(
  #  file = preprocessed_filename,
  #  provider = pars$storage$google$key,
  #  options = pars$storage$google$options
  # )
}

#' Pre-process Blue Alliance Surveys
#'
#' Downloads and preprocesses raw structured Blue Alliance survey data from cloud storage into a binary format.
#' The process includes date standardization and survey ID generation for unique trip identification.
#'
#' Configurations are read from `conf.yml` with the following necessary parameters:
#'
#' ```
#' surveys:
#'   ba_surveys:
#'     asset_id:
#'     username:
#'     password:
#'     file_prefix:
#'   version:
#'     preprocess:
#' storage:
#'   storage_name:
#'     key:
#'     options:
#'       project:
#'       bucket:
#'       service_account_key:
#' ```
#'
#' The function uses logging to track progress and creates unique survey IDs using CRC32 hashing
#' of concatenated trip attributes.
#'
#' @inheritParams ingest_surveys
#' @return None; the function is used for its side effects.
#' @export
#' @keywords workflow preprocessing
#'
preprocess_ba_surveys <- function(log_threshold = logger::DEBUG) {
  logger::log_threshold(log_threshold)

  pars <- read_config()

  ba_surveys_csv <- cloud_object_name(
    prefix = pars$surveys$ba_surveys$raw_surveys$file_prefix,
    provider = pars$storage$google$key,
    extension = "csv",
    version = pars$surveys$ba_surveys$version$preprocess,
    options = pars$storage$google$options
  )

  logger::log_info("Retrieving {ba_surveys_csv}")
  download_cloud_file(
    name = ba_surveys_csv,
    provider = pars$storage$google$key,
    options = pars$storage$google$options
  )

  catch_surveys_raw <-
    readr::read_csv2(ba_surveys_csv, show_col_types = FALSE) |>
    dplyr::mutate(date = paste(.data$date, .data$year)) %>% # Combine date and year
    dplyr::mutate(date = lubridate::dmy(.data$date)) %>% # Convert to Date format
    dplyr::rename(
      landing_date = "date",
      catch_taxon = "Taxon"
    ) |>
    dplyr::select(-c("month", "year")) |>
    dplyr::rowwise() %>%
    dplyr::mutate(survey_id = digest::digest(
      paste(.data$landing_date, .data$fisher_id, .data$landing_site, .data$n_fishers, .data$gear, .data$trip_duration, .data$n_fishers,
        sep = "_"
      ),
      algo = "crc32"
    )) |>
    dplyr::ungroup() |>
    dplyr::select("survey_id", dplyr::everything()) |>
    dplyr::mutate(dplyr::across(dplyr::where(is.character), ~ tolower(.x))) |>
    # fix taxa names
    dplyr::mutate(catch_taxon = dplyr::case_when(
      .data$catch_taxon == "cheilunus trilobatus" ~ "cheilinus trilobatus",
      .data$catch_taxon == "pufflamen crysopterum" ~ "sufflamen chrysopterum",
      .data$catch_taxon == "sufflamen crysopterum" ~ "sufflamen chrysopterum",
      .data$catch_taxon == "perupeneus fraserorum" ~ "parupeneus fraserorum",
      .data$catch_taxon == "perupeneus macronemus" ~ "parupeneus macronemus",
      .data$catch_taxon == "octopus mucronatus" ~ "octopus",
      .data$catch_taxon == "parupeneus signutus" ~ "parupeneus",
      .data$catch_taxon == "perupeneus" ~ "parupeneus",
      .data$catch_taxon == "acanhocybium solandri" ~ "acanthocybium solandri",
      .data$catch_taxon == "parupeneus signutu" ~ "parupeneus",
      .data$catch_taxon == "gymnothorax crobroris" ~ "gymnothorax cribroris",
      .data$catch_taxon == "perupeneus signatus" ~ "parupeneus signatus",
      .data$catch_taxon == "gymonthorax favagineus" ~ "gymnothorax favagineus",
      .data$catch_taxon == "colotomus carolinus" ~ "calotomus carolinus",
      .data$catch_taxon == "pegullus naatalensis" ~ "pagellus natalensis",
      .data$catch_taxon == "scomberomorus plurineatus" ~ "scomberomorus plurilineatus",
      .data$catch_taxon == "lethrinus fulvus" ~ "lethrinus",
      .data$catch_taxon == "neotrygon caeruieopunctata" ~ "neotrygon caeruleopunctata",
      .data$catch_taxon == "myuripristis berndti" ~ "myripristis berndti",
      TRUE ~ .data$catch_taxon
    ))

  preprocessed_filename <- pars$surveys$ba_surveys$preprocessed_surveys$file_prefix %>%
    add_version(extension = "parquet")

  arrow::write_parquet(
    x = catch_surveys_raw,
    sink = preprocessed_filename,
    compression = "lz4",
    compression_level = 12
  )

  logger::log_info("Uploading {preprocessed_filename} to cloud storage")
  upload_cloud_file(
    file = preprocessed_filename,
    provider = pars$storage$google$key,
    options = pars$storage$google$options
  )
}


#' Nest Length Group Columns
#'
#' Nests length group columns obtained from the structured data of WCS landings surveys.
#' This reduces the width of data by converting multiple related columns into a single nested column.
#'
#' @param x Data frame of WCS survey data in tabular format.
#' @return A data frame with length data nested into a single 'length' column,
#'         which contains a tibble for each row with multiple measurements.
#' @keywords internal
#'
pt_nest_length <- function(x) {
  x %>%
    dplyr::select(
      survey_id = .data$`_id`,
      dplyr::starts_with("Length_Frequency_Survey")
    ) %>%
    tidyr::pivot_longer(-c(.data$survey_id)) %>%
    dplyr::mutate(
      n = stringr::str_extract(.data$name, "(\\d+)"),
      name = stringr::str_remove(.data$name, "(\\d+)"),
      name = stringr::str_remove(.data$name, pattern = "Length_Frequency_Survey/catch_length..Length_Frequency_Survey/catch_length/")
    ) %>%
    tidyr::pivot_wider(names_from = .data$name, values_from = .data$value) %>%
    dplyr::mutate(content = dplyr::coalesce(!!!.[3:ncol(.)])) %>%
    dplyr::filter(.data$n == 0 | !is.na(.data$content)) %>%
    dplyr::select(-"content") |>
    dplyr::mutate(total_length = as.numeric(.data$total_length)) |>
    tidyr::nest(
      "length" = c(
        .data$family, .data$species,
        .data$sex, .data$total_length
      ),
      .by = .data$survey_id
    )
}

#' Nest Market Group Columns
#'
#' Nests market group columns from structured WCS landings survey data. This method organizes
#' multiple related market data points into a single nested 'market' column per row.
#'
#' @param x Data frame of WCS survey data in tabular format.
#' @return A data frame with market data nested into a 'market' column, containing a tibble
#'         for each row with various market-related attributes.
#' @keywords internal
#' @export
#'
pt_nest_market <- function(x) {
  x %>%
    dplyr::select(
      survey_id = .data$`_id`,
      dplyr::starts_with("Market_Catch_Survey")
    ) %>%
    tidyr::pivot_longer(-c("survey_id")) %>%
    dplyr::mutate(
      n = stringr::str_extract(.data$name, "(\\d+)"),
      name = stringr::str_remove(.data$name, "(\\d+)"),
      name = stringr::str_remove(.data$name, pattern = "Market_Catch_Survey/catch_market..Market_Catch_Survey/catch_market/")
    ) %>%
    tidyr::pivot_wider(names_from = .data$name, values_from = .data$value) %>%
    dplyr::mutate(content = dplyr::coalesce(!!!.[3:ncol(.)])) %>%
    dplyr::filter(.data$n == 0 | !is.na(.data$content)) %>%
    dplyr::select(-.data$content) %>%
    dplyr::rename(
      catch_price = .data$price_sold_for,
      catch_kg_market = .data$weight_market
    ) |>
    dplyr::mutate(
      catch_kg_market = as.numeric(.data$catch_kg_market),
      catch_price = as.numeric(.data$catch_price)
    ) |>
    tidyr::nest(
      "market" = c(
        .data$group_market, .data$species_market,
        .data$catch_kg_market, .data$catch_price
      ),
      .by = .data$survey_id
    )
}

#' Nest Catch Group Columns
#'
#' Nests catch group columns from WCS structured survey data to organize multiple
#' related catch data points into a single nested 'catch' column per row.
#'
#' @param x Data frame of WCS survey data in tabular format.
#' @return A data frame with catch data nested into a 'catch' column, containing a tibble
#'         for each row with various catch-related attributes.
#' @keywords internal
#' @export
#'
pt_nest_catch <- function(x) {
  x %>%
    dplyr::select(
      survey_id = .data$`_id`,
      dplyr::starts_with("Total_Catch_Survey")
    ) %>%
    tidyr::pivot_longer(-c(.data$survey_id)) %>%
    dplyr::mutate(
      n = stringr::str_extract(.data$name, "(\\d+)"),
      name = stringr::str_remove(.data$name, "(\\d+)"),
      name = stringr::str_remove(.data$name, pattern = "Total_Catch_Survey/catch_catch..Total_Catch_Survey/catch_catch/")
    ) %>%
    tidyr::pivot_wider(names_from = .data$name, values_from = .data$value) %>%
    dplyr::mutate(content = dplyr::coalesce(!!!.[3:ncol(.)])) %>%
    dplyr::filter(.data$n == 0 | !is.na(.data$content)) %>%
    dplyr::mutate(type_measure = dplyr::case_when(
      !is.na(.data$nb_ind_catch) ~ "individual",
      !is.na(.data$nb_buckets_catch) ~ "bucket",
      TRUE ~ .data$type_measure
    )) |>
    dplyr::mutate(
      weight_kg = dplyr::coalesce(.data$weight_catch, .data$wgt_ind_catch, .data$wgt_buckets_catch),
      nb_elements = dplyr::coalesce(.data$nb_ind_catch, .data$nb_buckets_catch)
    ) %>%
    dplyr::select(-c(
      .data$content, .data$weight_catch, .data$wgt_ind_catch, .data$wgt_buckets_catch,
      .data$nb_ind_catch, .data$nb_buckets_catch
    )) %>%
    dplyr::rename(
      n_elements = .data$nb_elements,
      length_individual = .data$len_ind_catch,
      all_catch_in_boat = .data$All_catch_in_boat,
      catch_kg = .data$weight_kg
    ) |>
    dplyr::mutate(
      all_catch_in_boat = tolower(.data$all_catch_in_boat),
      all_catch_in_boat = cleaner::clean_character(.data$all_catch_in_boat),
      length_individual = as.numeric(.data$length_individual),
      catch_kg = as.numeric(.data$catch_kg),
      n_elements = as.numeric(.data$n_elements)
    ) |>
    dplyr::mutate(
      group_catch = dplyr::case_when(
        .data$group_catch %in% c("reef_fish", "goatfish", "parrotfish", "rabbitfish", "scavengers") ~ "reef_fish",
        .data$group_catch %in% c("sharks_rays") ~ "sharks_rays",
        .data$group_catch %in% c("small_pelagic") ~ "small_pelagic",
        .data$group_catch %in% c("large_pelagic", "pelagic") ~ "large_pelagic",
        .data$group_catch %in% c("tuna_like", "tunalike") ~ "tuna_like",
        .data$group_catch %in% c("molluscs_crustaceans", "octopus", "squid") ~ "molluscs_crustaceans",
        TRUE ~ .data$group_catch
      )
    ) |>
    tidyr::nest(
      "catch" = c(
        .data$type_measure, .data$all_catch_in_boat, .data$group_catch,
        .data$species_catch, .data$n_elements, .data$catch_kg
      ),
      .by = .data$survey_id
    )
}
#' Nest Trip Group Columns
#'
#' Processes and nests trip-related columns from structured WCS landings survey data into a single 'trip' column. This approach consolidates trip information into nested tibbles within the dataframe, simplifying the structure for analysis.
#'
#' @param x A data frame containing structured survey data in tabular format.
#' @return A data frame with trip data nested into a single 'trip' column containing a tibble for each row, corresponding to the various trip details.
#' @keywords internal
#' @export
#'
pt_nest_trip <- function(x) {
  x %>%
    dplyr::select(
      survey_id = .data$`_id`,
      dplyr::starts_with("Fishing_Trip")
    ) %>%
    tidyr::pivot_longer(-c(.data$survey_id)) %>%
    dplyr::mutate(
      n = stringr::str_extract(.data$name, "(\\d+)"),
      name = stringr::str_remove(.data$name, "(\\d+)"),
      name = stringr::str_remove(.data$name, pattern = "Fishing_Trip/")
    ) %>%
    tidyr::pivot_wider(names_from = .data$name, values_from = .data$value) %>%
    dplyr::mutate(content = dplyr::coalesce(!!!.[3:ncol(.)])) %>%
    # dplyr::filter(!is.na(.data$content)) %>%
    dplyr::select(-"content", -"n") |>
    dplyr::rename(
      fishing_ground_name = .data$fishing_ground_name,
      habitat = .data$fishing_ground_type,
      trip_length_days = .data$fishing_duration,
      vessel_type = .data$boat_type,
      vessel_type_other = .data$other_boat,
      boat_engine = .data$engine_yn,
      engine_hp = .data$engine,
      gear = .data$gear_type,
      gear_other = .data$gear_type_other,
    ) |>
    dplyr::mutate(
      gear = dplyr::case_when(.data$gear == "spear" ~ "spear_gun", TRUE ~ .data$gear),
      boat_engine = ifelse(.data$boat_engine == "yes", "motorised", "unmotorised"),
      fishing_location = tolower(.data$fishing_location),
      fishing_location = cleaner::clean_character(.data$fishing_location),
      gear_other = tolower(.data$gear_other),
      gear_other = cleaner::clean_character(.data$gear_other),
      fishing_ground_name = tolower(.data$fishing_ground_name),
      fishing_ground_name = cleaner::clean_character(.data$fishing_ground_name),
      engine_hp = as.numeric(.data$engine_hp),
      trip_length_days = dplyr::case_when(
        trip_length_days == ">3" ~ 4,
        TRUE ~ as.numeric(.data$trip_length_days)
      )
    )
}

#' Nest Attachment Columns
#'
#' Nests attachment-related columns from structured WCS survey data, organizing multiple attachment entries into a single nested column. This function addresses the challenge of handling wide data tables by converting them into more manageable nested data frames.
#'
#' @param x A data frame containing raw survey data, potentially with multiple attachments per survey entry.
#' @return A data frame with attachment information nested into a single '_attachments' column, containing a tibble for each row.
#' @keywords internal
#' @export
#'
#' @examples
#' \dontrun{
#' dummy_landings <- tidyr::tibble(
#'   `_id` = "123",
#'   `_attachments.0.download_url` = "http://url-1.com",
#'   `_attachments.0.id` = "01",
#'   `_attachments.1.download_url` = "http://url-2.com",
#'   `_attachments.1.id` = "02",
#'   `_attachments.2.download_url` = NA,
#'   `_attachments.2.id` = NA
#' )
#' pt_nest_attachments(dummy_landings)
#' }
#'
pt_nest_attachments <- function(x) {
  x %>%
    # Using the .data pronoun to avoid RMD check notes
    dplyr::select(
      survey_id = .data$`_id`,
      dplyr::starts_with("_attachments")
    ) %>%
    dplyr::mutate_all(as.character) %>%
    # Column names follow the form "_attachments.0.download_large_url"
    tidyr::pivot_longer(
      cols = -.data$survey_id,
      names_to = c("n", "field"),
      names_prefix = "_attachments.",
      names_sep = "\\."
    ) %>%
    # We want one attachment per row and fields as columns
    tidyr::pivot_wider(names_from = "field", values_from = "value") %>%
    # Attachments already have id and this column is superfluous
    dplyr::select(-.data$n) %>%
    dplyr::group_by(.data$survey_id) %>%
    tidyr::nest() %>%
    dplyr::ungroup() %>%
    dplyr::rename("_attachments" = "data") %>%
    # If there are no attachments empty the nested data frames
    dplyr::mutate(
      survey_id = as.integer(.data$survey_id),
      `_attachments` = purrr::map(
        .data$`_attachments`,
        ~ dplyr::filter(., !is.na(.data$id))
      )
    )
}


#' Expand Taxonomic Vectors into a Data Frame
#'
#' Converts a vector of species identifiers into a detailed data frame containing taxonomic classification. Each identifier should follow the format 'family_genus_species', which is expanded to include comprehensive taxonomic details.
#'
#' @param data A vector of species identifiers formatted as 'family_genus_species'. If not provided, the function will return an error.
#' @return A data frame where each row corresponds to a species, enriched with taxonomic classification information including family, genus, species, and additional taxonomic ranks.
#' @keywords data transformation
#' @export
#' @examples
#' \dontrun{
#' species_vector <- c("lutjanidae_lutjanus_spp", "scaridae_spp", "acanthuridae_naso_hexacanthus")
#' expanded_data <- expand_taxa(species_vector)
#' }
#' @details This function splits each species identifier into its constituent parts, replaces underscores with spaces for readability, and retrieves taxonomic classification from the GBIF database using the `taxize` package.
#' @note Requires internet access to fetch data from the GBIF database. The accuracy of results depends on the correct formatting of input data and the availability of taxonomic data in the GBIF database.
#'
expand_taxa <- function(data = NULL) {
  taxa_expanded <-
    data %>%
    dplyr::mutate(species_list = stringr::str_split(.data$species_catch, pattern = " ")) %>%
    tidyr::unnest(.data$species_list) %>%
    dplyr::mutate(
      species_list = stringr::str_replace(.data$species_list, pattern = "_", replacement = " "),
      species_list = stringr::str_replace(.data$species_list, pattern = "_", replacement = " "),
      words = stringi::stri_count_words(.data$species_list),
      genus_species = dplyr::case_when(
        .data$words == 3 ~ stringr::str_extract(.data$species_list, "\\S+\\s+\\S+$"),
        TRUE ~ NA_character_
      ),
      species_list = ifelse(.data$words == 3, NA_character_, .data$species_list),
      catch_group = dplyr::coalesce(.data$species_list, .data$genus_species),
      catch_group = stringr::str_replace(.data$catch_group, pattern = " spp.", replacement = ""),
      catch_group = stringr::str_replace(.data$catch_group, pattern = " spp", replacement = ""),
      catch_group = stringr::str_replace(.data$catch_group, pattern = "_spp", replacement = ""),
      catch_group = ifelse(.data$catch_group == "acanthocybium solandiri", "acanthocybium solandri", .data$catch_group),
      catch_group = ifelse(.data$catch_group == "panaeidae", "penaeidae", .data$catch_group),
      catch_group = ifelse(.data$catch_group == "mulidae", "mullidae", .data$catch_group),
      catch_group = ifelse(.data$catch_group == "casio xanthonotus", "caesio xanthonotus", .data$catch_group),
    ) %>%
    dplyr::select(-c(.data$species_list, .data$genus_species, .data$words))

  groups_rank <-
    taxize::classification(unique(taxa_expanded$catch_group), db = "gbif", rows = 1) %>%
    purrr::imap(~ .x %>%
      dplyr::as_tibble() %>%
      dplyr::mutate(catch_group = .y)) %>%
    dplyr::bind_rows() %>%
    tidyr::pivot_wider(id_cols = .data$catch_group, names_from = .data$rank, values_from = .data$name) %>%
    dplyr::select(dplyr::everything(), -dplyr::any_of(c("class", "NA")))

  dplyr::left_join(taxa_expanded, groups_rank, by = "catch_group") |>
    dplyr::select(-c("kingdom", "phylum", "order"))
}

#' Preprocess Pelagic Data Systems (PDS) Track Data
#'
#' @description
#' Downloads raw GPS tracks and creates a gridded summary of fishing activity.
#'
#' @param log_threshold The logging threshold to use. Default is logger::DEBUG.
#' @param grid_size Numeric. Size of grid cells in meters (100, 250, 500, or 1000).
#'
#' @return None (invisible). Creates and uploads preprocessed files.
#'
#' @keywords workflow preprocessing
#' @export
preprocess_pds_tracks <- function(log_threshold = logger::DEBUG, grid_size = 500) {
  logger::log_threshold(log_threshold)
  pars <- read_config()

  # Get already preprocessed tracks
  logger::log_info("Checking existing preprocessed tracks...")
  preprocessed_filename <- cloud_object_name(
    prefix = paste0(pars$pds$pds_tracks$file_prefix, "-preprocessed"),
    provider = pars$storage$google$key,
    extension = "parquet",
    version = pars$pds$pds_tracks$version,
    options = pars$storage$google$options
  )

  # Get preprocessed trip IDs if file exists
  preprocessed_trips <- tryCatch(
    {
      download_cloud_file(
        name = preprocessed_filename,
        provider = pars$storage$google$key,
        options = pars$storage$google$options
      )
      preprocessed_data <- arrow::read_parquet(preprocessed_filename)
      unique(preprocessed_data$Trip)
    },
    error = function(e) {
      logger::log_info("No existing preprocessed tracks file found")
      character(0)
    }
  )

  # List raw tracks
  logger::log_info("Listing raw tracks...")
  raw_tracks <- googleCloudStorageR::gcs_list_objects(
    bucket = pars$pds_storage$google$options$bucket,
    prefix = pars$pds$pds_tracks$file_prefix
  )$name

  raw_trip_ids <- extract_trip_ids_from_filenames(raw_tracks)
  new_trip_ids <- setdiff(raw_trip_ids, preprocessed_trips)

  if (length(new_trip_ids) == 0) {
    logger::log_info("No new tracks to preprocess")
    return(invisible())
  }

  # Get raw tracks that need preprocessing
  new_tracks <- raw_tracks[raw_trip_ids %in% new_trip_ids]

  workers <- parallel::detectCores() - 1
  logger::log_info("Setting up parallel processing with {workers} workers...")
  future::plan(future::multisession, workers = workers)

  logger::log_info("Processing {length(new_tracks)} tracks in parallel...")
  new_processed_data <- furrr::future_map_dfr(
    new_tracks,
    function(track_file) {
      download_cloud_file(
        name = track_file,
        provider = pars$pds_storage$google$key,
        options = pars$pds_storage$google$options
      )

      track_data <- arrow::read_parquet(track_file) %>%
        preprocess_track_data(grid_size = grid_size)

      unlink(track_file)
      track_data
    },
    .options = furrr::furrr_options(seed = TRUE),
    .progress = TRUE
  )

  future::plan(future::sequential)

  # Combine with existing preprocessed data if it exists
  final_data <- if (length(preprocessed_trips) > 0) {
    dplyr::bind_rows(preprocessed_data, new_processed_data)
  } else {
    new_processed_data
  }

  output_filename <-
    paste0(pars$pds$pds_tracks$file_prefix, "-preprocessed") |>
    add_version(extension = "parquet")

  arrow::write_parquet(
    final_data,
    sink = output_filename,
    compression = "lz4",
    compression_level = 12
  )

  logger::log_info("Uploading preprocessed tracks...")
  upload_cloud_file(
    file = output_filename,
    provider = pars$storage$google$key,
    options = pars$storage$google$options
  )

  unlink(output_filename)
  if (exists("preprocessed_filename")) unlink(preprocessed_filename)

  logger::log_success("Track preprocessing complete")
}







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
  grid_degrees <- switch(as.character(grid_size),
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
        as.numeric(difftime(.data$Time, dplyr::lag(.data$Time), units = "mins")),
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
    dplyr::filter(!(dplyr::row_number() %in% c(1, 2, dplyr::n() - 1, dplyr::n()))) %>%
    dplyr::ungroup()
}
