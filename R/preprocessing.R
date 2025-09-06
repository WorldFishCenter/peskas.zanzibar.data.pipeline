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

  catch_surveys_raw <-
    download_parquet_from_cloud(
      prefix = pars$surveys$wcs_surveys$raw_surveys$file_prefix,
      provider = pars$storage$google$key,
      options = pars$storage$google$options
    ) |>
    dplyr::filter(.data$survey_real == "real")

  other_info <-
    catch_surveys_raw %>%
    tidyr::separate(
      .data$gps,
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
      submission_date = lubridate::with_tz(
        .data$submission_date,
        "Africa/Dar_es_Salaam"
      ),
      submission_date = as.Date(.data$submission_date),
      dplyr::across(
        .cols = c(
          "lat",
          "lon",
          "n_fishers",
          "boats_landed"
        ),
        ~ as.numeric(.x)
      )
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

  upload_parquet_to_cloud(
    data = wcs_surveys_nested,
    prefix = pars$surveys$wcs_surveys$preprocessed_surveys$file_prefix,
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
  asfis <- download_parquet_from_cloud(
    prefix = "asfis",
    provider = pars$storage$google$key,
    options = pars$storage$google$options
  )

  # metadata <- get_metadata()

  catch_surveys_raw <-
    download_parquet_from_cloud(
      prefix = pars$surveys$wf_surveys$raw_surveys$file_prefix,
      provider = pars$storage$google$key,
      options = pars$storage$google$options
    ) |>
    dplyr::select(-dplyr::starts_with("_att")) |>
    dplyr::rename(submission_id = "_id")

  general_info <-
    catch_surveys_raw %>%
    dplyr::rename_with(~ stringr::str_remove(., "group_general/")) %>%
    dplyr::rename_with(~ stringr::str_remove(., "group_trip/")) %>%
    dplyr::rename_with(~ stringr::str_remove(., "no_fishers/")) %>%
    dplyr::rename_with(
      ~ stringr::str_remove(., "group_conservation_trading/")
    ) %>%
    dplyr::select(
      "submission_id",
      submitted_by = "_submitted_by",
      submission_date = "today",
      "landing_date",
      district = "District",
      dplyr::contains("landing_site"),
      "collect_data_today",
      "survey_activity",
      "has_boat",
      "vessel_type",
      "propulsion_gear",
      "fuel_L",
      "has_PDS",
      "habitat",
      "fishing_ground",
      "gear",
      "mesh_size",
      "trip_duration",
      dplyr::ends_with("_fishers"),
      "catch_outcome",
      "conservation",
      "catch_use",
      "trader",
      "catch_price",
      "happiness_rating"
    ) %>%
    dplyr::mutate(
      landing_site = dplyr::coalesce(
        !!!dplyr::select(., dplyr::contains("landing_site"))
      )
    ) |>
    dplyr::select(-dplyr::contains("landing_site"), "landing_site") |>
    dplyr::relocate("landing_site", .after = "district") |>
    # dplyr::mutate(landing_code = dplyr::coalesce(.data$landing_site_palma, .data$landing_site_mocimboa)) %>%
    # tidyr::separate(.data$gps,
    #  into = c("lat", "lon", "drop1", "drop2"),
    #  sep = " "
    # ) %>%
    # dplyr::relocate("landing_code", .after = "district") %>%
    dplyr::mutate(
      landing_date = lubridate::as_date(.data$landing_date),
      submission_date = lubridate::as_date(.data$submission_date),
      dplyr::across(
        c("trip_duration", "catch_price", dplyr::ends_with("_fishers")),
        ~ as.double(.x)
      )
    )

  catch_info <-
    catch_surveys_raw |>
    dplyr::select("submission_id", dplyr::starts_with("species_group")) |>
    reshape_catch_data() |>
    dplyr::mutate(
      count_method = dplyr::coalesce(
        .data$counting_method,
        .data$COUNTING_METHOD_USED_TO_RECORD
      ),
      count_method = dplyr::case_when(
        !is.na(.data$count) ~ "1",
        !is.na(.data$n_buckets) ~ "2",
        TRUE ~ .data$count_method
      )
    ) |>
    dplyr::select(-c("counting_method", "COUNTING_METHOD_USED_TO_RECORD")) |>
    dplyr::mutate(
      length_range = dplyr::case_when(
        .data$length_range == "over100" ~ NA_character_,
        TRUE ~ .data$length_range
      )
    ) |>
    tidyr::separate_wider_delim(
      "length_range",
      delim = "_",
      names = c("min", "max")
    ) |>
    dplyr::mutate(
      length = as.numeric(.data$min) +
        ((as.numeric(.data$max) - as.numeric(.data$min)) / 2),
      length = dplyr::coalesce(.data$length, as.numeric(.data$length_over))
    ) |>
    dplyr::select(
      "submission_id",
      "n_catch",
      "count_method",
      catch_taxon = "species",
      "n_buckets",
      "weight_bucket",
      individuals = "count",
      "length"
    ) |>
    # fix fields
    dplyr::mutate(
      dplyr::across(c("n_buckets":"length"), ~ as.double(.x))
    ) |>
    # replace TUN with TUS and SKH to Carcharhiniformes as more pertinent
    dplyr::mutate(
      catch_taxon = dplyr::case_when(
        .data$catch_taxon == "TUN" ~ "TUS",
        .data$catch_taxon == "SKH" ~ "CVX",
        TRUE ~ .data$catch_taxon
      )
    )

  # Try to get length-weight coefficients from Rfishbase
  lwcoeffs <- tryCatch(
    {
      getLWCoeffs(
        taxa_list = unique(catch_info$catch_taxon),
        asfis_list = asfis
      )
    },
    error = function(e) {
      message("Error in getLWCoeffs, using local fallback: ", e$message)
      # Fallback to local data
      readr::read_rds(system.file(
        "length_weight_params.rds",
        package = "peskas.zanzibar.data.pipeline"
      ))
    }
  )

  # add flyng fish estimates
  fly_lwcoeffs <- dplyr::tibble(
    catch_taxon = "FLY",
    n = 0,
    a_6 = 0.00631,
    b_6 = 3.05
  )
  lwcoeffs$lw <- dplyr::bind_rows(lwcoeffs$lw, fly_lwcoeffs)

  catch_df <-
    calculate_catch(catch_data = catch_info, lwcoeffs = lwcoeffs$lw) |>
    dplyr::left_join(lwcoeffs$ml, by = "catch_taxon") |>
    dplyr::select(-"max_weightkg_75")

  preprocessed_data <-
    dplyr::left_join(general_info, catch_df, by = "submission_id") |>
    dplyr::arrange(.data$submission_id, .data$n_catch)

  upload_parquet_to_cloud(
    data = preprocessed_data,
    prefix = pars$surveys$wf_surveys$preprocessed_surveys$file_prefix,
    provider = pars$storage$google$key,
    options = pars$storage$google$options
  )
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

  catch_surveys_preprocessed <-
    readr::read_csv2(ba_surveys_csv, show_col_types = FALSE) |>
    dplyr::mutate(date = paste(.data$date, .data$year)) %>% # Combine date and year
    dplyr::mutate(date = lubridate::dmy(.data$date)) %>% # Convert to Date format
    dplyr::rename(
      landing_date = "date",
      catch_taxon = "Taxon"
    ) |>
    dplyr::select(-c("month", "year")) |>
    dplyr::rowwise() %>%
    dplyr::mutate(
      survey_id = digest::digest(
        paste(
          .data$landing_date,
          .data$fisher_id,
          .data$landing_site,
          .data$n_fishers,
          .data$gear,
          .data$trip_duration,
          .data$n_fishers,
          sep = "_"
        ),
        algo = "crc32"
      )
    ) |>
    dplyr::ungroup() |>
    dplyr::select("survey_id", dplyr::everything()) |>
    dplyr::mutate(dplyr::across(dplyr::where(is.character), ~ tolower(.x))) |>
    # fix taxa names
    dplyr::mutate(
      catch_taxon = dplyr::case_when(
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
        .data$catch_taxon == "gymonthorax favagineus" ~
          "gymnothorax favagineus",
        .data$catch_taxon == "colotomus carolinus" ~ "calotomus carolinus",
        .data$catch_taxon == "pegullus naatalensis" ~ "pagellus natalensis",
        .data$catch_taxon == "scomberomorus plurineatus" ~
          "scomberomorus plurilineatus",
        .data$catch_taxon == "lethrinus fulvus" ~ "lethrinus",
        .data$catch_taxon == "neotrygon caeruieopunctata" ~
          "neotrygon caeruleopunctata",
        .data$catch_taxon == "myuripristis berndti" ~ "myripristis berndti",
        TRUE ~ .data$catch_taxon
      )
    )

  upload_parquet_to_cloud(
    data = catch_surveys_preprocessed,
    prefix = pars$surveys$ba_surveys$preprocessed_surveys$file_prefix,
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
      name = stringr::str_remove(
        .data$name,
        pattern = "Length_Frequency_Survey/catch_length..Length_Frequency_Survey/catch_length/"
      )
    ) %>%
    tidyr::pivot_wider(names_from = .data$name, values_from = .data$value) %>%
    dplyr::mutate(content = dplyr::coalesce(!!!.[3:ncol(.)])) %>%
    dplyr::filter(.data$n == 0 | !is.na(.data$content)) %>%
    dplyr::select(-"content") |>
    dplyr::mutate(total_length = as.numeric(.data$total_length)) |>
    tidyr::nest(
      "length" = c(
        .data$family,
        .data$species,
        .data$sex,
        .data$total_length
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
      name = stringr::str_remove(
        .data$name,
        pattern = "Market_Catch_Survey/catch_market..Market_Catch_Survey/catch_market/"
      )
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
        .data$group_market,
        .data$species_market,
        .data$catch_kg_market,
        .data$catch_price
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
      name = stringr::str_remove(
        .data$name,
        pattern = "Total_Catch_Survey/catch_catch..Total_Catch_Survey/catch_catch/"
      )
    ) %>%
    tidyr::pivot_wider(names_from = .data$name, values_from = .data$value) %>%
    dplyr::mutate(content = dplyr::coalesce(!!!.[3:ncol(.)])) %>%
    dplyr::filter(.data$n == 0 | !is.na(.data$content)) %>%
    dplyr::mutate(
      type_measure = dplyr::case_when(
        !is.na(.data$nb_ind_catch) ~ "individual",
        !is.na(.data$nb_buckets_catch) ~ "bucket",
        TRUE ~ .data$type_measure
      )
    ) |>
    dplyr::mutate(
      weight_kg = dplyr::coalesce(
        .data$weight_catch,
        .data$wgt_ind_catch,
        .data$wgt_buckets_catch
      ),
      nb_elements = dplyr::coalesce(.data$nb_ind_catch, .data$nb_buckets_catch)
    ) %>%
    dplyr::select(
      -c(
        .data$content,
        .data$weight_catch,
        .data$wgt_ind_catch,
        .data$wgt_buckets_catch,
        .data$nb_ind_catch,
        .data$nb_buckets_catch
      )
    ) %>%
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
        .data$group_catch %in%
          c("reef_fish", "goatfish", "parrotfish", "rabbitfish", "scavengers") ~
          "reef_fish",
        .data$group_catch %in% c("sharks_rays") ~ "sharks_rays",
        .data$group_catch %in% c("small_pelagic") ~ "small_pelagic",
        .data$group_catch %in% c("large_pelagic", "pelagic") ~ "large_pelagic",
        .data$group_catch %in% c("tuna_like", "tunalike") ~ "tuna_like",
        .data$group_catch %in% c("molluscs_crustaceans", "octopus", "squid") ~
          "molluscs_crustaceans",
        TRUE ~ .data$group_catch
      )
    ) |>
    tidyr::nest(
      "catch" = c(
        .data$type_measure,
        .data$all_catch_in_boat,
        .data$group_catch,
        .data$species_catch,
        .data$n_elements,
        .data$catch_kg
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
      gear = dplyr::case_when(
        .data$gear == "spear" ~ "spear_gun",
        TRUE ~ .data$gear
      ),
      boat_engine = ifelse(
        .data$boat_engine == "yes",
        "motorised",
        "unmotorised"
      ),
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
preprocess_pds_tracks <- function(
  log_threshold = logger::DEBUG,
  grid_size = 500
) {
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
  if (exists("preprocessed_filename")) {
    unlink(preprocessed_filename)
  }

  logger::log_success("Track preprocessing complete")

  grid_summaries <- generate_track_summaries(final_data)

  output_filename <-
    paste0(pars$pds$pds_tracks$file_prefix, "-grid_summaries") |>
    add_version(extension = "parquet")

  arrow::write_parquet(
    grid_summaries,
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

#' Reshape Species Groups from Wide to Long Format
#'
#' This function converts a data frame containing repeated species group columns
#' (species_group.0, species_group.1, etc.) into a long format where each species group
#' is represented as a separate row, with a column indicating which group it belongs to.
#'
#' @param df A data frame containing species group data in wide format
#'
#' @return A data frame in long format with each row representing a single species group record
#' @export
#'
#' @details
#' The function identifies columns that follow the pattern "species_group.X" and restructures
#' the data so that each species group from a submission is represented as a separate row.
#' It removes the position prefix from column names and adds an n_catch column to track
#' the group number (1-based indexing). Rows that contain only NA values are filtered out.
#'
#' @keywords preprocessing
#'
#' @examples
#' \dontrun{
#' long_species_data <- reshape_species_groups(catch_info)
#' }
#'
reshape_species_groups <- function(df = NULL) {
  # Get all column names that have the pattern species_group.X
  species_columns <- names(df)[grep("species_group\\.[0-9]+", names(df))]

  # Find unique position indicators (0, 1, 2, etc.)
  positions <- unique(stringr::str_extract(
    species_columns,
    "species_group\\.[0-9]+"
  ))

  # Create a list to store each position's data
  position_dfs <- list()

  # For each position, extract its columns and create a data frame
  for (pos in positions) {
    # Get columns for this position
    pos_cols <- c(
      "submission_id",
      names(df)[grep(paste0(pos, "\\."), names(df))]
    )

    # Skip if there are no columns for this position
    if (length(pos_cols) <= 1) {
      next
    }

    # Create data frame for this position
    pos_df <- df[, pos_cols, drop = FALSE]

    # Rename columns to remove the position prefix
    new_names <- names(pos_df)
    new_names[-1] <- stringr::str_replace(new_names[-1], paste0(pos, "\\."), "")
    names(pos_df) <- new_names

    # Add position identifier column
    pos_num <- as.numeric(stringr::str_extract(pos, "[0-9]+"))
    pos_df$n_catch <- pos_num + 1 # Adding 1 to make it 1-based instead of 0-based

    # Add to list
    position_dfs[[length(position_dfs) + 1]] <- pos_df
  }

  # Bind all position data frames
  result <- dplyr::bind_rows(position_dfs)

  # Remove rows where all species group columns are NA
  # (These are empty species group entries)
  species_detail_cols <- names(result)[
    !names(result) %in% c("submission_id", "n_catch")
  ]
  result <- result %>%
    dplyr::filter(
      rowSums(is.na(.[species_detail_cols])) != length(species_detail_cols)
    )

  # Sort by submission_id and n_catch
  result <- result %>%
    dplyr::arrange(.data$submission_id, .data$n_catch) |>
    dplyr::rename_with(~ stringr::str_remove(., "species_group/"))

  return(result)
}


#' Reshape Catch Data with Length Groupings
#'
#' This function takes a data frame with species catch information and reshapes it into a
#' long format while properly handling nested length group information.
#'
#' @param df A data frame containing catch data with species groups and length information
#'
#' @return A data frame in long format with each row representing a species at a specific
#'         length range, or just species data if no length information is available
#' @export
#'
#' @details
#' The function first calls `reshape_species_groups()` to convert the species data to long format,
#' then handles the length group information. For length data, it creates separate rows for each
#' length category while preserving the original structure for catches without length measurements.
#' Special handling is provided for fish over 100cm, where the actual length is moved to a separate column.
#'
#' The function preserves all rows, even those without length data, by creating consistent column
#' structure across all rows.
#'
#' @keywords preprocessing
#'
#' @examples
#' \dontrun{
#' final_data <- reshape_catch_data(catch_info)
#'
#' # Analyze counts by length range
#' final_data |>
#'   filter(!is.na(count)) |>
#'   group_by(species, length_range) |>
#'   summarize(total_count = sum(as.numeric(count), na.rm = TRUE))
#' }
reshape_catch_data <- function(df = NULL) {
  # First, reshape species groups

  species_long <- reshape_species_groups(df)

  # Extract length group columns
  length_cols <- names(species_long)[grep(
    "no_fish_by_length_group/",
    names(species_long)
  )]

  # Create a base dataset without length columns
  base_data <- species_long %>%
    dplyr::select(-dplyr::any_of(length_cols))

  # If there are no length columns, just return the base data
  if (length(length_cols) == 0) {
    return(base_data)
  }

  # Check if there are any non-NA values in length columns
  has_length_data <- species_long %>%
    dplyr::select(dplyr::all_of(length_cols)) %>%
    dplyr::mutate(has_data = rowSums(!is.na(.)) > 0) %>%
    dplyr::pull(.data$has_data)

  # Split the data into rows with and without length data
  rows_with_length <- which(has_length_data)

  # If no rows have length data, return the base data
  if (length(rows_with_length) == 0) {
    return(base_data)
  }

  # Process rows with length data
  length_data <- species_long[rows_with_length, ] %>%
    tidyr::pivot_longer(
      cols = dplyr::all_of(length_cols),
      names_to = "length_category",
      values_to = "count",
      values_drop_na = TRUE
    ) %>%
    # Clean up length_category column
    dplyr::mutate(
      length_category = stringr::str_remove(
        .data$length_category,
        "no_fish_by_length_group/"
      ),
      # Extract actual length ranges or special categories
      length_range = dplyr::case_when(
        stringr::str_detect(.data$length_category, "no_individuals_") ~
          stringr::str_replace(.data$length_category, "no_individuals_", ""),
        length_category == "fish_length_over100" ~ "over100",
        TRUE ~ .data$length_category
      ),
      # For over100 entries, move the value to length_over and set count to 1
      length_over = dplyr::if_else(
        .data$length_category == "fish_length_over100",
        .data$count,
        NA_character_
      ),
      count = dplyr::if_else(
        .data$length_category == "fish_length_over100",
        "1",
        .data$count
      )
    ) %>%
    dplyr::ungroup() |>
    # Remove the original length_category column and length columns
    dplyr::select(-"length_category", -dplyr::any_of(length_cols)) |>
    dplyr::filter(!(.data$length_range == "over100" & is.na(.data$length_over)))

  # Process rows without length data
  if (length(rows_with_length) < nrow(species_long)) {
    rows_without_length <- which(!has_length_data)
    no_length_data <- species_long[rows_without_length, ] %>%
      dplyr::select(-dplyr::any_of(length_cols)) %>%
      # Add empty length columns to match length_data structure
      dplyr::mutate(
        count = NA_character_,
        length_range = NA_character_,
        length_over = NA_character_
      )

    # Combine both datasets
    result <- dplyr::bind_rows(length_data, no_length_data) %>%
      dplyr::arrange(.data$submission_id, .data$n_catch)
  } else {
    result <- length_data
  }

  return(result)
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
    dplyr::select(.data$landing_site, .data$year_month, .data$gear, .data$pct_main_gear) |>
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
    dplyr::arrange(.data$landing_site, .data$year_month, dplyr::desc(.data$species_pct)) |>
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
