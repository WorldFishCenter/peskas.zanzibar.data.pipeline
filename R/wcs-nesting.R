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
    ) |>
    dplyr::distinct()
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
      survey_id = "submission_id",
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
    dplyr::select(
      "survey_id",
      "n",
      "species_market",
      "catch_kg_market",
      "catch_price"
    ) |>
    dplyr::distinct()
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
      survey_id = "submission_id",
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
    dplyr::mutate(content = dplyr::coalesce(!!!.[3:ncol(.)])) |>
    dplyr::filter(.data$n == 0 | !is.na(.data$content)) |>
    dplyr::rename(
      length_individual = "len_ind_catch",
      individuals = "nb_ind_catch",
      weight_individuals = "wgt_ind_catch",
      n_buckets = "nb_buckets_catch",
      weight_bucket = "wgt_buckets_catch"
    ) |>
    dplyr::mutate(
      type_measure = dplyr::case_when(
        !is.na(.data$individuals) |
          !is.na(.data$weight_individuals) ~ "individual",
        !is.na(.data$n_buckets) |
          !is.na(.data$weight_bucket) ~ "bucket",
        !is.na(.data$weight_catch) ~ "weight",
        TRUE ~ .data$type_measure
      )
    ) |>
    tidyr::separate_longer_delim("species_catch", delim = " ") |>
    dplyr::select(
      "survey_id",
      "n",
      "type_measure",
      "species_catch",
      weight = "weight_catch",
      "individuals",
      "weight_individuals",
      "n_buckets",
      "weight_bucket"
    ) |>
    dplyr::distinct()
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
      survey_id = "submission_id",
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
      fishing_location = tolower(.data$fishing_location),
      fishing_location = cleaner::clean_character(.data$fishing_location),
      gear_other = tolower(.data$gear_other),
      gear_other = cleaner::clean_character(.data$gear_other),
      fishing_ground_name = tolower(.data$fishing_ground_name),
      fishing_ground_name = cleaner::clean_character(.data$fishing_ground_name),
      engine_hp = as.numeric(.data$engine_hp)
    ) |>
    dplyr::distinct()
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
      survey_id = "submission_id",
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
    dplyr::rename("_attachments" = "data") |>
    # If there are no attachments empty the nested data frames
    dplyr::mutate(
      survey_id = as.integer(.data$survey_id),
      `_attachments` = purrr::map(
        .data$`_attachments`,
        ~ dplyr::filter(., !dplyr::if_all(dplyr::everything(), is.na))
      )
    ) |>
    dplyr::distinct()
}
