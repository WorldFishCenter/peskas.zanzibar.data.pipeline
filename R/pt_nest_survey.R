#' Nest length group columns
#'
#' Nests length group columns obtained when reading structured data from the kobo
#' landings survey.
#'
#' @param x Kobo survey in tabular format.
#'
#' @return Nested Landings data in which the information about multiple length information has
#'   been nested into a single column (`length`) this column contains a
#'   tibble for every row. This, attachment tibble has as many rows as there are
#'   length information
#' @export
#'
pt_nest_length <- function(x) {
  x %>%
    dplyr::select(.data$`_id`, dplyr::starts_with("Length_Frequency_Survey")) %>%
    tidyr::pivot_longer(-c(.data$`_id`)) %>%
    dplyr::mutate(
      n = stringr::str_extract(.data$name, "(\\d+)"),
      name = stringr::str_remove(.data$name, "(\\d+)"),
      name = stringr::str_remove(.data$name, pattern = "Length_Frequency_Survey/catch_length..Length_Frequency_Survey/catch_length/")
    ) %>%
    tidyr::pivot_wider(names_from = .data$name, values_from = .data$value) %>%
    dplyr::mutate(content = dplyr::coalesce(!!!.[3:ncol(.)])) %>%
    dplyr::filter(.data$n == 0 | !is.na(.data$content)) %>%
    dplyr::select(-.data$content) %>%
    tidyr::nest("length" = c(.data$family, .data$species, .data$sex, .data$total_length), .by = .data$`_id`)
}

#' Nest market group columns
#'
#' Nests market group columns obtained when reading structured data from the kobo
#' landings survey.
#'
#' @param x Kobo survey in tabular format.
#'
#' @return Nested Landings data in which the information about multiple market information has
#'   been nested into a single column (`market`) this column contains a
#'   tibble for every row. This, attachment tibble has as many rows as there are
#'   market information.
#' @export
#'
pt_nest_market <- function(x) {
  x %>%
    dplyr::select(.data$`_id`, dplyr::starts_with("Market_Catch_Survey")) %>%
    tidyr::pivot_longer(-c(.data$`_id`)) %>%
    dplyr::mutate(
      n = stringr::str_extract(.data$name, "(\\d+)"),
      name = stringr::str_remove(.data$name, "(\\d+)"),
      name = stringr::str_remove(.data$name, pattern = "Market_Catch_Survey/catch_market..Market_Catch_Survey/catch_market/")
    ) %>%
    tidyr::pivot_wider(names_from = .data$name, values_from = .data$value) %>%
    dplyr::mutate(content = dplyr::coalesce(!!!.[3:ncol(.)])) %>%
    dplyr::filter(.data$n == 0 | !is.na(.data$content)) %>%
    dplyr::select(-.data$content) %>%
    tidyr::nest("market" = c(.data$group_market, .data$species_market, .data$weight_market, .data$price_sold_for), .by = .data$`_id`)
}

#' Nest catch catch columns
#'
#' Nests catch group columns obtained when reading structured data from the kobo
#' landings survey.
#'
#' @param x Kobo survey in tabular format.
#'
#' @return Nested Landings data in which the information about multiple catch information has
#'   been nested into a single column (`catch`) this column contains a
#'   tibble for every row. This, attachment tibble has as many rows as there are
#'   catch information.
#' @export
#'
pt_nest_catch <- function(x) {
  x %>%
    dplyr::select(.data$`_id`, dplyr::starts_with("Total_Catch_Survey")) %>%
    tidyr::pivot_longer(-c(.data$`_id`)) %>%
    dplyr::mutate(
      n = stringr::str_extract(.data$name, "(\\d+)"),
      name = stringr::str_remove(.data$name, "(\\d+)"),
      name = stringr::str_remove(.data$name, pattern = "Total_Catch_Survey/catch_catch..Total_Catch_Survey/catch_catch/")
    ) %>%
    tidyr::pivot_wider(names_from = .data$name, values_from = .data$value) %>%
    dplyr::mutate(content = dplyr::coalesce(!!!.[3:ncol(.)])) %>%
    dplyr::filter(.data$n == 0 | !is.na(.data$content)) %>%
    dplyr::mutate(weight_kg = dplyr::coalesce(.data$weight_catch, .data$wgt_ind_catch, .data$wgt_buckets_catch)) %>%
    dplyr::select(-c(.data$content, .data$weight_catch, .data$wgt_ind_catch, .data$wgt_buckets_catch)) %>%
    tidyr::nest(
      "catch" = c(
        .data$type_measure, .data$All_catch_in_boat, .data$group_catch,
        .data$species_catch, .data$nb_ind_catch, .data$nb_buckets_catch,
        .data$weight_kg
      ),
      .by = .data$`_id`
    )
}

#' Nest trip catch columns
#'
#' Nests trip group columns obtained when reading structured data from the kobo
#' landings survey.
#'
#' @param x Kobo survey in tabular format.
#'
#' @return Nested Landings data in which the information about multiple trip information has
#'   been nested into a single column (`trip`) this column contains a
#'   tibble for every row. This, attachment tibble has as many rows as there are
#'   trip information.
#' @export
pt_nest_trip <- function(x) {
  x %>%
    dplyr::select(.data$`_id`, dplyr::starts_with("Fishing_Trip")) %>%
    tidyr::pivot_longer(-c(.data$`_id`)) %>%
    dplyr::mutate(
      n = stringr::str_extract(.data$name, "(\\d+)"),
      name = stringr::str_remove(.data$name, "(\\d+)"),
      name = stringr::str_remove(.data$name, pattern = "Fishing_Trip/")
    ) %>%
    tidyr::pivot_wider(names_from = .data$name, values_from = .data$value) %>%
    dplyr::mutate(content = dplyr::coalesce(!!!.[3:ncol(.)])) %>%
    # dplyr::filter(!is.na(.data$content)) %>%
    dplyr::select(-.data$content, -.data$n)
}

#' Nest attachment columns
#'
#' Nests attachment columns obtained when reading structured data from the kobo
#' landings survey
#'
#' One of the disadvantages of using structured survey data is that the tables
#' can become very wide (many columns). This happens when question groups or
#' other fields can be recorded multiple times. For example in the landings
#' survey, for each species captured, about 17 questions are recorded. There is
#' no limit to the number of species that can be recorded in the trip. If, for
#' example a survey records seven species we will have over a hundred columns in
#' the data corresponding to species information.
#'
#' To improve that situation an avoid using multiple tables we use **nested data
#' frames** (see [tidyr::nest]). In nested data frames columns can be lists and
#' can contain arbitrary information, like other data frames, lists, vectors, or
#' models.
#'
#' @param x A data frame containing raw landings data from the Timor operations.
#'
#' @return Landings data in which the information about multiple attachments has
#'   been nested into a single column (`_attachments`) this column contains a
#'   tibble for every row. This, attachment tibble has as many rows as there are
#'   attachments.
#'
#' @export
#' @importFrom rlang .data
#'
#' @examples
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
pt_nest_attachments <- function(x) {
  x %>%
    # Using the .data pronoun to avoid RMD check notes
    dplyr::select(.data$`_id`, dplyr::starts_with("_attachments")) %>%
    dplyr::mutate_all(as.character) %>%
    # Column names follow the form "_attachments.0.download_large_url"
    tidyr::pivot_longer(
      cols = -.data$`_id`,
      names_to = c("n", "field"),
      names_prefix = "_attachments.",
      names_sep = "\\."
    ) %>%
    # We want one attachment per row and fields as columns
    tidyr::pivot_wider(names_from = "field", values_from = "value") %>%
    # Attachments already have id and this column is superfluous
    dplyr::select(-.data$n) %>%
    dplyr::group_by(.data$`_id`) %>%
    tidyr::nest() %>%
    dplyr::ungroup() %>%
    dplyr::rename("_attachments" = "data") %>%
    # If there are no attachments empty the nested data frames
    dplyr::mutate(`_attachments` = purrr::map(
      .data$`_attachments`,
      ~ dplyr::filter(., !is.na(.data$id))
    ))
}
