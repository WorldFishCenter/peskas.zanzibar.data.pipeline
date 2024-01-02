#' Expand Taxonomic Vectors into a Data Frame
#'
#' @description This function takes a vector of species identifiers and expands it into a
#' data frame that includes taxonomic classification information for each species.
#' Each species identifier is expected to follow the format 'family_genus_species'.
#'
#' @param data A vector of species identifiers where each entry is a string with
#' the format 'family_genus_species'. If not supplied, the function will throw an error.
#'
#' @return A data frame where each row corresponds to a unique species from the input data.
#' The data frame includes columns for species catch, number of elements, weight, and taxonomic
#' classification including catch group, kingdom, phylum, order, family, genus and species.
#'
#' @examples
#' \dontrun{
#' species_vector <- c("lutjanidae_lutjanus_spp", "scaridae_spp", "acanthuridae_naso_hexacanthus")
#' expanded_data <- expand_taxa(species_vector)
#' }
#' @export
#'
#' @details The function first splits the species identifier into a list of species,
#' replaces underscores with spaces, and counts the number of words to help with further
#' classification. It then uses conditional logic to extract genus and species names,
#' handle special cases, and clean up group names. The cleaned data is then used to fetch
#' taxonomic classification from the GBIF database using the `taxize` package. The final
#' data frame is constructed by joining the expanded species data with the taxonomic ranks.
#'
#' @import dplyr tidyr stringr taxize purrr
#'
#' @note The function requires internet access to fetch data from the GBIF database.
#' It also assumes that the input vector is properly formatted.
#' If there are any formatting issues or the GBIF database does not recognize a species,
#' the function may return unexpected results or throw an error.
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
    dplyr::select(-c(.data$class, .data$`NA`))

  dplyr::left_join(taxa_expanded, groups_rank, by = "catch_group")
}
