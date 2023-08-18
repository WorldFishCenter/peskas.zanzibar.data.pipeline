
read_landings <- function(file_path = NULL){
  ext <- tools::file_ext(file_path)

  if(ext == "xlsx"){
    data <- readxl::read_xlsx(file_path)
  } else {
    data <- readr::read_csv(file_path)
  }
  data
}

format_landings <- function(data = NULL, date_col = NULL, species_col = NULL){

  data_formatted <-
    data %>%
    dplyr::mutate()

}


#dat <- read_landings("dummy_landings_data.csv")
#ranks <- taxize::tax_rank(dat$species, db = "gbif", rows = 2)

#df <- dplyr::tibble(
#  species = names(ranks),
#  rank = unlist(ranks)
#) %>%
#  dplyr::mutate(rank = stringr::str_to_title(rank)) %>%
#  dplyr::mutate(rank = ifelse(is.na(rank), "comm_name", rank))


#sp_len <-
#  get_fish_length(
#    taxa = df$species[1],
#    rank = df$rank[1],
#    country_code = c(626, 360)
#  )

#sp_len <-
#  purrr::map2(df$species, df$rank, get_fish_length, "626")

#names(sp_len) <- df$species

#coeff <-
#  sp_len %>%
#  dplyr::bind_rows(.id = "species") %>%
#  dplyr::group_by(species) %>%
#  dplyr::filter(.data$Length1 %in% c("TL", "FL") &
#                  .data$Length2 %in% c("TL", "FL")) %>%
#  dplyr::summarise(a = median(a, na.rm = T),
#                   b = median(b, na.rm = T))

#dplyr::left_join(dat, coeff, by = "species")
