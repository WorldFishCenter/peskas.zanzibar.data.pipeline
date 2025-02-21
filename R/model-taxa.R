#' Calculate Catch Weight from Length-Weight Relationships or Bucket Measurements
#'
#' @description
#' Calculates total catch weight using either length-weight relationships or bucket measurements.
#' The function prioritizes length-based calculations when available, falling back to bucket-based
#' measurements when length data is missing.
#'
#' @param catch_data A data frame containing catch information with columns:
#'   \itemize{
#'     \item submission_id - Unique identifier for the catch
#'     \item n_catch - Number of catch events
#'     \item catch_taxon - FAO 3-alpha code
#'     \item individuals - Number of individuals (for length-based calculations)
#'     \item length - Length measurement in cm
#'     \item n_buckets - Number of buckets
#'     \item weight_bucket - Weight per bucket in kg
#'   }
#' @param lwcoeffs A data frame containing length-weight coefficients with columns:
#'   \itemize{
#'     \item catch_taxon - FAO 3-alpha code
#'     \item a_75 - 75th percentile of parameter 'a'
#'     \item b_75 - 75th percentile of parameter 'b'
#'   }
#'
#' @return A tibble with the following columns:
#'   \itemize{
#'     \item submission_id - Unique identifier for the catch
#'     \item n_catch - Number of catch events
#'     \item catch_taxon - FAO 3-alpha code
#'     \item individuals - Number of individuals
#'     \item length - Length measurement in cm
#'     \item n_buckets - Number of buckets
#'     \item weight_bucket - Weight per bucket in kg
#'     \item catch_kg - Total catch weight in kg
#'   }
#'
#' @details
#' The function calculates catch weight using two methods:
#' 1. Length-based calculation: W = a * L^b * N / 1000
#'    Where:
#'    - W is total weight in kg
#'    - a and b are length-weight relationship coefficients (75th percentile)
#'    - L is length in cm
#'    - N is number of individuals
#'
#' 2. Bucket-based calculation: W = n_buckets * weight_bucket
#'    Where:
#'    - W is total weight in kg
#'    - n_buckets is number of buckets
#'    - weight_bucket is weight per bucket in kg
#'
#' The final catch_kg uses length-based calculation when available,
#' falling back to bucket-based calculation when length data is missing.
#'
#' @examples
#' \dontrun{
#' # Calculate catch weights
#' catch_weights <- calculate_catch(
#'   catch_data = catch_data,
#'   lwcoeffs = length_weight_coeffs
#' )
#' }
#'
#' @note
#' - Length-based calculations use 75th percentile of length-weight coefficients
#' - All weights are returned in kilograms
#' - NA values are returned when neither calculation method is possible
#'
#' @importFrom dplyr left_join mutate case_when coalesce select
#'
#' @keywords mining
#' @export
calculate_catch <- function(catch_data = NULL, lwcoeffs = NULL) {
  catch_data |>
    dplyr::left_join(lwcoeffs, by = "catch_taxon") |>
    dplyr::mutate(
      # Calculate weight in grams for records with length measurements
      catch_length_gr = dplyr::case_when(
        # When we have length and coefficients
        !is.na(.data$length) & !is.na(.data$a_75) & !is.na(.data$b_75) ~
          .data$a_75 * (.data$length^.data$b_75),
        # Otherwise NA
        TRUE ~ NA_real_
      ),
      catch_length_kg = (.data$catch_length_gr * .data$individuals) / 1000,
      catch_bucket_kg = dplyr::case_when(
        # When we have bucket information
        !is.na(.data$n_buckets) & !is.na(.data$weight_bucket) ~
          .data$n_buckets * (.data$weight_bucket),
        # Otherwise NA
        TRUE ~ NA_real_
      )
    ) |>
    dplyr::mutate(catch_kg = dplyr::coalesce(.data$catch_length_kg, .data$catch_bucket_kg)) |>
    dplyr::select(
      "submission_id", "n_catch", "catch_taxon", "individuals", "length", "n_buckets",
      "weight_bucket", "catch_kg"
    )
}

#' Get Length-Weight Coefficients for FAO Taxa
#'
#' @description
#' Retrieves and summarizes length-weight relationship coefficients for FAO taxonomic groups
#' from both FishBase and SeaLifeBase databases. The function calculates 75th percentile
#' for the 'a' and 'b' parameters of the length-weight relationship (W = aL^b).
#'
#' @param taxa_list A character vector of FAO 3-alpha codes
#' @param asfis_list A data frame containing the FAO ASFIS list (see details)
#'
#' @return A tibble with the following columns:
#'   \itemize{
#'     \item catch_taxon - FAO 3-alpha code
#'     \item n - Number of length-weight relationships found
#'     \item a_75 - 75th percentile of parameter 'a'
#'     \item b_75 - 75th percentile of parameter 'b'
#'   }
#'
#' @details
#' The function processes fish and non-fish species differently:
#'
#' For fish species (FishBase):
#' 1. Filters for species present in FAO Area 51
#' 2. Retrieves length-weight relationships from FishBase
#' 3. Filters for total length (TL) measurements
#' 4. Excludes quality-checked relationships (EsQ = "yes")
#'
#' For non-fish species (SeaLifeBase):
#' 1. Retrieves species distribution data
#' 2. Retrieves length-weight relationships from SeaLifeBase
#' 3. Excludes quality-checked relationships (EsQ = "yes")
#'
#' The 75th percentile is used instead of median as it provides more biologically
#' reasonable estimates of weights, possibly accounting for specimens in good condition
#' while avoiding extreme values.
#'
#' @examples
#' \dontrun{
#' # Get coefficients for both fish and non-fish groups
#' lw_coeffs <- getLWCoeffs(
#'   taxa_list = c("TUN", "BEN", "IAX", "OCZ"),
#'   asfis_list = asfis_data
#' )
#' }
#'
#' @note
#' - Requires rfishbase package
#' - Internet connection needed for FishBase and SeaLifeBase queries
#' - Only includes non-quality-checked relationships
#' - Area 51 filtering applied only to fish species
#' - Uses 75th percentile of coefficients for more realistic weight estimates
#' - Automatically selects appropriate database based on FAO code
#'
#' @seealso
#' \code{\link{get_fao_groups}}, \code{\link{get_fishbase_species}}
#'
#' @importFrom rfishbase length_weight distribution
#' @importFrom dplyr filter select mutate group_by summarise left_join right_join full_join bind_rows
#' @importFrom stats quantile
#'
#' @keywords mining
#' @export
getLWCoeffs <- function(taxa_list = NULL, asfis_list = NULL) {
  # Define non-fish FAO codes
  nonfish_codes <- c("IAX", "OCZ", "PEZ", "SLV", "CRA", "COZ") # cuttlefish, octopus, shrimp, lobster, crab, cockles

  species_list <-
    get_fao_groups(fao_codes = taxa_list, asfis_list = asfis_list) |>
    get_all_species() |>
    purrr::map(~ as.character(.)) |>
    purrr::map(~ dplyr::as_tibble(.)) |>
    dplyr::bind_rows(.id = "catch_taxon") |>
    dplyr::rename(species = "value")

  # Split species into fish and non-fish
  fish_species <- species_list |>
    dplyr::filter(!.data$catch_taxon %in% nonfish_codes)

  nonfish_species <- species_list |>
    dplyr::filter(.data$catch_taxon %in% nonfish_codes)

  # Get fish distributions and LW relationships from FishBase
  fish_distribution <- NULL
  if (nrow(fish_species) > 0) {
    fish_distribution <- rfishbase::faoareas(
      unique(fish_species$species),
      fields = c("AreaCode")
    ) |>
      dplyr::filter(.data$AreaCode == 51) |>
      dplyr::rename(species = "Species") |>
      dplyr::left_join(fish_species, by = "species") |>
      dplyr::select("catch_taxon", "SpecCode", "species")
  }

  # Get non-fish distributions and LW relationships from SeaLifeBase
  nonfish_distribution <- NULL
  if (nrow(nonfish_species) > 0) {
    nonfish_distribution <- rfishbase::faoareas(
      unique(nonfish_species$species),
      # fields = c("AreaCode"),
      server = "sealifebase"
    ) |>
      # dplyr::filter(.data$AreaCode == 51) |>
      dplyr::rename(species = "Species") |>
      dplyr::left_join(nonfish_species, by = "species") |>
      dplyr::select("catch_taxon", "SpecCode", "species")
  }


  # Process fish LW relationships
  fish_coeffs <- NULL
  if (!is.null(fish_distribution) && nrow(fish_distribution) > 0) {
    fish_coeffs <- rfishbase::length_weight(
      fish_distribution$species,
      fields = c("Species", "SpecCode", "Type", "EsQ", "a", "b")
    ) |>
      dplyr::mutate(EsQ = tolower(.data$EsQ)) |>
      dplyr::filter(.data$Type == "TL", is.na(.data$EsQ) | .data$EsQ != "yes") |>
      dplyr::rename(species = "Species") |>
      dplyr::full_join(fish_distribution, by = c("species", "SpecCode")) |>
      dplyr::select("catch_taxon", "SpecCode", "species", "a", "b") |>
      dplyr::group_by(.data$catch_taxon) |>
      dplyr::summarise(
        n = dplyr::n(),
        a_75 = stats::quantile(.data$a, 0.75, na.rm = T),
        b_75 = stats::quantile(.data$b, 0.75, na.rm = T)
      )
  }

  # Process non-fish LW relationships
  nonfish_coeffs <- NULL
  if (!is.null(nonfish_distribution) && nrow(nonfish_distribution) > 0) {
    nonfish_coeffs <- rfishbase::length_weight(
      nonfish_distribution$species,
      fields = c("Species", "Type", "EsQ", "a", "b"),
      server = "sealifebase"
    ) |>
      dplyr::mutate(EsQ = tolower(.data$EsQ)) |>
      dplyr::filter(is.na(.data$EsQ) | .data$EsQ != "yes") |>
      dplyr::rename(species = "Species") |>
      dplyr::full_join(nonfish_distribution, by = c("species")) |>
      dplyr::select("catch_taxon", "species", "a", "b") |>
      dplyr::group_by(.data$catch_taxon) |>
      dplyr::summarise(
        n = dplyr::n(),
        a_75 = stats::quantile(.data$a, 0.75, na.rm = T),
        b_75 = stats::quantile(.data$b, 0.75, na.rm = T)
      )
  }

  # Combine results
  lw_coeffs <- dplyr::bind_rows(fish_coeffs, nonfish_coeffs) |>
    dplyr::select(
      "catch_taxon",
      "n",
      "a_75",
      "b_75"
    )

  return(lw_coeffs)
}


#' Extract and Format FAO Taxonomic Groups
#'
#' @description
#' Filters and formats taxonomic information from the FAO ASFIS list for specified FAO 3-alpha codes,
#' excluding miscellaneous ("MZZ") and unknown ("UNKN") categories.
#'
#' @param fao_codes A character vector of FAO 3-alpha codes to extract. If NULL, returns empty dataset.
#' @param asfis_list A data frame containing the FAO ASFIS list with required columns:
#'   \itemize{
#'     \item Alpha3_Code - FAO 3-alpha code
#'     \item Scientific_Name - Scientific name of taxon
#'     \item English_name - Common name in English
#'     \item Family - Family name
#'     \item Order - Order name
#'     \item ISSCAAP_Group - FAO ISSCAAP group number
#'   }
#'
#' @return A tibble with standardized column names containing taxonomic information:
#'   \itemize{
#'     \item a3_code - FAO 3-alpha code
#'     \item scientific_name - Scientific name
#'     \item english_name - Common name in English
#'     \item family - Family name
#'     \item order - Order name
#'     \item taxon_group - ISSCAAP group number
#'   }
#'
#' @details
#' The function:
#' 1. Filters ASFIS list for specified FAO codes
#' 2. Standardizes column names for consistency
#' 3. Removes miscellaneous ("MZZ") and unknown ("UNKN") categories
#' 4. Preserves the taxonomic hierarchy information
#'
#' @examples
#' # Example ASFIS data
#' asfis <- data.frame(
#'   Alpha3_Code = c("TUN", "MZZ", "RAG"),
#'   Scientific_Name = c("Thunnini", "Marine fishes nei", "Rastrelliger kanagurta"),
#'   English_name = c("Tunas", "Marine fishes", "Indian mackerel"),
#'   Family = c("SCOMBRIDAE", NA, "SCOMBRIDAE"),
#'   Order = c("PERCIFORMES", NA, "PERCIFORMES"),
#'   ISSCAAP_Group = c(36, 39, 37)
#' )
#'
#' # Get taxonomic information for specific codes
#' fao_taxa <- get_fao_groups(c("TUN", "RAG"), asfis)
#'
#' @note
#' - Requires dplyr package
#' - MZZ (Miscellaneous marine fishes) and UNKN (Unknown) are automatically excluded
#' - Column names are standardized for consistency with other functions
#'
#' @keywords mining
#'
#' @export
get_fao_groups <- function(fao_codes = NULL, asfis_list = NULL) {
  asfis_list %>%
    dplyr::filter(.data$Alpha3_Code %in% fao_codes) %>%
    dplyr::select(
      a3_code = "Alpha3_Code",
      scientific_name = "Scientific_Name",
      english_name = "English_name",
      family = "Family",
      order = "Order",
      taxon_group = "ISSCAAP_Group"
    ) |>
    dplyr::filter(!.data$a3_code %in% c("MZZ", "UNKN"))
}

#' Get FishBase Species for FAO 3-Alpha Codes
#'
#' @description
#' Retrieves all FishBase species associated with FAO 3-alpha codes.
#' The function processes each taxonomic level (species, genus, family, order) and returns
#' a complete list of associated species for each FAO code.
#'
#' @param fao_data A data frame containing FAO 3-alpha codes and taxonomic information with columns:
#'   \itemize{
#'     \item a3_code - FAO 3-alpha code
#'     \item scientific_name - Scientific name (can include "spp" or "nei" suffixes)
#'     \item family - Family name (can be NA)
#'     \item order - Order name (can be NA)
#'   }
#' @param database_server A character string specifying which database to use ("fishbase" or "sealifebase")
#'
#' @return A named list where:
#'   \itemize{
#'     \item Names are FAO 3-alpha codes
#'     \item Values are vectors of FishBase species
#'     \item Empty vectors (integer(0)) indicate no matches found
#'   }
#'
#' @details
#' The function implements a hierarchical search strategy:
#' 1. Tries exact species match if scientific name contains two words
#' 2. Tries genus level match
#' 3. Tries family level match if family name is available
#' 4. Tries order level match if order name is available
#'
#' Performance optimizations include:
#' \itemize{
#'   \item Caching of API results to avoid redundant calls
#'   \item Requesting only Species field to minimize data transfer
#'   \item Batch processing of species lookups
#' }
#'
#' @examples
#' # Example data frame
#' fao_data <- data.frame(
#'   a3_code = c("TUN", "RAG"),
#'   scientific_name = c("Thunnini", "Rastrelliger kanagurta"),
#'   family = c("SCOMBRIDAE", "SCOMBRIDAE"),
#'   order = c("PERCIFORMES", "PERCIFORMES")
#' )
#'
#' # Get species
#' speccodes <- get_fishbase_species(fao_data)
#'
#' # Check number of species for each code
#' sapply(speccodes, length)
#'
#' @note
#' - Requires the rfishbase package
#' - Internet connection needed for FishBase API queries
#' - Performance depends on API response times and number of species in each group
#'
#' @seealso
#' \code{\link[rfishbase]{species}}, \code{\link[rfishbase]{species_list}}
#'
#' @importFrom rfishbase species species_list
#' @importFrom stats setNames
#'
#' @keywords mining
#' @export
get_fishbase_species <- function(fao_data = NULL, database_server = "fishbase") {
  # Initialize empty list to store results
  result_list <- list()

  # Cache lookup results to avoid repeated API calls
  species_cache <- list()
  genus_cache <- list()
  family_cache <- list()
  order_cache <- list()

  # Process each FAO code
  for (i in 1:nrow(fao_data)) {
    code <- fao_data$a3_code[i]
    sci_name <- fao_data$scientific_name[i]
    family_name <- fao_data$family[i]
    order_name <- fao_data$order[i]

    # Clean name
    clean_name <- gsub(" spp$| nei$", "", sci_name)

    # Try species lookup for exact matches
    if (grepl(" ", clean_name)) {
      # Check cache first
      if (!is.null(species_cache[[clean_name]])) {
        species_data <- species_cache[[clean_name]]
      } else {
        species_data <- tryCatch(
          rfishbase::species(clean_name, fields = "Species", server = database_server),
          error = function(e) NULL
        )
        # Cache the result
        species_cache[[clean_name]] <- species_data
      }

      if (!is.null(species_data) && nrow(species_data) > 0) {
        result_list[[code]] <- species_data$Species
        next
      }
    }

    # Try genus lookup
    genus <- ifelse(grepl(" ", clean_name),
      sub(" .*$", "", clean_name),
      clean_name
    )

    # Check genus cache
    if (!is.null(genus_cache[[genus]])) {
      genus_data <- genus_cache[[genus]]
    } else {
      genus_data <- tryCatch(
        rfishbase::species_list(Genus = genus, server = database_server),
        error = function(e) NULL
      )
      # Cache the result
      genus_cache[[genus]] <- genus_data
    }

    if (!is.null(genus_data) && length(genus_data) > 0) {
      if (is.character(genus_data)) {
        # Use cached species data where possible
        all_speccodes <- c()
        # Get SpecCodes for all species in genus at once
        species_data <- tryCatch(
          rfishbase::species(genus_data, fields = "Species", server = database_server),
          error = function(e) NULL
        )
        if (!is.null(species_data) && nrow(species_data) > 0) {
          result_list[[code]] <- species_data$Species
          next
        }
      } else if (is.data.frame(genus_data) && nrow(genus_data) > 0) {
        result_list[[code]] <- genus_data$Species
        next
      }
    }

    # Try family lookup
    if (!is.na(family_name) && family_name != "") {
      family_proper <- gsub("^([A-Z])([A-Z]+)$", "\\1\\L\\2", family_name, perl = TRUE)

      if (!is.null(family_cache[[family_proper]])) {
        family_data <- family_cache[[family_proper]]
      } else {
        family_data <- tryCatch(
          rfishbase::species_list(Family = family_proper, server = database_server),
          error = function(e) NULL
        )
        family_cache[[family_proper]] <- family_data
      }

      if (!is.null(family_data) && length(family_data) > 0) {
        if (is.character(family_data)) {
          # Get SpecCodes for all species in family at once
          species_data <- tryCatch(
            rfishbase::species(family_data, fields = "Species", server = database_server),
            error = function(e) NULL
          )
          if (!is.null(species_data) && nrow(species_data) > 0) {
            result_list[[code]] <- species_data$Species
            next
          }
        } else if (is.data.frame(family_data) && nrow(family_data) > 0) {
          result_list[[code]] <- family_data$Species
          next
        }
      }
    }

    # Try order lookup
    if (!is.na(order_name) && order_name != "") {
      order_proper <- gsub("^([A-Z])([A-Z]+)$", "\\1\\L\\2", order_name, perl = TRUE)

      if (!is.null(order_cache[[order_proper]])) {
        order_data <- order_cache[[order_proper]]
      } else {
        order_data <- tryCatch(
          rfishbase::species_list(Order = order_proper, server = database_server),
          error = function(e) NULL
        )
        order_cache[[order_proper]] <- order_data
      }

      if (!is.null(order_data) && length(order_data) > 0) {
        if (is.character(order_data)) {
          # Get SpecCodes for all species in order at once
          species_data <- tryCatch(
            rfishbase::species(order_data, fields = "Species", server = database_server),
            error = function(e) NULL
          )
          if (!is.null(species_data) && nrow(species_data) > 0) {
            result_list[[code]] <- species_data$Species
            next
          }
        } else if (is.data.frame(order_data) && nrow(order_data) > 0) {
          result_list[[code]] <- order_data$Species
          next
        }
      }
    }

    # If no match found, add empty vector
    result_list[[code]] <- integer(0)
  }

  return(result_list)
}

#' Get Species from Both FishBase and SeaLifeBase Databases
#'
#' @description
#' Splits FAO taxa between fish and non-fish groups based on ISSCAAP codes and retrieves
#' species information from the appropriate database (FishBase or SeaLifeBase).
#'
#' @param fao_data A data frame containing FAO taxonomic information with columns:
#'   \itemize{
#'     \item a3_code - FAO 3-alpha code
#'     \item scientific_name - Scientific name
#'     \item family - Family name
#'     \item order - Order name
#'     \item taxon_group - ISSCAAP group number
#'   }
#'
#' @return A named list where:
#'   \itemize{
#'     \item Names are FAO 3-alpha codes
#'     \item Values are vectors of species names from either FishBase or SeaLifeBase
#'     \item Empty vectors (integer(0)) indicate no matches found
#'   }
#'
#' @details
#' The function uses ISSCAAP groups to determine the appropriate database:
#' - Group 57: Cephalopods (SeaLifeBase)
#' - Group 45: Shrimps (SeaLifeBase)
#' - Group 43: Lobsters (SeaLifeBase)
#' - Group 42: Crabs (SeaLifeBase)
#' - Group 56: Molluscs (SeaLifeBase)
#' - Others: FishBase
#'
#' @examples
#' \dontrun{
#' # Example data frame with both fish and non-fish taxa
#' fao_data <- data.frame(
#'   a3_code = c("TUN", "IAX"),
#'   scientific_name = c("Thunnini", "Sepia spp"),
#'   family = c("SCOMBRIDAE", "SEPIIDAE"),
#'   order = c("PERCIFORMES", "SEPIIDA"),
#'   taxon_group = c(36, 57)
#' )
#'
#' # Get species from both databases
#' species_list <- get_all_species(fao_data)
#' }
#'
#' @note
#' - Requires rfishbase package
#' - Internet connection needed for database queries
#' - Uses caching to improve performance
#' - Automatically switches between databases based on taxonomic group
#'
#' @seealso
#' \code{\link{get_fishbase_species}}, \code{\link{get_fao_groups}}
#'
#' @importFrom dplyr filter
#'
#' @keywords mining
#' @export
get_all_species <- function(fao_data = NULL) {
  # Split data based on ISSCAAP groups
  sealifebase_groups <- c(57, 45, 43, 42, 56) # cephalopods, shrimps, lobsters, crabs, molluscs

  fish_data <- fao_data %>%
    dplyr::filter(!.data$taxon_group %in% sealifebase_groups)

  nonfish_data <- fao_data %>%
    dplyr::filter(.data$taxon_group %in% sealifebase_groups)

  # Get fish species from FishBase
  fish_species <- get_fishbase_species(fish_data, database_server = "fishbase")

  # Get non-fish species from SeaLifeBase
  nonfish_species <- get_fishbase_species(nonfish_data, database_server = "sealifebase") # reuse same function as it works for both

  # Combine results maintaining list structure
  result_list <- c(fish_species, nonfish_species)

  return(result_list)
}

#' Expand Taxonomic Vectors into a Data Frame
#'
#' Converts a vector of species identifiers into a detailed data frame containing taxonomic classification. Each identifier should follow the format 'family_genus_species', which is expanded to include comprehensive taxonomic details.
#'
#' @param data A vector of species identifiers formatted as 'family_genus_species'. If not provided, the function will return an error.
#' @return A data frame where each row corresponds to a species, enriched with taxonomic classification information including family, genus, species, and additional taxonomic ranks.
#' @keywords mining
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
