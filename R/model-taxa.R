#' Calculate Catch Weight from Length-Weight Relationships or Bucket Measurements
#'
#' @description
#' Calculates total catch weight using either length-weight relationships or bucket measurements.
#' The function prioritizes length-based calculations when available, falling back to bucket-based
#' measurements when length data is missing. For octopus (`OCZ` and `OQC`), the
#' function converts the recorded arm-span to mantle length by dividing by 5.5
#' before applying the length-weight formula, because the published
#' coefficients for both are fitted on mantle length.
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
#'     \item lw_a - geometric mean of parameter 'a' across studies
#'     \item lw_b - arithmetic mean of parameter 'b' across studies
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
#'    - a and b are length-weight relationship coefficients aggregated across
#'      studies (geometric mean of a, arithmetic mean of b; cf. Froese 2006)
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
#' - Length-based calculations aggregate study-level (a, b) pairs as
#'   a = exp(mean(log(a))) (geometric mean), b = mean(b) (arithmetic mean).
#'   This preserves the log-linear nature of the length-weight relationship.
#' - All weights are returned in kilograms
#' - NA values are returned when neither calculation method is possible
#'
#' @keywords mining preprocessing
#' @export
calculate_catch <- function(catch_data = NULL, lwcoeffs = NULL) {
  catch_data |>
    dplyr::left_join(lwcoeffs, by = "catch_taxon") |>
    dplyr::mutate(
      # Calculate weight in grams for records with length measurements
      catch_length_gr = dplyr::case_when(
        # Octopus coefficients are fitted on mantle length, but surveys record
        # arm-span. Both octopus codes need the conversion.
        !is.na(.data$length) &
          !is.na(.data$lw_a) &
          !is.na(.data$lw_b) &
          .data$catch_taxon %in% c("OCZ", "OQC") ~
          .data$lw_a * ((.data$length / 5.5)^.data$lw_b),
        # General case for other species - direct calculation
        !is.na(.data$length) & !is.na(.data$lw_a) & !is.na(.data$lw_b) ~
          .data$lw_a * (.data$length^.data$lw_b),
        # Otherwise NA
        TRUE ~ NA_real_
      ),
      # Convert to kilograms
      catch_length_kg = (.data$catch_length_gr * .data$individuals) / 1000,
      # Calculate weight from bucket information if available
      catch_bucket_kg = dplyr::case_when(
        !is.na(.data$n_buckets) & !is.na(.data$weight_bucket) ~
          .data$n_buckets * .data$weight_bucket,
        # Otherwise NA
        TRUE ~ NA_real_
      )
    ) |>
    dplyr::mutate(
      catch_kg = dplyr::coalesce(.data$catch_length_kg, .data$catch_bucket_kg)
    ) |>
    dplyr::select(
      dplyr::any_of(c(
        "submission_id",
        "n_catch",
        "fish_group",
        "catch_taxon",
        "individuals",
        "length",
        "n_buckets",
        "weight_bucket",
        "catch_kg"
      ))
    )
}

#' Median ratio of common length to maximum length in FishBase
#'
#' Measured over the 3,748 species in release 25.04 that carry both fields:
#' median 0.625, IQR 0.50-0.72, 5-95% 0.341-0.857. Used by [getLWCoeffs()] to
#' estimate a common length for the 90% of species FishBase leaves without one.
#'
#' @return A single numeric.
#' @keywords internal
#' @noRd
common_length_ratio <- function() 0.625

#' Minimum of a vector, NA rather than Inf when everything is missing
#'
#' `min(x, na.rm = TRUE)` returns `Inf` for an all-NA vector, which then
#' propagates as `NaN` through any arithmetic and compares as `NA` against
#' everything -- turning "no data" into "no problem found".
#'
#' @param x A numeric vector.
#' @return The minimum, or `NA_real_` if `x` is entirely missing.
#' @keywords internal
#' @noRd
safe_min <- function(x) {
  if (all(is.na(x))) NA_real_ else min(x, na.rm = TRUE)
}

#' Get Length-Weight Coefficients and Morphological Data for Species
#'
#' @description
#' Retrieves and summarizes length-weight relationship coefficients and morphological data
#' by handling both FishBase and SeaLifeBase data in a single workflow.
#'
#' @param taxa_list Character vector of FAO 3-alpha codes
#' @param asfis_list ASFIS list data frame
#' @param fb_version FishBase release to read, e.g. `"25.04"`. Pinned in
#'   `inst/config.yml` under `metadata:fishbase`; `"latest"` is unsafe.
#' @param slb_version SeaLifeBase release to read, e.g. `"24.07"`.
#' @param fao_areas FAO major fishing areas to keep species from. Zanzibar,
#'   Kenya and Mozambique are all area 51 (Western Indian Ocean); set it from
#'   `metadata:fishbase:fao_areas` in config when porting to another country.
#' @return A list with two elements:
#'   \itemize{
#'     \item lw - A data frame with length-weight coefficients:
#'       \itemize{
#'         \item catch_taxon - FAO 3-alpha code
#'         \item n - Number of (a, b) records aggregated
#'         \item lw_a - Geometric mean of parameter 'a' across studies
#'         \item lw_b - Arithmetic mean of parameter 'b' across studies
#'       }
#'     \item ml - A data frame with morphological data:
#'       \itemize{
#'         \item catch_taxon - FAO 3-alpha code
#'         \item n - Number of measurements
#'         \item max_length_75 - 75th percentile of maximum length
#'         \item max_weightkg_75 - 75th percentile of maximum weight in kg
#'       }
#'   }
#' @examples
#' \dontrun{
#' # Get coefficients and morphological data
#' results <- getLWCoeffs(taxa_list, asfis_list)
#'
#' # Access length-weight coefficients
#' lw_coeffs <- results$lw
#'
#' # Access morphological data
#' morph_data <- results$ml
#' }
#' @keywords mining preprocessing
#' @export
#'
getLWCoeffs <- function(
  taxa_list = NULL,
  asfis_list = NULL,
  fb_version = "latest",
  slb_version = "latest",
  fao_areas = 51
) {
  logger::log_info(
    "Reading FishBase {fb_version} / SeaLifeBase {slb_version} for ",
    "{length(taxa_list)} taxa"
  )

  # 1. Load both databases
  taxa_data <- load_taxa_databases(
    fb_version = fb_version,
    slb_version = slb_version
  )
  logger::log_info(
    "Taxonomic backbone: {nrow(taxa_data$fishbase)} FishBase and ",
    "{nrow(taxa_data$sealifebase)} SeaLifeBase species"
  )

  # 2. Process species list
  species_list <- process_species_list(
    fao_codes = taxa_list,
    asfis_list = asfis_list
  )

  # 3. Match species in databases
  matched_species <- match_species_from_taxa(species_list, taxa_data)

  # 4. Get FAO areas and filter for area 51
  species_areas <- get_species_areas_batch(
    matched_species,
    fb_version = fb_version,
    slb_version = slb_version
  )
  species_areas_filtered <- species_areas %>%
    dplyr::filter(.data$area_code %in% fao_areas)

  # 5. Get length-weight parameters
  lw_data <- get_length_weight_batch(
    species_areas_filtered,
    include_morphology = TRUE,
    fb_version = fb_version,
    slb_version = slb_version
  )

  # 6. Format output
  lw <-
    lw_data$length_weight %>%
    dplyr::filter(!(.data$a3_code == "PEZ" & .data$type != "TL")) %>%
    dplyr::filter(
      !(.data$a3_code %in% c("OCZ", "OQC") & !.data$type == "ML")
    ) %>%
    dplyr::filter(.data$a > 0, !is.na(.data$a), !is.na(.data$b)) %>%
    dplyr::group_by(.data$a3_code) %>%
    dplyr::summarise(
      n = dplyr::n(),
      lw_a = exp(mean(log(.data$a))),
      lw_b = mean(.data$b),
      .groups = "drop"
    ) %>%
    dplyr::select(
      catch_taxon = "a3_code",
      "n",
      "lw_a",
      "lw_b"
    )

  ml <-
    lw_data$morphology %>%
    # FishBase populates CommonLength for only 10% of species, against 91% for
    # Length. Estimate the gaps from Length -- see common_length_ratio().
    dplyr::mutate(
      common_length = dplyr::coalesce(
        .data$CommonLength,
        common_length_ratio() * .data$Length
      )
    ) %>%
    dplyr::group_by(.data$a3_code) %>%
    dplyr::summarise(
      n = dplyr::n(),
      # min() on an all-NA group gives Inf, which became NaN below and silently
      # disabled the length alerts. NA keeps the absence visible.
      min_length = safe_min(.data$common_length),
      max_length_75 = stats::quantile(.data$Length, 0.95, na.rm = TRUE), #(make it more permissive)
      max_weightkg_75 = stats::quantile(.data$Weight, 0.75, na.rm = TRUE) /
        1000,
      .groups = "drop"
    ) %>%
    dplyr::mutate(
      max_length_75 = dplyr::case_when(
        .data$a3_code == "IAX" ~ 100,
        TRUE ~ .data$max_length_75
      ),
      min_length = .data$min_length - 0.75 * .data$min_length, #(make it more permissive, we don't know the exact value from fishbase)
      min_length = dplyr::case_when(
        .data$a3_code %in% c("OCZ", "OQC", "IAX") ~ 15,
        .data$a3_code == "PEZ" ~ 5,
        .data$a3_code == "COZ" ~ 2,
        TRUE ~ .data$min_length
      )
    ) |>
    dplyr::select(
      catch_taxon = "a3_code",
      "n",
      "min_length",
      "max_length_75",
      "max_weightkg_75"
    )

  return(list(lw = lw, ml = ml))
}

#' Fail the run when a taxon loses its length-weight coefficients
#'
#' @description
#' The FishBase read is a live network read of a remote parquet dataset. A new
#' release therefore reaches the pipeline the moment a container is rebuilt,
#' with no code change. Release 26.06 dissolved `Caesionidae` into `Lutjanidae`
#' and `Scaridae` into `Labridae`; both family names survive with **zero
#' species** in them, so any taxon whose reference name is one of those families
#' expands to nothing and gets no coefficients.
#'
#' Nothing fails on its own when that happens: [calculate_catch()] left-joins
#' the coefficients, so a taxon with no `(lw_a, lw_b)` pair yields `NA` weight,
#' and `NA` sums to zero. The taxon disappears from the portal and the run stays
#' green. This turns that silence into a failed job.
#'
#' @param taxa_list Character vector of FAO 3-alpha codes requested.
#' @param lw The `lw` table from [getLWCoeffs()], after any manually curated
#'   coefficients have been bound on.
#' @param exempt Codes that carry no coefficients today. This is a **baseline,
#'   not a whitelist**: it records the taxa that were already uncovered when the
#'   check was introduced (measured 2026-09-05 against FishBase 25.04 /
#'   SeaLifeBase 24.07, 120 of 134 codes resolving), so that any *new* loss fails
#'   the run. `CJX` and `PWT` are deliberately absent — they resolve at 25.04
#'   and are the two codes that break at 26.06, so a release move fails here.
#'   Shrinking this list is follow-up work; each group below is a separate fix.
#'
#'   \describe{
#'     \item{Not a taxon}{`MZZ` and `UNKN` are dropped by [get_fao_groups()]
#'       before the search runs; `UNK` is absent from ASFIS entirely and is
#'       rewritten to the fish group's most frequent taxon later in
#'       `preprocess_wf_surveys()`.}
#'     \item{Wrong reference name}{The ASFIS name does not describe the animal
#'       landed in Zanzibar, so the area 51 filter correctly removes it.
#'       `MAE` and `TAG` name species absent from FAO 51. `AHI`, `BFL` and
#'       `MAC` were the same kind of error and are now remapped on read in
#'       `reshape_catch_data()` -- to `BAF`, `TEI` and (for the sharks-and-rays
#'       group) `AQX` -- so they no longer reach this check.}
#'     \item{No published coefficients}{`GQT` (*Plectorhinchus gaterinus*)
#'       resolves to a species that does occur in FAO 51, but FishBase carries
#'       no length-weight pair for it in any length type. There is nothing to
#'       convert and nothing to alias; the measurement does not exist.}
#'     \item{Outdated synonym}{Names valid when ASFIS was written and since
#'       synonymised are now corrected in [taxa_search_aliases()], which
#'       recovered `CLP`, `ESR`, `LZV`, `OQC`, `RPO` and `VMX`. Only `CRA`
#'       (*Brachyura*) is left: an infraorder, a rank the backbone omits.}
#'     \item{No TL-type coefficients}{The species resolves and occurs in FAO 51
#'       and has published (a, b) pairs, but every one is fork, standard or
#'       another length type. [coasts::convert_lw_to_tl()] now restates most of
#'       these on a total-length basis, which recovered 15 codes including
#'       swordfish, the tunas, the marlins and the trevallies. The five left
#'       are `KAK`, `LHV`, `RMB`, `RTY` and `SSP`, which have no usable
#'       length-length fit to convert through.}
#'   }
#'
#' @section Porting: The check transfers unchanged, but `exempt` is
#'   country-specific. Run once against the country's own taxa list and record
#'   whatever it reports as the starting baseline.
#'
#' @return `lw`, invisibly.
#' @keywords mining preprocessing
#' @export
assert_taxa_coverage <- function(
  taxa_list,
  lw,
  exempt = c(
    # not a taxon
    "MZZ", "UNK", "UNKN",
    # wrong reference name for the animal landed here
    "MAE", "TAG",
    # in FAO 51, but FishBase carries no published length-weight pair at all
    "GQT",
    # infraorder Brachyura, a rank the backbone omits
    "CRA",
    # non-TL coefficients with no length-length fit to convert through
    "KAK", "LHV", "RMB", "RTY", "SSP"
  )
) {
  requested <- setdiff(unique(stats::na.omit(taxa_list)), exempt)
  missing <- setdiff(requested, lw$catch_taxon)

  if (length(missing) > 0) {
    stop(
      "No length-weight coefficients resolved for: ",
      paste(sort(missing), collapse = ", "),
      ". Every catch row of these taxa would weigh NA, which sums to zero. ",
      "Before changing any code, check which FishBase release was used: a new ",
      "release can empty a family without removing its name, which is how ",
      "Caesionidae and Scaridae broke in 26.06. The release is pinned in ",
      "inst/config.yml under metadata:fishbase, and rfishbase is pinned to ",
      "5.0.1 in both Dockerfiles for the same reason.",
      call. = FALSE
    )
  }

  logger::log_info(
    "Length-weight coefficients resolved for {nrow(lw)} taxa from ",
    "{sum(lw$n, na.rm = TRUE)} published records"
  )
  invisible(lw)
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
#' @keywords mining preprocessing
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


#' Load Taxa Data from FishBase and SeaLifeBase
#'
#' @description
#' Retrieves taxonomic data from both FishBase and SeaLifeBase databases in a single function call.
#' This is typically the first step in species identification and classification.
#'
#' @return A list with two elements:
#'   \itemize{
#'     \item fishbase: Data frame containing FishBase taxonomic data
#'     \item sealifebase: Data frame containing SeaLifeBase taxonomic data
#'   }
#' @examples
#' \dontrun{
#' taxa_data <- load_taxa_databases()
#' fishbase_taxa <- taxa_data$fishbase
#' sealifebase_taxa <- taxa_data$sealifebase
#' }
#' @param fb_version FishBase release to read, e.g. `"25.04"`. `"latest"` lets
#'   the installed `rfishbase` choose, which is what broke the pipeline.
#' @param slb_version SeaLifeBase release to read, e.g. `"24.07"`.
#' @keywords mining preprocessing
#' @export
load_taxa_databases <- function(
  fb_version = "latest",
  slb_version = "latest"
) {
  list(
    fishbase = rfishbase::load_taxa(
      server = "fishbase",
      version = fb_version
    ),
    sealifebase = rfishbase::load_taxa(
      server = "sealifebase",
      version = slb_version
    )
  )
}

#' Process Species List with Taxonomic Information
#'
#' @description
#' Processes a list of species by assigning database sources and taxonomic ranks.
#' Determines whether species should be looked up in FishBase or SeaLifeBase based
#' on their ISSCAAP group.
#'
#' @param fao_codes Vector of FAO 3-alpha codes
#' @param asfis_list ASFIS list data frame containing taxonomic information
#' @return A data frame with columns:
#'   \itemize{
#'     \item a3_code: FAO 3-alpha code
#'     \item scientific_name: Scientific name (cleaned)
#'     \item database: "fishbase" or "sealifebase"
#'     \item rank: Taxonomic rank ("Genus", "Family", "Order", "Species")
#'     \item ... (other taxonomic fields)
#'   }
#' @note
#' ISSCAAP groups 57, 45, 43, 42, 56 are assigned to SeaLifeBase;
#' all others to FishBase
#' @examples
#' \dontrun{
#' species_list <- process_species_list(c("TUN", "PEZ"), asfis_data)
#' }
#' @keywords mining preprocessing
#' @export
process_species_list <- function(fao_codes, asfis_list) {
  get_fao_groups(fao_codes = fao_codes, asfis_list = asfis_list) %>%
    dplyr::mutate(
      # ISSCAAP below 40 is finfish (FishBase), 40 and above everything else
      # (SeaLifeBase). The old list sent most invertebrates to the wrong one.
      database = dplyr::case_when(
        as.integer(.data$taxon_group) >= 40 ~ "sealifebase",
        TRUE ~ "fishbase"
      ),
      # Species must be tested before the family suffix, or a species like
      # `Haliotis midae` is read as a family and matches nothing.
      rank = dplyr::case_when(
        grepl(" spp$", .data$scientific_name) ~ "Genus",
        grepl(" ", .data$scientific_name) &
          !grepl(" spp$|nei$", .data$scientific_name) ~ "Species",
        grepl("idae$", .data$scientific_name) ~ "Family",
        grepl("formes$", .data$scientific_name) ~ "Order",
        TRUE ~ NA_character_
      ),
      scientific_name = gsub(" spp$", "", .data$scientific_name)
    ) %>%
    # Some ASFIS names match nothing in the backbone; substitute one that does.
    apply_taxa_aliases()
}

#' Search names that override the ASFIS reference name
#'
#' @description
#' A few ASFIS reference names match nothing in the taxonomic backbone, so the
#' taxon is dropped, gets no coefficients, and every catch row of it weighs
#' `NA` -- which sums to zero. This table substitutes a name that does match.
#' Each entry is a correction to the *reference data*, not to FishBase.
#'
#' @details
#' \describe{
#'   \item{`CLP`}{ASFIS calls it `Clupeidae`, but FishBase moved *Sardinella*,
#'     *Amblygaster* and *Herklotsichthys* to `Dorosomatidae` in 2022, leaving
#'     `Clupeidae` with 15 temperate species and **none** in FAO area 51. The
#'     dagaa landed in Zanzibar are Dorosomatidae. Searching the family rather
#'     than the three genera makes no practical difference -- 31 g against
#'     28 g for a 15 cm fish -- and matches the row already in Timor's
#'     `taxa_search_aliases()`, so the pipelines agree.}
#'   \item{Synonyms}{`ESR`, `RPO`, `LZV`, `OQC` carry names that were valid
#'     when ASFIS was written and have since been synonymised. `VMX` is
#'     *Valamugil*, a genus the backbone no longer carries at all; its species
#'     were split across *Osteomugil* and *Moolgarda* (6 species each at
#'     release 25.04), so both are searched. *Crenimugil* also absorbed some
#'     but carries 0 species in the backbone, so listing it would only produce
#'     a standing unmatched-name warning.}
#' }
#'
#' `CRA` ("marine crabs nei", *Brachyura*) is deliberately absent. Brachyura is
#' an infraorder and SeaLifeBase carries no rank between order *Decapoda* and
#' family, so there is nothing to alias it to without deciding which crab
#' families Zanzibar actually lands. That is a local question, not a lookup.
#'
#' @section Porting: Country-specific. The mechanism transfers unchanged; the
#'   rows do not. Rebuild the table for each country's own taxa list.
#'
#' @return A tibble of `a3_code`, `scientific_name` and `rank`. Several rows
#'   may share an `a3_code`; all of them are searched.
#' @keywords mining preprocessing
#' @export
taxa_search_aliases <- function() {
  dplyr::tribble(
    ~a3_code,
    ~scientific_name,
    ~rank,
    "CLP",
    "Dorosomatidae",
    "Family",
    "ESR",
    "Stolephorus commersonnii",
    "Species",
    "RPO",
    "Parupeneus macronemus",
    "Species",
    "LZV",
    "Ellochelon vaigiensis",
    "Species",
    "OQC",
    "Octopus cyanea",
    "Species",
    "VMX",
    "Osteomugil",
    "Genus",
    "VMX",
    "Moolgarda",
    "Genus"
  )
}

#' Apply the search-name aliases to a processed species list
#'
#' @description
#' Replaces the ASFIS name and rank for any code in [taxa_search_aliases()],
#' leaving every other row untouched. A code with several aliases expands to
#' one row per alias, so all of them are searched and their coefficients
#' pooled.
#'
#' `rank` is taken from the table rather than re-derived from the name: the
#' suffix rules in [process_species_list()] cannot recognise a bare genus like
#' *Osteomugil*, which has no space and no `-idae` or `-formes` ending.
#'
#' @param species_list Output of [process_species_list()] before aliasing.
#' @param aliases Alias table, defaulting to [taxa_search_aliases()].
#' @return `species_list` with aliased rows substituted.
#' @keywords mining preprocessing
#' @export
apply_taxa_aliases <- function(species_list, aliases = taxa_search_aliases()) {
  if (is.null(aliases) || nrow(aliases) == 0) {
    return(species_list)
  }

  hit <- intersect(species_list$a3_code, aliases$a3_code)
  if (length(hit) == 0) {
    return(species_list)
  }

  aliased <- species_list %>%
    dplyr::filter(.data$a3_code %in% hit) %>%
    dplyr::select(-"scientific_name", -"rank") %>%
    dplyr::inner_join(aliases, by = "a3_code", relationship = "many-to-many")

  logger::log_info(
    "Applied search-name aliases for {length(hit)} taxa: ",
    "{paste(sort(hit), collapse = ', ')}"
  )

  dplyr::bind_rows(
    species_list %>% dplyr::filter(!.data$a3_code %in% hit),
    aliased
  )
}

#' Match Species from Taxa Databases
#'
#' @description
#' Matches species between FAO codes and database records, handling different taxonomic
#' levels (species, genus, family, order) appropriately.
#'
#' @param species_list Processed species list from process_species_list()
#' @param taxa_data Taxa data from load_taxa_databases()
#' @return A data frame with columns:
#'   \itemize{
#'     \item a3_code: FAO 3-alpha code
#'     \item species: Scientific name
#'     \item database: Source database
#'   }
#' @examples
#' \dontrun{
#' taxa_data <- load_taxa_databases()
#' species_list <- process_species_list(fao_codes, asfis)
#' matches <- match_species_from_taxa(species_list, taxa_data)
#' }
#' @keywords mining preprocessing
#' @export
match_species_from_taxa <- function(species_list, taxa_data) {
  matches <- list()
  unmatched <- character(0)

  for (i in 1:nrow(species_list)) {
    row <- species_list[i, ]
    taxa <- taxa_data[[row$database]]

    matched_species <- switch(
      row$rank,
      "Genus" = taxa %>% dplyr::filter(.data$Genus == row$scientific_name),
      "Family" = taxa %>% dplyr::filter(.data$Family == row$scientific_name),
      "Order" = taxa %>% dplyr::filter(.data$Order == row$scientific_name),
      "Species" = taxa %>% dplyr::filter(.data$Species == row$scientific_name),
      NULL
    )

    if (!is.null(matched_species) && nrow(matched_species) > 0) {
      matches[[i]] <- matched_species %>%
        dplyr::mutate(
          a3_code = row$a3_code,
          original_rank = row$rank,
          original_name = row$scientific_name,
          database = row$database
        )
    } else {
      # A name matching nothing is dropped here and its catch then weighs NA,
      # which sums to zero. Log it rather than fail silently.
      unmatched <- c(
        unmatched,
        sprintf(
          "%s (%s, rank %s, %s)",
          row$a3_code,
          row$scientific_name,
          if (is.na(row$rank)) "unknown" else row$rank,
          row$database
        )
      )
    }
  }

  if (length(unmatched) > 0) {
    logger::log_warn(
      "{length(unmatched)} taxa matched no species in the taxonomic backbone: ",
      "{paste(sort(unmatched), collapse = '; ')}"
    )
  }

  dplyr::bind_rows(matches) %>%
    dplyr::select(
      "a3_code",
      species = "Species",
      "database"
      # original_rank,
      # original_name
    ) %>%
    dplyr::distinct()
}


#' Get FAO Areas for Species (Batch Version)
#'
#' @description
#' Efficiently retrieves FAO areas for multiple species by processing them in batches
#' by database source, reducing API calls and processing time.
#'
#' @param matched_species Data frame from match_species_from_taxa()
#' @param fb_version FishBase release to read, e.g. `"25.04"`. Pinned in
#'   `inst/config.yml` under `metadata:fishbase`; `"latest"` is unsafe.
#' @param slb_version SeaLifeBase release to read, e.g. `"24.07"`.
#' @return A data frame with columns:
#'   \itemize{
#'     \item a3_code: FAO 3-alpha code
#'     \item species: Scientific name
#'     \item area_code: FAO area code
#'     \item database: Source database
#'   }
#' @examples
#' \dontrun{
#' species_areas <- get_species_areas_batch(matched_species)
#' # Filter for specific FAO area
#' area_51_species <- species_areas %>%
#'   dplyr::filter(area_code == 51)
#' }
#' @keywords mining preprocessing
#' @export
get_species_areas_batch <- function(
  matched_species,
  fb_version = "latest",
  slb_version = "latest"
) {
  fishbase_species <- matched_species %>%
    dplyr::filter(.data$database == "fishbase") %>%
    dplyr::pull(.data$species)

  sealifebase_species <- matched_species %>%
    dplyr::filter(.data$database == "sealifebase") %>%
    dplyr::pull(.data$species)

  areas_fishbase <- if (length(fishbase_species) > 0) {
    rfishbase::faoareas(
      fishbase_species,
      fields = "AreaCode",
      server = "fishbase",
      version = fb_version
    ) %>%
      dplyr::mutate(database = "fishbase")
  }

  areas_sealifebase <- if (length(sealifebase_species) > 0) {
    rfishbase::faoareas(
      sealifebase_species,
      fields = "AreaCode",
      server = "sealifebase",
      version = slb_version
    ) %>%
      dplyr::mutate(database = "sealifebase")
  }

  dplyr::bind_rows(areas_fishbase, areas_sealifebase) %>%
    dplyr::left_join(
      matched_species,
      by = c("Species" = "species", "database")
    ) %>%
    dplyr::select(
      .data$a3_code,
      species = "Species",
      area_code = "AreaCode",
      .data$database
    ) %>%
    dplyr::distinct()
}


#' Get Length-Weight and Morphological Parameters for Species (Batch Version)
#'
#' @description
#' Retrieves length-weight relationship parameters and optional morphological data
#' for multiple species efficiently by processing them in batches. Handles both
#' fish and non-fish species appropriately.
#'
#' @param species_areas_filtered Data frame with filtered species
#' @param include_morphology Logical, whether to include morphological data (Length,
#'   CommonLength, Weight). Default is FALSE.
#' @param fb_version FishBase release to read, e.g. `"25.04"`. Pinned in
#'   `inst/config.yml` under `metadata:fishbase`; `"latest"` is unsafe.
#' @param slb_version SeaLifeBase release to read, e.g. `"24.07"`.
#' @return If include_morphology is FALSE (default), a data frame with columns:
#'   \itemize{
#'     \item a3_code: FAO 3-alpha code
#'     \item species: Scientific name
#'     \item area_code: FAO area code
#'     \item database: Source database
#'     \item type: Measurement type (e.g., "TL" for total length)
#'     \item a: Length-weight parameter a
#'     \item b: Length-weight parameter b
#'   }
#'
#'   If include_morphology is TRUE, a list with two elements:
#'   \itemize{
#'     \item length_weight: Data frame as described above
#'     \item morphology: Data frame with columns:
#'       \itemize{
#'         \item a3_code: FAO 3-alpha code
#'         \item species: Scientific name
#'         \item area_code: FAO area code
#'         \item database: Source database
#'         \item Length: Maximum recorded length
#'         \item CommonLength: Common length
#'         \item Weight: Maximum weight
#'       }
#'   }
#'
#' @note
#' - For FishBase species, only total length (TL) measurements are used
#' - Questionable estimates (EsQ = "yes") are excluded
#' @examples
#' \dontrun{
#' # Get just length-weight parameters
#' lw_data <- get_length_weight_batch(species_areas_filtered)
#'
#' # Get both length-weight and morphological data
#' results <- get_length_weight_batch(species_areas_filtered, include_morphology = TRUE)
#' lw_data <- results$length_weight
#' morph_data <- results$morphology
#' }
#' @keywords mining preprocessing
#' @export
#'
get_length_weight_batch <- function(
  species_areas_filtered,
  include_morphology = FALSE,
  fb_version = "latest",
  slb_version = "latest"
) {
  fishbase_species <- species_areas_filtered %>%
    dplyr::filter(.data$database == "fishbase") %>%
    dplyr::pull(.data$species)

  sealifebase_species <- species_areas_filtered %>%
    dplyr::filter(.data$database == "sealifebase") %>%
    dplyr::pull(.data$species)

  # Get length-weight parameters
  lw_fishbase <- if (length(fishbase_species) > 0) {
    rfishbase::length_weight(
      fishbase_species,
      fields = c("Species", "SpecCode", "Type", "EsQ", "a", "b"),
      server = "fishbase",
      version = fb_version
    ) %>%
      dplyr::mutate(database = "fishbase")
  }

  lw_sealifebase <- if (length(sealifebase_species) > 0) {
    rfishbase::length_weight(
      sealifebase_species,
      fields = c("Species", "SpecCode", "Type", "EsQ", "a", "b"),
      server = "sealifebase",
      version = slb_version
    ) %>%
      dplyr::mutate(database = "sealifebase")
  }

  lw_all <- dplyr::bind_rows(lw_fishbase, lw_sealifebase) %>%
    dplyr::left_join(
      species_areas_filtered,
      by = c("Species" = "species", "database")
    ) %>%
    dplyr::filter(is.na(.data$EsQ) | tolower(.data$EsQ) != "yes")

  # Pairs already on the basis the surveys measure. SeaLifeBase is left as it
  # was, and the type rules in getLWCoeffs() still apply to it.
  lw_native <- lw_all %>%
    dplyr::filter(
      (.data$database == "fishbase" & .data$Type == "TL") |
        .data$database == "sealifebase"
    )

  # Taxa whose every FishBase pair uses another length type would weigh NA.
  # Restate those on a TL basis; taxa that already resolved are untouched.
  uncovered <- setdiff(
    unique(lw_all$a3_code[lw_all$database == "fishbase"]),
    unique(lw_native$a3_code)
  )

  lw_converted <- if (length(uncovered) > 0) {
    candidates <- lw_all %>%
      dplyr::filter(
        .data$database == "fishbase",
        .data$a3_code %in% uncovered,
        .data$Type != "TL"
      )
    # The restatement lives in coasts, which takes the POPLL rows as published
    # and reduces them to one ratio per species and type itself. It passes
    # unconvertible rows through unchanged, so keep only what became TL.
    conversions <- rfishbase::length_length(
      unique(candidates$Species),
      fields = c("Species", "Length1", "Length2", "a", "b"),
      server = "fishbase",
      version = fb_version
    )
    converted <- if (is.null(conversions) || nrow(conversions) == 0) {
      candidates[0, ]
    } else {
      coasts::convert_lw_to_tl(
        dplyr::rename(
          candidates,
          species_found = "Species",
          server = "database"
        ),
        dplyr::transmute(
          conversions,
          species_found = .data$Species,
          server = "fishbase",
          .data$Length1,
          .data$Length2,
          aL = .data$a,
          bL = .data$b
        )
      ) %>%
        dplyr::filter(.data$Type == "TL") %>%
        dplyr::rename(Species = "species_found", database = "server")
    }
    if (nrow(converted) > 0) {
      logger::log_info(
        "Restated {nrow(converted)} length-weight pairs on a total-length ",
        "basis for {dplyr::n_distinct(converted$a3_code)} taxa that had none: ",
        "{paste(sort(unique(converted$a3_code)), collapse = ', ')}"
      )
    }
    converted
  } else {
    NULL
  }

  lw_data <- dplyr::bind_rows(lw_native, lw_converted) %>%
    dplyr::select(
      .data$a3_code,
      species = "Species",
      .data$area_code,
      .data$database,
      type = "Type",
      .data$a,
      .data$b
    ) %>%
    dplyr::distinct()

  # Get morphological information if requested
  if (include_morphology) {
    morph_fishbase <- if (length(fishbase_species) > 0) {
      rfishbase::species(
        fishbase_species,
        fields = c("Species", "SpecCode", "Length", "CommonLength", "Weight"),
        server = "fishbase",
        version = fb_version
      ) %>%
        dplyr::mutate(database = "fishbase")
    }

    morph_sealifebase <- if (length(sealifebase_species) > 0) {
      rfishbase::species(
        sealifebase_species,
        fields = c("Species", "SpecCode", "Length", "CommonLength", "Weight"),
        server = "sealifebase",
        version = slb_version
      ) %>%
        dplyr::mutate(database = "sealifebase")
    }

    morph_data <- dplyr::bind_rows(morph_fishbase, morph_sealifebase) %>%
      dplyr::left_join(
        species_areas_filtered,
        by = c("Species" = "species", "database")
      ) %>%
      dplyr::select(
        .data$a3_code,
        species = "Species",
        .data$area_code,
        .data$database,
        .data$Length,
        .data$CommonLength,
        .data$Weight
      ) %>%
      dplyr::distinct()

    # Return a list with both datasets
    return(list(
      length_weight = lw_data,
      morphology = morph_data
    ))
  }

  # Return only length-weight data if morphology not requested
  return(lw_data)
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
    dplyr::mutate(
      species_list = stringr::str_split(.data$species_catch, pattern = " ")
    ) %>%
    tidyr::unnest(.data$species_list) %>%
    dplyr::mutate(
      species_list = stringr::str_replace(
        .data$species_list,
        pattern = "_",
        replacement = " "
      ),
      species_list = stringr::str_replace(
        .data$species_list,
        pattern = "_",
        replacement = " "
      ),
      words = stringi::stri_count_words(.data$species_list),
      genus_species = dplyr::case_when(
        .data$words == 3 ~ stringr::str_extract(
          .data$species_list,
          "\\S+\\s+\\S+$"
        ),
        TRUE ~ NA_character_
      ),
      species_list = ifelse(
        .data$words == 3,
        NA_character_,
        .data$species_list
      ),
      catch_group = dplyr::coalesce(.data$species_list, .data$genus_species),
      catch_group = stringr::str_replace(
        .data$catch_group,
        pattern = " spp.",
        replacement = ""
      ),
      catch_group = stringr::str_replace(
        .data$catch_group,
        pattern = " spp",
        replacement = ""
      ),
      catch_group = stringr::str_replace(
        .data$catch_group,
        pattern = "_spp",
        replacement = ""
      ),
      catch_group = ifelse(
        .data$catch_group == "acanthocybium solandiri",
        "acanthocybium solandri",
        .data$catch_group
      ),
      catch_group = ifelse(
        .data$catch_group == "panaeidae",
        "penaeidae",
        .data$catch_group
      ),
      catch_group = ifelse(
        .data$catch_group == "mulidae",
        "mullidae",
        .data$catch_group
      ),
      catch_group = ifelse(
        .data$catch_group == "casio xanthonotus",
        "caesio xanthonotus",
        .data$catch_group
      ),
    ) %>%
    dplyr::select(-c(.data$species_list, .data$genus_species, .data$words))

  groups_rank <-
    taxize::classification(
      unique(taxa_expanded$catch_group),
      db = "gbif",
      rows = 1
    ) %>%
    purrr::imap(
      ~ .x %>%
        dplyr::as_tibble() %>%
        dplyr::mutate(catch_group = .y)
    ) %>%
    dplyr::bind_rows() %>%
    tidyr::pivot_wider(
      id_cols = .data$catch_group,
      names_from = .data$rank,
      values_from = .data$name
    ) %>%
    dplyr::select(dplyr::everything(), -dplyr::any_of(c("class", "NA")))

  dplyr::left_join(taxa_expanded, groups_rank, by = "catch_group") |>
    dplyr::select(-c("kingdom", "phylum", "order"))
}
