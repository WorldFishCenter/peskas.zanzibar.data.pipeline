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

#' Reshape Catch Data with Length Groupings - Version 2
#'
#' Enhanced version of reshape_catch_data that handles both survey versions.
#' This function takes a data frame with species catch information and reshapes it into a
#' long format while properly handling nested length group information for both survey versions.
#'
#' @param df A data frame containing catch data with species groups and length information
#'
#' @return A data frame in long format with each row representing a species at a specific
#'         length range, or just species data if no length information is available
#' @export
#'
#' @details
#' The function auto-detects survey version based on column patterns:
#' - Version 1: Uses single 'species' field, length data nested within species groups
#' - Version 2: Uses multiple species fields (species_TL, species_RF, etc.),
#'   fish >100cm in separate repeated group
#'
#' For Version 2, species fields are normalized using coalesce, and the separate
#' length group structure for fish >100cm is properly handled.
#'
#' @keywords preprocessing
#'
#' @examples
#' \dontrun{
#' final_data <- reshape_catch_data_v2(catch_info)
#'
#' # Analyze counts by length range
#' final_data |>
#'   filter(!is.na(count)) |>
#'   group_by(species, length_range) |>
#'   summarize(total_count = sum(as.numeric(count), na.rm = TRUE))
#' }
reshape_catch_data_v2 <- function(df = NULL) {
  # Detect survey version based on column patterns
  has_multiple_species_fields <- any(grepl(
    "species_TL|species_RF|species_MC",
    names(df)
  ))
  has_separate_length_group <- any(grepl(
    "species_group/no_fish_by_length_group_100/",
    names(df)
  ))

  is_version_2 <- has_multiple_species_fields || has_separate_length_group

  # First, reshape species groups
  species_long <- reshape_species_groups(df)

  # Handle version-specific species field normalization
  if (is_version_2) {
    # Version 2: Normalize multiple species fields into single 'species' field
    species_long <- species_long |>
      dplyr::mutate(
        species = dplyr::coalesce(
          .data$species_TL,
          .data$species_RF,
          .data$species_MC,
          .data$species_SR,
          .data$species_LP,
          .data$species_SP
        )
      ) |>
      dplyr::select(
        -c(
          "species_TL",
          "species_RF",
          "species_MC",
          "species_SR",
          "species_LP",
          "species_SP"
        )
      ) |>
      dplyr::relocate("species", .after = "counting_method")
  }

  # Extract length group columns (Version 1 pattern)
  length_cols <- names(species_long)[grep(
    "no_fish_by_length_group/",
    names(species_long)
  )]

  # Store separate length group data for later processing (Version 2)
  separate_length_processed <- NULL
  if (is_version_2 && has_separate_length_group) {
    # Get separate length group columns for Version 2
    separate_length_cols <- names(df)[grep(
      "species_group/no_fish_by_length_group_100/",
      names(df)
    )]

    if (length(separate_length_cols) > 0) {
      # Process the separate length group data
      separate_length_data <- df |>
        dplyr::select("submission_id", dplyr::all_of(separate_length_cols)) |>
        tidyr::pivot_longer(
          cols = -"submission_id",
          names_to = "length_category",
          values_to = "count_value", # Use different name to avoid conflicts
          values_drop_na = TRUE
        ) |>
        dplyr::mutate(
          length_category_clean = stringr::str_remove(
            .data$length_category,
            "species_group/no_fish_by_length_group_100/"
          ),
          # Remove any trailing suffixes like ...32, ...38
          length_category_clean = stringr::str_remove(
            .data$length_category_clean,
            "\\.\\.\\..*$"
          ),
          # Extract actual length ranges or special categories
          length_range = dplyr::case_when(
            stringr::str_detect(
              .data$length_category_clean,
              "no_individuals_over100"
            ) ~
              "over100",
            stringr::str_detect(
              .data$length_category_clean,
              "fish_length_over100"
            ) ~
              "over100",
            TRUE ~ .data$length_category_clean
          ),
          # For fish_length_over100 entries, move the value to length_over and set count to 1
          length_over = dplyr::if_else(
            stringr::str_detect(
              .data$length_category_clean,
              "fish_length_over100"
            ),
            .data$count_value,
            NA_character_
          ),
          count = dplyr::if_else(
            stringr::str_detect(
              .data$length_category_clean,
              "fish_length_over100"
            ),
            "1",
            .data$count_value
          )
        ) |>
        dplyr::filter(
          !(.data$length_range == "over100" &
            is.na(.data$length_over) &
            is.na(.data$count))
        ) |>
        dplyr::select("submission_id", "length_range", "length_over", "count")

      # Prepare separate length data with species information for later combination
      if (nrow(separate_length_data) > 0) {
        # Create base species data for separate length entries
        base_species_cols <- intersect(
          c(
            "submission_id",
            "n_catch",
            "counting_method",
            "species",
            "fish_group",
            "catch_label",
            "photo",
            "other_species_name",
            "n_buckets",
            "weight_bucket"
          ),
          names(species_long)
        )

        base_species_data <- species_long |>
          dplyr::select(dplyr::all_of(base_species_cols)) |>
          dplyr::distinct() |>
          dplyr::filter(
            .data$submission_id %in% separate_length_data$submission_id
          )

        # Get max n_catch per submission_id to avoid conflicts
        max_n_catch <- species_long |>
          dplyr::group_by(.data$submission_id) |>
          dplyr::summarise(
            max_n_catch = max(.data$n_catch, na.rm = TRUE),
            .groups = "drop"
          )

        # Join separate length data with base species data
        separate_length_processed <- separate_length_data |>
          dplyr::left_join(base_species_data, by = "submission_id") |>
          dplyr::left_join(max_n_catch, by = "submission_id") |>
          # Create a new n_catch value for the separate length entries
          dplyr::group_by(.data$submission_id) |>
          dplyr::mutate(
            n_catch = .data$max_n_catch + dplyr::row_number()
          ) |>
          dplyr::ungroup() |>
          dplyr::select(-"max_n_catch")
      }
    }
  }

  # Create a base dataset without length columns
  base_data <- species_long %>%
    dplyr::select(-dplyr::any_of(length_cols))

  # If there are no length columns, just return the base data
  if (
    length(length_cols) == 0 && (!is_version_2 || !has_separate_length_group)
  ) {
    return(base_data)
  }

  # Check if there are any non-NA values in length columns (Version 1 style)
  has_length_data <- if (length(length_cols) > 0) {
    species_long %>%
      dplyr::select(dplyr::all_of(length_cols)) %>%
      dplyr::mutate(has_data = rowSums(!is.na(.)) > 0) %>%
      dplyr::pull(.data$has_data)
  } else {
    rep(FALSE, nrow(species_long))
  }

  # Check for Version 2 separate length data
  has_separate_length_data <- rep(FALSE, nrow(species_long))

  # Combine both length data indicators
  has_any_length_data <- has_length_data | has_separate_length_data

  # Split the data into rows with and without length data
  rows_with_length <- which(has_any_length_data)

  # If no rows have length data, return the base data
  if (length(rows_with_length) == 0) {
    return(base_data)
  }

  # Process rows with length data (Version 1 style)
  if (length(length_cols) > 0) {
    # Check if there are already count columns that would conflict
    data_for_pivot <- species_long[rows_with_length, ] %>%
      dplyr::filter(rowSums(is.na(.[length_cols])) != length(length_cols))

    # If there's already a 'count' column, rename it to avoid conflicts
    if ("count" %in% names(data_for_pivot)) {
      data_for_pivot <- data_for_pivot %>%
        dplyr::rename(count_existing = "count")
    }

    length_data_v1 <- data_for_pivot %>%
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
      dplyr::filter(
        !(.data$length_range == "over100" & is.na(.data$length_over))
      )
  } else {
    length_data_v1 <- data.frame()
  }

  # Use the separate length data processed earlier (Version 2)
  length_data_v2 <- if (!is.null(separate_length_processed)) {
    separate_length_processed
  } else {
    data.frame()
  }

  # Combine Version 1 and Version 2 length data
  length_data <- if (nrow(length_data_v1) > 0 && nrow(length_data_v2) > 0) {
    dplyr::bind_rows(length_data_v1, length_data_v2)
  } else if (nrow(length_data_v1) > 0) {
    length_data_v1
  } else if (nrow(length_data_v2) > 0) {
    length_data_v2
  } else {
    data.frame()
  }

  # Process rows without length data
  if (length(rows_with_length) < nrow(species_long)) {
    rows_without_length <- which(!has_any_length_data)
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

#' Preprocess General Survey Information
#'
#' Processes general survey information including trip details, fisher counts,
#' and survey metadata from WorldFish survey data.
#'
#' @param data A data frame containing raw survey data
#'
#' @return A data frame with processed general survey information including:
#'   submission_id, dates, location, gear, trip details, fisher counts, etc.
#'
#' @keywords internal
#' @export
preprocess_general <- function(data = NULL) {
  general_info <-
    data %>%
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
      dplyr::contains("fishing_days_week"),
      "boat_reg_no",
      "vessel_type",
      "propulsion_gear",
      "fuel_L",
      "has_PDS",
      "has_boat",
      dplyr::any_of(c("boat_name", "fisher_name")),
      "boat_reg_no",
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
        c(
          dplyr::contains("fishing_days_week"),
          "trip_duration",
          "catch_price",
          dplyr::ends_with("_fishers")
        ),
        ~ as.double(.x)
      )
    )
  general_info
}


#' Sanitize physical catch inputs
#'
#' Drops (sets to NA) values for `length`, `weight_bucket`, `n_buckets`, and
#' `individuals` that fall outside physically plausible bounds. Defensive
#' against unit errors (length in metres instead of cm, weight_bucket in
#' grams instead of kg), decimal-separator confusion ("25.000" parsed as
#' 25000 instead of 25.0), and obvious data-entry slips.
#'
#' Bounds are conservative — they remove only the unambiguously impossible.
#' Downstream validation applies stricter, taxon-aware checks; sanitization
#' here protects the preprocessed parquet against extreme outliers
#' (e.g. `length = 195000` cm collapsing a trip total to billions of kg
#' via `W proportional to L^b` with b approximately 3).
#'
#' Bounds:
#' \itemize{
#'   \item `length` in (0, 300] cm — no fish or octopus exceeds approximately 300 cm
#'   \item `weight_bucket` in (0, 100] kg — typical bucket is 5–30 kg
#'   \item `n_buckets` in (0, 200] — productive seine landings reach approximately 50
#'   \item `individuals` in (0, 10000] — large pelagic schools can be thousands
#' }
#'
#' Columns absent from the input are left untouched.
#'
#' @param df A data frame containing any subset of the columns above.
#' @return The input with out-of-range values replaced by NA.
#' @keywords internal
sanitize_catch_inputs <- function(df) {
  df |>
    dplyr::mutate(
      dplyr::across(
        dplyr::any_of("length"),
        ~ dplyr::if_else(.x > 0 & .x <= 300, .x, NA_real_)
      ),
      dplyr::across(
        dplyr::any_of("weight_bucket"),
        ~ dplyr::if_else(.x > 0 & .x <= 100, .x, NA_real_)
      ),
      dplyr::across(
        dplyr::any_of("n_buckets"),
        ~ dplyr::if_else(.x > 0 & .x <= 200, .x, NA_real_)
      ),
      dplyr::across(
        dplyr::any_of("individuals"),
        ~ dplyr::if_else(.x > 0 & .x <= 10000, .x, NA_real_)
      )
    )
}


#' Preprocess Catch Data for Both Survey Versions
#'
#' Processes catch data from WorldFish surveys, handling both version 1 and version 2
#' survey structures. Automatically detects survey version and applies appropriate
#' species field normalization and length group processing.
#'
#' @param data A data frame containing raw survey data with species groups
#' @param version Character string specifying version ("v1", "v2", or "v3").
#'   v2 and v3 share the same structure. If NULL, version will be auto-detected
#'   based on column patterns.
#'
#' @return A data frame with processed catch data including:
#'   submission_id, n_catch, count_method, catch_taxon, n_buckets, weight_bucket,
#'   individuals, length
#'
#' @details
#' The function uses `reshape_catch_data_v2()` internally which automatically handles:
#' - Version 1: Single species field, nested length groups
#' - Version 2: Multiple species fields (species_TL, species_RF, etc.),
#'   separate length group structure for fish >100cm
#'
#' @keywords internal
#' @export
preprocess_catch <- function(data = NULL, version = NULL) {
  catch_info <-
    data |>
    dplyr::select("submission_id", dplyr::starts_with("species_group")) |>
    reshape_catch_data_v2()

  result <- if (version == "v1") {
    catch_info |>
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
  } else {
    catch_info |>
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
        count_method = "counting_method",
        "fish_group",
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
          is.na(.data$catch_taxon) & .data$fish_group == "MZZ" ~ "MZZ",
          TRUE ~ .data$catch_taxon
        )
      )
  }

  sanitize_catch_inputs(result)
}

#### GLEANING ######

#' Sanitize Gleaning Catch Inputs
#'
#' Drops (sets to NA) values that fall outside physically plausible bounds.
#' Defensive against data-entry slips and unit/decimal-separator confusion
#' (e.g. a single 10 kg bucket recorded as `bucket_weight = 13000`, or a full
#' bucket count of `7000`). Bounds are deliberately conservative: they remove
#' only the unambiguously impossible. Stricter, taxon-aware checks belong
#' downstream — this only protects the reshaped output from extreme outliers.
#'
#' Bounds:
#' \itemize{
#'   \item `n_individuals` in 0-10000 — a bucket of small shells can hold
#'         thousands; counts above this are almost certainly errors. Recorded
#'         zeros are kept (a true "none in this size class").
#'   \item `unit_weight_kg` in 0-100 — nominal containers are 5–50 kg.
#'   \item `n_containers` in 0-200 — hand gleaning rarely exceeds a few
#'         full containers; very large counts are data slips.
#' }
#'
#' Columns absent from the input are left untouched.
#'
#' @param df A data frame containing any subset of the columns above.
#' @return The input with out-of-range values replaced by NA.
#' @keywords internal
#' @export
sanitize_gleaning_inputs <- function(df) {
  df |>
    dplyr::mutate(
      dplyr::across(
        dplyr::any_of("n_individuals"),
        ~ dplyr::if_else(.x >= 0 & .x <= 10000, .x, NA_real_)
      ),
      dplyr::across(
        dplyr::any_of("unit_weight_kg"),
        ~ dplyr::if_else(.x > 0 & .x <= 100, .x, NA_real_)
      ),
      dplyr::across(
        dplyr::any_of("n_containers"),
        ~ dplyr::if_else(.x >= 0 & .x <= 200, .x, NA_real_)
      )
    )
}


#' Reshape Gleaning Catch Data from Wide to Long Format
#'
#' Reshapes the `group_catch` section of the Zanzibar/Pemba intertidal gleaning
#' KoboToolbox survey into a tidy long format. Unlike the boat-fishery surveys
#' (see `reshape_catch_data_v2()`), `group_catch` is a *single* (non-repeated)
#' group per submission, so there is no `n_catch` index. The wide-to-long work
#' instead unpacks three layered structures:
#'
#' \enumerate{
#'   \item \strong{Counting method.} `counting_method` (`backet` / `plastic_bag`)
#'         populates two mutually exclusive sets of columns (`bucket_*` vs
#'         `plastic_*`). These are coalesced into one unified container block:
#'         `container_type`, `container_size`, `container_size_kg`,
#'         `catch_fraction`, `unit_weight_kg`, `n_containers`.
#'   \item \strong{Shell group + species.} `shell_group`
#'         (`bivalves` / `gastropod` / `both` / `others`) drives which species
#'         multi-selects (`Group_Bivalves`, `Group_Gastropod`) and which length
#'         block apply. Species stay as a space-separated code list per group
#'         (no per-species quantities exist in the instrument), with a derived
#'         `n_species` token count.
#'   \item \strong{Length frequency.} Individual counts by size class
#'         (`<5`, `5-15`, `>15` cm) are recorded once per shell GROUP, not per
#'         species. Each present group emits one row per size class.
#' }
#'
#' Output grain: one row per `submission_id` x `group` x `size_class`. A
#' submission recorded under `bivalves` or `gastropod` yields 3 rows; `both`
#' yields 6; `others` or a missing `shell_group` (no shell detail) is preserved
#' as a single context row with `group`/`size_class`/`n_individuals` = NA so no
#' submission is silently dropped. All three size classes are retained per
#' present group (NA = not recorded, 0 = a recorded zero) so the size-frequency
#' distribution stays explicit and complete.
#'
#' @param data A data frame of the gleaning survey export. Catch columns are
#'   expected with the raw Kobo prefixes (`group_catch/...`, including the
#'   nested `group_catch/group_length_gastropods/...` /
#'   `group_catch/group_length_bivalves/...`).
#'
#' @return A long data frame with one row per submission x shell group x size
#'   class (plus a single context row for submissions without shell detail).
#' @export
#'
#' @keywords workflow preprocessing
#' @examples
#' \dontrun{
#' gleaning_long <- reshape_gleaning_catch(gleaning)
#'
#' # Size-frequency by shell group across all submissions
#' gleaning_long |>
#'   dplyr::filter(!is.na(n_individuals)) |>
#'   dplyr::group_by(group, size_class) |>
#'   dplyr::summarise(total = sum(n_individuals), .groups = "drop")
#' }
reshape_gleaning_catch <- function(data = NULL) {
  # ---- 0. Strip the outer group prefix; keep the nested length-group prefix
  #         (group_length_*/...) so the two length blocks stay distinguishable.
  catch <- data |>
    dplyr::select(
      "submission_id",
      dplyr::any_of(c("catch_outcome", "survey_activity")),
      dplyr::starts_with("group_catch/")
    ) |>
    dplyr::rename_with(~ stringr::str_remove(., "^group_catch/"))

  # ---- 1. Unify the parallel bucket_* / plastic_* container fields ----------
  catch <- catch |>
    dplyr::mutate(
      container_type = dplyr::case_when(
        .data$counting_method == "backet" ~ "bucket",
        .data$counting_method == "plastic_bag" ~ "plastic_bag",
        TRUE ~ NA_character_
      ),
      container_size = dplyr::coalesce(.data$bucket_size, .data$plastic_size),
      # Nominal capacity parsed from the size code (kg).
      container_size_kg = dplyr::case_when(
        .data$container_size == "small_bucket_5kg" ~ 5,
        .data$container_size == "medium_bucket_10kg" ~ 10,
        .data$container_size == "large_bucket_20_kg" ~ 20,
        .data$container_size == "small_size_10_kg" ~ 10,
        .data$container_size == "medium_size_25kg" ~ 25,
        .data$container_size == "large_50kg" ~ 50,
        TRUE ~ NA_real_
      ),
      # half_of_the_bucket / half_of_plastic_bag -> "half"; full_* -> "full".
      catch_fraction = dplyr::coalesce(
        .data$bucket_catch_size,
        .data$plastic_catch_size
      ),
      catch_fraction = stringr::str_replace(
        .data$catch_fraction,
        "^(half|full)_.*$",
        "\\1"
      ),
      unit_weight_kg = dplyr::coalesce(
        as.double(.data$bucket_weight),
        as.double(.data$plastic_weight)
      ),
      n_containers = dplyr::coalesce(
        as.double(.data$full_bucket_number),
        as.double(.data$full_plastic_number)
      )
    )

  # Submission-level context carried onto every output row.
  context <- catch |>
    dplyr::select(
      "submission_id",
      dplyr::any_of(c("catch_outcome", "survey_activity")),
      "shell_group",
      "container_type",
      "container_size",
      "container_size_kg",
      "catch_fraction",
      "unit_weight_kg",
      "n_containers",
      others_catch = "others"
    )

  # ---- 2. Per-group length-frequency long table ----------------------------
  bivalve_long <- catch |>
    dplyr::filter(.data$shell_group %in% c("bivalves", "both")) |>
    dplyr::transmute(
      submission_id = .data$submission_id,
      group = "bivalves",
      species_codes = .data$Group_Bivalves,
      length_photo = .data$`group_length_bivalves/photo_001`,
      `<5` = as.double(.data$`group_length_bivalves/small_5_001`),
      `5-15` = as.double(.data$`group_length_bivalves/_5_15_001`),
      `>15` = as.double(.data$`group_length_bivalves/large_15_001`)
    )

  gastropod_long <- catch |>
    dplyr::filter(.data$shell_group %in% c("gastropod", "both")) |>
    dplyr::transmute(
      submission_id = .data$submission_id,
      group = "gastropod",
      species_codes = .data$Group_Gastropod,
      length_photo = .data$`group_length_gastropods/photo`,
      `<5` = as.double(.data$`group_length_gastropods/small_5`),
      `5-15` = as.double(.data$`group_length_gastropods/_5_15`),
      `>15` = as.double(.data$`group_length_gastropods/large_15`)
    )

  size_levels <- c("<5", "5-15", ">15")

  groups_long <- dplyr::bind_rows(bivalve_long, gastropod_long) |>
    tidyr::pivot_longer(
      cols = dplyr::all_of(size_levels),
      names_to = "size_class",
      values_to = "n_individuals"
      # NB: NA counts are intentionally kept so all three size classes stay
      # present per group (do not set values_drop_na = TRUE here).
    ) |>
    dplyr::mutate(
      size_class = factor(
        .data$size_class,
        levels = size_levels,
        ordered = TRUE
      ),
      # token count of the space-separated multi-select (NA stays NA).
      n_species = stringr::str_count(.data$species_codes, "\\S+")
    )

  # ---- 3. Submissions with no shell detail (shell_group 'others' or NA) -----
  #         Preserve them as a single context row.
  no_detail <- context |>
    dplyr::filter(!.data$submission_id %in% groups_long$submission_id) |>
    dplyr::transmute(
      submission_id = .data$submission_id,
      group = NA_character_,
      species_codes = NA_character_,
      length_photo = NA_character_,
      n_individuals = NA_real_,
      size_class = factor(NA_character_, levels = size_levels, ordered = TRUE),
      n_species = NA_integer_
    )

  # ---- 4. Assemble, attach context, sanitize, order ------------------------
  dplyr::bind_rows(groups_long, no_detail) |>
    dplyr::left_join(context, by = "submission_id") |>
    sanitize_gleaning_inputs() |>
    dplyr::select(
      "submission_id",
      dplyr::any_of(c("catch_outcome", "survey_activity")),
      "shell_group",
      "group",
      "species_codes",
      "n_species",
      "size_class",
      "n_individuals",
      "container_type",
      "container_size_kg",
      "catch_fraction",
      "unit_weight_kg",
      "n_containers",
      "others_catch",
      "length_photo"
    ) |>
    dplyr::arrange(.data$submission_id, .data$group, .data$size_class)
}
