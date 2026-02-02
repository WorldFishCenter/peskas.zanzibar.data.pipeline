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

#' Preprocess Catch Data for Both Survey Versions
#'
#' Processes catch data from WorldFish surveys, handling both version 1 and version 2
#' survey structures. Automatically detects survey version and applies appropriate
#' species field normalization and length group processing.
#'
#' @param data A data frame containing raw survey data with species groups
#' @param version Character string specifying version ("v1" or "v2"). If NULL,
#'   version will be auto-detected based on column patterns.
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

  if (version == "v1") {
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
          TRUE ~ .data$catch_taxon
        )
      )
  }
}
