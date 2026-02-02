# Changelog

## peskas.zanzibar.data.pipeline 4.2.0

### New Features

- **API Data Export Pipeline**: Added new
  [`export_api_raw()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/export_api_raw.md)
  function to export raw preprocessed survey data in API-friendly format
  - Exports raw/preprocessed trip data (before validation) to cloud
    storage
  - Part of a two-stage API export pipeline (raw and validated exports)
  - Transforms nested survey data into flat structure with standardized
    trip-level records
  - Generates unique trip IDs using xxhash64 algorithm
  - Integrates with Airtable metadata for form-specific asset lookups
  - Exports versioned parquet files to `zanzibar/raw/` path for external
    API consumption
  - Includes comprehensive output schema with 14 standardized fields
    (trip_id, landing_date, gear, catch metrics, etc.)
- **Airtable Integration**: New helper functions for managing Airtable
  metadata and form configurations
  - [`get_airtable_form_id()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/get_airtable_form_id.md):
    Retrieves Airtable record IDs from KoBoToolbox asset IDs
  - [`airtable_to_df()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/airtable_to_df.md):
    Downloads complete Airtable tables with automatic pagination
    handling
  - [`get_writable_fields()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/get_writable_fields.md):
    Identifies updatable fields in Airtable tables (excludes computed
    fields)
  - [`update_airtable_record()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/update_airtable_record.md):
    Updates individual records with field validation
  - [`bulk_update_airtable()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/bulk_update_airtable.md):
    Batch updates multiple records efficiently (up to 10 records per
    request)
  - [`device_sync()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/device_sync.md):
    Synchronizes GPS device metadata between Airtable and MongoDB

### Improvements

- **Configuration Enhancements**:
  - Added `api` configuration section for trip data exports with
    separate raw/validated paths
  - Configured cloud storage paths for API exports (zanzibar/raw,
    zanzibar/validated)
  - Added Airtable base ID and token configuration for metadata
    management
  - Enhanced `options_api` storage configuration for peskas-coasts
    bucket
- **GitHub Actions Workflow**:
  - Added new `export-api-data` job to automated pipeline workflow
  - Integrated Airtable authentication with GitHub Secrets
    (AIRTABLE_TOKEN, AIRTABLE_BASE_ID_FRAME, AIRTABLE_BASE_ID_ASSETS)
  - Configured API export job to run after survey preprocessing step
  - Added production environment configuration for API data exports
- **Code Quality**:
  - Improved documentation with comprehensive roxygen2 comments for all
    new functions
  - Added detailed examples and cross-references in function
    documentation
  - Enhanced error handling and input validation in Airtable operations
  - Implemented proper cleanup of temporary local files after cloud
    uploads

## peskas.zanzibar.data.pipeline 4.1.1

### Major Changes

- **Streamlined Validation Workflow**: Replaced KoboToolbox API updates
  with direct MongoDB storage to improve performance.
  - New
    [`export_validation_flags()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/export_validation_flags.md)
    function exports validation flags directly to MongoDB
  - Validation status queries now only identify manually edited
    submissions, not update them
  - Disabled
    [`sync_validation_submissions()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/sync_validation_submissions.md)
    workflow steps in GitHub Actions
  - Significantly reduced pipeline execution time by avoiding slow
    KoboToolbox API calls

### Improvements

- **Validation System**:

  - Validation functions now preserve manual human approvals while
    updating system-generated statuses

- **Code Quality**:

  - Fixed SeaLifeBase API calls by pinning to version 24.07 to avoid
    server errors
  - Standardized function parameter formatting across validation and
    preprocessing modules

## peskas.zanzibar.data.pipeline 4.1.0

#### Major Changes

- **Integration of New KoBoToolbox Survey Form Version:** Added support
  for a new version of the WorldFish survey form (`wf_surveys_v2`)
  alongside the existing form (`wf_surveys_v1`). Data from both survey
  versions is now processed together in the preprocessing pipeline and
  handled properly throughout the validation workflow.

#### Improvements

- **Multi-Asset Validation Support:**
  - Updated validation system to query approval statuses from both
    survey form versions
  - Enhanced
    [`validate_wf_surveys()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/validate_wf_surveys.md)
    and
    [`sync_validation_submissions()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/sync_validation_submissions.md)
    to handle submissions from multiple KoBoToolbox assets
  - Ensured manually approved submissions from either form version are
    protected from automated flagging
- **Configuration Updates:**
  - Added configuration for the new survey form version with shared
    credentials
  - Cleaned up redundant configuration entries
  - Updated code references to use versioned asset configurations

#### Bug Fixes

- Fixed validation logic that was only checking approval status from the
  original survey form, causing incorrect flagging of valid submissions
  from the new form version

## peskas.zanzibar.data.pipeline 4.0.0

#### Major Changes

- **Fleet Activity Analysis Pipeline:** Introduced a comprehensive
  pipeline for estimating and analyzing fishing fleet activity using
  GPS-tracked boats and boat registry data. This includes new functions
  for preparing boat registries, processing trip data, calculating
  monthly trip statistics, estimating fleet-wide activity, and
  calculating district-level total catch and revenue.
- **New Modeling and Summarization Functions:**
  - [`prepare_boat_registry()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/prepare_boat_registry.md):
    Summarizes boat registry data by district.
  - [`process_trip_data()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/process_trip_data.md):
    Processes trip data with district information and filters outliers.
  - [`calculate_monthly_trip_stats()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/calculate_monthly_trip_stats.md):
    Computes monthly fishing activity statistics by district.
  - [`estimate_fleet_activity()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/estimate_fleet_activity.md):
    Scales up sample-based trip statistics to fleet-wide estimates.
  - [`calculate_district_totals()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/calculate_district_totals.md):
    Combines fleet activity and catch data for district-level totals.
  - [`generate_fleet_analysis()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/generate_fleet_analysis.md):
    Orchestrates the full analysis pipeline and uploads results.
  - [`summarize_data()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/summarize_data.md):
    Generates and uploads summary datasets (monthly, taxa, district,
    gear, grid) for WorldFish survey data.
- **Enhanced Data Export and Integration:**
  - [`export_wf_data()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/export_wf_data.md):
    Exports summarized WorldFish survey data and modeled estimates to
    MongoDB, including new geographic regional summaries.
  - [`create_geos()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/create_geos.md):
    Generates geospatial regional summaries and exports as GeoJSON for
    spatial visualization.
- **Expanded Documentation:** New and updated Rd files for all major new
  functions, with improved examples and cross-references.

#### Improvements

- **Consistent Time Series and Grouping:** All summary tables (taxa,
  districts, gear) now include a ‘date’ (monthly) column and are grouped
  by month, with missing months filled as NA for consistent time series
  exports.
- **Parallel Processing:** Improved use of parallelization (via `future`
  and `furrr`) for validation and summarization steps, enhancing
  performance for large datasets.
- **Data Quality and Validation:**
  - Enhanced filtering and validation of survey data before
    summarization and export.
  - Improved handling of flagged/invalid submissions.

#### Infrastructure & Workflow

- **Configuration and Documentation:** Updated configuration files and
  documentation to support new modeling and export workflows.
- **Workflow Automation:** Updates to GitHub Actions and Docker
  configuration to support the expanded pipeline.

## peskas.zanzibar.data.pipeline 3.3.0

#### Major Changes

- All summary tables (taxa, districts, gear) now include a ‘date’
  (monthly) column and are grouped by month. Missing months are filled
  as NA for consistent time series exports.

#### Validation Updates

- The maximum number of individuals per catch is now 200 (was 80).
- Validation flag for ‘number of fishers too high’ is now triggered at
  \>100 (was \>70) for non-ring nets.
- Documentation updated to reflect new validation thresholds.

#### Code Quality

- Improved code formatting and clarity in validation functions and
  documentation.

## peskas.zanzibar.data.pipeline 3.2.0

#### Improvements

- Export standardized fishery metrics for general usage with other
  peskas datasets

## peskas.zanzibar.data.pipeline 3.1.0

#### New Features

- Added
  [`create_geos()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/create_geos.md)
  function to generate geospatial regional summaries of fishery data
- Added support for GPS track data visualization through new grid-based
  analytics
- Added
  [`generate_track_summaries()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/generate_track_summaries.md)
  function to process GPS tracks into 1km grid cells

#### Improvements

- Integrated spatial data with dashboard exports through new GeoJSON
  support
- Enhanced code readability and formatting throughout codebase
- Added Region-based aggregation of fishery metrics (CPUE, RPUE, price
  per kg)
- Added grid summaries to MongoDB exports for dashboard integration

## peskas.zanzibar.data.pipeline 3.0.0

#### New Features

- Generate clean dataframes to export to dashboard. These include
  districts and taxa summaries and monthly regional time series of the
  main fishery indicators, CPUE, RPUE and price per kg

## peskas.zanzibar.data.pipeline 2.6.0

#### New Features

- Added new `sync-validation` job to GitHub Actions workflow for
  synchronizing survey validation submissions

#### Improvements

- Implemented error handling in `getLWCoeffs` to fallback on local data
  if Rfishbase retrieval fails
- Enhanced code readability by restructuring functions and adding line
  breaks
- Updated documentation for `get_preprocessed_surveys` and
  `get_validated_surveys` functions

## peskas.zanzibar.data.pipeline 2.5.0

#### Major Changes

- Enhanced validation workflow with KoboToolbox integration:
  - Added
    [`update_validation_status()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/update_validation_status.md)
    function to update submission status via API
  - Added
    [`sync_validation_submissions()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/sync_validation_submissions.md)
    for parallel processing of validation flags
  - Updated Kobo URL endpoint from kf.kobotoolbox.org to
    eu.kobotoolbox.org

#### New Features

- Implemented parallel processing for validation operations using
  future/furrr packages
- Added progress reporting during validation operations via progressr
  package
- Enhanced validation status synchronization between local system and
  KoboToolbox

#### Improvements

- Updated data preprocessing to handle flying fish estimates and taxa
  corrections (TUN→TUS, SKH→CVX)
- Updated export workflow to use validation status instead of flags for
  data filtering
- Added taxa information to catch export data
- Added Zanzibar SSF report template with visualization examples
- Improved package documentation structure with better categorization

## peskas.zanzibar.data.pipeline 2.4.0

#### Major Changes

- Implemented support for multiple survey data sources:
  - Refactored
    [`get_validated_surveys()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/get_validated_surveys.md)
    to handle WCS, WF, and BA sources
  - Added source parameter to specify which datasets to retrieve
  - Improved handling of data sources with different column structures

#### New Features

- Added
  [`export_wf_data()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/export_wf_data.md)
  function for WorldFish-specific data export
- Enhanced validation with additional composite metrics:
  - Price per kg validation
  - CPUE (Catch Per Unit Effort) validation
  - RPUE (Revenue Per Unit Effort) validation

#### Improvements

- Added min_length parameter for better length validation thresholds
- Updated LW coefficient filtering logic in model-taxa.R
- Enhanced alert flag handling with combined flags from different
  validation steps
- Improved catch price and catch weight handling for zero-catch outcomes
- Enhanced data preprocessing with better field type conversion

#### Bug Fixes

- Fixed issue with catch_price field type in WF survey preprocessing
- Corrected filter condition for taxa coefficients

## peskas.zanzibar.data.pipeline 2.3.0

#### Major Changes

- Enhanced KoboToolbox integration:
  - Implemented new validation status retrieval from KoboToolbox API
  - Updated validation workflow to incorporate submission validation
    status
  - Improved data validation process through direct API integration

#### New Features

- New KoboToolbox interaction functions:
  - [`get_validation_status()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/get_validation_status.md):
    Retrieves submission validation status from KoboToolbox API

#### Improvements

- Modified configuration files to support new KoboToolbox API token
- Added new environment variable for KoboToolbox API authentication
- Enhanced validation workflow with integrated validation status checks

## peskas.zanzibar.data.pipeline 2.2.0

#### Major Changes

- Completely restructured taxonomic data processing:
  - Introduced new modular functions for taxa handling in model-taxa.R
  - Added efficient batch processing for species matching
  - Implemented optimized FAO area retrieval system
  - Streamlined length-weight coefficient calculations
  - Enhanced integration with FishBase and SeaLifeBase

#### New Features

- New taxonomic processing functions:
  - [`load_taxa_databases()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/load_taxa_databases.md):
    Unified database loading from FishBase and SeaLifeBase
  - [`process_species_list()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/process_species_list.md):
    Enhanced species list processing with taxonomic ranks
  - [`match_species_from_taxa()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/match_species_from_taxa.md):
    Improved species matching across databases
  - [`get_species_areas_batch()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/get_species_areas_batch.md):
    Efficient FAO area retrieval
  - [`get_length_weight_batch()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/get_length_weight_batch.md):
    Optimized length-weight parameter retrieval

#### Improvements

- Enhanced performance through batch processing
- Reduced API calls to external databases
- Better error handling and input validation
- More comprehensive documentation
- Improved code organization and modularity

#### Deprecations

- Removed legacy taxonomic processing functions
- Deprecated redundant species matching methods
- Removed outdated data transformation utilities

#### Documentation

- Added detailed function documentation
- Updated vignettes with new workflows
- Improved code examples
- Enhanced README with new features

## peskas.zanzibar.data.pipeline 2.1.0

##### Major Changes

- Enhanced taxonomic and catch data processing capabilities:
  - Added comprehensive functions for species and catch data processing
  - Implemented length-weight coefficient retrieval from FishBase and
    SeaLifeBase
  - Created functions for calculating catch weights using multiple
    methods
  - Added new data reshaping utilities for species and catch information
- Extended Wild Fishing (WF) survey validation with detailed quality
  checks
- Updated cloud storage and data download/upload functions

## peskas.zanzibar.data.pipeline 2.0.0

##### Major Changes

- Complete overhaul of the data pipeline architecture
- Added PDS (Pelagic Data Systems) integration:
  - New trip ingestion and preprocessing functionality
  - GPS track data processing capabilities
- Implemented MongoDB export and storage functions
- Removed renv dependency management for improved reliability
- Updated Docker configuration for more robust builds

##### New Features

- Enhanced validation system for survey data
- Added new data processing steps:
  - GPS track preprocessing
  - Catch data validation
  - Length measurements validation
  - Market data validation
- Flexible data export capabilities
- Improved GitHub Actions workflow with additional processing steps

##### Infrastructure Updates

- Streamlined package dependencies
- Updated build and deployment processes
- Enhanced data storage and retrieval mechanisms

## peskas.zanzibar.data.pipeline 1.0.0

##### Improvements

- All the functions are now documented and indexed according to keywords
- Thin out the R folder gathering functions by modules

##### Changes

- Move to parquet format rather than CSV/RDS

## peskas.zanzibar.data.pipeline 0.2.0

##### New features

Added the validation step and updated the preprocessing step for wcs
kobo surveys data, see
[`preprocess_wcs_surveys()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/preprocess_wcs_surveys.md)
and
[`validate_wcs_surveys()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/validate_wcs_surveys.md)
functions. Currently, validation for catch weight, length and market
values are obtained using median absolute deviation method (MAD)
leveraging on the k parameters of the
[`univOutl::LocScaleB`](https://rdrr.io/pkg/univOutl/man/LocScaleB.html)
function.

In order to accurately spot any outliers, validation is performed based
on gear type and species.

**N.B. VALIDATION PARAMETERS ARE NOT YET TUNED**

##### Changes

No need to run the pipeline every two days, decreased not to every 4
days.

## peskas.zanzibar.data.pipeline 0.1.0

Drop parent repository code (peskas.timor.pipeline), add infrastructure
to download WCS survey data and upload it to cloud storage providers

#### New features

- The ingestion of WCS Zanzibar surveys is implemented in
  `ingest_wcs_surveys()`.
- The functions `retrieve_wcs_surveys()` downloads WCS Zanzibar surveys
  data

##### Changes

- Updated configuration management:
  - Moved configuration settings to inst/conf.yml
  - Improved configuration structure and organization
  - Enhanced configuration flexibility
