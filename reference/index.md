# Package index

## Data Pipeline Workflow

Core functions that execute each step in the data pipeline, from data
ingestion through validation, analysis, and export to MongoDB and cloud
storage.

- [`export_api_raw()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/export_api_raw.md)
  : Export Raw API-Ready Trip Data
- [`export_api_validated()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/export_api_validated.md)
  : Export Validated API-Ready Trip Data
- [`export_validation_flags()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/export_validation_flags.md)
  : Export Validation Flags to MongoDB
- [`export_wf_data()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/export_wf_data.md)
  : Export WorldFish Summary Data to MongoDB
- [`get_validation_status()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/get_validation_status.md)
  : Get Validation Status from KoboToolbox
- [`ingest_wcs_surveys()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/ingest_wcs_surveys.md)
  : Ingest WCS Catch Survey Data
- [`ingest_wf_surveys()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/ingest_wf_surveys.md)
  : Ingest WF Catch Survey Data
- [`match_surveys_to_gps_trips()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/match_surveys_to_gps_trips.md)
  : Match Catch Surveys to GPS Trips
- [`merge_trips()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/merge_trips.md)
  : Merge Survey and GPS Trip Data
- [`preprocess_ba_surveys()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/preprocess_ba_surveys.md)
  : Pre-process Blue Alliance Surveys
- [`preprocess_wcs_surveys()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/preprocess_wcs_surveys.md)
  : Pre-process Zanzibar WCS Surveys
- [`preprocess_wf_surveys()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/preprocess_wf_surveys.md)
  : Pre-process and Combine WorldFish Surveys - Both Versions
- [`sync_validation_submissions()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/sync_validation_submissions.md)
  : Synchronize Validation Statuses with KoboToolbox
- [`update_validation_status()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/update_validation_status.md)
  : Update Validation Status in KoboToolbox
- [`validate_ba_surveys()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/validate_ba_surveys.md)
  : Validate Blue Alliance (BA) Surveys Data
- [`validate_wcs_surveys()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/validate_wcs_surveys.md)
  : Validate WCS Surveys Data
- [`validate_wf_surveys()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/validate_wf_surveys.md)
  : Validate worldfish Survey Data

## Data Ingestion

Functions for pulling data from external sources (KoboToolbox, Pelagic
Data Systems) and transforming it into standardized formats.

- [`ingest_wcs_surveys()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/ingest_wcs_surveys.md)
  : Ingest WCS Catch Survey Data
- [`ingest_wf_surveys()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/ingest_wf_surveys.md)
  : Ingest WF Catch Survey Data

## Cloud Storage Management

Functions for interacting with cloud storage providers (Google Cloud
Storage, MongoDB), uploading, downloading, and managing data files in
various formats.

## Data Preprocessing

Functions for cleaning, transforming, and structuring raw data into
standardized formats ready for analysis, including data nesting,
reshaping, and trip processing.

- [`calculate_catch()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/calculate_catch.md)
  : Calculate Catch Weight from Length-Weight Relationships or Bucket
  Measurements
- [`getLWCoeffs()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/getLWCoeffs.md)
  : Get Length-Weight Coefficients and Morphological Data for Species
- [`get_airtable_form_id()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/get_airtable_form_id.md)
  : Get Airtable Form ID from KoBoToolbox Asset ID
- [`get_fao_groups()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/get_fao_groups.md)
  : Extract and Format FAO Taxonomic Groups
- [`get_length_weight_batch()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/get_length_weight_batch.md)
  : Get Length-Weight and Morphological Parameters for Species (Batch
  Version)
- [`get_species_areas_batch()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/get_species_areas_batch.md)
  : Get FAO Areas for Species (Batch Version)
- [`load_taxa_databases()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/load_taxa_databases.md)
  : Load Taxa Data from FishBase and SeaLifeBase
- [`map_surveys()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/map_surveys.md)
  : Map Survey Labels to Standardized Taxa, Gear, and Vessel Names
- [`match_species_from_taxa()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/match_species_from_taxa.md)
  : Match Species from Taxa Databases
- [`preprocess_ba_surveys()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/preprocess_ba_surveys.md)
  : Pre-process Blue Alliance Surveys
- [`preprocess_wcs_surveys()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/preprocess_wcs_surveys.md)
  : Pre-process Zanzibar WCS Surveys
- [`preprocess_wf_surveys()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/preprocess_wf_surveys.md)
  : Pre-process and Combine WorldFish Surveys - Both Versions
- [`process_species_list()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/process_species_list.md)
  : Process Species List with Taxonomic Information
- [`reshape_catch_data()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/reshape_catch_data.md)
  : Reshape Catch Data with Length Groupings
- [`reshape_catch_data_v2()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/reshape_catch_data_v2.md)
  : Reshape Catch Data with Length Groupings - Version 2
- [`reshape_species_groups()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/reshape_species_groups.md)
  : Reshape Species Groups from Wide to Long Format

## Data Mining & Summarization

Functions for enriching fisheries data with scientific information,
taxonomic classification, biological parameters, and creating summary
datasets for analysis.

- [`calculate_catch()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/calculate_catch.md)
  : Calculate Catch Weight from Length-Weight Relationships or Bucket
  Measurements
- [`expand_taxa()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/expand_taxa.md)
  : Expand Taxonomic Vectors into a Data Frame
- [`getLWCoeffs()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/getLWCoeffs.md)
  : Get Length-Weight Coefficients and Morphological Data for Species
- [`get_fao_groups()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/get_fao_groups.md)
  : Extract and Format FAO Taxonomic Groups
- [`get_length_weight_batch()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/get_length_weight_batch.md)
  : Get Length-Weight and Morphological Parameters for Species (Batch
  Version)
- [`get_species_areas_batch()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/get_species_areas_batch.md)
  : Get FAO Areas for Species (Batch Version)
- [`load_taxa_databases()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/load_taxa_databases.md)
  : Load Taxa Data from FishBase and SeaLifeBase
- [`match_species_from_taxa()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/match_species_from_taxa.md)
  : Match Species from Taxa Databases
- [`process_species_list()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/process_species_list.md)
  : Process Species List with Taxonomic Information

## Data Modeling & Analysis

Functions for statistical modeling, fleet activity estimation, and
scaling sample-based GPS data to fleet-wide estimates using boat
registry information.

## Data Validation

Functions for validating fisheries data through quality checks,
statistical outlier detection, and applying domain-specific validation
rules.

- [`add_validation_flags()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/add_validation_flags.md)
  : Add validation flags to catch data
- [`aggregate_survey_data()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/aggregate_survey_data.md)
  : Aggregate survey data and calculate metrics
- [`calculate_catch_revenue()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/calculate_catch_revenue.md)
  : Calculate catch revenue from validated data
- [`export_validation_flags()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/export_validation_flags.md)
  : Export Validation Flags to MongoDB
- [`extract_trips_info()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/extract_trips_info.md)
  : Extract trip information from preprocessed surveys
- [`get_catch_bounds()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/get_catch_bounds.md)
  : Get catch bounds for survey data
- [`get_length_bounds()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/get_length_bounds.md)
  : Get length bounds for survey data
- [`get_validation_status()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/get_validation_status.md)
  : Get Validation Status from KoboToolbox
- [`process_catch_data()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/process_catch_data.md)
  : Process catch data from surveys
- [`sync_validation_submissions()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/sync_validation_submissions.md)
  : Synchronize Validation Statuses with KoboToolbox
- [`update_validation_status()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/update_validation_status.md)
  : Update Validation Status in KoboToolbox
- [`validate_ba_surveys()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/validate_ba_surveys.md)
  : Validate Blue Alliance (BA) Surveys Data
- [`validate_catches()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/validate_catches.md)
  : Validate catches using quality flags
- [`validate_prices()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/validate_prices.md)
  : Validate market prices
- [`validate_wcs_surveys()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/validate_wcs_surveys.md)
  : Validate WCS Surveys Data
- [`validate_wf_surveys()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/validate_wf_surveys.md)
  : Validate worldfish Survey Data

## Data Export & Visualization

Functions for exporting processed data to MongoDB collections, creating
geographic visualizations, and preparing data for portals and reporting.

- [`export_api_raw()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/export_api_raw.md)
  : Export Raw API-Ready Trip Data
- [`export_api_validated()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/export_api_validated.md)
  : Export Validated API-Ready Trip Data
- [`export_wf_data()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/export_wf_data.md)
  : Export WorldFish Summary Data to MongoDB
- [`kepler_mapper()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/kepler_mapper.md)
  : Generate a Kepler.gl map

## Pipeline Orchestration

High-level functions that orchestrate complete analysis pipelines,
combining multiple processing steps into integrated workflows.

## Helper Functions

Utility functions that support the main pipeline operations, providing
common data manipulation and processing capabilities.

- [`add_version()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/add_version.md)
  : Add timestamp and sha string to a file name
- [`get_airtable_form_id()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/get_airtable_form_id.md)
  : Get Airtable Form ID from KoBoToolbox Asset ID
- [`map_surveys()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/map_surveys.md)
  : Map Survey Labels to Standardized Taxa, Gear, and Vessel Names
- [`read_config()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/read_config.md)
  : Read configuration file

## Airtable Integration

Functions for interacting with Airtable API.

- [`airtable_to_df()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/airtable_to_df.md)
  : Get All Records from Airtable with Pagination
- [`get_writable_fields()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/get_writable_fields.md)
  : Get Writable Fields from Airtable Table
- [`update_airtable_record()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/update_airtable_record.md)
  : Update Single Airtable Record
- [`bulk_update_airtable()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/bulk_update_airtable.md)
  : Bulk Update Multiple Airtable Records
- [`df_to_airtable()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/df_to_airtable.md)
  : Create New Airtable Records
- [`device_sync()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/device_sync.md)
  : Sync Data with Airtable (Update + Create)
