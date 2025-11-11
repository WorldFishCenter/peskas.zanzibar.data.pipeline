# Package index

## Data Pipeline Workflow

Core functions that execute each step in the data pipeline, from data
ingestion through validation, analysis, and export to MongoDB and cloud
storage.

- [`calculate_district_totals()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/calculate_district_totals.md)
  : Calculate District-Level Total Catch and Revenue
- [`calculate_monthly_trip_stats()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/calculate_monthly_trip_stats.md)
  : Calculate Monthly Trip Statistics by District
- [`estimate_fleet_activity()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/estimate_fleet_activity.md)
  : Estimate Fleet-Wide Activity from Sample Data
- [`export_wf_data()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/export_wf_data.md)
  : Export WorldFish Summary Data to MongoDB
- [`generate_fleet_analysis()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/generate_fleet_analysis.md)
  : Generate Complete Fleet Activity Analysis Pipeline
- [`get_validation_status()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/get_validation_status.md)
  : Get Validation Status from KoboToolbox
- [`ingest_pds_tracks()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/ingest_pds_tracks.md)
  : Ingest Pelagic Data Systems (PDS) Track Data
- [`ingest_pds_trips()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/ingest_pds_trips.md)
  : Ingest Pelagic Data Systems (PDS) Trip Data
- [`ingest_surveys()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/ingest_surveys.md)
  : Ingest WCS and WF Catch Survey Data
- [`prepare_boat_registry()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/prepare_boat_registry.md)
  : Prepare Boat Registry Data from Metadata
- [`preprocess_ba_surveys()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/preprocess_ba_surveys.md)
  : Pre-process Blue Alliance Surveys
- [`preprocess_pds_tracks()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/preprocess_pds_tracks.md)
  : Preprocess Pelagic Data Systems (PDS) Track Data
- [`preprocess_wcs_surveys()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/preprocess_wcs_surveys.md)
  : Pre-process Zanzibar WCS Surveys
- [`preprocess_wf_surveys()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/preprocess_wf_surveys.md)
  : Pre-process and Combine WorldFish Surveys - Both Versions
- [`process_trip_data()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/process_trip_data.md)
  : Process Trip Data with District Information
- [`summarize_data()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/summarize_data.md)
  : Summarize WorldFish Survey Data
- [`sync_validation_submissions()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/sync_validation_submissions.md)
  : Synchronize Validation Statuses with KoboToolbox
- [`update_validation_status()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/update_validation_status.md)
  : Update Validation Status in KoboToolbox
- [`validate_ba_surveys()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/validate_ba_surveys.md)
  : Validate Blue Alliance (BA) Surveys Data
- [`validate_wcs_surveys()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/validate_wcs_surveys.md)
  : Validate WCS Surveys Data
- [`validate_wf_surveys()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/validate_wf_surveys.md)
  : Validate Wild Fishing Survey Data

## Data Ingestion

Functions for pulling data from external sources (KoboToolbox, Pelagic
Data Systems) and transforming it into standardized formats.

- [`get_trip_points()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/get_trip_points.md)
  : Get Trip Points from Pelagic Data Systems API
- [`get_trips()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/get_trips.md)
  : Retrieve Trip Details from Pelagic Data API
- [`ingest_pds_tracks()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/ingest_pds_tracks.md)
  : Ingest Pelagic Data Systems (PDS) Track Data
- [`ingest_pds_trips()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/ingest_pds_trips.md)
  : Ingest Pelagic Data Systems (PDS) Trip Data
- [`ingest_surveys()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/ingest_surveys.md)
  : Ingest WCS and WF Catch Survey Data
- [`retrieve_surveys()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/retrieve_surveys.md)
  : Retrieve Surveys from Kobotoolbox

## Cloud Storage Management

Functions for interacting with cloud storage providers (Google Cloud
Storage, MongoDB), uploading, downloading, and managing data files in
various formats.

- [`cloud_object_name()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/cloud_object_name.md)
  : Retrieve Full Name of Versioned Cloud Object
- [`cloud_storage_authenticate()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/cloud_storage_authenticate.md)
  : Authenticate to a Cloud Storage Provider
- [`download_cloud_file()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/download_cloud_file.md)
  : Download Object from Cloud Storage
- [`download_parquet_from_cloud()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/download_parquet_from_cloud.md)
  : \#' Download Parquet File from Cloud Storage
- [`get_metadata()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/get_metadata.md)
  : Get metadata tables
- [`get_preprocessed_surveys()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/get_preprocessed_surveys.md)
  : Download Preprocessed Surveys
- [`get_validated_surveys()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/get_validated_surveys.md)
  : Download Validated Surveys
- [`mdb_collection_pull()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/mdb_collection_pull.md)
  : Retrieve Data from MongoDB
- [`mdb_collection_push()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/mdb_collection_push.md)
  : Upload Data to MongoDB and Overwrite Existing Content
- [`upload_cloud_file()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/upload_cloud_file.md)
  : Upload File to Cloud Storage
- [`upload_parquet_to_cloud()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/upload_parquet_to_cloud.md)
  : Upload Processed Data to Cloud Storage

## Data Preprocessing

Functions for cleaning, transforming, and structuring raw data into
standardized formats ready for analysis, including data nesting,
reshaping, and trip processing.

- [`calculate_catch()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/calculate_catch.md)
  : Calculate Catch Weight from Length-Weight Relationships or Bucket
  Measurements
- [`calculate_fishery_metrics()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/calculate_fishery_metrics.md)
  : Calculate Fishery Metrics
- [`generate_track_summaries()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/generate_track_summaries.md)
  : Generate Grid Summaries for Track Data
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
- [`prepare_boat_registry()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/prepare_boat_registry.md)
  : Prepare Boat Registry Data from Metadata
- [`preprocess_ba_surveys()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/preprocess_ba_surveys.md)
  : Pre-process Blue Alliance Surveys
- [`preprocess_pds_tracks()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/preprocess_pds_tracks.md)
  : Preprocess Pelagic Data Systems (PDS) Track Data
- [`preprocess_track_data()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/preprocess_track_data.md)
  : Preprocess Track Data into Spatial Grid Summary
- [`preprocess_wcs_surveys()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/preprocess_wcs_surveys.md)
  : Pre-process Zanzibar WCS Surveys
- [`preprocess_wf_surveys()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/preprocess_wf_surveys.md)
  : Pre-process and Combine WorldFish Surveys - Both Versions
- [`process_species_list()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/process_species_list.md)
  : Process Species List with Taxonomic Information
- [`process_trip_data()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/process_trip_data.md)
  : Process Trip Data with District Information
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
- [`summarize_data()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/summarize_data.md)
  : Summarize WorldFish Survey Data

## Data Modeling & Analysis

Functions for statistical modeling, fleet activity estimation, and
scaling sample-based GPS data to fleet-wide estimates using boat
registry information.

- [`estimate_fleet_activity()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/estimate_fleet_activity.md)
  : Estimate Fleet-Wide Activity from Sample Data
- [`calculate_district_totals()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/calculate_district_totals.md)
  : Calculate District-Level Total Catch and Revenue
- [`calculate_monthly_trip_stats()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/calculate_monthly_trip_stats.md)
  : Calculate Monthly Trip Statistics by District
- [`generate_fleet_analysis()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/generate_fleet_analysis.md)
  : Generate Complete Fleet Activity Analysis Pipeline

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
  : Validate Wild Fishing Survey Data

## Data Export & Visualization

Functions for exporting processed data to MongoDB collections, creating
geographic visualizations, and preparing data for portals and reporting.

- [`create_geos()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/create_geos.md)
  : Generate Geographic Regional Summaries of Fishery Data
- [`create_geos_v1()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/create_geos_v1.md)
  : Generate Geographic Regional Summaries of Fishery Data (Version 1)
- [`export_wf_data()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/export_wf_data.md)
  : Export WorldFish Summary Data to MongoDB
- [`kepler_mapper()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/kepler_mapper.md)
  : Generate a Kepler.gl map

## Pipeline Orchestration

High-level functions that orchestrate complete analysis pipelines,
combining multiple processing steps into integrated workflows.

- [`generate_fleet_analysis()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/generate_fleet_analysis.md)
  : Generate Complete Fleet Activity Analysis Pipeline
- [`summarize_data()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/summarize_data.md)
  : Summarize WorldFish Survey Data

## Helper Functions

Utility functions that support the main pipeline operations, providing
common data manipulation and processing capabilities.

- [`add_version()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/add_version.md)
  : Add timestamp and sha string to a file name
- [`read_config()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/read_config.md)
  : Read configuration file
