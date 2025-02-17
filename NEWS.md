# peskas.zanzibar.data.pipeline 2.0.0

#### Major Changes
- Complete overhaul of the data pipeline architecture
- Added PDS (Pelagic Data Systems) integration:
  - New trip ingestion and preprocessing functionality
  - GPS track data processing capabilities
- Implemented MongoDB export and storage functions
- Removed renv dependency management for improved reliability
- Updated Docker configuration for more robust builds

#### New Features
- Enhanced validation system for survey data
- Added new data processing steps:
  - GPS track preprocessing
  - Catch data validation
  - Length measurements validation
  - Market data validation
- Flexible data export capabilities
- Improved GitHub Actions workflow with additional processing steps

#### Infrastructure Updates
- Streamlined package dependencies
- Updated build and deployment processes
- Enhanced data storage and retrieval mechanisms

# peskas.zanzibar.data.pipeline 1.0.0

#### Improvements
- All the functions are now documented and indexed according to keywords
- Thin out the R folder gathering functions by modules

#### Changes
- Move to parquet format rather than CSV/RDS


# peskas.zanzibar.data.pipeline 0.2.0

#### New features

Added the validation step and updated the preprocessing step for wcs kobo surveys data, see `preprocess_wcs_surveys()` and `validate_wcs_surveys()` functions. Currently, validation for catch weight, length and market values are obtained using median absolute deviation method (MAD) leveraging on the k parameters of the `univOutl::LocScaleB` function.

In order to accurately spot any outliers, validation is performed based on gear type and species.

**N.B. VALIDATION PARAMETERS ARE NOT YET TUNED**

#### Changes

No need to run the pipeline every two days, decreased not to every 4 days.

# peskas.zanzibar.data.pipeline 0.1.0

Drop parent repository code (peskas.timor.pipeline), add infrastructure to download WCS survey data and upload it to cloud storage providers

### New features

- The ingestion of WCS Zanzibar surveys is implemented in `ingest_wcs_surveys()`. 
- The functions `retrieve_wcs_surveys()` downloads  WCS Zanzibar surveys data 


