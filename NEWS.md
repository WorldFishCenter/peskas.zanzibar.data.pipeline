# peskas.zanzibar.data.pipeline 2.4.0

### Major Changes
- Implemented support for multiple survey data sources:
  - Refactored `get_validated_surveys()` to handle WCS, WF, and BA sources
  - Added source parameter to specify which datasets to retrieve
  - Improved handling of data sources with different column structures

### New Features
- Added `export_wf_data()` function for WorldFish-specific data export
- Enhanced validation with additional composite metrics:
  - Price per kg validation
  - CPUE (Catch Per Unit Effort) validation
  - RPUE (Revenue Per Unit Effort) validation

### Improvements
- Added min_length parameter for better length validation thresholds
- Updated LW coefficient filtering logic in model-taxa.R
- Enhanced alert flag handling with combined flags from different validation steps
- Improved catch price and catch weight handling for zero-catch outcomes
- Enhanced data preprocessing with better field type conversion

### Bug Fixes
- Fixed issue with catch_price field type in WF survey preprocessing
- Corrected filter condition for taxa coefficients

# peskas.zanzibar.data.pipeline 2.3.0

### Major Changes
- Enhanced KoboToolbox integration:
  - Implemented new validation status retrieval from KoboToolbox API
  - Updated validation workflow to incorporate submission validation status
  - Improved data validation process through direct API integration

### New Features
- New KoboToolbox interaction functions:
  - `get_validation_status()`: Retrieves submission validation status from KoboToolbox API
  
### Improvements
- Modified configuration files to support new KoboToolbox API token
- Added new environment variable for KoboToolbox API authentication
- Enhanced validation workflow with integrated validation status checks

# peskas.zanzibar.data.pipeline 2.2.0

### Major Changes
- Completely restructured taxonomic data processing:
  - Introduced new modular functions for taxa handling in model-taxa.R
  - Added efficient batch processing for species matching
  - Implemented optimized FAO area retrieval system
  - Streamlined length-weight coefficient calculations
  - Enhanced integration with FishBase and SeaLifeBase

### New Features
- New taxonomic processing functions:
  - `load_taxa_databases()`: Unified database loading from FishBase and SeaLifeBase
  - `process_species_list()`: Enhanced species list processing with taxonomic ranks
  - `match_species_from_taxa()`: Improved species matching across databases
  - `get_species_areas_batch()`: Efficient FAO area retrieval
  - `get_length_weight_batch()`: Optimized length-weight parameter retrieval

### Improvements
- Enhanced performance through batch processing
- Reduced API calls to external databases
- Better error handling and input validation
- More comprehensive documentation
- Improved code organization and modularity

### Deprecations
- Removed legacy taxonomic processing functions
- Deprecated redundant species matching methods
- Removed outdated data transformation utilities

### Documentation
- Added detailed function documentation
- Updated vignettes with new workflows
- Improved code examples
- Enhanced README with new features

# peskas.zanzibar.data.pipeline 2.1.0

#### Major Changes
- Enhanced taxonomic and catch data processing capabilities:
  - Added comprehensive functions for species and catch data processing
  - Implemented length-weight coefficient retrieval from FishBase and SeaLifeBase
  - Created functions for calculating catch weights using multiple methods
  - Added new data reshaping utilities for species and catch information
- Extended Wild Fishing (WF) survey validation with detailed quality checks
- Updated cloud storage and data download/upload functions

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

#### Changes
- Updated configuration management:
  - Moved configuration settings to inst/conf.yml
  - Improved configuration structure and organization
  - Enhanced configuration flexibility


