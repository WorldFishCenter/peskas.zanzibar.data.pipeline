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


