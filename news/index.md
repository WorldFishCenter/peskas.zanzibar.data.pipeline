# Changelog

## peskas.zanzibar.data.pipeline 4.9.1

### Refactor

- **The total-length restatement now comes from coasts**:
  `get_length_conversions()` and `convert_lw_to_tl()` are deleted;
  [`get_length_weight_batch()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/get_length_weight_batch.md)
  calls
  [`coasts::convert_lw_to_tl()`](https://github.com/WorldFishCenter/peskas.coasts)
  instead. The same arithmetic existed here, in Mozambique and in Timor,
  written three times with different semantics. coasts passes
  unconvertible rows through rather than dropping them, so the call site
  keeps `Type == "TL"` to preserve the behaviour this pipeline had.
  Verified against the live taxa list at FishBase 25.04 / SeaLifeBase
  24.07, area 51: the same 16 rows restated for the same 7 taxa (`BET`,
  `BLM`, `BUM`, `MLS`, `NXT`, `QJR`, `SWO`), and the final table is
  [`identical()`](https://rdrr.io/r/base/identical.html) across all 43
  codes — zero change to any published coefficient.
- **`coasts (>= 4.13.0)`** is now a declared floor in `DESCRIPTION`.

## peskas.zanzibar.data.pipeline 4.9.0

### Bug Fixes

- **FishBase releases are now pinned, and a missing coefficient fails
  the run**: `rfishbase` reads a remote parquet dataset over the
  network, so an unpinned `"latest"` let a new FishBase release reach
  the pipeline the moment a container was rebuilt, with no code change.
  Release 26.06 dissolved `Caesionidae` into `Lutjanidae` and `Scaridae`
  into `Labridae` — both family names survive with zero species in them
  — so any taxon named after one expanded to nothing, got no
  length-weight coefficients, and weighed `NA`, which sums to zero. The
  releases are now pinned per server in `inst/config.yml` under
  `metadata:fishbase` and threaded through all five `rfishbase` reads in
  [`getLWCoeffs()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/getLWCoeffs.md),
  which previously could mix snapshots within a single run. `rfishbase`
  is additionally pinned to 5.0.1 in both Dockerfiles as the last
  install step, because `remotes::install_local(dependencies = TRUE)`
  upgrades it otherwise.

- **[`assert_taxa_coverage()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/assert_taxa_coverage.md)
  fails the run when a taxon resolves to no coefficients**: previously a
  taxon that matched nothing was dropped in silence and the pipeline
  stayed green while publishing a hole.

- **Unmatched taxa are now logged**:
  [`match_species_from_taxa()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/match_species_from_taxa.md)
  dropped any name that matched no species without a warning.

- **SeaLifeBase routing corrected**:
  [`process_species_list()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/process_species_list.md)
  routed only ISSCAAP groups 57, 45, 43, 42 and 56 to SeaLifeBase,
  sending sea cucumbers, gastropods, oysters, mussels, scallops and
  mantis shrimp to FishBase, where they matched nothing and were
  dropped. Routing is now ISSCAAP \>= 40.

- **Species names ending in “idae” are no longer read as families**: the
  rank test placed the family suffix before the species test, so
  `Haliotis midae` and `Jordanella floridae` were searched as families
  and matched nothing.

- **Length-type conversion recovers 16 taxa that weighed `NA`**
  (`get_length_conversions()`, `convert_lw_to_tl()`): FishBase tags
  every published length-weight pair with the length type the original
  study measured, and for tunas, billfish and several carangids that is
  fork length.
  [`get_length_weight_batch()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/get_length_weight_batch.md)
  kept only `Type == "TL"`, so those taxa got no coefficients at all and
  every length-measured catch row of them weighed `NA`. The conversions
  are published data in FishBase’s POPLL table, which this pipeline
  never read;
  [`coasts::get_taxa_morphometrics()`](https://rdrr.io/pkg/coasts/man/get_taxa_morphometrics.html)
  already does. Reading it restates `a` on a total-length basis
  (`a_TL = a * ratio^b`, `b` unchanged) and recovers
  `ALB BET BLM BUM CJC EWM FLY LJK LTQ LWO MLS NAB NXM NXP QJR SWO`.
  Coverage goes from 98 to 114 of 134 codes, and **none of the 98
  coefficients that already resolved changed** — the conversion is
  applied only to taxa that would otherwise have nothing.

  Validated against the 632 species carrying both a native TL pair and
  an FL one: converting halves the median error in predicted weight
  (15.6% against 27.5% for using the FL pair as-is), and against the
  1,186 with both TL and SL pairs it cuts it fourfold (16.8% against
  70.6%). Ratios are physically sensible (median FL/TL 0.962, SL/TL
  0.831). Compared against FishBase’s independent TL-basis Bayesian
  estimates for the recovered taxa, converting is closer than raw on 10
  of 15.

- **Retired survey codes are remapped on read**
  ([`reshape_catch_data()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/reshape_catch_data.md)):
  editing a KoBoToolbox form’s choice list does not rewrite submissions
  already collected, so a retired code persists in historical data
  indefinitely. `AHI` (171 rows) and `BFL` (6 rows) had additionally
  been removed from the Airtable taxa table, which orphaned them —
  [`map_surveys()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/map_surveys.md)
  left those rows with `NA` scientific name and alpha3 code. They now
  remap to `BAF` (*Ablennes hians*) and `TEI` (*Pterocaesio pisang*),
  joining the `TUN` and `SKH` remaps that were already there.

- **The `MAC` correction now runs early enough to matter**: `MAC` was
  wrongly offered under the sharks-and-rays group in the form, and all
  150 rows carrying it are `fish_group == "SR"` with a median length of
  85 cm — eagle rays, not the Atlantic mackerel ASFIS maps `MAC` to. The
  `SR`/`MAC` → `AQX` rule existed in
  [`preprocess_wf_surveys()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/preprocess_wf_surveys.md)
  but ran *after*
  [`calculate_catch()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/calculate_catch.md),
  so the rows were weighed as `MAC` (which resolves to no coefficients,
  giving `NA`) and only relabelled afterwards. Moving it into
  [`reshape_catch_data()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/reshape_catch_data.md)
  lets them pick up the `AQX` coefficients: 137 of 190 `AQX` rows now
  carry a weight, median 34.9 kg, where the 150 previously carried none.

- **Morphology bounds no longer silently disable length validation**:
  `min(CommonLength, na.rm = TRUE)` returns `Inf` for a taxon whose
  matched species all lack that field, and the permissiveness step then
  computed `Inf - 0.75 * Inf` = `NaN`. Every comparison against `NaN` is
  `NA`, which `case_when()` treats as no-match, so alert codes 3 and 4
  never fired for those taxa — a missing bound was indistinguishable
  from a passed check. `safe_min()` now yields `NA` rather than `Inf`,
  and because FishBase populates `CommonLength` for only 10% of species
  against 91% for `Length`, missing values are estimated as
  `0.625 * Length` — the median ratio across the 3,748 species carrying
  both (`common_length_ratio()`). All 128 taxa with morphology now have
  usable bounds, against 20 previously broken. Expect a wave of new
  length alerts on the first run: those records were never checked
  before.

- **Search-name aliases fix six taxa the ASFIS names could not match**
  ([`taxa_search_aliases()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/taxa_search_aliases.md),
  [`apply_taxa_aliases()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/apply_taxa_aliases.md)):
  a handful of ASFIS reference names match nothing in the taxonomic
  backbone, so the taxon is dropped and every catch row of it weighs
  `NA`. `CLP` is named `Clupeidae`, which FishBase emptied of
  Indo-Pacific species in 2022 — it now searches `Dorosomatidae`,
  matching the row already in Timor’s equivalent table. The rest are
  synonyms that have moved on: `ESR` to *Stolephorus commersonnii*,
  `RPO` to *Parupeneus macronemus*, `LZV` to *Ellochelon vaigiensis*,
  `OQC` to *Octopus cyanea*, and `VMX` — *Valamugil*, a genus the
  backbone no longer carries — to *Osteomugil* and *Moolgarda*. The
  table carries an explicit `rank` because the suffix rules in
  [`process_species_list()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/process_species_list.md)
  cannot recognise a bare genus name.

  `CRA` (“marine crabs nei”, *Brachyura*) is deliberately not aliased:
  it is an infraorder, and SeaLifeBase carries no rank between order
  *Decapoda* and family, so choosing a target means deciding which crab
  families Zanzibar lands.

- **`OQC` now gets the octopus mantle-length conversion**: `OCZ`
  (*Octopus spp*) was special-cased in three places — an ML-only
  coefficient filter, the arm-span-to-mantle `/5.5` conversion in
  [`calculate_catch()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/calculate_catch.md),
  and the `min_length` floor — but `OQC` (*Octopus cyaneus*) was not,
  despite being the larger of the two in the data at 694 rows with 484
  lengths, median 85 cm. `OQC` resolves to a mantle-length pair, so
  applying it to arm-span unconverted weighed a single octopus at **263
  kg** instead of 2.43 kg. This was latent while `OQC` had no
  coefficients and would have gone live with the alias above.

- **Fixed a duplicate join key for `FLY`**:
  [`preprocess_wf_surveys()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/preprocess_wf_surveys.md)
  appended a hardcoded flying-fish coefficient unconditionally. Now that
  the conversion recovers Exocoetidae pairs,
  [`getLWCoeffs()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/getLWCoeffs.md)
  returns a `FLY` row of its own, and two rows on the same key would
  have doubled every flying fish catch record in
  [`calculate_catch()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/calculate_catch.md).
  The manual value now replaces rather than appends, and stays
  authoritative: FishBase’s recovered pairs weigh a 30 cm flying fish at
  498 g against the hardcoded 202 g, and changing that is a separate
  decision.

- **Removed a dead fallback in
  [`preprocess_wf_surveys()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/preprocess_wf_surveys.md)**:
  the `tryCatch` around
  [`getLWCoeffs()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/getLWCoeffs.md)
  read `inst/length_weight_params.rds`, which is not in the package, so
  the fallback could only ever fail — while hiding the original error
  behind it.

### Known Issues

Measured 2026-09-06 against FishBase 25.04 / SeaLifeBase 24.07 over the
live KoBo data: **122 of 134 codes resolve length-weight coefficients**,
up from 98 before this release. The other 12 form the documented
baseline in
[`assert_taxa_coverage()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/assert_taxa_coverage.md),
so any *new* loss fails the run. `CJX` and `PWT` are deliberately not in
it — they resolve at 25.04 and are the two codes that break at 26.06, so
a release move fails the check.

- **Wrong reference name (2)** — `MAE`, `TAG` name species absent from
  FAO 51.
- **No published coefficients (1)** — `GQT` (*Plectorhinchus gaterinus*)
  occurs in FAO 51 but FishBase carries no length-weight pair for it in
  any length type. Nothing to convert, nothing to alias.
- **No convertible length type (5)** — `KAK`, `LHV`, `RMB`, `RTY`,
  `SSP`.
- **Not a taxon, or a rank the backbone omits (4)** — `MZZ`, `UNKN`,
  `UNK`, and `CRA` (the infraorder *Brachyura*).

## peskas.zanzibar.data.pipeline 4.8.0

### New Features

- **Gleaning Survey Data Integration**: Added support for KoBoToolbox
  gleaning survey data collection and processing
  - New survey type (`gleaning`) is now ingested from KoBoToolbox,
    preprocessed, and validated alongside existing catch surveys
  - Gleaning data captures informal fisheries activity not covered by
    formal catch surveys, providing a more complete picture of
    small-scale fishing effort
  - Gleaning submissions are now included in the unified validation
    pipeline with tailored quality checks

## peskas.zanzibar.data.pipeline 4.7.0

### New Features

- **Unified API export across survey programs**: The raw and validated
  API exports now combine data from both WCS and WorldFish surveys into
  a single output file. Previously only WorldFish data was exported; WCS
  trips are now included alongside them with a consistent set of fields.

### Improvements

- **WCS validation enhancements**:
  - Added two new quality checks: one that catches contradictions
    between bucket count and bucket weight (e.g. buckets recorded but no
    weight, or vice versa), and one that flags implausible negative
    values in catch measurements
  - Price and revenue validation thresholds corrected to Tanzanian
    Shilling values (previous values were in Mozambican metical)
  - Submissions recorded before 2020 are now excluded from the validated
    dataset
  - Catch outcome is now correctly carried through to the validation
    step
- **WCS price calculation corrected**: Catch prices are now split
  proportionally across species within a trip. Missing species prices
  now fall back to the species-level median rather than being left
  empty.

## peskas.zanzibar.data.pipeline 4.6.0

#### Infrastructure & Workflow

- **Delegating to `coasts` most of the core storage and databse-related
  functions**: Now core and other countries shared storage functions are
  delagated to central and upgraded features of the `coasts`pacakge for
  improved standardization and maintainability

## peskas.zanzibar.data.pipeline 4.5.0

### Major Changes

- **Adopted `coasts` as the shared multicountry analytics engine**:
  Aggregated data summarization and dashboard export are now delegated
  to
  [`WorldFishCenter/peskas.coasts`](https://github.com/WorldFishCenter/peskas.coasts)
  (dev branch). This centralizes the logic for producing monthly, taxa,
  district, and gear summaries — as well as fishery metrics — across all
  Peskas country deployments (Zanzibar, Kenya, Mozambique), ensuring
  consistent outputs and a single place to maintain and improve the
  shared pipeline logic.
  - Added `coasts` to `Imports` and `Remotes` in `DESCRIPTION`
  - Added
    `remotes::install_github("WorldFishCenter/peskas.coasts", ref = "dev")`
    to both `Dockerfile` and `Dockerfile.prod` so the image ships the
    package
  - Pipeline steps that previously used local `summarize_data()` and
    `generate_fleet_analysis()` now call the equivalent `coasts::`
    functions, passing `package = "peskas.zanzibar.data.pipeline"` so
    they read the country-specific `inst/conf.yml`

## peskas.zanzibar.data.pipeline 4.4.0

### Improvements

- **Standardized configuration structure**: Replaced `inst/conf.yml`
  with a unified multi-country template harmonized across all Peskas
  deployments (Zanzibar, Kenya, Mozambique). Key structural changes:
  - Survey credentials moved from `surveys.*` into a new top-level
    `ingestion.*` section
  - Stage keys shortened (`raw_surveys` → `raw`, `preprocessed_surveys`
    → `preprocessed`, etc.)
  - Source names shortened (`wcs_surveys` → `wcs`, `wf_surveys_v1` →
    `wf_v1`, etc.)
  - MongoDB structure reorganized: connection strings under
    `connection_strings.*`, databases under `databases.*`, collections
    key pluralized, `portal` renamed to `dashboard`
  - Airtable config moved from top-level `airtable.*` to
    `metadata.airtable.*`
  - All R code updated to use the new config paths
- **`summarize_data()` correctness and clarity fixes**:
  - Fixed data quality bug: `taxa_summaries` was incorrectly summing
    trip-total catch kg per taxon instead of the actual per-taxon catch
    weight — values are now correct
  - Fixed `districts_summaries` round-trip pivot: data is now stored
    wide and pivoted once in
    [`export_wf_data()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/export_wf_data.md)
    instead of pivot-long → store → pivot-wide → pivot-long
  - Fixed `gear_summaries` `complete()` running after `pivot_longer`
    with wrong column names in the fill list; now completes before
    pivoting across all gear × district × month combinations
  - Replaced fragile `across(everything(), first)` pattern in trip-level
    collapse with explicit `slice(1)`, which is clearer and avoids
    silent column overwrites
  - Fixed inconsistent `na.rm` usage in gear summaries
- **Removed dead code**: Deleted `create_geos()` and `create_geos_v1()`
  from `export.R`; the geographic summary logic had already been inlined
  into
  [`export_wf_data()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/export_wf_data.md)

### Bug Fixes

- Fixed `match_surveys_to_registry.Rd` cross-reference warning caused by
  `[0,1]` being parsed as a markdown link
- Documented missing `devices_table` argument in `process_trip_data()`
- Added `stringdist` to `Imports` in `DESCRIPTION` (was used via `::`
  but not declared)

## peskas.zanzibar.data.pipeline 4.3.0

### New Features

- **Survey-GPS Trip Matching Pipeline**: Added comprehensive fuzzy
  matching system to link catch survey records with GPS trip data
  - New
    [`merge_trips()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/merge_trips.md)
    workflow function for Kenya and Zanzibar sites
  - [`match_surveys_to_gps_trips()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/match_surveys_to_gps_trips.md):
    Universal two-step matching (surveys → registry → trips)
  - Fuzzy string matching using Levenshtein distance on registration
    numbers, boat names, and fisher names
  - Conservative one-trip-per-day constraint to avoid ambiguous matches
  - Support for both explicit device registries (Kenya) and implicit
    registries built from trip data (Zanzibar)
  - Configurable matching thresholds (registration: 15%, names: 25%
    difference allowed)
  - Exports merged dataset with matched pairs plus all unmatched surveys
    and trips
  - Helper functions:
    [`standardize_column_names()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/standardize_column_names.md),
    [`clean_matching_fields()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/clean_matching_fields.md),
    [`build_registry_from_trips()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/build_registry_from_trips.md)

### Improvements

- **PDS Data Ingestion**:
  - Updated `ingest_pds_trips()` to load device registry from cloud
    storage instead of Airtable metadata
  - Added device info retrieval and proper filtering for Zanzibar
    devices
  - Improved configuration variable naming (pars → conf)
- **GitHub Actions Workflow**:
  - Added new `merge-trips` job to automated pipeline
  - Runs after survey preprocessing and before summarization
  - Integrated with production environment configuration

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
  - `prepare_boat_registry()`: Summarizes boat registry data by
    district.
  - `process_trip_data()`: Processes trip data with district information
    and filters outliers.
  - `calculate_monthly_trip_stats()`: Computes monthly fishing activity
    statistics by district.
  - `estimate_fleet_activity()`: Scales up sample-based trip statistics
    to fleet-wide estimates.
  - `calculate_district_totals()`: Combines fleet activity and catch
    data for district-level totals.
  - `generate_fleet_analysis()`: Orchestrates the full analysis pipeline
    and uploads results.
  - `summarize_data()`: Generates and uploads summary datasets (monthly,
    taxa, district, gear, grid) for WorldFish survey data.
- **Enhanced Data Export and Integration:**
  - [`export_wf_data()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/export_wf_data.md):
    Exports summarized WorldFish survey data and modeled estimates to
    MongoDB, including new geographic regional summaries.
  - `create_geos()`: Generates geospatial regional summaries and exports
    as GeoJSON for spatial visualization.
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

- Added `create_geos()` function to generate geospatial regional
  summaries of fishery data
- Added support for GPS track data visualization through new grid-based
  analytics
- Added `generate_track_summaries()` function to process GPS tracks into
  1km grid cells

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
  - Refactored `get_validated_surveys()` to handle WCS, WF, and BA
    sources
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
  [`ingest_wcs_surveys()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/ingest_wcs_surveys.md).
- The functions `retrieve_wcs_surveys()` downloads WCS Zanzibar surveys
  data

##### Changes

- Updated configuration management:
  - Moved configuration settings to inst/conf.yml
  - Improved configuration structure and organization
  - Enhanced configuration flexibility
