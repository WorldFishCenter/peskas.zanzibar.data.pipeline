# peskas.zanzibar.data.pipeline

R package for the Zanzibar node of Peskas: ingests, preprocesses and
validates WCS, WorldFish (WF v1-v3), Blue Alliance (BA) and WF gleaning
surveys, matches surveys to PDS trips, and feeds the peskas-api bucket,
Mongo `validation-*` and the coasts portal. Ecosystem context (other
repos, data flow, cross-repo contracts): loaded by the `peskas` Claude
Code plugin (repo `peskas-context`).

## Commands

``` r

devtools::install_deps()
devtools::load_all()
devtools::document()    # after any roxygen change; man/ is committed
devtools::check()
```

- Pipeline steps and schedule: read
  `.github/workflows/data-pipeline.yaml`. The every-4-days cron is
  intentional; do not “fix” it to match other pipelines.

## Architecture

- **Ingestion** (`R/ingestion-surveys.R`): `ingest_wcs_surveys`
  (kf.kobotoolbox.org) and `ingest_wf_surveys` (eu.kobotoolbox.org; WF
  v1, v2, v3 and gleaning forms in one call). Both go through
  [`coasts::get_kobo_data()`](https://rdrr.io/pkg/coasts/man/get_kobo_data.html).
- **Survey chains**: `preprocess_wcs_surveys` / `preprocess_wf_surveys`
  / `preprocess_ba_surveys` (`R/preprocessing.R`) -\>
  `validate_wcs_surveys` / `validate_wf_surveys` / `validate_ba_surveys`
  (`R/validation.R`). Gleaning is a separate job:
  `preprocess_wf_gleaning` (`R/process-catch-gleaning.R`) -\>
  `validate_gleaning_surveys` (`R/validation-functions.R`).
- **WCS nesting** (`R/wcs-nesting.R`): `pt_nest_trip`, `pt_nest_catch`,
  `pt_nest_length`, `pt_nest_market`, `pt_nest_attachments` turn the
  wide WCS form into nested tables joined by `survey_id`.
- **Joint steps**: `merge_trips(site = "zanzibar")`,
  `export_api_raw/validated`, then
  [`coasts::summarize_data`](https://rdrr.io/pkg/coasts/man/summarize_data.html),
  [`coasts::generate_fleet_analysis`](https://rdrr.io/pkg/coasts/man/generate_fleet_analysis.html)
  and
  [`coasts::export_portal`](https://rdrr.io/pkg/coasts/man/export_portal.html)
  with `package = "peskas.zanzibar.data.pipeline"`. PDS ingestion is
  `coasts::ingest_pds_*` and
  [`coasts::describe_pds_tracks`](https://rdrr.io/pkg/coasts/man/describe_pds_tracks.html)
  with the same `package` argument; this repo has no storage or PDS code
  of its own. The fleet estimate places each tracker by where its trips
  land (`pds.fleet_location: landing`), not by the Airtable `gaul 2`
  link.
- **Taxa and weights** (`R/model-taxa.R`): FishBase/SeaLifeBase versions
  are pinned separately in `inst/config.yml` (`metadata.fishbase`); read
  the comment there before bumping either.
- `export_wf_data` (`R/export.R`) exists but is not called from any
  workflow.

## Rules

- Tune validation in `inst/config.yml` under `validation.<survey>`
  (`wcs`, `wf`: bucket weight and count, individuals, price, CPUE, RPUE,
  length; `ba`: the MAD multipliers), not in code. Gleaning thresholds
  live in
  [`gleaning_validation_thresholds()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/gleaning_validation_thresholds.md)
  and can be overridden through its arguments.
- Keep flag numbers stable when editing validation (see the Peskas
  context, validation flags contract).
- Prefer `coasts::` over the local copies of Airtable helpers in
  `R/airtable-helpers.R`; flag the duplicate when you touch one.
- [`merge_trips()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/merge_trips.md)
  takes `site`; most other workflow functions take only `log_threshold`.

## Gotchas

- Nothing in this package ingests BA raw data: `preprocess_ba_surveys`
  reads the latest `ba-surveys-raw` object that already exists in the
  bucket.
