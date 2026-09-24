# Peskas Zanzibar data pipeline

[![R-CMD-check](https://github.com/WorldFishCenter/peskas.zanzibar.data.pipeline/actions/workflows/R-CMD-check.yaml/badge.svg)](https://github.com/WorldFishCenter/peskas.zanzibar.data.pipeline/actions/workflows/R-CMD-check.yaml)
[![pkgdown](https://github.com/WorldFishCenter/peskas.zanzibar.data.pipeline/actions/workflows/pkgdown.yaml/badge.svg)](https://github.com/WorldFishCenter/peskas.zanzibar.data.pipeline/actions/workflows/pkgdown.yaml)

The code that turns fish landing surveys and boat GPS tracks from
Zanzibar into checked data for Peskas.

**See the results:** [Peskas Zanzibar](https://zanzibar.peskas.org),
[Peskas Management Platform](https://validation.peskas.org), [Peskas
Fishery Data API](https://api.peskas.org/docs) and [Peskas
Coasts](https://coasts.peskas.org).

## What it is

This pipeline serves fisheries managers, survey teams and researchers
working on small-scale fisheries in Zanzibar, Tanzania. It processes
four survey programmes: WorldFish (WF) landing surveys, Wildlife
Conservation Society (WCS) landing surveys, Blue Alliance (BA) surveys
and gleaning surveys. It links surveys to trips recorded by GPS trackers
on boats, checks every record for likely errors, and publishes the
results.

## What it produces

- Prepares monthly summaries of catch, effort, revenue and price by
  district, species and fishing gear for Peskas Zanzibar.
- Flags likely errors in each WorldFish survey so survey teams can
  review and correct them in the Peskas Management Platform.
- Publishes WorldFish landing records, before and after checks, through
  the Peskas Fishery Data API.
- Estimates the weight of the catch from measured fish lengths, using
  reference data from FishBase and SeaLifeBase.
- Links surveys to GPS-tracked trips and supplies Zanzibar’s figures to
  the Peskas Coasts regional comparison.

## Where the data comes from

- **WorldFish (WF) surveys.** Enumerators (trained data collectors) from
  the Zanzibar Fisheries and Marine Resources Research Institute
  (ZAFIRI) record landings on KoboToolbox, the free mobile survey app. A
  landing is a boat’s return to shore with its catch.
- **WCS surveys.** WCS landing surveys, also recorded on KoboToolbox.
- **Blue Alliance (BA) surveys.** Loaded into the pipeline’s storage as
  a file. The pipeline does not download them itself.
- **Gleaning surveys.** Gleaning is collecting seafood on foot along the
  shore at low tide. These surveys use their own KoboToolbox form.
- **GPS trackers (Pelagic Data Systems).** Small solar-powered devices
  on boats record where they travel. A trip is one fishing outing, from
  leaving shore to landing.
- **Reference data.** Boat and tracker records kept in Airtable,
  reference tables kept in Google Sheets, and species data from FishBase
  and SeaLifeBase.

The data is updated every four days.

Known limits:

- Peskas Zanzibar and the Peskas Fishery Data API use the WorldFish
  surveys only. WCS, Blue Alliance and gleaning surveys are processed
  and checked but not yet published there.
- Blue Alliance data changes only when someone adds a new file to
  storage.
- Catch weights worked out from fish lengths are estimates.

## Who runs it

WorldFish runs this pipeline with the Zanzibar Fisheries and Marine
Resources Research Institute (ZAFIRI), which collects the WorldFish
surveys. For questions, write to <peskas.platform@gmail.com>.

## Part of Peskas

Peskas is WorldFish’s open-source platform for monitoring small-scale
fisheries (<https://peskas.org>).

- [Peskas Zanzibar](https://zanzibar.peskas.org), [Peskas
  Kenya](https://peskas-dashboard-kenya.vercel.app/en), [Peskas
  Mozambique](https://peskas-dashboard-mozambique.vercel.app): country
  dashboards
- [Peskas Timor-Leste](https://timor.peskas.org): Timor-Leste portal
- [Peskas Coasts](https://coasts.peskas.org): regional comparison across
  countries
- [Peskas Tracks](https://tracks.peskas.org): app for fishers to see
  their trips and log catches
- [Peskas Kenya BMU
  dashboard](https://digitalfisheries.kenya.peskas.org): dashboard for
  Beach Management Units in Kenya
- [Peskas Management Platform](https://validation.peskas.org): data
  review and download for survey teams
- [Peskas Fishery Data API](https://api.peskas.org/docs): programmatic
  access to landing data
- Data pipelines:
  [Kenya](https://github.com/WorldFishCenter/peskas.kenya.data.pipeline),
  [Mozambique](https://github.com/WorldFishCenter/peskas.mozambique.data.pipeline),
  [Timor-Leste](https://github.com/WorldFishCenter/peskas.timor.data.pipeline),
  [Coasts](https://github.com/WorldFishCenter/peskas.coasts)

## For developers

The code is an R package called `peskas.zanzibar.data.pipeline`. It
relies on the shared
[`coasts`](https://github.com/WorldFishCenter/peskas.coasts) package for
storage, KoboToolbox, GPS tracks and dashboard data. Function reference:
<https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/>.

### Requirements

- R 4.5 with the GDAL, GEOS and PROJ spatial libraries. Production uses
  the `rocker/geospatial:4.5` image.
- The `coasts` package, installed from GitHub by
  `devtools::install_deps()`.

### Setup

``` r

devtools::install_deps()
devtools::load_all()
```

Credentials come from environment variables. Create a `.env` file in the
repository root (it is git-ignored).
[`read_config()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/read_config.md)
loads it and reads
[`inst/config.yml`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/inst/config.yml),
which expects:

- KoboToolbox: `WF_SURVEYS_ID`, `WF_SURVEYS_ID_V2`, `WF_SURVEYS_ID_V3`,
  `WF_SURVEYS_UNAME`, `WF_SURVEYS_PSSW`, `WF_SURVEYS_TOKEN`,
  `WF_GLEANING_ID`, `WCS_SURVEYS_ID`, `WCS_SURVEYS_UNAME`,
  `WCS_SURVEYS_PSSW`
- Google Cloud: `GCP_SA_KEY` (the service account JSON on a single line)
- MongoDB: `MONGODB_CONNECTION_STRING`,
  `MONGODB_CONNECTION_STRING_VALIDATION`
- Pelagic Data Systems: `PDS_TOKEN`, `PDS_SECRET`
- Metadata: `GOOGLE_SHEET_ID`, `AIRTABLE_TOKEN`,
  `AIRTABLE_BASE_ID_FRAME`

The `default` profile uses the development buckets and databases. The
`production` profile is switched on only by CI on `main`.

### Main commands

``` r

devtools::document()  # after editing roxygen comments; man/ is committed
devtools::check()
```

Each pipeline step is one exported function, for example
[`validate_wf_surveys()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/validate_wf_surveys.md).
The main workflow runs them in this order, with the GPS steps running
alongside the survey steps:

1.  Build the Docker image.
2.  Download WCS and WorldFish surveys from KoboToolbox, then clean the
    BA, WCS and WorldFish surveys.
3.  Download and process GPS trips and tracks (with `coasts`).
4.  Check the WCS, BA and WorldFish surveys for likely errors.
5.  Match surveys to GPS trips.
6.  Export raw and checked landing records for the Peskas Fishery Data
    API.
7.  Summarise the checked data and estimate fleet activity, then export
    the dashboard data (with `coasts`).

Gleaning surveys run as a separate chain: download, clean, then check.

### How it runs in production

[`data-pipeline.yaml`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/.github/workflows/data-pipeline.yaml)
runs at 00:00 UTC every fourth day of the month (1st, 5th, 9th, …) and
on every push. Its first job builds the Docker image from
`Dockerfile.prod` with the latest `coasts` release; every other job runs
inside it. Runs on `main` use the `production` profile; runs on any
other branch use `default`.

### Releases

Bump `Version:` in `DESCRIPTION` and add a block at the top of
[`NEWS.md`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/NEWS.md)
headed `# peskas.zanzibar.data.pipeline X.Y.Z`, written for
non-technical readers. On push to `main`, `release.yml` turns that block
into a GitHub release if the version is new.

### Tests

There are no automated tests yet. `R-CMD-check.yaml` checks that the
package builds and its documentation is consistent. Check a change by
running the affected function against the `default` profile.

### Contributing

Read
[`.github/CONTRIBUTING.md`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/CONTRIBUTING.md).
Code follows the [tidyverse style guide](https://style.tidyverse.org);
comment `/style` on a pull request to apply it. New to R packages? See
[*R Packages*](https://r-pkgs.org) by Hadley Wickham and Jenny Bryan.
For AI-assisted work, see
[`CLAUDE.md`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/CLAUDE.md).

## Licence

GPL-3. See
[`LICENSE.md`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/LICENSE.md).
