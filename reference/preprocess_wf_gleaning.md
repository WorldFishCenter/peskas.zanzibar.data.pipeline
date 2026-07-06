# Pre-process WorldFish Zanzibar Gleaning Surveys

Downloads raw structured gleaning survey data from cloud storage and
preprocesses it into a single analysis-ready data frame. The function
assembles three pieces and joins them on the submission:

1.  **General info** – strips the Kobo group prefixes (`group_general/`,
    `group_trip/`, `no_fishers/`, `demographics/`,
    `group_gleaning_activity/`, `group_supply_chain/`), selects and
    renames the trip, demographic, activity and supply-chain fields,
    coalesces the conditional `landing_site` columns into one, and
    coerces dates and numeric fields.

2.  **Catch info** – reshapes the wide `group_catch` block into a tidy
    long table (one row per submission x shell group x size class) via
    [`reshape_gleaning_catch`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/reshape_gleaning_catch.md),
    unifying the parallel bucket/plastic container fields and applying
    conservative sanitisation.

3.  **Catch totals** – per submission, sums individuals across size
    classes (`total_individuals`) and reconstructs catch weight as
    `unit_weight_kg * n_containers` (`total_catch_kg`); the container
    fields are constant within a submission, hence `first()`.

## Usage

``` r
preprocess_wf_gleaning(log_threshold = logger::DEBUG)
```

## Arguments

- log_threshold:

  Logging threshold level (default:
  [`logger::DEBUG`](https://daroczig.github.io/logger/reference/log_levels.html)).

## Value

A data frame of preprocessed gleaning surveys: one row per submission x
shell group x size class, with general/demographic/activity/
supply-chain fields plus `total_individuals` and `total_catch_kg`.

## Details

Configurations are read from `config.yml` with the following necessary
parameters:

    surveys:
      wf_gleaning:
        raw:
          file_prefix:
          version:
    storage:
      google:
        key:
        options:
          project:
          bucket:
          service_account_key:

The function uses logging to track progress.

## See also

[`reshape_gleaning_catch`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/reshape_gleaning_catch.md),
[`sanitize_gleaning_inputs`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/sanitize_gleaning_inputs.md)
