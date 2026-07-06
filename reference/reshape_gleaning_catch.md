# Reshape Gleaning Catch Data from Wide to Long Format

Reshapes the `group_catch` section of the Zanzibar/Pemba intertidal
gleaning KoboToolbox survey into a tidy long format. Unlike the
boat-fishery surveys (see
[`reshape_catch_data_v2()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/reshape_catch_data_v2.md)),
`group_catch` is a *single* (non-repeated) group per submission, so
there is no `n_catch` index. The wide-to-long work instead unpacks three
layered structures:

Reshapes the `group_catch` section of the Zanzibar/Pemba intertidal
gleaning KoboToolbox survey into a tidy long format. Unlike the
boat-fishery surveys (see
[`reshape_catch_data_v2()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/reshape_catch_data_v2.md)),
`group_catch` is a *single* (non-repeated) group per submission, so
there is no `n_catch` index. The wide-to-long work instead unpacks three
layered structures:

## Usage

``` r
reshape_gleaning_catch(data = NULL)

reshape_gleaning_catch(data = NULL)
```

## Arguments

- data:

  A data frame of the gleaning survey export. Catch columns are expected
  with the raw Kobo prefixes (`group_catch/...`, including the nested
  `group_catch/group_length_gastropods/...` /
  `group_catch/group_length_bivalves/...`).

## Value

A long data frame with one row per submission x shell group x size class
(plus a single context row for submissions without shell detail).

A long data frame with one row per submission x shell group x size class
(plus a single context row for submissions without shell detail).

## Details

1.  **Counting method.** `counting_method` (`backet` / `plastic_bag`)
    populates two mutually exclusive sets of columns (`bucket_*` vs
    `plastic_*`). These are coalesced into one unified container block:
    `container_type`, `container_size`, `container_size_kg`,
    `catch_fraction`, `unit_weight_kg`, `n_containers`.

2.  **Shell group + species.** `shell_group` (`bivalves` / `gastropod` /
    `both` / `others`) drives which species multi-selects
    (`Group_Bivalves`, `Group_Gastropod`) and which length block apply.
    Species stay as a space-separated code list per group (no
    per-species quantities exist in the instrument), with a derived
    `n_species` token count.

3.  **Length frequency.** Individual counts by size class (`<5`, `5-15`,
    `>15` cm) are recorded once per shell GROUP, not per species. Each
    present group emits one row per size class.

Output grain: one row per `submission_id` x `group` x `size_class`. A
submission recorded under `bivalves` or `gastropod` yields 3 rows;
`both` yields 6; `others` or a missing `shell_group` (no shell detail)
is preserved as a single context row with
`group`/`size_class`/`n_individuals` = NA so no submission is silently
dropped. All three size classes are retained per present group (NA = not
recorded, 0 = a recorded zero) so the size-frequency distribution stays
explicit and complete.

1.  **Counting method.** `counting_method` (`backet` / `plastic_bag`)
    populates two mutually exclusive sets of columns (`bucket_*` vs
    `plastic_*`). These are coalesced into one unified container block:
    `container_type`, `container_size`, `container_size_kg`,
    `catch_fraction`, `unit_weight_kg`, `n_containers`.

2.  **Shell group + species.** `shell_group` (`bivalves` / `gastropod` /
    `both` / `others`) drives which species multi-selects
    (`Group_Bivalves`, `Group_Gastropod`) and which length block apply.
    Species stay as a space-separated code list per group (no
    per-species quantities exist in the instrument), with a derived
    `n_species` token count.

3.  **Length frequency.** Individual counts by size class (`<5`, `5-15`,
    `>15` cm) are recorded once per shell GROUP, not per species. Each
    present group emits one row per size class.

Output grain: one row per `submission_id` x `group` x `size_class`. A
submission recorded under `bivalves` or `gastropod` yields 3 rows;
`both` yields 6; `others` or a missing `shell_group` (no shell detail)
is preserved as a single context row with
`group`/`size_class`/`n_individuals` = NA so no submission is silently
dropped. All three size classes are retained per present group (NA = not
recorded, 0 = a recorded zero) so the size-frequency distribution stays
explicit and complete.

## Examples

``` r
if (FALSE) { # \dontrun{
gleaning_long <- reshape_gleaning_catch(gleaning)

# Size-frequency by shell group across all submissions
gleaning_long |>
  dplyr::filter(!is.na(n_individuals)) |>
  dplyr::group_by(group, size_class) |>
  dplyr::summarise(total = sum(n_individuals), .groups = "drop")
} # }

if (FALSE) { # \dontrun{
gleaning_long <- reshape_gleaning_catch(gleaning)

# Size-frequency by shell group across all submissions
gleaning_long |>
  dplyr::filter(!is.na(n_individuals)) |>
  dplyr::group_by(group, size_class) |>
  dplyr::summarise(total = sum(n_individuals), .groups = "drop")
} # }
```
