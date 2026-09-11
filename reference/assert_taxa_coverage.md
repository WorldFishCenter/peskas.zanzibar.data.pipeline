# Fail the run when a taxon loses its length-weight coefficients

The FishBase read is a live network read of a remote parquet dataset. A
new release therefore reaches the pipeline the moment a container is
rebuilt, with no code change. Release 26.06 dissolved `Caesionidae` into
`Lutjanidae` and `Scaridae` into `Labridae`; both family names survive
with **zero species** in them, so any taxon whose reference name is one
of those families expands to nothing and gets no coefficients.

Nothing fails on its own when that happens:
[`calculate_catch()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/calculate_catch.md)
left-joins the coefficients, so a taxon with no `(lw_a, lw_b)` pair
yields `NA` weight, and `NA` sums to zero. The taxon disappears from the
portal and the run stays green. This turns that silence into a failed
job.

## Usage

``` r
assert_taxa_coverage(
  taxa_list,
  lw,
  exempt = c("MZZ", "UNK", "UNKN", "MAE", "TAG", "GQT", "CRA", "KAK", "LHV", "RMB",
    "RTY", "SSP")
)
```

## Arguments

- taxa_list:

  Character vector of FAO 3-alpha codes requested.

- lw:

  The `lw` table from
  [`getLWCoeffs()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/getLWCoeffs.md),
  after any manually curated coefficients have been bound on.

- exempt:

  Codes that carry no coefficients today. This is a **baseline, not a
  whitelist**: it records the taxa that were already uncovered when the
  check was introduced (measured 2026-09-05 against FishBase 25.04 /
  SeaLifeBase 24.07, 120 of 134 codes resolving), so that any *new* loss
  fails the run. `CJX` and `PWT` are deliberately absent — they resolve
  at 25.04 and are the two codes that break at 26.06, so a release move
  fails here. Shrinking this list is follow-up work; each group below is
  a separate fix.

  Not a taxon

  :   `MZZ` and `UNKN` are dropped by
      [`get_fao_groups()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/get_fao_groups.md)
      before the search runs; `UNK` is absent from ASFIS entirely and is
      rewritten to the fish group's most frequent taxon later in
      [`preprocess_wf_surveys()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/preprocess_wf_surveys.md).

  Wrong reference name

  :   The ASFIS name does not describe the animal landed in Zanzibar, so
      the area 51 filter correctly removes it. `MAE` and `TAG` name
      species absent from FAO 51. `AHI`, `BFL` and `MAC` were the same
      kind of error and are now remapped on read in
      [`reshape_catch_data()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/reshape_catch_data.md)
      – to `BAF`, `TEI` and (for the sharks-and-rays group) `AQX` – so
      they no longer reach this check.

  No published coefficients

  :   `GQT` (*Plectorhinchus gaterinus*) resolves to a species that does
      occur in FAO 51, but FishBase carries no length-weight pair for it
      in any length type. There is nothing to convert and nothing to
      alias; the measurement does not exist.

  Outdated synonym

  :   Names valid when ASFIS was written and since synonymised are now
      corrected in
      [`taxa_search_aliases()`](https://worldfishcenter.github.io/peskas.zanzibar.data.pipeline/reference/taxa_search_aliases.md),
      which recovered `CLP`, `ESR`, `LZV`, `OQC`, `RPO` and `VMX`. Only
      `CRA` (*Brachyura*) is left: an infraorder, a rank the backbone
      omits.

  No TL-type coefficients

  :   The species resolves and occurs in FAO 51 and has published (a, b)
      pairs, but every one is fork, standard or another length type.
      [`coasts::convert_lw_to_tl()`](https://rdrr.io/pkg/coasts/man/convert_lw_to_tl.html)
      now restates most of these on a total-length basis, which
      recovered 15 codes including swordfish, the tunas, the marlins and
      the trevallies. The five left are `KAK`, `LHV`, `RMB`, `RTY` and
      `SSP`, which have no usable length-length fit to convert through.

## Value

`lw`, invisibly.

## Porting

The check transfers unchanged, but `exempt` is country-specific. Run
once against the country's own taxa list and record whatever it reports
as the starting baseline.
