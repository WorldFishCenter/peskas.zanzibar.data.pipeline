# Search names that override the ASFIS reference name

A few ASFIS reference names match nothing in the taxonomic backbone, so
the taxon is dropped, gets no coefficients, and every catch row of it
weighs `NA` – which sums to zero. This table substitutes a name that
does match. Each entry is a correction to the *reference data*, not to
FishBase.

## Usage

``` r
taxa_search_aliases()
```

## Value

A tibble of `a3_code`, `scientific_name` and `rank`. Several rows may
share an `a3_code`; all of them are searched.

## Details

- `CLP`:

  ASFIS calls it `Clupeidae`, but FishBase moved *Sardinella*,
  *Amblygaster* and *Herklotsichthys* to `Dorosomatidae` in 2022,
  leaving `Clupeidae` with 15 temperate species and **none** in FAO
  area 51. The dagaa landed in Zanzibar are Dorosomatidae. Searching the
  family rather than the three genera makes no practical difference – 31
  g against 28 g for a 15 cm fish – and matches the row already in
  Timor's `taxa_search_aliases()`, so the pipelines agree.

- Synonyms:

  `ESR`, `RPO`, `LZV`, `OQC` carry names that were valid when ASFIS was
  written and have since been synonymised. `VMX` is *Valamugil*, a genus
  the backbone no longer carries at all; its species were split across
  *Osteomugil* and *Moolgarda* (6 species each at release 25.04), so
  both are searched. *Crenimugil* also absorbed some but carries 0
  species in the backbone, so listing it would only produce a standing
  unmatched-name warning.

`CRA` ("marine crabs nei", *Brachyura*) is deliberately absent.
Brachyura is an infraorder and SeaLifeBase carries no rank between order
*Decapoda* and family, so there is nothing to alias it to without
deciding which crab families Zanzibar actually lands. That is a local
question, not a lookup.

## Porting

Country-specific. The mechanism transfers unchanged; the rows do not.
Rebuild the table for each country's own taxa list.
