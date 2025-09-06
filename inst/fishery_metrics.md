# Fishery Metrics Dataset: Normalized Long Format

## Overview

This dataset provides standardized fishery performance indicators in a fully normalized long format structure. Each row represents a single metric observation for a specific landing site and month, enabling maximum interoperability and analytical flexibility for fishery data analysis.

## Format Structure

The dataset uses a **fully normalized long format** where:
- Each row contains one metric value
- All metrics are standardized into a consistent structure
- Related attributes (gear_type, species, rank) are stored as separate columns
- Missing values (NA) indicate the attribute is not applicable to that metric

## Schema

| Column Name  | Data Type | Description |
|-------------|-----------|-------------|
| `landing_site` | character | Unique identifier for fishing landing location |
| `year_month` | date | First day of each month (YYYY-MM-01 format) |
| `metric_type` | character | Type of metric being measured |
| `metric_value` | numeric | Numerical value of the metric |
| `gear_type` | character | Fishing gear type (NA for site-level metrics) |
| `species` | character | Fish species/category (NA for non-species metrics) |
| `rank` | integer | Rank order for ranked metrics (NA for others) |

## Metric Types

### Site-Level Metrics (gear_type = NA, species = NA, rank = NA)

#### `avg_fishers_per_trip`
- **Description**: Average number of fishers participating per fishing trip
- **Units**: Count (fishers)
- **Calculation**: Mean of `no_of_fishers` across all trips in site-month

#### `avg_catch_per_trip`
- **Description**: Average total catch weight per fishing trip
- **Units**: Kilograms (kg)
- **Calculation**: Sum `catch_kg` by trip, then calculate mean across trips

#### `pct_main_gear`
- **Description**: Percentage of trips using the predominant gear type
- **Units**: Percentage (%)
- **Range**: 0-100

### Gear-Specific Metrics (species = NA, rank = NA)

#### `predominant_gear`
- **Description**: Most frequently used fishing gear type
- **metric_value**: NA (gear name stored in `gear_type` column)
- **gear_type**: The predominant gear name
- **Calculation**: Mode of gear types across all trips

#### `cpue`
- **Description**: Catch Per Unit Effort by gear type
- **Units**: kg per fisher
- **gear_type**: Specific gear type (e.g., "traps", "nets", "hook and stick")
- **Calculation**: 
  1. Calculate trip-level CPUE: `trip_total_catch / no_of_fishers`
  2. Average across all trips using that gear in the site-month

#### `rpue`
- **Description**: Revenue Per Unit Effort by gear type
- **Units**: Currency per fisher (e.g., KES per fisher)
- **gear_type**: Specific gear type (e.g., "traps", "nets", "hook and stick")
- **Calculation**: 
  1. Calculate trip-level RPUE: `trip_total_revenue / no_of_fishers`
  2. Average across all trips using that gear in the site-month

### Species-Specific Metrics (gear_type = NA)

#### `species_pct`
- **Description**: Percentage of total catch comprised by each species
- **Units**: Percentage (%)
- **species**: Fish species/category name
- **rank**: Species rank by catch weight (1 = highest, 2 = second highest)
- **Note**: Only top 2 species per site-month are included

## Example Data Structure

```
landing_site | year_month | metric_type           | metric_value | gear_type | species    | rank
-------------|------------|----------------------|--------------|-----------|------------|------
bureni       | 2024-07-01 | avg_fishers_per_trip | 1.13         | NA        | NA         | NA
bureni       | 2024-07-01 | avg_catch_per_trip   | 1.70         | NA        | NA         | NA
bureni       | 2024-07-01 | predominant_gear     | NA           | nets      | NA         | NA
bureni       | 2024-07-01 | pct_main_gear        | 84.21        | NA        | NA         | NA
bureni       | 2024-07-01 | cpue                 | 2.50         | nets      | NA         | NA
bureni       | 2024-07-01 | cpue                 | 1.80         | traps     | NA         | NA
bureni       | 2024-07-01 | rpue                 | 150.25       | nets      | NA         | NA
bureni       | 2024-07-01 | rpue                 | 120.40       | traps     | NA         | NA
bureni       | 2024-07-01 | species_pct          | 69.90        | NA        | octopus    | 1
bureni       | 2024-07-01 | species_pct          | 14.50        | NA        | parrotfish | 2
```

## Usage Examples

### Filtering Data

```r
# Get all CPUE metrics
cpue_data <- fishery_metrics %>% 
  filter(metric_type == "cpue")

# Get all RPUE metrics
rpue_data <- fishery_metrics %>% 
  filter(metric_type == "rpue")

# Get predominant gear by site
main_gear <- fishery_metrics %>% 
  filter(metric_type == "predominant_gear") %>%
  select(landing_site, year_month, gear_type)

# Get top species by site and month
species_data <- fishery_metrics %>% 
  filter(metric_type == "species_pct") %>%
  arrange(landing_site, year_month, rank)

# Get site-level metrics only
site_metrics <- fishery_metrics %>%
  filter(is.na(gear_type) & is.na(species))
```

### Comparative Analysis

```r
# Compare CPUE across gear types
gear_comparison <- fishery_metrics %>%
  filter(metric_type == "cpue") %>%
  group_by(gear_type) %>%
  summarise(
    avg_cpue = mean(metric_value, na.rm = TRUE),
    sites_using_gear = n_distinct(landing_site)
  )

# Compare RPUE across gear types
revenue_comparison <- fishery_metrics %>%
  filter(metric_type == "rpue") %>%
  group_by(gear_type) %>%
  summarise(
    avg_rpue = mean(metric_value, na.rm = TRUE),
    sites_using_gear = n_distinct(landing_site)
  )

# Temporal trends for specific metrics
effort_trends <- fishery_metrics %>%
  filter(metric_type == "avg_fishers_per_trip") %>%
  group_by(year_month) %>%
  summarise(mean_effort = mean(metric_value, na.rm = TRUE))
```

### Visualization Preparation

```r
# Prepare data for gear CPUE comparison chart
cpue_viz <- fishery_metrics %>%
  filter(metric_type == "cpue", !is.na(gear_type)) %>%
  group_by(gear_type) %>%
  summarise(
    median_cpue = median(metric_value, na.rm = TRUE),
    q25 = quantile(metric_value, 0.25, na.rm = TRUE),
    q75 = quantile(metric_value, 0.75, na.rm = TRUE)
  )
```

## Advantages of This Format

### 1. **Maximum Interoperability**
- Compatible with all major data analysis tools (R, Python, SQL)
- Easy integration with databases and data pipelines
- Standard format recognized by visualization libraries

### 2. **Analytical Flexibility**
- Simple filtering by metric type, gear, or species
- Easy aggregation across any dimension
- Supports complex queries without pivoting

### 3. **Scalability**
- New metric types can be added without schema changes
- Efficient storage of sparse data (many NAs in wide format)
- Database-optimized structure

### 4. **Future-Proof Design**
- Independent of specific gear types or species lists
- Accommodates varying numbers of gears/species across sites
- Extensible for additional attributes

## Data Requirements

Source data must contain these columns:
- `submission_id`: Unique trip identifier
- `landing_date`: Date/datetime of landing
- `landing_site`: Landing location identifier
- `gear`: Fishing gear type
- `no_of_fishers`: Number of fishers on trip
- `fish_category`: Species or species group name
- `catch_kg`: Catch weight in kilograms
- `total_catch_price`: Total revenue from catch in local currency

## AI Service Integration

This format is optimized for:
- **Machine Learning**: Direct input for algorithms without preprocessing
- **API Responses**: Efficient JSON serialization
- **Time Series Analysis**: Natural temporal structure
- **Cross-System Integration**: Standard relational format
- **Automated Reporting**: Consistent metric identification

## Limitations

- Larger file sizes compared to wide format (due to repeated site/month values)
- Requires filtering to reconstruct site-month summary views
- May need additional context for metric interpretation by end users

## Migration from Wide Format

If you need wide format for specific use cases:

```r
# Convert back to wide format for site-level metrics
wide_format <- fishery_metrics %>%
  filter(is.na(gear_type) & is.na(species)) %>%
  select(landing_site, year_month, metric_type, metric_value) %>%
  pivot_wider(names_from = metric_type, values_from = metric_value)

# Convert CPUE to wide format with gear columns
cpue_wide <- fishery_metrics %>%
  filter(metric_type == "cpue") %>%
  select(landing_site, year_month, gear_type, metric_value) %>%
  pivot_wider(
    names_from = gear_type, 
    values_from = metric_value, 
    names_prefix = "cpue_"
  )
```