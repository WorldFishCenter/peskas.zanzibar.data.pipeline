# Process Submissions with Rate-Limited Parallel API Calls

Helper function to process multiple submissions in parallel with rate
limiting, progress tracking, and error logging. This function is
designed to handle KoboToolbox API calls efficiently while respecting
rate limits.

## Usage

``` r
process_submissions_parallel(
  submission_ids,
  process_fn,
  description = "submissions",
  rate_limit = 0.2
)
```

## Arguments

- submission_ids:

  Character vector of submission IDs to process

- process_fn:

  Function to apply to each submission ID. Should return a data frame.

- description:

  Character string describing what's being processed (for logging)

- rate_limit:

  Numeric delay in seconds between requests (default: 0.2)

## Value

Data frame with results from process_fn for all submissions
