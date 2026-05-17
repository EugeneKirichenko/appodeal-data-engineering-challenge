# Appodeal Data Engineering Challenge Solution

This repository contains a Python solution for the Appodeal Data Engineering Challenge.

The application reads impression and click event JSON files, calculates the requested business outputs, writes each result to a separate JSON file, and prints the results to the terminal.

## Problem Summary

The task is to process two input datasets:

- impression events
- click events

The application produces three outputs:

1. Metrics by `app_id` and `country_code`
   - impressions count
   - clicks count
   - revenue sum

2. Top advertiser recommendations by `app_id` and `country_code`
   - top 5 `advertiser_id` values by `revenue / impressions`
   - only advertisers with at least 5 impressions are considered

3. Median user spend by country
   - spend is calculated as user-level total revenue
   - users with impressions and no clicks are included with zero spend

## Technology Choice

The solution uses **Polars** instead of Spark because the challenge is expected to run on a single machine with 8 cores.

Polars is a good fit here because it is:

- lightweight and easy to run on a regular laptop
- multi-threaded by default
- fast for JSON-based analytical workloads
- much simpler to set up than Spark for this task

Docker and Docker Compose are included to make execution reproducible.

## Project Structure

```text
.
├── src/
│   ├── __init__.py
│   └── main.py
├── tests/
│   └── test_pipeline.py
├── output/
├── config.yaml
├── requirements.txt
├── Dockerfile
├── docker-compose.yml
├── run.sh
├── run.ps1
├── .dockerignore
├── .gitignore
└── README.md
```

## Configuration

Input files, processing parameters, output options, and logging settings are stored in `config.yaml`.

```yaml
input:
  click_files:
    - "https://gist.githubusercontent.com/mpasa/e7db25ec3d9146502cfc62384e6d315c/raw/clicks.json"
  impression_files:
    - "https://gist.githubusercontent.com/mpasa/e7db25ec3d9146502cfc62384e6d315c/raw/impressions.json"

processing:
  max_workers: 8
  request_timeout_seconds: 30
  min_advertiser_impressions: 5
  top_n_advertisers: 5

output:
  directory: "output"
  print_results: true

logging:
  level: "INFO"
  file_name: "pipeline.log"

validation:
  drop_duplicates: true
  fail_on_empty_input: true
  allow_negative_revenue: true
  fail_on_unmatched_clicks: false
  strict_schema: true
```

Both local paths and HTTPS URLs are supported. Logging settings are also config-driven. The log file is written into the configured output directory, for example `output/pipeline.log`.

### Configurable Settings

The following values can be changed without editing Python code:

- input click and impression files or HTTPS URLs
- number of parallel workers
- HTTP request timeout
- minimum impression threshold for advertiser recommendations
- number of recommended advertisers
- output directory
- terminal result printing
- logging level and log file name
- lightweight data validation behavior

## Run Locally Without Docker

Requirements:

- Python 3.11+

Create a virtual environment:

```bash
python -m venv .venv
source .venv/bin/activate
```

On Windows PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

Install dependencies:

```bash
pip install -r requirements.txt
```

Run the pipeline:

```bash
python src/main.py --config config.yaml
```

## Run With Docker

Build the Docker image:

```bash
docker build -t appodeal-de-challenge .
```

Run the container:

```bash
docker run --rm -v "$(pwd)/output:/app/output" appodeal-de-challenge
```

On Windows PowerShell:

```powershell
docker run --rm -v ${PWD}/output:/app/output appodeal-de-challenge
```

## Run With Docker Compose

```bash
docker compose up --build
```

This command builds the image, runs the pipeline, and writes the JSON outputs to the local `output` directory.

## Run Scripts

Linux / macOS:

```bash
chmod +x run.sh
./run.sh
```

Windows PowerShell:

```powershell
.\run.ps1
```

The scripts check whether Docker is available, build the image, run the pipeline, and mount the local `output` directory into the container.

## CLI Overrides

You can override input files from the command line:

```bash
python src/main.py \
  --clicks data/clicks_1.json data/clicks_2.json \
  --impressions data/impressions_1.json data/impressions_2.json \
  --output-dir output
```

The same works with URLs:

```bash
python src/main.py \
  --clicks https://example.com/clicks.json \
  --impressions https://example.com/impressions.json
```

## Output Files

The application writes three JSON result files and one log file:

```text
output/metrics_by_app_country.json
output/recommended_advertisers.json
output/median_user_spend_by_country.json
output/pipeline.log
```

The JSON results are also printed to the terminal when `output.print_results` is set to `true` in `config.yaml`. Logs are written both to the terminal and to `output/pipeline.log`.


## Data Validation and Quality Checks

The pipeline includes lightweight validation checks that are useful for a laptop-friendly test assignment without adding a heavy validation framework.

Implemented checks:

- input source errors: HTTP, file, and invalid JSON errors are handled with clear messages
- JSON structure: each input source must contain a JSON array of objects
- required columns: the pipeline validates mandatory columns before processing
- extra columns: extra fields are logged and ignored when `validation.strict_schema` is enabled
- type casting: IDs, app IDs, advertiser IDs, and revenue are cast to expected types
- null handling: rows with null required fields are removed and logged
- duplicate handling: duplicate impressions and clicks are removed by ID when `validation.drop_duplicates` is enabled
- invalid revenue: non-numeric, NaN, and infinite revenue values are removed and logged
- negative revenue: kept by default as potential corrections or refunds, controlled by `validation.allow_negative_revenue`
- unmatched clicks: clicks without matching impressions are logged and ignored by default, controlled by `validation.fail_on_unmatched_clicks`
- safe division: advertiser revenue-per-impression calculation protects against division by zero

These checks are configurable in `config.yaml`:

```yaml
validation:
  drop_duplicates: true
  fail_on_empty_input: true
  allow_negative_revenue: true
  fail_on_unmatched_clicks: false
  strict_schema: true
```

Validation warnings are written both to the terminal and to `output/pipeline.log`.

## Data Processing Notes

### Metrics

Clicks are joined to impressions using:

```text
click.impression_id = impression.id
```

Unmatched clicks are ignored because they cannot be attributed to an app, country, advertiser, or user.

### Advertiser Recommendations

For each `(app_id, country_code, advertiser_id)` group:

```text
revenue_per_impression = total_revenue / impressions
```

Only advertisers with at least 5 impressions are considered.

### Median User Spend

User spend is calculated per `(country_code, user_id)` as total click revenue linked to that user's impressions.

Users who had impressions but no clicks are included with spend equal to zero.

## Tests

Run tests locally:

```bash
pytest
```

Run tests inside Docker:

```bash
docker build -t appodeal-de-challenge .
docker run --rm appodeal-de-challenge pytest
```

## Assumptions

- Input files are JSON arrays.
- Click revenue is attributed to the impression referenced by `impression_id`.
- Multiple clicks for the same impression are supported and aggregated.
- Negative revenue values are not removed because the challenge data may include real-world correction/refund scenarios.
- Users with impressions and no clicks are included in median spend calculation with zero revenue.

## Future Improvements

For a production-grade version, I would consider adding:

- structured JSON logging
- detailed data quality report output as a separate JSON artifact
- support for newline-delimited JSON
- output schema validation
- partitioned Parquet output
- CI pipeline with linting and tests
- optional cloud storage support, for example S3
