from __future__ import annotations

import argparse
import json
import logging
import math
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import polars as pl
import requests
import yaml


@dataclass(frozen=True)
class AppConfig:
    click_files: list[str]
    impression_files: list[str]
    output_dir: Path
    min_advertiser_impressions: int
    top_n_advertisers: int
    max_workers: int
    request_timeout_seconds: int
    print_results: bool
    log_level: str
    log_file: Path
    drop_duplicates: bool
    fail_on_empty_input: bool
    allow_negative_revenue: bool
    fail_on_unmatched_clicks: bool
    strict_schema: bool


def setup_logging(log_level: str, log_file: Path) -> None:
    """Configure logging to both terminal and a file under the output directory."""
    log_file.parent.mkdir(parents=True, exist_ok=True)

    numeric_level = getattr(logging, log_level.upper(), logging.INFO)

    logging.basicConfig(
        level=numeric_level,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_file, encoding="utf-8"),
        ],
        force=True,
    )


def is_url(path: str) -> bool:
    parsed = urlparse(path)
    return parsed.scheme in {"http", "https"}


def read_json_records(source: str, timeout_seconds: int) -> list[dict[str, Any]]:
    """Read a JSON array from a local file path or URL."""
    logging.info("Reading input source: %s", source)

    try:
        if is_url(source):
            response = requests.get(source, timeout=timeout_seconds)
            response.raise_for_status()
            data = response.json()
        else:
            with Path(source).open("r", encoding="utf-8") as file:
                data = json.load(file)
    except requests.RequestException as exc:
        raise RuntimeError(f"HTTP error while reading {source}: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON in source {source}: {exc}") from exc
    except OSError as exc:
        raise RuntimeError(f"File error while reading {source}: {exc}") from exc

    if not isinstance(data, list):
        raise ValueError(f"Input source must contain a JSON array: {source}")

    non_object_count = sum(1 for row in data if not isinstance(row, dict))
    if non_object_count:
        raise ValueError(f"Input source contains {non_object_count} non-object rows: {source}")

    return data


def load_many_json_files(sources: list[str], max_workers: int, timeout_seconds: int) -> pl.DataFrame:
    """Load multiple JSON files in parallel and return a single Polars DataFrame."""
    if not sources:
        raise ValueError("At least one JSON input source must be provided.")

    records: list[dict[str, Any]] = []
    worker_count = min(max_workers, len(sources)) or 1

    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        futures = {
            executor.submit(read_json_records, source, timeout_seconds): source
            for source in sources
        }

        for future in as_completed(futures):
            source = futures[future]
            try:
                source_records = future.result()
                logging.info("Loaded %s records from %s", len(source_records), source)
                records.extend(source_records)
            except Exception as exc:
                raise RuntimeError(f"Failed to load source {source}: {exc}") from exc

    if not records:
        return pl.DataFrame()

    return pl.from_dicts(records, infer_schema_length=None)


def _log_extra_columns(df: pl.DataFrame, required_columns: set[str], dataset_name: str) -> None:
    extra_columns = sorted(set(df.columns) - required_columns)
    if extra_columns:
        logging.info("%s dataset contains extra columns that will be ignored: %s", dataset_name, extra_columns)


def _validate_required_columns(df: pl.DataFrame, required_columns: set[str], dataset_name: str) -> None:
    missing_columns = sorted(required_columns - set(df.columns))
    if missing_columns:
        raise ValueError(f"Missing {dataset_name} columns: {missing_columns}")


def _drop_null_rows(df: pl.DataFrame, required_columns: list[str], dataset_name: str) -> pl.DataFrame:
    before_count = df.height
    for column in required_columns:
        null_count = df.filter(pl.col(column).is_null()).height
        if null_count:
            logging.warning("%s dataset has %s rows with null %s", dataset_name, null_count, column)

    cleaned = df.drop_nulls(required_columns)
    removed_count = before_count - cleaned.height
    if removed_count:
        logging.warning("Removed %s rows from %s dataset because required fields were null", removed_count, dataset_name)

    return cleaned


def _drop_duplicate_rows(df: pl.DataFrame, subset: list[str], dataset_name: str) -> pl.DataFrame:
    before_count = df.height
    cleaned = df.unique(subset=subset, keep="first", maintain_order=True)
    duplicate_count = before_count - cleaned.height
    if duplicate_count:
        logging.warning("Removed %s duplicate rows from %s dataset using key columns %s", duplicate_count, dataset_name, subset)
    return cleaned


def _cast_column(df: pl.DataFrame, column: str, dtype: pl.DataType, dataset_name: str) -> pl.DataFrame:
    before_nulls = df.filter(pl.col(column).is_null()).height
    casted = df.with_columns(pl.col(column).cast(dtype, strict=False).alias(column))
    after_nulls = casted.filter(pl.col(column).is_null()).height
    invalid_count = after_nulls - before_nulls

    if invalid_count > 0:
        logging.warning(
            "%s dataset has %s invalid values in %s after casting to %s",
            dataset_name,
            invalid_count,
            column,
            dtype,
        )

    return casted


def _safe_bool(value: Any, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"true", "1", "yes", "y"}
    return bool(value)


def normalize_inputs(
    impressions: pl.DataFrame,
    clicks: pl.DataFrame,
    *,
    drop_duplicates: bool = True,
    fail_on_empty_input: bool = True,
    allow_negative_revenue: bool = True,
    strict_schema: bool = True,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Validate, clean, and normalize raw input datasets."""
    required_impression_cols = {"id", "user_id", "app_id", "country_code", "advertiser_id"}
    required_click_cols = {"id", "impression_id", "revenue"}

    if impressions.is_empty():
        message = "Impression dataset is empty."
        if fail_on_empty_input:
            raise ValueError(message)
        logging.warning(message)

    if clicks.is_empty():
        message = "Click dataset is empty. Revenue and click metrics will be zero."
        if fail_on_empty_input:
            raise ValueError(message)
        logging.warning(message)

    _validate_required_columns(impressions, required_impression_cols, "impression")
    _validate_required_columns(clicks, required_click_cols, "click")
    _log_extra_columns(impressions, required_impression_cols, "impression")
    _log_extra_columns(clicks, required_click_cols, "click")

    if strict_schema:
        impressions = impressions.select(sorted(required_impression_cols))
        clicks = clicks.select(sorted(required_click_cols))

    # Cast with strict=False to detect and then remove invalid values instead of crashing with a cryptic error.
    for column in ["id", "user_id", "country_code"]:
        impressions = _cast_column(impressions, column, pl.Utf8, "impression")
    for column in ["app_id", "advertiser_id"]:
        impressions = _cast_column(impressions, column, pl.Int64, "impression")

    clicks = _cast_column(clicks, "id", pl.Utf8, "click")
    clicks = _cast_column(clicks, "impression_id", pl.Utf8, "click")
    clicks = _cast_column(clicks, "revenue", pl.Float64, "click")

    impressions = _drop_null_rows(
        impressions,
        ["id", "user_id", "app_id", "country_code", "advertiser_id"],
        "impression",
    )
    clicks = _drop_null_rows(clicks, ["id", "impression_id", "revenue"], "click")

    # Remove NaN/Inf revenue values after casting.
    invalid_revenue_count = clicks.filter(~pl.col("revenue").is_finite()).height
    if invalid_revenue_count:
        logging.warning("Removed %s click rows with NaN or infinite revenue", invalid_revenue_count)
        clicks = clicks.filter(pl.col("revenue").is_finite())

    negative_revenue_count = clicks.filter(pl.col("revenue") < 0).height
    if negative_revenue_count:
        message = f"Click dataset contains {negative_revenue_count} rows with negative revenue"
        if allow_negative_revenue:
            logging.warning("%s. Keeping them as possible corrections/refunds.", message)
        else:
            logging.warning("%s. Removing them based on config.", message)
            clicks = clicks.filter(pl.col("revenue") >= 0)

    if drop_duplicates:
        impressions = _drop_duplicate_rows(impressions, ["id"], "impression")
        clicks = _drop_duplicate_rows(clicks, ["id"], "click")

    if impressions.is_empty():
        raise ValueError("No valid impression rows remain after validation.")

    if clicks.is_empty():
        logging.warning("No valid click rows remain after validation. Continuing with zero click/revenue metrics.")

    logging.info("Validation completed. Valid impressions: %s, valid clicks: %s", impressions.height, clicks.height)

    return impressions, clicks


def build_impression_fact(
    impressions: pl.DataFrame,
    clicks: pl.DataFrame,
    *,
    fail_on_unmatched_clicks: bool = False,
) -> pl.DataFrame:
    """Build one row per impression with click count and revenue attached."""
    if clicks.is_empty():
        click_agg = pl.DataFrame(
            schema={
                "impression_id": pl.Utf8,
                "clicks": pl.UInt32,
                "revenue": pl.Float64,
            }
        )
    else:
        unmatched_clicks = clicks.join(
            impressions.select(pl.col("id").alias("impression_id")),
            on="impression_id",
            how="anti",
        )
        if unmatched_clicks.height:
            message = f"Found {unmatched_clicks.height} clicks without a matching impression"
            if fail_on_unmatched_clicks:
                raise ValueError(message)
            logging.warning("%s. They will be ignored because attribution is not possible.", message)

        click_agg = (
            clicks.group_by("impression_id")
            .agg(
                pl.len().alias("clicks"),
                pl.col("revenue").sum().alias("revenue"),
            )
        )

    return (
        impressions.join(click_agg, left_on="id", right_on="impression_id", how="left")
        .with_columns(
            pl.col("clicks").fill_null(0).cast(pl.Int64),
            pl.col("revenue").fill_null(0.0).cast(pl.Float64),
        )
    )


def calculate_metrics(fact: pl.DataFrame) -> pl.DataFrame:
    return (
        fact.group_by(["app_id", "country_code"])
        .agg(
            pl.len().alias("impressions"),
            pl.col("clicks").sum().alias("clicks"),
            pl.col("revenue").sum().round(6).alias("revenue"),
        )
        .sort(["app_id", "country_code"])
    )


def recommend_advertisers(
    fact: pl.DataFrame,
    min_impressions: int,
    top_n: int,
) -> pl.DataFrame:
    advertiser_perf = (
        fact.group_by(["app_id", "country_code", "advertiser_id"])
        .agg(
            pl.len().alias("impressions"),
            pl.col("revenue").sum().alias("revenue"),
        )
        .filter(pl.col("impressions") >= min_impressions)
        .with_columns(
            pl.when(pl.col("impressions") > 0)
            .then(pl.col("revenue") / pl.col("impressions"))
            .otherwise(0.0)
            .alias("revenue_per_impression")
        )
        .sort(
            ["app_id", "country_code", "revenue_per_impression", "advertiser_id"],
            descending=[False, False, True, False],
        )
    )

    if advertiser_perf.is_empty():
        return pl.DataFrame(
            schema={
                "app_id": pl.Int64,
                "country_code": pl.Utf8,
                "recommended_advertiser_ids": pl.List(pl.Int64),
            }
        )

    return (
        advertiser_perf.group_by(["app_id", "country_code"], maintain_order=True)
        .agg(pl.col("advertiser_id").head(top_n).alias("recommended_advertiser_ids"))
        .sort(["app_id", "country_code"])
    )


def calculate_median_user_spend(fact: pl.DataFrame) -> pl.DataFrame:
    user_spend = (
        fact.group_by(["country_code", "user_id"])
        .agg(pl.col("revenue").sum().alias("user_spend"))
    )

    return (
        user_spend.group_by("country_code")
        .agg(pl.col("user_spend").median().round(6).alias("median_spend"))
        .sort("country_code")
    )


def dataframe_to_records(df: pl.DataFrame) -> list[dict[str, Any]]:
    records = df.to_dicts()
    # Convert any Polars-specific nested values to plain JSON-safe values.
    return json.loads(json.dumps(records))


def write_json(records: list[dict[str, Any]], output_path: Path, print_results: bool) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as file:
        json.dump(records, file, ensure_ascii=False, indent=2)
        file.write("\n")

    logging.info("Written output file: %s", output_path)

    if print_results:
        print(f"\n=== {output_path.name} ===")
        print(json.dumps(records, ensure_ascii=False, indent=2))


def load_config(config_path: Path) -> dict[str, Any]:
    with config_path.open("r", encoding="utf-8") as file:
        return yaml.safe_load(file) or {}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Appodeal Data Engineering Challenge solution")
    parser.add_argument("--config", default="config.yaml", help="Path to YAML config file")
    parser.add_argument("--clicks", nargs="+", help="Click JSON files or URLs. Overrides config.")
    parser.add_argument("--impressions", nargs="+", help="Impression JSON files or URLs. Overrides config.")
    parser.add_argument("--output-dir", help="Output directory. Overrides config.")
    return parser.parse_args()


def build_config(args: argparse.Namespace) -> AppConfig:
    raw = load_config(Path(args.config))

    input_config = raw.get("input", {})
    processing_config = raw.get("processing", {})
    output_config = raw.get("output", {})
    logging_config = raw.get("logging", {})
    validation_config = raw.get("validation", {})

    click_files = args.clicks or input_config.get("click_files", [])
    impression_files = args.impressions or input_config.get("impression_files", [])
    output_dir = Path(args.output_dir or output_config.get("directory", "output"))
    log_file_name = logging_config.get("file_name", "pipeline.log")
    log_file = output_dir / log_file_name

    return AppConfig(
        click_files=click_files,
        impression_files=impression_files,
        output_dir=output_dir,
        min_advertiser_impressions=int(processing_config.get("min_advertiser_impressions", 5)),
        top_n_advertisers=int(processing_config.get("top_n_advertisers", 5)),
        max_workers=int(processing_config.get("max_workers", 8)),
        request_timeout_seconds=int(processing_config.get("request_timeout_seconds", 30)),
        print_results=_safe_bool(output_config.get("print_results"), True),
        log_level=str(logging_config.get("level", "INFO")),
        log_file=log_file,
        drop_duplicates=_safe_bool(validation_config.get("drop_duplicates"), True),
        fail_on_empty_input=_safe_bool(validation_config.get("fail_on_empty_input"), True),
        allow_negative_revenue=_safe_bool(validation_config.get("allow_negative_revenue"), True),
        fail_on_unmatched_clicks=_safe_bool(validation_config.get("fail_on_unmatched_clicks"), False),
        strict_schema=_safe_bool(validation_config.get("strict_schema"), True),
    )


def run(config: AppConfig) -> None:
    logging.info("Starting Appodeal challenge pipeline")
    logging.info("Click sources: %s", config.click_files)
    logging.info("Impression sources: %s", config.impression_files)

    clicks = load_many_json_files(config.click_files, config.max_workers, config.request_timeout_seconds)
    impressions = load_many_json_files(config.impression_files, config.max_workers, config.request_timeout_seconds)
    impressions, clicks = normalize_inputs(
        impressions,
        clicks,
        drop_duplicates=config.drop_duplicates,
        fail_on_empty_input=config.fail_on_empty_input,
        allow_negative_revenue=config.allow_negative_revenue,
        strict_schema=config.strict_schema,
    )

    fact = build_impression_fact(impressions, clicks, fail_on_unmatched_clicks=config.fail_on_unmatched_clicks)

    metrics = calculate_metrics(fact)
    recommendations = recommend_advertisers(
        fact,
        min_impressions=config.min_advertiser_impressions,
        top_n=config.top_n_advertisers,
    )
    median_spend = calculate_median_user_spend(fact)

    write_json(dataframe_to_records(metrics), config.output_dir / "metrics_by_app_country.json", config.print_results)
    write_json(dataframe_to_records(recommendations), config.output_dir / "recommended_advertisers.json", config.print_results)
    write_json(dataframe_to_records(median_spend), config.output_dir / "median_user_spend_by_country.json", config.print_results)

    logging.info("Pipeline completed successfully")


def main() -> int:
    try:
        args = parse_args()
        config = build_config(args)
        setup_logging(config.log_level, config.log_file)
        logging.info("Log file: %s", config.log_file)
        run(config)
        return 0
    except Exception as exc:
        if logging.getLogger().handlers:
            logging.exception("Pipeline failed: %s", exc)
        else:
            print(f"Pipeline failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
