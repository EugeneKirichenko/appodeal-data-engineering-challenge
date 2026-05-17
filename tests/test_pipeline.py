import polars as pl
import pytest

from src.main import (
    build_impression_fact,
    calculate_median_user_spend,
    calculate_metrics,
    normalize_inputs,
    recommend_advertisers,
)


def test_metrics_and_median_spend():
    impressions = pl.DataFrame(
        [
            {"id": "i1", "user_id": "u1", "app_id": 1, "country_code": "US", "advertiser_id": 10},
            {"id": "i2", "user_id": "u1", "app_id": 1, "country_code": "US", "advertiser_id": 10},
            {"id": "i3", "user_id": "u2", "app_id": 1, "country_code": "US", "advertiser_id": 20},
        ]
    )
    clicks = pl.DataFrame(
        [
            {"id": "c1", "impression_id": "i1", "revenue": 1.5},
            {"id": "c2", "impression_id": "i2", "revenue": 2.5},
        ]
    )

    fact = build_impression_fact(impressions, clicks)
    metrics = calculate_metrics(fact).to_dicts()
    median = calculate_median_user_spend(fact).to_dicts()

    assert metrics == [
        {"app_id": 1, "country_code": "US", "impressions": 3, "clicks": 2, "revenue": 4.0}
    ]
    assert median == [{"country_code": "US", "median_spend": 2.0}]


def test_recommend_advertisers_min_impressions():
    impressions = pl.DataFrame(
        [
            *[{"id": f"a{i}", "user_id": f"u{i}", "app_id": 1, "country_code": "US", "advertiser_id": 100} for i in range(5)],
            *[{"id": f"b{i}", "user_id": f"v{i}", "app_id": 1, "country_code": "US", "advertiser_id": 200} for i in range(4)],
        ]
    )
    clicks = pl.DataFrame(
        [{"id": "c1", "impression_id": "a0", "revenue": 10.0}]
    )

    fact = build_impression_fact(impressions, clicks)
    result = recommend_advertisers(fact, min_impressions=5, top_n=5).to_dicts()

    assert result == [
        {"app_id": 1, "country_code": "US", "recommended_advertiser_ids": [100]}
    ]


def test_normalize_inputs_removes_nulls_invalid_revenue_and_duplicates():
    impressions = pl.DataFrame(
        [
            {"id": "i1", "user_id": "u1", "app_id": "1", "country_code": "US", "advertiser_id": "10"},
            {"id": "i1", "user_id": "u1", "app_id": "1", "country_code": "US", "advertiser_id": "10"},
            {"id": None, "user_id": "u2", "app_id": "1", "country_code": "US", "advertiser_id": "20"},
        ]
    )
    clicks = pl.DataFrame(
        [
            {"id": "c1", "impression_id": "i1", "revenue": "2.5"},
            {"id": "c1", "impression_id": "i1", "revenue": "2.5"},
            {"id": "c2", "impression_id": "i1", "revenue": "bad-value"},
        ]
    )

    clean_impressions, clean_clicks = normalize_inputs(impressions, clicks)

    assert clean_impressions.height == 1
    assert clean_clicks.height == 1
    assert clean_impressions.schema["app_id"] == pl.Int64
    assert clean_clicks.schema["revenue"] == pl.Float64


def test_normalize_inputs_requires_columns():
    impressions = pl.DataFrame([{"id": "i1"}])
    clicks = pl.DataFrame([{"id": "c1", "impression_id": "i1", "revenue": 1.0}])

    with pytest.raises(ValueError, match="Missing impression columns"):
        normalize_inputs(impressions, clicks)
