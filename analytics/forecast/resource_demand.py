
"""
EpiScope AN-6
Forecast -> Resource Demand

Purpose
-------
Convert short-term disease forecasts into operational demand estimates for:

1. Hospital beds
2. Saline units
3. Diagnostic test kits

The model uses:
- Gold feature mart
- S5 facility registry
- S6 latest stock/bed snapshot

Outputs:
- District + disease + horizon demand
- Point estimate
- Lower / upper uncertainty bounds
- Required beds
- Saline demand
- Test-kit demand
- Current available beds
- Current saline stock
- Current test-kit stock
- Resource gaps
- Assumptions metadata

Important
---------
This script does NOT read ground_truth_outbreaks.
"""

from __future__ import annotations

import json
import math
import sys
from pathlib import Path
from datetime import datetime

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))
from district_names import canonical_district_name_column


# ============================================================
# PATHS
# ============================================================

FEATURE_PATH = (
    PROJECT_ROOT
    / "data"
    / "gold"
    / "features"
    / "district_disease_epiweek_features"
)

FACILITY_PATH = (
    PROJECT_ROOT
    / "data"
    / "silver"
    / "S5_facilities"
)

STOCK_PATH = (
    PROJECT_ROOT
    / "data"
    / "silver"
    / "S6_stock_beds"
)

OUTPUT_ROOT = (
    PROJECT_ROOT
    / "data"
    / "gold"
    / "analytics"
    / "resource_demand"
)

DEMAND_OUTPUT = OUTPUT_ROOT / "forecast_resource_demand"
SUMMARY_OUTPUT = OUTPUT_ROOT / "resource_demand_summary.json"
ASSUMPTIONS_OUTPUT = OUTPUT_ROOT / "demand_assumptions.json"
DISTRICT_DEMAND_OUTPUT = OUTPUT_ROOT / "four_week_district_demand.csv"


# ============================================================
# MODEL / RESOURCE ASSUMPTIONS
# ============================================================

# Forecast horizons
FORECAST_HORIZONS = [1, 2, 3, 4]

# Operational resource assumptions.
#
# These are explicit planning assumptions, NOT clinical guidelines.
#
# BED:
# proportion of reported cases expected to require an occupied bed.
BED_RATE = 0.08

# SALINE:
# average saline units per reported case.
SALINE_UNITS_PER_CASE = 1.20

# TEST KIT:
# tests required per reported case.
TEST_KITS_PER_CASE = 1.50

# Additional operational safety buffer.
#
# Demand upper bound:
# point demand * (1 + uncertainty buffer)
UNCERTAINTY_BUFFER = 0.25

# Stock planning buffer.
STOCK_SAFETY_BUFFER = 0.10

# Small numerical protection.
EPSILON = 1e-9


# ============================================================
# SPARK
# ============================================================

def create_spark() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("EpiScope_AN6_Resource_Demand")
        .master("local[*]")
        .config("spark.driver.memory", "5g")
        .config("spark.executor.memory", "5g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.default.parallelism", "8")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.autoBroadcastJoinThreshold", "16MB")
        .config("spark.sql.debug.maxToStringFields", "200")
        .config("spark.python.worker.reuse", "true")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark


# ============================================================
# HELPERS
# ============================================================

def check_path(path: Path, description: str) -> None:
    if not path.exists():
        raise FileNotFoundError(
            f"{description} not found:\n{path}"
        )


def safe_float(value, default=0.0):
    if value is None:
        return default

    try:
        value = float(value)

        if not math.isfinite(value):
            return default

        return value

    except Exception:
        return default


# ============================================================
# LOAD FEATURE MART
# ============================================================

def load_features(spark: SparkSession):
    check_path(
        FEATURE_PATH,
        "Gold feature mart"
    )

    df = spark.read.parquet(str(FEATURE_PATH))

    required = [
        "district_name",
        "disease",
        "epi_week_start",
        "case_count",
        "population_total",
        "incidence_per_100k",
    ]

    missing = [
        c for c in required
        if c not in df.columns
    ]

    if missing:
        raise ValueError(
            "Feature mart is missing required columns: "
            + ", ".join(missing)
        )

    df = (
        df
        .withColumn(
            "epi_week_start",
            F.to_date("epi_week_start")
        )
        .withColumn(
            "case_count",
            F.coalesce(
                F.col("case_count").cast("double"),
                F.lit(0.0)
            )
        )
        .withColumn(
            "population_total",
            F.col("population_total").cast("double")
        )
    )

    return df


# ============================================================
# BUILD 52-WEEK SEASONAL-NAIVE FORECAST
# ============================================================

def build_forecast_base(df):
    """
    Operational forecast:

    For each district+disease:
        forecast(h) = case count from approximately 52 weeks earlier.

    If the 52-week observation is unavailable, use the latest
    available 4-week average.

    This is intentionally robust and lightweight for the
    16 GB laptop environment.
    """

    current = df.select(
        "district_name",
        "disease",
        F.col("epi_week_start").alias("forecast_origin"),
        F.col("case_count").alias("current_cases"),
        "population_total",
    )

    historical = df.select(
        "district_name",
        "disease",
        F.date_add(
            F.col("epi_week_start"),
            364
        ).alias("forecast_origin"),
        F.col("case_count").alias("seasonal_case_count"),
    )

    joined = (
        current
        .join(
            historical,
            on=[
                "district_name",
                "disease",
                "forecast_origin",
            ],
            how="left"
        )
    )

    # Window for latest 4-week average.
    w = (
        Window
        .partitionBy("district_name", "disease")
        .orderBy("forecast_origin")
        .rowsBetween(-4, -1)
    )

    joined = joined.withColumn(
        "recent_4w_average",
        F.avg("current_cases").over(w)
    )

    joined = joined.withColumn(
        "base_forecast_cases",
        F.coalesce(
            F.col("seasonal_case_count"),
            F.col("recent_4w_average"),
            F.col("current_cases"),
            F.lit(0.0)
        )
    )

    return joined


# ============================================================
# SELECT LATEST FORECAST ORIGIN
# ============================================================

def select_latest_origin(df):
    latest_row = (
        df
        .select(F.max("epi_week_start").alias("latest"))
        .collect()[0]
    )

    latest = latest_row["latest"]

    if latest is None:
        raise ValueError(
            "Could not determine latest epi-week."
        )

    return latest


# ============================================================
# BUILD FOUR-WEEK FORECAST
# ============================================================

def build_four_week_forecast(
    feature_df,
    latest_origin
):
    base = build_forecast_base(feature_df)

    latest = (
        base
        .filter(
            F.col("forecast_origin") == F.lit(latest_origin)
        )
        .select(
            "district_name",
            "disease",
            "forecast_origin",
            "population_total",
            "current_cases",
            "base_forecast_cases",
        )
    )

    # Add horizons 1-4.
    horizon_dfs = []

    for horizon in FORECAST_HORIZONS:

        horizon_df = (
            latest
            .withColumn(
                "forecast_horizon_week",
                F.lit(horizon)
            )
            .withColumn(
                "forecast_week_start",
                F.date_add(
                    F.col("forecast_origin"),
                    horizon * 7
                )
            )
            .withColumn(
                "forecast_cases",
                F.greatest(
                    F.lit(0.0),
                    F.col("base_forecast_cases")
                )
            )
        )

        horizon_dfs.append(horizon_df)

    forecast = horizon_dfs[0]

    for other in horizon_dfs[1:]:
        forecast = forecast.unionByName(other)

    return forecast


# ============================================================
# LOAD S5 FACILITIES
# ============================================================

def load_facilities(spark: SparkSession):
    check_path(
        FACILITY_PATH,
        "S5 Silver facilities"
    )

    facilities = spark.read.parquet(str(FACILITY_PATH))

    required = [
        "facility_id",
        "district_name",
        "bed_capacity",
        "active",
    ]

    missing = [
        c for c in required
        if c not in facilities.columns
    ]

    if missing:
        raise ValueError(
            "S5 facilities is missing required columns: "
            + ", ".join(missing)
        )

    facilities = (
        facilities
        .withColumn(
            "district_name_source",
            F.trim(F.col("district_name").cast("string"))
        )
        .withColumn(
            "district_name",
            canonical_district_name_column(F.col("district_name_source"))
        )
        .withColumn(
            "bed_capacity",
            F.col("bed_capacity").cast("double")
        )
        .withColumn(
            "active",
            F.col("active").cast("boolean")
        )
    )

    active_facilities = (
        facilities
        .filter(F.col("active") == True)
    )

    district_capacity = (
        active_facilities
        .groupBy("district_name")
        .agg(
            F.sum("bed_capacity").alias(
                "district_bed_capacity"
            ),
            F.countDistinct("facility_id").alias(
                "active_facility_count"
            ),
        )
    )

    return district_capacity


# ============================================================
# LOAD LATEST STOCK
# ============================================================

def load_latest_stock(spark: SparkSession):
    check_path(
        STOCK_PATH,
        "S6 Silver stock/beds"
    )

    stock = spark.read.parquet(str(STOCK_PATH))

    required = [
        "snapshot_date",
        "facility_id",
        "district_name",
        "bed_capacity",
        "available_beds",
        "occupied_beds",
        "saline_stock",
        "test_kit_stock",
    ]

    missing = [
        c for c in required
        if c not in stock.columns
    ]

    if missing:
        raise ValueError(
            "S6 stock/beds is missing required columns: "
            + ", ".join(missing)
        )

    stock = (
        stock
        .withColumn(
            "snapshot_date",
            F.to_date("snapshot_date")
        )
        .withColumn(
            "district_name_source",
            F.trim(F.col("district_name").cast("string"))
        )
        .withColumn(
            "district_name",
            canonical_district_name_column(F.col("district_name_source"))
        )
        .withColumn(
            "available_beds",
            F.col("available_beds").cast("double")
        )
        .withColumn(
            "occupied_beds",
            F.col("occupied_beds").cast("double")
        )
        .withColumn(
            "saline_stock",
            F.col("saline_stock").cast("double")
        )
        .withColumn(
            "test_kit_stock",
            F.col("test_kit_stock").cast("double")
        )
    )

    latest_date = (
        stock
        .select(
            F.max("snapshot_date").alias("latest")
        )
        .collect()[0]["latest"]
    )

    if latest_date is None:
        raise ValueError(
            "No snapshot date found in S6."
        )

    latest_stock = (
        stock
        .filter(
            F.col("snapshot_date") == F.lit(latest_date)
        )
        .groupBy("district_name")
        .agg(
            F.countDistinct("facility_id").alias(
                "stock_facility_count"
            ),
            F.sum("bed_capacity").alias(
                "stock_bed_capacity"
            ),
            F.sum("available_beds").alias(
                "available_beds"
            ),
            F.sum("occupied_beds").alias(
                "occupied_beds"
            ),
            F.sum("saline_stock").alias(
                "saline_stock"
            ),
            F.sum("test_kit_stock").alias(
                "test_kit_stock"
            ),
        )
        .withColumn(
            "stock_snapshot_date",
            F.lit(latest_date).cast("date")
        )
    )

    return latest_stock, latest_date


# ============================================================
# RESOURCE DEMAND CALCULATION
# ============================================================

def calculate_resource_demand(
    forecast,
    capacity,
    stock
):
    result = (
        forecast
        .join(
            capacity,
            on="district_name",
            how="left"
        )
        .join(
            stock,
            on="district_name",
            how="left"
        )
    )

    numeric_columns = [
        "district_bed_capacity",
        "active_facility_count",
        "stock_facility_count",
        "stock_bed_capacity",
        "available_beds",
        "occupied_beds",
        "saline_stock",
        "test_kit_stock",
    ]

    for column in numeric_columns:
        result = result.withColumn(
            column,
            F.col(column).cast("double")
        )

    result = result.withColumn(
        "stock_data_status",
        F.when(
            F.col("stock_snapshot_date").isNull(),
            F.lit("MISSING")
        ).when(
            F.col("active_facility_count").isNotNull()
            & (F.col("stock_facility_count") < F.col("active_facility_count")),
            F.lit("INCOMPLETE")
        ).otherwise(F.lit("AVAILABLE"))
    )

    # --------------------------------------------------------
    # Point demand
    # --------------------------------------------------------

    result = result.withColumn(
        "bed_demand_point",
        F.col("forecast_cases") * F.lit(BED_RATE)
    )

    result = result.withColumn(
        "saline_demand_point",
        F.col("forecast_cases")
        * F.lit(SALINE_UNITS_PER_CASE)
    )

    result = result.withColumn(
        "test_kit_demand_point",
        F.col("forecast_cases")
        * F.lit(TEST_KITS_PER_CASE)
    )

    # --------------------------------------------------------
    # Uncertainty range
    # --------------------------------------------------------

    result = result.withColumn(
        "bed_demand_lower",
        F.greatest(
            F.lit(0.0),
            F.col("bed_demand_point")
            * F.lit(1.0 - UNCERTAINTY_BUFFER)
        )
    )

    result = result.withColumn(
        "bed_demand_upper",
        F.col("bed_demand_point")
        * F.lit(1.0 + UNCERTAINTY_BUFFER)
    )

    result = result.withColumn(
        "saline_demand_lower",
        F.greatest(
            F.lit(0.0),
            F.col("saline_demand_point")
            * F.lit(1.0 - UNCERTAINTY_BUFFER)
        )
    )

    result = result.withColumn(
        "saline_demand_upper",
        F.col("saline_demand_point")
        * F.lit(1.0 + UNCERTAINTY_BUFFER)
    )

    result = result.withColumn(
        "test_kit_demand_lower",
        F.greatest(
            F.lit(0.0),
            F.col("test_kit_demand_point")
            * F.lit(1.0 - UNCERTAINTY_BUFFER)
        )
    )

    result = result.withColumn(
        "test_kit_demand_upper",
        F.col("test_kit_demand_point")
        * F.lit(1.0 + UNCERTAINTY_BUFFER)
    )

    # --------------------------------------------------------
    # Safety-buffer adjusted requirement
    # --------------------------------------------------------

    result = result.withColumn(
        "bed_planning_requirement",
        F.ceil(
            F.col("bed_demand_upper")
            * F.lit(1.0 + STOCK_SAFETY_BUFFER)
        )
    )

    result = result.withColumn(
        "saline_planning_requirement",
        F.ceil(
            F.col("saline_demand_upper")
            * F.lit(1.0 + STOCK_SAFETY_BUFFER)
        )
    )

    result = result.withColumn(
        "test_kit_planning_requirement",
        F.ceil(
            F.col("test_kit_demand_upper")
            * F.lit(1.0 + STOCK_SAFETY_BUFFER)
        )
    )

    # --------------------------------------------------------
    # Current resource gap
    # --------------------------------------------------------

    result = result.withColumn(
        "bed_gap",
        F.when(
            F.col("available_beds").isNull(),
            F.lit(None).cast("double")
        ).otherwise(
            F.greatest(
                F.lit(0.0),
                F.col("bed_planning_requirement") - F.col("available_beds")
            )
        )
    )

    result = result.withColumn(
        "saline_gap",
        F.when(
            F.col("saline_stock").isNull(),
            F.lit(None).cast("double")
        ).otherwise(
            F.greatest(
                F.lit(0.0),
                F.col("saline_planning_requirement") - F.col("saline_stock")
            )
        )
    )

    result = result.withColumn(
        "test_kit_gap",
        F.when(
            F.col("test_kit_stock").isNull(),
            F.lit(None).cast("double")
        ).otherwise(
            F.greatest(
                F.lit(0.0),
                F.col("test_kit_planning_requirement") - F.col("test_kit_stock")
            )
        )
    )

    # --------------------------------------------------------
    # Utilization / risk indicators
    # --------------------------------------------------------

    result = result.withColumn(
        "projected_bed_utilization",
        F.when(
            F.col("district_bed_capacity").isNull(),
            F.lit(None).cast("double")
        ).when(
            F.col("district_bed_capacity") > 0,
            (
                F.col("occupied_beds")
                + F.col("bed_planning_requirement")
            )
            / F.col("district_bed_capacity")
        ).otherwise(F.lit(0.0))
    )

    result = result.withColumn(
        "resource_risk_score",
        F.when(
            F.col("bed_gap").isNull()
            | F.col("saline_gap").isNull()
            | F.col("test_kit_gap").isNull(),
            F.lit(None).cast("integer")
        ).otherwise(
            (
            F.when(F.col("bed_gap") > 0, 1).otherwise(0)
            +
            F.when(F.col("saline_gap") > 0, 1).otherwise(0)
            +
            F.when(F.col("test_kit_gap") > 0, 1).otherwise(0)
            )
        )
    )

    result = result.withColumn(
        "resource_risk_level",
        F.when(
            F.col("resource_risk_score").isNull(),
            F.lit("UNKNOWN")
        )
        .when(
            F.col("resource_risk_score") >= 3,
            F.lit("HIGH")
        )
        .when(
            F.col("resource_risk_score") == 2,
            F.lit("MEDIUM")
        )
        .when(
            F.col("resource_risk_score") == 1,
            F.lit("LOW")
        )
        .otherwise(
            F.lit("NO_GAP")
        )
    )

    # --------------------------------------------------------
    # Round operational quantities
    # --------------------------------------------------------

    integer_columns = [
        "bed_demand_point",
        "bed_demand_lower",
        "bed_demand_upper",
        "bed_planning_requirement",
        "bed_gap",
        "saline_demand_point",
        "saline_demand_lower",
        "saline_demand_upper",
        "saline_planning_requirement",
        "saline_gap",
        "test_kit_demand_point",
        "test_kit_demand_lower",
        "test_kit_demand_upper",
        "test_kit_planning_requirement",
        "test_kit_gap",
    ]

    for column in integer_columns:
        result = result.withColumn(
            column,
            F.round(F.col(column), 0)
        )

    return result


# ============================================================
# WRITE OUTPUT
# ============================================================

def write_outputs(
    result,
    latest_origin,
    stock_date,
    input_rows,
):
    OUTPUT_ROOT.mkdir(
        parents=True,
        exist_ok=True
    )

    (
        result
        .repartition(
            "forecast_horizon_week"
        )
        .write
        .mode("overwrite")
        .partitionBy("forecast_horizon_week")
        .parquet(str(DEMAND_OUTPUT))
    )

    # --------------------------------------------------------
    # Summary
    # --------------------------------------------------------

    total_rows = result.count()

    districts = (
        result
        .select("district_name")
        .distinct()
        .count()
    )

    diseases = (
        result
        .select("disease")
        .distinct()
        .count()
    )

    horizons = (
        result
        .select("forecast_horizon_week")
        .distinct()
        .count()
    )

    summary_row = (
        result
        .agg(
            F.sum("forecast_cases")
                .alias("forecast_cases"),
            F.sum("bed_planning_requirement")
                .alias("beds_required"),
            F.sum("saline_planning_requirement")
                .alias("saline_required"),
            F.sum("test_kit_planning_requirement")
                .alias("test_kits_required"),
            F.sum("bed_gap")
                .alias("bed_gap"),
            F.sum("saline_gap")
                .alias("saline_gap"),
            F.sum("test_kit_gap")
                .alias("test_kit_gap"),
        )
        .collect()[0]
    )

    high_risk_rows = (
        result
        .filter(
            F.col("resource_risk_level") == "HIGH"
        )
        .count()
    )

    district_demand = (
        result
        .groupBy("district_name")
        .agg(
            F.sum("forecast_cases").alias("four_week_forecast_cases"),
            F.sum("bed_demand_point").alias("four_week_bed_demand_point"),
            F.sum("bed_planning_requirement").alias(
                "four_week_bed_planning_requirement"
            ),
            F.sum("saline_planning_requirement").alias(
                "four_week_saline_planning_requirement"
            ),
            F.sum("test_kit_planning_requirement").alias(
                "four_week_test_kit_planning_requirement"
            ),
            F.max("district_bed_capacity").alias("district_bed_capacity"),
            F.max("available_beds").alias("available_beds"),
            F.max("stock_facility_count").alias("stock_facility_count"),
            F.max("stock_data_status").alias("stock_data_status"),
            F.max("stock_snapshot_date").alias("stock_snapshot_date"),
            F.countDistinct("forecast_horizon_week").alias(
                "forecast_horizon_count"
            ),
        )
        .orderBy("district_name")
    )
    district_demand_pdf = district_demand.toPandas()
    if len(district_demand_pdf) != 64 or not (
        district_demand_pdf["forecast_horizon_count"] == len(FORECAST_HORIZONS)
    ).all():
        raise RuntimeError(
            "Four-week district demand is incomplete: expected 64 districts "
            "with all four forecast horizons."
        )
    district_demand_pdf.to_csv(DISTRICT_DEMAND_OUTPUT, index=False)
    print()
    print("Four-week demand by district (sum of horizons 1-4):")
    print(district_demand_pdf.to_string(index=False))
    print(
        "Four-week totals: "
        f"point beds={district_demand_pdf['four_week_bed_demand_point'].sum():,.1f}, "
        "planning beds="
        f"{district_demand_pdf['four_week_bed_planning_requirement'].sum():,.0f}, "
        f"districts={len(district_demand_pdf)}"
    )

    summary = {
        "dataset": "EpiScope AN-6 Forecast Resource Demand",
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "feature_input_rows": int(input_rows),
        "forecast_origin": str(latest_origin),
        "stock_snapshot_date": str(stock_date),
        "districts": int(districts),
        "diseases": int(diseases),
        "forecast_horizons": int(horizons),
        "output_rows": int(total_rows),
        "planning_horizon_weeks": int(len(FORECAST_HORIZONS)),
        "demand_aggregation": (
            "Cumulative total across forecast horizons 1-4; each horizon "
            "represents a distinct forecast week."
        ),

        "total_forecast_cases": safe_float(
            summary_row["forecast_cases"]
        ),

        "total_beds_required": safe_float(
            summary_row["beds_required"]
        ),
        "four_week_bed_demand_point": safe_float(
            district_demand_pdf["four_week_bed_demand_point"].sum()
        ),
        "four_week_bed_planning_requirement": safe_float(
            district_demand_pdf["four_week_bed_planning_requirement"].sum()
        ),
        "four_week_district_demand_output": str(DISTRICT_DEMAND_OUTPUT),

        "total_saline_required": safe_float(
            summary_row["saline_required"]
        ),

        "total_test_kits_required": safe_float(
            summary_row["test_kits_required"]
        ),

        "total_bed_gap": safe_float(
            summary_row["bed_gap"]
        ),

        "total_saline_gap": safe_float(
            summary_row["saline_gap"]
        ),

        "total_test_kit_gap": safe_float(
            summary_row["test_kit_gap"]
        ),

        "high_risk_rows": int(high_risk_rows),

        "assumptions": {
            "bed_rate": BED_RATE,
            "saline_units_per_case": SALINE_UNITS_PER_CASE,
            "test_kits_per_case": TEST_KITS_PER_CASE,
            "uncertainty_buffer": UNCERTAINTY_BUFFER,
            "stock_safety_buffer": STOCK_SAFETY_BUFFER,
            "forecast_method": (
                "52-week seasonal naive; "
                "4-week recent average fallback"
            ),
        },

        "ground_truth_used": False,

        "output": str(DEMAND_OUTPUT),
    }

    with open(
        SUMMARY_OUTPUT,
        "w",
        encoding="utf-8"
    ) as f:
        json.dump(
            summary,
            f,
            indent=2,
            ensure_ascii=False
        )

    # --------------------------------------------------------
    # Assumptions file
    # --------------------------------------------------------

    assumptions = {
        "purpose": (
            "Operational planning assumptions for converting "
            "disease forecasts into resource demand."
        ),

        "bed_rate": {
            "value": BED_RATE,
            "unit": "beds per forecast case",
            "description": (
                "Planning assumption for expected bed requirement."
            ),
        },

        "saline_units_per_case": {
            "value": SALINE_UNITS_PER_CASE,
            "unit": "saline units per forecast case",
            "description": (
                "Planning assumption for saline demand."
            ),
        },

        "test_kits_per_case": {
            "value": TEST_KITS_PER_CASE,
            "unit": "test kits per forecast case",
            "description": (
                "Planning assumption for diagnostic testing demand."
            ),
        },

        "uncertainty_buffer": {
            "value": UNCERTAINTY_BUFFER,
            "description": (
                "Applied symmetrically around point demand "
                "to create a planning uncertainty range."
            ),
        },

        "stock_safety_buffer": {
            "value": STOCK_SAFETY_BUFFER,
            "description": (
                "Additional operational buffer applied to "
                "upper-bound demand."
            ),
        },

        "forecast_method": (
            "52-week seasonal-naive forecast with "
            "recent 4-week average fallback."
        ),

        "important_note": (
            "These assumptions are analytical planning assumptions "
            "for the EpiScope project and are not clinical guidelines."
        ),
    }

    with open(
        ASSUMPTIONS_OUTPUT,
        "w",
        encoding="utf-8"
    ) as f:
        json.dump(
            assumptions,
            f,
            indent=2,
            ensure_ascii=False
        )

    return summary


# ============================================================
# MAIN
# ============================================================

def main():
    print("=" * 72)
    print("EpiScope AN-6 Forecast -> Resource Demand")
    print("=" * 72)

    print()
    print("Checking input paths...")

    check_path(
        FEATURE_PATH,
        "Gold feature mart"
    )

    check_path(
        FACILITY_PATH,
        "S5 facilities Silver"
    )

    check_path(
        STOCK_PATH,
        "S6 stock/beds Silver"
    )

    print("All required inputs found.")

    print()
    print("Starting Spark...")

    spark = create_spark()

    try:
        # ----------------------------------------------------
        # 1. Load feature mart
        # ----------------------------------------------------

        print()
        print("[1/6] Loading Gold feature mart...")

        features = load_features(spark)

        input_rows = features.count()

        print(
            f"Feature rows          : {input_rows:,}"
        )

        # ----------------------------------------------------
        # 2. Latest origin
        # ----------------------------------------------------

        print()
        print("[2/6] Detecting latest forecast origin...")

        latest_origin = select_latest_origin(
            features
        )

        print(
            f"Latest epi-week       : {latest_origin}"
        )

        # ----------------------------------------------------
        # 3. Forecast
        # ----------------------------------------------------

        print()
        print("[3/6] Building 1-4 week forecast...")

        forecast = build_four_week_forecast(
            features,
            latest_origin
        )

        forecast_rows = forecast.count()

        print(
            f"Forecast rows         : {forecast_rows:,}"
        )

        # ----------------------------------------------------
        # 4. Facility capacity
        # ----------------------------------------------------

        print()
        print("[4/6] Loading S5 facility capacity...")

        capacity = load_facilities(
            spark
        )

        capacity_rows = capacity.count()

        print(
            f"District capacity rows: {capacity_rows:,}"
        )

        # ----------------------------------------------------
        # 5. Current stock
        # ----------------------------------------------------

        print()
        print("[5/6] Loading latest S6 stock snapshot...")

        stock, stock_date = load_latest_stock(
            spark
        )

        stock_rows = stock.count()

        print(
            f"Stock district rows   : {stock_rows:,}"
        )

        print(
            f"Stock snapshot date   : {stock_date}"
        )

        # ----------------------------------------------------
        # 6. Demand calculation
        # ----------------------------------------------------

        print()
        print("[6/6] Calculating resource demand...")

        result = calculate_resource_demand(
            forecast,
            capacity,
            stock
        )

        result = result.cache()

        summary = write_outputs(
            result=result,
            latest_origin=latest_origin,
            stock_date=stock_date,
            input_rows=input_rows,
        )

        print()
        print("=" * 72)
        print("AN-6 RESOURCE DEMAND COMPLETE")
        print("=" * 72)

        print(
            f"Forecast origin       : {latest_origin}"
        )

        print(
            f"Stock snapshot        : {stock_date}"
        )

        print(
            f"Output rows           : {summary['output_rows']:,}"
        )

        print(
            f"Districts             : {summary['districts']}"
        )

        print(
            f"Diseases              : {summary['diseases']}"
        )

        print(
            f"Horizons              : {summary['forecast_horizons']}"
        )

        print()
        print("Planning totals:")
        print(
            f"  Forecast cases      : "
            f"{summary['total_forecast_cases']:,.0f}"
        )

        print(
            f"  Beds required       : "
            f"{summary['total_beds_required']:,.0f}"
        )

        print(
            f"  Saline required     : "
            f"{summary['total_saline_required']:,.0f}"
        )

        print(
            f"  Test kits required  : "
            f"{summary['total_test_kits_required']:,.0f}"
        )

        print()
        print("Resource gaps:")
        print(
            f"  Bed gap             : "
            f"{summary['total_bed_gap']:,.0f}"
        )

        print(
            f"  Saline gap          : "
            f"{summary['total_saline_gap']:,.0f}"
        )

        print(
            f"  Test-kit gap        : "
            f"{summary['total_test_kit_gap']:,.0f}"
        )

        print()
        print(
            f"High-risk rows        : "
            f"{summary['high_risk_rows']:,}"
        )

        print()
        print(
            "Assumptions:"
        )
        print(
            f"  Bed rate            : {BED_RATE}"
        )
        print(
            f"  Saline/case         : "
            f"{SALINE_UNITS_PER_CASE}"
        )
        print(
            f"  Test kits/case      : "
            f"{TEST_KITS_PER_CASE}"
        )
        print(
            f"  Uncertainty buffer  : "
            f"{UNCERTAINTY_BUFFER * 100:.0f}%"
        )
        print(
            f"  Safety buffer       : "
            f"{STOCK_SAFETY_BUFFER * 100:.0f}%"
        )

        print()
        print(
            f"Demand output         : {DEMAND_OUTPUT}"
        )

        print(
            f"Summary               : {SUMMARY_OUTPUT}"
        )

        print(
            f"Assumptions           : {ASSUMPTIONS_OUTPUT}"
        )

        print()
        print(
            "Ground truth used     : NO"
        )

        result.unpersist()

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
