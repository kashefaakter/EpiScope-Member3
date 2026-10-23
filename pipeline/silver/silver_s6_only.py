from __future__ import annotations

import json
import os
import shutil
import sys
from pathlib import Path


# ============================================================
# Force PySpark workers to use the current virtual environment
# ============================================================

PYTHON_EXE = sys.executable

os.environ["PYSPARK_PYTHON"] = PYTHON_EXE
os.environ["PYSPARK_DRIVER_PYTHON"] = PYTHON_EXE


# ============================================================
# Project paths
# ============================================================

ROOT = Path(__file__).resolve().parents[2]

BRONZE_S6 = ROOT / "data" / "bronze" / "S6_stock_beds"
SILVER_S6 = ROOT / "data" / "silver" / "S6_stock_beds"
QUARANTINE_S6 = ROOT / "data" / "quarantine" / "S6_stock_beds"
QUALITY_DIR = ROOT / "data" / "gold" / "quality" / "s6_stock_beds"


# ============================================================
# Spark imports
# ============================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ============================================================
# Create Spark session
# ============================================================

def create_spark():

    spark = (
        SparkSession.builder
        .appName("EpiScope-S6-Silver")
        .master("local[*]")
        .config("spark.pyspark.python", PYTHON_EXE)
        .config("spark.pyspark.driver.python", PYTHON_EXE)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark


# ============================================================
# Utility
# ============================================================

def remove_output(path: Path):

    if path.exists():
        print(f"Removing old output: {path}")
        shutil.rmtree(path)


# ============================================================
# S6 transformation
# ============================================================

def transform_s6(spark):

    print()
    print("=" * 70)
    print("EpiScope - S6 Stock/Beds Silver Transformation")
    print("=" * 70)

    print()
    print(f"Bronze source : {BRONZE_S6}")
    print(f"Silver output : {SILVER_S6}")
    print(f"Quarantine    : {QUARANTINE_S6}")
    print(f"Python worker : {PYTHON_EXE}")

    # --------------------------------------------------------
    # Check Bronze source
    # --------------------------------------------------------

    if not BRONZE_S6.exists():

        raise FileNotFoundError(
            f"S6 Bronze directory not found: {BRONZE_S6}"
        )

    # --------------------------------------------------------
    # Read Bronze
    # --------------------------------------------------------

    print()
    print("Reading S6 Bronze data...")

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(str(BRONZE_S6))
    )

    print()
    print("Bronze schema:")
    df.printSchema()

    total_rows = df.count()

    print()
    print(f"Total Bronze rows: {total_rows:,}")

    # --------------------------------------------------------
    # Required columns
    # --------------------------------------------------------

    required_columns = [
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
        column
        for column in required_columns
        if column not in df.columns
    ]

    if missing:

        raise ValueError(
            "Missing S6 columns: "
            + ", ".join(missing)
        )

    df = df.select(*required_columns)

    # --------------------------------------------------------
    # Type normalization
    # --------------------------------------------------------

    df = (
        df
        .withColumn(
            "snapshot_date",
            F.to_date(F.col("snapshot_date"))
        )
        .withColumn(
            "facility_id",
            F.trim(
                F.col("facility_id").cast("string")
            )
        )
        .withColumn(
            "district_name",
            F.trim(
                F.col("district_name").cast("string")
            )
        )
        .withColumn(
            "bed_capacity",
            F.col("bed_capacity").cast("integer")
        )
        .withColumn(
            "available_beds",
            F.col("available_beds").cast("integer")
        )
        .withColumn(
            "occupied_beds",
            F.col("occupied_beds").cast("integer")
        )
        .withColumn(
            "saline_stock",
            F.col("saline_stock").cast("integer")
        )
        .withColumn(
            "test_kit_stock",
            F.col("test_kit_stock").cast("integer")
        )
    )

    # --------------------------------------------------------
    # Data-quality rules
    #
    # IMPORTANT:
    # We do NOT enforce:
    #
    # available_beds + occupied_beds == bed_capacity
    #
    # because the source data demonstrably does not follow
    # that relationship.
    # --------------------------------------------------------

    rules = {

        "missing_snapshot_date":
            F.col("snapshot_date").isNull(),

        "missing_facility_id":
            (
                F.col("facility_id").isNull()
                |
                (F.length(F.col("facility_id")) == 0)
            ),

        "missing_district":
            (
                F.col("district_name").isNull()
                |
                (F.length(F.col("district_name")) == 0)
            ),

        "invalid_bed_capacity":
            (
                F.col("bed_capacity").isNull()
                |
                (F.col("bed_capacity") < 0)
            ),

        "invalid_available_beds":
            (
                F.col("available_beds").isNull()
                |
                (F.col("available_beds") < 0)
                |
                (
                    F.col("available_beds")
                    > F.col("bed_capacity")
                )
            ),

        "invalid_occupied_beds":
            (
                F.col("occupied_beds").isNull()
                |
                (F.col("occupied_beds") < 0)
                |
                (
                    F.col("occupied_beds")
                    > F.col("bed_capacity")
                )
            ),

        "invalid_saline_stock":
            (
                F.col("saline_stock").isNull()
                |
                (F.col("saline_stock") < 0)
            ),

        "invalid_test_kit_stock":
            (
                F.col("test_kit_stock").isNull()
                |
                (F.col("test_kit_stock") < 0)
            ),
    }

    # --------------------------------------------------------
    # Build DQ reason from standard rules
    # --------------------------------------------------------

    reason_parts = []

    for rule_name, condition in rules.items():

        reason_parts.append(
            F.when(
                condition,
                F.lit(rule_name)
            )
        )

    df = df.withColumn(
        "dq_reason",
        F.concat_ws(
            ";",
            *reason_parts
        )
    )

    # --------------------------------------------------------
    # Duplicate snapshot + facility detection
    # --------------------------------------------------------

    duplicate_keys = (
        df
        .groupBy(
            "snapshot_date",
            "facility_id"
        )
        .count()
        .filter(
            F.col("count") > 1
        )
        .select(
            "snapshot_date",
            "facility_id"
        )
    )

    df = (
        df.alias("d")
        .join(
            duplicate_keys.alias("dup"),
            (
                (
                    F.col("d.snapshot_date")
                    == F.col("dup.snapshot_date")
                )
                &
                (
                    F.col("d.facility_id")
                    == F.col("dup.facility_id")
                )
            ),
            "left"
        )
        .withColumn(
            "dq_reason",
            F.when(
                F.col("dup.facility_id").isNotNull(),

                F.when(
                    F.length(
                        F.col("d.dq_reason")
                    ) > 0,

                    F.concat(
                        F.col("d.dq_reason"),
                        F.lit(";duplicate_snapshot_facility")
                    )

                ).otherwise(
                    F.lit("duplicate_snapshot_facility")
                )

            ).otherwise(
                F.col("d.dq_reason")
            )
        )
        .select(
            "d.*"
        )
    )

    # --------------------------------------------------------
    # Clean records
    # --------------------------------------------------------

    clean_df = (
        df
        .filter(
            F.col("dq_reason") == ""
        )
        .drop("dq_reason")
    )

    # --------------------------------------------------------
    # Quarantine records
    # --------------------------------------------------------

    quarantine_df = (
        df
        .filter(
            F.length(F.col("dq_reason")) > 0
        )
    )

    clean_count = clean_df.count()
    quarantine_count = quarantine_df.count()

    # --------------------------------------------------------
    # Print result
    # --------------------------------------------------------

    print()
    print("=" * 70)
    print("S6 DATA QUALITY RESULT")
    print("=" * 70)

    print(
        f"Total rows       : {total_rows:,}"
    )

    print(
        f"Clean rows       : {clean_count:,}"
    )

    print(
        f"Quarantine rows  : {quarantine_count:,}"
    )

    # --------------------------------------------------------
    # Rule counts
    # --------------------------------------------------------

    print()
    print("DQ rule counts:")

    rule_count_expressions = []

    for rule_name, condition in rules.items():

        rule_count_expressions.append(
            F.sum(
                F.when(
                    condition,
                    1
                ).otherwise(0)
            ).alias(rule_name)
        )

    rule_counts = (
        df
        .select(*rule_count_expressions)
        .collect()[0]
        .asDict()
    )

    for rule_name, count in rule_counts.items():

        print(
            f"  {rule_name}: "
            f"{int(count or 0):,}"
        )

    # --------------------------------------------------------
    # Duplicate rule count
    # --------------------------------------------------------

    duplicate_count = (
        df
        .filter(
            F.instr(
                F.col("dq_reason"),
                "duplicate_snapshot_facility"
            ) > 0
        )
        .count()
    )

    rule_counts[
        "duplicate_snapshot_facility"
    ] = duplicate_count

    print(
        f"  duplicate_snapshot_facility: "
        f"{duplicate_count:,}"
    )

    # --------------------------------------------------------
    # Remove previous outputs
    # --------------------------------------------------------

    remove_output(SILVER_S6)
    remove_output(QUARANTINE_S6)

    # --------------------------------------------------------
    # Write Silver
    # --------------------------------------------------------

    print()
    print("Writing S6 Silver...")

    (
        clean_df
        .repartition("snapshot_date")
        .write
        .mode("overwrite")
        .partitionBy("snapshot_date")
        .parquet(
            str(SILVER_S6)
        )
    )

    # --------------------------------------------------------
    # Write quarantine
    # --------------------------------------------------------

    print()
    print("Writing S6 quarantine...")

    (
        quarantine_df
        .write
        .mode("overwrite")
        .partitionBy("snapshot_date")
        .parquet(
            str(QUARANTINE_S6)
        )
    )

    # --------------------------------------------------------
    # Write quality report
    # --------------------------------------------------------

    print()
    print("Writing S6 quality report...")

    remove_output(QUALITY_DIR)

    QUALITY_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    generated_at = (
        spark.sql(
            "SELECT current_timestamp() AS ts"
        )
        .collect()[0]["ts"]
        .isoformat()
    )

    quality_rows = []

    for rule_name, count in rule_counts.items():

        quality_rows.append(
            (
                "s6_stock_beds",
                rule_name,
                int(count or 0),
                generated_at,
            )
        )

    quality_schema = [
        "dataset",
        "rule",
        "failed_rows",
        "generated_at",
    ]

    quality_df = spark.createDataFrame(
        quality_rows,
        quality_schema
    )

    (
        quality_df
        .coalesce(1)
        .write
        .mode("overwrite")
        .parquet(
            str(QUALITY_DIR)
        )
    )

    # --------------------------------------------------------
    # Write summary JSON
    # --------------------------------------------------------

    summary = {
        "dataset": "s6_stock_beds",
        "total_rows": int(total_rows),
        "clean_rows": int(clean_count),
        "quarantine_rows": int(quarantine_count),
        "quality_rules": {
            key: int(value or 0)
            for key, value in rule_counts.items()
        },
        "generated_at": generated_at,
        "notes": [
            "bed_balance_mismatch rule is intentionally not applied",
            "available_beds and occupied_beds are not assumed to be additive",
        ],
    }

    summary_path = (
        QUALITY_DIR.parent
        / "s6_stock_beds_summary.json"
    )

    with open(
        summary_path,
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
    # Final status
    # --------------------------------------------------------

    print()
    print("=" * 70)
    print("S6 SILVER TRANSFORMATION COMPLETED")
    print("=" * 70)

    print()
    print(
        f"Silver output  : {SILVER_S6}"
    )

    print(
        f"Quarantine     : {QUARANTINE_S6}"
    )

    print(
        f"Quality report : {QUALITY_DIR}"
    )

    print(
        f"Summary        : {summary_path}"
    )

    return {
        "total": total_rows,
        "clean": clean_count,
        "quarantine": quarantine_count,
    }


# ============================================================
# Entry point
# ============================================================

def main():

    spark = None

    try:

        spark = create_spark()

        transform_s6(spark)

    finally:

        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()