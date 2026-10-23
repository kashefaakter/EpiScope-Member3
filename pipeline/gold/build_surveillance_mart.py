from __future__ import annotations

import json
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DoubleType

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))
from district_names import canonical_district_name_column


# ============================================================
# EpiScope - Gold Surveillance Mart Builder
# ============================================================

PROJECT_START = "2021-01-01"
PROJECT_END = "2025-12-31"

S4_PATH = "data/silver/S4_encounters"
S2_PATH = "data/silver/S2_population"
S3_PATH = "data/silver/S3_weather"

GOLD_SURVEILLANCE_PATH = "data/gold/surveillance/district_disease_epiweek"
GOLD_LINEAGE_PATH = "data/gold/lineage/surveillance_mart"
SUMMARY_PATH = "data/gold/surveillance/surveillance_mart_summary.json"

DISEASE_MAPPING = {
    "A90": "Dengue",
    "A91": "Dengue",
    "A00": "Cholera/Diarrhoeal Disease",
    "A09": "Cholera/Diarrhoeal Disease",
    "J10": "Influenza/ILI",
    "J11": "Influenza/ILI",
    "B50": "Malaria",
    "B51": "Malaria",
    "B52": "Malaria",
    "B53": "Malaria",
    "B54": "Malaria",
}


# ============================================================
# Spark
# ============================================================

def create_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("EpiScope-Gold-Surveillance-Mart")
        .master("local[*]")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )


# ============================================================
# Helpers
# ============================================================

def remove_path(path: str) -> None:
    shutil.rmtree(path, ignore_errors=True)


def epi_week_start(column):
    """
    Spark date_trunc('week') uses Monday as the beginning
    of the week.
    """
    return F.to_date(
        F.date_trunc(
            "week",
            F.to_timestamp(column)
        )
    )


def normalize_s4_district_name(column):
    return canonical_district_name_column(column)


def normalize_s3_district_name(column):
    return canonical_district_name_column(column)


# ============================================================
# Main
# ============================================================

def main():

    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    print("=" * 70)
    print("EpiScope Gold Surveillance Mart Builder")
    print("=" * 70)
    print(f"Project start : {PROJECT_START}")
    print(f"Project end   : {PROJECT_END}")
    print()

    # --------------------------------------------------------
    # Read Silver datasets
    # --------------------------------------------------------

    print("[1/8] Reading Silver datasets...")

    s4 = spark.read.parquet(S4_PATH)
    s2 = spark.read.parquet(S2_PATH)
    s3 = spark.read.parquet(S3_PATH)

    print(f"S4 columns: {len(s4.columns)}")
    print(f"S2 columns: {len(s2.columns)}")
    print(f"S3 columns: {len(s3.columns)}")
    print()

    # --------------------------------------------------------
    # S4 - Encounters
    # --------------------------------------------------------

    print("[2/8] Preparing S4 encounter data...")

    s4 = (
        s4
        .withColumn("onset_date", F.to_date("onset_date"))
        .filter(
            (F.col("onset_date") >= F.lit(PROJECT_START)) &
            (F.col("onset_date") <= F.lit(PROJECT_END))
        )
        .withColumn(
            "district_name",
            normalize_s4_district_name(F.col("district_name"))
        )
        .withColumn(
            "disease",
            F.create_map(
                *[
                    F.lit(x)
                    for pair in DISEASE_MAPPING.items()
                    for x in pair
                ]
            )[F.col("icd10_code")]
        )
        .filter(F.col("disease").isNotNull())
        .withColumn(
            "epi_week_start",
            epi_week_start(F.col("onset_date"))
        )
    )

    print("S4 disease counts:")

    (
        s4
        .groupBy("disease")
        .count()
        .orderBy("disease")
        .show(truncate=False)
    )

    # --------------------------------------------------------
    # S4 weekly case aggregation
    # --------------------------------------------------------

    print("[3/8] Aggregating weekly disease counts...")

    weekly_cases = (
        s4
        .groupBy(
            "district_name",
            "disease",
            "epi_week_start"
        )
        .agg(
            F.count("*")
            .cast("long")
            .alias("case_count"),

            F.countDistinct("encounter_id")
            .cast("long")
            .alias("unique_encounter_count"),

            F.sum(
                F.when(
                    F.col("lab_confirmed") == True,
                    1
                ).otherwise(0)
            )
            .cast("long")
            .alias("lab_confirmed_count"),

            F.sum(
                F.when(
                    F.col("admitted") == True,
                    1
                ).otherwise(0)
            )
            .cast("long")
            .alias("admitted_count"),
        )
    )

    observed_weekly_rows = weekly_cases.count()

    print(
        f"Observed weekly rows: {observed_weekly_rows}"
    )
    print()

    # --------------------------------------------------------
    # S2 - Population reference
    # --------------------------------------------------------

    print("[4/8] Preparing S2 population reference...")

    population = (
        s2
        .select(
            F.trim(
                F.col("district_name_canonical")
            ).alias("district_name"),

            F.col("division_name"),

            F.col("district_geocode"),

            F.col("population_total"),

            F.col("population_density"),
        )
        .dropDuplicates(["district_name"])
        .filter(
            F.col("district_name").isNotNull()
        )
    )

    population = (
        population
        .withColumn(
            "population_total",
            F.col("population_total").cast("double")
        )
        .withColumn(
            "population_density",
            F.col("population_density").cast("double")
        )
    )

    district_count = (
        population
        .select("district_name")
        .distinct()
        .count()
    )

    print(
        f"Population districts: {district_count}"
    )

    if district_count != 64:
        raise RuntimeError(
            f"Expected 64 districts in S2 population, "
            f"found {district_count}"
        )

    # --------------------------------------------------------
    # S3 - Weather
    # --------------------------------------------------------

    print("[5/8] Preparing S3 weather data...")

    weather = (
        s3
        .withColumn(
            "date",
            F.to_date("date")
        )
        .filter(
            (F.col("date") >= F.lit(PROJECT_START)) &
            (F.col("date") <= F.lit(PROJECT_END))
        )
        .withColumn(
            "district_name",
            normalize_s3_district_name(
                F.col("district_name")
            )
        )
        .withColumn(
            "epi_week_start",
            epi_week_start(F.col("date"))
        )
    )

    weekly_weather = (
        weather
        .groupBy(
            "district_name",
            "epi_week_start"
        )
        .agg(
            F.avg(
                F.col("t2m")
            )
            .cast("double")
            .alias("temperature_mean"),

            F.avg(
                F.col("t2m_max")
            )
            .cast("double")
            .alias("temperature_max_mean"),

            F.avg(
                F.col("t2m_min")
            )
            .cast("double")
            .alias("temperature_min_mean"),

            F.sum(
                F.col("precipitation_mm")
            )
            .cast("double")
            .alias("precipitation_sum_mm"),

            F.avg(
                F.col("rh2m")
            )
            .cast("double")
            .alias("humidity_mean"),
        )
    )

    weather_district_count = (
        weekly_weather
        .select("district_name")
        .distinct()
        .count()
    )

    print(
        f"Weather districts: {weather_district_count}"
    )

    print(
        "Cox's Bazar weather rows after normalization:"
    )

    (
        weekly_weather
        .filter(
            F.col("district_name") == "Cox's Bazar"
        )
        .agg(
            F.count("*").alias("weekly_rows"),
            F.min("epi_week_start").alias("min_week"),
            F.max("epi_week_start").alias("max_week")
        )
        .show(truncate=False)
    )

    print(
        "Chapainababganj weather rows after normalization:"
    )

    (
        weekly_weather
        .filter(
            F.col("district_name") == "Chapainababganj"
        )
        .agg(
            F.count("*").alias("weekly_rows"),
            F.min("epi_week_start").alias("min_week"),
            F.max("epi_week_start").alias("max_week")
        )
        .show(truncate=False)
    )

    print(
        "Moulvibazar weather rows after normalization:"
    )

    (
        weekly_weather
        .filter(
            F.col("district_name") == "Moulvibazar"
        )
        .agg(
            F.count("*").alias("weekly_rows"),
            F.min("epi_week_start").alias("min_week"),
            F.max("epi_week_start").alias("max_week")
        )
        .show(truncate=False)
    )

    # --------------------------------------------------------
    # Complete epi-week dimension
    # --------------------------------------------------------

    print(
        "[6/8] Creating complete district × disease × "
        "epi-week grid..."
    )

    # Spark Monday-week behavior:
    #
    # 2021-01-01 -> 2020-12-28
    # 2025-12-31 -> 2025-12-29
    #
    # Therefore the complete project-aligned week dimension
    # contains 262 Monday-start buckets.

    start_week = "2020-12-28"
    end_week = "2025-12-29"

    week_dimension = (
        spark.sql(
            f"""
            SELECT explode(
                sequence(
                    to_date('{start_week}'),
                    to_date('{end_week}'),
                    interval 7 days
                )
            ) AS epi_week_start
            """
        )
        .withColumn(
            "epi_year",
            F.year("epi_week_start")
        )
        .withColumn(
            "epi_week",
            F.weekofyear("epi_week_start")
        )
    )

    disease_dimension = spark.createDataFrame(
        [(disease,) for disease in sorted(set(DISEASE_MAPPING.values()))],
        ["disease"],
    )

    district_dimension = (
        population
        .select("district_name")
        .distinct()
    )

    complete_grid = (
        district_dimension
        .crossJoin(disease_dimension)
        .crossJoin(week_dimension)
    )

    expected_rows = complete_grid.count()

    print(
        f"Districts : {district_dimension.count()}"
    )

    print(
        f"Diseases  : {disease_dimension.count()}"
    )

    print(
        f"Epi-weeks : {week_dimension.count()}"
    )

    print(
        f"Expected complete rows: {expected_rows}"
    )

    if disease_dimension.count() != 4:
        raise RuntimeError(
            "Expected four core disease groups from the ICD-10 mapping."
        )

    if expected_rows != (
        district_dimension.count()
        * disease_dimension.count()
        * week_dimension.count()
    ):
        raise RuntimeError(
            "The district × disease × epi-week grid is incomplete: "
            f"generated {expected_rows} rows."
        )

    # --------------------------------------------------------
    # Join observed cases onto complete grid
    # --------------------------------------------------------

    surveillance = (
        complete_grid
        .join(
            weekly_cases,
            on=[
                "district_name",
                "disease",
                "epi_week_start",
            ],
            how="left"
        )
    )

    # Missing disease-week combinations mean zero observed
    # cases, not missing data.

    surveillance = (
        surveillance
        .withColumn(
            "case_count",
            F.coalesce(
                F.col("case_count"),
                F.lit(0)
            ).cast("long")
        )
        .withColumn(
            "unique_encounter_count",
            F.coalesce(
                F.col("unique_encounter_count"),
                F.lit(0)
            ).cast("long")
        )
        .withColumn(
            "lab_confirmed_count",
            F.coalesce(
                F.col("lab_confirmed_count"),
                F.lit(0)
            ).cast("long")
        )
        .withColumn(
            "admitted_count",
            F.coalesce(
                F.col("admitted_count"),
                F.lit(0)
            ).cast("long")
        )
    )

    # --------------------------------------------------------
    # Join population
    # --------------------------------------------------------

    surveillance = (
        surveillance
        .join(
            population,
            on="district_name",
            how="left"
        )
    )

    # --------------------------------------------------------
    # Incidence
    # --------------------------------------------------------

    surveillance = (
        surveillance
        .withColumn(
            "incidence_per_100k",
            F.when(
                F.col("population_total") > 0,
                (
                    F.col("case_count")
                    /
                    F.col("population_total")
                    *
                    F.lit(100000.0)
                )
            )
            .otherwise(
                F.lit(None).cast(DoubleType())
            )
        )
    )

    # --------------------------------------------------------
    # Join weekly weather
    # --------------------------------------------------------

    surveillance = (
        surveillance
        .join(
            weekly_weather,
            on=[
                "district_name",
                "epi_week_start",
            ],
            how="left"
        )
    )

    # --------------------------------------------------------
    # Final columns
    # --------------------------------------------------------

    surveillance = (
        surveillance
        .select(
            "epi_year",
            "epi_week",
            "epi_week_start",
            "district_name",
            "division_name",
            "district_geocode",
            "disease",
            "case_count",
            "unique_encounter_count",
            "lab_confirmed_count",
            "admitted_count",
            "population_total",
            "population_density",
            "incidence_per_100k",
            "temperature_mean",
            "temperature_max_mean",
            "temperature_min_mean",
            "precipitation_sum_mm",
            "humidity_mean",
        )
        .orderBy(
            "epi_week_start",
            "district_name",
            "disease"
        )
    )

    # --------------------------------------------------------
    # Weather diagnostic
    # --------------------------------------------------------

    missing_weather = (
        surveillance
        .filter(
            F.col("temperature_mean").isNull()
        )
        .count()
    )

    missing_weather_districts = (
        surveillance
        .filter(
            F.col("temperature_mean").isNull()
        )
        .select("district_name")
        .distinct()
        .orderBy("district_name")
        .collect()
    )

    missing_weather_district_names = [
        row["district_name"]
        for row in missing_weather_districts
    ]

    print()
    print(
        "Weather join diagnostic:"
    )

    print(
        f"Missing weather rows: {missing_weather}"
    )

    print(
        "Missing weather districts:",
        missing_weather_district_names
    )

    if missing_weather != 0:
        print(
            "WARNING: Some weather values are still missing. "
            "These remain NULL rather than being imputed."
        )

    # --------------------------------------------------------
    # Lineage
    # --------------------------------------------------------

    print("[7/8] Building lineage...")

    lineage_rows = []

    lineage_timestamp = (
        datetime.now(timezone.utc).isoformat()
    )

    # S4 source batch IDs
    if "silver_batch_id" in s4.columns:

        batch_values = (
            s4
            .select("silver_batch_id")
            .where(
                F.col("silver_batch_id").isNotNull()
            )
            .distinct()
            .collect()
        )

        for row in batch_values:

            lineage_rows.append(
                (
                    "surveillance_mart",
                    "S4_encounters",
                    str(row["silver_batch_id"]),
                    lineage_timestamp,
                )
            )

    else:

        lineage_rows.append(
            (
                "surveillance_mart",
                "S4_encounters",
                "silver_batch_id_not_available",
                lineage_timestamp,
            )
        )

    # S2 source batch IDs
    if "silver_batch_id" in s2.columns:

        batch_values = (
            s2
            .select("silver_batch_id")
            .where(
                F.col("silver_batch_id").isNotNull()
            )
            .distinct()
            .collect()
        )

        for row in batch_values:

            lineage_rows.append(
                (
                    "surveillance_mart",
                    "S2_population",
                    str(row["silver_batch_id"]),
                    lineage_timestamp,
                )
            )

    else:

        lineage_rows.append(
            (
                "surveillance_mart",
                "S2_population",
                "silver_batch_id_not_available",
                lineage_timestamp,
            )
        )

    # S3 source batch IDs
    if "silver_batch_id" in s3.columns:

        batch_values = (
            s3
            .select("silver_batch_id")
            .where(
                F.col("silver_batch_id").isNotNull()
            )
            .distinct()
            .collect()
        )

        for row in batch_values:

            lineage_rows.append(
                (
                    "surveillance_mart",
                    "S3_weather",
                    str(row["silver_batch_id"]),
                    lineage_timestamp,
                )
            )

    else:

        lineage_rows.append(
            (
                "surveillance_mart",
                "S3_weather",
                "silver_batch_id_not_available",
                lineage_timestamp,
            )
        )

    lineage_schema = [
        "gold_dataset",
        "source_dataset",
        "source_batch_id",
        "lineage_generated_at",
    ]

    lineage_df = spark.createDataFrame(
        lineage_rows,
        lineage_schema
    )

    # --------------------------------------------------------
    # Write Gold surveillance mart
    # --------------------------------------------------------

    remove_path(
        GOLD_SURVEILLANCE_PATH
    )

    remove_path(
        GOLD_LINEAGE_PATH
    )

    (
        surveillance
        .write
        .mode("overwrite")
        .partitionBy(
            "epi_year",
            "disease"
        )
        .parquet(
            GOLD_SURVEILLANCE_PATH
        )
    )

    (
        lineage_df
        .write
        .mode("overwrite")
        .parquet(
            GOLD_LINEAGE_PATH
        )
    )

    # --------------------------------------------------------
    # Summary statistics
    # --------------------------------------------------------

    actual_rows = surveillance.count()

    actual_districts = (
        surveillance
        .select("district_name")
        .distinct()
        .count()
    )

    actual_diseases = (
        surveillance
        .select("disease")
        .distinct()
        .count()
    )

    actual_weeks = (
        surveillance
        .select("epi_week_start")
        .distinct()
        .count()
    )

    zero_case_rows = (
        surveillance
        .filter(
            F.col("case_count") == 0
        )
        .count()
    )

    nonzero_case_rows = (
        surveillance
        .filter(
            F.col("case_count") > 0
        )
        .count()
    )

    min_week = (
        surveillance
        .agg(
            F.min("epi_week_start")
        )
        .collect()[0][0]
    )

    max_week = (
        surveillance
        .agg(
            F.max("epi_week_start")
        )
        .collect()[0][0]
    )

    disease_counts = (
        surveillance
        .groupBy("disease")
        .agg(
            F.sum("case_count")
            .alias("case_count"),

            F.count("*")
            .alias("rows")
        )
        .orderBy("disease")
        .collect()
    )

    disease_summary = {
        row["disease"]: {
            "case_count": int(
                row["case_count"]
            ),
            "rows": int(
                row["rows"]
            ),
        }
        for row in disease_counts
    }

    summary = {
        "dataset": "district_disease_epiweek",

        "project_start": PROJECT_START,

        "project_end": PROJECT_END,

        "epi_week_start": str(
            min_week
        ),

        "epi_week_end": str(
            max_week
        ),

        "rows": actual_rows,

        "districts": actual_districts,

        "diseases": actual_diseases,

        "epi_weeks": actual_weeks,

        "expected_complete_rows": expected_rows,

        "complete_grid": (
            actual_rows == expected_rows
        ),

        "zero_case_rows": zero_case_rows,

        "nonzero_case_rows": nonzero_case_rows,

        "missing_weather_rows": missing_weather,

        "missing_weather_districts":
            missing_weather_district_names,

        "disease_summary":
            disease_summary,

        "disease_mapping":
            DISEASE_MAPPING,

        "weather_district_normalization": {
            "Cox's_Bazar": "Cox's Bazar",
            "Nawabganj": "Chapainababganj",
            "Maulvibazar": "Moulvibazar",
        },

        "generated_at":
            datetime.now(
                timezone.utc
            ).isoformat(),
    }

    with open(
        SUMMARY_PATH,
        "w",
        encoding="utf-8"
    ) as f:

        json.dump(
            summary,
            f,
            indent=2,
            ensure_ascii=False,
            default=str
        )

    # --------------------------------------------------------
    # Final validation
    # --------------------------------------------------------

    print()
    print("=" * 70)
    print(
        "GOLD SURVEILLANCE MART COMPLETE"
    )
    print("=" * 70)

    print(
        f"Rows                  : {actual_rows}"
    )

    print(
        f"Expected rows         : {expected_rows}"
    )

    print(
        f"Districts             : {actual_districts}"
    )

    print(
        f"Diseases              : {actual_diseases}"
    )

    print(
        f"Epi-weeks             : {actual_weeks}"
    )

    print(
        f"Date min              : {min_week}"
    )

    print(
        f"Date max              : {max_week}"
    )

    print(
        f"Zero-case rows        : {zero_case_rows}"
    )

    print(
        f"Non-zero case rows    : {nonzero_case_rows}"
    )

    print(
        f"Missing weather rows  : {missing_weather}"
    )

    print()

    print(
        "Disease summary:"
    )

    for disease, values in disease_summary.items():

        print(
            f"  {disease}: "
            f"{values['case_count']} cases, "
            f"{values['rows']} rows"
        )

    print()

    print(
        f"Gold output : "
        f"{GOLD_SURVEILLANCE_PATH}"
    )

    print(
        f"Lineage     : "
        f"{GOLD_LINEAGE_PATH}"
    )

    print(
        f"Summary     : "
        f"{SUMMARY_PATH}"
    )

    print("=" * 70)

    if actual_rows != expected_rows:

        raise RuntimeError(
            f"Gold mart incomplete: "
            f"{actual_rows} rows instead of "
            f"{expected_rows}"
        )

    spark.stop()


if __name__ == "__main__":
    main()