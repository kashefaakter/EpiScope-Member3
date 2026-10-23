from __future__ import annotations

import json
import shutil
from datetime import datetime, timezone

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


# ============================================================
# EpiScope - Gold Feature Engineering
# ============================================================

INPUT_PATH = (
    "data/gold/surveillance/"
    "district_disease_epiweek"
)

OUTPUT_PATH = (
    "data/gold/features/"
    "district_disease_epiweek_features"
)

SUMMARY_PATH = (
    "data/gold/features/"
    "feature_mart_summary.json"
)


# ============================================================
# Spark
# ============================================================

def create_spark() -> SparkSession:

    return (
        SparkSession.builder
        .appName("EpiScope-Gold-Feature-Engineering")
        .master("local[*]")
        .config(
            "spark.sql.adaptive.enabled",
            "true"
        )
        .config(
            "spark.sql.adaptive.coalescePartitions.enabled",
            "true"
        )
        .config(
            "spark.sql.shuffle.partitions",
            "8"
        )
        .config(
            "spark.sql.parquet.compression.codec",
            "snappy"
        )
        .getOrCreate()
    )


# ============================================================
# Helpers
# ============================================================

def remove_path(path: str) -> None:

    shutil.rmtree(
        path,
        ignore_errors=True
    )


# ============================================================
# Main
# ============================================================

def main():

    spark = create_spark()

    spark.sparkContext.setLogLevel("WARN")

    print("=" * 70)
    print("EpiScope Gold Feature Engineering")
    print("=" * 70)

    # --------------------------------------------------------
    # 1. Read Gold Surveillance Mart
    # --------------------------------------------------------

    print()
    print("[1/7] Reading Gold Surveillance Mart...")

    df = spark.read.parquet(
        INPUT_PATH
    )

    input_rows = df.count()

    print(
        f"Input rows: {input_rows}"
    )

    districts = df.select("district_name").distinct().count()
    diseases = df.select("disease").distinct().count()
    epi_weeks = df.select("epi_week_start").distinct().count()
    unique_keys = df.select(
        "district_name", "disease", "epi_week_start"
    ).distinct().count()
    expected_rows = districts * diseases * epi_weeks

    if diseases != 4 or input_rows != expected_rows or input_rows != unique_keys:
        raise RuntimeError(
            "Unexpected Gold surveillance grid: "
            f"rows={input_rows}, districts={districts}, diseases={diseases}, "
            f"epi_weeks={epi_weeks}, unique_keys={unique_keys}."
        )

    # --------------------------------------------------------
    # 2. Prepare base temporal features
    # --------------------------------------------------------

    print()
    print("[2/7] Creating temporal features...")

    df = (
        df

        # Ensure correct types
        .withColumn(
            "epi_week_start",
            F.to_date("epi_week_start")
        )

        .withColumn(
            "case_count",
            F.col("case_count").cast("double")
        )

        .withColumn(
            "incidence_per_100k",
            F.col("incidence_per_100k").cast("double")
        )

        # Calendar features
        .withColumn(
            "calendar_year",
            F.year("epi_week_start")
        )

        .withColumn(
            "calendar_month",
            F.month("epi_week_start")
        )

        .withColumn(
            "quarter",
            F.quarter("epi_week_start")
        )

        .withColumn(
            "month_sin",
            F.sin(
                2.0
                * F.lit(3.141592653589793)
                * F.col("calendar_month")
                / F.lit(12.0)
            )
        )

        .withColumn(
            "month_cos",
            F.cos(
                2.0
                * F.lit(3.141592653589793)
                * F.col("calendar_month")
                / F.lit(12.0)
            )
        )

        .withColumn(
            "week_sin",
            F.sin(
                2.0
                * F.lit(3.141592653589793)
                * F.col("epi_week")
                / F.lit(52.0)
            )
        )

        .withColumn(
            "week_cos",
            F.cos(
                2.0
                * F.lit(3.141592653589793)
                * F.col("epi_week")
                / F.lit(52.0)
            )
        )
    )

    # --------------------------------------------------------
    # 3. Lag features
    # --------------------------------------------------------

    print()
    print("[3/7] Creating disease lag features...")

    partition_window = (
        Window
        .partitionBy(
            "district_name",
            "disease"
        )
        .orderBy(
            "epi_week_start"
        )
    )

    df = (
        df

        # Case-count lags
        .withColumn(
            "cases_lag_1",
            F.lag(
                "case_count",
                1
            ).over(partition_window)
        )

        .withColumn(
            "cases_lag_2",
            F.lag(
                "case_count",
                2
            ).over(partition_window)
        )

        .withColumn(
            "cases_lag_3",
            F.lag(
                "case_count",
                3
            ).over(partition_window)
        )

        .withColumn(
            "cases_lag_4",
            F.lag(
                "case_count",
                4
            ).over(partition_window)
        )

        .withColumn(
            "cases_lag_8",
            F.lag(
                "case_count",
                8
            ).over(partition_window)
        )

        .withColumn(
            "cases_lag_12",
            F.lag(
                "case_count",
                12
            ).over(partition_window)
        )

        # Incidence lags
        .withColumn(
            "incidence_lag_1",
            F.lag(
                "incidence_per_100k",
                1
            ).over(partition_window)
        )

        .withColumn(
            "incidence_lag_2",
            F.lag(
                "incidence_per_100k",
                2
            ).over(partition_window)
        )

        .withColumn(
            "incidence_lag_4",
            F.lag(
                "incidence_per_100k",
                4
            ).over(partition_window)
        )
    )

    # --------------------------------------------------------
    # 4. Rolling features
    # --------------------------------------------------------

    print()
    print("[4/7] Creating rolling statistics...")

    rolling_4 = (
        partition_window
        .rowsBetween(
            -4,
            -1
        )
    )

    rolling_8 = (
        partition_window
        .rowsBetween(
            -8,
            -1
        )
    )

    rolling_12 = (
        partition_window
        .rowsBetween(
            -12,
            -1
        )
    )

    df = (
        df

        # Previous 4 weeks
        .withColumn(
            "cases_rolling_mean_4w",
            F.avg(
                "case_count"
            ).over(rolling_4)
        )

        .withColumn(
            "cases_rolling_std_4w",
            F.stddev(
                "case_count"
            ).over(rolling_4)
        )

        .withColumn(
            "cases_rolling_sum_4w",
            F.sum(
                "case_count"
            ).over(rolling_4)
        )

        # Previous 8 weeks
        .withColumn(
            "cases_rolling_mean_8w",
            F.avg(
                "case_count"
            ).over(rolling_8)
        )

        .withColumn(
            "cases_rolling_std_8w",
            F.stddev(
                "case_count"
            ).over(rolling_8)
        )

        .withColumn(
            "cases_rolling_sum_8w",
            F.sum(
                "case_count"
            ).over(rolling_8)
        )

        # Previous 12 weeks
        .withColumn(
            "cases_rolling_mean_12w",
            F.avg(
                "case_count"
            ).over(rolling_12)
        )

        .withColumn(
            "cases_rolling_std_12w",
            F.stddev(
                "case_count"
            ).over(rolling_12)
        )

        .withColumn(
            "cases_rolling_sum_12w",
            F.sum(
                "case_count"
            ).over(rolling_12)
        )
    )

    # --------------------------------------------------------
    # 5. Weather lag features
    # --------------------------------------------------------

    print()
    print("[5/7] Creating weather lag features...")

    weather_columns = [
        "temperature_mean",
        "temperature_max_mean",
        "temperature_min_mean",
        "precipitation_sum_mm",
        "humidity_mean",
    ]

    for column_name in weather_columns:

        safe_name = column_name

        df = (
            df

            .withColumn(
                f"{safe_name}_lag_1",
                F.lag(
                    safe_name,
                    1
                ).over(partition_window)
            )

            .withColumn(
                f"{safe_name}_lag_2",
                F.lag(
                    safe_name,
                    2
                ).over(partition_window)
            )

            .withColumn(
                f"{safe_name}_lag_4",
                F.lag(
                    safe_name,
                    4
                ).over(partition_window)
            )

            .withColumn(
                f"{safe_name}_lag_8",
                F.lag(
                    safe_name,
                    8
                ).over(partition_window)
            )
        )

    # --------------------------------------------------------
    # 6. Epidemiological / anomaly features
    # --------------------------------------------------------

    print()
    print("[6/7] Creating anomaly and growth features...")

    df = (
        df

        # Absolute change from previous week
        .withColumn(
            "cases_change_1w",
            F.when(
                F.col("cases_lag_1").isNotNull(),
                F.col("case_count")
                -
                F.col("cases_lag_1")
            )
        )

        # Percentage growth
        .withColumn(
            "cases_growth_rate_1w",
            F.when(
                (
                    F.col("cases_lag_1").isNotNull()
                )
                &
                (
                    F.col("cases_lag_1") > 0
                ),
                (
                    F.col("case_count")
                    -
                    F.col("cases_lag_1")
                )
                /
                F.col("cases_lag_1")
            )
        )

        # Ratio against 4-week baseline
        .withColumn(
            "cases_vs_4w_mean_ratio",
            F.when(
                (
                    F.col(
                        "cases_rolling_mean_4w"
                    ).isNotNull()
                )
                &
                (
                    F.col(
                        "cases_rolling_mean_4w"
                    ) > 0
                ),
                F.col("case_count")
                /
                F.col(
                    "cases_rolling_mean_4w"
                )
            )
        )

        # Standardized anomaly score
        .withColumn(
            "cases_zscore_4w",
            F.when(
                (
                    F.col(
                        "cases_rolling_std_4w"
                    ).isNotNull()
                )
                &
                (
                    F.col(
                        "cases_rolling_std_4w"
                    ) > 0
                ),
                (
                    F.col("case_count")
                    -
                    F.col(
                        "cases_rolling_mean_4w"
                    )
                )
                /
                F.col(
                    "cases_rolling_std_4w"
                )
            )
        )

        # 8-week standardized anomaly
        .withColumn(
            "cases_zscore_8w",
            F.when(
                (
                    F.col(
                        "cases_rolling_std_8w"
                    ).isNotNull()
                )
                &
                (
                    F.col(
                        "cases_rolling_std_8w"
                    ) > 0
                ),
                (
                    F.col("case_count")
                    -
                    F.col(
                        "cases_rolling_mean_8w"
                    )
                )
                /
                F.col(
                    "cases_rolling_std_8w"
                )
            )
        )

        # Admission rate
        .withColumn(
            "admission_rate",
            F.when(
                F.col("case_count") > 0,
                F.col("admitted_count")
                /
                F.col("case_count")
            )
        )

        # Laboratory confirmation rate
        .withColumn(
            "lab_confirmation_rate",
            F.when(
                F.col("case_count") > 0,
                F.col("lab_confirmed_count")
                /
                F.col("case_count")
            )
        )
    )

    # --------------------------------------------------------
    # Fill only rolling features that are naturally undefined
    # at the beginning of a series.
    #
    # We intentionally DO NOT fill lag values with zero because
    # those are genuinely unavailable observations.
    # --------------------------------------------------------

    df = (
        df
        .withColumn(
            "cases_rolling_std_4w",
            F.coalesce(
                F.col("cases_rolling_std_4w"),
                F.lit(0.0)
            )
        )
        .withColumn(
            "cases_rolling_std_8w",
            F.coalesce(
                F.col("cases_rolling_std_8w"),
                F.lit(0.0)
            )
        )
        .withColumn(
            "cases_rolling_std_12w",
            F.coalesce(
                F.col("cases_rolling_std_12w"),
                F.lit(0.0)
            )
        )
    )

    # --------------------------------------------------------
    # Select final feature columns
    # --------------------------------------------------------

    final_columns = [
        # Time
        "epi_year",
        "epi_week",
        "epi_week_start",
        "calendar_year",
        "calendar_month",
        "quarter",
        "month_sin",
        "month_cos",
        "week_sin",
        "week_cos",

        # Geography
        "district_name",
        "division_name",
        "district_geocode",

        # Disease
        "disease",

        # Current target / observations
        "case_count",
        "unique_encounter_count",
        "lab_confirmed_count",
        "admitted_count",
        "population_total",
        "population_density",
        "incidence_per_100k",

        # Current weather
        "temperature_mean",
        "temperature_max_mean",
        "temperature_min_mean",
        "precipitation_sum_mm",
        "humidity_mean",

        # Case lags
        "cases_lag_1",
        "cases_lag_2",
        "cases_lag_3",
        "cases_lag_4",
        "cases_lag_8",
        "cases_lag_12",

        # Incidence lags
        "incidence_lag_1",
        "incidence_lag_2",
        "incidence_lag_4",

        # Rolling case features
        "cases_rolling_mean_4w",
        "cases_rolling_std_4w",
        "cases_rolling_sum_4w",
        "cases_rolling_mean_8w",
        "cases_rolling_std_8w",
        "cases_rolling_sum_8w",
        "cases_rolling_mean_12w",
        "cases_rolling_std_12w",
        "cases_rolling_sum_12w",

        # Weather lags
        "temperature_mean_lag_1",
        "temperature_mean_lag_2",
        "temperature_mean_lag_4",
        "temperature_mean_lag_8",

        "temperature_max_mean_lag_1",
        "temperature_max_mean_lag_2",
        "temperature_max_mean_lag_4",
        "temperature_max_mean_lag_8",

        "temperature_min_mean_lag_1",
        "temperature_min_mean_lag_2",
        "temperature_min_mean_lag_4",
        "temperature_min_mean_lag_8",

        "precipitation_sum_mm_lag_1",
        "precipitation_sum_mm_lag_2",
        "precipitation_sum_mm_lag_4",
        "precipitation_sum_mm_lag_8",

        "humidity_mean_lag_1",
        "humidity_mean_lag_2",
        "humidity_mean_lag_4",
        "humidity_mean_lag_8",

        # Anomaly / epidemiological
        "cases_change_1w",
        "cases_growth_rate_1w",
        "cases_vs_4w_mean_ratio",
        "cases_zscore_4w",
        "cases_zscore_8w",
        "admission_rate",
        "lab_confirmation_rate",
    ]

    feature_df = df.select(
        *final_columns
    )

    # --------------------------------------------------------
    # 7. Write feature mart
    # --------------------------------------------------------

    print()
    print("[7/7] Writing Gold Feature Mart...")

    remove_path(
        OUTPUT_PATH
    )

    (
        feature_df
        .write
        .mode("overwrite")
        .partitionBy(
            "epi_year",
            "disease"
        )
        .parquet(
            OUTPUT_PATH
        )
    )

    # --------------------------------------------------------
    # Validation
    # --------------------------------------------------------

    output_rows = feature_df.count()

    output_districts = (
        feature_df
        .select("district_name")
        .distinct()
        .count()
    )

    output_diseases = (
        feature_df
        .select("disease")
        .distinct()
        .count()
    )

    output_weeks = (
        feature_df
        .select("epi_week_start")
        .distinct()
        .count()
    )

    lag_rows = (
        feature_df
        .filter(
            F.col("cases_lag_1").isNotNull()
        )
        .count()
    )

    weather_lag_rows = (
        feature_df
        .filter(
            F.col(
                "temperature_mean_lag_1"
            ).isNotNull()
        )
        .count()
    )

    anomaly_rows = (
        feature_df
        .filter(
            F.col(
                "cases_zscore_4w"
            ).isNotNull()
        )
        .count()
    )

    summary = {

        "dataset":
            "district_disease_epiweek_features",

        "input_rows":
            input_rows,

        "output_rows":
            output_rows,

        "districts":
            output_districts,

        "diseases":
            output_diseases,

        "epi_weeks":
            output_weeks,

        "feature_count":
            len(final_columns),

        "lag_1_available_rows":
            lag_rows,

        "weather_lag_1_available_rows":
            weather_lag_rows,

        "anomaly_feature_available_rows":
            anomaly_rows,

        "features": final_columns,

        "generated_at":
            datetime.now(
                timezone.utc
            ).isoformat(),
    }

    with open(
        SUMMARY_PATH,
        "w",
        encoding="utf-8"
    ) as file:

        json.dump(
            summary,
            file,
            indent=2,
            ensure_ascii=False
        )

    # --------------------------------------------------------
    # Final output
    # --------------------------------------------------------

    print()
    print("=" * 70)
    print("GOLD FEATURE MART COMPLETE")
    print("=" * 70)

    print(
        f"Input rows              : {input_rows}"
    )

    print(
        f"Output rows             : {output_rows}"
    )

    print(
        f"Districts               : {output_districts}"
    )

    print(
        f"Diseases                : {output_diseases}"
    )

    print(
        f"Epi-weeks               : {output_weeks}"
    )

    print(
        f"Feature columns         : {len(final_columns)}"
    )

    print(
        f"Lag-1 available rows    : {lag_rows}"
    )

    print(
        f"Weather lag-1 rows      : {weather_lag_rows}"
    )

    print(
        f"Anomaly feature rows    : {anomaly_rows}"
    )

    print()
    print(
        f"Feature output : {OUTPUT_PATH}"
    )

    print(
        f"Summary        : {SUMMARY_PATH}"
    )

    print("=" * 70)

    if output_rows != input_rows:
        raise RuntimeError(
            "Feature engineering changed row count."
        )

    if output_districts != 64:
        raise RuntimeError(
            "Feature mart does not contain 64 districts."
        )

    if output_diseases != 4:
        raise RuntimeError(
            "Feature mart does not contain 4 core diseases."
        )

    if output_weeks != 262:
        raise RuntimeError(
            "Feature mart does not contain 262 epi-weeks."
        )

    spark.stop()


if __name__ == "__main__":
    main()