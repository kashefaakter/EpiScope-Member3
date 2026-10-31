from __future__ import annotations

import json
import shutil
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


# ============================================================
# EpiScope - Outbreak Detection
# ============================================================
#
# Methods:
#   1. EARS C1
#   2. EARS C2
#   3. EARS C3
#   4. CUSUM-style detector
#
# IMPORTANT:
# This script MUST NOT read ground_truth_outbreaks.csv.
#
# Ground truth is reserved for the independent evaluation stage.
# ============================================================


# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parents[2]

INPUT_PATH = (
    BASE_DIR
    / "data"
    / "gold"
    / "features"
    / "district_disease_epiweek_features"
)

OUTPUT_DIR = (
    BASE_DIR
    / "data"
    / "gold"
    / "analytics"
    / "outbreak_detection"
)

ALERTS_PATH = (
    OUTPUT_DIR
    / "outbreak_alerts"
)

SUMMARY_PATH = (
    OUTPUT_DIR
    / "detection_summary.json"
)


# ------------------------------------------------------------
# Configuration
# ------------------------------------------------------------

EARS_Z_THRESHOLD = 2.0

CUSUM_THRESHOLD = 3.0

BASELINE_WEEKS = 4

CUSUM_BASELINE_WEEKS = 8

MIN_HISTORY_FOR_ALERT = 4


DISEASES = [
    "Dengue",
    "Cholera/Diarrhoeal Disease",
    "Influenza/ILI",
    "Malaria",
]


# ------------------------------------------------------------
# Spark
# ------------------------------------------------------------

def create_spark() -> SparkSession:

    return (
        SparkSession.builder
        .appName(
            "EpiScope-Outbreak-Detection"
        )
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


# ------------------------------------------------------------
# Helpers
# ------------------------------------------------------------

def remove_path(path: Path) -> None:

    if path.exists():

        shutil.rmtree(
            path
        )


def safe_divide(
    numerator,
    denominator,
):

    return F.when(
        denominator.isNotNull()
        & (denominator != 0),
        numerator / denominator,
    )


# ------------------------------------------------------------
# Main
# ------------------------------------------------------------

def main():

    spark = create_spark()

    spark.sparkContext.setLogLevel(
        "WARN"
    )

    print("=" * 70)
    print("EpiScope Outbreak Detection")
    print("=" * 70)

    # --------------------------------------------------------
    # 1. Read feature mart
    # --------------------------------------------------------

    print()
    print(
        "[1/8] Reading Gold Feature Mart..."
    )

    df = spark.read.parquet(
        str(INPUT_PATH)
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

    if diseases != len(DISEASES) or input_rows != expected_rows or input_rows != unique_keys:
        raise RuntimeError(
            "Unexpected feature grid for outbreak detection: "
            f"rows={input_rows}, districts={districts}, diseases={diseases}, "
            f"epi_weeks={epi_weeks}, unique_keys={unique_keys}."
        )

    # --------------------------------------------------------
    # 2. Select required fields
    # --------------------------------------------------------

    print()
    print(
        "[2/8] Preparing detection dataset..."
    )

    df = (
        df
        .select(
            "epi_year",
            "epi_week",
            "epi_week_start",
            "district_name",
            "division_name",
            "district_geocode",
            "disease",
            "case_count",
            "incidence_per_100k",
            "population_total",
            "cases_lag_1",
            "cases_lag_2",
            "cases_lag_3",
            "cases_lag_4",
            "cases_rolling_mean_4w",
            "cases_rolling_std_4w",
            "cases_rolling_mean_8w",
            "cases_rolling_std_8w",
        )
        .filter(
            F.col("disease").isin(
                DISEASES
            )
        )
    )

    # --------------------------------------------------------
    # Detection ordering
    # --------------------------------------------------------

    detection_window = (
        Window
        .partitionBy(
            "district_name",
            "disease",
        )
        .orderBy(
            "epi_week_start"
        )
    )

    # --------------------------------------------------------
    # 3. EARS baseline
    # --------------------------------------------------------

    print()
    print(
        "[3/8] Building EARS baselines..."
    )

    #
    # EARS C1:
    # Current observation compared with previous 4 weeks.
    #
    # z = (current - baseline mean) / baseline std
    #
    # We use rowsBetween(-4,-1), so the current week is NEVER
    # included in its own baseline.
    #

    ears_c1_window = (
        detection_window
        .rowsBetween(
            -4,
            -1
        )
    )

    df = (
        df

        .withColumn(
            "ears_c1_mean",
            F.avg(
                "case_count"
            ).over(
                ears_c1_window
            )
        )

        .withColumn(
            "ears_c1_std",
            F.stddev(
                "case_count"
            ).over(
                ears_c1_window
            )
        )

        .withColumn(
            "ears_c1_z",
            safe_divide(
                F.col("case_count")
                -
                F.col("ears_c1_mean"),
                F.col("ears_c1_std"),
            )
        )

        .withColumn(
            "ears_c1_alert",
            F.when(
                (
                    F.col("ears_c1_z")
                    >=
                    F.lit(EARS_Z_THRESHOLD)
                )
                &
                (
                    F.col("ears_c1_mean")
                    > 0
                ),
                F.lit(1),
            )
            .otherwise(
                F.lit(0)
            )
        )
    )

    # --------------------------------------------------------
    # EARS C2
    # --------------------------------------------------------

    print(
        "  Creating EARS C2..."
    )

    #
    # C2 uses a short-term baseline shifted back by 2 weeks.
    # This reduces sensitivity to very recent increases.
    #

    ears_c2_baseline_window = (
        detection_window
        .rowsBetween(
            -6,
            -3
        )
    )

    df = (
        df

        .withColumn(
            "ears_c2_mean",
            F.avg(
                "case_count"
            ).over(
                ears_c2_baseline_window
            )
        )

        .withColumn(
            "ears_c2_std",
            F.stddev(
                "case_count"
            ).over(
                ears_c2_baseline_window
            )
        )

        .withColumn(
            "ears_c2_z",
            safe_divide(
                F.col("case_count")
                -
                F.col("ears_c2_mean"),
                F.col("ears_c2_std"),
            )
        )

        .withColumn(
            "ears_c2_alert",
            F.when(
                (
                    F.col("ears_c2_z")
                    >=
                    F.lit(EARS_Z_THRESHOLD)
                )
                &
                (
                    F.col("ears_c2_mean")
                    > 0
                ),
                F.lit(1),
            )
            .otherwise(
                F.lit(0)
            )
        )
    )

    # --------------------------------------------------------
    # EARS C3
    # --------------------------------------------------------

    print(
        "  Creating EARS C3..."
    )

    #
    # C3 accumulates consecutive standardized deviations.
    #
    # Instead of requiring only one large spike, the detector
    # can identify sustained growth.
    #

    c3_score = (
        F.when(
            F.col("ears_c1_z").isNotNull()
            &
            (
                F.col("ears_c1_z")
                > F.lit(1.0)
            ),
            F.col("ears_c1_z")
        )
        .otherwise(
            F.lit(0.0)
        )
    )

    df = (
        df
        .withColumn(
            "ears_c3_increment",
            c3_score
        )
    )

    c3_window = (
        detection_window
        .rowsBetween(
            -2,
            0
        )
    )

    df = (
        df
        .withColumn(
            "ears_c3_score",
            F.sum(
                "ears_c3_increment"
            ).over(
                c3_window
            )
        )

        .withColumn(
            "ears_c3_alert",
            F.when(
                (
                    F.col("ears_c3_score")
                    >=
                    F.lit(6.0)
                )
                &
                (
                    F.col("ears_c1_mean")
                    > 0
                ),
                F.lit(1),
            )
            .otherwise(
                F.lit(0)
            )
        )
    )

    # --------------------------------------------------------
    # 4. CUSUM-style detector
    # --------------------------------------------------------

    print()
    print(
        "[4/8] Building CUSUM detector..."
    )

    #
    # Historical baseline:
    # previous 8 weeks, excluding current week.
    #

    cusum_baseline_window = (
        detection_window
        .rowsBetween(
            -8,
            -1
        )
    )

    df = (
        df

        .withColumn(
            "cusum_mean",
            F.avg(
                "case_count"
            ).over(
                cusum_baseline_window
            )
        )

        .withColumn(
            "cusum_std",
            F.stddev(
                "case_count"
            ).over(
                cusum_baseline_window
            )
        )
    )

    #
    # Standardized positive deviation.
    #

    df = (
        df
        .withColumn(
            "cusum_increment",
            F.when(
                (
                    F.col("cusum_mean")
                    .isNotNull()
                )
                &
                (
                    F.col("cusum_std")
                    .isNotNull()
                )
                &
                (
                    F.col("cusum_std")
                    > 0
                ),
                F.greatest(
                    F.lit(0.0),
                    (
                        F.col("case_count")
                        -
                        F.col("cusum_mean")
                    )
                    /
                    F.col("cusum_std")
                    -
                    F.lit(0.5)
                ),
            )
            .otherwise(
                F.lit(0.0)
            )
        )
    )

    #
    # A recursive CUSUM is easiest to express with Spark's
    # cumulative sum over ordered observations.
    #
    # To prevent indefinite accumulation, we reset the signal
    # after a non-positive observation.
    #

    df = (
        df
        .withColumn(
            "cusum_positive_signal",
            F.when(
                F.col("cusum_increment") > 0,
                F.col("cusum_increment")
            )
            .otherwise(
                F.lit(0.0)
            )
        )
    )

    #
    # Three-week accumulated positive deviation.
    # This is the operational CUSUM-style signal used for
    # the first implementation.
    #

    cusum_window = (
        detection_window
        .rowsBetween(
            -2,
            0
        )
    )

    df = (
        df
        .withColumn(
            "cusum_score",
            F.sum(
                "cusum_positive_signal"
            ).over(
                cusum_window
            )
        )

        .withColumn(
            "cusum_alert",
            F.when(
                (
                    F.col("cusum_score")
                    >=
                    F.lit(CUSUM_THRESHOLD)
                )
                &
                (
                    F.col("cusum_mean")
                    > 0
                ),
                F.lit(1),
            )
            .otherwise(
                F.lit(0)
            )
        )
    )

    # --------------------------------------------------------
    # 5. Combined alert classification
    # --------------------------------------------------------

    print()
    print(
        "[5/8] Creating combined alert signals..."
    )

    df = (
        df

        .withColumn(
            "ears_any_alert",
            F.greatest(
                F.col("ears_c1_alert"),
                F.col("ears_c2_alert"),
                F.col("ears_c3_alert"),
            )
        )

        .withColumn(
            "detector_count",
            F.col("ears_c1_alert")
            +
            F.col("ears_c2_alert")
            +
            F.col("ears_c3_alert")
            +
            F.col("cusum_alert")
        )

        .withColumn(
            "consensus_alert",
            F.when(
                F.col("detector_count") >= 2,
                F.lit(1)
            )
            .otherwise(
                F.lit(0)
            )
        )

        .withColumn(
            "alert_level",
            F.when(
                F.col("detector_count") >= 3,
                F.lit("HIGH")
            )
            .when(
                F.col("detector_count") >= 2,
                F.lit("MEDIUM")
            )
            .when(
                F.col("detector_count") == 1,
                F.lit("LOW")
            )
            .otherwise(
                F.lit("NONE")
            )
        )
    )

    # --------------------------------------------------------
    # Require sufficient historical baseline.
    # --------------------------------------------------------

    df = (
        df
        .withColumn(
            "history_available",
            F.when(
                F.col("cases_lag_4").isNotNull(),
                F.lit(1)
            )
            .otherwise(
                F.lit(0)
            )
        )
    )

    # --------------------------------------------------------
    # 6. Write alert mart
    # --------------------------------------------------------

    print()
    print(
        "[6/8] Writing outbreak detection results..."
    )

    remove_path(
        OUTPUT_DIR
    )

    alerts = (
        df
        .select(
            "epi_year",
            "epi_week",
            "epi_week_start",
            "district_name",
            "division_name",
            "district_geocode",
            "disease",
            "case_count",
            "incidence_per_100k",

            # EARS C1
            "ears_c1_mean",
            "ears_c1_std",
            "ears_c1_z",
            "ears_c1_alert",

            # EARS C2
            "ears_c2_mean",
            "ears_c2_std",
            "ears_c2_z",
            "ears_c2_alert",

            # EARS C3
            "ears_c3_score",
            "ears_c3_alert",

            # CUSUM
            "cusum_mean",
            "cusum_std",
            "cusum_score",
            "cusum_alert",

            # Combined
            "ears_any_alert",
            "detector_count",
            "consensus_alert",
            "alert_level",
            "history_available",
        )
        .filter(
            F.col("history_available") == 1
        )
    )

    (
        alerts
        .write
        .mode("overwrite")
        .partitionBy(
            "epi_year",
            "disease"
        )
        .parquet(
            str(ALERTS_PATH)
        )
    )

    # --------------------------------------------------------
    # 7. Detection statistics
    # --------------------------------------------------------

    print()
    print(
        "[7/8] Calculating detection statistics..."
    )

    total_alert_rows = (
        alerts
        .count()
    )

    ears_c1_alert_rows = (
        alerts
        .filter(
            F.col("ears_c1_alert") == 1
        )
        .count()
    )

    ears_c2_alert_rows = (
        alerts
        .filter(
            F.col("ears_c2_alert") == 1
        )
        .count()
    )

    ears_c3_alert_rows = (
        alerts
        .filter(
            F.col("ears_c3_alert") == 1
        )
        .count()
    )

    cusum_alert_rows = (
        alerts
        .filter(
            F.col("cusum_alert") == 1
        )
        .count()
    )

    consensus_alert_rows = (
        alerts
        .filter(
            F.col("consensus_alert") == 1
        )
        .count()
    )

    high_alert_rows = (
        alerts
        .filter(
            F.col("alert_level") == "HIGH"
        )
        .count()
    )

    medium_alert_rows = (
        alerts
        .filter(
            F.col("alert_level") == "MEDIUM"
        )
        .count()
    )

    low_alert_rows = (
        alerts
        .filter(
            F.col("alert_level") == "LOW"
        )
        .count()
    )

    detector_summary = (
        alerts
        .groupBy("disease")
        .agg(
            F.count("*").alias("rows"),

            F.sum(
                "ears_c1_alert"
            ).alias(
                "ears_c1_alerts"
            ),

            F.sum(
                "ears_c2_alert"
            ).alias(
                "ears_c2_alerts"
            ),

            F.sum(
                "ears_c3_alert"
            ).alias(
                "ears_c3_alerts"
            ),

            F.sum(
                "cusum_alert"
            ).alias(
                "cusum_alerts"
            ),

            F.sum(
                "consensus_alert"
            ).alias(
                "consensus_alerts"
            ),
        )
        .orderBy("disease")
        .collect()
    )

    disease_summary = {}

    for row in detector_summary:

        disease_summary[
            row["disease"]
        ] = {

            "rows": int(
                row["rows"]
            ),

            "ears_c1_alerts": int(
                row["ears_c1_alerts"]
                or 0
            ),

            "ears_c2_alerts": int(
                row["ears_c2_alerts"]
                or 0
            ),

            "ears_c3_alerts": int(
                row["ears_c3_alerts"]
                or 0
            ),

            "cusum_alerts": int(
                row["cusum_alerts"]
                or 0
            ),

            "consensus_alerts": int(
                row["consensus_alerts"]
                or 0
            ),
        }

    # --------------------------------------------------------
    # 8. Save summary
    # --------------------------------------------------------

    print()
    print(
        "[8/8] Saving detection summary..."
    )

    summary = {

        "dataset":
            "district_disease_epiweek_outbreak_detection",

        "input_rows":
            input_rows,

        "alert_rows":
            total_alert_rows,

        "thresholds": {

            "ears_z_threshold":
                EARS_Z_THRESHOLD,

            "cusum_threshold":
                CUSUM_THRESHOLD,

            "baseline_weeks":
                BASELINE_WEEKS,

            "cusum_baseline_weeks":
                CUSUM_BASELINE_WEEKS,
        },

        "detectors": [
            "EARS_C1",
            "EARS_C2",
            "EARS_C3",
            "CUSUM_STYLE",
        ],

        "alert_counts": {

            "ears_c1":
                ears_c1_alert_rows,

            "ears_c2":
                ears_c2_alert_rows,

            "ears_c3":
                ears_c3_alert_rows,

            "cusum":
                cusum_alert_rows,

            "consensus":
                consensus_alert_rows,

            "high":
                high_alert_rows,

            "medium":
                medium_alert_rows,

            "low":
                low_alert_rows,
        },

        "disease_summary":
            disease_summary,

        "ground_truth_used":
            False,

        "ground_truth_path":
            None,

        "generated_at":
            datetime.now(
                timezone.utc
            ).isoformat(),
    }

    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

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
    print(
        "OUTBREAK DETECTION COMPLETE"
    )
    print("=" * 70)

    print(
        f"Input rows              : {input_rows}"
    )

    print(
        f"Detection rows          : {total_alert_rows}"
    )

    print()
    print(
        "Alert counts:"
    )

    print(
        f"  EARS C1               : "
        f"{ears_c1_alert_rows}"
    )

    print(
        f"  EARS C2               : "
        f"{ears_c2_alert_rows}"
    )

    print(
        f"  EARS C3               : "
        f"{ears_c3_alert_rows}"
    )

    print(
        f"  CUSUM-style           : "
        f"{cusum_alert_rows}"
    )

    print(
        f"  Consensus (>=2)       : "
        f"{consensus_alert_rows}"
    )

    print()
    print(
        "Alert levels:"
    )

    print(
        f"  HIGH                  : "
        f"{high_alert_rows}"
    )

    print(
        f"  MEDIUM                : "
        f"{medium_alert_rows}"
    )

    print(
        f"  LOW                   : "
        f"{low_alert_rows}"
    )

    print()
    print(
        f"Alerts output : {ALERTS_PATH}"
    )

    print(
        f"Summary       : {SUMMARY_PATH}"
    )

    print()
    print(
        "Ground truth used: NO"
    )

    print("=" * 70)

    spark.stop()


if __name__ == "__main__":
    main()