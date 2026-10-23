from __future__ import annotations

import argparse
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T


# ============================================================
# EpiScope - Silver Transformation + Data Quality
# ============================================================
#
# T2 / ING-2 / DQ-1 / DQ-2 foundation
#
# Main responsibilities:
#   1. Read Bronze S4 encounter Parquet with Spark
#   2. Preserve provenance columns
#   3. Apply automated data-quality rules
#   4. Separate valid and invalid records
#   5. Write invalid records to quarantine
#   6. Write clean records to Silver
#   7. Produce a machine-readable quality report
#
# Important:
#   - Ground truth outbreaks are NEVER read here.
#   - Raw/Bronze data is never modified.
#   - Pandas is NOT used for the heavy encounter transformation.
# ============================================================


PROJECT_ROOT = Path(__file__).resolve().parents[2]

BRONZE_ROOT = PROJECT_ROOT / "data" / "bronze"
SILVER_ROOT = PROJECT_ROOT / "data" / "silver"
QUARANTINE_ROOT = PROJECT_ROOT / "data" / "quarantine"
GOLD_ROOT = PROJECT_ROOT / "data" / "gold"


S4_BRONZE = BRONZE_ROOT / "S4_encounters"
S4_SILVER = SILVER_ROOT / "S4_encounters"
S4_QUARANTINE = QUARANTINE_ROOT / "S4_encounters"

S5_BRONZE = BRONZE_ROOT / "S5_facilities"
S5_SILVER = SILVER_ROOT / "S5_facilities"


QUALITY_GOLD = GOLD_ROOT / "quality"

SILVER_DISEASE_MAPPING = {
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
    "J06.9": "Respiratory infection (background)",
}


# ============================================================
# Spark
# ============================================================


def create_spark() -> SparkSession:
    """
    Create local Spark session.

    Adaptive execution and partition settings are enabled because
    the encounter dataset is much larger than the reference data.
    """
    spark = (
        SparkSession.builder
        .appName("EpiScope-Silver-DQ")
        .master("local[*]")
        .config(
            "spark.sql.adaptive.enabled",
            "true",
        )
        .config(
            "spark.sql.adaptive.coalescePartitions.enabled",
            "true",
        )
        .config(
            "spark.sql.shuffle.partitions",
            "8",
        )
        .config(
            "spark.sql.files.maxPartitionBytes",
            "134217728",
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark


# ============================================================
# Helpers
# ============================================================


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def create_batch_id() -> str:
    timestamp = datetime.now(timezone.utc).strftime(
        "%Y%m%dT%H%M%SZ"
    )

    return (
        f"SILVER_{timestamp}_{uuid.uuid4().hex[:10]}"
    )


def path_exists(path: Path) -> bool:
    return path.exists()


def find_parquet_files(root: Path) -> list[str]:
    """
    Find parquet files recursively.

    Ground-truth directories are explicitly excluded.
    """
    if not root.exists():
        return []

    files = []

    for path in root.rglob("*.parquet"):
        normalized = str(path).replace("\\", "/").lower()

        if "ground_truth_outbreaks" in normalized:
            continue

        files.append(str(path))

    return sorted(files)


def write_json(path: Path, data: dict) -> None:
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with path.open(
        "w",
        encoding="utf-8",
    ) as file:
        json.dump(
            data,
            file,
            indent=2,
            ensure_ascii=False,
        )


# ============================================================
# Read Bronze facilities
# ============================================================


def read_facilities(
    spark: SparkSession,
) -> DataFrame:
    """
    Read facility registry.

    Facility registry is small compared with encounters, so it is
    suitable for a broadcast join.
    """

    facility_files = list(
        S5_BRONZE.glob("*.csv")
    )

    if not facility_files:
        raise FileNotFoundError(
            f"No facility CSV found in {S5_BRONZE}"
        )

    facility_path = str(
        facility_files[0]
    )

    print()
    print(
        f"Reading facilities: {facility_path}"
    )

    facilities = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(facility_path)
    )

    print(
        f"Facility columns: {facilities.columns}"
    )

    required = {
        "facility_id",
        "district_name",
    }

    missing = required.difference(
        facilities.columns
    )

    if missing:
        raise ValueError(
            "Facility registry is missing required columns: "
            + ", ".join(sorted(missing))
        )

    # Keep only columns required for validation and useful
    # conformance.
    keep_columns = [
        c
        for c in [
            "facility_id",
            "district_name",
            "facility_type",
            "bed_capacity",
            "active",
        ]
        if c in facilities.columns
    ]

    facilities = facilities.select(
        *keep_columns
    )

    facilities = facilities.dropDuplicates(
        ["facility_id"]
    )

    return facilities


# ============================================================
# Read Bronze encounters
# ============================================================


def read_encounters(
    spark: SparkSession,
) -> DataFrame:
    """
    Read S4 encounter Parquet files.

    Ground truth is excluded.
    """

    parquet_files = find_parquet_files(
        S4_BRONZE
    )

    if not parquet_files:
        raise FileNotFoundError(
            f"No encounter Parquet files found in {S4_BRONZE}"
        )

    print()
    print(
        f"Encounter parquet files: {len(parquet_files)}"
    )

    encounters = (
        spark.read
        .parquet(*parquet_files)
    )

    print()
    print(
        "Encounter columns:"
    )

    for column in encounters.columns:
        print(
            f"  - {column}"
        )

    return encounters


# ============================================================
# Schema normalization
# ============================================================


def normalize_encounter_schema(
    df: DataFrame,
) -> DataFrame:
    """
    Normalize known encounter fields.

    This transformation creates consistent types while retaining
    all source columns.
    """

    expected_columns = [
        "encounter_id",
        "patient_id",
        "facility_id",
        "onset_date",
        "report_date",
        "age_years",
        "sex",
        "icd10_code",
        "lab_confirmed",
        "admitted",
        "outcome",
    ]

    missing = [
        c
        for c in expected_columns
        if c not in df.columns
    ]

    if missing:
        raise ValueError(
            "S4 encounter data is missing required columns: "
            + ", ".join(missing)
        )

    # Date normalization.
    df = (
        df
        .withColumn(
            "onset_date",
            F.to_date(
                F.col("onset_date")
            ),
        )
        .withColumn(
            "report_date",
            F.to_date(
                F.col("report_date")
            ),
        )
        .withColumn(
            "age_years",
            F.col("age_years").cast(
                T.IntegerType()
            ),
        )
    )

    # Normalize text fields.
    text_columns = [
        "encounter_id",
        "patient_id",
        "facility_id",
        "sex",
        "icd10_code",
        "outcome",
    ]

    for column in text_columns:
        df = df.withColumn(
            column,
            F.trim(
                F.col(column).cast("string")
            ),
        )

    # Normalize Boolean-like columns.
    for column in [
        "lab_confirmed",
        "admitted",
    ]:
        if column in df.columns:
            df = df.withColumn(
                column,
                F.when(
                    F.lower(
                        F.col(column).cast("string")
                    ).isin(
                        "true",
                        "1",
                        "yes",
                        "y",
                    ),
                    F.lit(True),
                )
                .when(
                    F.lower(
                        F.col(column).cast("string")
                    ).isin(
                        "false",
                        "0",
                        "no",
                        "n",
                    ),
                    F.lit(False),
                )
                .otherwise(
                    F.lit(None).cast("boolean")
                ),
            )

    disease_expression = F.create_map(
        *[
            F.lit(value)
            for pair in SILVER_DISEASE_MAPPING.items()
            for value in pair
        ]
    )
    df = df.withColumn(
        "disease",
        F.coalesce(
            disease_expression[F.upper(F.col("icd10_code"))],
            F.col("disease"),
        ),
    )

    return df


# ============================================================
# DQ rules
# ============================================================


def add_quality_flags(
    df: DataFrame,
    facilities: DataFrame,
) -> DataFrame:
    """
    Apply automated DQ rules.

    At least 12 independent quality rules are represented.
    """

    # --------------------------------------------------------
    # Reference facility lookup
    # --------------------------------------------------------

    facility_keys = (
        facilities
        .select(
            "facility_id",
            F.col("district_name")
            .alias("facility_district"),
        )
        .dropDuplicates(
            ["facility_id"]
        )
    )

    facility_keys = F.broadcast(
        facility_keys
    )

    df = df.join(
        facility_keys,
        on="facility_id",
        how="left",
    )

    # --------------------------------------------------------
    # Rule 1 - encounter_id missing
    # --------------------------------------------------------

    df = df.withColumn(
        "dq_missing_encounter_id",
        F.col("encounter_id").isNull()
        | (F.length("encounter_id") == 0),
    )

    # --------------------------------------------------------
    # Rule 2 - patient_id missing
    # --------------------------------------------------------

    df = df.withColumn(
        "dq_missing_patient_id",
        F.col("patient_id").isNull()
        | (F.length("patient_id") == 0),
    )

    # --------------------------------------------------------
    # Rule 3 - facility_id missing
    # --------------------------------------------------------

    df = df.withColumn(
        "dq_missing_facility_id",
        F.col("facility_id").isNull()
        | (F.length("facility_id") == 0),
    )

    # --------------------------------------------------------
    # Rule 4 - orphan facility key
    # --------------------------------------------------------

    df = df.withColumn(
        "dq_orphan_facility",
        (~F.col("dq_missing_facility_id"))
        & F.col("facility_district").isNull(),
    )

    # --------------------------------------------------------
    # Rule 5 - invalid age
    #
    # Valid synthetic encounter age range:
    # 0 <= age <= 120
    # --------------------------------------------------------

    df = df.withColumn(
        "dq_invalid_age",
        F.col("age_years").isNull()
        | (F.col("age_years") < 0)
        | (F.col("age_years") > 120),
    )

    # --------------------------------------------------------
    # Rule 6 - onset date missing
    # --------------------------------------------------------

    df = df.withColumn(
        "dq_missing_onset_date",
        F.col("onset_date").isNull(),
    )

    # --------------------------------------------------------
    # Rule 7 - report date missing
    # --------------------------------------------------------

    df = df.withColumn(
        "dq_missing_report_date",
        F.col("report_date").isNull(),
    )

    # --------------------------------------------------------
    # Rule 8 - report before onset
    # --------------------------------------------------------

    df = df.withColumn(
        "dq_report_before_onset",
        F.col("report_date").isNotNull()
        & F.col("onset_date").isNotNull()
        & (
            F.col("report_date")
            < F.col("onset_date")
        ),
    )

    # --------------------------------------------------------
    # Rule 9 - report delay > 21 days
    # --------------------------------------------------------

    report_delay = F.datediff(
        F.col("report_date"),
        F.col("onset_date"),
    )

    df = df.withColumn(
        "report_delay_days",
        report_delay,
    )

    df = df.withColumn(
        "dq_report_delay_gt_21",
        F.col("report_delay_days") > 21,
    )

    # --------------------------------------------------------
    # Rule 10 - invalid sex
    # --------------------------------------------------------

    df = df.withColumn(
        "dq_invalid_sex",
        F.col("sex").isNotNull()
        & (~F.upper(F.col("sex")).isin(
            "M",
            "F",
            "MALE",
            "FEMALE",
            "OTHER",
            "UNKNOWN",
            "U",
        )),
    )

    # --------------------------------------------------------
    # Rule 11 - missing disease code
    # --------------------------------------------------------

    df = df.withColumn(
        "dq_missing_icd10",
        F.col("icd10_code").isNull()
        | (F.length("icd10_code") == 0),
    )

    # --------------------------------------------------------
    # Rule 12 - invalid lab_confirmed
    # --------------------------------------------------------

    df = df.withColumn(
        "dq_invalid_lab_confirmed",
        F.col("lab_confirmed").isNull(),
    )

    # --------------------------------------------------------
    # Rule 13 - invalid admitted
    # --------------------------------------------------------

    df = df.withColumn(
        "dq_invalid_admitted",
        F.col("admitted").isNull(),
    )

    # --------------------------------------------------------
    # Rule 14 - invalid outcome
    # --------------------------------------------------------

    valid_outcomes = [
        "recovered",
        "death",
        "deceased",
        "active",
        "discharged",
        "unknown",
        "pending",
    ]

    df = df.withColumn(
        "dq_invalid_outcome",
        F.col("outcome").isNotNull()
        & (
            ~F.lower(
                F.col("outcome")
            ).isin(*valid_outcomes)
        ),
    )

    # --------------------------------------------------------
    # Rule 15 - future date
    #
    # Synthetic data ends 2025-12-31.
    # --------------------------------------------------------

    df = df.withColumn(
        "dq_future_onset_date",
        F.col("onset_date")
        > F.lit("2025-12-31").cast("date"),
    )

    # --------------------------------------------------------
    # Rule 16 - date before project start
    # --------------------------------------------------------

    df = df.withColumn(
        "dq_before_project_start",
        F.col("onset_date")
        < F.lit("2021-01-01").cast("date"),
    )

    return df


# ============================================================
# Duplicate detection
# ============================================================


def add_duplicate_flags(
    df: DataFrame,
) -> DataFrame:
    """
    Identify duplicate encounter IDs.

    The assignment intentionally injects approximately 2%
    duplicate encounter IDs, so these records are quarantined.
    """

    duplicate_ids = (
        df
        .filter(
            F.col("encounter_id").isNotNull()
        )
        .groupBy("encounter_id")
        .count()
        .filter(
            F.col("count") > 1
        )
        .select(
            "encounter_id"
        )
        .withColumn(
            "dq_duplicate_encounter_id",
            F.lit(True),
        )
    )

    df = df.join(
        F.broadcast(duplicate_ids),
        on="encounter_id",
        how="left",
    )

    df = df.withColumn(
        "dq_duplicate_encounter_id",
        F.coalesce(
            F.col("dq_duplicate_encounter_id"),
            F.lit(False),
        ),
    )

    return df


# ============================================================
# Quality reason
# ============================================================


def build_quality_reason(
    df: DataFrame,
) -> DataFrame:
    """
    Build human-readable DQ reason string.
    """

    rules = [
        (
            "MISSING_ENCOUNTER_ID",
            "dq_missing_encounter_id",
        ),
        (
            "MISSING_PATIENT_ID",
            "dq_missing_patient_id",
        ),
        (
            "MISSING_FACILITY_ID",
            "dq_missing_facility_id",
        ),
        (
            "ORPHAN_FACILITY",
            "dq_orphan_facility",
        ),
        (
            "INVALID_AGE",
            "dq_invalid_age",
        ),
        (
            "MISSING_ONSET_DATE",
            "dq_missing_onset_date",
        ),
        (
            "MISSING_REPORT_DATE",
            "dq_missing_report_date",
        ),
        (
            "REPORT_BEFORE_ONSET",
            "dq_report_before_onset",
        ),
        (
            "REPORT_DELAY_GT_21",
            "dq_report_delay_gt_21",
        ),
        (
            "INVALID_SEX",
            "dq_invalid_sex",
        ),
        (
            "MISSING_ICD10",
            "dq_missing_icd10",
        ),
        (
            "INVALID_LAB_CONFIRMED",
            "dq_invalid_lab_confirmed",
        ),
        (
            "INVALID_ADMITTED",
            "dq_invalid_admitted",
        ),
        (
            "INVALID_OUTCOME",
            "dq_invalid_outcome",
        ),
        (
            "FUTURE_ONSET_DATE",
            "dq_future_onset_date",
        ),
        (
            "BEFORE_PROJECT_START",
            "dq_before_project_start",
        ),
        (
            "DUPLICATE_ENCOUNTER_ID",
            "dq_duplicate_encounter_id",
        ),
    ]

    expressions = []

    for label, column in rules:
        expressions.append(
            F.when(
                F.col(column),
                F.lit(label),
            )
        )

    df = df.withColumn(
        "dq_reason_array",
        F.array(*expressions),
    )

    df = df.withColumn(
        "dq_reason_array",
        F.expr(
            """
            filter(dq_reason_array, x -> x is not null)
            """
        ),
    )

    df = df.withColumn(
        "dq_reason",
        F.when(
            F.size(
                F.col("dq_reason_array")
            ) > 0,
            F.array_join(
                F.col("dq_reason_array"),
                "|",
            ),
        ).otherwise(
            F.lit(None)
        ),
    )

    df = df.drop(
        "dq_reason_array"
    )

    return df


# ============================================================
# Quality report
# ============================================================


def calculate_quality_report(
    flagged: DataFrame,
    total_count: int,
    batch_id: str,
) -> DataFrame:
    """
    Produce one-row-per-rule quality report.
    """

    rules = [
        (
            "DQ01",
            "missing_encounter_id",
            "dq_missing_encounter_id",
        ),
        (
            "DQ02",
            "missing_patient_id",
            "dq_missing_patient_id",
        ),
        (
            "DQ03",
            "missing_facility_id",
            "dq_missing_facility_id",
        ),
        (
            "DQ04",
            "orphan_facility",
            "dq_orphan_facility",
        ),
        (
            "DQ05",
            "invalid_age",
            "dq_invalid_age",
        ),
        (
            "DQ06",
            "missing_onset_date",
            "dq_missing_onset_date",
        ),
        (
            "DQ07",
            "missing_report_date",
            "dq_missing_report_date",
        ),
        (
            "DQ08",
            "report_before_onset",
            "dq_report_before_onset",
        ),
        (
            "DQ09",
            "report_delay_gt_21",
            "dq_report_delay_gt_21",
        ),
        (
            "DQ10",
            "invalid_sex",
            "dq_invalid_sex",
        ),
        (
            "DQ11",
            "missing_icd10",
            "dq_missing_icd10",
        ),
        (
            "DQ12",
            "invalid_lab_confirmed",
            "dq_invalid_lab_confirmed",
        ),
        (
            "DQ13",
            "invalid_admitted",
            "dq_invalid_admitted",
        ),
        (
            "DQ14",
            "invalid_outcome",
            "dq_invalid_outcome",
        ),
        (
            "DQ15",
            "future_onset_date",
            "dq_future_onset_date",
        ),
        (
            "DQ16",
            "before_project_start",
            "dq_before_project_start",
        ),
        (
            "DQ17",
            "duplicate_encounter_id",
            "dq_duplicate_encounter_id",
        ),
    ]

    aggregations = []

    for _, _, column in rules:
        aggregations.append(
            F.sum(
                F.when(
                    F.col(column),
                    1,
                ).otherwise(0)
            ).alias(column)
        )

    result = flagged.agg(
        *aggregations
    ).collect()[0]

    rows = []

    for rule_id, rule_name, column in rules:
        failed_count = int(
            result[column] or 0
        )

        failure_rate = (
            failed_count / total_count
            if total_count > 0
            else 0.0
        )

        rows.append(
            (
                batch_id,
                utc_now(),
                rule_id,
                rule_name,
                total_count,
                failed_count,
                failure_rate,
            )
        )

    schema = T.StructType(
        [
            T.StructField(
                "batch_id",
                T.StringType(),
                False,
            ),
            T.StructField(
                "report_generated_at",
                T.StringType(),
                False,
            ),
            T.StructField(
                "rule_id",
                T.StringType(),
                False,
            ),
            T.StructField(
                "rule_name",
                T.StringType(),
                False,
            ),
            T.StructField(
                "total_records",
                T.LongType(),
                False,
            ),
            T.StructField(
                "failed_records",
                T.LongType(),
                False,
            ),
            T.StructField(
                "failure_rate",
                T.DoubleType(),
                False,
            ),
        ]
    )

    return flagged.sparkSession.createDataFrame(
        rows,
        schema=schema,
    )


# ============================================================
# Silver metadata
# ============================================================


def add_silver_metadata(
    df: DataFrame,
    batch_id: str,
) -> DataFrame:
    """
    Add technical/provenance fields needed for downstream
    lineage.
    """

    return (
        df
        .withColumn(
            "silver_batch_id",
            F.lit(batch_id),
        )
        .withColumn(
            "silver_load_time",
            F.current_timestamp(),
        )
        .withColumn(
            "source_layer",
            F.lit("bronze"),
        )
        .withColumn(
            "data_layer",
            F.lit("silver"),
        )
    )


# ============================================================
# Main transformation
# ============================================================


def run(
    mode: str = "all",
) -> None:

    spark = create_spark()

    batch_id = create_batch_id()

    print()
    print("=" * 70)
    print("EpiScope - Silver Transformation + Data Quality")
    print("=" * 70)

    print(
        f"Batch ID: {batch_id}"
    )

    print(
        f"Bronze S4: {S4_BRONZE}"
    )

    print(
        f"Silver S4: {S4_SILVER}"
    )

    print(
        f"Quarantine: {S4_QUARANTINE}"
    )

    try:
        # ----------------------------------------------------
        # Read sources
        # ----------------------------------------------------

        encounters = read_encounters(
            spark
        )

        facilities = read_facilities(
            spark
        )

        # ----------------------------------------------------
        # Basic count
        # ----------------------------------------------------

        print()
        print(
            "Counting Bronze encounters..."
        )

        total_count = encounters.count()

        print(
            f"Bronze encounter records: {total_count:,}"
        )

        # ----------------------------------------------------
        # Normalize
        # ----------------------------------------------------

        print()
        print(
            "Normalizing encounter schema..."
        )

        normalized = normalize_encounter_schema(
            encounters
        )

        # ----------------------------------------------------
        # Add DQ flags
        # ----------------------------------------------------

        print()
        print(
            "Applying data-quality rules..."
        )

        flagged = add_quality_flags(
            normalized,
            facilities,
        )

        # ----------------------------------------------------
        # Duplicate detection
        # ----------------------------------------------------

        print(
            "Detecting duplicate encounter IDs..."
        )

        flagged = add_duplicate_flags(
            flagged
        )

        # ----------------------------------------------------
        # Quality reason
        # ----------------------------------------------------

        flagged = build_quality_reason(
            flagged
        )

        # ----------------------------------------------------
        # Add batch metadata
        # ----------------------------------------------------

        flagged = add_silver_metadata(
            flagged,
            batch_id,
        )

        # ----------------------------------------------------
        # Persist intermediate DataFrame
        #
        # This prevents Spark from recomputing the expensive
        # duplicate detection and joins multiple times.
        # ----------------------------------------------------

        flagged = flagged.repartition(
            8
        ).persist()

        # Materialize cache.
        flagged_count = flagged.count()

        print(
            f"Flagged records materialized: {flagged_count:,}"
        )

        # ----------------------------------------------------
        # Valid / quarantine split
        # ----------------------------------------------------

        invalid_condition = (
            F.col("dq_missing_encounter_id")
            | F.col("dq_missing_patient_id")
            | F.col("dq_missing_facility_id")
            | F.col("dq_orphan_facility")
            | F.col("dq_invalid_age")
            | F.col("dq_missing_onset_date")
            | F.col("dq_missing_report_date")
            | F.col("dq_report_before_onset")
            | F.col("dq_report_delay_gt_21")
            | F.col("dq_invalid_sex")
            | F.col("dq_missing_icd10")
            | F.col("dq_invalid_lab_confirmed")
            | F.col("dq_invalid_admitted")
            | F.col("dq_invalid_outcome")
            | F.col("dq_future_onset_date")
            | F.col("dq_before_project_start")
            | F.col("dq_duplicate_encounter_id")
        )

        quarantine = flagged.filter(
            invalid_condition
        )

        clean = flagged.filter(
            ~invalid_condition
        )

        # ----------------------------------------------------
        # Quality counts
        # ----------------------------------------------------

        quarantine_count = quarantine.count()
        clean_count = clean.count()

        print()
        print(
            "=" * 70
        )
        print(
            "QUALITY SUMMARY"
        )
        print(
            "=" * 70
        )

        print(
            f"Total records     : {total_count:,}"
        )

        print(
            f"Clean records     : {clean_count:,}"
        )

        print(
            f"Quarantine records: {quarantine_count:,}"
        )

        if total_count > 0:
            print(
                "Quarantine rate   : "
                f"{quarantine_count / total_count:.4%}"
            )

        # ----------------------------------------------------
        # Write Silver
        # ----------------------------------------------------

        print()
        print(
            f"Writing Silver data: {S4_SILVER}"
        )

        (
            clean
            .write
            .mode("overwrite")
            .partitionBy("onset_date")
            .parquet(
                str(S4_SILVER)
            )
        )

        # ----------------------------------------------------
        # Write quarantine
        # ----------------------------------------------------

        print()
        print(
            f"Writing quarantine: {S4_QUARANTINE}"
        )

        (
            quarantine
            .write
            .mode("overwrite")
            .partitionBy("onset_date")
            .parquet(
                str(S4_QUARANTINE)
            )
        )

        # ----------------------------------------------------
        # Quality report
        # ----------------------------------------------------

        print()
        print(
            "Generating quality report..."
        )

        quality_report = calculate_quality_report(
            flagged=flagged,
            total_count=total_count,
            batch_id=batch_id,
        )

        quality_report = quality_report.withColumn(
            "source",
            F.lit("S4_encounters"),
        )

        (
            quality_report
            .write
            .mode("overwrite")
            .parquet(
                str(
                    QUALITY_GOLD
                    / "s4_encounters"
                )
            )
        )

        # ----------------------------------------------------
        # Save JSON summary
        # ----------------------------------------------------

        summary = {
            "project": "EpiScope",
            "layer": "Silver",
            "source": "S4_encounters",
            "batch_id": batch_id,
            "generated_at_utc": utc_now(),
            "bronze_records": total_count,
            "clean_records": clean_count,
            "quarantine_records": quarantine_count,
            "quarantine_rate": (
                quarantine_count / total_count
                if total_count > 0
                else 0.0
            ),
            "dq_rule_count": 17,
            "silver_path": str(S4_SILVER),
            "quarantine_path": str(
                S4_QUARANTINE
            ),
            "quality_report_path": str(
                QUALITY_GOLD
                / "s4_encounters"
            ),
        }

        summary_path = (
            QUALITY_GOLD
            / "s4_encounters_summary.json"
        )

        write_json(
            summary_path,
            summary,
        )

        # ----------------------------------------------------
        # Unpersist
        # ----------------------------------------------------

        flagged.unpersist()

        print()
        print("=" * 70)
        print("SILVER TRANSFORMATION COMPLETE")
        print("=" * 70)

        print(
            f"Silver output : {S4_SILVER}"
        )

        print(
            f"Quarantine    : {S4_QUARANTINE}"
        )

        print(
            f"Quality report: {QUALITY_GOLD / 's4_encounters'}"
        )

        print(
            f"Summary JSON  : {summary_path}"
        )

        print("=" * 70)

    finally:
        spark.stop()


# ============================================================
# CLI
# ============================================================


def main() -> None:

    parser = argparse.ArgumentParser(
        description=(
            "EpiScope S4 Bronze -> Silver "
            "transformation with data quality"
        )
    )

    parser.add_argument(
        "--mode",
        default="all",
        choices=[
            "all",
        ],
    )

    args = parser.parse_args()

    run(
        mode=args.mode
    )


if __name__ == "__main__":
    main()