from pathlib import Path
from datetime import datetime
import json

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.compute as pc
import pyarrow.parquet as pq


# ============================================================
# EpiScope - S4 Silver Quality Report
# PyArrow-based verification report
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

SILVER_S4 = (
    PROJECT_ROOT
    / "data"
    / "silver"
    / "S4_encounters"
)

QUALITY_GOLD = (
    PROJECT_ROOT
    / "data"
    / "gold"
    / "quality"
    / "s4_encounters"
)

SUMMARY_JSON = (
    PROJECT_ROOT
    / "data"
    / "gold"
    / "quality"
    / "s4_encounters_summary.json"
)


REQUIRED_COLUMNS = [
    "encounter_id",
    "patient_id",
    "facility_id",
    "age_years",
    "onset_date",
    "report_date",
    "sex",
    "icd10_code",
    "lab_confirmed",
    "admitted",
    "outcome",
]


def is_null_or_empty(column):
    """
    True for null or empty/whitespace strings.
    """

    null_mask = pc.is_null(column)

    string_column = pc.cast(
        column,
        pa.string(),
        safe=False,
    )

    empty_mask = pc.equal(
        pc.utf8_trim_whitespace(string_column),
        "",
    )

    return pc.or_(
        null_mask,
        empty_mask,
    )


def date_column(table, name):
    """
    Convert a date-like column to date32 where possible.
    """

    column = table[name]

    try:
        return pc.cast(
            column,
            pa.date32(),
            safe=False,
        )

    except Exception:

        parsed = pc.strptime(
            pc.cast(
                column,
                pa.string(),
                safe=False,
            ),
            format="%Y-%m-%d",
            unit="s",
        )

        return pc.cast(
            parsed,
            pa.date32(),
            safe=False,
        )


def count_mask(mask):
    """
    Count True values in a boolean Arrow array.
    """

    if mask is None:
        return 0

    value = pc.sum(
        pc.cast(
            mask,
            pa.int64(),
        )
    )

    if value is None:
        return 0

    return int(value.as_py())


def main():

    print("=" * 70)
    print("EpiScope - S4 Silver Quality Report")
    print("=" * 70)

    # --------------------------------------------------------
    # Check Silver path
    # --------------------------------------------------------

    if not SILVER_S4.exists():

        raise FileNotFoundError(
            f"Silver S4 directory not found: {SILVER_S4}"
        )

    print()
    print("Silver path:")
    print(SILVER_S4)

    # --------------------------------------------------------
    # Create Parquet dataset
    # --------------------------------------------------------

    print()
    print("Creating Parquet dataset...")

    dataset = ds.dataset(
        str(SILVER_S4),
        format="parquet",
        partitioning="hive",
    )

    available_columns = set(
        dataset.schema.names
    )

    missing_columns = [
        column
        for column in REQUIRED_COLUMNS
        if column not in available_columns
    ]

    if missing_columns:

        raise ValueError(
            "Required Silver columns are missing: "
            + ", ".join(missing_columns)
        )

    # --------------------------------------------------------
    # Read required columns
    # --------------------------------------------------------

    print()
    print("Reading required Silver columns...")

    table = dataset.to_table(
        columns=REQUIRED_COLUMNS
    )

    total_count = table.num_rows

    print()
    print(
        f"Total Silver records: {total_count:,}"
    )

    # --------------------------------------------------------
    # Columns
    # --------------------------------------------------------

    encounter_id = table["encounter_id"]
    patient_id = table["patient_id"]
    facility_id = table["facility_id"]
    age_years = table["age_years"]

    onset_date = date_column(
        table,
        "onset_date",
    )

    report_date = date_column(
        table,
        "report_date",
    )

    sex = table["sex"]
    icd10_code = table["icd10_code"]
    lab_confirmed = table["lab_confirmed"]
    admitted = table["admitted"]
    outcome = table["outcome"]

    # --------------------------------------------------------
    # Build DQ masks
    # --------------------------------------------------------

    rules = []

    # --------------------------------------------------------
    # DQ-S4-01
    # --------------------------------------------------------

    rules.append(
        (
            "DQ-S4-01",
            "missing_encounter_id",
            is_null_or_empty(encounter_id),
        )
    )

    # --------------------------------------------------------
    # DQ-S4-02
    # --------------------------------------------------------

    rules.append(
        (
            "DQ-S4-02",
            "missing_patient_id",
            is_null_or_empty(patient_id),
        )
    )

    # --------------------------------------------------------
    # DQ-S4-03
    # --------------------------------------------------------

    rules.append(
        (
            "DQ-S4-03",
            "missing_facility_id",
            is_null_or_empty(facility_id),
        )
    )

    # --------------------------------------------------------
    # DQ-S4-04
    # Invalid age
    # --------------------------------------------------------

    age_numeric = pc.cast(
        age_years,
        pa.int64(),
        safe=False,
    )

    invalid_age = pc.or_(
        pc.is_null(age_numeric),
        pc.or_(
            pc.less(
                age_numeric,
                0,
            ),
            pc.greater(
                age_numeric,
                120,
            ),
        ),
    )

    rules.append(
        (
            "DQ-S4-04",
            "invalid_age",
            invalid_age,
        )
    )

    # --------------------------------------------------------
    # DQ-S4-05
    # --------------------------------------------------------

    rules.append(
        (
            "DQ-S4-05",
            "missing_onset_date",
            pc.is_null(onset_date),
        )
    )

    # --------------------------------------------------------
    # DQ-S4-06
    # --------------------------------------------------------

    rules.append(
        (
            "DQ-S4-06",
            "missing_report_date",
            pc.is_null(report_date),
        )
    )

    # --------------------------------------------------------
    # DQ-S4-07
    # Report before onset
    # --------------------------------------------------------

    both_dates_present = pc.and_(
        pc.invert(
            pc.is_null(report_date)
        ),
        pc.invert(
            pc.is_null(onset_date)
        ),
    )

    report_before_onset = pc.and_(
        both_dates_present,
        pc.less(
            report_date,
            onset_date,
        ),
    )

    rules.append(
        (
            "DQ-S4-07",
            "report_before_onset",
            report_before_onset,
        )
    )

    # --------------------------------------------------------
    # DQ-S4-08
    # Report delay > 21 days
    # --------------------------------------------------------

    report_delay = pc.days_between(
        onset_date,
        report_date,
    )

    report_delay_gt_21 = pc.and_(
        pc.invert(
            pc.is_null(report_delay)
        ),
        pc.greater(
            report_delay,
            21,
        ),
    )

    rules.append(
        (
            "DQ-S4-08",
            "report_delay_gt_21",
            report_delay_gt_21,
        )
    )

    # --------------------------------------------------------
    # String normalisation
    # --------------------------------------------------------

    sex_string = pc.utf8_upper(
        pc.utf8_trim_whitespace(
            pc.cast(
                sex,
                pa.string(),
                safe=False,
            )
        )
    )

    valid_sex = pc.is_in(
        sex_string,
        value_set=pa.array(
            [
                "M",
                "F",
                "MALE",
                "FEMALE",
                "OTHER",
                "UNKNOWN",
            ]
        ),
    )

    invalid_sex = pc.or_(
        pc.is_null(sex),
        pc.invert(valid_sex),
    )

    rules.append(
        (
            "DQ-S4-09",
            "invalid_sex",
            invalid_sex,
        )
    )

    # --------------------------------------------------------
    # DQ-S4-10
    # --------------------------------------------------------

    rules.append(
        (
            "DQ-S4-10",
            "missing_icd10",
            is_null_or_empty(icd10_code),
        )
    )

    # --------------------------------------------------------
    # Boolean-like fields
    # --------------------------------------------------------

    valid_boolean_values = pa.array(
        [
            "TRUE",
            "FALSE",
            "1",
            "0",
            "YES",
            "NO",
        ]
    )

    # --------------------------------------------------------
    # DQ-S4-11
    # --------------------------------------------------------

    lab_string = pc.utf8_upper(
        pc.utf8_trim_whitespace(
            pc.cast(
                lab_confirmed,
                pa.string(),
                safe=False,
            )
        )
    )

    valid_lab = pc.is_in(
        lab_string,
        value_set=valid_boolean_values,
    )

    invalid_lab = pc.or_(
        pc.is_null(lab_confirmed),
        pc.invert(valid_lab),
    )

    rules.append(
        (
            "DQ-S4-11",
            "invalid_lab_confirmed",
            invalid_lab,
        )
    )

    # --------------------------------------------------------
    # DQ-S4-12
    # --------------------------------------------------------

    admitted_string = pc.utf8_upper(
        pc.utf8_trim_whitespace(
            pc.cast(
                admitted,
                pa.string(),
                safe=False,
            )
        )
    )

    valid_admitted = pc.is_in(
        admitted_string,
        value_set=valid_boolean_values,
    )

    invalid_admitted = pc.or_(
        pc.is_null(admitted),
        pc.invert(valid_admitted),
    )

    rules.append(
        (
            "DQ-S4-12",
            "invalid_admitted",
            invalid_admitted,
        )
    )

    # --------------------------------------------------------
    # DQ-S4-13
    # --------------------------------------------------------

    rules.append(
        (
            "DQ-S4-13",
            "missing_outcome",
            is_null_or_empty(outcome),
        )
    )

    # --------------------------------------------------------
    # DQ-S4-14
    # Future onset date
    # --------------------------------------------------------

    future_onset = pc.and_(
        pc.invert(
            pc.is_null(onset_date)
        ),
        pc.greater(
            onset_date,
            pa.scalar(
                datetime(
                    2025,
                    12,
                    31,
                ).date(),
                type=pa.date32(),
            ),
        ),
    )

    rules.append(
        (
            "DQ-S4-14",
            "future_onset_date",
            future_onset,
        )
    )

    # --------------------------------------------------------
    # DQ-S4-15
    # Onset before 2021
    # --------------------------------------------------------

    before_2021 = pc.and_(
        pc.invert(
            pc.is_null(onset_date)
        ),
        pc.less(
            onset_date,
            pa.scalar(
                datetime(
                    2021,
                    1,
                    1,
                ).date(),
                type=pa.date32(),
            ),
        ),
    )

    rules.append(
        (
            "DQ-S4-15",
            "onset_before_2021",
            before_2021,
        )
    )

    # --------------------------------------------------------
    # Calculate results
    # --------------------------------------------------------

    print()
    print("Calculating quality rules...")

    generated_at = datetime.now().isoformat(
        timespec="seconds"
    )

    report_rows = []

    for rule_id, rule_name, mask in rules:

        failed_records = count_mask(
            mask
        )

        if total_count > 0:

            failure_rate = (
                failed_records
                / total_count
            )

        else:

            failure_rate = 0.0

        status = (
            "PASS"
            if failed_records == 0
            else "CHECK"
        )

        report_rows.append(
            {
                "rule_id": rule_id,
                "rule_name": rule_name,
                "total_records": total_count,
                "failed_records": failed_records,
                "failure_rate": float(
                    failure_rate
                ),
                "status": status,
                "source": "S4_encounters",
                "report_generated_at": generated_at,
            }
        )

    # --------------------------------------------------------
    # Write Gold report using PyArrow
    # --------------------------------------------------------

    print()
    print("Writing Gold quality report...")

    QUALITY_GOLD.mkdir(
        parents=True,
        exist_ok=True,
    )

    report_table = pa.Table.from_pylist(
        report_rows
    )

    report_file = (
        QUALITY_GOLD
        / "part-00000.parquet"
    )

    # Remove old report files
    for old_file in QUALITY_GOLD.glob(
        "*.parquet"
    ):

        old_file.unlink()

    # IMPORTANT:
    # Use pyarrow.parquet module directly.
    pq.write_table(
        report_table,
        report_file,
    )

    # --------------------------------------------------------
    # Spark-compatible success marker
    # --------------------------------------------------------

    success_file = (
        QUALITY_GOLD
        / "_SUCCESS"
    )

    success_file.touch()

    # --------------------------------------------------------
    # Summary
    # --------------------------------------------------------

    failed_rules = sum(
        1
        for row in report_rows
        if row["failed_records"] > 0
    )

    total_rule_failures = sum(
        row["failed_records"]
        for row in report_rows
    )

    summary = {
        "project": "EpiScope",
        "layer": "Gold",
        "source": "S4_encounters",
        "generated_at": generated_at,
        "silver_path": str(
            SILVER_S4
        ),
        "quality_report_path": str(
            QUALITY_GOLD
        ),
        "total_silver_records": total_count,
        "number_of_quality_rules": len(
            report_rows
        ),
        "rules_with_failures": failed_rules,
        "total_rule_failures": total_rule_failures,
        "rules": report_rows,
    }

    with open(
        SUMMARY_JSON,
        "w",
        encoding="utf-8",
    ) as file:

        json.dump(
            summary,
            file,
            indent=2,
            ensure_ascii=False,
        )

    # --------------------------------------------------------
    # Console output
    # --------------------------------------------------------

    print()
    print("=" * 70)
    print("QUALITY REPORT SUMMARY")
    print("=" * 70)

    print(
        f"Silver records       : "
        f"{total_count:,}"
    )

    print(
        f"Quality rules        : "
        f"{len(report_rows)}"
    )

    print(
        f"Rules with failures  : "
        f"{failed_rules}"
    )

    print(
        f"Total rule failures  : "
        f"{total_rule_failures:,}"
    )

    print()
    print("Rule results:")
    print("-" * 70)

    for row in report_rows:

        print(
            f"{row['rule_id']} | "
            f"{row['rule_name']:<30} | "
            f"{row['failed_records']:>8,} | "
            f"{row['status']}"
        )

    print()
    print(
        f"Quality report: "
        f"{QUALITY_GOLD}"
    )

    print(
        f"Summary JSON  : "
        f"{SUMMARY_JSON}"
    )

    print()
    print(
        "QUALITY REPORT GENERATION COMPLETE"
    )

    print("=" * 70)


if __name__ == "__main__":
    main()