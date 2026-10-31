
"""
EpiScope Serving Layer
======================

Purpose
-------
Create dashboard-safe aggregated JSON files from Gold outputs.

Important privacy rule:
    - No line-level S4 encounter data is exposed.
    - Only aggregated district/disease/week information is exported.
    - No patient_id, encounter_id, facility_id or other direct identifiers.
    - Small-cell suppression is applied to case counts 1-4.
    - Outputs are intended for the D3.js dashboard.

Inputs
------
1. Gold surveillance mart
2. Gold outbreak detection alerts
3. Gold seasonal preparedness calendar
4. Gold feature mart (for selected operational indicators)

Outputs
-------
    data/gold/serving/

    dashboard_summary.json
    district_choropleth.json
    time_series.json
    seasonal_heatmap.json
    early_warning.json
    preparedness_calendar.json
    manifest.json
"""

from __future__ import annotations

import json
import math
import sys
from datetime import date, datetime
from pathlib import Path

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))
from district_names import canonical_district_name


# ============================================================
# PROJECT PATHS
# ============================================================

SURVEILLANCE_PATH = (
    PROJECT_ROOT
    / "data"
    / "gold"
    / "surveillance"
    / "district_disease_epiweek"
)
SURVEILLANCE_SUMMARY_PATH = (
    PROJECT_ROOT
    / "data"
    / "gold"
    / "surveillance"
    / "surveillance_mart_summary.json"
)

DETECTION_PATH = (
    PROJECT_ROOT
    / "data"
    / "gold"
    / "analytics"
    / "outbreak_detection"
    / "outbreak_alerts"
)

ANALYTICS_ROOT = (
    PROJECT_ROOT
    / "data"
    / "gold"
    / "analytics"
)

FORECAST_PATH = (
    ANALYTICS_ROOT
    / "forecast"
    / "forecast_results"
)

RESOURCE_DEMAND_PATH = (
    ANALYTICS_ROOT
    / "resource_demand"
    / "forecast_resource_demand"
)

RESOURCE_ALLOCATION_PATH = (
    ANALYTICS_ROOT
    / "resource_allocation"
    / "optimized_allocation.csv"
)

RESOURCE_BASELINE_PATH = (
    ANALYTICS_ROOT
    / "resource_allocation"
    / "population_proportional_baseline.csv"
)

RESOURCE_COMPARISON_PATH = (
    ANALYTICS_ROOT
    / "resource_allocation"
    / "allocation_comparison.csv"
)

RESOURCE_ALLOCATION_SUMMARY_PATH = (
    ANALYTICS_ROOT
    / "resource_allocation"
    / "allocation_summary.json"
)

DISTRICT_SEASONAL_PATH = (
    ANALYTICS_ROOT
    / "seasonal_preparedness"
    / "district_seasonal_calendar.csv"
)

NATIONAL_SEASONAL_PATH = (
    ANALYTICS_ROOT
    / "seasonal_preparedness"
    / "seasonal_weekly_profile.csv"
)

BOUNDARIES_PATH = (
    PROJECT_ROOT
    / "data"
    / "bronze"
    / "S1_boundaries"
    / "bangladesh_districts.geojson"
)

PREPAREDNESS_PATH = (
    PROJECT_ROOT
    / "data"
    / "gold"
    / "analytics"
    / "seasonal_preparedness"
    / "next_year_preparedness_calendar.csv"
)

OUTPUT_ROOT = (
    PROJECT_ROOT
    / "data"
    / "gold"
    / "serving"
)

FORECAST_OUTPUT = OUTPUT_ROOT / "forecast.json"
RESOURCE_OUTPUT = OUTPUT_ROOT / "resource_allocation.json"
BOUNDARIES_OUTPUT = OUTPUT_ROOT / "district_boundaries.json"


# ============================================================
# OUTPUT FILES
# ============================================================

SUMMARY_OUTPUT = (
    OUTPUT_ROOT
    / "dashboard_summary.json"
)

CHOROPLETH_OUTPUT = (
    OUTPUT_ROOT
    / "district_choropleth.json"
)

TIME_SERIES_OUTPUT = (
    OUTPUT_ROOT
    / "time_series.json"
)

HEATMAP_OUTPUT = (
    OUTPUT_ROOT
    / "seasonal_heatmap.json"
)

EARLY_WARNING_OUTPUT = (
    OUTPUT_ROOT
    / "early_warning.json"
)

PREPAREDNESS_OUTPUT = (
    OUTPUT_ROOT
    / "preparedness_calendar.json"
)

MANIFEST_OUTPUT = (
    OUTPUT_ROOT
    / "manifest.json"
)


# ============================================================
# SETTINGS
# ============================================================

# Assignment requirement:
# suppress counts 1-4.
SMALL_CELL_THRESHOLD = 5

DISEASES = [
    "Dengue",
    "Cholera/Diarrhoeal Disease",
    "Influenza/ILI",
    "Malaria",
]


# ============================================================
# HELPERS
# ============================================================

def ensure_output_directory():
    OUTPUT_ROOT.mkdir(
        parents=True,
        exist_ok=True
    )


def json_safe(value):
    """
    Convert pandas / numpy values into JSON-safe values.
    """

    if pd.isna(value):
        return None

    if isinstance(
        value,
        (
            np.integer,
            int,
        )
    ):
        return int(value)

    if isinstance(
        value,
        (
            np.floating,
            float,
        )
    ):
        if not math.isfinite(float(value)):
            return None

        return float(value)

    if isinstance(
        value,
        (
            np.bool_,
            bool,
        )
    ):
        return bool(value)

    if isinstance(value, (pd.Timestamp, datetime, date)):
        return value.strftime(
            "%Y-%m-%d"
        )

    return value


def write_json(path, payload):

    with open(
        path,
        "w",
        encoding="utf-8"
    ) as f:

        json.dump(
            payload,
            f,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False
        )


def index_records(records, columns, categorical_columns, generated_at):
    """Dictionary-encode repeated text/date dimensions without losing values."""
    dictionaries = {
        column: list(dict.fromkeys(
            record.get(column)
            for record in records
            if record.get(column) is not None
        ))
        for column in categorical_columns
    }
    lookups = {
        column: {value: index for index, value in enumerate(values)}
        for column, values in dictionaries.items()
    }
    encoded = []
    for record in records:
        row = []
        for column in columns:
            value = record.get(column)
            if value is not None and column in lookups:
                value = lookups[column][value]
            row.append(value)
        encoded.append(row)
    return {
        "generated_at": generated_at,
        "encoding": "indexed-records-v1",
        "columns": columns,
        "dictionaries": dictionaries,
        "records": encoded,
    }


def expand_indexed_records(payload):
    columns = payload.get("columns", [])
    dictionaries = payload.get("dictionaries", {})
    records = []
    for values in payload.get("records", []):
        record = {}
        for index, column in enumerate(columns):
            value = values[index] if index < len(values) else None
            if column in dictionaries and value is not None:
                value = dictionaries[column][int(value)]
            record[column] = value
        records.append(record)
    return records


def suppress_count(value):
    """
    Small-cell suppression.

    Counts 1-4 are suppressed.
    Zero remains visible because zero is not a
    small positive cell.
    """

    if pd.isna(value):
        return None

    value = int(value)

    if 1 <= value < SMALL_CELL_THRESHOLD:
        return None

    return value


def suppress_count_series(series):
    return series.apply(
        suppress_count
    )


def weekly_small_cell_flags(surveillance):
    source = surveillance[
        ["district_name", "disease", "epi_week", "case_count"]
    ].copy()
    source["district_name"] = source["district_name"].map(
        canonical_district_name
    )
    source["epi_week"] = pd.to_numeric(source["epi_week"], errors="coerce")
    counts = pd.to_numeric(source["case_count"], errors="coerce")
    source["has_small_cell"] = counts.between(
        1, SMALL_CELL_THRESHOLD - 1
    )
    return source.groupby(
        ["district_name", "disease", "epi_week"],
        as_index=False,
        dropna=False,
    )["has_small_cell"].any()


def apply_seasonal_suppression(frame, surveillance):
    result = frame.copy()
    result["district_name"] = result["district_name"].map(
        canonical_district_name
    )
    result["epi_week"] = pd.to_numeric(result["epi_week"], errors="coerce")
    result = result.merge(
        weekly_small_cell_flags(surveillance),
        on=["district_name", "disease", "epi_week"],
        how="left",
        validate="many_to_one",
    )
    result["has_small_cell"] = result["has_small_cell"].fillna(False)
    return result


def data_status_series(series):
    numeric = pd.to_numeric(series, errors="coerce")
    return pd.Series(
        np.select(
            [numeric.isna(), numeric.between(1, SMALL_CELL_THRESHOLD - 1)],
            ["MISSING", "SUPPRESSED"],
            default="AVAILABLE",
        ),
        index=series.index,
    )


def suppressed_derived_metrics(frame, source_count="case_count"):
    counts = pd.to_numeric(frame[source_count], errors="coerce")
    suppressed = counts.between(1, SMALL_CELL_THRESHOLD - 1)
    for column in ("incidence_per_100k",):
        if column in frame:
            frame.loc[suppressed, column] = np.nan
    return suppressed


def latest_week_status(latest_week):
    if latest_week is None or pd.isna(latest_week):
        return "UNKNOWN", None
    week_start = pd.Timestamp(latest_week).normalize()
    week_end = week_start + pd.Timedelta(days=6)
    cutoff = None
    if SURVEILLANCE_SUMMARY_PATH.exists():
        with open(SURVEILLANCE_SUMMARY_PATH, encoding="utf-8") as source:
            metadata = json.load(source)
        cutoff_value = metadata.get("data_cutoff_date") or metadata.get("epi_week_end")
        if cutoff_value:
            cutoff = pd.to_datetime(cutoff_value, errors="coerce")
    if cutoff is not None and not pd.isna(cutoff) and week_end > cutoff:
        return (
            "PROVISIONAL",
            "The latest epi-week extends beyond the available source-data cutoff; "
            "counts may change as delayed reports arrive.",
        )
    if cutoff is None or pd.isna(cutoff):
        return (
            "POSSIBLY_INCOMPLETE",
            "Completeness metadata is unavailable; the latest week may be "
            "incomplete because of reporting delays.",
        )
    return "OBSERVED", None


def check_path(path, description):

    if not path.exists():
        raise FileNotFoundError(
            f"{description} not found:\n"
            f"{path}"
        )


# ============================================================
# LOAD SURVEILLANCE
# ============================================================

def load_surveillance():

    print(
        "Loading Gold surveillance mart..."
    )

    check_path(
        SURVEILLANCE_PATH,
        "Gold surveillance mart"
    )

    df = pd.read_parquet(
        SURVEILLANCE_PATH
    )

    required = [
        "district_name",
        "disease",
        "epi_week",
        "epi_week_start",
        "case_count",
        "incidence_per_100k",
    ]

    missing = [
        column
        for column in required
        if column not in df.columns
    ]

    if missing:
        raise ValueError(
            "Surveillance mart missing columns:\n"
            + "\n".join(missing)
        )

    df["epi_week"] = pd.to_numeric(
        df["epi_week"],
        errors="coerce"
    )

    df["epi_week_start"] = pd.to_datetime(
        df["epi_week_start"],
        errors="coerce"
    )

    df["case_count"] = pd.to_numeric(
        df["case_count"],
        errors="coerce"
    )

    df["incidence_per_100k"] = pd.to_numeric(
        df["incidence_per_100k"],
        errors="coerce"
    )

    df = df[
        df["disease"].isin(DISEASES)
    ].copy()

    df = df[
        df["epi_week"].notna()
    ].copy()

    df["epi_week"] = (
        df["epi_week"]
        .astype(int)
    )

    return df


# ============================================================
# LOAD DETECTION ALERTS
# ============================================================

def load_detection():

    print(
        "Loading outbreak detection alerts..."
    )

    check_path(
        DETECTION_PATH,
        "Outbreak detection output"
    )

    df = pd.read_parquet(
        DETECTION_PATH
    )

    print(
        f"Detection columns    : "
        f"{len(df.columns)}"
    )

    # Flexible column handling because the detector
    # may have slightly different naming.

    if "district_name" not in df.columns:
        raise ValueError(
            "Detection output must contain district_name."
        )

    if "disease" not in df.columns:
        raise ValueError(
            "Detection output must contain disease."
        )

    required = {
        "epi_week",
        "epi_week_start",
        "case_count",
        "alert_level",
    }
    missing = required - set(df.columns)
    if missing:
        raise ValueError(
            "Detection output is missing required columns: "
            + ", ".join(sorted(missing))
        )

    df["epi_week"] = pd.to_numeric(
        df["epi_week"], errors="coerce"
    )
    df["epi_week_start"] = pd.to_datetime(
        df["epi_week_start"], errors="coerce"
    )

    return df


def attach_detection(surveillance, detection):
    keys = [
        "district_name",
        "disease",
        "epi_week",
        "epi_week_start",
    ]
    signal_columns = [
        "alert_level",
        "consensus_alert",
        "detector_count",
        "ears_c1_alert",
        "ears_c2_alert",
        "cusum_alert",
        "ears_c1_mean",
        "ears_c1_std",
        "ears_c1_z",
        "ears_c2_mean",
        "ears_c2_std",
        "ears_c2_z",
        "ears_c3_score",
        "cusum_mean",
        "cusum_std",
        "cusum_score",
    ]
    selected = detection[
        keys + [column for column in signal_columns if column in detection]
    ].copy()
    return surveillance.merge(
        selected,
        on=keys,
        how="left",
        validate="one_to_one",
    )


def build_weekly_alert_lookup(detection):
    severity = {"NONE": 0, "LOW": 1, "MEDIUM": 2, "HIGH": 3}
    level_by_severity = {value: key for key, value in severity.items()}
    alerts = detection[
        ["district_name", "disease", "epi_week", "alert_level"]
    ].copy()
    alerts["epi_week"] = pd.to_numeric(alerts["epi_week"], errors="coerce")
    alerts["severity"] = alerts["alert_level"].map(severity).fillna(0)
    weekly = alerts.groupby(
        ["district_name", "disease", "epi_week"], as_index=False
    )["severity"].max()
    return {
        (row.district_name, row.disease, int(row.epi_week)):
            level_by_severity[int(row.severity)]
        for row in weekly.itertuples(index=False)
    }


# ============================================================
# 1. DISTRICT CHOROPLETH
# ============================================================

def build_choropleth(df):

    print(
        "Building district choropleth..."
    )

    latest_date = df["epi_week_start"].max()
    district = df[df["epi_week_start"] == latest_date].copy()
    district["data_status"] = data_status_series(district["case_count"])
    district["suppressed_cases"] = suppress_count_series(district["case_count"])
    suppressed_derived_metrics(district)
    week_status, _ = latest_week_status(latest_date)

    records = []

    for _, row in district.iterrows():

        records.append(
            {
                "district_name": row[
                    "district_name"
                ],
                "disease": row["disease"],
                "epi_week_start": json_safe(
                    row["epi_week_start"]
                ),
                "week_status": week_status,
                "case_count": json_safe(
                    row["suppressed_cases"]
                ),
                "incidence_per_100k": json_safe(
                    row["incidence_per_100k"]
                ),
                "alert_level": (
                    "N/A"
                    if row["data_status"] != "AVAILABLE"
                    else json_safe(row.get("alert_level")) or "N/A"
                ),
                "data_status": row[
                    "data_status"
                ],
            }
        )

    return {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "latest_epi_week_start": json_safe(latest_date),
        "suppression_threshold": SMALL_CELL_THRESHOLD,
        "records": records,
    }


# ============================================================
# 2. TIME SERIES
# ============================================================

def build_time_series(df):

    print(
        "Building time-series data..."
    )

    weekly = df.copy()
    weekly["data_status"] = data_status_series(weekly["case_count"])
    weekly["case_count"] = suppress_count_series(weekly["case_count"])
    suppressed_derived_metrics(weekly)
    latest_date = weekly["epi_week_start"].max()
    week_status, provisional_reason = latest_week_status(latest_date)
    weekly["week_status"] = np.where(
        weekly["epi_week_start"] == latest_date,
        week_status,
        "OBSERVED",
    )

    records = []

    for _, row in weekly.iterrows():

        records.append(
            {
                "district_name": row["district_name"],
                "disease": row["disease"],
                "epi_week_start": json_safe(row["epi_week_start"]),
                "week_status": row["week_status"],
                "data_status": row["data_status"],
                "case_count": json_safe(row["case_count"]),
                "incidence_per_100k": json_safe(row["incidence_per_100k"]),
                "alert_level": (
                    "N/A"
                    if row["data_status"] != "AVAILABLE"
                    else json_safe(row.get("alert_level")) or "N/A"
                ),
            }
        )

    return index_records(
        records=records,
        columns=[
            "district_name",
            "disease",
            "epi_week_start",
            "case_count",
            "incidence_per_100k",
            "alert_level",
            "week_status",
            "data_status",
        ],
        categorical_columns=[
            "district_name", "disease", "epi_week_start", "alert_level",
            "week_status", "data_status",
        ],
        generated_at=datetime.utcnow().isoformat() + "Z",
    ) | {
        "suppression_threshold": SMALL_CELL_THRESHOLD,
        "latest_epi_week_start": json_safe(latest_date),
        "latest_week_status": week_status,
        "provisional_reason": provisional_reason,
    }


# ============================================================
# 3. SEASONAL HEATMAP
# ============================================================

def build_seasonal_heatmap(df, detection, surveillance):

    print(
        "Building seasonal heatmap..."
    )

    temp = apply_seasonal_suppression(df, surveillance)
    alert_lookup = build_weekly_alert_lookup(detection)

    temp["epi_week"] = temp["epi_week"].astype(int)
    temp.loc[
        temp["has_small_cell"],
        ["seasonal_index", "preparedness_level"],
    ] = None

    records = []

    for _, row in temp.iterrows():

        records.append(
            {
                "district_name": row.get("district_name"),
                "disease": row[
                    "disease"
                ],
                "epi_week": json_safe(
                    row["epi_week"]
                ),
                "incidence_per_100k": json_safe(
                    row.get("mean_incidence", row.get("incidence_per_100k"))
                ),
                "seasonal_index": json_safe(row.get("seasonal_index")),
                "preparedness_level": row.get("preparedness_level"),
                "data_status": (
                    "SUPPRESSED" if row["has_small_cell"] else "AVAILABLE"
                ),
                "alert_level": alert_lookup.get(
                    (row.get("district_name"), row["disease"], int(row["epi_week"])),
                    "N/A",
                ) if not row["has_small_cell"] else "N/A",
                "alert_basis": "Highest observed level for this district, disease, and epi-week",
            }
        )

    return index_records(
        records=records,
        columns=[
            "district_name", "disease", "epi_week", "seasonal_index",
            "preparedness_level", "alert_level", "data_status",
        ],
        categorical_columns=[
            "district_name", "disease", "epi_week",
            "preparedness_level", "alert_level", "data_status",
        ],
        generated_at=datetime.utcnow().isoformat() + "Z",
    )


# ============================================================
# 4. EARLY WARNING
# ============================================================

def build_early_warning(detection):

    print(
        "Building early-warning table..."
    )

    df = detection.copy()

    if "alert_level" not in df.columns:
        raise ValueError("Detection output has no alert_level column.")

    keep = [
        "district_name",
        "disease",
        "epi_week",
        "epi_week_start",
        "division_name",
        "case_count",
        "incidence_per_100k",
        "alert_level",
        "consensus_alert",
        "detector_count",
        "ears_c1_alert",
        "ears_c2_alert",
        "ears_c3_alert",
        "cusum_alert",
        "ears_c1_mean",
        "ears_c1_std",
        "ears_c1_z",
        "ears_c2_mean",
        "ears_c2_std",
        "ears_c2_z",
        "ears_c3_score",
        "cusum_mean",
        "cusum_std",
        "cusum_score",
    ]

    existing = [
        column
        for column in keep
        if column in df.columns
    ]

    temp = df[
        existing
    ].copy()

    temp["alert_level"] = temp["alert_level"].astype(str).str.upper()
    temp["data_status"] = data_status_series(temp["case_count"])
    temp = temp[temp["data_status"] == "AVAILABLE"].copy()
    temp = temp[temp["alert_level"].isin(["HIGH", "MEDIUM", "LOW"])].copy()

    records = []

    for _, row in temp.iterrows():

        record = {
            column: json_safe(row[column])
            for column in existing
        }
        method_flags = [
            ("EARS C1", row.get("ears_c1_alert")),
            ("EARS C2", row.get("ears_c2_alert")),
            ("EARS C3", row.get("ears_c3_alert")),
            ("CUSUM", row.get("cusum_alert")),
        ]
        record["detection_method"] = ", ".join(
            name for name, active in method_flags
            if active is not None and bool(active)
        ) or "Consensus"
        record["alert"] = record["alert_level"]

        records.append(
            record
        )

    columns = [
        column for column in existing if column != "alert"
    ] + ["detection_method", "data_status"]
    return index_records(
        records=records,
        columns=columns,
        categorical_columns=[
            "district_name", "disease", "epi_week", "epi_week_start",
            "division_name", "alert_level", "detection_method", "data_status",
        ],
        generated_at=datetime.utcnow().isoformat() + "Z",
    )


# ============================================================
# 5. PREPAREDNESS CALENDAR
# ============================================================

def build_preparedness(detection, surveillance):

    print(
        "Building preparedness calendar..."
    )

    if not PREPAREDNESS_PATH.exists() or not DISTRICT_SEASONAL_PATH.exists():

        return {
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "records": [],
            "note": (
                "AN-9 national or district preparedness output was not found."
            ),
        }

    national = pd.read_csv(
        PREPAREDNESS_PATH
    )
    district = pd.read_csv(DISTRICT_SEASONAL_PATH)
    district = apply_seasonal_suppression(district, surveillance)
    national["epi_week"] = pd.to_numeric(national["epi_week"], errors="coerce")
    enrich = national[
        [
            "calendar_year",
            "disease",
            "epi_week",
            "representative_month",
            "representative_month_name",
            "seasonal_index",
            "preparedness_level",
            "recommended_action",
        ]
    ].rename(
        columns={
            "seasonal_index": "national_seasonal_index",
            "preparedness_level": "national_preparedness_level",
        }
    )
    df = district.merge(
        enrich,
        on=["disease", "epi_week"],
        how="inner",
        validate="many_to_one",
    )
    national_flags = (
        weekly_small_cell_flags(surveillance)
        .groupby(["disease", "epi_week"], as_index=False)["has_small_cell"]
        .any()
        .rename(columns={"has_small_cell": "national_has_small_cell"})
    )
    df = df.merge(
        national_flags,
        on=["disease", "epi_week"],
        how="left",
        validate="many_to_one",
    )
    df["national_has_small_cell"] = df[
        "national_has_small_cell"
    ].fillna(False)
    df["data_status"] = np.where(
        df["has_small_cell"] | df["national_has_small_cell"],
        "SUPPRESSED",
        "AVAILABLE",
    )
    suppressed = df["data_status"] == "SUPPRESSED"
    df.loc[
        suppressed,
        [
            "seasonal_index",
            "preparedness_level",
            "national_seasonal_index",
            "national_preparedness_level",
            "recommended_action",
        ],
    ] = None
    alert_lookup = build_weekly_alert_lookup(detection)
    df["alert_level"] = [
        (
            "N/A"
            if row.data_status == "SUPPRESSED"
            else alert_lookup.get(
                (row.district_name, row.disease, int(row.epi_week)), "N/A"
            )
        )
        for row in df[
            ["district_name", "disease", "epi_week", "data_status"]
        ].itertuples(index=False)
    ]
    df["alert_basis"] = "Highest observed level for this district, disease, and epi-week"

    records = []
    for _, row in df.iterrows():
        records.append({
            "district_name": row["district_name"],
            "disease": row["disease"],
            "epi_week": json_safe(row["epi_week"]),
            "representative_month_name": row.get("representative_month_name_x"),
            "preparedness_level": row.get("preparedness_level"),
            "seasonal_index": json_safe(row.get("seasonal_index")),
            "national_preparedness_level": row.get("national_preparedness_level"),
            "national_seasonal_index": json_safe(row.get("national_seasonal_index")),
            "recommended_action": row.get("recommended_action"),
            "alert_level": row.get("alert_level", "N/A"),
            "data_status": row.get("data_status"),
        })

    return index_records(
        records=records,
        columns=[
            "district_name", "disease", "epi_week", "representative_month_name",
            "preparedness_level", "seasonal_index", "national_preparedness_level",
            "national_seasonal_index", "recommended_action", "alert_level",
            "data_status",
        ],
        categorical_columns=[
            "district_name", "disease", "epi_week", "representative_month_name",
            "preparedness_level", "national_preparedness_level",
            "recommended_action", "alert_level", "data_status",
        ],
        generated_at=datetime.utcnow().isoformat() + "Z",
    )


def build_forecast():
    if not FORECAST_PATH.exists():
        return {
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "records": [],
            "note": "AN-4 forecast output was not found.",
        }

    df = pd.read_parquet(FORECAST_PATH)
    required = {
        "district_name", "disease", "forecast_date",
        "forecast_horizon_weeks", "predicted_cases", "model",
    }
    missing = required - set(df.columns)
    if missing:
        raise ValueError(
            "Forecast output is missing required columns: "
            + ", ".join(sorted(missing))
        )

    records = []
    for _, row in df.iterrows():
        records.append({
            "district_name": row["district_name"],
            "division_name": row.get("division_name"),
            "disease": row["disease"],
            "forecast_origin": json_safe(row.get("forecast_origin")),
            "forecast_date": json_safe(row["forecast_date"]),
            "forecast_horizon_weeks": json_safe(row["forecast_horizon_weeks"]),
            "predicted_cases": json_safe(row["predicted_cases"]),
            "model": row["model"],
            "model_version": row.get("model_version"),
        })
    return {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "records": records,
    }


def build_resources():
    if not RESOURCE_DEMAND_PATH.exists():
        return {
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "records": [],
            "note": "AN-6 resource-demand output was not found.",
        }

    demand = pd.read_parquet(RESOURCE_DEMAND_PATH)
    demand["forecast_horizon_week"] = pd.to_numeric(
        demand["forecast_horizon_week"], errors="coerce"
    )
    required = {
        "district_name", "disease", "forecast_horizon_week",
        "district_bed_capacity", "active_facility_count", "available_beds",
        "occupied_beds", "saline_stock", "test_kit_stock",
        "stock_facility_count", "stock_data_status", "stock_snapshot_date",
        "forecast_cases", "bed_demand_point", "bed_planning_requirement",
        "saline_demand_point",
        "test_kit_demand_point", "bed_gap", "saline_gap", "test_kit_gap",
    }
    missing = required - set(demand.columns)
    if missing:
        raise ValueError(
            "Resource-demand output is missing required columns: "
            + ", ".join(sorted(missing))
        )

    horizon = int(demand["forecast_horizon_week"].max())
    horizons = sorted(
        demand["forecast_horizon_week"].dropna().astype(int).unique().tolist()
    )
    expected_horizons = [1, 2, 3, 4]
    if horizons != expected_horizons:
        raise ValueError(
            "The four-week AN-6 planning window requires horizons "
            f"{expected_horizons}; found {horizons}."
        )
    if demand.duplicated(
        ["district_name", "disease", "forecast_horizon_week"]
    ).any():
        raise ValueError(
            "AN-6 has duplicate district/disease/horizon demand rows."
        )
    horizon_counts = demand.groupby(
        ["district_name", "disease"]
    )["forecast_horizon_week"].nunique()
    expected_group_count = (
        demand["district_name"].nunique() * demand["disease"].nunique()
    )
    if (
        len(horizon_counts) != expected_group_count
        or horizon_counts.ne(4).any()
    ):
        raise ValueError(
            "Every district/disease pair must contain all four forecast "
            "horizons before serving aggregation."
        )
    planning = demand[
        demand["forecast_horizon_week"].between(1, horizon)
    ].copy()
    planning_week_start = (
        json_safe(planning["forecast_week_start"].max())
        if "forecast_week_start" in planning
        else None
    )
    forecast_origin = (
        json_safe(planning["forecast_origin"].max())
        if "forecast_origin" in planning
        else None
    )
    physical_columns = [
        "district_bed_capacity", "active_facility_count", "available_beds",
        "occupied_beds", "saline_stock", "test_kit_stock",
        "stock_facility_count", "stock_data_status", "stock_snapshot_date",
    ]
    physical = planning.groupby("district_name", as_index=False)[physical_columns].max()
    demand_columns = [
        "forecast_cases", "bed_demand_point", "bed_planning_requirement",
        "saline_demand_point", "saline_planning_requirement",
        "test_kit_demand_point", "test_kit_planning_requirement",
        "bed_gap", "saline_gap", "test_kit_gap",
    ]
    by_disease = planning.groupby(
        ["district_name", "disease"], as_index=False
    )[demand_columns].sum()

    comparison = None
    if RESOURCE_COMPARISON_PATH.exists():
        comparison = pd.read_csv(RESOURCE_COMPARISON_PATH)
        if comparison["district_name"].duplicated().any():
            raise ValueError("AN-7 comparison contains duplicate districts.")
        physical = physical.merge(
            comparison,
            on="district_name",
            how="left",
            validate="one_to_one",
        )
    else:
        if RESOURCE_ALLOCATION_PATH.exists():
            physical = physical.merge(
                pd.read_csv(RESOURCE_ALLOCATION_PATH),
                on="district_name",
                how="left",
                validate="one_to_one",
            )
        if RESOURCE_BASELINE_PATH.exists():
            baseline = pd.read_csv(RESOURCE_BASELINE_PATH)
            physical = physical.merge(
                baseline,
                on="district_name",
                how="left",
                validate="one_to_one",
            )

    records = []
    for _, row in physical.iterrows():
        disease_rows = by_disease[by_disease["district_name"] == row["district_name"]]
        details = []
        for _, disease_row in disease_rows.iterrows():
            detail = {"disease": disease_row["disease"]}
            detail.update({
                column: json_safe(disease_row[column])
                for column in demand_columns
            })
            details.append(detail)
        record = {"district_name": row["district_name"]}
        record.update({
            column: json_safe(row.get(column))
            for column in physical_columns
        })
        if comparison is not None:
            for column in comparison.columns:
                if column != "district_name":
                    record[column] = json_safe(row.get(column))
        else:
            for column in [
                "optimized_beds", "optimized_beds_unmet",
                "optimized_saline", "optimized_saline_unmet",
                "optimized_test_kits", "optimized_test_kits_unmet",
                "baseline_beds", "baseline_saline", "baseline_test_kits",
                "baseline_bed_unmet", "baseline_saline_unmet",
                "baseline_test_kit_unmet", "baseline_total_unmet",
                "population_share", "population_total",
            ]:
                if column in physical.columns:
                    record[column] = json_safe(row.get(column))
        total_unmet = row.get("optimized_total_unmet")
        if pd.notna(total_unmet):
            record["allocation_status"] = "GAP" if float(total_unmet) > 0 else "COVERED"
        else:
            record["allocation_status"] = "N/A"
        record["disease_demand"] = details
        record["planning_horizon_weeks"] = horizon
        record["planning_week_start"] = planning_week_start
        record["forecast_origin"] = forecast_origin
        record["demand_aggregation"] = (
            "Cumulative forecast demand for horizons 1 through "
            f"{horizon}; planning requirements include documented buffers."
        )
        records.append(record)

    allocation_summary = {}
    if RESOURCE_ALLOCATION_SUMMARY_PATH.exists():
        with open(RESOURCE_ALLOCATION_SUMMARY_PATH, encoding="utf-8") as source:
            source_summary = json.load(source)
        allocation_summary = {
            key: source_summary.get(key)
            for key in [
                "planning_horizon_weeks",
                "national_budgets",
                "optimized",
                "population_proportional_baseline",
                "unmet_demand_reduction",
                "optimizer",
                "budget_fractions",
                "national_stock_available",
                "allocated",
                "constraint_checks",
            ]
            if key in source_summary
        }

    return {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "planning_horizon_weeks": horizon,
        "planning_week_start": planning_week_start,
        "forecast_origin": forecast_origin,
        "demand_aggregation": (
            "Cumulative forecast demand across distinct forecast weeks "
            f"1-{horizon}."
        ),
        "allocation_summary": allocation_summary,
        "records": records,
    }


def build_boundaries():
    if not BOUNDARIES_PATH.exists():
        return {
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "type": "FeatureCollection",
            "features": [],
            "note": "District boundary source was not found.",
        }
    from shapely.geometry import LinearRing, MultiPolygon, Polygon, mapping
    from shapely.validation import make_valid

    with open(BOUNDARIES_PATH, encoding="utf-8") as source:
        source_data = json.load(source)

    features = []
    for source_feature in source_data.get("features", []):
        rings = source_feature.get("geometry", {}).get("rings", [])
        if not rings:
            continue

        exteriors = []
        interiors = []
        for ring in rings:
            if len(ring) < 4:
                continue
            if LinearRing(ring).is_ccw:
                interiors.append(ring)
            else:
                exteriors.append(ring)

        if not exteriors:
            exteriors, interiors = [rings[0]], rings[1:]

        polygons = []
        exterior_polygons = [Polygon(ring) for ring in exteriors]
        for exterior, shell in zip(exteriors, exterior_polygons):
            holes = [
                ring for ring in interiors
                if shell.contains(Polygon(ring).representative_point())
            ]
            polygon = Polygon(exterior, holes)
            if not polygon.is_valid:
                polygon = make_valid(polygon)
            polygons.append(
                polygon.simplify(0.003, preserve_topology=True)
            )

        geometry = (
            polygons[0]
            if len(polygons) == 1
            else MultiPolygon(polygons)
        )
        attributes = dict(source_feature.get("attributes", {}))
        source_name = attributes.get("adm2_en")
        if source_name:
            attributes["district_name_source"] = source_name
            attributes["district_name"] = canonical_district_name(source_name)
        features.append({
            "type": "Feature",
            "properties": attributes,
            "geometry": mapping(geometry),
        })

    return {
        "type": "FeatureCollection",
        "features": features,
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "source": "data/bronze/S1_boundaries/bangladesh_districts.geojson",
        "simplification_tolerance_degrees": 0.003,
    }


# ============================================================
# 6. DASHBOARD SUMMARY
# ============================================================

def build_summary(
    surveillance,
    choropleth,
    time_series,
    heatmap,
    early_warning,
    preparedness,
    forecast,
    resources,
):

    latest_date = (
        surveillance["epi_week_start"]
        .max()
    )

    suppressed_rows = pd.to_numeric(
        surveillance["case_count"], errors="coerce"
    ).between(1, SMALL_CELL_THRESHOLD - 1)

    # Never expose raw small disease counts.
    disease_summary = {}

    for disease, group in surveillance.groupby("disease"):
        disease_summary[disease] = (
            None
            if pd.to_numeric(group["case_count"], errors="coerce")
            .between(1, SMALL_CELL_THRESHOLD - 1).any()
            else suppress_count(group["case_count"].sum())
        )

    alert_records = (
        expand_indexed_records(early_warning)
        if early_warning.get("encoding") == "indexed-records-v1"
        else early_warning.get("records", [])
    )
    latest_alerts = [
        record for record in alert_records
        if record.get("epi_week_start") == json_safe(latest_date)
    ]

    resource_bed_total = sum(
        float(item.get("bed_planning_requirement") or 0.0)
        for record in resources.get("records", [])
        for item in record.get("disease_demand", [])
        if item.get("bed_planning_requirement") is not None
    )

    summary = {
        "project": "EpiScope",

        "generated_at": (
            datetime.utcnow().isoformat()
            + "Z"
        ),

        "latest_epi_week_start": json_safe(
            latest_date
        ),
        "latest_week_status": latest_week_status(latest_date)[0],
        "provisional_reason": latest_week_status(latest_date)[1],

        "district_count": int(
            surveillance[
                "district_name"
            ].nunique()
        ),

        "disease_count": int(
            surveillance[
                "disease"
            ].nunique()
        ),

        "diseases": sorted(
            surveillance[
                "disease"
            ]
            .dropna()
            .unique()
            .tolist()
        ),

        "total_aggregated_cases": (
            None
            if suppressed_rows.any()
            else suppress_count(surveillance["case_count"].sum())
        ),

        "active_alert_count": sum(
            record.get("alert_level") in {"HIGH", "MEDIUM", "LOW"}
            for record in latest_alerts
        ),

        "high_priority_alert_count": sum(
            record.get("alert_level") == "HIGH"
            for record in latest_alerts
        ),

        "forecast_record_count": len(forecast.get("records", [])),

        "resource_district_count": len(resources.get("records", [])),

        "four_week_bed_demand": json_safe(
            resource_bed_total
        ),
        "four_week_bed_demand_scope": (
            "Cumulative across forecast horizons 1-4; district demand and "
            "national demand use the same buffered planning requirement."
        ),

        "disease_case_summary": disease_summary,

        "dashboard_views": {
            "district_choropleth": (
                "district_choropleth.json"
            ),
            "time_series": (
                "time_series.json"
            ),
            "seasonal_heatmap": (
                "seasonal_heatmap.json"
            ),
            "early_warning": (
                "early_warning.json"
            ),
            "preparedness_calendar": (
                "preparedness_calendar.json"
            ),
        },

        "privacy": {
            "line_level_data_exposed": False,
            "direct_identifiers_exposed": False,
            "small_cell_threshold": SMALL_CELL_THRESHOLD,
            "counts_1_to_4_suppressed": True,
            "incidence_suppressed_with_small_counts": True,
            "count_derived_detection_statistics_suppressed": True,
            "aggregate_totals_suppressed_when_components_are": True,
        },

        "ground_truth_used": False,
    }

    return summary


# ============================================================
# MANIFEST
# ============================================================

def build_manifest():

    files = [
        SUMMARY_OUTPUT,
        CHOROPLETH_OUTPUT,
        TIME_SERIES_OUTPUT,
        HEATMAP_OUTPUT,
        EARLY_WARNING_OUTPUT,
        PREPAREDNESS_OUTPUT,
        FORECAST_OUTPUT,
        RESOURCE_OUTPUT,
        BOUNDARIES_OUTPUT,
        MANIFEST_OUTPUT,
    ]

    entries = []

    for path in files:

        if path.exists():

            entries.append(
                {
                    "file": path.name,
                    "size_bytes": path.stat().st_size,
                }
            )

    total_size = sum(
        item["size_bytes"]
        for item in entries
    )

    return {
        "project": "EpiScope",

        "generated_at": (
            datetime.utcnow().isoformat()
            + "Z"
        ),

        "format": "JSON",

        "browser_data_only": True,

        "line_level_data_exposed": False,

        "direct_identifiers_exposed": False,

        "small_cell_suppression": {
            "enabled": True,
            "threshold": SMALL_CELL_THRESHOLD,
        },

        "file_count": len(entries),

        "total_size_bytes": total_size,

        "total_size_mb": round(
            total_size / (1024 * 1024),
            4
        ),

        "files": entries,

        "sources": {
            "dashboard_summary.json": [
                "data/gold/surveillance/district_disease_epiweek",
                "data/gold/analytics/outbreak_detection/outbreak_alerts",
            ],
            "district_choropleth.json": [
                "data/gold/surveillance/district_disease_epiweek",
                "data/gold/analytics/outbreak_detection/outbreak_alerts",
                "compact latest-week district snapshot; historic weeks use time_series.json",
            ],
            "time_series.json": [
                "data/gold/surveillance/district_disease_epiweek",
                "data/gold/analytics/outbreak_detection/outbreak_alerts",
            ],
            "early_warning.json": [
                "data/gold/analytics/outbreak_detection/outbreak_alerts",
            ],
            "seasonal_heatmap.json": [
                "data/gold/analytics/seasonal_preparedness/district_seasonal_calendar.csv",
            ],
            "preparedness_calendar.json": [
                "data/gold/analytics/seasonal_preparedness/next_year_preparedness_calendar.csv",
                "data/gold/analytics/seasonal_preparedness/district_seasonal_calendar.csv",
            ],
            "forecast.json": [
                "data/gold/analytics/forecast/forecast_results",
            ],
            "resource_allocation.json": [
                "data/gold/analytics/resource_demand/forecast_resource_demand",
                "data/gold/analytics/resource_allocation/optimized_allocation.csv",
                "data/gold/analytics/resource_allocation/allocation_comparison.csv",
                "data/gold/analytics/resource_allocation/population_proportional_baseline.csv",
                "data/gold/analytics/resource_allocation/allocation_summary.json",
            ],
            "district_boundaries.json": [
                "data/bronze/S1_boundaries/bangladesh_districts.geojson",
            ],
        },
    }


def refresh_manifest_file_sizes(manifest):
    for _ in range(4):
        entries = [
            {"file": path.name, "size_bytes": path.stat().st_size}
            for path in sorted(OUTPUT_ROOT.glob("*.json"))
        ]
        manifest["files"] = entries
        manifest["file_count"] = len(entries)
        manifest["total_size_bytes"] = sum(
            entry["size_bytes"] for entry in entries
        )
        manifest["total_size_mb"] = round(
            manifest["total_size_bytes"] / (1024 * 1024), 4
        )
        write_json(MANIFEST_OUTPUT, manifest)


# ============================================================
# MAIN
# ============================================================

def main():

    print("=" * 72)
    print("EpiScope Dashboard Serving Layer")
    print("=" * 72)

    ensure_output_directory()

    # --------------------------------------------------------
    # Load source data
    # --------------------------------------------------------

    surveillance = load_surveillance()

    print(
        f"Surveillance rows    : "
        f"{len(surveillance):,}"
    )

    detection = load_detection()

    print(
        f"Detection rows       : "
        f"{len(detection):,}"
    )

    surveillance = attach_detection(
        surveillance,
        detection,
    )

    # --------------------------------------------------------
    # Build views
    # --------------------------------------------------------

    print()
    print("[1/6] District choropleth...")

    choropleth = build_choropleth(
        surveillance
    )

    write_json(
        CHOROPLETH_OUTPUT,
        choropleth
    )

    print(
        f"  Records             : "
        f"{len(choropleth['records']):,}"
    )

    # --------------------------------------------------------

    print()
    print("[2/6] Time series...")

    time_series = build_time_series(
        surveillance
    )

    write_json(
        TIME_SERIES_OUTPUT,
        time_series
    )

    print(
        f"  Records             : "
        f"{len(time_series['records']):,}"
    )

    # --------------------------------------------------------

    print()
    print("[3/6] Seasonal heatmap...")

    seasonal_profile = (
        pd.read_csv(DISTRICT_SEASONAL_PATH)
        if DISTRICT_SEASONAL_PATH.exists()
        else pd.DataFrame()
    )
    heatmap = build_seasonal_heatmap(
        seasonal_profile,
        detection,
        surveillance,
    )

    write_json(
        HEATMAP_OUTPUT,
        heatmap
    )

    print(
        f"  Records             : "
        f"{len(heatmap['records']):,}"
    )

    # --------------------------------------------------------

    print()
    print("[4/6] Early-warning table...")

    early_warning = build_early_warning(
        detection
    )

    write_json(
        EARLY_WARNING_OUTPUT,
        early_warning
    )

    print(
        f"  Records             : "
        f"{len(early_warning['records']):,}"
    )

    # --------------------------------------------------------

    print()
    print("[5/6] Preparedness calendar...")

    preparedness = build_preparedness(detection, surveillance)

    write_json(
        PREPAREDNESS_OUTPUT,
        preparedness
    )

    print(
        f"  Records             : "
        f"{len(preparedness['records']):,}"
    )

    print()
    print("[6/9] Forecast results...")
    forecast = build_forecast()
    write_json(FORECAST_OUTPUT, forecast)
    print(f"  Records             : {len(forecast['records']):,}")

    print()
    print("[7/9] Resource demand and allocation...")
    resources = build_resources()
    write_json(RESOURCE_OUTPUT, resources)
    print(f"  Districts           : {len(resources['records']):,}")

    print()
    print("[8/9] District boundaries...")
    boundaries = build_boundaries()
    write_json(BOUNDARIES_OUTPUT, boundaries)
    print(f"  Features            : {len(boundaries.get('features', [])):,}")

    # --------------------------------------------------------

    print()
    print("[9/9] Dashboard summary...")

    summary = build_summary(
        surveillance=surveillance,
        choropleth=choropleth,
        time_series=time_series,
        heatmap=heatmap,
        early_warning=early_warning,
        preparedness=preparedness,
        forecast=forecast,
        resources=resources,
    )

    write_json(
        SUMMARY_OUTPUT,
        summary
    )

    # --------------------------------------------------------
    # Manifest
    # --------------------------------------------------------

    manifest = build_manifest()

    write_json(
        MANIFEST_OUTPUT,
        manifest
    )
    refresh_manifest_file_sizes(manifest)

    # --------------------------------------------------------
    # Final verification
    # --------------------------------------------------------

    print()
    print("=" * 72)
    print("SERVING LAYER COMPLETE")
    print("=" * 72)

    print()
    print(
        f"Districts            : "
        f"{summary['district_count']}"
    )

    print(
        f"Diseases             : "
        f"{summary['disease_count']}"
    )

    print(
        f"Latest epi-week      : "
        f"{summary['latest_epi_week_start']}"
    )

    print()
    print("Privacy checks:")

    print(
        "  Line-level exposed : "
        f"{summary['privacy']['line_level_data_exposed']}"
    )

    print(
        "  Direct identifiers : "
        f"{summary['privacy']['direct_identifiers_exposed']}"
    )

    print(
        "  Small-cell <=4     : "
        f"{summary['privacy']['counts_1_to_4_suppressed']}"
    )

    print()
    print(
        f"Total JSON size      : "
        f"{manifest['total_size_mb']:.4f} MB"
    )

    print()
    print("Serving output:")

    print(
        f"  {OUTPUT_ROOT}"
    )

    print()
    print("Files:")

    for entry in manifest["files"]:

        print(
            f"  {entry['file']:<38}"
            f"{entry['size_bytes']:>10,} bytes"
        )

    print()
    print(
        "Ground truth used    : NO"
    )


if __name__ == "__main__":
    main()
