
"""
EpiScope AN-9
Seasonal Preparedness Calendar

Purpose
-------
Build a seasonal preparedness calendar from the Gold surveillance mart.

Ground truth outbreaks are NOT used.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd


# ============================================================
# PATHS
# ============================================================

PROJECT_ROOT = Path(r"E:\EpiScope")

SURVEILLANCE_PATH = (
    PROJECT_ROOT
    / "data"
    / "gold"
    / "surveillance"
    / "district_disease_epiweek"
)

OUTPUT_ROOT = (
    PROJECT_ROOT
    / "data"
    / "gold"
    / "analytics"
    / "seasonal_preparedness"
)

WEEKLY_OUTPUT = OUTPUT_ROOT / "seasonal_weekly_profile.csv"
DISTRICT_OUTPUT = OUTPUT_ROOT / "district_seasonal_calendar.csv"
NATIONAL_OUTPUT = OUTPUT_ROOT / "national_seasonal_calendar.csv"
NEXT_YEAR_OUTPUT = OUTPUT_ROOT / "next_year_preparedness_calendar.csv"
SUMMARY_OUTPUT = OUTPUT_ROOT / "seasonal_preparedness_summary.json"


# ============================================================
# SETTINGS
# ============================================================

DISEASES = [
    "Dengue",
    "Cholera/Diarrhoeal Disease",
    "Influenza/ILI",
    "Malaria",
]

PEAK_THRESHOLD = 1.50
ELEVATED_THRESHOLD = 1.15

MONTH_NAMES = {
    1: "January",
    2: "February",
    3: "March",
    4: "April",
    5: "May",
    6: "June",
    7: "July",
    8: "August",
    9: "September",
    10: "October",
    11: "November",
    12: "December",
}


# ============================================================
# HELPERS
# ============================================================

def ensure_output_directory():
    OUTPUT_ROOT.mkdir(
        parents=True,
        exist_ok=True
    )


def check_input():
    if not SURVEILLANCE_PATH.exists():
        raise FileNotFoundError(
            "Gold surveillance mart not found:\n"
            f"{SURVEILLANCE_PATH}\n\n"
            "Run first:\n"
            "python pipeline\\gold\\build_surveillance_mart.py"
        )


def classify_preparedness(index):
    if pd.isna(index):
        return "NORMAL"

    if index >= PEAK_THRESHOLD:
        return "PEAK"

    if index >= ELEVATED_THRESHOLD:
        return "ELEVATED"

    return "NORMAL"


# ============================================================
# LOAD GOLD SURVEILLANCE MART
# ============================================================

def load_surveillance():

    check_input()

    print("Loading Gold surveillance mart...")

    df = pd.read_parquet(
        SURVEILLANCE_PATH
    )

    required = [
        "district_name",
        "disease",
        "epi_week",
        "epi_week_start",
        "case_count",
        "population_total",
        "incidence_per_100k",
    ]

    missing = [
        column
        for column in required
        if column not in df.columns
    ]

    if missing:
        raise ValueError(
            "Gold surveillance mart is missing columns:\n"
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
    ).fillna(0)

    df["incidence_per_100k"] = pd.to_numeric(
        df["incidence_per_100k"],
        errors="coerce"
    ).fillna(0)

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

    df["year"] = (
        df["epi_week_start"]
        .dt.year
    )

    df["month"] = (
        df["epi_week_start"]
        .dt.month
    )

    return df


# ============================================================
# NATIONAL WEEKLY SEASONAL PROFILE
# ============================================================

def build_national_weekly_profile(df):

    print(
        "Building national weekly seasonal profile..."
    )

    weekly = (
        df
        .groupby(
            [
                "disease",
                "epi_week",
            ],
            as_index=False
        )
        .agg(
            mean_incidence=(
                "incidence_per_100k",
                "mean"
            ),
            median_incidence=(
                "incidence_per_100k",
                "median"
            ),
            mean_cases=(
                "case_count",
                "mean"
            ),
            district_count=(
                "district_name",
                "nunique"
            ),
        )
    )

    overall = (
        weekly
        .groupby(
            "disease",
            as_index=False
        )
        .agg(
            overall_mean_incidence=(
                "mean_incidence",
                "mean"
            ),
            overall_mean_cases=(
                "mean_cases",
                "mean"
            ),
        )
    )

    weekly = weekly.merge(
        overall,
        on="disease",
        how="left"
    )

    weekly["seasonal_index"] = np.where(
        weekly["overall_mean_incidence"] > 0,
        weekly["mean_incidence"]
        / weekly["overall_mean_incidence"],
        0.0
    )

    weekly["preparedness_level"] = (
        weekly["seasonal_index"]
        .apply(classify_preparedness)
    )

    weekly["representative_month"] = (
        (
            (
                weekly["epi_week"] - 1
            )
            * 7
            + 3
        )
        // 30
        + 1
    )

    weekly["representative_month"] = (
        weekly["representative_month"]
        .clip(
            lower=1,
            upper=12
        )
        .astype(int)
    )

    weekly["representative_month_name"] = (
        weekly["representative_month"]
        .map(MONTH_NAMES)
    )

    return weekly


# ============================================================
# DISTRICT SEASONAL PROFILE
# ============================================================

def build_district_calendar(df):

    print(
        "Building district-level seasonal calendar..."
    )

    district_week = (
        df
        .groupby(
            [
                "district_name",
                "disease",
                "epi_week",
            ],
            as_index=False
        )
        .agg(
            mean_incidence=(
                "incidence_per_100k",
                "mean"
            ),
            median_incidence=(
                "incidence_per_100k",
                "median"
            ),
            mean_cases=(
                "case_count",
                "mean"
            ),
        )
    )

    district_overall = (
        district_week
        .groupby(
            [
                "district_name",
                "disease",
            ],
            as_index=False
        )
        .agg(
            overall_incidence=(
                "mean_incidence",
                "mean"
            )
        )
    )

    district_week = district_week.merge(
        district_overall,
        on=[
            "district_name",
            "disease",
        ],
        how="left"
    )

    district_week["seasonal_index"] = np.where(
        district_week["overall_incidence"] > 0,
        district_week["mean_incidence"]
        / district_week["overall_incidence"],
        0.0
    )

    district_week["preparedness_level"] = (
        district_week["seasonal_index"]
        .apply(classify_preparedness)
    )

    district_week["representative_month"] = (
        (
            (
                district_week["epi_week"] - 1
            )
            * 7
            + 3
        )
        // 30
        + 1
    )

    district_week["representative_month"] = (
        district_week["representative_month"]
        .clip(
            lower=1,
            upper=12
        )
        .astype(int)
    )

    district_week["representative_month_name"] = (
        district_week["representative_month"]
        .map(MONTH_NAMES)
    )

    return district_week


# ============================================================
# NATIONAL PREPAREDNESS PERIODS
# ============================================================

def build_national_calendar(weekly):

    print(
        "Creating national preparedness calendar..."
    )

    rows = []

    for disease in DISEASES:

        disease_df = (
            weekly[
                weekly["disease"] == disease
            ]
            .sort_values("epi_week")
            .reset_index(drop=True)
        )

        if disease_df.empty:
            continue

        current_level = None
        period_start = None
        period_values = []
        previous_week = None

        for _, row in disease_df.iterrows():

            week = int(
                row["epi_week"]
            )

            level = row[
                "preparedness_level"
            ]

            starts_new_period = False

            if current_level is None:
                starts_new_period = True

            elif level != current_level:
                starts_new_period = True

            elif (
                previous_week is not None
                and week != previous_week + 1
            ):
                starts_new_period = True

            if starts_new_period:

                if period_values:

                    values = [
                        item["seasonal_index"]
                        for item in period_values
                    ]

                    rows.append(
                        {
                            "disease": disease,
                            "start_epi_week": period_start,
                            "end_epi_week": previous_week,
                            "preparedness_level": current_level,
                            "peak_seasonal_index": max(values),
                            "mean_seasonal_index": sum(values) / len(values),
                            "duration_weeks": (
                                previous_week
                                - period_start
                                + 1
                            ),
                        }
                    )

                period_start = week
                period_values = []
                current_level = level

            period_values.append(
                {
                    "epi_week": week,
                    "seasonal_index": float(
                        row["seasonal_index"]
                    ),
                }
            )

            previous_week = week

        # Close final period.
        if period_values:

            values = [
                item["seasonal_index"]
                for item in period_values
            ]

            rows.append(
                {
                    "disease": disease,
                    "start_epi_week": period_start,
                    "end_epi_week": previous_week,
                    "preparedness_level": current_level,
                    "peak_seasonal_index": max(values),
                    "mean_seasonal_index": sum(values) / len(values),
                    "duration_weeks": (
                        previous_week
                        - period_start
                        + 1
                    ),
                }
            )

    return pd.DataFrame(rows)


# ============================================================
# NEXT-YEAR CALENDAR
# ============================================================

def build_next_year_calendar(
    weekly,
    df
):

    latest_year = int(
        df["year"].max()
    )

    next_year = latest_year + 1

    print(
        f"Preparing calendar for year: {next_year}"
    )

    rows = []

    for disease in DISEASES:

        disease_weekly = (
            weekly[
                weekly["disease"] == disease
            ]
            .sort_values("epi_week")
        )

        for _, row in disease_weekly.iterrows():

            rows.append(
                {
                    "calendar_year": next_year,
                    "disease": disease,
                    "epi_week": int(
                        row["epi_week"]
                    ),
                    "seasonal_index": float(
                        row["seasonal_index"]
                    ),
                    "mean_incidence_per_100k": float(
                        row["mean_incidence"]
                    ),
                    "preparedness_level": row[
                        "preparedness_level"
                    ],
                    "representative_month": int(
                        row["representative_month"]
                    ),
                    "representative_month_name": row[
                        "representative_month_name"
                    ],
                }
            )

    next_year_df = pd.DataFrame(
        rows
    )

    action_map = {
        "PEAK": (
            "Activate peak-season preparedness: "
            "increase surveillance, review resource stock, "
            "and verify response capacity."
        ),
        "ELEVATED": (
            "Increase monitoring and begin resource "
            "readiness checks."
        ),
        "NORMAL": (
            "Maintain routine surveillance and baseline "
            "resource readiness."
        ),
    }

    next_year_df["recommended_action"] = (
        next_year_df["preparedness_level"]
        .map(action_map)
    )

    return next_year_df


# ============================================================
# SUMMARY
# ============================================================

def build_summary(
    df,
    weekly,
    district_calendar,
    national_calendar,
    next_year
):

    latest_year = int(
        df["year"].max()
    )

    preparedness_counts = {}

    for level in [
        "PEAK",
        "ELEVATED",
        "NORMAL",
    ]:

        preparedness_counts[level] = int(
            (
                next_year["preparedness_level"]
                == level
            ).sum()
        )

    summary = {
        "dataset": (
            "EpiScope AN-9 Seasonal Preparedness"
        ),

        "generated_at": (
            datetime.utcnow().isoformat()
            + "Z"
        ),

        "historical_year_min": int(
            df["year"].min()
        ),

        "historical_year_max": latest_year,

        "next_calendar_year": latest_year + 1,

        "district_count": int(
            df["district_name"].nunique()
        ),

        "disease_count": int(
            df["disease"].nunique()
        ),

        "diseases": sorted(
            df["disease"]
            .dropna()
            .unique()
            .tolist()
        ),

        "weekly_profile_rows": int(
            len(weekly)
        ),

        "district_calendar_rows": int(
            len(district_calendar)
        ),

        "national_calendar_periods": int(
            len(national_calendar)
        ),

        "next_year_rows": int(
            len(next_year)
        ),

        "thresholds": {
            "peak_index": PEAK_THRESHOLD,
            "elevated_index": ELEVATED_THRESHOLD,
        },

        "preparedness_counts": preparedness_counts,

        "national_peak_weeks": {},

        "ground_truth_used": False,

        "outputs": {
            "weekly_profile": str(
                WEEKLY_OUTPUT
            ),
            "district_calendar": str(
                DISTRICT_OUTPUT
            ),
            "national_calendar": str(
                NATIONAL_OUTPUT
            ),
            "next_year_calendar": str(
                NEXT_YEAR_OUTPUT
            ),
        },
    }

    for disease in DISEASES:

        disease_df = next_year[
            next_year["disease"] == disease
        ]

        peak = disease_df[
            disease_df["preparedness_level"] == "PEAK"
        ]

        summary[
            "national_peak_weeks"
        ][disease] = (
            peak["epi_week"]
            .astype(int)
            .tolist()
        )

    return summary


# ============================================================
# MAIN
# ============================================================

def main():

    print("=" * 72)
    print("EpiScope AN-9 Seasonal Preparedness Calendar")
    print("=" * 72)

    ensure_output_directory()

    # --------------------------------------------------------
    # 1. Load
    # --------------------------------------------------------

    print()
    print("[1/6] Loading Gold surveillance mart...")

    df = load_surveillance()

    print(
        f"Rows                 : {len(df):,}"
    )

    print(
        f"Districts            : "
        f"{df['district_name'].nunique()}"
    )

    print(
        f"Diseases             : "
        f"{df['disease'].nunique()}"
    )

    print(
        f"Historical years      : "
        f"{df['year'].min()} - {df['year'].max()}"
    )

    # --------------------------------------------------------
    # 2. National weekly
    # --------------------------------------------------------

    print()
    print(
        "[2/6] Building national seasonal profile..."
    )

    weekly = build_national_weekly_profile(
        df
    )

    weekly.to_csv(
        WEEKLY_OUTPUT,
        index=False
    )

    print(
        f"Weekly profile rows   : "
        f"{len(weekly):,}"
    )

    # --------------------------------------------------------
    # 3. District profile
    # --------------------------------------------------------

    print()
    print(
        "[3/6] Building district seasonal profile..."
    )

    district_calendar = build_district_calendar(
        df
    )

    district_calendar.to_csv(
        DISTRICT_OUTPUT,
        index=False
    )

    print(
        f"District calendar rows: "
        f"{len(district_calendar):,}"
    )

    # --------------------------------------------------------
    # 4. National calendar
    # --------------------------------------------------------

    print()
    print(
        "[4/6] Building national preparedness periods..."
    )

    national_calendar = build_national_calendar(
        weekly
    )

    national_calendar.to_csv(
        NATIONAL_OUTPUT,
        index=False
    )

    print(
        f"National periods      : "
        f"{len(national_calendar):,}"
    )

    # --------------------------------------------------------
    # 5. Next year
    # --------------------------------------------------------

    print()
    print(
        "[5/6] Building next-year preparedness calendar..."
    )

    next_year = build_next_year_calendar(
        weekly=weekly,
        df=df,
    )

    next_year.to_csv(
        NEXT_YEAR_OUTPUT,
        index=False
    )

    print(
        f"Next-year rows        : "
        f"{len(next_year):,}"
    )

    # --------------------------------------------------------
    # 6. Summary
    # --------------------------------------------------------

    print()
    print(
        "[6/6] Writing AN-9 summary..."
    )

    summary = build_summary(
        df=df,
        weekly=weekly,
        district_calendar=district_calendar,
        national_calendar=national_calendar,
        next_year=next_year,
    )

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
    # Console output
    # --------------------------------------------------------

    print()
    print("=" * 72)
    print("AN-9 SEASONAL PREPAREDNESS COMPLETE")
    print("=" * 72)

    print()

    print(
        f"Historical period     : "
        f"{summary['historical_year_min']} - "
        f"{summary['historical_year_max']}"
    )

    print(
        f"Next calendar year    : "
        f"{summary['next_calendar_year']}"
    )

    print(
        f"Districts             : "
        f"{summary['district_count']}"
    )

    print(
        f"Diseases              : "
        f"{summary['disease_count']}"
    )

    print()
    print("Next-year preparedness:")

    print(
        f"  PEAK                : "
        f"{summary['preparedness_counts']['PEAK']:,} rows"
    )

    print(
        f"  ELEVATED            : "
        f"{summary['preparedness_counts']['ELEVATED']:,} rows"
    )

    print(
        f"  NORMAL              : "
        f"{summary['preparedness_counts']['NORMAL']:,} rows"
    )

    print()
    print("Peak weeks by disease:")

    for disease in DISEASES:

        weeks = summary[
            "national_peak_weeks"
        ].get(
            disease,
            []
        )

        if weeks:

            print(
                f"  {disease}: "
                + ", ".join(
                    str(week)
                    for week in weeks
                )
            )

        else:

            print(
                f"  {disease}: No peak weeks"
            )

    print()
    print("Outputs:")

    print(
        f"  Weekly profile      : "
        f"{WEEKLY_OUTPUT}"
    )

    print(
        f"  District calendar   : "
        f"{DISTRICT_OUTPUT}"
    )

    print(
        f"  National calendar   : "
        f"{NATIONAL_OUTPUT}"
    )

    print(
        f"  Next-year calendar  : "
        f"{NEXT_YEAR_OUTPUT}"
    )

    print(
        f"  Summary             : "
        f"{SUMMARY_OUTPUT}"
    )

    print()
    print(
        "Ground truth used    : NO"
    )


if __name__ == "__main__":
    main()

