"""
EpiScope - AN-2 Outbreak Detection Evaluation

Evaluates outbreak detections against the generator ground truth.

Ground-truth schema produced by the S4 generator:

    outbreak_id
    district_name
    disease
    start_date
    end_date
    multiplier
    slow_growth
    nationwide

Metrics:
    - Sensitivity
    - False alerts per district-year
    - Median days to detection
    - Disease-level performance
    - Growth-type performance
    - Threshold trade-off curve

IMPORTANT:
    Ground truth is used ONLY in this evaluation script.
    outbreak_detection.py must never read ground truth.
"""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd


# ============================================================
# PROJECT PATHS
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

DETECTION_PATH = (
    PROJECT_ROOT
    / "data"
    / "gold"
    / "analytics"
    / "outbreak_detection"
    / "outbreak_alerts"
)

GROUND_TRUTH_PATH = (
    PROJECT_ROOT
    / "data"
    / "raw"
    / "S4_encounters"
    / "ground_truth_outbreaks"
    / "ground_truth_outbreaks.csv"
)

OUTPUT_DIR = (
    PROJECT_ROOT
    / "data"
    / "gold"
    / "analytics"
    / "outbreak_detection"
    / "evaluation"
)

SUMMARY_PATH = (
    OUTPUT_DIR
    / "evaluation_summary.json"
)


# ============================================================
# CONFIGURATION
# ============================================================

# Alerts are matched to an outbreak if the alert falls:
#
#   1. during the outbreak, OR
#   2. within this many days after the outbreak starts.
#
# This is useful because our surveillance data is weekly.
DETECTION_WINDOW_AFTER_START_DAYS = 21

# Main AN-2 evaluation uses the production consensus alert.
# consensus_alert >= 1 means at least two detectors agreed.
CONSENSUS_ONLY = True


# ============================================================
# UTILITY FUNCTIONS
# ============================================================

def safe_divide(numerator, denominator):
    if denominator == 0:
        return 0.0

    return float(numerator) / float(denominator)


def normalize_district_name(value):
    """
    Normalize known cross-source district naming differences.
    """

    value = str(value).strip()

    mapping = {
        "Cox's_Bazar": "Cox's Bazar",
        "Cox's Bazar": "Cox's Bazar",

        "Nawabganj": "Chapainababganj",

        "Maulvibazar": "Moulvibazar",
    }

    return mapping.get(
        value,
        value,
    )


def normalize_disease_name(value):
    """
    Convert generator disease names to Gold surveillance names.
    """

    value = str(value).strip().lower()

    mapping = {
        "dengue": "Dengue",

        "influenza": "Influenza/ILI",
        "flu": "Influenza/ILI",
        "influenza/ili": "Influenza/ILI",
        "diarrhoeal": "Cholera/Diarrhoeal Disease",
        "cholera/diarrhoeal disease": "Cholera/Diarrhoeal Disease",
        "malaria": "Malaria",

        "respiratory": (
            "Acute Respiratory Infection"
        ),

        "acute respiratory infection": (
            "Acute Respiratory Infection"
        ),

        "a90": "Dengue",
        "a91": "Dengue",
        "a00": "Cholera/Diarrhoeal Disease",
        "a09": "Cholera/Diarrhoeal Disease",
        "j10": "Influenza/ILI",
        "j11": "Influenza/ILI",
        "b50": "Malaria",
        "b51": "Malaria",
        "b52": "Malaria",
        "b53": "Malaria",
        "b54": "Malaria",
        "j06.9": (
            "Acute Respiratory Infection"
        ),
    }

    return mapping.get(
        value,
        value,
    )


def parse_bool_series(series):
    """
    Robust boolean parser.

    Important:
        bool("False") is True in Python,
        so we explicitly parse text values.
    """

    values = (
        series
        .astype(str)
        .str.strip()
        .str.lower()
    )

    return values.isin(
        [
            "true",
            "1",
            "yes",
            "y",
            "t",
        ]
    )


# ============================================================
# LOAD DETECTION OUTPUT
# ============================================================

def load_detection_output():
    if not DETECTION_PATH.exists():
        raise FileNotFoundError(
            f"Detection output not found:\n"
            f"{DETECTION_PATH}"
        )

    df = pd.read_parquet(
        DETECTION_PATH
    )

    required = {
        "district_name",
        "disease",
        "epi_week_start",
        "consensus_alert",
        "ears_c1_z",
        "ears_c2_z",
        "ears_c3_score",
        "cusum_score",
    }

    missing = required - set(
        df.columns
    )

    if missing:
        raise ValueError(
            "Detection output is missing columns:\n"
            + ", ".join(sorted(missing))
        )

    df = df.copy()

    df["epi_week_start"] = pd.to_datetime(
        df["epi_week_start"]
    )

    df["district_name"] = (
        df["district_name"]
        .map(normalize_district_name)
    )

    df["disease"] = (
        df["disease"]
        .map(normalize_disease_name)
    )

    numeric_columns = [
        "consensus_alert",
        "ears_c1_z",
        "ears_c2_z",
        "ears_c3_score",
        "cusum_score",
    ]

    for column in numeric_columns:
        df[column] = pd.to_numeric(
            df[column],
            errors="coerce",
        ).fillna(0)

    df["calendar_year"] = (
        df["epi_week_start"]
        .dt.year
    )

    # --------------------------------------------------------
    # Detector votes
    # --------------------------------------------------------
    #
    # These are the same production thresholds used by
    # outbreak_detection.py.
    #

    df["c1_vote"] = (
        df["ears_c1_z"] >= 2.0
    ).astype(int)

    df["c2_vote"] = (
        df["ears_c2_z"] >= 2.0
    ).astype(int)

    df["c3_vote"] = (
        df["ears_c3_score"] >= 2.0
    ).astype(int)

    df["cusum_vote"] = (
        df["cusum_score"] >= 3.0
    ).astype(int)

    df["detector_votes"] = (
        df["c1_vote"]
        + df["c2_vote"]
        + df["c3_vote"]
        + df["cusum_vote"]
    )

    return df


# ============================================================
# LOAD GROUND TRUTH
# ============================================================

def load_ground_truth():
    if not GROUND_TRUTH_PATH.exists():
        raise FileNotFoundError(
            f"Ground truth not found:\n"
            f"{GROUND_TRUTH_PATH}"
        )

    df = pd.read_csv(
        GROUND_TRUTH_PATH
    )

    # --------------------------------------------------------
    # EXACT schema produced by generate_encounters.py
    # --------------------------------------------------------

    required = {
        "outbreak_id",
        "district_name",
        "disease",
        "start_date",
        "end_date",
        "multiplier",
        "slow_growth",
        "nationwide",
    }

    missing = required - set(
        df.columns
    )

    if missing:
        raise ValueError(
            "Ground-truth file is missing columns:\n"
            + ", ".join(sorted(missing))
        )

    df = df.copy()

    df["outbreak_id"] = pd.to_numeric(
        df["outbreak_id"],
        errors="coerce",
    ).astype("Int64")

    df["district_name"] = (
        df["district_name"]
        .map(normalize_district_name)
    )

    df["disease"] = (
        df["disease"]
        .map(normalize_disease_name)
    )

    df["start_date"] = pd.to_datetime(
        df["start_date"]
    )

    df["end_date"] = pd.to_datetime(
        df["end_date"]
    )

    df["multiplier"] = pd.to_numeric(
        df["multiplier"],
        errors="coerce",
    ).fillna(1.0)

    df["slow_growth"] = parse_bool_series(
        df["slow_growth"]
    )

    df["nationwide"] = parse_bool_series(
        df["nationwide"]
    )

    # --------------------------------------------------------
    # Derive duration.
    #
    # The generator stores start/end dates rather than a
    # duration_days column.
    # --------------------------------------------------------

    df["duration_days"] = (
        df["end_date"]
        - df["start_date"]
    ).dt.days

    # --------------------------------------------------------
    # Derive a readable growth type.
    # --------------------------------------------------------

    df["growth_type"] = (
        df["slow_growth"]
        .map(
            {
                True: "slow-growing",
                False: "rapid/step-change",
            }
        )
    )

    return df


# ============================================================
# MATCH ALERT TO GROUND-TRUTH OUTBREAK
# ============================================================

def alert_matches_outbreak(
    alert,
    outbreak,
):
    """
    Determine whether one alert belongs to one outbreak.
    """

    alert_date = alert[
        "epi_week_start"
    ]

    outbreak_start = outbreak[
        "start_date"
    ]

    outbreak_end = outbreak[
        "end_date"
    ]

    # --------------------------------------------------------
    # Date matching
    # --------------------------------------------------------

    start_window_end = (
        outbreak_start
        + pd.Timedelta(
            days=DETECTION_WINDOW_AFTER_START_DAYS
        )
    )

    during_outbreak = (
        alert_date >= outbreak_start
        and
        alert_date <= outbreak_end
    )

    within_early_window = (
        alert_date >= outbreak_start
        and
        alert_date <= start_window_end
    )

    if not (
        during_outbreak
        or
        within_early_window
    ):
        return False

    # --------------------------------------------------------
    # Disease matching
    # --------------------------------------------------------

    if (
        alert["disease"]
        != outbreak["disease"]
    ):
        return False

    # --------------------------------------------------------
    # District matching
    # --------------------------------------------------------
    #
    # Nationwide outbreak applies to every district.
    #

    if not bool(
        outbreak["nationwide"]
    ):

        if (
            alert["district_name"]
            != outbreak["district_name"]
        ):
            return False

    return True


# ============================================================
# FIND MATCHING OUTBREAKS
# ============================================================

def find_matching_outbreaks(
    alert,
    ground_truth,
):
    matches = []

    for _, outbreak in (
        ground_truth.iterrows()
    ):

        if alert_matches_outbreak(
            alert,
            outbreak,
        ):
            matches.append(
                outbreak
            )

    if not matches:
        return ground_truth.iloc[
            0:0
        ].copy()

    return pd.DataFrame(
        matches
    )


# ============================================================
# OUTBREAK-LEVEL EVALUATION
# ============================================================

def evaluate_outbreaks(
    detections,
    ground_truth,
):
    results = []

    for _, outbreak in (
        ground_truth.iterrows()
    ):

        if bool(
            outbreak["nationwide"]
        ):

            candidates = detections[
                detections["disease"]
                ==
                outbreak["disease"]
            ]

        else:

            candidates = detections[
                (
                    detections[
                        "district_name"
                    ]
                    ==
                    outbreak[
                        "district_name"
                    ]
                )
                &
                (
                    detections[
                        "disease"
                    ]
                    ==
                    outbreak[
                        "disease"
                    ]
                )
            ]

        matching_alerts = []

        for _, alert in (
            candidates.iterrows()
        ):

            if alert_matches_outbreak(
                alert,
                outbreak,
            ):
                matching_alerts.append(
                    alert
                )

        if matching_alerts:

            detection_dates = [
                alert[
                    "epi_week_start"
                ]
                for alert
                in matching_alerts
            ]

            first_detection = min(
                detection_dates
            )

            days_to_detection = (
                first_detection
                - outbreak["start_date"]
            ).days

            detected = True

        else:

            first_detection = pd.NaT

            days_to_detection = None

            detected = False

        results.append(
            {
                "outbreak_id": int(
                    outbreak[
                        "outbreak_id"
                    ]
                ),
                "district_name": (
                    outbreak[
                        "district_name"
                    ]
                ),
                "disease": (
                    outbreak[
                        "disease"
                    ]
                ),
                "start_date": (
                    outbreak[
                        "start_date"
                    ]
                ),
                "end_date": (
                    outbreak[
                        "end_date"
                    ]
                ),
                "duration_days": int(
                    outbreak[
                        "duration_days"
                    ]
                ),
                "multiplier": float(
                    outbreak[
                        "multiplier"
                    ]
                ),
                "growth_type": (
                    outbreak[
                        "growth_type"
                    ]
                ),
                "nationwide": bool(
                    outbreak[
                        "nationwide"
                    ]
                ),
                "detected": detected,
                "first_detection_date": (
                    first_detection
                ),
                "days_to_detection": (
                    days_to_detection
                ),
            }
        )

    return pd.DataFrame(
        results
    )


# ============================================================
# SENSITIVITY
# ============================================================

def calculate_sensitivity(
    outbreak_results,
):
    if outbreak_results.empty:
        return 0.0

    total = len(
        outbreak_results
    )

    detected = int(
        outbreak_results[
            "detected"
        ].sum()
    )

    return safe_divide(
        detected,
        total,
    )


# ============================================================
# FALSE ALERTS
# ============================================================

def identify_false_alerts(
    detections,
    ground_truth,
):
    false_rows = []

    for _, alert in (
        detections.iterrows()
    ):

        matches = find_matching_outbreaks(
            alert,
            ground_truth,
        )

        if matches.empty:

            false_rows.append(
                alert.to_dict()
            )

    if not false_rows:

        return pd.DataFrame(
            columns=detections.columns
        )

    return pd.DataFrame(
        false_rows
    )


def calculate_false_alert_summary(
    false_alerts,
    detections,
):
    district_year_units = (
        detections[
            [
                "district_name",
                "calendar_year",
            ]
        ]
        .drop_duplicates()
        .shape[0]
    )

    false_alert_count = len(
        false_alerts
    )

    rate = safe_divide(
        false_alert_count,
        district_year_units,
    )

    return {
        "false_alerts": int(
            false_alert_count
        ),
        "district_year_units": int(
            district_year_units
        ),
        "false_alerts_per_district_year": (
            float(rate)
        ),
    }


# ============================================================
# DETECTION DELAY
# ============================================================

def calculate_detection_delay(
    outbreak_results,
):
    detected = outbreak_results[
        outbreak_results["detected"]
        &
        outbreak_results[
            "days_to_detection"
        ].notna()
    ].copy()

    if detected.empty:

        return {
            "median_days": None,
            "p25_days": None,
            "p75_days": None,
        }

    delays = (
        detected[
            "days_to_detection"
        ]
        .astype(float)
    )

    return {
        "median_days": float(
            delays.median()
        ),
        "p25_days": float(
            delays.quantile(0.25)
        ),
        "p75_days": float(
            delays.quantile(0.75)
        ),
    }


# ============================================================
# DISEASE SUMMARY
# ============================================================

def disease_summary(
    outbreak_results,
):
    rows = []

    for disease, group in (
        outbreak_results
        .groupby("disease")
    ):

        total = len(group)

        detected = int(
            group["detected"].sum()
        )

        detected_group = group[
            group["detected"]
            &
            group[
                "days_to_detection"
            ].notna()
        ]

        if detected_group.empty:
            median_delay = None
        else:
            median_delay = float(
                detected_group[
                    "days_to_detection"
                ]
                .astype(float)
                .median()
            )

        rows.append(
            {
                "disease": disease,
                "total_outbreaks": int(
                    total
                ),
                "detected_outbreaks": (
                    detected
                ),
                "sensitivity": safe_divide(
                    detected,
                    total,
                ),
                "median_days_to_detection": (
                    median_delay
                ),
            }
        )

    return pd.DataFrame(
        rows
    )


# ============================================================
# GROWTH-TYPE SUMMARY
# ============================================================

def growth_type_summary(
    outbreak_results,
):
    rows = []

    for growth_type, group in (
        outbreak_results
        .groupby("growth_type")
    ):

        total = len(group)

        detected = int(
            group["detected"].sum()
        )

        detected_group = group[
            group["detected"]
            &
            group[
                "days_to_detection"
            ].notna()
        ]

        if detected_group.empty:
            median_delay = None
        else:
            median_delay = float(
                detected_group[
                    "days_to_detection"
                ]
                .astype(float)
                .median()
            )

        rows.append(
            {
                "growth_type": growth_type,
                "total_outbreaks": int(
                    total
                ),
                "detected_outbreaks": (
                    detected
                ),
                "sensitivity": safe_divide(
                    detected,
                    total,
                ),
                "median_days_to_detection": (
                    median_delay
                ),
            }
        )

    return pd.DataFrame(
        rows
    )


# ============================================================
# THRESHOLD TRADE-OFF CURVE
# ============================================================

def threshold_tradeoff(
    detections,
    ground_truth,
):
    """
    Calculate sensitivity / false-alert trade-off
    by requiring different numbers of detector votes.

    1 vote  = any detector
    2 votes = production consensus
    3 votes = stronger agreement
    4 votes = all detectors agree
    """

    rows = []

    district_year_units = (
        detections[
            [
                "district_name",
                "calendar_year",
            ]
        ]
        .drop_duplicates()
        .shape[0]
    )

    for required_votes in [
        1,
        2,
        3,
        4,
    ]:

        selected = detections[
            detections[
                "detector_votes"
            ]
            >= required_votes
        ].copy()

        if selected.empty:

            sensitivity = 0.0

            false_alert_count = 0

        else:

            outbreak_results = (
                evaluate_outbreaks(
                    selected,
                    ground_truth,
                )
            )

            sensitivity = (
                calculate_sensitivity(
                    outbreak_results
                )
            )

            false_alerts = (
                identify_false_alerts(
                    selected,
                    ground_truth,
                )
            )

            false_alert_count = len(
                false_alerts
            )

        false_alert_rate = safe_divide(
            false_alert_count,
            district_year_units,
        )

        rows.append(
            {
                "required_detector_votes": (
                    required_votes
                ),
                "alerts": int(
                    len(selected)
                ),
                "sensitivity": float(
                    sensitivity
                ),
                "false_alerts": int(
                    false_alert_count
                ),
                "false_alerts_per_district_year": (
                    float(
                        false_alert_rate
                    )
                ),
            }
        )

    return pd.DataFrame(
        rows
    )


# ============================================================
# JSON SERIALIZATION
# ============================================================

def json_safe(value):

    if isinstance(
        value,
        pd.Timestamp,
    ):
        return value.isoformat()

    if pd.isna(value):
        return None

    if hasattr(
        value,
        "item",
    ):
        try:
            return value.item()
        except Exception:
            pass

    return value


# ============================================================
# MAIN
# ============================================================

def main():

    print("=" * 70)
    print(
        "EpiScope AN-2 Outbreak Detection Evaluation"
    )
    print("=" * 70)

    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    # ========================================================
    # 1. LOAD DETECTIONS
    # ========================================================

    print()
    print(
        "Loading detection output..."
    )

    detections = load_detection_output()

    print(
        f"Detection rows        : "
        f"{len(detections):,}"
    )

    # ========================================================
    # 2. SELECT PRODUCTION CONSENSUS ALERTS
    # ========================================================

    if CONSENSUS_ONLY:

        evaluation_detections = (
            detections[
                detections[
                    "consensus_alert"
                ]
                >= 1
            ]
            .copy()
        )

    else:

        evaluation_detections = (
            detections.copy()
        )

    print(
        f"Consensus alerts      : "
        f"{len(evaluation_detections):,}"
    )

    # ========================================================
    # 3. LOAD GROUND TRUTH
    # ========================================================

    print()
    print(
        "Loading ground truth..."
    )

    ground_truth = (
        load_ground_truth()
    )

    print(
        f"Ground-truth outbreaks: "
        f"{len(ground_truth):,}"
    )

    print(
        "Ground truth used only for evaluation: YES"
    )

    # ========================================================
    # 4. OUTBREAK-LEVEL EVALUATION
    # ========================================================

    print()
    print(
        "Evaluating outbreaks..."
    )

    outbreak_results = (
        evaluate_outbreaks(
            evaluation_detections,
            ground_truth,
        )
    )

    total_outbreaks = len(
        outbreak_results
    )

    detected_outbreaks = int(
        outbreak_results[
            "detected"
        ].sum()
    )

    missed_outbreaks = (
        total_outbreaks
        - detected_outbreaks
    )

    sensitivity = (
        calculate_sensitivity(
            outbreak_results
        )
    )

    # ========================================================
    # 5. DETECTION DELAY
    # ========================================================

    print(
        "Calculating detection delay..."
    )

    delay_summary = (
        calculate_detection_delay(
            outbreak_results
        )
    )

    # ========================================================
    # 6. FALSE ALERTS
    # ========================================================

    print(
        "Calculating false alerts..."
    )

    false_alerts = (
        identify_false_alerts(
            evaluation_detections,
            ground_truth,
        )
    )

    false_alert_summary = (
        calculate_false_alert_summary(
            false_alerts,
            evaluation_detections,
        )
    )

    # ========================================================
    # 7. DISEASE SUMMARY
    # ========================================================

    disease_results = (
        disease_summary(
            outbreak_results
        )
    )

    # ========================================================
    # 8. GROWTH-TYPE SUMMARY
    # ========================================================

    growth_results = (
        growth_type_summary(
            outbreak_results
        )
    )

    # ========================================================
    # 9. THRESHOLD TRADE-OFF
    # ========================================================

    print(
        "Calculating threshold trade-off..."
    )

    tradeoff_results = (
        threshold_tradeoff(
            detections,
            ground_truth,
        )
    )

    # ========================================================
    # 10. SAVE PARQUET RESULTS
    # ========================================================

    outbreak_results.to_parquet(
        OUTPUT_DIR
        / "outbreak_level_results.parquet",
        index=False,
    )

    false_alerts.to_parquet(
        OUTPUT_DIR
        / "false_alerts.parquet",
        index=False,
    )

    disease_results.to_parquet(
        OUTPUT_DIR
        / "disease_summary.parquet",
        index=False,
    )

    growth_results.to_parquet(
        OUTPUT_DIR
        / "growth_type_summary.parquet",
        index=False,
    )

    tradeoff_results.to_parquet(
        OUTPUT_DIR
        / "threshold_tradeoff.parquet",
        index=False,
    )

    # ========================================================
    # 11. BUILD JSON SUMMARY
    # ========================================================

    summary = {
        "project": "EpiScope",
        "analysis": (
            "AN-2 Outbreak Detection Evaluation"
        ),

        "ground_truth_used": True,

        "ground_truth_file": str(
            GROUND_TRUTH_PATH
        ),

        "detection_output": str(
            DETECTION_PATH
        ),

        "consensus_only": (
            CONSENSUS_ONLY
        ),

        "detection_window_after_start_days": (
            DETECTION_WINDOW_AFTER_START_DAYS
        ),

        "detection_rows": int(
            len(detections)
        ),

        "consensus_alert_rows": int(
            len(evaluation_detections)
        ),

        "ground_truth_outbreaks": int(
            total_outbreaks
        ),

        "detected_outbreaks": int(
            detected_outbreaks
        ),

        "missed_outbreaks": int(
            missed_outbreaks
        ),

        "sensitivity": float(
            sensitivity
        ),

        "false_alerts": (
            false_alert_summary[
                "false_alerts"
            ]
        ),

        "district_year_units": (
            false_alert_summary[
                "district_year_units"
            ]
        ),

        "false_alerts_per_district_year": (
            false_alert_summary[
                "false_alerts_per_district_year"
            ]
        ),

        "median_days_to_detection": (
            delay_summary[
                "median_days"
            ]
        ),

        "p25_days_to_detection": (
            delay_summary[
                "p25_days"
            ]
        ),

        "p75_days_to_detection": (
            delay_summary[
                "p75_days"
            ]
        ),

        "disease_summary": (
            disease_results
            .to_dict(
                orient="records"
            )
        ),

        "growth_type_summary": (
            growth_results
            .to_dict(
                orient="records"
            )
        ),

        "threshold_tradeoff": (
            tradeoff_results
            .to_dict(
                orient="records"
            )
        ),
    }

    # Convert pandas-specific objects.
    summary = json.loads(
        json.dumps(
            summary,
            default=json_safe,
        )
    )

    with open(
        SUMMARY_PATH,
        "w",
        encoding="utf-8",
    ) as file:

        json.dump(
            summary,
            file,
            indent=2,
            ensure_ascii=False,
        )

    # ========================================================
    # 12. CONSOLE REPORT
    # ========================================================

    print()
    print("=" * 70)
    print(
        "AN-2 EVALUATION COMPLETE"
    )
    print("=" * 70)

    print(
        f"Ground-truth outbreaks      : "
        f"{total_outbreaks:,}"
    )

    print(
        f"Detected outbreaks          : "
        f"{detected_outbreaks:,}"
    )

    print(
        f"Missed outbreaks            : "
        f"{missed_outbreaks:,}"
    )

    print(
        f"Sensitivity                 : "
        f"{sensitivity:.4f} "
        f"({sensitivity * 100:.2f}%)"
    )

    print(
        f"False alerts                : "
        f"{false_alert_summary['false_alerts']:,}"
    )

    print(
        f"District-year units         : "
        f"{false_alert_summary['district_year_units']:,}"
    )

    print(
        f"False alerts / district-year: "
        f"{false_alert_summary['false_alerts_per_district_year']:.4f}"
    )

    if (
        delay_summary[
            "median_days"
        ]
        is not None
    ):

        print(
            f"Median days to detection    : "
            f"{delay_summary['median_days']:.2f}"
        )

        print(
            f"P25 days to detection      : "
            f"{delay_summary['p25_days']:.2f}"
        )

        print(
            f"P75 days to detection      : "
            f"{delay_summary['p75_days']:.2f}"
        )

    else:

        print(
            "Median days to detection    : N/A"
        )

    # ========================================================
    # DISEASE RESULTS
    # ========================================================

    print()
    print(
        "Disease summary:"
    )

    if disease_results.empty:

        print(
            "  No disease-level results."
        )

    else:

        for _, row in (
            disease_results.iterrows()
        ):

            print(
                f"  {row['disease']}: "
                f"{int(row['detected_outbreaks'])}/"
                f"{int(row['total_outbreaks'])} "
                f"detected, "
                f"sensitivity="
                f"{row['sensitivity']:.4f}, "
                f"median delay="
                f"{row['median_days_to_detection']}"
            )

    # ========================================================
    # GROWTH RESULTS
    # ========================================================

    print()
    print(
        "Growth-type summary:"
    )

    if growth_results.empty:

        print(
            "  No growth-type results."
        )

    else:

        for _, row in (
            growth_results.iterrows()
        ):

            print(
                f"  {row['growth_type']}: "
                f"{int(row['detected_outbreaks'])}/"
                f"{int(row['total_outbreaks'])} "
                f"detected, "
                f"sensitivity="
                f"{row['sensitivity']:.4f}, "
                f"median delay="
                f"{row['median_days_to_detection']}"
            )

    # ========================================================
    # TRADE-OFF CURVE
    # ========================================================

    print()
    print(
        "Threshold trade-off:"
    )

    print(
        tradeoff_results.to_string(
            index=False
        )
    )

    # ========================================================
    # OUTPUT PATHS
    # ========================================================

    print()
    print(
        f"Outbreak results : "
        f"{OUTPUT_DIR / 'outbreak_level_results.parquet'}"
    )

    print(
        f"False alerts     : "
        f"{OUTPUT_DIR / 'false_alerts.parquet'}"
    )

    print(
        f"Disease summary  : "
        f"{OUTPUT_DIR / 'disease_summary.parquet'}"
    )

    print(
        f"Growth summary   : "
        f"{OUTPUT_DIR / 'growth_type_summary.parquet'}"
    )

    print(
        f"Trade-off curve  : "
        f"{OUTPUT_DIR / 'threshold_tradeoff.parquet'}"
    )

    print(
        f"JSON summary     : "
        f"{SUMMARY_PATH}"
    )

    print("=" * 70)


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    main()