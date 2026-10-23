from pathlib import Path
from datetime import date, timedelta
import math

import numpy as np
import pandas as pd


# ============================================================
# EpiScope - S4 Synthetic Encounter Generator
# ============================================================
#
# Assignment requirements covered:
#
# - 2021-01-01 to 2025-12-31
# - SF=1 => 25,000,000+ encounters
# - SF configurable
# - deterministic random seed
# - all 64 districts
# - diseases:
#       dengue
#       influenza
#       respiratory
# - disease seasonality
# - weather-related structure through seasonal/climate proxy
# - outbreak injection
# - >=30 outbreak events
# - slow-growing outbreaks
# - nationwide respiratory surge
# - report delay 0-21 days
# - weekend reporting effect
# - data-quality defects:
#       ~2% duplicate encounter IDs
#       ~0.5% orphan facility IDs
#       ~1% invalid ages
# - separate ground_truth_outbreaks file
#
# IMPORTANT:
# Detection/forecasting code must NEVER read
# ground_truth_outbreaks.csv.
#
# ============================================================


# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parents[1]

FACILITY_FILE = (
    BASE_DIR
    / "data"
    / "raw"
    / "S5_facilities"
    / "facilities.csv"
)

OUTPUT_DIR = (
    BASE_DIR
    / "data"
    / "raw"
    / "S4_encounters"
)

GROUND_TRUTH_DIR = (
    OUTPUT_DIR
    / "ground_truth_outbreaks"
)


# ------------------------------------------------------------
# Configuration
# ------------------------------------------------------------

SEED = 4345

START_DATE = date(2021, 1, 1)
END_DATE = date(2025, 12, 31)

# ------------------------------------------------------------
# IMPORTANT:
#
# Start with 0.01 for testing.
#
# SF = 0.01  -> ~250,000 encounters
# SF = 0.25  -> ~6,250,000 encounters
# SF = 0.50  -> ~12,500,000 encounters
# SF = 1.00  -> ~25,000,000 encounters
# ------------------------------------------------------------

SCALE_FACTOR = 0.01

BASE_COUNT_SF1 = 25_000_000

CHUNK_SIZE = 100_000


# ------------------------------------------------------------
# Required data-quality rates
# ------------------------------------------------------------

DUPLICATE_RATE = 0.02
ORPHAN_RATE = 0.005
INVALID_AGE_RATE = 0.01


# ============================================================
# Utility functions
# ============================================================

def date_range(start_date, end_date):
    """
    Yield every date between start_date and end_date.
    """

    current = start_date

    while current <= end_date:
        yield current
        current += timedelta(days=1)


def disease_code(disease):
    """
    ICD-10 code mapping.
    """

    if disease == "dengue":
        return "A90"

    if disease == "influenza":
        return "J10"

    if disease == "respiratory":
        return "J06"

    return "Z00"


# ============================================================
# Disease seasonality
# ============================================================

def seasonal_multiplier(
    disease,
    day_of_year
):
    """
    Broad seasonal pattern.

    Dengue:
        stronger during monsoon period.

    Influenza:
        stronger during cooler months.

    Respiratory:
        stronger during cooler/drier period.
    """

    angle = (
        2.0
        * math.pi
        * day_of_year
        / 365.25
    )

    if disease == "dengue":

        # Monsoon-oriented seasonal pattern.
        value = (
            1.0
            + 0.70
            * math.sin(
                angle
                - math.pi / 2
            )
        )

        return max(
            0.45,
            value
        )

    if disease == "influenza":

        value = (
            1.0
            + 0.45
            * math.cos(angle)
        )

        return max(
            0.55,
            value
        )

    if disease == "respiratory":

        value = (
            1.0
            + 0.35
            * math.cos(angle)
        )

        return max(
            0.60,
            value
        )

    return 1.0


# ============================================================
# Weather/climate proxy
# ============================================================

def climate_multiplier(
    disease,
    day_of_year
):
    """
    Synthetic climate-driver component.

    This provides a deterministic climate-related structure
    in the generated health data.

    Later, during the analytics stage, actual S3 NASA POWER
    weather data will be joined using district/date and
    lagged weather variables will be used for forecasting.
    """

    angle = (
        2.0
        * math.pi
        * day_of_year
        / 365.25
    )

    # Temperature-like annual signal.
    temperature_signal = (
        math.sin(angle)
    )

    # Rainfall-like signal, shifted relative to temperature.
    rainfall_signal = (
        math.sin(
            angle
            - math.pi / 3
        )
    )

    if disease == "dengue":

        return (
            1.0
            + 0.20 * rainfall_signal
            + 0.08 * temperature_signal
        )

    if disease == "influenza":

        return (
            1.0
            - 0.10 * temperature_signal
        )

    if disease == "respiratory":

        return (
            1.0
            - 0.08 * temperature_signal
        )

    return 1.0


# ============================================================
# Age generation
# ============================================================

def generate_ages(
    rng,
    n
):
    """
    Generate synthetic ages 0-95.

    Invalid ages are injected later.
    """

    ages = rng.normal(
        loc=34,
        scale=22,
        size=n
    )

    ages = np.clip(
        np.rint(ages),
        0,
        95
    ).astype(np.int16)

    return ages


# ============================================================
# Sex generation
# ============================================================

def generate_sex(
    rng,
    n
):
    return rng.choice(
        ["Male", "Female"],
        size=n,
        p=[
            0.51,
            0.49
        ]
    )


# ============================================================
# Report delay
# ============================================================

def generate_report_delays(
    rng,
    n
):
    """
    Generate delays from 0 to 21 days.

    Mostly short delays with a long tail.
    """

    values = np.arange(
        22
    )

    probabilities = np.array(
        [
            0.15,
            0.14,
            0.12,
            0.10,
            0.09,
            0.08,
            0.07,
            0.06,
            0.05,
            0.04,
            0.025,
            0.020,
            0.015,
            0.010,
            0.008,
            0.006,
            0.005,
            0.004,
            0.003,
            0.002,
            0.002,
            0.002,
        ],
        dtype=float
    )

    probabilities /= (
        probabilities.sum()
    )

    return rng.choice(
        values,
        size=n,
        p=probabilities
    )


# ============================================================
# Weekend reporting effect
# ============================================================

def weekend_reporting_delay(
    current_date,
    rng,
    report_dates
):
    """
    Weekend reporting artifact.

    Saturday/Sunday records have some additional
    reporting delay, representing weekend effects.
    """

    if current_date.weekday() >= 5:

        n = len(report_dates)

        extra_mask = (
            rng.random(n)
            < 0.28
        )

        extra_delay = np.zeros(
            n,
            dtype=int
        )

        extra_delay[
            extra_mask
        ] = rng.integers(
            1,
            4,
            size=extra_mask.sum()
        )

        report_dates = (
            report_dates
            + extra_delay.astype(
                "timedelta64[D]"
            )
        )

    return report_dates


# ============================================================
# Ground-truth outbreak generation
# ============================================================

def generate_outbreaks(
    districts,
    rng
):
    """
    Generate >=30 injected outbreaks.

    Includes:
      - rapid outbreaks
      - moderate outbreaks
      - slow-growing outbreaks
      - nationwide respiratory surge
    """

    minimum_outbreaks = 36

    disease_list = [
        "dengue",
        "influenza",
        "respiratory"
    ]

    total_days = (
        END_DATE - START_DATE
    ).days

    outbreaks = []

    for i in range(
        minimum_outbreaks
    ):

        district = rng.choice(
            districts
        )

        disease = rng.choice(
            disease_list
        )

        start_offset = int(
            rng.integers(
                0,
                max(
                    1,
                    total_days - 70
                )
            )
        )

        outbreak_start = (
            START_DATE
            + timedelta(
                days=start_offset
            )
        )

        duration = int(
            rng.integers(
                14,
                70
            )
        )

        outbreak_end = (
            outbreak_start
            + timedelta(
                days=duration
            )
        )

        if outbreak_end > END_DATE:
            outbreak_end = END_DATE

        magnitude = float(
            rng.uniform(
                1.5,
                5.0
            )
        )

        growth_type = rng.choice(
            [
                "rapid",
                "moderate",
                "slow"
            ],
            p=[
                0.35,
                0.40,
                0.25
            ]
        )

        outbreaks.append(
            {
                "outbreak_id":
                    f"OB{i + 1:04d}",

                "district_name":
                    district,

                "disease":
                    disease,

                "start_date":
                    outbreak_start.isoformat(),

                "end_date":
                    outbreak_end.isoformat(),

                "duration_days":
                    (
                        outbreak_end
                        - outbreak_start
                    ).days + 1,

                "magnitude":
                    magnitude,

                "growth_type":
                    growth_type,
            }
        )

    # --------------------------------------------------------
    # Nationwide novel-respiratory surge.
    # --------------------------------------------------------

    nationwide_start = date(
        2024,
        10,
        1
    )

    nationwide_end = date(
        2025,
        2,
        28
    )

    outbreaks.append(
        {
            "outbreak_id":
                "OB_NATIONWIDE_RESP",

            "district_name":
                "ALL",

            "disease":
                "respiratory",

            "start_date":
                nationwide_start.isoformat(),

            "end_date":
                nationwide_end.isoformat(),

            "duration_days":
                (
                    nationwide_end
                    - nationwide_start
                ).days + 1,

            "magnitude":
                2.5,

            "growth_type":
                "slow",
        }
    )

    return pd.DataFrame(
        outbreaks
    )


# ============================================================
# Build outbreak lookup
# ============================================================

def build_outbreak_lookup(
    outbreak_df
):
    lookup = {}

    for row in outbreak_df.to_dict(
        "records"
    ):

        start = date.fromisoformat(
            row["start_date"]
        )

        end = date.fromisoformat(
            row["end_date"]
        )

        key = (
            row["district_name"],
            row["disease"]
        )

        lookup.setdefault(
            key,
            []
        ).append(
            {
                "start": start,
                "end": end,
                "magnitude":
                    float(
                        row["magnitude"]
                    ),
                "growth_type":
                    row["growth_type"],
                "outbreak_id":
                    row["outbreak_id"],
            }
        )

    return lookup


# ============================================================
# Outbreak multiplier
# ============================================================

def outbreak_multiplier(
    district,
    disease,
    current_date,
    lookup
):
    """
    Calculate outbreak effect for a district/date/disease.
    """

    multiplier = 1.0

    keys = [
        (
            district,
            disease
        ),
        (
            "ALL",
            disease
        ),
    ]

    for key in keys:

        events = lookup.get(
            key,
            []
        )

        for event in events:

            if (
                event["start"]
                <= current_date
                <= event["end"]
            ):

                elapsed = (
                    current_date
                    - event["start"]
                ).days

                duration = (
                    event["end"]
                    - event["start"]
                ).days + 1

                progress = (
                    elapsed
                    / max(
                        1,
                        duration - 1
                    )
                )

                magnitude = (
                    event["magnitude"]
                )

                if (
                    event["growth_type"]
                    == "slow"
                ):

                    # Gradual growth.
                    shape = (
                        0.20
                        + 0.80
                        * progress
                    )

                elif (
                    event["growth_type"]
                    == "moderate"
                ):

                    shape = (
                        0.50
                        + 0.50
                        * math.sin(
                            math.pi
                            * progress
                        )
                    )

                else:

                    shape = 1.0

                multiplier *= (
                    1.0
                    + (
                        magnitude
                        - 1.0
                    )
                    * shape
                )

    return multiplier


# ============================================================
# Inject required data-quality problems
# ============================================================

def inject_data_quality_issues(
    output_dir,
    facility_file,
    seed=4345,
    duplicate_rate=0.02,
    orphan_rate=0.005,
    invalid_age_rate=0.01,
):
    """
    Inject assignment-required synthetic defects.

    Duplicate encounter IDs:
        ~2%

    Orphan facility IDs:
        ~0.5%

    Invalid ages:
        ~1%
    """

    rng = np.random.default_rng(
        seed
    )

    files = sorted(
        output_dir.glob(
            "encounters_*.parquet"
        )
    )

    if not files:
        raise FileNotFoundError(
            "No encounter parquet files found."
        )

    print()
    print("=" * 70)
    print("Injecting required data-quality issues")
    print("=" * 70)

    # --------------------------------------------------------
    # Read chunks.
    # --------------------------------------------------------

    frames = []

    for file in files:

        frames.append(
            pd.read_parquet(file)
        )

    df = pd.concat(
        frames,
        ignore_index=True
    )

    total_rows = len(df)

    print(
        f"Total rows: {total_rows:,}"
    )

    if total_rows < 10:
        raise ValueError(
            "Too few rows for quality injection."
        )

    # --------------------------------------------------------
    # Duplicate encounter IDs
    # --------------------------------------------------------

    duplicate_count = max(
        1,
        int(
            total_rows
            * duplicate_rate
        )
    )

    duplicate_count = min(
        duplicate_count,
        total_rows // 2
    )

    source_indices = rng.choice(
        total_rows,
        size=duplicate_count,
        replace=False
    )

    target_indices = rng.choice(
        total_rows,
        size=duplicate_count,
        replace=False
    )

    df.loc[
        target_indices,
        "encounter_id"
    ] = (
        df.loc[
            source_indices,
            "encounter_id"
        ]
        .to_numpy()
    )

    print(
        "Duplicate encounter IDs:",
        f"{duplicate_count:,}"
    )

    # --------------------------------------------------------
    # Orphan facility IDs
    # --------------------------------------------------------

    orphan_count = max(
        1,
        int(
            total_rows
            * orphan_rate
        )
    )

    orphan_indices = rng.choice(
        total_rows,
        size=orphan_count,
        replace=False
    )

    orphan_ids = np.array(
        [
            f"ORPHAN_FAC_{i:08d}"
            for i in range(
                orphan_count
            )
        ],
        dtype=object
    )

    df.loc[
        orphan_indices,
        "facility_id"
    ] = orphan_ids

    print(
        "Orphan facility IDs:",
        f"{orphan_count:,}"
    )

    # --------------------------------------------------------
    # Invalid ages
    # --------------------------------------------------------

    invalid_age_count = max(
        1,
        int(
            total_rows
            * invalid_age_rate
        )
    )

    invalid_indices = rng.choice(
        total_rows,
        size=invalid_age_count,
        replace=False
    )

    invalid_values = rng.choice(
        [
            -5,
            -1,
            121,
            150,
            999
        ],
        size=invalid_age_count
    )

    df.loc[
        invalid_indices,
        "age_years"
    ] = invalid_values

    print(
        "Invalid ages:",
        f"{invalid_age_count:,}"
    )

    # --------------------------------------------------------
    # Rewrite chunks.
    # --------------------------------------------------------

    start = 0

    for file in files:

        original = pd.read_parquet(
            file
        )

        end = (
            start
            + len(original)
        )

        chunk = df.iloc[
            start:end
        ].copy()

        chunk.to_parquet(
            file,
            index=False
        )

        start = end

    # --------------------------------------------------------
    # Save injection report.
    # --------------------------------------------------------

    report = pd.DataFrame(
        [
            {
                "metric":
                    "total_rows",

                "count":
                    total_rows,
            },
            {
                "metric":
                    "duplicate_encounter_ids_injected",

                "count":
                    duplicate_count,
            },
            {
                "metric":
                    "orphan_facility_ids_injected",

                "count":
                    orphan_count,
            },
            {
                "metric":
                    "invalid_age_rows_injected",

                "count":
                    invalid_age_count,
            },
        ]
    )

    report_file = (
        output_dir
        / "data_quality_injection_report.csv"
    )

    report.to_csv(
        report_file,
        index=False
    )

    print(
        f"Report: {report_file}"
    )

    print(
        "Quality injection complete."
    )


# ============================================================
# Main generator
# ============================================================

def main():

    print()
    print("=" * 70)
    print("EpiScope S4 Synthetic Encounter Generator")
    print("=" * 70)

    print(
        f"Scale factor : {SCALE_FACTOR}"
    )

    print(
        f"Seed         : {SEED}"
    )

    print(
        f"Start date   : {START_DATE}"
    )

    print(
        f"End date     : {END_DATE}"
    )

    # --------------------------------------------------------
    # Check facility registry.
    # --------------------------------------------------------

    if not FACILITY_FILE.exists():

        raise FileNotFoundError(
            "Facility registry not found:\n"
            f"{FACILITY_FILE}"
        )

    facilities = pd.read_csv(
        FACILITY_FILE
    )

    required_columns = {
        "facility_id",
        "district_name",
        "facility_type",
        "bed_capacity",
        "active",
    }

    missing_columns = (
        required_columns
        - set(
            facilities.columns
        )
    )

    if missing_columns:

        raise ValueError(
            "Missing facility columns: "
            f"{sorted(missing_columns)}"
        )

    # --------------------------------------------------------
    # Active facilities only.
    # --------------------------------------------------------

    facilities = facilities[
        facilities["active"] == True
    ].copy()

    facilities[
        "facility_id"
    ] = facilities[
        "facility_id"
    ].astype(str)

    facilities[
        "district_name"
    ] = facilities[
        "district_name"
    ].astype(str)

    districts = sorted(
        facilities[
            "district_name"
        ]
        .dropna()
        .unique()
        .tolist()
    )

    print()
    print(
        f"Active facilities : {len(facilities):,}"
    )

    print(
        f"Districts          : {len(districts)}"
    )

    if len(districts) != 64:

        print(
            "WARNING:"
            " Expected 64 districts."
        )

    # --------------------------------------------------------
    # Create output directories.
    # --------------------------------------------------------

    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    GROUND_TRUTH_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    # --------------------------------------------------------
    # Remove old encounter chunks.
    # --------------------------------------------------------

    for old_file in OUTPUT_DIR.glob(
        "encounters_*.parquet"
    ):

        old_file.unlink()

    old_report = (
        OUTPUT_DIR
        / "data_quality_injection_report.csv"
    )

    if old_report.exists():
        old_report.unlink()

    # --------------------------------------------------------
    # Random generators.
    # --------------------------------------------------------

    rng_outbreak = (
        np.random.default_rng(
            SEED
        )
    )

    rng_counts = (
        np.random.default_rng(
            SEED + 1
        )
    )

    rng_records = (
        np.random.default_rng(
            SEED + 2
        )
    )

    # --------------------------------------------------------
    # Generate ground truth.
    # --------------------------------------------------------

    outbreak_df = generate_outbreaks(
        districts,
        rng_outbreak
    )

    ground_truth_file = (
        GROUND_TRUTH_DIR
        / "ground_truth_outbreaks.csv"
    )

    outbreak_df.to_csv(
        ground_truth_file,
        index=False
    )

    print()
    print(
        "Ground-truth outbreaks:",
        len(outbreak_df)
    )

    # --------------------------------------------------------
    # Outbreak lookup.
    # --------------------------------------------------------

    outbreak_lookup = (
        build_outbreak_lookup(
            outbreak_df
        )
    )

    # --------------------------------------------------------
    # Facility weights.
    #
    # Bed capacity is used as a synthetic proxy for
    # encounter volume.
    # --------------------------------------------------------

    facilities[
        "bed_capacity"
    ] = pd.to_numeric(
        facilities[
            "bed_capacity"
        ],
        errors="coerce"
    ).fillna(20)

    facilities[
        "bed_capacity"
    ] = facilities[
        "bed_capacity"
    ].clip(
        lower=10
    )

    facility_ids = (
        facilities[
            "facility_id"
        ]
        .to_numpy()
    )

    facility_districts = (
        facilities[
            "district_name"
        ]
        .to_numpy()
    )

    facility_weights = (
    	facilities[
       	    "bed_capacity"
   	 ]
    	.astype(float)
    	.to_numpy()
    	.copy()
     )

     facility_weights = (
   	 facility_weights
    		/ facility_weights.sum()
      )

    # --------------------------------------------------------
    # Facility indices by district.
    # --------------------------------------------------------

    facility_index_by_district = {}

    for district in districts:

        indices = np.where(
            facility_districts
            == district
        )[0]

        if len(indices) == 0:
            continue

        weights = (
            facility_weights[
                indices
            ]
        )

        weights /= (
            weights.sum()
        )

        facility_index_by_district[
            district
        ] = (
            indices,
            weights
        )

    # --------------------------------------------------------
    # District capacity weights.
    # --------------------------------------------------------

    district_capacity = {}

    for district in districts:

        indices, _ = (
            facility_index_by_district[
                district
            ]
        )

        capacity = (
            facilities.iloc[
                indices
            ]["bed_capacity"]
            .sum()
        )

        district_capacity[
            district
        ] = float(
            capacity
        )

    district_names = np.array(
        list(
            district_capacity.keys()
        )
    )

    district_probs = np.array(
        list(
            district_capacity.values()
        ),
        dtype=float
    )

    district_probs /= (
        district_probs.sum()
    )

    # --------------------------------------------------------
    # Disease configuration.
    # --------------------------------------------------------

    diseases = [
        "dengue",
        "influenza",
        "respiratory"
    ]

    disease_base_probabilities = np.array(
        [
            0.52,
            0.28,
            0.20
        ],
        dtype=float
    )

    disease_base_probabilities /= (
        disease_base_probabilities.sum()
    )

    # --------------------------------------------------------
    # Target number of encounters.
    # --------------------------------------------------------

    target_count = int(
        BASE_COUNT_SF1
        * SCALE_FACTOR
    )

    print()
    print(
        f"Target encounters: "
        f"{target_count:,}"
    )

    # --------------------------------------------------------
    # Generate daily records.
    # --------------------------------------------------------

    total_days = (
        END_DATE - START_DATE
    ).days + 1

    daily_base = (
        target_count
        / total_days
    )

    generated = 0

    chunk_number = 1

    chunk_rows = []

    # --------------------------------------------------------
    # Encounter ID counter.
    # --------------------------------------------------------

    next_encounter_number = 1

    # --------------------------------------------------------
    # Date loop.
    # --------------------------------------------------------

    for day_number, current_date in enumerate(
        date_range(
            START_DATE,
            END_DATE
        ),
        start=1
    ):

        if generated >= target_count:
            break

        # ----------------------------------------------------
        # Day of year.
        # ----------------------------------------------------

        day_of_year = (
            current_date
            .timetuple()
            .tm_yday
        )

        # ----------------------------------------------------
        # Daily count.
        # ----------------------------------------------------

        remaining = (
            target_count
            - generated
        )

        expected_daily = min(
            remaining,
            max(
                1,
                int(
                    rng_counts.uniform(
                        0.90,
                        1.10
                    )
                    * daily_base
                )
            )
        )

        # ----------------------------------------------------
        # District counts.
        # ----------------------------------------------------

        district_counts = (
            rng_counts.multinomial(
                expected_daily,
                district_probs
            )
        )

        daily_records = []

        # ----------------------------------------------------
        # District loop.
        # ----------------------------------------------------

        for district_idx, district in enumerate(
            district_names
        ):

            n_district = int(
                district_counts[
                    district_idx
                ]
            )

            if n_district <= 0:
                continue

            indices, facility_probs = (
                facility_index_by_district[
                    district
                ]
            )

            selected_facility_indices = (
                rng_records.choice(
                    indices,
                    size=n_district,
                    p=facility_probs
                )
            )

            selected_facility_ids = (
                facility_ids[
                    selected_facility_indices
                ]
            )

            # ------------------------------------------------
            # Disease probabilities.
            # ------------------------------------------------

            local_disease_scores = []

            for disease_idx, disease in enumerate(
                diseases
            ):

                season = (
                    seasonal_multiplier(
                        disease,
                        day_of_year
                    )
                )

                climate = (
                    climate_multiplier(
                        disease,
                        day_of_year
                    )
                )

                outbreak = (
                    outbreak_multiplier(
                        district,
                        disease,
                        current_date,
                        outbreak_lookup
                    )
                )

                score = (
                    disease_base_probabilities[
                        disease_idx
                    ]
                    * season
                    * climate
                    * outbreak
                )

                local_disease_scores.append(
                    score
                )

            local_disease_scores = np.array(
                local_disease_scores,
                dtype=float
            )

            local_disease_scores /= (
                local_disease_scores.sum()
            )

            selected_diseases = (
                rng_records.choice(
                    diseases,
                    size=n_district,
                    p=local_disease_scores
                )
            )

            # ------------------------------------------------
            # Demographics.
            # ------------------------------------------------

            ages = generate_ages(
                rng_records,
                n_district
            )

            sexes = generate_sex(
                rng_records,
                n_district
            )

            # ------------------------------------------------
            # Laboratory confirmation.
            # ------------------------------------------------

            lab_probabilities = np.where(
                selected_diseases
                == "dengue",
                0.72,
                np.where(
                    selected_diseases
                    == "influenza",
                    0.62,
                    0.48
                )
            )

            lab_confirmed = (
                rng_records.random(
                    n_district
                )
                < lab_probabilities
            )

            # ------------------------------------------------
            # Admission.
            # ------------------------------------------------

            admission_probability = np.where(
                ages >= 65,
                0.25,
                np.where(
                    ages <= 5,
                    0.22,
                    0.08
                )
            )

            admitted = (
                rng_records.random(
                    n_district
                )
                < admission_probability
            )

            # ------------------------------------------------
            # Outcome.
            # ------------------------------------------------

            outcome_random = (
                rng_records.random(
                    n_district
                )
            )

            outcomes = np.where(
                outcome_random < 0.94,
                "Recovered",
                np.where(
                    outcome_random < 0.995,
                    "Ongoing",
                    "Deceased"
                )
            )

            # ------------------------------------------------
            # Reporting delay.
            # ------------------------------------------------

            delays = generate_report_delays(
                rng_records,
                n_district
            )

            report_dates = np.array(
                [
                    current_date
                    + timedelta(
                        days=int(delay)
                    )
                    for delay in delays
                ],
                dtype="datetime64[D]"
            )

            # Weekend reporting effect.
            report_dates = (
                weekend_reporting_delay(
                    current_date,
                    rng_records,
                    report_dates
                )
            )

            # ------------------------------------------------
            # Record creation.
            # ------------------------------------------------

            for i in range(
                n_district
            ):

                encounter_number = (
                    next_encounter_number
                )

                next_encounter_number += 1

                disease = (
                    selected_diseases[i]
                )

                daily_records.append(
                    {
                        "encounter_id":
                            f"ENC{encounter_number:010d}",

                        "patient_id":
                            f"PAT{int(rng_records.integers(1, max(2, target_count // 2))):010d}",

                        "facility_id":
                            selected_facility_ids[i],

                        "district_name":
                            district,

                        "onset_date":
                            current_date.isoformat(),

                        "report_date":
                            pd.Timestamp(
                                report_dates[i]
                            ).strftime(
                                "%Y-%m-%d"
                            ),

                        "age_years":
                            int(
                                ages[i]
                            ),

                        "sex":
                            sexes[i],

                        "icd10_code":
                            disease_code(
                                disease
                            ),

                        "lab_confirmed":
                            bool(
                                lab_confirmed[i]
                            ),

                        "admitted":
                            bool(
                                admitted[i]
                            ),

                        "outcome":
                            outcomes[i],

                        "disease":
                            disease,
                    }
                )

        # ----------------------------------------------------
        # Add daily records to buffer.
        # ----------------------------------------------------

        chunk_rows.extend(
            daily_records
        )

        generated += len(
            daily_records
        )

        # ----------------------------------------------------
        # Write full chunks.
        # ----------------------------------------------------

        while len(chunk_rows) >= CHUNK_SIZE:

            output_rows = (
                chunk_rows[
                    :CHUNK_SIZE
                ]
            )

            chunk_rows = (
                chunk_rows[
                    CHUNK_SIZE:
                ]
            )

            chunk_df = pd.DataFrame(
                output_rows
            )

            output_file = (
                OUTPUT_DIR
                / (
                    f"encounters_"
                    f"{chunk_number:05d}.parquet"
                )
            )

            chunk_df.to_parquet(
                output_file,
                index=False
            )

            print(
                f"Written chunk "
                f"{chunk_number:05d} | "
                f"{len(chunk_df):,} rows | "
                f"total generated "
                f"{generated:,}/"
                f"{target_count:,}"
            )

            chunk_number += 1

        # ----------------------------------------------------
        # Progress every 30 days.
        # ----------------------------------------------------

        if day_number % 30 == 0:

            print(
                f"Progress: "
                f"{current_date} | "
                f"{generated:,}/"
                f"{target_count:,}"
            )

    # --------------------------------------------------------
    # Write remaining rows.
    # --------------------------------------------------------

    if chunk_rows:

        chunk_df = pd.DataFrame(
            chunk_rows
        )

        output_file = (
            OUTPUT_DIR
            / (
                f"encounters_"
                f"{chunk_number:05d}.parquet"
            )
        )

        chunk_df.to_parquet(
            output_file,
            index=False
        )

        print(
            f"Written final chunk "
            f"{chunk_number:05d} | "
            f"{len(chunk_df):,} rows"
        )

    # --------------------------------------------------------
    # Inject data-quality issues.
    # --------------------------------------------------------

    inject_data_quality_issues(
        output_dir=OUTPUT_DIR,
        facility_file=FACILITY_FILE,
        seed=SEED + 100,
        duplicate_rate=DUPLICATE_RATE,
        orphan_rate=ORPHAN_RATE,
        invalid_age_rate=INVALID_AGE_RATE,
    )

    # --------------------------------------------------------
    # Final information.
    # --------------------------------------------------------

    print()
    print("=" * 70)
    print("S4 generation COMPLETE")
    print("=" * 70)

    print(
        f"Generated records : {generated:,}"
    )

    print(
        f"Output directory  : {OUTPUT_DIR}"
    )

    print(
        f"Ground truth      : {ground_truth_file}"
    )

    print()
    print(
        "IMPORTANT:"
    )

    print(
        "Do NOT commit generated S4 data to GitHub."
    )

    print(
        "Do NOT allow detection code to read "
        "ground_truth_outbreaks.csv."
    )


# ============================================================
# Entry point
# ============================================================

if __name__ == "__main__":
    main()