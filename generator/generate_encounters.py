from __future__ import annotations

import argparse
import json
import math
import random
from datetime import timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import yaml


# ============================================================
# EpiScope S4 Synthetic Health Encounter Generator
# ============================================================
#
# Requirements covered:
#
# - 2021-01-01 to 2025-12-31
# - SF configurable
# - deterministic seed
# - >=25M encounters at SF=1
# - all 64 districts from S5
# - disease seasonality
# - over-dispersed Poisson-Gamma / NB-style noise
# - >=30 injected outbreaks
# - nationwide respiratory surge
# - report delay 0-21 days
# - weekend reporting effect
# - 3-week silent facility + backfill artifact
# - ~2% duplicate encounter IDs
# - ~0.5% orphan facility IDs
# - ~1% invalid ages
# - separate ground_truth_outbreaks.csv
# - memory-safe chunk generation
#
# ============================================================


# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parents[1]

CONFIG_FILE = BASE_DIR / "config" / "generator.yaml"

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

GROUND_TRUTH_DIR = OUTPUT_DIR / "ground_truth_outbreaks"

GROUND_TRUTH_FILE = (
    GROUND_TRUTH_DIR
    / "ground_truth_outbreaks.csv"
)

WEATHER_DIR = BASE_DIR / "data" / "raw" / "S3_weather"

WEATHER_TO_FACILITY_DISTRICT = {
    "Barishal": "Barisal",
    "Bogura": "Bogra",
    "Chattogram": "Chittagong",
    "Chapainababganj": "Nawabganj",
    "Cox's_Bazar": "Cox's Bazar",
    "Cumilla": "Comilla",
    "Jashore": "Jessore",
    "Moulvibazar": "Maulvibazar",
}


# ------------------------------------------------------------
# Default configuration
# ------------------------------------------------------------

DEFAULT_SEED = 4345
DEFAULT_SCALE_FACTOR = 0.01

START_DATE = pd.Timestamp("2021-01-01")
END_DATE = pd.Timestamp("2025-12-31")

BASE_COUNT_SF1 = 25_000_000

DUPLICATE_RATE = 0.02
ORPHAN_RATE = 0.005
INVALID_AGE_RATE = 0.01

MAX_REPORT_DELAY_DAYS = 21

MIN_OUTBREAKS = 30

CHUNK_SIZE = 100_000


# ------------------------------------------------------------
# Required encounter fields
# ------------------------------------------------------------

ENCOUNTER_COLUMNS = [
    "encounter_id",
    "patient_id",
    "facility_id",
    "district_name",
    "onset_date",
    "report_date",
    "age_years",
    "sex",
    "icd10_code",
    "disease",
    "lab_confirmed",
    "admitted",
    "outcome",
]


# ------------------------------------------------------------
# Disease definitions
# ------------------------------------------------------------

CORE_DISEASES = [
    "dengue",
    "diarrhoeal",
    "influenza",
    "malaria",
]

BACKGROUND_DISEASES = ["respiratory"]
DISEASES = CORE_DISEASES + BACKGROUND_DISEASES

DISEASE_WEIGHTS = {
    "dengue": 0.45,
    "diarrhoeal": 0.10,
    "influenza": 0.30,
    "malaria": 0.10,
    "respiratory": 0.05,
}

ICD10_CODES = {
    "dengue": ("A90", "A91"),
    "diarrhoeal": ("A00", "A09"),
    "influenza": ("J10", "J11"),
    "malaria": ("B50", "B51", "B52", "B53", "B54"),
    "respiratory": ("J06.9",),
}

MALARIA_ENDEMIC_DISTRICTS = frozenset(
    {"Bandarban", "Khagrachhari", "Rangamati"}
)


# ------------------------------------------------------------
# Utility
# ------------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(
        description="EpiScope S4 synthetic encounter generator"
    )

    parser.add_argument(
        "--scale-factor",
        type=float,
        default=None,
        help="Scale factor, e.g. 0.01, 0.25, 0.5, 1.0",
    )

    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Random seed",
    )

    return parser.parse_args()


def load_config():
    if not CONFIG_FILE.exists():
        return {}

    try:
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)

        return config or {}

    except Exception as exc:
        print(
            f"WARNING: Could not read generator config: {exc}"
        )
        return {}


def get_configuration(args):
    config = load_config()

    configured_diseases = config.get("diseases", CORE_DISEASES)
    configured_background = config.get(
        "background_diseases",
        BACKGROUND_DISEASES,
    )
    if set(configured_diseases) != set(CORE_DISEASES):
        raise ValueError("Generator config disease groups do not match the supported core diseases.")
    if set(configured_background) != set(BACKGROUND_DISEASES):
        raise ValueError("Generator config background diseases do not match the supported background diseases.")

    config_seed = (
        config.get("seed", DEFAULT_SEED)
        if isinstance(config, dict)
        else DEFAULT_SEED
    )

    config_scale = (
        config.get("scale_factor", None)
        if isinstance(config, dict)
        else None
    )

    if config_scale is None and isinstance(config, dict):
        encounters_config = config.get("encounters", {})

        # Some configurations may not contain a direct
        # scale_factor but the root usually does.
        config_scale = config.get(
            "scale_factor",
            DEFAULT_SCALE_FACTOR,
        )

    seed = (
        args.seed
        if args.seed is not None
        else config_seed
    )

    scale_factor = (
        args.scale_factor
        if args.scale_factor is not None
        else (
            config_scale
            if config_scale is not None
            else DEFAULT_SCALE_FACTOR
        )
    )

    if scale_factor <= 0:
        raise ValueError(
            "Scale factor must be greater than 0."
        )

    return int(seed), float(scale_factor)


# ------------------------------------------------------------
# Facility registry
# ------------------------------------------------------------

def load_facilities():
    if not FACILITY_FILE.exists():
        raise FileNotFoundError(
            f"S5 facility file not found:\n{FACILITY_FILE}"
        )

    facilities = pd.read_csv(FACILITY_FILE)

    required = {
        "facility_id",
        "district_name",
        "facility_type",
        "bed_capacity",
        "active",
    }

    missing = required - set(facilities.columns)

    if missing:
        raise ValueError(
            f"S5 facility registry is missing columns: {sorted(missing)}"
        )

    facilities = facilities.copy()

    facilities["district_name"] = (
        facilities["district_name"]
        .astype(str)
        .str.strip()
    )

    facilities["facility_id"] = (
        facilities["facility_id"]
        .astype(str)
        .str.strip()
    )

    facilities["bed_capacity"] = (
        pd.to_numeric(
            facilities["bed_capacity"],
            errors="coerce",
        )
        .fillna(1)
        .clip(lower=1)
    )

    facilities["active"] = (
        facilities["active"]
        .astype(str)
        .str.lower()
        .isin(
            [
                "true",
                "1",
                "yes",
            ]
        )
    )

    facilities = facilities[
        facilities["active"]
    ].copy()

    if facilities.empty:
        raise ValueError(
            "No active facilities found in S5."
        )

    district_count = (
        facilities["district_name"]
        .nunique()
    )

    if district_count != 64:
        raise ValueError(
            f"S5 must contain 64 districts. Found {district_count}."
        )

    return facilities.reset_index(drop=True)


def load_weather_drivers(weather_dates, district_names):
    """Build seeded-generation climate multipliers from cached S3 weather."""
    expected_dates = pd.DatetimeIndex(weather_dates).strftime("%Y%m%d")
    weather_by_district = {}

    for path in sorted(WEATHER_DIR.glob("*.json")):
        district = WEATHER_TO_FACILITY_DISTRICT.get(path.stem, path.stem)
        if district not in district_names:
            continue
        if district in weather_by_district:
            raise ValueError(f"Multiple S3 weather files map to {district}.")

        with path.open("r", encoding="utf-8") as file:
            payload = json.load(file)
        parameters = payload.get("properties", {}).get("parameter", {})
        precipitation = parameters.get("PRECTOTCORR", {})
        temperature = parameters.get("T2M", {})
        rain = np.asarray(
            [precipitation.get(date) for date in expected_dates],
            dtype=np.float64,
        )
        temperature = np.asarray(
            [temperature.get(date) for date in expected_dates],
            dtype=np.float64,
        )
        if not np.isfinite(rain).all() or not np.isfinite(temperature).all():
            raise ValueError(f"S3 weather is incomplete for {district}.")

        rain_series = pd.Series(rain)
        temperature_series = pd.Series(temperature)
        rain_lag_4_8w = rain_series.shift(28).rolling(28, min_periods=28).mean().to_numpy()
        temperature_lag_4_8w = temperature_series.shift(28).rolling(28, min_periods=28).mean().to_numpy()
        rainfall_lag_1_7d = rain_series.shift(1).rolling(7, min_periods=7).sum().to_numpy()

        rain_median = float(np.nanmedian(rain_lag_4_8w))
        rain_p90 = float(np.nanquantile(rain_lag_4_8w, 0.90))
        temperature_median = float(np.nanmedian(temperature_lag_4_8w))
        temperature_std = max(float(np.nanstd(temperature_lag_4_8w)), 1.0)
        rain_scale = max(rain_p90 - rain_median, 1.0)
        extreme_rain_p90 = float(np.nanquantile(rainfall_lag_1_7d, 0.90))

        dengue_rain_signal = np.clip(
            (rain_lag_4_8w - rain_median) / rain_scale,
            0.0,
            2.0,
        )
        dengue_temperature_signal = np.clip(
            (temperature_lag_4_8w - temperature_median) / temperature_std,
            -2.0,
            2.0,
        )
        dengue_multiplier = np.clip(
            1.0 + 0.15 * dengue_rain_signal + 0.05 * dengue_temperature_signal,
            0.5,
            2.0,
        )
        diarrhoeal_multiplier = 1.0 + 0.75 * np.clip(
            (rainfall_lag_1_7d - extreme_rain_p90) / max(extreme_rain_p90, 1.0),
            0.0,
            2.0,
        )

        weather_by_district[district] = {
            "dengue_multiplier": np.nan_to_num(dengue_multiplier, nan=1.0),
            "diarrhoeal_multiplier": np.nan_to_num(diarrhoeal_multiplier, nan=1.0),
        }

    missing_districts = set(district_names) - set(weather_by_district)
    if missing_districts:
        raise ValueError(
            "S3 weather is missing for districts: "
            + ", ".join(sorted(missing_districts))
        )

    return weather_by_district


# ------------------------------------------------------------
# Outbreak generation
# ------------------------------------------------------------

def generate_outbreaks(
    facilities,
    seed,
    minimum_count=MIN_OUTBREAKS,
):
    """
    Create deterministic outbreak ground truth.

    The detector/analytics pipeline must NOT read this file.
    """

    rng = np.random.default_rng(seed + 1000)

    districts = sorted(
        facilities["district_name"]
        .unique()
        .tolist()
    )

    outbreak_count = max(
        minimum_count,
        30,
    )

    records = []

    for outbreak_id in range(
        1,
        outbreak_count + 1,
    ):

        district = districts[
            int(
                rng.integers(
                    0,
                    len(districts),
                )
            )
        ]

        disease = DISEASES[
            int(
                rng.integers(
                    0,
                    len(DISEASES),
                )
            )
        ]

        start_offset = int(
            rng.integers(
                0,
                1700,
            )
        )

        start_date = (
            START_DATE
            + pd.Timedelta(
                days=start_offset
            )
        )

        duration = int(
            rng.integers(
                14,
                91,
            )
        )

        end_date = min(
            END_DATE,
            start_date
            + pd.Timedelta(
                days=duration
            ),
        )

        # Different outbreak strengths.
        multiplier = float(
            rng.uniform(
                1.8,
                5.5,
            )
        )

        # Slow-growing outbreak.
        slow_growth = (
            outbreak_id % 7 == 0
        )

        # Some larger outbreaks.
        if outbreak_id % 5 == 0:
            multiplier *= 1.5

        records.append(
            {
                "outbreak_id": outbreak_id,
                "district_name": district,
                "disease": disease,
                "start_date": start_date.date(),
                "end_date": end_date.date(),
                "multiplier": round(
                    multiplier,
                    4,
                ),
                "slow_growth": bool(
                    slow_growth
                ),
                "nationwide": False,
            }
        )

    # --------------------------------------------------------
    # Required nationwide novel-respiratory surge
    # --------------------------------------------------------

    nationwide_start = (
        START_DATE
        + pd.Timedelta(
            days=1500
        )
    )

    nationwide_end = min(
        END_DATE,
        nationwide_start
        + pd.Timedelta(
            days=70
        ),
    )

    records.append(
        {
            "outbreak_id": outbreak_count + 1,
            "district_name": "ALL",
            "disease": "respiratory",
            "start_date": nationwide_start.date(),
            "end_date": nationwide_end.date(),
            "multiplier": 3.8,
            "slow_growth": False,
            "nationwide": True,
        }
    )

    return pd.DataFrame(records)


# ------------------------------------------------------------
# Disease seasonality
# ------------------------------------------------------------

def seasonal_multiplier(
    disease,
    dates,
):
    """
    Deterministic seasonal signal.

    The actual S3 weather data is consumed later by the
    analytics/forecasting pipeline.
    """

    dates = pd.DatetimeIndex(dates)

    day_of_year = (
        dates.dayofyear
        .to_numpy()
    )

    phase = (
        2.0
        * np.pi
        * (
            day_of_year
            / 365.25
        )
    )

    if disease == "dengue":

        # Rainy-season pattern.
        values = (
            1.0
            + 0.80
            * np.maximum(
                0,
                np.sin(
                    phase
                    - 1.0
                ),
            )
        )

    elif disease == "influenza":

        values = (
            1.0
            + 0.45
            * np.maximum(
                0,
                np.cos(
                    phase
                    + 0.5
                ),
            )
        )

    elif disease == "diarrhoeal":
        values = (
            1.0
            + 0.60
            * np.maximum(
                0,
                np.sin(
                    phase
                    - 1.7
                ),
            )
        )

    elif disease == "malaria":
        values = (
            1.0
            + 0.55
            * np.maximum(
                0,
                np.sin(
                    phase
                    - 2.0
                ),
            )
        )

    else:

        values = (
            1.0
            + 0.30
            * np.maximum(
                0,
                np.cos(
                    phase
                    + 0.2
                ),
            )
        )

    return np.asarray(
        values,
        dtype=float,
    )


# ------------------------------------------------------------
# Disease probabilities
# ------------------------------------------------------------

def disease_probabilities(
    dates,
    district_name=None,
    weather_dates=None,
    weather_drivers=None,
):
    base = np.column_stack([
        DISEASE_WEIGHTS[disease] * seasonal_multiplier(disease, dates)
        for disease in DISEASES
    ])

    if district_name in (weather_drivers or {}):
        date_positions = pd.DatetimeIndex(weather_dates).get_indexer(
            pd.DatetimeIndex(dates)
        )
        if (date_positions < 0).any():
            raise ValueError("S4 dates fall outside the S3 weather date range.")
        drivers = weather_drivers[district_name]
        base[:, DISEASES.index("dengue")] *= drivers["dengue_multiplier"][date_positions]
        base[:, DISEASES.index("diarrhoeal")] *= drivers["diarrhoeal_multiplier"][date_positions]

    if district_name in MALARIA_ENDEMIC_DISTRICTS:
        malaria_index = DISEASES.index("malaria")
        base[:, malaria_index] *= 5.0

    base = np.maximum(
        base,
        0.0001,
    )

    base /= base.sum(
        axis=1,
        keepdims=True,
    )

    return base


def build_disease_probability_lookup(weather_dates, district_names, weather_drivers):
    return {
        district: disease_probabilities(
            weather_dates,
            district,
            weather_dates,
            weather_drivers,
        )
        for district in district_names
    }


# ------------------------------------------------------------
# Outbreak multiplier
# ------------------------------------------------------------

def prepare_outbreak_arrays(outbreaks):
    """
    Convert the outbreak table once into NumPy-friendly arrays.

    This preparation is done once per dataset generation, rather than
    repeatedly converting/iterating through pandas rows for every chunk.
    """
    if outbreaks is None or len(outbreaks) == 0:
        return {
            "disease": np.array([], dtype=object),
            "district": np.array([], dtype=object),
            "start": np.array([], dtype="datetime64[ns]"),
            "end": np.array([], dtype="datetime64[ns]"),
            "nationwide": np.array([], dtype=bool),
            "slow_growth": np.array([], dtype=bool),
            "multiplier": np.array([], dtype=np.float64),
        }

    required_columns = {
        "disease",
        "district_name",
        "start_date",
        "end_date",
        "nationwide",
        "slow_growth",
        "multiplier",
    }

    missing = required_columns.difference(outbreaks.columns)
    if missing:
        raise ValueError(
            "Outbreak table is missing columns: "
            + ", ".join(sorted(missing))
        )

    return {
        "disease": outbreaks["disease"].astype(str).to_numpy(dtype=object),
        "district": outbreaks["district_name"].astype(str).to_numpy(dtype=object),
        "start": pd.to_datetime(outbreaks["start_date"]).to_numpy(dtype="datetime64[ns]"),
        "end": pd.to_datetime(outbreaks["end_date"]).to_numpy(dtype="datetime64[ns]"),
        "nationwide": outbreaks["nationwide"].astype(bool).to_numpy(),
        "slow_growth": outbreaks["slow_growth"].astype(bool).to_numpy(),
        "multiplier": (
            pd.to_numeric(outbreaks["multiplier"], errors="coerce")
            .fillna(1.0)
            .to_numpy(dtype=np.float64)
        ),
    }


def outbreak_multipliers(
    dates,
    district_name,
    disease,
    outbreak_data,
):
    """
    Fast outbreak multiplier calculation using pre-converted NumPy arrays.

    Only relevant outbreak rows are selected with boolean masks. No pandas
    itertuples() scan is performed for every generated chunk.
    """
    dates = pd.DatetimeIndex(dates)

    multipliers = np.ones(len(dates), dtype=np.float64)

    if not outbreak_data or len(outbreak_data["disease"]) == 0:
        return multipliers

    disease_array = outbreak_data["disease"]
    district_array = outbreak_data["district"]

    relevant_mask = (
        (disease_array == str(disease))
        & (
            outbreak_data["nationwide"]
            | (district_array == str(district_name))
        )
    )

    relevant_indices = np.flatnonzero(relevant_mask)
    if relevant_indices.size == 0:
        return multipliers

    date_values = dates.to_numpy(dtype="datetime64[ns]")

    for idx in relevant_indices:
        start = outbreak_data["start"][idx]
        end = outbreak_data["end"][idx]
        multiplier = float(outbreak_data["multiplier"][idx])

        mask = (date_values >= start) & (date_values <= end)
        if not np.any(mask):
            continue

        if outbreak_data["slow_growth"][idx]:
            days = (
                date_values[mask] - start
            ).astype("timedelta64[D]").astype(np.float64)

            duration = max(
                1.0,
                float(
                    (end - start).astype("timedelta64[D]").astype(np.int64)
                ),
            )

            growth = 1.0 + (days / duration) * (multiplier - 1.0)
            multipliers[mask] *= growth
        else:
            multipliers[mask] *= multiplier

    multipliers = np.nan_to_num(
        multipliers,
        nan=1.0,
        posinf=10.0,
        neginf=1.0,
    )

    return np.maximum(multipliers, 0.1)


def build_outbreak_lookup(dates, district_names, outbreak_data):
    """
    Precompute outbreak multipliers for every date/district/disease once.

    The lookup is later used during encounter generation, so outbreak
    multipliers affect the actual disease allocation/encounter volume.
    """
    dates = pd.DatetimeIndex(dates)
    lookup = {}

    for district in district_names:
        for disease in DISEASES:
            lookup[(str(district), disease)] = outbreak_multipliers(
                dates,
                district,
                disease,
                outbreak_data,
            )

    return lookup


# ------------------------------------------------------------
# Generate report delays
# ------------------------------------------------------------

def generate_report_delays(
    rng,
    n,
):
    """
    Generate base reporting delays from 0–21 days.

    Final dates are capped again after weekend effects.
    """

    # Gamma distribution creates mostly short delays
    # with a realistic long tail.
    raw = rng.gamma(
        shape=1.8,
        scale=2.3,
        size=n,
    )

    delays = np.rint(
        raw
    ).astype(
        np.int16
    )

    delays = np.clip(
        delays,
        0,
        MAX_REPORT_DELAY_DAYS,
    )

    return delays


# ------------------------------------------------------------
# Weekend reporting effect
# ------------------------------------------------------------

def weekend_reporting_delay(
    report_dates,
):
    """
    Push some weekend reports forward.

    Final report dates are capped at onset + 21 days
    later in the pipeline.
    """

    dates = pd.DatetimeIndex(
        report_dates
    )

    weekday = dates.weekday.to_numpy()

    rng = np.random.default_rng(
        9001 + len(dates)
    )

    extra = np.zeros(
        len(dates),
        dtype=np.int16,
    )

    weekend_mask = (
        weekday >= 5
    )

    if weekend_mask.any():

        extra[weekend_mask] = (
            rng.integers(
                1,
                3,
                size=weekend_mask.sum(),
            )
        )

    return (
        dates
        + pd.to_timedelta(
            extra,
            unit="D",
        )
    )


# ------------------------------------------------------------
# Outcome generation
# ------------------------------------------------------------

def generate_outcomes(
    rng,
    n,
    disease,
    ages,
):
    """
    Generate lab confirmation, admission and outcome.
    """

    # Disease-specific confirmation probability.
    if disease == "dengue":
        lab_probability = 0.65
        admission_probability = 0.08

    elif disease == "influenza":
        lab_probability = 0.55
        admission_probability = 0.06

    elif disease == "diarrhoeal":
        lab_probability = 0.35
        admission_probability = 0.07

    elif disease == "malaria":
        lab_probability = 0.70
        admission_probability = 0.10

    else:
        lab_probability = 0.45
        admission_probability = 0.07

    lab_confirmed = (
        rng.random(n)
        < lab_probability
    )

    # Older patients have somewhat higher admission risk.
    age_factor = (
        np.clip(
            ages,
            0,
            100,
        )
        / 100.0
    )

    admission_probability_array = (
        admission_probability
        + 0.10
        * age_factor
    )

    admitted = (
        rng.random(n)
        < admission_probability_array
    )

    outcome_random = rng.random(n)

    outcomes = np.full(
        n,
        "recovered",
        dtype=object,
    )

    death_mask = (
        outcome_random < 0.005
    )

    outcomes[death_mask] = (
        "died"
    )

    recovered_mask = (
        outcome_random >= 0.005
    )

    outcomes[
        recovered_mask
    ] = "recovered"

    return (
        lab_confirmed,
        admitted,
        outcomes,
    )


# ------------------------------------------------------------
# Silent facility artifact
# ------------------------------------------------------------

def choose_silent_facility(
    facilities,
    seed,
):
    """
    Select one deterministic facility for the
    3-week silent reporting artifact.
    """

    active = facilities[
        facilities["active"]
    ].copy()

    active = active.sort_values(
        [
            "district_name",
            "facility_id",
        ]
    )

    rng = np.random.default_rng(
        seed + 5000
    )

    index = int(
        rng.integers(
            0,
            len(active),
        )
    )

    row = active.iloc[index]

    return (
        row["facility_id"],
        row["district_name"],
    )


# ------------------------------------------------------------
# Apply silent + backfill artifact
# ------------------------------------------------------------

def apply_backfill_artifact(
    chunk,
    silent_facility_id,
    silent_start,
    silent_end,
):
    """
    Facility becomes silent for 3 weeks.

    Encounters occurring during the silent interval are
    reported together on the backfill date.

    We keep report delay <= 21 days by only modifying
    encounters whose onset is within 21 days before the
    selected backfill date.
    """

    mask = (
        (
            chunk["facility_id"].astype(str)
            == str(silent_facility_id)
        )
        & (
            pd.to_datetime(
                chunk["onset_date"]
            )
            >= silent_start
        )
        & (
            pd.to_datetime(
                chunk["onset_date"]
            )
            <= silent_end
        )
    )

    if not mask.any():
        return chunk

    backfill_date = (
        silent_end
        + pd.Timedelta(
            days=1
        )
    )

    onset = pd.to_datetime(
        chunk.loc[
            mask,
            "onset_date",
        ]
    )

    # Do not allow delay > 21.
    max_allowed = (
        onset
        + pd.Timedelta(
            days=MAX_REPORT_DELAY_DAYS
        )
    )

    final_dates = pd.Series(
        backfill_date,
        index=onset.index,
    )

    final_dates = pd.to_datetime(
        np.minimum(
            final_dates.to_numpy(
                dtype="datetime64[ns]"
            ),
            max_allowed.to_numpy(
                dtype="datetime64[ns]"
            ),
        )
    )

    chunk.loc[
        mask,
        "report_date",
    ] = final_dates.strftime(
        "%Y-%m-%d"
    )

    return chunk


# ------------------------------------------------------------
# Data quality injection
# ------------------------------------------------------------

def apply_quality_injection(
    chunk,
    global_start_index,
    total_rows,
    seed,
):
    """
    Apply deterministic data-quality defects chunk-by-chunk.

    This function does not load the complete S4 dataset into RAM.

    Injected issues:
    - ~2% duplicate encounter IDs
    - ~0.5% orphan facility IDs
    - ~1% invalid ages
    """

    rng = np.random.default_rng(
        seed + 7000 + global_start_index
    )

    n = len(chunk)

    if n == 0:
        return chunk

    # Keep age dtype wide enough for deliberately invalid values.
    # This avoids pandas LossySetitemError with int16 columns.
    chunk["age_years"] = (
        pd.to_numeric(
            chunk["age_years"],
            errors="coerce",
        )
        .fillna(-1)
        .astype(np.int32)
    )

    # --------------------------------------------------------
    # Duplicate encounter IDs: approximately 2%
    # --------------------------------------------------------
    duplicate_count = int(
        round(n * DUPLICATE_RATE)
    )

    if duplicate_count > 0 and n > 1:
        source_positions = rng.integers(
            0,
            n,
            size=duplicate_count,
        )

        target_positions = rng.choice(
            n,
            size=duplicate_count,
            replace=False,
        )

        chunk.loc[
            target_positions,
            "encounter_id",
        ] = (
            chunk.iloc[source_positions][
                "encounter_id"
            ].to_numpy()
        )

    # --------------------------------------------------------
    # Orphan facility IDs: approximately 0.5%
    # --------------------------------------------------------
    orphan_count = int(
        round(n * ORPHAN_RATE)
    )

    if orphan_count > 0:
        orphan_positions = rng.choice(
            n,
            size=orphan_count,
            replace=False,
        )

        orphan_ids = np.array(
            [
                f"ORPHAN_FAC_{global_start_index + i:010d}"
                for i in range(orphan_count)
            ],
            dtype=object,
        )

        chunk.loc[
            orphan_positions,
            "facility_id",
        ] = orphan_ids

    # --------------------------------------------------------
    # Invalid ages: approximately 1%
    # --------------------------------------------------------
    invalid_age_count = int(
        round(n * INVALID_AGE_RATE)
    )

    if invalid_age_count > 0:
        invalid_positions = rng.choice(
            n,
            size=invalid_age_count,
            replace=False,
        )

        invalid_values = rng.choice(
            np.array(
                [-1, 121, 999],
                dtype=np.int32,
            ),
            size=invalid_age_count,
        )

        chunk.loc[
            invalid_positions,
            "age_years",
        ] = invalid_values

    return chunk


# ------------------------------------------------------------
# Calculate daily counts
# ------------------------------------------------------------

def calculate_daily_counts(
    dates,
    scale_factor,
    rng,
):
    """
    Allocate the required total number of encounters
    across the 5-year period.

    At SF=1 the expected total is >=25M.
    """

    dates = pd.DatetimeIndex(
        dates
    )

    n_days = len(dates)

    target_total = int(
        math.ceil(
            BASE_COUNT_SF1
            * scale_factor
        )
    )

    if target_total < 1:
        target_total = 1

    # Seasonal population intensity.
    phase = (
        2
        * np.pi
        * (
            dates.dayofyear.to_numpy()
            / 365.25
        )
    )

    seasonal = (
        1.0
        + 0.18
        * np.sin(phase)
        + 0.08
        * np.cos(
            2 * phase
        )
    )

    # Slow upward trend.
    trend = np.linspace(
        0.95,
        1.08,
        n_days,
    )

    expected = (
        seasonal
        * trend
    )

    expected = np.maximum(
        expected,
        0.1,
    )

    expected /= expected.sum()

    # Negative-binomial-like overdispersion:
    # gamma multiplicative noise followed by
    # Poisson sampling.
    gamma_noise = rng.gamma(
        shape=20.0,
        scale=1.0 / 20.0,
        size=n_days,
    )

    weights = (
        expected
        * gamma_noise
    )

    weights = np.maximum(
        weights,
        0.0,
    )

    weights /= weights.sum()

    counts = rng.multinomial(
        target_total,
        weights,
    )

    return counts.astype(
        np.int64
    )


# ------------------------------------------------------------
# Generate a chunk
# ------------------------------------------------------------

def generate_chunk(
    dates,
    count,
    facilities,
    outbreak_lookup,
    rng,
    encounter_start_number,
    weather_dates=None,
    weather_drivers=None,
    disease_probabilities_by_district=None,
    weather_date_indices=None,
):
    """
    Generate one memory-safe encounter chunk.
    """

    if count <= 0:
        return pd.DataFrame(
            columns=ENCOUNTER_COLUMNS
        )

    dates = pd.DatetimeIndex(
        dates
    )

    # --------------------------------------------------------
    # Assign districts
    # --------------------------------------------------------

    district_names = sorted(
        facilities["district_name"]
        .unique()
        .tolist()
    )

    district_weights = (
        facilities.groupby(
            "district_name"
        )["bed_capacity"]
        .sum()
        .astype(float)
    )

    district_weights = (
        district_weights
        / district_weights.sum()
    )

    selected_districts = rng.choice(
        district_names,
        size=count,
        p=district_weights[
            district_names
        ].to_numpy()
    )

    # --------------------------------------------------------
    # Assign facilities
    # --------------------------------------------------------

    facilities_by_district = {
        district: group
        for district, group
        in facilities.groupby(
            "district_name",
            sort=False,
        )
    }

    selected_facility_ids = np.empty(
        count,
        dtype=object,
    )

    selected_bed_weights = np.empty(
        count,
        dtype=float,
    )

    for district in district_names:

        positions = np.where(
            selected_districts
            == district
        )[0]

        if len(positions) == 0:
            continue

        group = facilities_by_district[
            district
        ]

        weights = (
            group["bed_capacity"]
            .astype(float)
            .to_numpy()
            .copy()
        )

        weights = (
            weights
            / weights.sum()
        )

        selected_indices = rng.choice(
            len(group),
            size=len(positions),
            p=weights,
        )

        selected_rows = (
            group.iloc[
                selected_indices
            ]
        )

        selected_facility_ids[
            positions
        ] = (
            selected_rows[
                "facility_id"
            ]
            .to_numpy()
        )

        selected_bed_weights[
            positions
        ] = (
            selected_rows[
                "bed_capacity"
            ]
            .to_numpy()
        )

    # --------------------------------------------------------
    # Disease assignment + ACTUAL outbreak volume effect
    # --------------------------------------------------------

    unique_dates = pd.DatetimeIndex(sorted(set(dates)))
    if disease_probabilities_by_district is None:
        disease_probabilities_by_district = {
            district: disease_probabilities(
                unique_dates,
                district,
                weather_dates,
                weather_drivers,
            )
            for district in district_names
        }
    date_indices = weather_date_indices or {
        date: index for index, date in enumerate(unique_dates)
    }

    disease_values = np.empty(
        count,
        dtype=object,
    )

    # Each generated chunk contains one onset date, but the code also
    # supports multiple dates. For each district/date group, multiply
    # disease probabilities by the corresponding outbreak multiplier.
    # This makes outbreaks change the actual number of generated
    # encounters for the affected disease instead of merely calculating
    # an unused multiplier.
    for date in unique_dates:
        date_positions = np.where(dates == date)[0]
        if len(date_positions) == 0:
            continue

        date_index = date_indices[date]

        for district in district_names:
            positions = date_positions[
                selected_districts[date_positions] == district
            ]

            if len(positions) == 0:
                continue

            date_probability = disease_probabilities_by_district[district][date_index].astype(np.float64)
            adjusted = date_probability.copy()

            for disease_index, disease in enumerate(DISEASES):
                multiplier_array = outbreak_lookup.get(
                    (str(district), disease)
                )

                if multiplier_array is not None:
                    adjusted[disease_index] *= float(
                        multiplier_array[date_index]
                    )

            total_probability = adjusted.sum()

            if not np.isfinite(total_probability) or total_probability <= 0:
                adjusted = date_probability
                total_probability = adjusted.sum()

            adjusted /= total_probability

            disease_values[positions] = rng.choice(
                DISEASES,
                size=len(positions),
                p=adjusted,
            )

    # --------------------------------------------------------
    # Ages
    # --------------------------------------------------------

    ages = np.clip(
        rng.gamma(
            shape=3.2,
            scale=13.0,
            size=count,
        ),
        0,
        100,
    )

    ages = np.rint(
        ages
    ).astype(
        np.int16
    )

    # --------------------------------------------------------
    # Sex
    # --------------------------------------------------------

    sex = rng.choice(
        ["Male", "Female"],
        size=count,
        p=[0.51, 0.49],
    )

    # --------------------------------------------------------
    # Reporting delays
    # --------------------------------------------------------

    delays = (
        generate_report_delays(
            rng,
            count,
        )
    )

    onset_dates = dates

    report_dates = np.array(
        [
            (
                date
                + pd.Timedelta(
                    days=int(delay)
                )
            )
            for date, delay
            in zip(
                onset_dates,
                delays,
            )
        ],
        dtype="datetime64[ns]",
    )

    # Weekend reporting effect.
    report_dates = (
        weekend_reporting_delay(
            report_dates
        )
        .to_numpy(
            dtype="datetime64[ns]"
        )
    )

    # --------------------------------------------------------
    # FINAL HARD CAP:
    # report delay MUST remain <=21 days.
    # --------------------------------------------------------

    max_report_dates = (
        pd.DatetimeIndex(
            onset_dates
        )
        + pd.Timedelta(
            days=MAX_REPORT_DELAY_DAYS
        )
    )

    report_dates = np.minimum(
        report_dates,
        max_report_dates.to_numpy(
            dtype="datetime64[ns]"
        ),
    )

    # --------------------------------------------------------
    # Outcomes
    # --------------------------------------------------------

    lab_confirmed = np.zeros(
        count,
        dtype=bool,
    )

    admitted = np.zeros(
        count,
        dtype=bool,
    )

    outcomes = np.empty(
        count,
        dtype=object,
    )

    for disease in DISEASES:

        mask = (
            disease_values
            == disease
        )

        if not mask.any():
            continue

        (
            lab,
            adm,
            out,
        ) = generate_outcomes(
            rng,
            int(mask.sum()),
            disease,
            ages[mask],
        )

        lab_confirmed[
            mask
        ] = lab

        admitted[
            mask
        ] = adm

        outcomes[
            mask
        ] = out

    # --------------------------------------------------------
    # Encounter IDs
    # --------------------------------------------------------

    numbers = np.arange(
        encounter_start_number,
        encounter_start_number
        + count,
    )

    encounter_ids = np.array(
        [
            f"ENC{number:010d}"
            for number in numbers
        ],
        dtype=object,
    )

    # Patient IDs:
    # multiple encounters can belong to same patient.
    patient_numbers = (
        rng.integers(
            1,
            max(
                100,
                int(
                    count
                    * 0.70
                ),
            ),
            size=count,
        )
    )

    patient_ids = np.array(
        [
            f"PAT{number:010d}"
            for number in patient_numbers
        ],
        dtype=object,
    )

    # --------------------------------------------------------
    # ICD-10
    # --------------------------------------------------------

    icd10 = np.empty(count, dtype=object)
    for disease, codes in ICD10_CODES.items():
        disease_mask = disease_values == disease
        if disease_mask.any():
            icd10[disease_mask] = rng.choice(
                codes,
                size=int(disease_mask.sum()),
            )

    # --------------------------------------------------------
    # DataFrame
    # --------------------------------------------------------

    chunk = pd.DataFrame(
        {
            "encounter_id": encounter_ids,
            "patient_id": patient_ids,
            "facility_id": selected_facility_ids,
            "district_name": selected_districts,
            "onset_date": pd.to_datetime(
                onset_dates
            ).strftime(
                "%Y-%m-%d"
            ),
            "report_date": pd.to_datetime(
                report_dates
            ).strftime(
                "%Y-%m-%d"
            ),
            "age_years": ages,
            "sex": sex,
            "icd10_code": icd10,
            "disease": disease_values,
            "lab_confirmed": lab_confirmed,
            "admitted": admitted,
            "outcome": outcomes,
        }
    )

    # --------------------------------------------------------
    # Apply outbreak effect by duplicating additional
    # encounter rows is deliberately NOT done here.
    #
    # Outbreak effect is incorporated in the disease
    # generation signal and ground truth metadata.
    # --------------------------------------------------------

    return chunk


# ------------------------------------------------------------
# Apply outbreak signal to actual daily encounter volume
# ------------------------------------------------------------

def apply_outbreak_to_daily_counts(
    dates,
    counts,
    facilities,
    outbreak_lookup,
    seed,
    weather_dates=None,
    weather_drivers=None,
    disease_probabilities_by_district=None,
):
    """
    Make outbreak multipliers affect actual encounter volume.

    The requested total number of encounters is preserved. Outbreak days
    receive a larger share of that total, while non-outbreak days receive
    a correspondingly smaller share. Disease-specific outbreak effects are
    also applied later during disease allocation.
    """
    dates = pd.DatetimeIndex(dates)
    counts = np.asarray(counts, dtype=np.int64)

    if len(dates) == 0 or counts.sum() <= 0:
        return counts

    district_weights = (
        facilities.groupby("district_name")["bed_capacity"]
        .sum()
        .astype(float)
    )
    district_weights = (
        district_weights / district_weights.sum()
    )

    districts = sorted(
        facilities["district_name"].unique().tolist()
    )

    # Base disease probabilities determine how much each disease
    # contributes to the daily outbreak-volume signal.
    if disease_probabilities_by_district is None:
        disease_probabilities_by_district = build_disease_probability_lookup(
            dates,
            districts,
            weather_drivers,
        )

    daily_factor = np.ones(len(dates), dtype=np.float64)

    for date_index in range(len(dates)):
        date_factor = 0.0

        for district in districts:
            district_weight = float(
                district_weights[district]
            )

            district_factor = 0.0

            disease_probs = disease_probabilities_by_district[district][date_index]
            for disease_index, disease in enumerate(DISEASES):
                values = outbreak_lookup.get(
                    (str(district), disease)
                )

                multiplier = 1.0
                if values is not None:
                    multiplier = float(values[date_index])

                district_factor += (
                    disease_probs[disease_index]
                    * multiplier
                )

            date_factor += district_weight * district_factor

        daily_factor[date_index] = max(
            0.1,
            date_factor,
        )

    # Reweight the already-generated daily plan while preserving the
    # exact target total. A small amount of multinomial sampling keeps
    # the existing stochastic behavior.
    base_weights = counts.astype(np.float64) * daily_factor

    if not np.isfinite(base_weights).all() or base_weights.sum() <= 0:
        return counts

    base_weights /= base_weights.sum()

    rng = np.random.default_rng(seed + 2500)
    adjusted_counts = rng.multinomial(
        int(counts.sum()),
        base_weights,
    )

    return adjusted_counts.astype(np.int64)


# ------------------------------------------------------------
# Generate encounter dates/counts
# ------------------------------------------------------------

def build_daily_generation_plan(
    scale_factor,
    seed,
):
    dates = pd.date_range(
        START_DATE,
        END_DATE,
        freq="D",
    )

    rng = np.random.default_rng(
        seed + 2000
    )

    counts = calculate_daily_counts(
        dates,
        scale_factor,
        rng,
    )

    return dates, counts


# ------------------------------------------------------------
# Main generation
# ------------------------------------------------------------

def generate_dataset(
    scale_factor,
    seed,
):
    print("=" * 70)
    print("EpiScope S4 Synthetic Encounter Generator")
    print("=" * 70)
    print(
        f"Scale factor : {scale_factor}"
    )
    print(
        f"Seed         : {seed}"
    )
    print(
        f"Start date   : {START_DATE.date()}"
    )
    print(
        f"End date     : {END_DATE.date()}"
    )
    print()

    # --------------------------------------------------------
    # Load facilities
    # --------------------------------------------------------

    facilities = load_facilities()

    print(
        f"Active facilities : {len(facilities):,}"
    )

    print(
        f"Districts          : "
        f"{facilities['district_name'].nunique()}"
    )

    if (
        facilities["district_name"]
        .nunique()
        != 64
    ):
        raise RuntimeError(
            "S5 does not contain all 64 districts."
        )

    # --------------------------------------------------------
    # Output directories
    # --------------------------------------------------------

    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    GROUND_TRUTH_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    # Remove old generated encounter chunks.
    old_chunks = list(
        OUTPUT_DIR.glob(
            "encounters_part_*.parquet"
        )
    )

    for file in old_chunks:
        file.unlink()

    # --------------------------------------------------------
    # Ground truth
    # --------------------------------------------------------

    outbreaks = generate_outbreaks(
        facilities,
        seed,
        MIN_OUTBREAKS,
    )

    outbreaks.to_csv(
        GROUND_TRUTH_FILE,
        index=False,
        encoding="utf-8",
    )

    print(
        f"Ground-truth outbreaks: "
        f"{len(outbreaks)}"
    )

    print(
        "Ground truth written separately:"
    )

    print(
        f"  {GROUND_TRUTH_FILE}"
    )

    # --------------------------------------------------------
    # Silent facility
    # --------------------------------------------------------

    (
        silent_facility_id,
        silent_district,
    ) = choose_silent_facility(
        facilities,
        seed,
    )

    # Choose a deterministic 21-day silent period
    # inside the data range.
    silent_start = pd.Timestamp(
        "2023-06-01"
    )

    silent_end = (
        silent_start
        + pd.Timedelta(
            days=20
        )
    )

    print()
    print(
        "Reporting artifact:"
    )

    print(
        f"  Silent facility : "
        f"{silent_facility_id}"
    )

    print(
        f"  District        : "
        f"{silent_district}"
    )

    print(
        f"  Silent period   : "
        f"{silent_start.date()} "
        f"to "
        f"{silent_end.date()}"
    )

    print(
        f"  Backfill date   : "
        f"{(silent_end + pd.Timedelta(days=1)).date()}"
    )

    # --------------------------------------------------------
    # Daily plan
    # --------------------------------------------------------

    dates, counts = (
        build_daily_generation_plan(
            scale_factor,
            seed,
        )
    )

    # Convert outbreak metadata once and precompute the multiplier lookup.
    # The lookup is then reused for every encounter chunk.
    outbreak_data = prepare_outbreak_arrays(outbreaks)
    district_names = sorted(
        facilities["district_name"].unique().tolist()
    )
    weather_dates = pd.date_range(START_DATE, END_DATE, freq="D")
    weather_drivers = load_weather_drivers(weather_dates, district_names)
    weather_date_indices = {
        date: index for index, date in enumerate(weather_dates)
    }
    disease_probabilities_by_district = build_disease_probability_lookup(
        weather_dates,
        district_names,
        weather_drivers,
    )
    outbreak_lookup = build_outbreak_lookup(
        dates,
        district_names,
        outbreak_data,
    )

    target_total = int(
        counts.sum()
    )

    # IMPORTANT: outbreak multipliers now affect actual encounter volume.
    # The exact target total is preserved, but outbreak-active dates receive
    # a larger share of encounters.
    counts = apply_outbreak_to_daily_counts(
        dates,
        counts,
        facilities,
        outbreak_lookup,
        seed,
        weather_dates,
        weather_drivers,
        disease_probabilities_by_district,
    )

    print()
    print(
        f"Target encounters : "
        f"{target_total:,}"
    )

    if scale_factor >= 1.0:
        if target_total < BASE_COUNT_SF1:
            raise RuntimeError(
                "SF=1 must generate at least 25M encounters."
            )

    # --------------------------------------------------------
    # RNG
    # --------------------------------------------------------

    rng = np.random.default_rng(
        seed + 3000
    )

    global_index = 0
    part_number = 1
    encounter_number = 1

    # --------------------------------------------------------
    # Generate day-by-day and write chunks.
    # --------------------------------------------------------

    buffer = []

    generated_rows = 0

    for date, count in zip(
        dates,
        counts,
    ):

        count = int(count)

        if count <= 0:
            continue

        remaining = count

        while remaining > 0:

            take = min(
                remaining,
                CHUNK_SIZE,
            )

            day_array = pd.DatetimeIndex(
                [date] * take
            )

            chunk = generate_chunk(
                day_array,
                take,
                facilities,
                outbreak_lookup,
                rng,
                encounter_number,
                weather_dates,
                weather_drivers,
                disease_probabilities_by_district,
                weather_date_indices,
            )

            # ------------------------------------------------
            # Reporting artifact
            # ------------------------------------------------

            chunk = apply_backfill_artifact(
                chunk,
                silent_facility_id,
                silent_start,
                silent_end,
            )

            # ------------------------------------------------
            # Data quality injection
            # ------------------------------------------------

            chunk = apply_quality_injection(
                chunk,
                global_index,
                target_total,
                seed,
            )

            # ------------------------------------------------
            # Ensure required columns/order.
            # ------------------------------------------------

            chunk = chunk[
                ENCOUNTER_COLUMNS
            ]

            buffer.append(
                chunk
            )

            generated_rows += len(
                chunk
            )

            global_index += len(
                chunk
            )

            encounter_number += len(
                chunk
            )

            remaining -= take

            # ------------------------------------------------
            # Flush buffer when sufficiently large.
            # ------------------------------------------------

            buffered_rows = sum(
                len(x)
                for x in buffer
            )

            if buffered_rows >= CHUNK_SIZE:

                output_chunk = pd.concat(
                    buffer,
                    ignore_index=True,
                )

                output_file = (
                    OUTPUT_DIR
                    / (
                        f"encounters_part_"
                        f"{part_number:05d}.parquet"
                    )
                )

                output_chunk.to_parquet(
                    output_file,
                    index=False,
                    engine="pyarrow",
                )

                print(
                    f"Written part "
                    f"{part_number:05d}: "
                    f"{len(output_chunk):,} rows"
                )

                part_number += 1

                buffer = []

    # --------------------------------------------------------
    # Final buffer
    # --------------------------------------------------------

    if buffer:

        output_chunk = pd.concat(
            buffer,
            ignore_index=True,
        )

        output_file = (
            OUTPUT_DIR
            / (
                f"encounters_part_"
                f"{part_number:05d}.parquet"
            )
        )

        output_chunk.to_parquet(
            output_file,
            index=False,
            engine="pyarrow",
        )

        print(
            f"Written part "
            f"{part_number:05d}: "
            f"{len(output_chunk):,} rows"
        )

    # --------------------------------------------------------
    # Summary
    # --------------------------------------------------------

    print()
    print("=" * 70)
    print("Generation complete")
    print("=" * 70)

    print(
        f"Rows generated : "
        f"{generated_rows:,}"
    )

    print(
        f"Target rows    : "
        f"{target_total:,}"
    )

    print(
        f"Parquet parts  : "
        f"{part_number}"
    )

    print(
        f"Output folder  : "
        f"{OUTPUT_DIR}"
    )

    print(
        f"Ground truth   : "
        f"{GROUND_TRUTH_FILE}"
    )

    print()
    print(
        "Data quality injection targets:"
    )

    print(
        f"  Duplicate IDs  ~ "
        f"{DUPLICATE_RATE * 100:.1f}%"
    )

    print(
        f"  Orphan facility ~ "
        f"{ORPHAN_RATE * 100:.1f}%"
    )

    print(
        f"  Invalid ages   ~ "
        f"{INVALID_AGE_RATE * 100:.1f}%"
    )

    print()
    print(
        "Reporting delay requirement:"
    )

    print(
        f"  Maximum allowed: "
        f"{MAX_REPORT_DELAY_DAYS} days"
    )

    print()
    print(
        "S4 generation finished successfully."
    )


# ------------------------------------------------------------
# Validation
# ------------------------------------------------------------

def validate_generated_data():
    """
    Validate generated Parquet files without loading
    the entire dataset into memory.

    Reads metadata and processes files one at a time.
    """

    files = sorted(
        OUTPUT_DIR.glob(
            "encounters_part_*.parquet"
        )
    )

    if not files:
        raise RuntimeError(
            "No encounter Parquet files found."
        )

    total_rows = 0
    district_values = set()

    duplicate_ids = 0
    orphan_ids = 0
    invalid_ages = 0
    delay_violations = 0

    # Track IDs seen across chunks.
    # At SF=1 this can still be large, so this is intended
    # for test/SF=0.01 validation. Production Spark DQ should
    # perform the full duplicate check.
    seen_ids = set()

    for file in files:

        df = pd.read_parquet(
            file
        )

        total_rows += len(df)

        district_values.update(
            df["district_name"]
            .dropna()
            .astype(str)
            .unique()
        )

        # Duplicate IDs.
        ids = (
            df["encounter_id"]
            .astype(str)
        )

        duplicate_ids += int(
            ids.duplicated().sum()
        )

        # Orphan facility IDs.
        orphan_ids += int(
            df["facility_id"]
            .astype(str)
            .str.startswith(
                "ORPHAN_FAC_"
            )
            .sum()
        )

        # Invalid age.
        ages = pd.to_numeric(
            df["age_years"],
            errors="coerce",
        )

        invalid_ages += int(
            (
                (ages < 0)
                | (ages > 120)
            ).sum()
        )

        # Report delay.
        onset = pd.to_datetime(
            df["onset_date"]
        )

        report = pd.to_datetime(
            df["report_date"]
        )

        delays = (
            report - onset
        ).dt.days

        delay_violations += int(
            (
                (delays < 0)
                | (
                    delays
                    > MAX_REPORT_DELAY_DAYS
                )
            ).sum()
        )

    print()
    print("=" * 70)
    print("S4 Validation")
    print("=" * 70)

    print(
        f"Total rows       : {total_rows:,}"
    )

    print(
        f"Districts        : {len(district_values)}"
    )

    print(
        f"Duplicate rows    : {duplicate_ids:,}"
    )

    print(
        f"Orphan facilities : {orphan_ids:,}"
    )

    print(
        f"Invalid ages      : {invalid_ages:,}"
    )

    print(
        f"Delay violations  : {delay_violations:,}"
    )

    if len(district_values) != 64:
        print(
            "WARNING: expected 64 districts."
        )

    if delay_violations > 0:
        raise RuntimeError(
            "Report delay validation failed."
        )

    print()
    print(
        "Validation complete."
    )


# ------------------------------------------------------------
# Main
# ------------------------------------------------------------

def main():

    args = parse_args()

    seed, scale_factor = (
        get_configuration(args)
    )

    # --------------------------------------------------------
    # Seed Python + NumPy
    # --------------------------------------------------------

    random.seed(seed)
    np.random.seed(seed)

    generate_dataset(
        scale_factor=scale_factor,
        seed=seed,
    )


if __name__ == "__main__":
    main()