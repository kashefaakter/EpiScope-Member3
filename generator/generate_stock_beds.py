from pathlib import Path
import argparse
import random
import pandas as pd


# ============================================================
# EpiScope - S6 Stock & Bed Occupancy Generator
# ============================================================

SEED = 4345

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
    / "S6_stock_beds"
)

OUTPUT_DIR.mkdir(
    parents=True,
    exist_ok=True
)

START_DATE = "2021-01-01"
END_DATE = "2025-12-31"


# ============================================================
# GENERATOR
# ============================================================

def generate_stock_beds(seed=SEED):
    rng = random.Random(seed)

    if not FACILITY_FILE.exists():
        raise FileNotFoundError(
            f"Facility file not found: {FACILITY_FILE}"
        )

    facilities = pd.read_csv(FACILITY_FILE)

    facilities["bed_capacity"] = (
        facilities["bed_capacity"]
        .astype(int)
    )

    dates = pd.date_range(
        START_DATE,
        END_DATE,
        freq="D"
    )

    rows = []

    for _, facility in facilities.iterrows():

        facility_id = facility["facility_id"]
        district_name = facility["district_name"]
        bed_capacity = int(facility["bed_capacity"])

        # Facility-specific deterministic starting level
        base_stock = rng.randint(100, 1000)

        for date in dates:

            # Seasonal/resource variation
            seasonal_factor = (
                1.0
                + 0.15
                * __import__("math").sin(
                    2
                    * __import__("math").pi
                    * date.dayofyear
                    / 365.25
                )
            )

            available_beds = max(
                0,
                min(
                    bed_capacity,
                    int(
                        bed_capacity
                        *                         rng.uniform(
                            0.55,
                            0.95
                        )
                        * seasonal_factor
                    )
                )
            )

            occupied_beds = max(
                0,
                min(
                    available_beds,
                    int(
                        available_beds
                        *                         rng.uniform(
                            0.45,
                            0.90
                        )
                    )
                )
            )

            saline_stock = max(
                0,
                int(
                    base_stock
                    *                     rng.uniform(
                        0.50,
                        1.20
                    )
                )
            )

            test_kit_stock = max(
                0,
                int(
                    base_stock
                    *                     rng.uniform(
                        0.20,
                        0.80
                    )
                )
            )

            rows.append(
                {
                    "snapshot_date": date.strftime(
                        "%Y-%m-%d"
                    ),
                    "facility_id": facility_id,
                    "district_name": district_name,
                    "bed_capacity": bed_capacity,
                    "available_beds": available_beds,
                    "occupied_beds": occupied_beds,
                    "saline_stock": saline_stock,
                    "test_kit_stock": test_kit_stock,
                }
            )

    df = pd.DataFrame(rows)

    output_file = (
        OUTPUT_DIR
        / "stock_beds.csv"
    )

    df.to_csv(
        output_file,
        index=False
    )

    print("=" * 60)
    print("EpiScope S6 Stock & Bed Generator")
    print("=" * 60)
    print(f"Facilities: {facilities['facility_id'].nunique()}")
    print(f"Districts: {facilities['district_name'].nunique()}")
    print(f"Seed: {seed}")
    print(f"Snapshots: {len(df)}")
    print(f"Output: {output_file}")
    print()
    print("Columns:")
    print(list(df.columns))
    print()
    print(df.head().to_string(index=False))


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate deterministic S6 snapshots")
    parser.add_argument("--seed", type=int, default=SEED)
    generate_stock_beds(parser.parse_args().seed)