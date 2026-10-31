from pathlib import Path
import random
import pandas as pd


# ============================================================
# EpiScope S5 — Synthetic Facility Registry Generator
# ============================================================

BASE_DIR = Path(__file__).resolve().parents[1]

OUTPUT_DIR = BASE_DIR / "data" / "raw" / "S5_facilities"
OUTPUT_FILE = OUTPUT_DIR / "facilities.csv"

SEED = 4345

# S1 district names are treated as the authoritative names.
# These are the 64 districts represented in the downloaded
# S1 administrative boundary dataset.
DISTRICTS = [
    "Bagerhat",
    "Bandarban",
    "Barguna",
    "Barisal",
    "Bhola",
    "Bogra",
    "Brahamanbaria",
    "Chandpur",
    "Chittagong",
    "Chuadanga",
    "Comilla",
    "Cox's Bazar",
    "Dhaka",
    "Dinajpur",
    "Faridpur",
    "Feni",
    "Gaibandha",
    "Gazipur",
    "Gopalganj",
    "Habiganj",
    "Jamalpur",
    "Jessore",
    "Jhalokati",
    "Jhenaidah",
    "Joypurhat",
    "Khagrachhari",
    "Khulna",
    "Kishoreganj",
    "Kurigram",
    "Kushtia",
    "Lakshmipur",
    "Lalmonirhat",
    "Madaripur",
    "Magura",
    "Manikganj",
    "Maulvibazar",
    "Meherpur",
    "Munshiganj",
    "Mymensingh",
    "Naogaon",
    "Narail",
    "Narayanganj",
    "Narsingdi",
    "Natore",
    "Nawabganj",
    "Netrakona",
    "Nilphamari",
    "Noakhali",
    "Pabna",
    "Panchagarh",
    "Patuakhali",
    "Pirojpur",
    "Rajbari",
    "Rajshahi",
    "Rangamati",
    "Rangpur",
    "Satkhira",
    "Shariatpur",
    "Sherpur",
    "Sirajganj",
    "Sunamganj",
    "Sylhet",
    "Tangail",
    "Thakurgaon",
]


FACILITY_TYPES = [
    "District Hospital",
    "Upazila Health Complex",
    "Medical College Hospital",
    "General Hospital",
    "Community Clinic",
]


def generate_facilities():
    """
    Generate deterministic synthetic healthcare facility registry.

    Requirements:
    - All 64 districts must be represented.
    - Deterministic using fixed seed.
    - Facility IDs are unique.
    - Includes bed capacity and active status.
    """

    random.seed(SEED)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    records = []

    facility_counter = 1

    for district in DISTRICTS:

        # Every district receives at least one facility.
        # Additional facilities are generated deterministically.
        facility_count = random.randint(15, 30)

        for i in range(facility_count):

            facility_id = f"FAC{facility_counter:05d}"

            facility_type = random.choice(FACILITY_TYPES)

            # Give different facility types different typical
            # capacity ranges.
            if facility_type == "Medical College Hospital":
                bed_capacity = random.randint(250, 800)

            elif facility_type == "District Hospital":
                bed_capacity = random.randint(100, 400)

            elif facility_type == "General Hospital":
                bed_capacity = random.randint(80, 250)

            elif facility_type == "Upazila Health Complex":
                bed_capacity = random.randint(30, 120)

            else:
                # Community Clinic
                bed_capacity = random.randint(5, 30)

            # Most facilities are active.
            active = random.random() < 0.95

            records.append(
                {
                    "facility_id": facility_id,
                    "district_name": district,
                    "facility_type": facility_type,
                    "bed_capacity": int(bed_capacity),
                    "active": bool(active),
                }
            )

            facility_counter += 1

    df = pd.DataFrame(records)

    # --------------------------------------------------------
    # Validation
    # --------------------------------------------------------

    expected_districts = set(DISTRICTS)
    actual_districts = set(df["district_name"].unique())

    missing = sorted(expected_districts - actual_districts)
    extra = sorted(actual_districts - expected_districts)

    if missing:
        raise RuntimeError(
            f"Facility generation failed. Missing districts: {missing}"
        )

    if extra:
        raise RuntimeError(
            f"Facility generation failed. Unexpected districts: {extra}"
        )

    if df["facility_id"].duplicated().any():
        raise RuntimeError(
            "Facility generation failed: duplicate facility IDs found."
        )

    if len(actual_districts) != 64:
        raise RuntimeError(
            f"Facility generation failed: expected 64 districts, "
            f"found {len(actual_districts)}."
        )

    # --------------------------------------------------------
    # Save
    # --------------------------------------------------------

    df.to_csv(
        OUTPUT_FILE,
        index=False,
        encoding="utf-8",
    )

    print("=" * 70)
    print("EpiScope S5 Synthetic Facility Registry")
    print("=" * 70)
    print(f"Seed                 : {SEED}")
    print(f"Facilities            : {len(df):,}")
    print(f"Districts             : {df['district_name'].nunique()}")
    print(f"Active facilities     : {df['active'].sum():,}")
    print(f"Output                : {OUTPUT_FILE}")
    print()

    print("Facilities by district:")
    print(
        df.groupby("district_name")
        .size()
        .sort_index()
        .to_string()
    )

    print()
    print("Validation: PASS")
    print("All 64 S1 districts are represented.")


if __name__ == "__main__":
    generate_facilities()