from pathlib import Path
import pandas as pd


INPUT_FILE = Path(
    "data/raw/S2_population/"
    "bangladesh_bbs_population-and-housing-census-dataset_2022_admin-02.xlsx"
)

SHEET_NAME = "Population_District"


def main():
    print("=" * 70)
    print("EpiScope - S2 Population Source Inspection")
    print("=" * 70)

    if not INPUT_FILE.exists():
        raise FileNotFoundError(
            f"Population file not found:\n{INPUT_FILE}"
        )

    print(f"\nInput file : {INPUT_FILE}")
    print(f"Sheet      : {SHEET_NAME}")

    df = pd.read_excel(
        INPUT_FILE,
        sheet_name=SHEET_NAME
    )

    print(f"\nRows       : {len(df)}")
    print(f"Columns    : {len(df.columns)}")

    print("\nColumn names:")
    for i, col in enumerate(df.columns, start=1):
        print(f"{i:02d}. {repr(col)}")

    print("\n" + "=" * 70)
    print("First 10 rows")
    print("=" * 70)

    print(df.head(10).to_string())

    print("\n" + "=" * 70)
    print("Possible district-related columns")
    print("=" * 70)

    for col in df.columns:
        name = str(col).lower()

        if (
            "district" in name
            or "dist" in name
            or "adm2" in name
            or "zilla" in name
        ):
            print(f"  DISTRICT CANDIDATE: {repr(col)}")

    print("\n" + "=" * 70)
    print("Possible population-related columns")
    print("=" * 70)

    for col in df.columns:
        name = str(col).lower()

        if (
            "population" in name
            or "pop" in name
            or "total" in name
        ):
            print(f"  POPULATION CANDIDATE: {repr(col)}")

    print("\nInspection complete.")


if __name__ == "__main__":
    main()