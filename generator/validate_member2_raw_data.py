from pathlib import Path
import pandas as pd
import json

# Project folders
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data" / "raw"

# Required columns
REQUIRED_COLUMNS = {
    "S4_encounters": [
        "encounter_id", "patient_id", "facility_id",
        "age_years", "onset_date", "report_date",
        "sex", "icd10_code", "lab_confirmed",
        "admitted", "outcome"
    ],
    "S5_facilities": [
        "facility_id", "district_name", "facility_type",
        "bed_capacity", "active"
    ],
    "S6_stock_beds": [
        "snapshot_date", "facility_id", "district_name",
        "bed_capacity", "available_beds", "occupied_beds",
        "saline_stock", "test_kit_stock"
    ]
}


def validate_csv(file_path, required_columns):
    """Check CSV columns, missing values and duplicate rows."""

    if not file_path.exists():
        return {"status": "Missing file"}

    df = pd.read_csv(file_path)

    missing_columns = [
        col for col in required_columns if col not in df.columns
    ]

    if missing_columns:
        return {
            "status": "Failed",
            "missing_columns": missing_columns
        }

    return {
        "status": "Passed",
        "total_rows": len(df),
        "missing_values": int(df.isnull().sum().sum()),
        "duplicate_rows": int(df.duplicated().sum())
    }


def validate_parquet_folder(folder_path, required_columns):
    """Check all Parquet files in the S4 folder."""

    files = sorted(folder_path.glob("*.parquet"))

    if not files:
        return {"status": "Missing Parquet files"}

    total_rows = 0
    results = []

    for file in files:
        df = pd.read_parquet(file)

        missing_columns = [
            col for col in required_columns if col not in df.columns
        ]

        if missing_columns:
            results.append({
                "file": file.name,
                "status": "Failed",
                "missing_columns": missing_columns
            })
        else:
            total_rows += len(df)
            results.append({
                "file": file.name,
                "status": "Passed",
                "rows": len(df),
                "duplicate_rows": int(df.duplicated().sum())
            })

    return {
        "status": "Passed" if all(
            r["status"] == "Passed" for r in results
        ) else "Failed",
        "total_rows": total_rows,
        "files": results
    }


def main():
    report = {}

    report["S4_encounters"] = validate_parquet_folder(
        DATA_DIR / "S4_encounters",
        REQUIRED_COLUMNS["S4_encounters"]
    )

    report["S5_facilities"] = validate_csv(
        DATA_DIR / "S5_facilities" / "facilities.csv",
        REQUIRED_COLUMNS["S5_facilities"]
    )

    report["S6_stock_beds"] = validate_csv(
        DATA_DIR / "S6_stock_beds" / "stock_beds.csv",
        REQUIRED_COLUMNS["S6_stock_beds"]
    )

    output_dir = PROJECT_ROOT / "generator" / "output"
    output_dir.mkdir(parents=True, exist_ok=True)

    output_file = output_dir / "member2_data_validation.json"

    with open(output_file, "w", encoding="utf-8") as file:
        json.dump(report, file, indent=4)

    print("\n===== MEMBER 2 DATA VALIDATION =====")

    for name, result in report.items():
        print(f"\n{name}: {result['status']}")

        if "total_rows" in result:
            print(f"Total rows: {result['total_rows']}")

        if "missing_values" in result:
            print(f"Missing values: {result['missing_values']}")

        if "duplicate_rows" in result:
            print(f"Duplicate rows: {result['duplicate_rows']}")

    print(f"\nReport saved to: {output_file}")


if __name__ == "__main__":
    main()