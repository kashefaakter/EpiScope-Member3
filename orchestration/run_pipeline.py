from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]

PIPELINE_STEPS = [
    ("Bronze ingestion", "pipeline/bronze/ingest_bronze.py"),
    ("Silver S1-S3", "pipeline/silver/silver_s1_s2_s3.py"),
    ("Silver S4 encounters", "pipeline/silver/silver_transform.py"),
    ("Silver S5-S6 resources", "pipeline/silver/silver_s5_s6.py"),
    ("Gold surveillance mart", "pipeline/gold/build_surveillance_mart.py"),
    ("Gold feature mart", "pipeline/gold/build_features.py"),
    ("Outbreak detection", "analytics/detection/outbreak_detection.py"),
    ("Disease forecasting", "analytics/forecast/forecast_models.py"),
    ("Seasonal preparedness", "analytics/calendar/seasonal_preparedness.py"),
    ("Resource demand", "analytics/forecast/resource_demand.py"),
    ("Resource allocation", "analytics/optimise/resource_allocation.py"),
    ("Dashboard serving", "serving/build_dashboard_data.py"),
]


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run the existing EpiScope pipeline from raw inputs to serving JSON."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate and print the stage sequence without running or overwriting outputs.",
    )
    args = parser.parse_args()

    missing = [
        relative_path
        for _, relative_path in PIPELINE_STEPS
        if not (PROJECT_ROOT / relative_path).is_file()
    ]
    if missing:
        parser.error("Pipeline scripts not found: " + ", ".join(missing))

    for index, (name, relative_path) in enumerate(PIPELINE_STEPS, start=1):
        script = PROJECT_ROOT / relative_path
        print(f"[{index}/{len(PIPELINE_STEPS)}] {name}: {relative_path}", flush=True)
        if args.dry_run:
            continue
        subprocess.run(
            [sys.executable, str(script)],
            cwd=PROJECT_ROOT,
            check=True,
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())