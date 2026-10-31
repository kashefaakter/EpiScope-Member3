from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd


# ============================================================
# EpiScope - Bronze Layer Ingestion
# ============================================================
#
# Purpose:
#   Ingest S1-S6 raw source data into the Bronze layer.
#
# Bronze principles:
#   1. Raw source data is NOT modified.
#   2. Every ingested object gets provenance metadata.
#   3. Each load receives a unique batch_id.
#   4. A SHA-256 checksum is calculated for every source file.
#   5. A manifest is written for lineage/audit purposes.
#
# Sources:
#   S1 - Administrative boundaries
#   S2 - Population
#   S3 - Weather
#   S4 - Synthetic encounters
#   S5 - Facility registry
#   S6 - Stock / bed snapshots
# ============================================================


PROJECT_ROOT = Path(__file__).resolve().parents[2]

RAW_ROOT = PROJECT_ROOT / "data" / "raw"
BRONZE_ROOT = PROJECT_ROOT / "data" / "bronze"


SOURCE_CONFIG = {
    "S1_boundaries": {
        "raw_dir": RAW_ROOT / "S1_boundaries",
        "bronze_dir": BRONZE_ROOT / "S1_boundaries",
    },
    "S2_population": {
        "raw_dir": RAW_ROOT / "S2_population",
        "bronze_dir": BRONZE_ROOT / "S2_population",
    },
    "S3_weather": {
        "raw_dir": RAW_ROOT / "S3_weather",
        "bronze_dir": BRONZE_ROOT / "S3_weather",
    },
    "S4_encounters": {
        "raw_dir": RAW_ROOT / "S4_encounters",
        "bronze_dir": BRONZE_ROOT / "S4_encounters",
    },
    "S5_facilities": {
        "raw_dir": RAW_ROOT / "S5_facilities",
        "bronze_dir": BRONZE_ROOT / "S5_facilities",
    },
    "S6_stock_beds": {
        "raw_dir": RAW_ROOT / "S6_stock_beds",
        "bronze_dir": BRONZE_ROOT / "S6_stock_beds",
    },
}


SUPPORTED_EXTENSIONS = {
    ".csv",
    ".xlsx",
    ".xls",
    ".json",
    ".geojson",
    ".txt",
    ".parquet",
}


# ============================================================
# Utility functions
# ============================================================


def utc_now() -> str:
    """Return current UTC timestamp in ISO-8601 format."""
    return datetime.now(timezone.utc).isoformat()


def calculate_sha256(file_path: Path, chunk_size: int = 1024 * 1024) -> str:
    """
    Calculate SHA-256 checksum without loading the whole file
    into memory.
    """
    sha256 = hashlib.sha256()

    with file_path.open("rb") as file:
        while True:
            chunk = file.read(chunk_size)

            if not chunk:
                break

            sha256.update(chunk)

    return sha256.hexdigest()


def safe_relative_path(path: Path, root: Path) -> str:
    """Return a normalized relative path."""
    try:
        return path.relative_to(root).as_posix()
    except ValueError:
        return path.name


def discover_files(raw_dir: Path) -> list[Path]:
    """
    Discover supported source files recursively.
    Hidden/system files are ignored.
    """
    if not raw_dir.exists():
        return []

    files = []

    for path in raw_dir.rglob("*"):
        if not path.is_file():
            continue

        if path.name.startswith("~$"):
            continue

        if path.suffix.lower() not in SUPPORTED_EXTENSIONS:
            continue

        files.append(path)

    return sorted(files)


def create_batch_id() -> str:
    """
    Generate a unique ingestion batch ID.
    """
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    short_uuid = uuid.uuid4().hex[:12]

    return f"BRONZE_{timestamp}_{short_uuid}"


def copy_source_file(
    source_file: Path,
    raw_dir: Path,
    bronze_dir: Path,
) -> Path:
    """
    Copy a source file into Bronze while preserving its relative
    directory structure.

    The source itself is never modified.
    """
    relative_path = source_file.relative_to(raw_dir)

    destination = bronze_dir / relative_path

    destination.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    shutil.copy2(
        source_file,
        destination,
    )

    return destination


def build_metadata(
    source_name: str,
    source_file: Path,
    bronze_file: Path,
    batch_id: str,
    load_time: str,
) -> dict:
    """
    Build provenance metadata for one source file.
    """
    source_size = source_file.stat().st_size
    checksum = calculate_sha256(source_file)

    return {
        "source_name": source_name,
        "source_file": source_file.name,
        "source_relative_path": safe_relative_path(
            source_file,
            RAW_ROOT,
        ),
        "bronze_relative_path": safe_relative_path(
            bronze_file,
            BRONZE_ROOT,
        ),
        "file_extension": source_file.suffix.lower(),
        "file_size_bytes": source_size,
        "source_checksum_sha256": checksum,
        "source_load_time": load_time,
        "batch_id": batch_id,
    }


def write_json(
    output_path: Path,
    data,
) -> None:
    """Write JSON with readable formatting."""
    output_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with output_path.open(
        "w",
        encoding="utf-8",
    ) as file:
        json.dump(
            data,
            file,
            indent=2,
            ensure_ascii=False,
        )


# ============================================================
# Optional small-file inspection
# ============================================================


def inspect_source_file(path: Path) -> dict:
    """
    Collect lightweight metadata.

    This function intentionally avoids reading large encounter
    or stock files into Pandas.
    """
    result = {
        "file_name": path.name,
        "extension": path.suffix.lower(),
        "size_bytes": path.stat().st_size,
    }

    extension = path.suffix.lower()

    try:
        if extension == ".csv":
            # Read only header to avoid loading large files.
            header = pd.read_csv(
                path,
                nrows=0,
            )

            result["columns"] = list(header.columns)

        elif extension in {".json", ".geojson"}:
            # Only inspect small JSON files.
            if path.stat().st_size <= 20 * 1024 * 1024:
                with path.open(
                    "r",
                    encoding="utf-8",
                ) as file:
                    obj = json.load(file)

                if isinstance(obj, dict):
                    result["json_keys"] = list(obj.keys())

        elif extension in {".xlsx", ".xls"}:
            # Excel workbook sheet names only.
            excel = pd.ExcelFile(path)
            result["sheet_names"] = excel.sheet_names

        elif extension == ".parquet":
            # Metadata only.
            parquet_df = pd.read_parquet(
                path,
                engine="pyarrow",
            )

            result["columns"] = list(parquet_df.columns)

    except Exception as exc:
        result["inspection_warning"] = str(exc)

    return result


# ============================================================
# Single source ingestion
# ============================================================


def ingest_source(
    source_name: str,
    batch_id: str,
    load_time: str,
) -> tuple[list[dict], int]:
    """
    Ingest one source directory into Bronze.

    Returns:
        manifest_records
        number_of_files
    """
    config = SOURCE_CONFIG[source_name]

    raw_dir = config["raw_dir"]
    bronze_dir = config["bronze_dir"]

    bronze_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    source_files = discover_files(raw_dir)

    if not source_files:
        print(
            f"[WARNING] {source_name}: no supported files found."
        )

        return [], 0

    print()
    print("=" * 70)
    print(f"INGESTING {source_name}")
    print("=" * 70)

    records = []

    for source_file in source_files:
        print(
            f"  Source: {safe_relative_path(source_file, RAW_ROOT)}"
        )

        bronze_file = copy_source_file(
            source_file,
            raw_dir,
            bronze_dir,
        )

        metadata = build_metadata(
            source_name=source_name,
            source_file=source_file,
            bronze_file=bronze_file,
            batch_id=batch_id,
            load_time=load_time,
        )

        inspection = inspect_source_file(
            source_file
        )

        metadata["inspection"] = inspection

        records.append(metadata)

        print(
            f"  Bronze: {safe_relative_path(bronze_file, BRONZE_ROOT)}"
        )
        print(
            f"  Size  : {metadata['file_size_bytes']:,} bytes"
        )
        print(
            f"  SHA256: {metadata['source_checksum_sha256'][:16]}..."
        )

    return records, len(source_files)


# ============================================================
# Manifest creation
# ============================================================


def write_manifest(
    batch_id: str,
    load_time: str,
    records: list[dict],
) -> Path:
    """
    Write batch-level provenance manifest.
    """
    manifest_dir = BRONZE_ROOT / "_manifests"

    manifest_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    manifest = {
        "project": "EpiScope",
        "layer": "Bronze",
        "batch_id": batch_id,
        "source_load_time": load_time,
        "created_at_utc": utc_now(),
        "file_count": len(records),
        "sources": records,
    }

    manifest_path = (
        manifest_dir
        / f"{batch_id}.json"
    )

    write_json(
        manifest_path,
        manifest,
    )

    return manifest_path


def write_latest_manifest(
    batch_id: str,
    load_time: str,
    records: list[dict],
) -> Path:
    """
    Write a stable latest-manifest pointer.

    This is useful for development and audit inspection.
    """
    latest_path = (
        BRONZE_ROOT
        / "_manifests"
        / "latest.json"
    )

    latest = {
        "project": "EpiScope",
        "layer": "Bronze",
        "latest_batch_id": batch_id,
        "source_load_time": load_time,
        "file_count": len(records),
        "sources": records,
    }

    write_json(
        latest_path,
        latest,
    )

    return latest_path


# ============================================================
# Summary
# ============================================================


def print_summary(
    batch_id: str,
    load_time: str,
    records: list[dict],
) -> None:
    """
    Print ingestion summary.
    """
    print()
    print()
    print("=" * 70)
    print("BRONZE INGESTION COMPLETE")
    print("=" * 70)

    print(f"Batch ID        : {batch_id}")
    print(f"Source load time : {load_time}")
    print(f"Files ingested  : {len(records)}")

    source_counts = {}

    for record in records:
        source = record["source_name"]

        source_counts[source] = (
            source_counts.get(source, 0) + 1
        )

    print()
    print("Files by source:")

    for source_name in SOURCE_CONFIG:
        count = source_counts.get(
            source_name,
            0,
        )

        print(
            f"  {source_name:<20} : {count}"
        )

    print()
    print(
        f"Bronze directory: {BRONZE_ROOT}"
    )

    print()
    print(
        "Provenance metadata recorded:"
    )
    print(
        "  - source_load_time"
    )
    print(
        "  - batch_id"
    )
    print(
        "  - source_file"
    )
    print(
        "  - source_checksum_sha256"
    )

    print()
    print(
        "Bronze rule: source files were copied without modification."
    )

    print("=" * 70)


# ============================================================
# Main
# ============================================================


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "EpiScope S1-S6 Bronze layer ingestion"
        )
    )

    parser.add_argument(
        "--source",
        choices=[
            "S1_boundaries",
            "S2_population",
            "S3_weather",
            "S4_encounters",
            "S5_facilities",
            "S6_stock_beds",
            "all",
        ],
        default="all",
        help="Source to ingest. Default: all.",
    )

    args = parser.parse_args()

    print()
    print("=" * 70)
    print("EpiScope - Bronze Layer Ingestion")
    print("=" * 70)

    print(
        f"Project root: {PROJECT_ROOT}"
    )

    print(
        f"Raw root    : {RAW_ROOT}"
    )

    print(
        f"Bronze root : {BRONZE_ROOT}"
    )

    # Create Bronze root.
    BRONZE_ROOT.mkdir(
        parents=True,
        exist_ok=True,
    )

    # Generate batch-level metadata.
    batch_id = create_batch_id()
    load_time = utc_now()

    print()
    print(
        f"Batch ID        : {batch_id}"
    )

    print(
        f"Source load time : {load_time}"
    )

    # Determine sources.
    if args.source == "all":
        source_names = list(
            SOURCE_CONFIG.keys()
        )
    else:
        source_names = [
            args.source
        ]

    all_records = []

    # Ingest each source.
    for source_name in source_names:
        records, _ = ingest_source(
            source_name=source_name,
            batch_id=batch_id,
            load_time=load_time,
        )

        all_records.extend(records)

    # Write provenance manifest.
    manifest_path = write_manifest(
        batch_id=batch_id,
        load_time=load_time,
        records=all_records,
    )

    latest_path = write_latest_manifest(
        batch_id=batch_id,
        load_time=load_time,
        records=all_records,
    )

    print_summary(
        batch_id=batch_id,
        load_time=load_time,
        records=all_records,
    )

    print()
    print(
        f"Batch manifest : {manifest_path}"
    )

    print(
        f"Latest manifest: {latest_path}"
    )

    print()


if __name__ == "__main__":
    main()