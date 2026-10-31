import json
import time
from pathlib import Path

import requests
from shapely.geometry import Polygon, MultiPolygon


# ============================================================
# PATHS
# ============================================================

BASE_DIR = Path(__file__).resolve().parents[1]

S1_FILE = (
    BASE_DIR
    / "data"
    / "raw"
    / "S1_boundaries"
    / "bangladesh_districts.geojson"
)

S3_DIR = (
    BASE_DIR
    / "data"
    / "raw"
    / "S3_weather"
)

S3_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================
# NASA POWER SETTINGS
# ============================================================

NASA_POWER_URL = "https://power.larc.nasa.gov/api/temporal/daily/point"

PARAMETERS = [
    "T2M",
    "T2M_MAX",
    "T2M_MIN",
    "PRECTOTCORR",
    "RH2M",
]

START_DATE = "20210101"
END_DATE = "20251231"


# ============================================================
# READ S1 DISTRICT BOUNDARIES
# ============================================================

def get_district_centroids():
    """
    Read Bangladesh district boundaries from the DGHS/ArcGIS
    GeoJSON-like file and calculate one centroid per district.

    The downloaded file uses:
        feature["attributes"]
        feature["geometry"]["rings"]

    rather than standard:
        feature["properties"]
        feature["geometry"]["type"]
        feature["geometry"]["coordinates"]
    """

    print("Reading S1 district boundary file...")

    if not S1_FILE.exists():
        raise FileNotFoundError(
            f"S1 boundary file not found:\n{S1_FILE}"
        )

    with open(S1_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    features = data.get("features", [])

    if not features:
        raise ValueError("No features found in S1 boundary file.")

    print(f"Total features found: {len(features)}")

    districts = []

    for feature in features:

        attributes = feature.get("attributes", {})
        geometry = feature.get("geometry", {})

        district_name = attributes.get("adm2_en")

        if not district_name:
            print("WARNING: District name missing.")
            continue

        rings = geometry.get("rings")

        if not rings:
            print(f"WARNING: Geometry rings missing for {district_name}")
            continue

        polygons = []

        for ring in rings:

            if len(ring) < 4:
                continue

            # ArcGIS coordinates are:
            # [longitude, latitude]
            coords = [
                (float(point[0]), float(point[1]))
                for point in ring
                if len(point) >= 2
            ]

            if len(coords) >= 4:
                try:
                    polygon = Polygon(coords)

                    if not polygon.is_valid:
                        polygon = polygon.buffer(0)

                    if not polygon.is_empty:
                        polygons.append(polygon)

                except Exception as e:
                    print(
                        f"WARNING: Could not create polygon "
                        f"for {district_name}: {e}"
                    )

        if not polygons:
            print(
                f"WARNING: No valid polygons for {district_name}"
            )
            continue

        # Combine rings belonging to the district.
        if len(polygons) == 1:
            district_shape = polygons[0]
        else:
            district_shape = MultiPolygon(polygons)

        # Use representative_point rather than centroid so that
        # the point is guaranteed to be inside the district.
        point = district_shape.representative_point()

        districts.append(
            {
                "district_name": district_name,
                "longitude": point.x,
                "latitude": point.y,
            }
        )

    print(f"Districts successfully processed: {len(districts)}")

    return districts


# ============================================================
# DOWNLOAD WEATHER FOR ONE DISTRICT
# ============================================================

def download_weather_for_district(
    district_name,
    latitude,
    longitude,
):
    """
    Download daily NASA POWER weather data for one district.
    """

    print(
        f"Downloading weather: "
        f"{district_name} "
        f"({latitude:.4f}, {longitude:.4f})"
    )

    parameters = ",".join(PARAMETERS)

    params = {
        "parameters": parameters,
        "community": "AG",
        "longitude": longitude,
        "latitude": latitude,
        "start": START_DATE,
        "end": END_DATE,
        "format": "JSON",
    }

    response = requests.get(
        NASA_POWER_URL,
        params=params,
        timeout=120,
    )

    response.raise_for_status()

    return response.json()


# ============================================================
# SAVE WEATHER JSON
# ============================================================

def save_weather_json(
    district_name,
    weather_data,
):
    """
    Save raw NASA POWER response for one district.
    """

    safe_name = (
        district_name
        .strip()
        .replace(" ", "_")
        .replace("/", "_")
        .replace("\\", "_")
    )

    output_file = S3_DIR / f"{safe_name}.json"

    with open(
        output_file,
        "w",
        encoding="utf-8",
    ) as f:
        json.dump(
            weather_data,
            f,
            indent=2,
        )

    return output_file


# ============================================================
# MAIN
# ============================================================

def main():

    print("=" * 70)
    print("EpiScope - S3 Weather Downloader")
    print("=" * 70)

    districts = get_district_centroids()

    if len(districts) != 64:
        print(
            f"WARNING: Expected 64 districts, "
            f"but found {len(districts)}."
        )

    print()
    print(
        f"Starting weather download for "
        f"{len(districts)} districts..."
    )
    print()

    successful = 0
    failed = 0

    for index, district in enumerate(districts, start=1):

        district_name = district["district_name"]
        latitude = district["latitude"]
        longitude = district["longitude"]

        print(
            f"[{index}/{len(districts)}] "
            f"{district_name}"
        )

        try:

            weather_data = download_weather_for_district(
                district_name=district_name,
                latitude=latitude,
                longitude=longitude,
            )

            output_file = save_weather_json(
                district_name=district_name,
                weather_data=weather_data,
            )

            print(
                f"  Saved: {output_file.name}"
            )

            successful += 1

        except Exception as e:

            print(
                f"  ERROR: {district_name}: {e}"
            )

            failed += 1

        # Small delay so we don't hammer the API.
        time.sleep(0.5)

    print()
    print("=" * 70)
    print("DOWNLOAD COMPLETE")
    print("=" * 70)

    print(f"Successful: {successful}")
    print(f"Failed:     {failed}")
    print(f"Output:     {S3_DIR}")
    print("=" * 70)


if __name__ == "__main__":
    main()