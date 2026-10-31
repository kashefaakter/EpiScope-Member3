import json
import pandas as pd
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from analytics.optimise.resource_allocation import (
    EQUITY_FLOOR,
    aggregate_district_demand,
    optimize_resource,
    population_proportional_allocation,
    select_planning_horizon,
)
from district_names import canonical_district_name
from generator import generate_stock_beds
from serving import build_dashboard_data as serving_builder
from serving.build_dashboard_data import (
    apply_seasonal_suppression,
    build_choropleth,
    build_early_warning,
    build_resources,
    build_time_series,
    data_status_series,
    suppress_count,
)


class PipelineRepairTests(unittest.TestCase):
    def test_unmapped_district_names_are_preserved_after_stripping(self):
        self.assertEqual(
            canonical_district_name("  Example District  "),
            "Example District",
        )

    def test_source_district_names_map_to_s1_administrative_names(self):
        raw_dir = Path(__file__).resolve().parents[1] / "data" / "raw"
        s1_files = list((raw_dir / "S1_boundaries").glob("*.geojson"))
        self.assertEqual(len(s1_files), 1, "expected one S1 boundaries file")
        with s1_files[0].open(encoding="utf-8") as source:
            boundaries = json.load(source)
        s1_names = {
            str(feature["attributes"]["adm2_en"]).strip()
            for feature in boundaries["features"]
            if feature.get("attributes", {}).get("adm2_en")
        }
        self.assertEqual(len(s1_names), 64)
        self.assertEqual(
            {canonical_district_name(name) for name in s1_names},
            s1_names,
        )

        source_names = set()
        s2_files = list((raw_dir / "S2_population").glob("*.xlsx"))
        self.assertEqual(len(s2_files), 1, "expected one S2 workbook")
        s2 = pd.read_excel(
            s2_files[0],
            sheet_name="Merged_All_Table",
            usecols=["District"],
        )
        source_names.update(
            s2["District"].dropna().astype(str).str.strip()
        )

        s4_files = list((raw_dir / "S4_encounters").glob("*.parquet"))
        self.assertTrue(s4_files, "expected raw S4 encounter Parquet files")
        for path in s4_files:
            s4 = pd.read_parquet(path, columns=["district_name"])
            source_names.update(
                s4["district_name"].dropna().astype(str).str.strip()
            )

        s5_files = list((raw_dir / "S5_facilities").glob("*.csv"))
        self.assertTrue(s5_files, "expected raw S5 facility CSV files")
        for path in s5_files:
            for chunk in pd.read_csv(
                path, usecols=["district_name"], chunksize=250_000
            ):
                source_names.update(
                    chunk["district_name"]
                    .dropna()
                    .astype(str)
                    .str.strip()
                )

        s6_files = list((raw_dir / "S6_stock_beds").glob("*.csv"))
        self.assertTrue(s6_files, "expected raw S6 stock CSV files")
        for path in s6_files:
            for chunk in pd.read_csv(
                path, usecols=["district_name"], chunksize=250_000
            ):
                source_names.update(
                    chunk["district_name"]
                    .dropna()
                    .astype(str)
                    .str.strip()
                )

        s3_files = list((raw_dir / "S3_weather").glob("*.json"))
        self.assertTrue(s3_files, "expected raw S3 weather JSON files")
        source_names.update(path.stem for path in s3_files)

        unmatched = sorted(
            (name, canonical_district_name(name))
            for name in source_names
            if canonical_district_name(name) not in s1_names
        )
        self.assertEqual(
            unmatched,
            [],
            f"district spellings mapping outside S1 adm2_en names: {unmatched}",
        )

    def test_district_aliases_resolve_to_s1_names(self):
        aliases = {
            "Barishal": "Barisal",
            "Bogura": "Bogra",
            "Brahmanbaria": "Brahamanbaria",
            "Chattogram": "Chittagong",
            "Cumilla": "Comilla",
            "Cox's_Bazar": "Cox's Bazar",
            "Jashore": "Jessore",
            "Moulvibazar": "Maulvibazar",
            "Chapainababganj": "Nawabganj",
            "Chapainawabganj": "Nawabganj",
        }
        for source, expected in aliases.items():
            with self.subTest(source=source):
                self.assertEqual(
                    canonical_district_name(source), expected
                )

    def test_small_cell_suppression_preserves_zero_and_suppresses_one_through_four(self):
        self.assertEqual(
            [suppress_count(value) for value in range(6)],
            [0, None, None, None, None, 5],
        )
        self.assertEqual(
            data_status_series(pd.Series([0, 1, 4, 5, None])).tolist(),
            ["AVAILABLE", "SUPPRESSED", "SUPPRESSED", "AVAILABLE", "MISSING"],
        )

    def test_planning_horizon_requires_all_four_weeks(self):
        complete = pd.DataFrame({"forecast_horizon_week": [1, 2, 3, 4]})
        incomplete = pd.DataFrame({"forecast_horizon_week": [1, 2, 4]})
        self.assertEqual(select_planning_horizon(complete), 4)
        with self.assertRaisesRegex(ValueError, "requires forecast horizons"):
            select_planning_horizon(incomplete)

    def test_district_demand_sums_all_four_horizons_and_rejects_gaps(self):
        rows = [
            {
                "district_name": f"District {district_number}",
                "disease": "Dengue",
                "forecast_horizon_week": horizon,
                "population_total": 1000,
                "forecast_cases": 1,
                "bed_planning_requirement": horizon,
                "saline_planning_requirement": horizon * 2,
                "test_kit_planning_requirement": horizon * 3,
                "available_beds": 100,
                "saline_stock": 100,
                "test_kit_stock": 100,
                "stock_data_status": "AVAILABLE",
            }
            for district_number in range(64)
            for horizon in range(1, 5)
        ]
        result = aggregate_district_demand(pd.DataFrame(rows), 4)
        self.assertEqual(result.loc[0, "bed_demand"], 10)
        self.assertEqual(result.loc[0, "saline_demand"], 20)
        self.assertEqual(result.loc[0, "test_kit_demand"], 30)

        incomplete = pd.DataFrame(rows[:-1])
        with self.assertRaisesRegex(ValueError, "all four forecast horizons"):
            aggregate_district_demand(incomplete, 4)

    def test_population_baseline_caps_at_demand_and_redistributes(self):
        allocation = population_proportional_allocation(
            demand=[1, 10],
            population_share=[0.5, 0.5],
            budget=9,
        )
        self.assertEqual(allocation, [1, 8])

    def test_optimizer_respects_budget_demand_and_equity_floor(self):
        districts = pd.DataFrame({
            "district_name": ["A", "B", "C"],
            "population_total": [60, 30, 10],
            "bed_demand": [10, 10, 10],
        })
        optimized = optimize_resource(
            districts,
            resource_name="beds",
            demand_column="bed_demand",
            budget=15,
            weight=1,
        )
        allocations = optimized["optimized_beds"]
        total = allocations.sum()
        shares = (
            districts["population_total"]
            / districts["population_total"].sum()
        )
        self.assertLessEqual(total, 15 + 1e-6)
        self.assertTrue((allocations >= -1e-6).all())
        self.assertTrue((allocations <= districts["bed_demand"] + 1e-6).all())
        self.assertTrue(
            (allocations >= EQUITY_FLOOR * shares * total - 1e-6).all()
        )
        self.assertEqual(
            optimized["optimized_beds_unmet"].to_list(),
            list(districts["bed_demand"] - allocations),
        )

    def test_suppressed_case_rows_do_not_publish_incidence_or_alert_level(self):
        surveillance = pd.DataFrame({
            "district_name": ["A", "B"],
            "disease": ["Dengue", "Dengue"],
            "epi_week": [1, 1],
            "epi_week_start": pd.to_datetime(["2025-01-06", "2025-01-06"]),
            "case_count": [3, 5],
            "incidence_per_100k": [0.3, 0.5],
            "alert_level": ["HIGH", "LOW"],
        })
        choropleth = build_choropleth(surveillance)
        first, second = choropleth["records"]
        self.assertEqual(first["data_status"], "SUPPRESSED")
        self.assertIsNone(first["case_count"])
        self.assertIsNone(first["incidence_per_100k"])
        self.assertEqual(first["alert_level"], "N/A")
        self.assertEqual(second["case_count"], 5)
        self.assertEqual(second["alert_level"], "LOW")

        time_series = build_time_series(surveillance)
        self.assertIn("data_status", time_series["columns"])
        self.assertIn("week_status", time_series["columns"])

        detection = pd.DataFrame({
            "district_name": ["A", "B"],
            "disease": ["Dengue", "Dengue"],
            "epi_week": [1, 1],
            "epi_week_start": pd.to_datetime(["2025-01-06", "2025-01-06"]),
            "case_count": [3, 5],
            "incidence_per_100k": [0.3, 0.5],
            "alert_level": ["HIGH", "LOW"],
        })
        early_warning = build_early_warning(detection)
        self.assertEqual(len(early_warning["records"]), 1)

    def test_seasonal_index_is_suppressed_when_it_includes_a_small_cell(self):
        surveillance = pd.DataFrame({
            "district_name": ["Nawabganj", "Nawabganj"],
            "disease": ["Dengue", "Dengue"],
            "epi_week": [10, 10],
            "case_count": [3, 8],
        })
        seasonal = pd.DataFrame({
            "district_name": ["Chapainababganj"],
            "disease": ["Dengue"],
            "epi_week": [10],
            "seasonal_index": [1.25],
            "preparedness_level": ["ELEVATED"],
        })
        protected = apply_seasonal_suppression(seasonal, surveillance)
        self.assertTrue(bool(protected.loc[0, "has_small_cell"]))

    def test_serving_resource_demand_aggregates_all_four_horizons(self):
        rows = [
            {
                "district_name": f"District {district_number}",
                "disease": "Dengue",
                "forecast_horizon_week": horizon,
                "forecast_week_start": pd.Timestamp("2025-01-06")
                + pd.Timedelta(weeks=horizon - 1),
                "forecast_origin": pd.Timestamp("2024-12-30"),
                "district_bed_capacity": 100,
                "active_facility_count": 2,
                "available_beds": 50,
                "occupied_beds": 25,
                "saline_stock": 40,
                "test_kit_stock": 30,
                "stock_facility_count": 2,
                "stock_data_status": "AVAILABLE",
                "stock_snapshot_date": pd.Timestamp("2024-12-31"),
                "forecast_cases": 1,
                "bed_demand_point": horizon,
                "bed_planning_requirement": horizon,
                "saline_demand_point": horizon,
                "saline_planning_requirement": horizon,
                "test_kit_demand_point": horizon,
                "test_kit_planning_requirement": horizon,
                "bed_gap": horizon,
                "saline_gap": horizon,
                "test_kit_gap": horizon,
            }
            for district_number in range(64)
            for horizon in range(1, 5)
        ]
        comparison = pd.DataFrame({
            "district_name": [f"District {number}" for number in range(64)],
            "optimized_total_unmet": [0] * 64,
            "optimized_beds": [1] * 64,
        })
        with TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            demand_directory = root / "demand"
            demand_directory.mkdir()
            demand_path = demand_directory / "part-00000.parquet"
            pd.DataFrame(rows).to_parquet(demand_path, index=False)
            comparison_path = root / "comparison.csv"
            comparison.to_csv(comparison_path, index=False)
            with (
                patch.object(
                    serving_builder, "RESOURCE_DEMAND_PATH", demand_directory
                ),
                patch.object(
                    serving_builder, "RESOURCE_COMPARISON_PATH", comparison_path
                ),
                patch.object(
                    serving_builder,
                    "RESOURCE_ALLOCATION_SUMMARY_PATH",
                    root / "missing-summary.json",
                ),
            ):
                result = build_resources()
                self.assertEqual(result["planning_horizon_weeks"], 4)
                self.assertEqual(
                    result["records"][0]["disease_demand"][0][
                        "bed_planning_requirement"
                    ],
                    10,
                )

                incomplete_directory = root / "incomplete"
                incomplete_directory.mkdir()
                pd.DataFrame(rows[:-1]).to_parquet(
                    incomplete_directory / "part-00000.parquet",
                    index=False,
                )
                with patch.object(
                    serving_builder,
                    "RESOURCE_DEMAND_PATH",
                    incomplete_directory,
                ):
                    with self.assertRaisesRegex(
                        ValueError, "all four forecast horizons"
                    ):
                        build_resources()

    def test_stock_generator_is_reproducible_without_touching_raw_inputs(self):
        with TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            facility_path = root / "facilities.csv"
            output_directory = root / "output"
            output_directory.mkdir()
            pd.DataFrame({
                "facility_id": ["F-1"],
                "district_name": ["Dhaka"],
                "bed_capacity": [20],
            }).to_csv(facility_path, index=False)
            with (
                patch.object(generate_stock_beds, "FACILITY_FILE", facility_path),
                patch.object(generate_stock_beds, "OUTPUT_DIR", output_directory),
            ):
                generate_stock_beds.generate_stock_beds(seed=12)
                first = pd.read_csv(output_directory / "stock_beds.csv")
                generate_stock_beds.generate_stock_beds(seed=12)
                second = pd.read_csv(output_directory / "stock_beds.csv")
                generate_stock_beds.generate_stock_beds(seed=13)
                different_seed = pd.read_csv(output_directory / "stock_beds.csv")
            pd.testing.assert_frame_equal(first, second)
            self.assertFalse(first.equals(different_seed))


if __name__ == "__main__":
    unittest.main()
