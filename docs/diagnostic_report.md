# EpiScope Initial Diagnostic Report

Inspection date: 2026-10-23

This report began as a pre-implementation diagnosis and has been updated with the code changes and validation completed so far. Existing unrelated working-tree changes (`requirements.txt`, `.gitignore`, and root-level untracked entries) were left untouched. No Bronze/raw source data was changed.

## Schemas and calculation flow

- **S1 boundaries:** 64 ArcGIS features, with district/division identifiers in `attributes` (`adm2_en`, `adm2_pcode`, `adm1_en`, etc.) and polygon rings in `geometry`. The serving boundary exporter copies those source names.
- **S2 population:** 64 rows; `district_name_source`, `district_name_canonical`, division/geocodes, population and density. The current canonical spellings include `Chapainababganj` and `Moulvibazar`.
- **S3 weather:** 64 district JSON files, daily T2M/T2M_MAX/T2M_MIN/PRECTOTCORR/RH2M values for 2021-01-01 through 2025-12-31; the quality summary reports 116,864 clean rows and no quarantined rows.
- **S4 encounters:** encounter/patient/facility IDs, district, onset/report dates, age/sex, ICD-10 disease, and clinical fields. Silver adds report-delay and source-batch metadata. The current configured scale factor is 0.01 of 25 million rows (250,000).
- **S5 facilities:** 1,420 rows with `facility_id`, `district_name`, `facility_type`, `bed_capacity`, and `active`.
- **S6 snapshots:** 2,592,920 source rows with `snapshot_date`, facility/district, capacity, available/occupied beds, saline, and test-kit stock. The intended per-field constraints are nonnegative values; available and occupied beds are each bounded by capacity.
- **Gold surveillance/features:** 64 districts × 4 diseases × 262 epi-weeks = 67,072 rows. The surveillance mart derives weekly `case_count` from S4 onset dates and `incidence_per_100k = case_count / population_total * 100,000`; it fills absent district/disease/week combinations with true zero.
- **AN-4/AN-6:** 1,024 demand rows (64 districts × 4 diseases × 4 forecast horizons). Per horizon, bed point demand is `forecast_cases * 0.08`; planning requirement is `ceil(point * 1.25 * 1.10)`. Horizons are separate weekly forecasts, so a four-week cumulative total must aggregate horizons 1–4 rather than select only horizon 4.
- **AN-7/serving:** AN-7 aggregates resource demand by district and writes optimized, baseline, and comparison CSVs. Serving writes dashboard summary, choropleth, time-series, early-warning, forecast, and resource JSON under `data/gold/serving/`.

## Confirmed findings

1. **Four-week demand scope is wrong (Bug 1).** AN-7 and serving select only the maximum horizon (4). Current AN-6 totals are 47 bed point-demand units and 221 buffered planning-demand units *per horizon*; summing the four forecast weeks gives 188 point units and 884 buffered planning units. The serving summary’s current 221 therefore represents horizon 4 only, not the cumulative four-week requirement.
2. **S6 data loss (Bug 3).** Silver adds `dq_bed_balance_mismatch` when `available_beds + occupied_beds > bed_capacity`, although the source generator defines and bounds those fields independently. That single rule quarantined 2,020,397 of 2,592,920 rows. On 2025-12-31, raw S6 has 1,420 facilities across 64 districts (capacity 283,215; available 211,122; occupied 140,971), while the clean Silver partition has only 276 rows across 63 districts (capacity 44,494; available 26,555; occupied 14,916); Gazipur has no clean row.
3. **District reconciliation is incomplete (Bug 4).** S2/Gold use `Chapainababganj` and `Moulvibazar`; S1, S5, and S6 contain the source spellings `Nawabganj` and `Maulvibazar`. S4/S3 normalization exists locally in the Gold mart, but S5/S6 are joined to Gold forecasts by raw `district_name`. Missing resource joins are then coalesced to zero in AN-6.
4. **Privacy leaks derived values (Bug 5).** Serving suppresses case counts 1–4 but exports their exact `incidence_per_100k` in choropleth, time-series, and early-warning records. Time-series records have no explicit data status. The dashboard’s aggregation of “all diseases” ignores null counts, which can present a suppressed component as a numeric zero.
5. **Small positive counts are real (Bug 6).** Gold contains many values 1–4 (including 12,398 ones, 10,211 twos, 7,318 threes, and 5,530 fours). The serving suppression function emits null rather than zero, but the all-disease frontend aggregation can turn mixed null/zero rows into zero.
6. **Latest week is incomplete by the input boundary (Bug 7).** The last Gold epi-week starts 2025-12-29 while the project input cutoff is 2025-12-31; the current serving time-series schema has neither a provisional status nor reporting-completeness metadata. No completeness percentage is justified by current outputs.
7. **Map period label is missing (Bug 9).** `renderMapLegend` displays disease and case-intensity labels but not the selected week/date.
8. **Optimizer output is not currently trustworthy (Bug 2).** The checked-in runtime has no available HiGHS solver, although bundled CBC is available. Existing AN-7 output (generated 2026-10-02) allocates zero units for every resource and reports all demand as unmet; the comparison reports optimized unmet 3,607 versus baseline 1,815. The current source also treats the return value from `LpProblem.solve()` as an object (`status.status`), while PuLP 3.3.2 returns a status integer. The optimizer must be rerun with a supported backend and validated before interpreting its allocation or equity results.
9. **Black chart line is not yet reproduced (Bug 8).** The chart clears the SVG before each render and creates one set of axes/series; CSS styles `.axis` paths but not the `.grid` axis domain path. Browser inspection is still required to identify whether the reported line is a D3 domain path or a meaningful reference.

## Not yet confirmed / downstream checks

- Historical-versus-latest S6 differences require the repaired Silver output and same-date comparisons.
- S5/S6/Fcst/serving reconciliation across all 64 districts needs rerun after canonicalization.
- Suppression reconstruction risks across totals/subtotals and chart/tooltips need automated coverage.
- A detection evaluation result and scalability experiment artifact were not found in the inspected analytics output directory; verify project-wide artifacts before concluding they are absent.
- Existing forecast backtest summary reports 52 rolling origins, 38,016 evaluation rows, model MAE 2.242 versus seasonal-naive MAE 2.723, and 80% interval coverage 0.724.
- Ground-truth outbreak input exists separately; the detection implementation reads the Gold feature mart, while the evaluation implementation reads ground truth.

## Planned downstream rebuilds (after fixes)

1. S5/S6 Silver and quality outputs (no Bronze/raw overwrite).
2. AN-6 resource demand.
3. AN-7 optimized allocation, population-proportional baseline, comparison, and summary.
4. Serving JSON and manifest.
5. Targeted automated tests and static-server/browser checks.

The 250,000-row S4 source generator will not be rerun unless subsequent evidence requires it.

## Implementation and validation status

Implemented:

- Added one explicit district-name alias map and reused it in Gold S3/S4 normalization, S5/S6 Silver, AN-6 joins, and serving boundaries. Silver retains the original district spelling in `district_name_source`.
- Removed the unsupported S6 available-plus-occupied bed balance quarantine rule while retaining individual nonnegative and capacity checks.
- Made the S6 generator use a local seeded random generator and accept an optional seed.
- Changed AN-6 to canonicalize stock/facility join keys, carry snapshot and completeness status, keep missing stock as unknown, and preserve cumulative district demand reporting.
- Changed AN-7 to require all horizons 1–4, sum cumulative demand, cap national budgets by measured stock, use bundled CBC, enforce exact unmet-demand balance, bound the proportional baseline by demand, and fail on violated allocation checks.
- Changed serving and dashboard code to preserve null/missing/suppressed distinctions, suppress 1–4 counts and associated displayed metrics, expose provisional-week status, retain it in indexed time-series records, require all four horizons during serving aggregation, label the selected map period, and show cumulative resource demand and stock status.
- Added focused standard-library `unittest` regressions in `tests/test_data_pipeline_repairs.py`.

Validation completed:

- `python -m unittest discover -s tests -p test_data_pipeline_repairs.py -v`: 10 tests passed, including alias resolution, zero versus 1–4 suppression, four-horizon completeness and aggregation in both AN-7 and serving, baseline demand caps, CBC allocation budget/demand/equity checks, seeded generator repeatability using a temporary facility file, and suppression propagation.
- Python `compileall` passed for the modified Python modules and tests.
- `node --check dashboard\js\app.js` passed.
- `git diff --check` passed.
- Pylance reported only existing deprecation/unused-variable diagnostics in the changed application modules; no new type or unresolved-import error remained after replacing the unavailable pytest dependency with `unittest`.

Rebuild not completed:

- The first Spark attempt used the configured Java 8 runtime, which is incompatible with Spark 4.2.0. A Java 17 runtime was found and used for a retry.
- The retry stopped before S5 output could be rewritten: the current Windows identity (`DESKTOP-ABAAU7K\a`) has read/execute access but not modify/delete access to `data\silver\S5_facilities` (the directory is owned by `BUILTIN\Administrators`). This same read-only ACL is present on the S6 Silver and quarantine output directories. No permissions were changed.
- Consequently, S6 Silver, AN-6, AN-7, serving JSON, regenerated output reconciliation, and browser rendering have not been verified. The current generated data must not be treated as reflecting these code changes.
- Permissions were left unchanged as requested. To finish, rerun the pipeline under an authorized account with write access to the specific generated Silver, quarantine, quality, and Gold surveillance/lineage output directories, using Java 17. AN-6, AN-7, and serving output directories are writable by the current account. Rebuild order: S5/S6 Silver → Gold surveillance mart → AN-6 → AN-7 → serving. Raw/Bronze folders are inputs and must remain unchanged.
