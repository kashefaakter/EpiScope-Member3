# EpiScope project report facts

## Scope and pipeline-layer gate

The read-only inventory found Bronze directories `S1`–`S6` and `_manifests`, Silver directories `S1`–`S6`, and Gold directories `analytics`, `features`, `lineage`, `quality`, `serving`, `serving_backup_phase2`, and `surveillance`. `data\gold\serving` exists and contains 10 JSON files. The required Silver directories `S4_encounters`, `S5_facilities`, and `S6_stock_beds` all exist, so the requested stop condition was not triggered. Source: PowerShell `Get-ChildItem -LiteralPath 'E:\EpiScope\data\bronze'`, `...\data\silver`, `...\data\gold`, and `...\data\gold\serving`.

All facts below refer to the files and outputs found in this checkout. Output counts and file sizes were read without running pipeline rebuilds. No pipeline, generator, or evaluation script was run.

## A. Machine and Spark settings

- CPU: Intel Core i5-8365U; logical processors: 8. Source: read-only PowerShell command `Get-CimInstance Win32_Processor | Select-Object -ExpandProperty Name; (Get-CimInstance Win32_ComputerSystem).NumberOfLogicalProcessors; (Get-CimInstance Win32_OperatingSystem).TotalVisibleMemorySize`.
- Visible RAM: 8,190,240 KiB = 8,386,805,760 bytes = 8.38680576 decimal GB. Source: same PowerShell command and exact unit conversion.
- Spark runs use `local[*]` in the encountered Spark builders; no explicit `spark.executor.cores` setting was found. Source: `analytics\forecast\forecast_models.py:91-95`, `analytics\detection\outbreak_detection.py` Spark builder, and repository search for `spark.executor.cores`.
- Explicit memory settings vary by stage: S5/S6 Silver sets driver memory to `4g`; resource demand and forecast backtest set driver and executor memory to `5g`. No global memory setting was found. Sources: `pipeline\silver\silver_s5_s6.py:753-760`, `analytics\forecast\resource_demand.py:138-140`, `analytics\forecast\forecast_backtest.py:119-121`.
- Explicit shuffle partitions include 8 in S5/S6, resource demand, and forecast backtest, and 16 in AN-4 forecasting. Sources: `pipeline\silver\silver_s5_s6.py:753-756`, `analytics\forecast\resource_demand.py:140`, `analytics\forecast\forecast_backtest.py:121`, `analytics\forecast\forecast_models.py:76-77`.

## B. Pipeline stage names and files

The 12 ordered stages in `orchestration\run_pipeline.py:11-25` are:

1. `Bronze ingestion` — `pipeline/bronze/ingest_bronze.py`
2. `Silver S1-S3` — `pipeline/silver/silver_s1_s2_s3.py`
3. `Silver S4 encounters` — `pipeline/silver/silver_transform.py`
4. `Silver S5-S6 resources` — `pipeline/silver/silver_s5_s6.py`
5. `Gold surveillance mart` — `pipeline/gold/build_surveillance_mart.py`
6. `Gold feature mart` — `pipeline/gold/build_features.py`
7. `Outbreak detection` — `analytics/detection/outbreak_detection.py`
8. `Disease forecasting` — `analytics/forecast/forecast_models.py`
9. `Seasonal preparedness` — `analytics/calendar/seasonal_preparedness.py`
10. `Resource demand` — `analytics/forecast/resource_demand.py`
11. `Resource allocation` — `analytics/optimise/resource_allocation.py`
12. `Dashboard serving` — `serving/build_dashboard_data.py`

## C. Existing table rows and Parquet files

Bronze rows below are source-record counts represented by the copied Bronze inputs; the cited quality summaries record the corresponding input totals. Bronze source-file inventory found Parquet only for S4. Silver row totals come from the cited quality summaries and Silver Parquet file counts from the PowerShell inventory; Gold Parquet row totals and file counts come from the PyArrow metadata scan. Non-Parquet files are not included in Parquet file counts.

| Layer/table | Rows | Parquet files | Source |
|---|---:|---:|---|
| Bronze S1 boundaries | 64 | 0 | `data\gold\quality\s1_boundaries_summary.json:3`; Bronze inventory command |
| Bronze S2 population | 64 | 0 | `data\gold\quality\s2_population_summary.json:3`; Bronze inventory command |
| Bronze S3 weather | 116,864 | 0 | `data\gold\quality\s3_weather_summary.json:3`; Bronze inventory command |
| Bronze S4 encounters | 250,000 | 3 | `data\gold\quality\s4_encounters_summary.json:7`; Bronze Parquet inventory command |
| Bronze S4 ground-truth outbreaks | 31 | 0 | `E:\EpiScope\data\bronze\S4_encounters\ground_truth_outbreaks\ground_truth_outbreaks.csv`; venv-Python CSV count (header excluded) |
| Bronze S5 facilities | 1,420 | 0 | `data\gold\quality\s5_facilities_summary.json:4`; Bronze inventory command |
| Bronze S6 stock/beds | 2,592,920 | 0 | `data\gold\quality\s6_stock_beds_summary.json:4`; Bronze inventory command |
| Silver S1 boundaries | 64 | 1 | `data\gold\quality\s1_boundaries_summary.json:3`; Silver Parquet metadata scan |
| Silver S2 population | 64 | 1 | `data\gold\quality\s2_population_summary.json:3`; Silver Parquet metadata scan |
| Silver S3 weather | 116,864 | 64 | `data\gold\quality\s3_weather_summary.json:3`; Silver Parquet metadata scan |
| Silver S4 encounters | 235,273 | 14,608 | `data\gold\quality\s4_encounters_summary.json:8`; Silver Parquet metadata scan |
| Silver S5 facilities | 1,420 | 1 | `data\gold\quality\s5_facilities_summary.json:5`; Silver Parquet metadata scan |
| Silver S6 stock/beds | 572,523 | 14,607 | `data\gold\quality\s6_stock_beds_summary.json:5`; Silver Parquet metadata scan |
| Gold quality S1 boundaries | 1 | 1 | Gold Parquet metadata scan |
| Gold quality S2 population | 1 | 1 | Gold Parquet metadata scan |
| Gold quality S3 weather | 1 | 1 | Gold Parquet metadata scan |
| Gold quality S4 encounters | 17 | 8 | Gold Parquet metadata scan |
| Gold quality S5 facilities | 6 | 1 | Gold Parquet metadata scan |
| Gold quality S6 stock/beds | 9 | 1 | Gold Parquet metadata scan |
| Gold lineage `surveillance_mart` | 3 | 4 | Gold Parquet metadata scan |
| Gold surveillance `district_disease_epiweek` | 67,072 | 28 | `data\gold\surveillance\surveillance_mart_summary.json:7`; Gold Parquet metadata scan |
| Gold feature `district_disease_epiweek_features` | 67,072 | 96 | `data\gold\features\feature_mart_summary.json:4`; Gold Parquet metadata scan |
| Gold `analytics\outbreak_detection\outbreak_alerts` | 66,048 | 20 | `data\gold\analytics\outbreak_detection\detection_summary.json:4`; Gold Parquet metadata scan |
| Gold AN-4 forecast results | 1,024 | 16 | `data\gold\analytics\forecast\forecast_summary.json:93`; Gold Parquet metadata scan |
| Gold AN-5 rolling-origin results | 38,016 | 40 | `data\gold\analytics\forecast\backtest\backtest_summary.json:55`; Gold Parquet metadata scan |
| Gold AN-5 backtest metrics | 12 | 1 | Gold Parquet metadata scan |
| Gold `analytics\resource_demand\forecast_resource_demand` | 1,024 | 4 | `data\gold\analytics\resource_demand\resource_demand_summary.json:10`; Gold Parquet metadata scan |
| Gold `analytics\resource_allocation\allocation_comparison.csv` | 64 | 0 | Gold CSV row-count command |
| Gold `analytics\resource_allocation\optimized_allocation.csv` | 64 | 0 | Gold CSV row-count command |
| Gold `analytics\resource_allocation\population_proportional_baseline.csv` | 64 | 0 | Gold CSV row-count command |
| Gold `analytics\seasonal_preparedness\district_seasonal_calendar.csv` | 13,568 | 0 | Gold CSV row-count command |
| Gold `analytics\seasonal_preparedness\national_seasonal_calendar.csv` | 40 | 0 | Gold CSV row-count command |
| Gold `analytics\seasonal_preparedness\next_year_preparedness_calendar.csv` | 212 | 0 | Gold CSV row-count command |
| Gold `analytics\seasonal_preparedness\seasonal_weekly_profile.csv` | 212 | 0 | Gold CSV row-count command |

Silver file counts were verified with PowerShell command `$root='E:\EpiScope\data\silver'; Get-ChildItem -LiteralPath $root -Directory | ForEach-Object { $n=(Get-ChildItem -LiteralPath $_.FullName -Recurse -File -Filter '*.parquet' | Measure-Object).Count; '{0} {1}' -f $_.Name,$n }`. Gold file counts and Parquet row totals were verified with `E:\EpiScope\.venv\Scripts\python.exe` using this read-only metadata scan: `import pathlib,collections,pyarrow.parquet as pq; root=pathlib.Path(r'E:\EpiScope\data\gold'); groups=collections.defaultdict(list); files=list(root.rglob('*.parquet')); [groups[tuple(x for x in f.relative_to(root).parts[:-1] if '=' not in x)].append(f) for f in files]; [print('/'.join(k),sum(pq.read_metadata(f).num_rows for f in v),len(v)) for k,v in sorted(groups.items())`. Gold CSV counts use `E:\EpiScope\.venv\Scripts\python.exe -c "import pathlib,csv; root=pathlib.Path(r'E:\EpiScope\data\gold'); [(print(str(f.relative_to(root)),sum(1 for _ in csv.reader(f.open(encoding='utf-8-sig')))-1)) for f in sorted(root.rglob('*.csv'))]"`. Bronze file inventory used PowerShell `Get-ChildItem -LiteralPath 'E:\EpiScope\data\bronze' -Recurse -File`; the ground-truth count uses `E:\EpiScope\.venv\Scripts\python.exe -c "import pathlib,csv; p=pathlib.Path(r'E:\EpiScope\data\bronze\S4_encounters\ground_truth_outbreaks\ground_truth_outbreaks.csv'); print(sum(1 for _ in csv.reader(p.open(encoding='utf-8-sig')))-1)"`.

## D. Generator settings and generated encounters

- Configured seed `4345`, scale factor `0.01`, date range `2021-01-01` to `2025-12-31`, and district count `64`. Source: `config\generator.yaml:1-9`.
- Configured base encounter count `25,000,000`, duplicate rate `0.02`, orphan-facility rate `0.005`, invalid-age rate `0.01`, and maximum report delay `21` days. Source: `config\generator.yaml:11-17`.
- Generator code defaults match those scale/seed/rate/delay values; `MIN_OUTBREAKS` is 30. Source: `generator\generate_encounters.py:83-94`.
- Configured outbreak minimum is 30 and outbreak seed is 4345. Source: `config\generator.yaml:19-21`.
- Existing raw S4 encounter data contains 250,000 rows, 3 Parquet files; existing facility input contains 1,420 records; existing ground-truth CSV contains 31 outbreak records. Sources: S4 and S5 quality summaries cited in section C, plus read-only ground-truth CSV row-count command.
- The generator's report-delay sampler uses a Gamma distribution with shape 1.8 and scale 2.3 days, rounds and clips delay to 0–21 days, and applies weekend reporting/backfill behavior. Source: `generator\generate_encounters.py:885-957` and `1066-1171`.
- Config enables weekend effect, holiday effect, backfill, and 3 silent facility weeks. Source: `config\generator.yaml:23-27`. A holiday-effect implementation was not found in generator code; see NOT FOUND / UNVERIFIED.

## E. Data-quality rules and quarantine

Only the Silver files called by `orchestration\run_pipeline.py:13-16` are counted here. The separate `pipeline\silver\silver_s6_only.py` is not counted because the orchestrator does not call it.

- `pipeline\silver\silver_s1_s2_s3.py`: 23 rules total: S1 has 6 (`missing_district_name`, `missing_district_pcode`, `missing_division_name`, `missing_geometry`, `duplicate_district_pcode`, `duplicate_district_name`); S2 has 8 (`missing_division`, `missing_district`, `missing_district_geocode`, `invalid_household_total`, `invalid_population_total`, `invalid_population_density`, `duplicate_district_geocode`, `duplicate_district_name`); S3 has 9 (`missing_district_name`, `missing_date`, `missing_t2m`, `missing_t2m_max`, `missing_t2m_min`, `missing_precipitation`, `missing_rh2m`, `invalid_date`, `duplicate_district_date`). Sources: `pipeline\silver\silver_s1_s2_s3.py:161-168`, `500-509`, `680-690`.
- `pipeline\silver\silver_transform.py`: 17 S4 rules: `dq_missing_encounter_id`, `dq_missing_patient_id`, `dq_missing_facility_id`, `dq_orphan_facility`, `dq_invalid_age`, `dq_missing_onset_date`, `dq_missing_report_date`, `dq_report_before_onset`, `dq_report_delay_gt_21`, `dq_invalid_sex`, `dq_missing_icd10`, `dq_invalid_lab_confirmed`, `dq_invalid_admitted`, `dq_invalid_outcome`, `dq_future_onset_date`, `dq_before_project_start`, `dq_duplicate_encounter_id`. Sources: `pipeline\silver\silver_transform.py:484-708`, `690-735`; persisted summary says `dq_rule_count: 17` at `data\gold\quality\s4_encounters_summary.json:10`.
- `pipeline\silver\silver_s5_s6.py`: 14 current rules total: S5 has 6 (`dq_missing_facility_id`, `dq_missing_district`, `dq_missing_facility_type`, `dq_invalid_bed_capacity`, `dq_invalid_active`, `dq_duplicate_facility_id`); S6 has 8 (`dq_missing_snapshot_date`, `dq_missing_facility_id`, `dq_missing_district`, `dq_invalid_bed_capacity`, `dq_invalid_available_beds`, `dq_invalid_occupied_beds`, `dq_invalid_saline_stock`, `dq_invalid_test_kit_stock`). Sources: `pipeline\silver\silver_s5_s6.py:261-314`, `549-611`.

| Dataset | Quarantined rows | Latest summary file and generated time |
|---|---:|---|
| S1 | 0 | `data\gold\quality\s1_boundaries_summary.json:5,14` — 2026-11-01 |
| S2 | 0 | `data\gold\quality\s2_population_summary.json:5,16` — 2026-11-01 |
| S3 | 0 | `data\gold\quality\s3_weather_summary.json:5,17` — 2026-11-01 |
| S4 | 14,727 | `data\gold\quality\s4_encounters_summary.json:5,9-10` — 2026-10-02 |
| S5 | 0 | `data\gold\quality\s5_facilities_summary.json:3,6-7` — 2026-10-02 |
| S6 | 2,020,397 | `data\gold\quality\s6_stock_beds_summary.json:3-7` — 2026-10-02 |

**Stale S6 report:** the persisted S6 summary says there were 9 rules and records `dq_bed_balance_mismatch` as responsible for 2,020,397 quarantined rows (`data\gold\quality\s6_stock_beds_summary.json:7-18`). The current orchestrated S5/S6 code has only 8 S6 rules and does not include that rule (`pipeline\silver\silver_s5_s6.py:549-611`). Treat that saved quarantine count/summary as stale relative to current code; the existing diagnostic report also flags the S6 output as problematic (`docs\diagnostic_report.md:14-16`).

## F. District-name audit

The existing audit test was run and passed. It found **0 unmatched observed source spellings** after canonicalization against the S1 administrative-name list; the test asserts that S1 has 64 names. Source: `tests\test_data_pipeline_repairs.py:36-111` and the read-only test command `E:\EpiScope\.venv\Scripts\python.exe -m unittest ...` (12 tests passed).

Scope caveat: this test compares observed names from S2, S4, S5, S6, and weather after canonicalization. It does not assert that every one of those sources independently contains all 64 distinct districts. Source: `tests\test_data_pipeline_repairs.py:36-111`.

## G. Privacy and suppression

- Small-cell suppression threshold is 5; the serving logic suppresses counts 1–4. Source: `serving\build_dashboard_data.py:211,342-356`.
- District-level serving time series contains 67,072 district-week-disease records, of which 35,457 have null `case_count`; share is exactly `35,457/67,072`. Source: existing `data\gold\serving\time_series.json` and read-only command `E:\EpiScope\.venv\Scripts\python.exe -c "import pathlib,json; d=json.loads(pathlib.Path(r'E:\EpiScope\data\gold\serving\time_series.json').read_text(encoding='utf-8')); rows=d['records']; n=len(rows); null=sum(r[3] is None for r in rows); leaked=sum(r[3] is None and r[4] is not None for r in rows); print('rows',n,'null_case_count',null,'share_fraction',f'{null}/{n}','null_count_nonnull_incidence',leaked)"`.
- Of those records, 35,457 have null `case_count` but non-null `incidence_per_100k`; expected after the fix is 0, so existing output does not meet that expectation. Source: same command. The code suppresses `case_count` at `build_dashboard_data.py:717-719`; the derived-metric suppression helper is at lines 415-419.
- No division-level or national-level weekly cell time-series file was found in the serving directory inventory. Source: `Get-ChildItem -LiteralPath 'E:\EpiScope\data\gold\serving' -File`.
- No salted or hashed identifier anonymization method was found. Bronze ingestion computes SHA-256 checksums for source files, not for identifiers. Sources: repository search for identifier hashing/salting; `pipeline\bronze\ingest_bronze.py:93-110,200-210`.

## H. Serving files

File sizes and array-entry counts are from read-only command `E:\EpiScope\.venv\Scripts\python.exe -c "import pathlib,json; root=pathlib.Path(r'E:\EpiScope\data\gold\serving'); [(lambda d: print(f.name,f.stat().st_size,[(k,len(v)) for k,v in d.items() if isinstance(v,list)],list(d.keys())))(json.loads(f.read_text(encoding='utf-8'))) for f in sorted(root.glob('*.json'))]"`; row-like counts below identify the payload's `records`, `features`, `diseases`, or `files` array.

| File | Bytes | Payload entries |
|---|---:|---:|
| `dashboard_summary.json` | 902 | 4 diseases |
| `district_boundaries.json` | 670,484 | 64 features |
| `district_choropleth.json` | 47,539 | 256 records |
| `early_warning.json` | 2,118,681 | 11,480 records |
| `forecast.json` | 256,586 | 1,024 records |
| `manifest.json` | 2,202 | 10 file entries |
| `preparedness_calendar.json` | 762,019 | 13,568 records |
| `resource_allocation.json` | 121,372 | 64 records |
| `seasonal_heatmap.json` | 422,319 | 13,568 records |
| `time_series.json` | 2,229,641 | 67,072 records |
| **Total** | **6,631,745** | — |

Total size is 6.631745 decimal MB (total bytes divided by 1,000,000), source: read-only command `E:\EpiScope\.venv\Scripts\python.exe -c "import pathlib,json; root=pathlib.Path(r'E:\EpiScope\data\gold\serving'); fs=sorted(root.glob('*.json')); total=sum(f.stat().st_size for f in fs); print('total_bytes',total,'decimal_MB',total/1000000); print('manifest',[(k,v) for k,v in json.loads((root/'manifest.json').read_text(encoding='utf-8')).items() if k.startswith('total_') or k=='file_count'])"`. The manifest's `total_size_mb` field is 6.3245, which corresponds to a binary-MiB conversion despite the field being named MB; source: `data\gold\serving\manifest.json`.

## I. Outbreak detection

- EARS alert z threshold is 2.0; C1 compares the current observation with the previous four weeks. Source: `analytics\detection\outbreak_detection.py:66,255-310`.
- C2 baseline is rows -6 through -3 (two most recent weeks excluded from that comparison). Source: `analytics\detection\outbreak_detection.py:323-382`.
- C3 uses positive increments over a three-week window and production score threshold 6.0. Source: `analytics\detection\outbreak_detection.py:437-460`.
- CUSUM uses an eight-week baseline, `k = 0.5`, a three-week accumulated positive signal, and `h = 3.0`. Sources: `analytics\detection\outbreak_detection.py:72,491-614`.
- Operational consensus requires at least 2 detector votes; HIGH is at least 3, MEDIUM is 2, LOW is 1, and NONE is 0. Source: `analytics\detection\outbreak_detection.py:651-699`.
- The existing evaluation script uses a C3 cutoff of 2.0 rather than the production cutoff 6.0. Source: `analytics\detection\outbreak_evaluation.py:262-274`.
- No current evaluation summary/output was found. The evaluation script writes outputs and was not run under the read-only constraint. The README documents only an earlier historical run: sensitivity 54.84%, median delay 29 days, 4,524 false alerts, and 14.59 false alerts per district-year; it does not report mean delay or per-method results. Source: `README.md:138-148`.

## J. Forecast model and backtest

**AN-4 forecast output**

- Existing forecast summary identifies Spark `RandomForestRegressor`, 80 trees, depth 8, 61 features, horizons 1–4, 16 trained models, 1,024 forecast rows, and latest origin `2025-12-29`. Source: `data\gold\analytics\forecast\forecast_summary.json:1-4,9-20,59-96`.
- AN-4 features: `epi_week`, `calendar_year`, `calendar_month`, `quarter`, `month_sin`, `month_cos`, `week_sin`, `week_cos`, `population_total`, `population_density`, `temperature_mean`, `temperature_max_mean`, `temperature_min_mean`, `precipitation_sum_mm`, `humidity_mean`, `cases_lag_1`, `cases_lag_2`, `cases_lag_3`, `cases_lag_4`, `cases_lag_8`, `cases_lag_12`, `incidence_lag_1`, `incidence_lag_2`, `incidence_lag_4`, `cases_rolling_mean_4w`, `cases_rolling_std_4w`, `cases_rolling_sum_4w`, `cases_rolling_mean_8w`, `cases_rolling_std_8w`, `cases_rolling_sum_8w`, `cases_rolling_mean_12w`, `cases_rolling_std_12w`, `cases_rolling_sum_12w`, `temperature_mean_lag_1`, `temperature_mean_lag_2`, `temperature_mean_lag_4`, `temperature_mean_lag_8`, `temperature_max_mean_lag_1`, `temperature_max_mean_lag_2`, `temperature_max_mean_lag_4`, `temperature_max_mean_lag_8`, `temperature_min_mean_lag_1`, `temperature_min_mean_lag_2`, `temperature_min_mean_lag_4`, `temperature_min_mean_lag_8`, `precipitation_sum_mm_lag_1`, `precipitation_sum_mm_lag_2`, `precipitation_sum_mm_lag_4`, `precipitation_sum_mm_lag_8`, `humidity_mean_lag_1`, `humidity_mean_lag_2`, `humidity_mean_lag_4`, `humidity_mean_lag_8`, `cases_change_1w`, `cases_growth_rate_1w`, `cases_vs_4w_mean_ratio`, `cases_zscore_4w`, `cases_zscore_8w`, `admission_rate`, `lab_confirmation_rate`, `epi_year`. Source: `data\gold\analytics\forecast\forecast_summary.json:27-89`.
- AN-4 training uses the prepared supervised data with non-null targets and fits each disease/horizon pipeline on those rows; the existing forecast summary does not report a separate held-out train/backtest split. Sources: `analytics\forecast\forecast_models.py:359-432`; `forecast_summary.json:59-96`.
- This AN-4 output is a forecast run, not the existing rolling-origin backtest. The backtest output below is Elastic-Net, not Random Forest.

**AN-5 existing backtest**

- Model: Spark ML Elastic-Net Linear Regression; 24 features; 52 rolling origins from `2025-01-06` through `2025-12-29`; horizons 1–4. Baseline is seasonal naive with a 52-week lag. Sources: `data\gold\analytics\forecast\backtest\backtest_summary.json:8-38,57-62`.
- AN-5 features: `cases_lag_1`, `cases_lag_2`, `cases_lag_4`, `cases_lag_8`, `cases_lag_12`, `incidence_lag_1`, `incidence_lag_2`, `incidence_lag_4`, `cases_rolling_mean_4w`, `cases_rolling_mean_8w`, `temperature_mean_lag_1`, `temperature_mean_lag_2`, `temperature_mean_lag_4`, `precipitation_sum_mm_lag_1`, `precipitation_sum_mm_lag_2`, `precipitation_sum_mm_lag_4`, `humidity_mean_lag_1`, `humidity_mean_lag_2`, `humidity_mean_lag_4`, `month_sin`, `month_cos`, `week_sin`, `week_cos`, `cases_growth_rate_1w`. Source: `data\gold\analytics\forecast\backtest\backtest_summary.json:21-48`.
- At each origin, training rows have `target_week <= origin`; predictions use features observed at the origin for the corresponding horizon. This is a 52-origin rolling-origin backtest, not a single fixed holdout split. Sources: `analytics\forecast\forecast_backtest.py:508-590,760-870`; summary `backtest_summary.json:8-16`.
- The interval is based on the 80th percentile of absolute training residuals, using training predictions on up to 8,000 rows per origin (`training.limit(8000)`); bounds are `max(0, prediction - interval_width)` and `prediction + interval_width`. Sources: `analytics\forecast\forecast_backtest.py:635-660,690-699`; summary note at `backtest_summary.json:66-68`.
- Existing metrics are read from `data\gold\analytics\forecast\backtest\backtest_metrics` with `E:\EpiScope\.venv\Scripts\python.exe -c "import pathlib,pyarrow.parquet as pq; root=pathlib.Path(r'E:\EpiScope\data\gold\analytics\forecast\backtest\backtest_metrics'); files=sorted(root.rglob('*.parquet')); rows=[row for f in files for row in pq.read_table(f).to_pylist()]; [print(row) for row in rows]"`. Values below are stored output values; WIS is not present.

| Disease | Horizon | Elastic-Net MAE | Seasonal-naive MAE | 80% interval coverage |
|---|---:|---:|---:|---:|
| Acute Respiratory Infection | 1 | 1.7302725080158068 | 2.190563725490196 | 0.7481617647058824 |
| Acute Respiratory Infection | 4 | 1.8443816551817915 | 2.150390625 | 0.7265625 |
| Dengue | 1 | 2.6869708952115663 | 3.5300245098039214 | 0.7365196078431373 |
| Dengue | 4 | 3.1139943111933968 | 3.6735026041666665 | 0.6917317708333334 |
| Influenza | 1 | 1.9615939201801746 | 2.4276960784313726 | 0.7334558823529411 |
| Influenza | 4 | 2.1087990435542823 | 2.4293619791666665 | 0.7080078125 |

Source for each metric: read-only `E:\EpiScope\.venv\Scripts\python.exe -c` command loading existing Parquet rows from `backtest_metrics`. Cholera is not in this backtest output; the metric rows cover only the three diseases listed in its summary. RF backtest MAE and WIS relative to naive are not found.

## K. Demand conversion

- Beds: `0.08` expected bed requirement per reported case (planning proxy; the code does not define a length-of-stay parameter).
- Saline: `1.20` saline units per reported case.
- Test kits: `1.50` kits per reported case.
- Uncertainty buffer: `0.25` (25%); stock safety buffer: `0.10` (10%).

Sources: `analytics\forecast\resource_demand.py:100-123`; calculation uses at lines 637-725. No length-of-stay value was found in the demand code/config.

## L. Resource optimizer

- Solver: PuLP/CBC; objective: minimize weighted unmet demand; weights are beds 3.0, saline 1.0, test kits 2.0. Sources: `analytics\optimise\resource_allocation.py:92-94,695`; saved output `data\gold\analytics\resource_allocation\allocation_summary.json:9-17`.
- Default equity floor is alpha `0.15`; code enforces each district's allocation to be at least `alpha × population share × total allocation` for the resource. Sources: `resource_allocation.py:99-106,680-695`.
- National resource budget is `min(70% × summed planning demand, summed available stock)`, with units of available beds/bed-capacity units, saline stock units, and test kits. The LP constrains total allocation to each resource's budget, each district's allocation to its demand, and unmet demand to demand minus allocation. Sources: `resource_allocation.py:117-119,401-433,628-688`.
- Existing allocation summary lists budgets of 154.7 beds, 1040.8999999999999 saline units, and 1329.3 test kits. Source: `data\gold\analytics\resource_allocation\allocation_summary.json:5-8`.
- Existing output results are from 2026-10-02. Weighted unmet below is computed from the saved `allocation_comparison.csv` using the code's weights; coverage is read from its per-resource coverage columns.

| Existing plan | Weighted unmet | Districts below 50% coverage (beds / saline / kits) | Minimum coverage (beds / saline / kits) |
|---|---:|---:|---|
| Population-proportional baseline | 2982.6475470677233 | 27 / 26 / 26 | 0.1502133726606993 / 0.0949154190167134 / 0.09652181088754098 |
| Optimized, configured alpha 0.15 | 5948.0 | 64 / 64 / 64 | -0.0 / -0.0 / -0.0 |

Source: read-only project-venv Python aggregation over `data\gold\analytics\resource_allocation\allocation_comparison.csv`: weighted unmet is `sum(3*beds_unmet + saline_unmet + 2*kits_unmet)` using the exact optimized/baseline unmet columns; below-50 counts and minima use the corresponding coverage columns. Weights are from `resource_allocation.py:92-94`. The diagnostic report warns that this saved optimizer output allocates zero units for every resource and is not trustworthy for operational interpretation (`docs\diagnostic_report.md:22-23`). No existing outputs for alpha 0, 0.1, or 0.25 were found.

## M. Seasonal calendar

- Seasonal index is weekly mean incidence divided by the overall mean incidence; `PEAK` is index ≥ 1.50, `ELEVATED` is index ≥ 1.15 and below 1.50, otherwise `NORMAL`. Sources: `analytics\calendar\seasonal_preparedness.py:63-64,103-112,250-270`.
- Calendar periods are grouped into consecutive epiweek ranges and record start/end epiweek, seasonal index, and representative month. Source: `seasonal_preparedness.py:464-514,530-605`.
- No distinct outbreak-onset-week rule or explicit campaign dates were found; the output contains recommended actions and epiweek/month fields, not scheduled campaign dates. Source: `seasonal_preparedness.py:530-605` and repository search for campaign/onset date fields.

## N. Spark optimizations present

- Broadcast joins: `pipeline\silver\silver_transform.py:469,714`.
- Column selection/pruning: `pipeline\silver\silver_transform.py:246`; `pipeline\gold\build_surveillance_mart.py:228,608`; `pipeline\gold\build_features.py:734`.
- Repartition/persist: `pipeline\silver\silver_transform.py:1208-1210`; `analytics\forecast\forecast_backtest.py:545,853-857`.
- Partitioned Parquet writes: `pipeline\gold\build_surveillance_mart.py:829`; `analytics\forecast\forecast_backtest.py:395,443,981`; `analytics\forecast\resource_demand.py:288,880-885`.
- Cache: `analytics\forecast\resource_demand.py:1283`; `analytics\forecast\forecast_backtest.py:857,875`.

## O. Incremental Bronze ingestion

`pipeline\bronze\ingest_bronze.py` recursively discovers input files, copies them to the corresponding Bronze path, calculates a SHA-256 checksum, and records the checksum in a batch manifest. Sources: `ingest_bronze.py:93-110,120-121,155-210,304-376,387-420`.

No test for adding new files after the first load was found. The code records checksums but no content-hash duplicate-suppression mechanism was found; the existing copy operation writes to the destination path. Source: `ingest_bronze.py:155-210` and repository test search for incremental/new-file ingestion.

## P. Tests

- `pytest -q` could not run because pytest is not installed in the selected project venv (`No module named pytest`). Source: command `E:\EpiScope\.venv\Scripts\python.exe -m pytest -q` output.
- Available suite: 12 tests passed, 0 failed using `E:\EpiScope\.venv\Scripts\python.exe -m unittest discover -s E:\EpiScope\tests -q`. The district-name audit test passed as part of this suite.

## Q. Git

- `git shortlog -sn --all`: 49 commits by Kashafa Akter.
- `git rev-list --all --count`: 49 total commits.

Source: those two read-only Git commands.

## R. Streaming

No project file outside `.venv` and `site-packages` contained `readStream` or `writeStream`. Source: read-only repository search `rg -n "(readStream|writeStream)"` excluding `.venv` and `site-packages`; no matches.

## S. README requirements

No formal requirement IDs were found in README, so there are no IDs to mark done/partly/not done based on code or tests. Source: `README.md` and repository search for requirement-ID patterns.

## NOT FOUND / UNVERIFIED

- `pytest -q` pass/fail test result: pytest unavailable in the selected venv; unittest alternative passed 12 tests.
- Current outbreak-evaluation result: evaluation was not run because the script writes output; no current evaluation output was found in the Gold analytics inventory. Requested mean delay and per-method sensitivity/false-alert metrics are unavailable. README has only historical median-delay results. Sources: `analytics\detection\outbreak_evaluation.py` output-writing code; `README.md:138-148`; Gold analytics directory inventory.
- AN-4 Random Forest backtest metrics, WIS, and WIS relative to naive: not found. Existing backtest summary identifies Elastic-Net and the Parquet metric schema has no WIS field. Sources: `data\gold\analytics\forecast\backtest\backtest_summary.json:17-52`; `backtest_metrics` Parquet schema read command in section J.
- AN-4 held-out train/backtest split: not reported in its forecast summary; only the forecast training flow and the separate AN-5 rolling-origin split are available. Sources: `analytics\forecast\forecast_models.py:359-432`; `data\gold\analytics\forecast\forecast_summary.json:1-96`.
- Division-level and national-level weekly suppression-cell shares: no corresponding serving files found. Source: `Get-ChildItem -LiteralPath 'E:\EpiScope\data\gold\serving' -File`; the inventory is listed in section H.
- Identifier salting/hash method: none found; SHA-256 is only used for source-file checksums. Sources: repository search for identifier hashing/salting; `pipeline\bronze\ingest_bronze.py:93-110,200-210`.
- Length of stay (LOS) in demand conversion: not found. Source: repository search for `length.of.stay|LOS` in demand/config code; demand constants at `analytics\forecast\resource_demand.py:100-123`.
- Optimizer results for alpha 0, 0.1, and 0.25, and a selectable alpha-value range: no saved outputs or supported range found; the configured hard-coded default is 0.15. Sources: `analytics\optimise\resource_allocation.py:92-119`; saved output inventory under `data\gold\analytics\resource_allocation`.
- Explicit campaign dates and a distinct onset-week rule: not found. Source: repository search for campaign/onset date fields in `analytics\calendar\seasonal_preparedness.py`; calendar output fields at `seasonal_preparedness.py:530-605`.
- Holiday-effect implementation: config sets `holiday_effect: true`, but generator implementation was not found. Sources: `config\generator.yaml:23-27`; repository search for `holiday_effect` in generator code.
- Explicit Spark executor-core count: not found; Spark builders use `local[*]`. Sources: repository search for `spark.executor.cores`; Spark builder at `analytics\forecast\forecast_models.py:91-95`.
- Incremental new-file-after-first-load test: not found. Source: repository test search for incremental/new-file ingestion; existing ingestion implementation at `pipeline\bronze\ingest_bronze.py:120-210`.
- README requirement IDs and verifiable per-ID statuses: not found. Source: README review and repository search for requirement-ID patterns.
- Name-audit proof that every source independently contains all 64 districts: not asserted by the existing audit test. Source: `tests\test_data_pipeline_repairs.py:36-111`.
