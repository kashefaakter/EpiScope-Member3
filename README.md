# EpiScope — Public Health Surveillance Dashboard

EpiScope is a public-health surveillance and outbreak-monitoring project designed to combine disease, weather, population, and health-facility information for district-level analysis in Bangladesh.

## Key Features

* District-level surveillance covering 64 districts.
* Monitoring of Dengue, Influenza, and Acute Respiratory Infection (ARI).
* Epidemiological time-series organized by epidemiological week.
* Weather and population context for surveillance analysis.
* Outbreak-detection outputs, including EARS C1, C2, C3, CUSUM-style signals, and consensus alerts.
* Seasonal-pattern and preparedness views.
* Dashboard sections for observed/forecast trends and analytical summaries.

## Technology Stack

* Python
* PySpark
* Parquet datasets
* HTML, CSS, and JavaScript
* Python's built-in HTTP server for local development

Check the project's environment or requirements files for exact dependency versions.

## Data Pipeline

The project uses layered data products:

* **Silver layer:** Cleaned boundaries, population, weather, encounters, facilities, and stock/beds datasets.
* **Gold surveillance mart:** District × disease × epidemiological-week dataset.
* **Gold feature mart:** Surveillance data enriched with temporal, lag, rolling-window, incidence, and weather features.
* **Analytics outputs:** Outbreak detection, evaluation, forecasting, and seasonal-preparedness results.

Cached S3 weather inputs are downloaded from NASA POWER by `pipeline/download_weather.py`. This requires the S1 district-boundary source file and writes district-level responses under `data/raw/S3_weather/`.

### Previously reported data checks

* Silver encounters dataset: 235,295 rows.
* Gold surveillance mart: 50,304 rows, 64 districts, 3 diseases, and 262 epidemiological weeks.
* Gold feature mart: 50,304 rows and 71 feature columns.
* Dashboard loader: 3 diseases, 64 districts, 786 time-series rows, 159 heatmap rows, 159 preparedness rows, and 192 warning rows.
* The dashboard loader previously reported 225,967 total cases across its loaded data. This is an all-period total, not the current/latest-week case count.

These are results from earlier runs and may change when data or scripts are updated.

## Repository Layout

The repository is organized into the following project areas. Generated data directories are local pipeline inputs and outputs; the dashboard is served from `dashboard/`.

```text
EpiScope/
├── data/
│   ├── raw/
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   └── quarantine/
├── analytics/
│   ├── calendar/
│   ├── detection/
│   ├── forecast/
│   └── optimise/
├── config/
├── dashboard/                 # Served app: /dashboard/
│   ├── css/
│   ├── data/serving/
│   └── js/
├── docs/
├── frontend/                  # Root-page redirect to /dashboard/index.html
├── generator/
├── orchestration/
├── pipeline/
│   ├── bronze/
│   ├── gold/
│   ├── quality/
│   └── silver/
├── serving/
├── tests/
├── AI_USAGE.md
├── Makefile
├── README.md
├── docker-compose.yml
└── requirements.txt
```

## Setup

### 1\. Requirements

Install a compatible Python version and the dependencies required by the project. PySpark is used by the data pipeline. Use the project's requirements or environment file if available.

### 2\. Start the local dashboard server

From the repository root:

```bat
python -m http.server 8080
```

Open this URL in your browser:

```text
http://localhost:8080/dashboard/
```

Keep the terminal open while using the dashboard. The server exposes files under the directory from which it was started.

### 4\. Refresh after updating files

After replacing `index.html`, `css/style.css`, or `js/app.js`, perform a hard refresh:

* `Ctrl + Shift + R`, or
* `Ctrl + F5`

## Running Analytics Scripts

Run these from the project root if the scripts exist in your local checkout:

```bat
python analytics\\\\detection\\\\outbreak\\\_detection.py
python analytics\\\\forecast\\\\forecast\\\_models.py
python analytics\\\\calendar\\\\seasonal\\\_preparedness.py
```

The surveillance and feature marts are built by separate scripts. Check the repository for their exact filenames and required arguments before running them.

## Dashboard Data Notes

* The dashboard is a static HTML/CSS/JavaScript application served over HTTP.
* The browser can only fetch files exposed by the HTTP server's served directory. If JSON or other data files are outside that directory, copy them into a served folder or configure the server to serve the appropriate project root.
* If the loading screen remains visible, open Developer Tools (`F12`) and inspect the **Console** and **Network** tabs for JavaScript errors, failed requests, or incorrect paths.
* A successful data-loader log is useful, but also verify that KPI cards, charts, tables, and empty-state messages render correctly.
* The previously logged total of 225,967 is an all-period total, not a latest-week value.
* Alert counts can depend on the selected time period and the alert definition used by the frontend.

## Data Quality and Evaluation

Previously reported checks included passing Silver quality checks for encounters, facilities, and stock/beds. An earlier outbreak-evaluation run reported:

* Ground-truth outbreaks: 31
* Detected outbreaks: 17
* Missed outbreaks: 14
* Sensitivity: 54.84%
* Median detection delay: 29 days
* False alerts: 4,524
* False alerts per district-year: 14.59

These are historical run results, not a guarantee of current performance. Review the latest evaluation outputs before using them in reports or operational decisions.

## Troubleshooting

### Dashboard returns 404

* Confirm the URL matches the directory being served.
* Confirm the requested file exists at that path.
* Start the HTTP server from the intended project root.
* Check the browser Network tab to identify the missing request.

### Dashboard stays on “Loading EpiScope”

* Open Developer Tools with `F12`.
* Check Console for JavaScript errors.
* Check Network for failed data requests.
* Verify the file paths used by `js/app.js`.
* Hard-refresh after replacing the files.

### KPIs show zero or blank values

* Verify that dashboard data files are accessible to the browser.
* Compare data field names with frontend mappings, such as `case\\\_count` versus `cases`.
* Confirm the selected disease, week, and alert-level filters match available records.

### Charts or empty-state messages look incorrect

* Confirm the relevant data arrays are non-empty.
* Verify empty-state messages are hidden when data exists.
* Check that the browser is loading the expected CSS and JavaScript files rather than cached versions.

## Limitations and Responsible Use

* This is an analytical and development dashboard, not a substitute for official public-health reporting or professional epidemiological assessment.
* Historical detection metrics do not establish real-world predictive performance.
* Validate forecasts and alert signals against current data and domain expertise before operational use.
* Do not include personal or identifiable health information in public-facing dashboard outputs.

## Contributing

1. Create a branch for your change.
2. Keep data schemas and frontend field mappings synchronized.
3. Re-run relevant data-quality checks and analytics scripts.
4. Test the dashboard in a browser and inspect both Console and Network tabs.
5. Document schema changes, assumptions, and known limitations.

## License

No license information was provided during development. Add a `LICENSE` file and update this section before distributing the project.

## Project Status

The dashboard previously loaded data in a local browser and reported 4 diseases and 64 districts. Verify the current local run before describing the project as production-ready.
