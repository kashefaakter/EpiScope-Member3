/* EpiScope Dashboard
   Professional D3.js front-end.
   Reads only aggregated Gold/Serving outputs.
*/
const DATA_BASE = "/data/gold/serving/";
const DATA_FILES = {
  summary: "dashboard_summary.json",
  choropleth: "district_choropleth.json",
  timeSeries: "time_series.json",
  heatmap: "seasonal_heatmap.json",
  earlyWarning: "early_warning.json",
  preparedness: "preparedness_calendar.json",
  forecasts: "forecast.json",
  resources: "resource_allocation.json",
  boundaries: "district_boundaries.json",
  manifest: "manifest.json"
};
const MAP_CLICK_DELAY_MS = 200;
let mapClickTimer = null;

const state = {
  summary: null, choropleth: [], timeSeries: [], heatmap: [], earlyWarning: [],
  preparedness: [], forecasts: [], resources: [], boundaries: [],
  divisionByDistrict: new Map(), resourceMeta: {}, loadWarnings: [],
  disease: "__all__", division: "__all__", district: "__all__",
  week: "__latest__", alert: "__all__", startDate: "", endDate: ""
};

const $ = (id) => document.getElementById(id);
const fmt = d3.format(",.0f");
const fmt1 = d3.format(",.1f");

document.addEventListener("DOMContentLoaded", () => {
  init().catch(err => {
    console.error("EpiScope dashboard initialization failed:", err);
    setLoading(false);
    hideLoadingScreen();
    $("systemStatus").textContent = "Dashboard error";
    $("systemStatusDetail").textContent = "Serving data could not be rendered";
    $("systemStatusDot").classList.add("error");
    const target = document.querySelector("#appError, .app-error");
    if (target) {
      target.textContent = "Dashboard data could not be rendered. Check the browser console for details.";
      target.classList.remove("hidden");
    }
  });
});

function hideLoadingScreen() {
  const selectors = [
    "#loadingScreen", "#loading-screen", "#loader", "#loading",
    ".loading-screen", ".loading-overlay", ".loader-screen", ".app-loader",
    ".loading-container"
  ];
  selectors.forEach(sel => {
    document.querySelectorAll(sel).forEach(el => {
      el.classList.add("hidden");
      el.style.display = "none";
      el.setAttribute("aria-hidden", "true");
    });
  });
}

function setLoading(isLoading) {
  const overlay = $("dashboardLoading");
  if (!overlay) return;
  overlay.classList.toggle("hidden", !isLoading);
  overlay.setAttribute("aria-hidden", isLoading ? "false" : "true");
  if (isLoading) {
    $("systemStatus").textContent = "Loading surveillance data";
    $("systemStatusDetail").textContent = "Validating serving JSON";
  }
}

async function init() {
  setLoading(true);
  bindUI();
  const loadedEntries = await Promise.all(Object.entries(DATA_FILES).map(async ([key, file]) => {
    try {
      return [key, await fetchServingJSON(file)];
    } catch (error) {
      if (key === "forecasts") console.error(`Could not load ${file}`, error);
      else console.warn(`Could not load ${file}`, error);
      return [key, null];
    }
  }));
  const loaded = Object.fromEntries(loadedEntries);
  state.loadWarnings = Object.entries(loaded)
    .filter(([, payload]) => payload === null)
    .map(([key]) => DATA_FILES[key]);

  state.summary = loaded.summary || {};
  state.choropleth = recordsFromPayload(loaded.choropleth).map(normalizeRecord);
  state.timeSeries = recordsFromPayload(loaded.timeSeries).map(normalizeRecord);
  state.heatmap = recordsFromPayload(loaded.heatmap).map(normalizeRecord);
  state.earlyWarning = recordsFromPayload(loaded.earlyWarning).map(normalizeRecord);
  state.preparedness = recordsFromPayload(loaded.preparedness).map(normalizeRecord);
  state.forecasts = normalizeArray(loaded.forecasts).map(normalizeRecord);
  state.resources = normalizeArray(loaded.resources).map(normalizeRecord);
  state.boundaries = Array.isArray(loaded.boundaries?.features) ? loaded.boundaries.features : [];
  state.resourceMeta = loaded.resources?.allocation_summary || {};
  state.divisionByDistrict = new Map(state.boundaries.map(feature => [
    normalize(featureName(feature)), featureDivision(feature)
  ]));

  hydrateFilters();
  renderAll();
  setUpdated(loaded.manifest || state.summary);
  setLoading(false);
  hideLoadingScreen();
  const loadedCount = Object.values(loaded).filter(payload => payload !== null).length;
  $("systemStatus").textContent = state.loadWarnings.length ? "Serving data incomplete" : "Serving data ready";
  $("systemStatusDetail").textContent = `${loadedCount}/${Object.keys(DATA_FILES).length} JSON files validated`;
  $("systemStatusDot").classList.toggle("error", state.loadWarnings.length > 0);
  if (state.loadWarnings.length) showDataError(`Unable to load serving data: ${state.loadWarnings.join(", ")}`);
  else clearDataError();
}

async function fetchServingJSON(file) {
  const url = `${DATA_BASE}${encodeURIComponent(file)}`;
  const res = await fetch(url, { cache: "no-store" });
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}: ${url}`);
  if (!res.headers.get("content-type")?.includes("application/json")) {
    throw new Error(`Expected JSON response: ${url}`);
  }
  return res.json();
}

function showDataError(message) {
  const target = $("appError");
  if (!target) return;
  target.textContent = message;
  target.classList.remove("hidden");
}

function clearDataError() {
  $("appError")?.classList.add("hidden");
}

function normalizeRecord(r) {
  if (!r || typeof r !== "object") return r;
  const out = {...r};
  if (out.disease == null) out.disease = out.disease_name ?? out.condition ?? out.pathogen;
  if (out.district_name == null) out.district_name = out.district ?? out.district_name_canonical ?? out.name;
  if (out.case_count == null) out.case_count = out.cases ?? out.observed_cases ?? out.total_cases ?? out.count;
  if (out.epi_week_start == null) out.epi_week_start = out.week_start ?? out.date ?? out.week;
  if (out.alert_level == null) out.alert_level = out.signal ?? out.consensus_level ?? out.priority_level;
  return out;
}

function normalizeArray(payload) {
  if (!payload) return [];
  if (Array.isArray(payload)) return payload;
  for (const key of ["data","rows","records","items","results","districts","alerts","calendar","features"]) {
    if (Array.isArray(payload[key])) return payload[key];
  }
  for (const key of ["result","payload","output","dataset"]) {
    if (payload[key] && typeof payload[key] === "object") {
      const nested = normalizeArray(payload[key]);
      if (nested.length) return nested;
    }
  }
  return [];
}

function recordsFromPayload(payload) {
  if (payload?.encoding !== "indexed-records-v1") {
    return normalizeArray(payload);
  }
  if (!Array.isArray(payload.columns) || !Array.isArray(payload.records)) {
    throw new Error("Indexed serving data is missing columns or records.");
  }
  const dictionaries = payload.dictionaries || {};
  return payload.records.map(values => Object.fromEntries(
    payload.columns.map((column, index) => {
      let value = values[index];
      const dictionary = dictionaries[column];
      if (dictionary && value !== null && value !== undefined) {
        value = dictionary[value];
      }
      return [column, value];
    })
  ));
}

function setEmptyState(id, show) {
  const el = $(id);
  if (!el) return;
  el.classList.toggle("hidden", !show);
  el.style.display = show ? "flex" : "none";
  el.setAttribute("aria-hidden", show ? "false" : "true");
}

function bindUI() {
  $("diseaseSelect").addEventListener("change", e => { state.disease = e.target.value; hydrateDistrictFilter(); renderAll(); });
  $("divisionSelect").addEventListener("change", e => setSelection({district:"__all__", division:e.target.value}));
  $("districtSelect").addEventListener("change", e => setSelection({district:e.target.value}));
  $("weekSelect").addEventListener("change", e => { state.week = e.target.value; renderAll(); });
  $("alertSelect").addEventListener("change", e => { state.alert = e.target.value; renderAll(); });
  $("startDate").addEventListener("change", e => { state.startDate = e.target.value; renderAll(); });
  $("endDate").addEventListener("change", e => { state.endDate = e.target.value; renderAll(); });
  document.querySelector(".map-panel").addEventListener("pointerleave", () => hideTip("#mapTooltip"));
  $("resetBtn").addEventListener("click", () => {
    clearTimeout(mapClickTimer);
    mapClickTimer = null;
    state.disease = "__all__";
    state.week = "__latest__"; state.alert = "__all__"; state.startDate = ""; state.endDate = "";
    $("diseaseSelect").value = state.disease; $("weekSelect").value = state.week;
    $("alertSelect").value = state.alert; $("startDate").value = ""; $("endDate").value = "";
    setSelection({district:"__all__", division:"__all__"});
    showToast("Dashboard filters reset");
  });
  document.querySelectorAll(".nav-item").forEach(btn => btn.addEventListener("click", () => {
    document.querySelectorAll(".nav-item").forEach(x => x.classList.remove("active"));
    btn.classList.add("active");
    const target = document.getElementById(btn.dataset.scroll);
    if (target) target.scrollIntoView({behavior:"smooth", block:"start"});
    $("sidebar").classList.remove("open");
  }));
  $("menuToggle")?.addEventListener("click", () => $("sidebar").classList.toggle("open"));
}

function hydrateFilters() {
  const diseases = unique([
    ...state.timeSeries.map(row => diseaseOf(row)),
    ...state.heatmap.map(row => diseaseOf(row)),
    ...state.earlyWarning.map(row => diseaseOf(row)),
    ...state.choropleth.map(row => diseaseOf(row))
  ]).filter(Boolean).sort();

  const diseaseSelect = $("diseaseSelect");
  diseaseSelect.innerHTML = `<option value="__all__">All diseases</option>` +
    diseases.map(d => `<option value="${esc(d)}">${esc(d)}</option>`).join("");
  diseaseSelect.value = state.disease;

  const divisions = unique(state.timeSeries.map(divisionOf)).filter(Boolean).sort();
  $("divisionSelect").innerHTML = `<option value="__all__">All divisions</option>` +
    divisions.map(d => `<option value="${esc(d)}">${esc(d)}</option>`).join("");
  $("divisionSelect").value = state.division;
  hydrateDistrictFilter();

  const weeks = unique([
    ...state.timeSeries.map(row => weekOf(row)),
    ...state.earlyWarning.map(row => weekOf(row)),
    ...state.choropleth.map(row => weekOf(row))
  ]).filter(Boolean).sort(compareDatesOrText);

  $("weekSelect").innerHTML = `<option value="__latest__">Latest available</option>` +
    weeks.map(w => `<option value="${esc(w)}">${esc(formatWeek(w))}</option>`).join("");
  $("weekSelect").value = state.week;
}

function hydrateDistrictFilter() {
  const districts = unique(state.timeSeries
    .filter(row => state.division === "__all__" || normalize(divisionOf(row)) === normalize(state.division))
    .map(districtOf).filter(Boolean)
    .concat(state.district !== "__all__" ? [state.district] : [])).sort();
  $("districtSelect").innerHTML = `<option value="__all__">All districts</option>` +
    districts.map(d => `<option value="${esc(d)}">${esc(d)}</option>`).join("");
  $("districtSelect").value = state.district;
}

function renderAll() {
  // Always clear stale empty/error overlays before rendering.
  setEmptyState("mapEmpty", false);
  setEmptyState("timeSeriesEmpty", false);
  setEmptyState("heatmapEmpty", false);
  renderKPIs();
  renderMap();
  renderTimeSeries();
  renderHeatmap();
  renderWarningTable();
  renderResources();
  renderCalendar();
  if (state.startDate && state.endDate && state.startDate > state.endDate) {
    showDataError("Start date must be on or before end date.");
  } else if (state.loadWarnings.length) {
    showDataError(`Some serving data could not be loaded: ${state.loadWarnings.join(", ")}`);
  } else {
    clearDataError();
  }
}

function filteredRows(rows, options={}) {
  return rows.filter(r => {
    const disease = diseaseOf(r);
    if (state.disease !== "__all__" && disease !== state.disease) return false;
    if (state.division !== "__all__" && normalize(divisionOf(r)) !== normalize(state.division)) return false;
    if (state.district !== "__all__" && normalize(districtOf(r)) !== normalize(state.district)) return false;
    if (state.week !== "__latest__" && options.week !== false && !matchesWeek(r, state.week)) return false;
    if (options.alert !== false && state.alert !== "__all__") {
      const a = alertOf(r);
      if (a !== state.alert) return false;
    }
    if (!matchesDateRange(r, options.seasonal === true)) return false;
    return true;
  });
}

function filteredWarningRows() {
  let rows = filteredRows(state.earlyWarning, {alert:true});
  const latest = state.week === "__latest__"
    ? latestWeek(filteredRows(state.timeSeries, {alert:false, week:false}))
    : state.week;
  if (state.week === "__latest__" && latest) rows = rows.filter(row => weekOf(row) === latest);
  return rows;
}

function latestWeek(rows) {
  const values = rows.map(weekOf).filter(Boolean).sort(compareDatesOrText);
  return values.length ? values[values.length - 1] : null;
}

function matchesWeek(row, selectedDate) {
  const date = row.epi_week_start;
  if (isCalendarDate(date)) return date === selectedDate;
  const rowWeek = number(row.epi_week ?? date);
  return rowWeek == null || rowWeek === isoWeekNumber(selectedDate);
}

function matchesDateRange(row, seasonal=false) {
  if (!state.startDate && !state.endDate) return true;
  const date = row.epi_week_start ?? row.forecast_date ?? row.planning_week_start ?? row.forecast_origin;
  if (isCalendarDate(date)) {
    return (!state.startDate || date >= state.startDate) && (!state.endDate || date <= state.endDate);
  }
  if (seasonal) {
    const epiWeek = number(row.epi_week ?? date);
    if (epiWeek == null) return false;
    const startWeek = state.startDate ? isoWeekNumber(state.startDate) : 1;
    const endWeek = state.endDate ? isoWeekNumber(state.endDate) : 53;
    return startWeek <= endWeek
      ? epiWeek >= startWeek && epiWeek <= endWeek
      : epiWeek >= startWeek || epiWeek <= endWeek;
  }
  return false;
}

function isCalendarDate(value) {
  return typeof value === "string" && /^\d{4}-\d{2}-\d{2}$/.test(value);
}

function isoWeekNumber(value) {
  const date = new Date(`${value}T00:00:00Z`);
  if (Number.isNaN(date.getTime())) return null;
  date.setUTCDate(date.getUTCDate() + 4 - (date.getUTCDay() || 7));
  const yearStart = new Date(Date.UTC(date.getUTCFullYear(), 0, 1));
  return Math.ceil((((date - yearStart) / 86400000) + 1) / 7);
}

function renderKPIs() {
  const rows = filteredRows(state.timeSeries);
  const districts = unique(rows.map(districtOf).filter(Boolean));
  const diseases = unique(rows.map(diseaseOf).filter(Boolean));
  const defaultScope = state.disease === "__all__" && state.division === "__all__" &&
    state.district === "__all__" && state.week === "__latest__" && state.alert === "__all__" &&
    !state.startDate && !state.endDate;
  const visibleCaseValues = rows.map(row => number(row.case_count)).filter(Number.isFinite);
  const hasSuppressedCases = rows.some(row => row.data_status === "SUPPRESSED");
  let cases = hasSuppressedCases
    ? null
    : visibleCaseValues.length ? sum(visibleCaseValues) : null;
  if (defaultScope) cases = summaryValue("total_aggregated_cases") ?? cases;
  else if (state.disease !== "__all__" && state.division === "__all__" && state.district === "__all__" &&
    state.week === "__latest__" && state.alert === "__all__" && !state.startDate && !state.endDate) {
    cases = number(state.summary?.disease_case_summary?.[state.disease]) ?? cases;
  }

  let alertRows = filteredRows(state.earlyWarning, {alert:true});
  const latest = state.week === "__latest__" ? latestWeek(rows) : state.week;
  if (state.week === "__latest__" && latest) alertRows = alertRows.filter(row => weekOf(row) === latest);
  const activeAlerts = alertRows.filter(r => ["HIGH", "MEDIUM", "LOW"].includes(alertOf(r)));
  const high = activeAlerts.filter(r => alertOf(r) === "HIGH").length;

  const summaryDistricts = summaryValue("district_count");
  const summaryDiseases = summaryValue("disease_count");
  $("kpiDistricts").textContent = districts.length
    ? fmt(districts.length)
    : summaryDistricts == null ? "N/A" : fmt(summaryDistricts);
  $("kpiDiseases").textContent = diseases.length
    ? fmt(diseases.length)
    : summaryDiseases == null ? "N/A" : fmt(summaryDiseases);
  $("kpiCases").textContent = cases == null ? "N/A" : fmt(cases);
  const warningUnavailable = state.loadWarnings.includes(DATA_FILES.earlyWarning);
  $("kpiAlerts").textContent = warningUnavailable ? "N/A" : fmt(activeAlerts.length);
  $("kpiHigh").textContent = warningUnavailable ? "N/A" : fmt(high);
  $("casePeriod").textContent = defaultScope ? "All available weeks" : latest ? formatWeek(latest) : "Selected range";
}

function renderMap() {
  const svg = d3.select("#districtMap");
  svg.selectAll("*").remove();
  const wrap = document.querySelector(".map-wrap");
  const width = wrap.clientWidth, height = wrap.clientHeight;
  svg.attr("viewBox", `0 0 ${width} ${height}`);

  const hasHistoricMapFilter = state.week !== "__latest__" || state.startDate || state.endDate;
  const mapSource = hasHistoricMapFilter ? state.timeSeries : state.choropleth;
  const rows = filteredRows(mapSource, {alert:false});
  const latest = state.week === "__latest__"
    ? latestWeek(filteredRows(state.timeSeries, {alert:false, week:false}))
    : state.week;
  const selectedRows = latest ? rows.filter(r => weekOf(r) === latest) : rows;
  const warningRows = filteredWarningRows();
  const alertByDistrict = d3.rollup(warningRows, records => records
    .slice()
    .sort((a,b) => severity(alertOf(b))-severity(alertOf(a)))[0],
  r => normalize(districtOf(r)));
  const data = Array.from(d3.rollup(selectedRows, records => {
    const counts = records.map(r => number(r.case_count)).filter(Number.isFinite);
    const hasSuppressed = records.some(r => r.data_status === "SUPPRESSED");
    const hasMissing = records.some(r => r.data_status === "MISSING");
    const alertRecord = alertByDistrict.get(normalize(districtOf(records[0])));
    const dataStatus = hasSuppressed
      ? "SUPPRESSED"
      : hasMissing || !counts.length ? "MISSING" : "AVAILABLE";
    return {
      district_name: districtOf(records[0]),
      disease: state.disease === "__all__" ? "All diseases" : state.disease,
      epi_week_start: weekOf(records[0]),
      week_status: records.some(r => r.week_status === "PROVISIONAL") ? "PROVISIONAL" : "OBSERVED",
      case_count: dataStatus === "AVAILABLE" ? sum(counts) : null,
      incidence_per_100k: dataStatus === "AVAILABLE"
        ? d3.mean(records, r => number(r.incidence_per_100k))
        : null,
      alert_level: alertRecord ? alertOf(alertRecord) : "NONE",
      alert_disease: alertRecord ? diseaseOf(alertRecord) : null,
      detection_method: alertRecord?.detection_method || "N/A",
      alert_week: alertRecord ? weekOf(alertRecord) : weekOf(records[0]),
      data_status: dataStatus
    };
  }, r => normalize(districtOf(r))).values())
    .filter(row => state.alert === "__all__" || row.alert_level === state.alert);

  const values = data.map(caseOf).filter(Number.isFinite);
  const max = d3.max(values) || 1;
  const color = d3.scaleSequential(d3.interpolateBlues).domain([0, max]);
  const mapWeek = selectedRows.length ? weekOf(selectedRows[0]) : latest;
  const mapWeekStatus = selectedRows.some(row => row.week_status === "PROVISIONAL")
    ? "PROVISIONAL"
    : "OBSERVED";
  renderMapLegend(max, color, mapWeek, mapWeekStatus);

  const geometryFeatures = state.boundaries;
  const districtSelection = state.district;
  const divisionSelection = state.division;
  const selectedDistrictName = districtSelection !== "__all__" ? districtSelection : "";
  const selectedDivisionName = divisionSelection !== "__all__" ? divisionSelection : "";
  const hasSelection = Boolean(selectedDistrictName || selectedDivisionName);
  let selectionLayer;
  let path;
  let lookup;
  let fillForFeature;
  if (geometryFeatures.length) {
    const geo = {type:"FeatureCollection", features: geometryFeatures};
    const projection = d3.geoMercator().fitExtent([[18,18],[width-18,height-18]], geo);
    path = d3.geoPath(projection);
    lookup = new Map(data.map(r => [normalize(districtOf(r)), r]));
    fillForFeature = feature => {
      const row = lookup.get(normalize(featureName(feature)));
      return row && row.data_status === "AVAILABLE" ? color(caseOf(row)) : "#edf4f5";
    };
    svg.selectAll("path.district").data(geometryFeatures).join("path")
      .attr("class",f => {
        const row = lookup.get(normalize(featureName(f)));
        return `district alert-${(row?.alert_level || "none").toLowerCase()}`;
      })
      .attr("d",path)
      .attr("fill",fillForFeature)
      .attr("tabindex",0)
      .attr("role","button")
      .attr("aria-label",f=>`Select ${featureName(f)} district`)
      .on("mouseenter", function(event,f) {
        const row = lookup.get(normalize(featureName(f)));
        showMapTip(event, row || {district_name:featureName(f), case_count:null, data_status:"N/A"});
      })
      .on("mousemove", event => moveTip(event, "#mapTooltip"))
      .on("mouseleave", () => hideTip("#mapTooltip"))
      .on("click", (event,f) => {
        const name = featureName(f);
        if (!name || event.detail === 0) return;
        if (event.detail === 2) {
          clearTimeout(mapClickTimer);
          mapClickTimer = null;
          const division = state.divisionByDistrict.get(normalize(name)) || featureDivision(f);
          setSelection({district:"__all__", division:division || "__all__"});
          return;
        }
        clearTimeout(mapClickTimer);
        mapClickTimer = window.setTimeout(() => {
          mapClickTimer = null;
          selectDistrict(name);
        }, MAP_CLICK_DELAY_MS);
      })
      .on("keydown", (event,f) => {
        if (event.key !== "Enter" && event.key !== " ") return;
        event.preventDefault();
        clearTimeout(mapClickTimer);
        mapClickTimer = null;
        selectDistrict(featureName(f));
      });
    $("mapEmpty").classList.add("hidden");
  } else {
    renderDistrictGrid(svg, data, width, height, color);
  }
  selectionLayer = svg.append("g").attr("class","selection-layer").attr("pointer-events","none");
  if (geometryFeatures.length) {
    const districtFeature = selectedDistrictName
      ? geometryFeatures.find(feature => normalize(featureName(feature)) === normalize(selectedDistrictName))
      : null;
    const divisionFeatures = selectedDivisionName
      ? geometryFeatures.filter(feature =>
        normalize(state.divisionByDistrict.get(normalize(featureName(feature))) || "") === normalize(selectedDivisionName))
      : [];

    if (selectedDistrictName && !districtFeature) {
      console.warn(`Selected district "${selectedDistrictName}" does not match a map feature.`);
    }
    if (selectedDivisionName && !divisionFeatures.length) {
      console.warn(`Selected division "${selectedDivisionName}" does not match a map feature.`);
    }

    if (divisionFeatures.length) {
      const divisionWidth = districtFeature ? 2.5 : 3;
      const divisionLayer = selectionLayer.append("g").attr("class","division-selection");
      divisionLayer.selectAll(".division-halo").data(divisionFeatures).join("path")
        .attr("class","selection-halo division-halo")
        .attr("d",path).attr("stroke-width",12);
      divisionLayer.selectAll(".division-outline").data(divisionFeatures).join("path")
        .attr("class","selection-outline division-outline")
        .attr("d",path).attr("stroke-width",divisionWidth * 2);
      divisionLayer.selectAll(".division-cover").data(divisionFeatures).join("path")
        .attr("class","division-cover")
        .attr("d",path).attr("fill",fillForFeature)
        .attr("stroke","#f8fbfc").attr("stroke-width",1.1);
    }

    if (districtFeature) {
      const districtLayer = selectionLayer.append("g").attr("class","district-selection");
      districtLayer.append("path").datum(districtFeature)
        .attr("class","selection-halo").attr("d",path).attr("stroke-width",6);
      districtLayer.append("path").datum(districtFeature)
        .attr("class","selection-outline").attr("d",path).attr("stroke-width",3.5);
    }

    const labelFeatures = districtFeature ? [districtFeature] : divisionFeatures;
    if (labelFeatures.length) {
      let centroidFeature = labelFeatures.length === 1 ? labelFeatures[0] : {
        type:"FeatureCollection", features:labelFeatures
      };
      if (districtFeature?.geometry?.type === "MultiPolygon") {
        const largestPolygon = districtFeature.geometry.coordinates
          .map(coordinates => ({type:"Polygon", coordinates}))
          .sort((a,b) => path.area(b) - path.area(a))[0];
        centroidFeature = {type:"Feature", properties:{}, geometry:largestPolygon};
      }
      const [x,y] = path.centroid(centroidFeature);
      if (Number.isFinite(x) && Number.isFinite(y)) {
        selectionLayer.append("text").attr("class","map-label selection-label")
          .attr("x",x).attr("y",y).attr("dy","0.35em")
          .text(selectedDistrictName || selectedDivisionName);
      }
    }
  } else if (hasSelection) {
    if (selectedDistrictName) {
      console.warn(`Selected district "${selectedDistrictName}" does not match a map feature.`);
    }
    if (selectedDivisionName) {
      console.warn(`Selected division "${selectedDivisionName}" does not match a map feature.`);
    }
  }
  const accessibleSelection = selectedDistrictName || selectedDivisionName;
  svg.attr("aria-label", accessibleSelection
    ? `Selected: ${accessibleSelection}`
    : "District surveillance map");
  $("selectedDistrict").textContent = state.district && state.district !== "__all__"
    ? state.district
    : state.division !== "__all__" ? state.division : "All districts";
}

function renderMapLegend(maxCases, color, week, weekStatus) {
  const period = week ? ` — week starting ${week}` : " — week unavailable";
  const provisional = weekStatus === "PROVISIONAL" ? " · provisional" : "";
  $("mapStatus").textContent = `District case intensity${state.disease === "__all__" ? " (all diseases)" : ` (${state.disease})`}${period}${provisional}`;
  $("mapLegendMin").textContent = "0";
  const midLabel = fmt(Math.round(maxCases / 2));
  const maxLabel = fmt(Math.round(maxCases));
  $("mapLegendMid").textContent = midLabel;
  $("mapLegendMid").classList.toggle("hidden", midLabel === $("mapLegendMin").textContent || midLabel === maxLabel);
  $("mapLegendMax").textContent = maxLabel;
  d3.selectAll(".map-legend-ramp i").style("background-color", (_, index) => color(maxCases * index / 4));
  d3.select(".map-legend").selectAll(".map-selection-key")
    .data(state.district !== "__all__" || state.division !== "__all__" ? [null] : [])
    .join(
      enter => enter.append("span").attr("class","map-selection-key").html("<i></i> Selected"),
      update => update,
      exit => exit.remove()
    );
}

function renderDistrictGrid(svg, data, width, height, color) {
  if (!data.length) {
    setEmptyState("mapEmpty", true);
    return;
  }
  // No GeoJSON is not an error: use the ranked district grid fallback.
  setEmptyState("mapEmpty", false);
  const cols = Math.max(6, Math.ceil(Math.sqrt(data.length * 1.65)));
  const rows = Math.ceil(data.length / cols);
  const cellW = Math.max(20, (width-40)/cols), cellH = Math.max(18, (height-40)/rows);
  const max = d3.max(data, caseOf) || 1;
  const gridColor = d3.scaleSequential(d3.interpolateBlues).domain([0,max]);
  const g = svg.append("g").attr("transform","translate(20,18)");
  g.selectAll("rect").data(data).join("rect")
    .attr("class",d=>`district alert-${(d.alert_level||"none").toLowerCase()}`)
    .attr("x",(d,i)=>(i%cols)*cellW).attr("y",(d,i)=>Math.floor(i/cols)*cellH)
    .attr("width",Math.max(13,cellW-3)).attr("height",Math.max(12,cellH-3))
    .attr("rx",5).attr("fill",d=>d.data_status==="AVAILABLE"?gridColor(caseOf(d)):"#edf4f5")
    .on("mouseenter",(event,d)=>showMapTip(event,d))
    .on("mousemove",event=>moveTip(event,"#mapTooltip"))
    .on("mouseleave",()=>hideTip("#mapTooltip"))
    .on("click",(event,d)=>selectDistrict(districtOf(d)));
  g.selectAll("text").data(data).join("text")
    .attr("x",(d,i)=>(i%cols)*cellW+4).attr("y",(d,i)=>Math.floor(i/cols)*cellH+Math.min(12,cellH-5))
    .attr("class","map-label").text(d=>shortDistrict(districtOf(d)));
}

function renderTimeSeries() {
  const svg = d3.select("#timeSeriesChart"); svg.selectAll("*").remove();
  let dataAll = filteredRows(state.timeSeries, {week:false});
  if (state.week !== "__latest__") {
    dataAll = dataAll.filter(row => compareDatesOrText(weekOf(row), state.week) <= 0);
  }
  let forecastRows = state.alert === "__all__"
    ? filteredRows(state.forecasts, {week:false, alert:false})
    : [];
  const origin = state.week === "__latest__"
    ? unique(forecastRows.map(row => row.forecast_origin).filter(Boolean)).sort(compareDatesOrText).at(-1)
    : state.week;
  forecastRows = forecastRows.filter(row => row.forecast_origin === origin);
  let data = aggregateSeries(dataAll, forecastRows);
  const observedWeek = latestWeek(dataAll);
  const observedWeekRecord = dataAll.find(row => weekOf(row) === observedWeek);
  const observedStatus = $("observedWeekStatus");
  if (observedStatus) {
    if (observedWeekRecord?.week_status === "PROVISIONAL") {
      observedStatus.textContent = `Latest observed week (${observedWeek}) may be incomplete; counts may change as delayed reports arrive.`;
      observedStatus.classList.remove("hidden");
    } else if (observedWeekRecord?.week_status === "POSSIBLY_INCOMPLETE") {
      observedStatus.textContent = `Latest observed week (${observedWeek}) may be incomplete; completeness data is unavailable.`;
      observedStatus.classList.remove("hidden");
    } else {
      observedStatus.textContent = "";
      observedStatus.classList.add("hidden");
    }
  }
  if (state.week === "__latest__" && !state.startDate && !state.endDate && forecastRows.length) {
    const lastObserved = data.filter(point => point.observed != null).at(-1);
    if (lastObserved) {
      const chartStart = dateFromWeek(lastObserved.week);
      chartStart.setUTCDate(chartStart.getUTCDate() - 364);
      data = data.filter(point => dateFromWeek(point.week) >= chartStart);
    }
  }
  const forecastMessage = $("forecastMessage");
  if (forecastMessage) {
    if (!state.forecasts.length) {
      forecastMessage.textContent = "Forecast data unavailable.";
      forecastMessage.classList.remove("hidden");
    } else if (state.alert !== "__all__") {
      forecastMessage.textContent = "Forecast not shown with an alert-level filter.";
      forecastMessage.classList.remove("hidden");
    } else if (!forecastRows.length) {
      forecastMessage.textContent = "Forecast unavailable for the selected filters.";
      forecastMessage.classList.remove("hidden");
    } else {
      forecastMessage.textContent = "";
      forecastMessage.classList.add("hidden");
    }
  }
  const empty = $("timeSeriesEmpty");
  if (!data.length) { setEmptyState("timeSeriesEmpty", true); return; }
  setEmptyState("timeSeriesEmpty", false);

  const wrap = document.querySelector("#timeSeriesWrap");
  const width = wrap.clientWidth, height = wrap.clientHeight;
  const margin = {top:22,right:72,bottom:38,left:54};
  const innerW=width-margin.left-margin.right, innerH=height-margin.top-margin.bottom;
  svg.attr("viewBox",`0 0 ${width} ${height}`);
  const g=svg.append("g").attr("transform",`translate(${margin.left},${margin.top})`);

  const parse = value => dateFromWeek(typeof value === "string" ? value : value.week);
  const xDomain=d3.extent(data,d=>parse(d));
  const x=d3.scaleTime().domain(xDomain).range([0,innerW]);
  const xTicks=xDomain[1]-xDomain[0]<=550*86400000?d3.timeMonth.every(2):d3.timeYear.every(1);
  const ymax=d3.max(data,d=>Math.max(d.observed||0,d.forecast||0))||1;
  const y=d3.scaleLinear().domain([0,ymax*1.12]).nice().range([innerH,0]);
  const forecastData=data.filter(d=>d.forecast != null);
  const observedData=data.filter(d=>d.observed != null);

  if (forecastData.length && observedData.length) {
    const observedEnd=parse(observedData[observedData.length-1].week);
    const forecastEnd=parse(forecastData[forecastData.length-1].week);
    g.append("rect").attr("class","forecast-window")
      .attr("x",x(observedEnd)).attr("y",0)
      .attr("width",Math.max(0,x(forecastEnd)-x(observedEnd))).attr("height",innerH);
    g.append("line").attr("class","forecast-start")
      .attr("x1",x(forecastData[0] ? parse(forecastData[0].week) : forecastEnd))
      .attr("x2",x(forecastData[0] ? parse(forecastData[0].week) : forecastEnd))
      .attr("y1",0).attr("y2",innerH);
  }

  g.append("g").attr("class","grid").call(d3.axisLeft(y).ticks(5).tickSize(-innerW).tickFormat(""));
  g.append("g").attr("class","axis").attr("transform",`translate(0,${innerH})`).call(d3.axisBottom(x).ticks(xTicks).tickFormat(d3.timeFormat("%b %y")));
  g.append("g").attr("class","axis").call(d3.axisLeft(y).ticks(5).tickFormat(d3.format(".2s")));

  g.append("path").datum(data).attr("class","observed-line").attr("fill","none").attr("stroke","#1769aa").attr("stroke-width",2.8)
    .attr("d",d3.line().defined(d=>d.observed!=null).x(d=>x(parse(d.week))).y(d=>y(d.observed)));
  if (forecastData.length) g.append("path").datum(forecastData).attr("class","forecast-line").attr("fill","none").attr("stroke","#d9482b").attr("stroke-width",3.6).attr("stroke-dasharray","8 4")
    .attr("d",d3.line().defined(d=>d.forecast!=null).x(d=>x(parse(d.week))).y(d=>y(d.forecast)));

  g.selectAll(".observed-point").data(observedData).join("circle")
    .attr("class","observed-point").attr("cx",d=>x(parse(d.week))).attr("cy",d=>y(d.observed)).attr("r",2.8).attr("fill","#fff").attr("stroke","#1769aa").attr("stroke-width",1.4)
    .on("mouseenter",(event,d)=>showChartTip(event,d)).on("mousemove",event=>moveTip(event,"#chartTooltip")).on("mouseleave",()=>hideTip("#chartTooltip"));

  g.selectAll(".forecast-point").data(forecastData).join("circle")
    .attr("class","forecast-point").attr("cx",d=>x(parse(d.week))).attr("cy",d=>y(d.forecast)).attr("r",4.2)
    .attr("fill","#d9482b").attr("stroke","#fff").attr("stroke-width",1.5)
    .on("mouseenter",(event,d)=>showChartTip(event,d)).on("mousemove",event=>moveTip(event,"#chartTooltip")).on("mouseleave",()=>hideTip("#chartTooltip"));

  if (observedData.length) {
    const last=observedData[observedData.length-1];
    g.append("text").attr("class","series-end-label observed-label")
      .attr("x",x(parse(last.week))+7).attr("y",y(last.observed)-7).text("Observed");
  }
  if (forecastData.length) {
    const last=forecastData[forecastData.length-1];
    g.append("text").attr("class","series-end-label forecast-label")
      .attr("x",x(parse(last.week))+7).attr("y",y(last.forecast)+4).text("Forecast");
  }
  g.append("text").attr("class","axis-label")
    .attr("transform",`translate(${-40},${innerH/2}) rotate(-90)`).attr("text-anchor","middle").text("Cases");
}

function aggregateSeries(observedRows, forecastRows) {
  const groups = new Map();
  const lastObservedWeek = observedRows.map(weekOf).filter(Boolean).sort(compareDatesOrText).at(-1);
  observedRows.forEach(row => {
    const week = weekOf(row);
    if (!week) return;
    const point = groups.get(week) || {week, observedValues: [], forecastValues: []};
    const cases = number(row.case_count);
    if (row.data_status === "SUPPRESSED") point.hasSuppressed = true;
    else if (row.data_status === "MISSING") point.hasMissing = true;
    else if (cases != null) point.observedValues.push(cases);
    if (row.week_status === "PROVISIONAL") point.weekStatus = "PROVISIONAL";
    groups.set(week, point);
  });
  forecastRows.forEach(row => {
    const week = row.forecast_date;
    const predicted = number(row.predicted_cases);
    if (!week || predicted == null || (lastObservedWeek && compareDatesOrText(week, lastObservedWeek) <= 0)) return;
    const point = groups.get(week) || {week, observedValues: [], forecastValues: []};
    point.forecastValues.push(predicted);
    groups.set(week, point);
  });
  return [...groups.values()].map(point => ({
    week: point.week,
    observed: point.hasSuppressed || point.hasMissing || !point.observedValues.length
      ? null
      : sum(point.observedValues),
    forecast: point.forecastValues.length ? sum(point.forecastValues) : null,
    data_status: point.hasSuppressed ? "SUPPRESSED" : point.hasMissing ? "MISSING" : "AVAILABLE",
    week_status: point.weekStatus || "OBSERVED"
  })).sort((a,b) => compareDatesOrText(a.week,b.week));
}

function renderHeatmap() {
  const svg=d3.select("#heatmapChart"); svg.selectAll("*").remove();
  const rows = filteredRows(state.heatmap, {seasonal:true});
  if(!rows.length){setEmptyState("heatmapEmpty", true);return}
  setEmptyState("heatmapEmpty", false);
  const cells = Array.from(d3.rollup(rows, records => ({
    disease: diseaseOf(records[0]),
    epi_week: Number(records[0].epi_week),
    data_status: records.some(r => r.data_status === "SUPPRESSED")
      ? "SUPPRESSED"
      : records.some(r => r.data_status === "MISSING") ? "MISSING" : "AVAILABLE",
    seasonal_index: records.some(r => r.data_status === "SUPPRESSED")
      ? null
      : d3.mean(records, r => number(r.seasonal_index)),
    districts: records.length
  }), r => diseaseOf(r), r => Number(r.epi_week)), ([disease, weeks]) =>
    Array.from(weeks, ([epiWeek, value]) => ({...value, disease, epi_week: epiWeek}))
  ).flat();
  const wrap=document.querySelector("#heatmapWrap"), width=wrap.clientWidth,height=wrap.clientHeight;
  const margin={top:22,right:10,bottom:30,left:72}; svg.attr("viewBox",`0 0 ${width} ${height}`);
  const diseases=unique(cells.map(d=>d.disease)).sort();
  const weeks=unique(cells.map(d=>String(d.epi_week))).sort((a,b)=>Number(a)-Number(b));
  const shownWeeks=weeks.slice(-53);
  const x=d3.scaleBand().domain(shownWeeks).range([margin.left,width-margin.right]).padding(.06);
  const y=d3.scaleBand().domain(diseases).range([margin.top,height-margin.bottom]).padding(.12);
  const vals=cells.filter(d=>shownWeeks.includes(String(d.epi_week)));
  const max=d3.max(vals,d=>d.seasonal_index)||1;
  const color=d3.scaleSequential(d3.interpolateBlues).domain([0,max]);
  svg.append("g").selectAll("rect").data(vals).join("rect")
    .attr("x",d=>x(String(d.epi_week))).attr("y",d=>y(d.disease)).attr("width",x.bandwidth()).attr("height",y.bandwidth())
    .attr("rx",2).attr("fill",d=>color(d.seasonal_index||0))
    .on("mouseenter",(event,d)=>{const tip=$("heatmapTooltip");const value=d.data_status==="SUPPRESSED"?"Suppressed":d.data_status==="MISSING"?"Missing":d.seasonal_index==null?"N/A":fmt1(d.seasonal_index);tip.innerHTML=`<strong>${esc(d.disease)}</strong><br>Epi week ${d.epi_week}<br>Mean seasonal index: ${value}<br>Districts: ${fmt(d.districts)}`;tip.style.opacity=1;moveTip(event,"#heatmapTooltip")})
    .on("mousemove",event=>moveTip(event,"#heatmapTooltip")).on("mouseleave",()=>hideTip("#heatmapTooltip"));
  svg.append("g").attr("class","axis").attr("transform",`translate(0,${height-margin.bottom})`).call(d3.axisBottom(x).tickValues(shownWeeks.filter((d,i)=>i%8===0)).tickFormat(d=>`W${d}`));
  svg.append("g").attr("class","axis").attr("transform",`translate(${margin.left},0)`).call(d3.axisLeft(y).tickSize(0));
}

function renderWarningTable() {
  const tbody=$("warningTable"); tbody.innerHTML="";
  let rows=filteredWarningRows();
  rows.sort((a,b)=>(severity(alertOf(b))-severity(alertOf(a))) || (caseOf(b)-caseOf(a)));
  rows=rows.slice(0,12);
  $("warningEmpty").classList.toggle("hidden",rows.length>0);
  rows.forEach((r,i)=>{
    const tr=document.createElement("tr");
    const caseValue=number(r.case_count);
    const incidence=number(r.incidence_per_100k);
    tr.innerHTML=`<td>${i+1}</td><td><strong>${esc(districtOf(r)||"—")}</strong></td><td>${esc(diseaseOf(r)||"—")}</td><td>${esc(formatWeek(weekOf(r)||"—"))}</td><td><span class="signal ${esc(alertOf(r))}">${esc(alertOf(r))}</span></td><td>${esc(r.detection_method||"N/A")}</td><td>${caseValue==null?"N/A":fmt(caseValue)}</td><td>${incidence==null?"N/A":fmt1(incidence)}</td>`;
    tr.addEventListener("click",()=>selectDistrict(districtOf(r))); tbody.appendChild(tr);
  });
}

function renderResources() {
  const records=state.resources.filter(row =>
    (state.district==="__all__" || normalize(districtOf(row))===normalize(state.district)) &&
    (state.division==="__all__" || normalize(divisionOf(row))===normalize(state.division)) &&
    (state.week==="__latest__" || row.forecast_origin===state.week) &&
    matchesDateRange(row)
  );
  const selected=records.map(row=>({
    ...row,
    demand:(row.disease_demand||[]).filter(item=>state.disease==="__all__"||item.disease===state.disease)
  })).filter(row=>row.demand.length);
  const total=key=>selected.length && selected.every(row=>number(row[key])!=null)
    ? sum(selected.map(row=>number(row[key])))
    : null;
  const demandTotal=key=>sum(selected.flatMap(row=>row.demand.map(item=>number(item[key])||0)));
  const capacityTotal=total("district_bed_capacity");
  const availableBedsTotal=total("available_beds");
  const optimizedBedsTotal=total("optimized_beds");
  $("resourceCapacity").textContent=capacityTotal==null?"N/A":fmt(capacityTotal);
  $("resourceBeds").textContent=availableBedsTotal==null?"N/A":fmt(availableBedsTotal);
  $("resourceDemand").textContent=selected.length?fmt1(demandTotal("bed_planning_requirement")):"N/A";
  $("resourceAllocation").textContent=optimizedBedsTotal==null?"N/A":fmt1(optimizedBedsTotal);
  const gapDistricts=selected.filter(row=>row.allocation_status==="GAP").length;
  $("allocationSummaryBadge").textContent=selected.length
    ? `${gapDistricts} of ${selected.length} districts with unmet demand`
    : "No allocation data";
  const budgets=state.resourceMeta.national_budgets||{};
  const optimized=state.resourceMeta.optimized||{};
  const baseline=state.resourceMeta.population_proportional_baseline||{};
  const reduction=state.resourceMeta.unmet_demand_reduction||{};
  const budgetText=[
    budgets.beds==null?null:`Beds ${fmt1(number(budgets.beds))}`,
    budgets.saline==null?null:`Saline ${fmt1(number(budgets.saline))}`,
    budgets.test_kits==null?null:`Test kits ${fmt1(number(budgets.test_kits))}`
  ].filter(Boolean).join(" · ");
  const optimizedUnmet=number(optimized.total_unmet);
  const baselineUnmet=number(baseline.total_unmet);
  const reductionText=number(reduction.percentage);
  const comparisonText=optimizedUnmet==null||baselineUnmet==null
    ? "Allocation comparison unavailable."
    : `Optimized unmet ${fmt1(optimizedUnmet)} vs population baseline ${fmt1(baselineUnmet)}${reductionText==null?"":` (${fmt1(reductionText)}% lower)`}.`;
  $("resourceSummary").textContent=budgetText
    ? `National ${fmt(state.resourceMeta.planning_horizon_weeks||4)}-week budgets: ${budgetText}. ${comparisonText}`
    : "National resource budgets are unavailable in AN-7 output.";
  const tbody=$("resourceTable"); tbody.innerHTML="";
  $("resourceEmpty").classList.toggle("hidden",selected.length>0);
  selected.sort((a,b)=>normalize(districtOf(a)).localeCompare(normalize(districtOf(b)))).forEach(row=>{
    const tr=document.createElement("tr");
    const cell=(value,precision=1)=>number(value)==null?"N/A":precision===0?fmt(number(value)):fmt1(number(value));
    tr.innerHTML=`<td><strong>${esc(districtOf(row))}</strong></td><td>${cell(row.active_facility_count,0)}</td><td>${cell(row.district_bed_capacity,0)}</td><td>${cell(row.available_beds,0)}</td><td>${esc(row.stock_data_status||"N/A")}</td><td>${cell(sum(row.demand.map(item=>number(item.forecast_cases)||0)))}</td><td>${cell(row.bed_demand,0)}</td><td>${cell(row.optimized_beds)}</td><td>${cell(row.optimized_beds_unmet)}</td><td>${cell(row.baseline_beds)}</td><td>${cell(row.saline_stock,0)}</td><td>${cell(row.saline_demand,0)}</td><td>${cell(row.optimized_saline)}</td><td>${cell(row.optimized_saline_unmet)}</td><td>${cell(row.baseline_saline)}</td><td>${cell(row.test_kit_stock,0)}</td><td>${cell(row.test_kit_demand,0)}</td><td>${cell(row.optimized_test_kits)}</td><td>${cell(row.optimized_test_kits_unmet)}</td><td>${cell(row.baseline_test_kits)}</td><td>${cell(row.unmet_reduction)}</td><td><span class="allocation-status ${row.allocation_status==="GAP"?"gap":"covered"}">${esc(row.allocation_status||"N/A")}</span></td>`;
    tr.addEventListener("click",()=>selectDistrict(districtOf(row))); tbody.appendChild(tr);
  });
}

function renderCalendar() {
  const grid=$("calendarGrid"); grid.innerHTML="";
  const rows=filteredRows(state.preparedness,{seasonal:true});
  const grouped=d3.group(rows,r=>String(r.representative_month_name||"").toUpperCase().slice(0,3));
  const months=["JAN","FEB","MAR","APR","MAY","JUN","JUL","AUG","SEP","OCT","NOV","DEC"];
  months.forEach(m=>{
    const monthRows=grouped.get(m)||[];
    const ranked=monthRows.slice().sort((a,b)=>preparednessSeverity(b.preparedness_level)-preparednessSeverity(a.preparedness_level));
    const peak=ranked[0];
    const level=peak?String(peak.preparedness_level).toUpperCase():"N/A";
    const priority=unique(monthRows.filter(r=>preparednessSeverity(r.preparedness_level)>0).map(districtOf).filter(Boolean)).slice(0,2).join(", ");
    const recommendation=peak?.recommended_action||"No preparedness data available";
    const div=document.createElement("div"); div.className="month "+(peak?level.toLowerCase():"empty");
    div.innerHTML=`<div class="month-name">${m}</div><div class="month-level">${esc(level)}</div><div class="month-note">${esc(priority||recommendation)}</div>`;
    grid.appendChild(div);
  });
}

function selectDistrict(name) {
  if (!name) return;
  const selected=state.timeSeries.find(row=>normalize(districtOf(row))===normalize(name));
  const district=selected?districtOf(selected):name;
  const division=state.divisionByDistrict.get(normalize(name)) ||
    state.divisionByDistrict.get(normalize(district)) || "__all__";
  setSelection({district, division});
  showToast(state.district!=="__all__" ? `${state.district} selected` : "All districts selected");
}

function setSelection({district=state.district, division=state.division}={}) {
  if (district !== "__all__") {
    state.district = district;
    state.division = state.divisionByDistrict.get(normalize(district)) || division || "__all__";
  } else {
    state.district = "__all__";
    state.division = division || "__all__";
  }
  $("divisionSelect").value = state.division;
  hydrateDistrictFilter();
  $("selectedDistrict").textContent = state.district !== "__all__"
    ? state.district
    : state.division !== "__all__" ? state.division : "All districts";
  renderAll();
}

function showMapTip(event,r) {
  const tip=$("mapTooltip");
  const cases=r.data_status==="SUPPRESSED"
    ? "Suppressed"
    : r.data_status==="MISSING" ? "Missing"
    : number(r.case_count)==null ? "N/A" : fmt(number(r.case_count));
  const incidence=number(r.incidence_per_100k);
  const week=r.alert_week??r.epi_week_start??r.epi_week;
  const disease=r.alert_disease||r.disease||"All diseases";
  tip.innerHTML=`<strong>${esc(districtOf(r)||"District")}</strong><br>Disease: ${esc(disease)}<br>Epi-week: ${esc(week==null?"N/A":formatWeek(week))}<br>Cases: ${cases}<br>Incidence: ${incidence==null?"N/A":fmt1(incidence)+"/100k"}<br>Alert: ${esc(alertOf(r))}<br>Detection signals: ${esc(r.detection_method||"N/A")}`;
  tip.style.opacity=1; moveTip(event,"#mapTooltip");
}
function showChartTip(event,d){const tip=$("chartTooltip");const observed=d.data_status==="SUPPRESSED"?"Suppressed":d.data_status==="MISSING"?"Missing":d.observed==null?"N/A":fmt(d.observed);tip.innerHTML=`<strong>${esc(formatWeek(d.week))}</strong><br>Observed: ${observed}${d.week_status==="PROVISIONAL"?" (provisional)":""}${d.forecast!=null?`<br>Forecast: ${fmt(d.forecast)}`:""}`;tip.style.opacity=1;moveTip(event,"#chartTooltip")}
function moveTip(event,id){const tip=$(id.slice(1)); const rect=tip.parentElement.getBoundingClientRect(); tip.style.left=(event.clientX-rect.left+12)+"px"; tip.style.top=(event.clientY-rect.top-8)+"px"}
function hideTip(id){$(id.slice(1)).style.opacity=0}

function findFeatures(rows){
  for(const r of rows){
    const g=r.geometry ?? r.geojson ?? r.feature;
    if(g && g.type==="Feature") return rows.map(x=>x.geometry ?? x.geojson ?? x.feature).filter(g=>g&&g.type==="Feature");
  }
  return [];
}
function featureName(f){return f.properties?.district_name ?? f.properties?.district ?? f.properties?.name ?? f.properties?.adm2_en ?? f.attributes?.adm2_en ?? f.attributes?.district ?? "Unknown"}
function featureDivision(f){return f.properties?.division_name ?? f.properties?.adm1_en ?? f.attributes?.adm1_en ?? ""}
function districtOf(r){return r.district_name ?? r.district ?? r.district_name_canonical ?? r.name ?? ""}
function divisionOf(r){return r.division_name ?? state.divisionByDistrict.get(normalize(districtOf(r))) ?? ""}
function diseaseOf(r){return r.disease ?? r.disease_name ?? r.condition ?? ""}
function weekOf(r){return String(r.epi_week_start ?? r.epi_week ?? r.week_start ?? r.week ?? r.date ?? "")}
function caseOf(r){return number(r.case_count ?? r.cases ?? r.observed_cases ?? r.total_cases ?? r.count)}
function alertOf(r){
  const raw = r.alert_level ?? r.signal ?? r.alert ?? r.consensus_level ??
    r.priority_level ?? r.severity ?? r.warning_level;
  if (raw !== undefined && raw !== null && raw !== "") {
    if (typeof raw === "boolean") return raw ? "MEDIUM" : "NONE";
    const s = String(raw).toUpperCase();
    if (s.includes("HIGH") || s === "3") return "HIGH";
    if (s.includes("MEDIUM") || s.includes("MODERATE") || s === "2") return "MEDIUM";
    if (s.includes("LOW") || s === "1") return "LOW";
    if (["NONE","NORMAL","NO ALERT","NO_ALERT","FALSE","0"].includes(s)) return "NONE";
    if (s.includes("ALERT") || s.includes("WARNING")) return "MEDIUM";
    return s;
  }
  if (r.consensus_alert === true || r.consensus === true || r.is_alert === true) return "MEDIUM";
  return "NONE";
}
function severity(x){return x==="HIGH"?3:x==="MEDIUM"?2:x==="LOW"?1:0}
function preparednessSeverity(x){return x==="PEAK"?3:x==="ELEVATED"?2:x==="NORMAL"?1:0}
function number(v){if(v===null||v===undefined||v==="")return null;const n=Number(v);return Number.isFinite(n)?n:null}
function firstNumber(arr){for(const v of arr){if(v!==null&&v!==undefined&&Number.isFinite(v))return v}return null}
function sum(arr){return arr.reduce((a,b)=>a+(Number.isFinite(b)?b:0),0)}
function unique(arr){return [...new Set(arr)]}
function normalize(v){return String(v||"").trim().toLowerCase().replace(/[’']/g,"'").replace(/\s+/g," ")}
function shortDistrict(v){const s=String(v||"");return s.length>12?s.slice(0,10)+"…":s}
function compareDatesOrText(a,b){const da=Date.parse(a),db=Date.parse(b);if(Number.isFinite(da)&&Number.isFinite(db))return da-db;return String(a).localeCompare(String(b))}
function dateFromWeek(w){const d=new Date(w);if(!isNaN(d))return d;const m=String(w).match(/(\d{4})[- ]?W?(\d{1,2})/i);if(m){const y=+m[1],week=+m[2],jan4=new Date(Date.UTC(y,0,4));const day=jan4.getUTCDay()||7;return new Date(jan4.getTime()+(week-1)*604800000-(day-1)*86400000)}return new Date()}
function formatWeek(w){if(!w)return"—";const d=dateFromWeek(w);return isNaN(d)?"Week "+w:d3.timeFormat("%d %b %Y")(d)}
function summaryValue(key){
  const s=state.summary||{};
  return number(s[key] ?? s.metrics?.[key] ?? s.summary?.[key] ??
    s.kpis?.[key] ?? s.statistics?.[key]);
}
function setUpdated(payload){const raw=payload?.generated_at ?? payload?.updated_at ?? payload?.generatedAt ?? payload?.last_updated ?? payload?.build_time;const formatted=raw?(()=>{const date=new Date(raw);return isNaN(date)?"N/A":d3.timeFormat("%d %b %Y, %H:%M")(date)})():"N/A";$("lastUpdated").textContent=formatted;$("kpiUpdated").textContent=formatted}
function esc(v){return String(v??"").replace(/[&<>"']/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]))}
function showToast(message){const t=$("toast");t.textContent=message;t.classList.add("show");clearTimeout(window.__toast);window.__toast=setTimeout(()=>t.classList.remove("show"),1800)}

window.addEventListener("resize",()=>{renderMap();renderTimeSeries();renderHeatmap()});
