let selectedCorridorId = null;
let scoreChart;
let componentChart;
let hourlyProfileChart;
let latestRun = null;
let latestCameras = [];
let selectedCamera = null;
let corridorsById = new Map();
let latestWeather = null;
let radarMap = null;
let radarBaseLayer = null;
let radarWmsLayer = null;
let radarCorridorRect = null;
let radarFittedCorridorId = null;
let radarFrameTimes = [];
let radarFrameIndex = -1;
let radarFrameSignature = "";
let radarAnimTimer = null;
let radarAnimPlaying = false;
let radarAnimSpeedMs = 800;
const SETTINGS_KEYS = [
  "poll_seconds",
  "sample_limit",
  "baseline_alpha",
  "min_scoped_cameras_for_strict",
  "weight_traffic",
  "weight_image_with_traffic",
  "weight_image_no_traffic",
  "incident_saturation_max",
  "incident_saturation_k",
  "closure_saturation_max",
  "closure_saturation_k",
  "weather_component_max",
  "weather_component_exponent",
  "weather_precip_weight",
  "weather_wind_weight",
  "weather_visibility_weight",
  "weather_alert_weight",
  "weather_wind_ref_mph",
  "weather_visibility_ref_miles",
  "weather_text_boost_rain",
  "weather_text_boost_storm",
  "weather_text_boost_snow",
  "weather_text_boost_fog",
  "weather_alert_major_bonus",
  "alert_high_score_threshold",
  "alert_spike_min_score",
  "alert_spike_delta",
  "alert_freshness_min_pct",
  "model_min_train_rows",
  "model_ridge_lambda",
  "model_p90_z",
  "model_conf_sigma_factor",
  "model_conf_drift_factor",
  "model_drift_norm_divisor"
];

const $ = (id) => document.getElementById(id);

async function api(path, opts = {}) {
  const res = await fetch(path, {
    headers: { "Content-Type": "application/json" },
    ...opts
  });
  if (!res.ok) throw new Error(await res.text());
  return res.json();
}

function scoreColor(score) {
  if (score >= 80) return "bad";
  if (score >= 60) return "warn";
  return "good";
}

function activateTab(name) {
  document.querySelectorAll(".tab-btn").forEach((b) => {
    b.classList.toggle("active", b.dataset.tab === name);
  });
  document.querySelectorAll(".panel").forEach((p) => {
    p.classList.toggle("active", p.id === `tab-${name}`);
  });
  if (name === "overview" && radarMap) {
    setTimeout(() => radarMap.invalidateSize(), 120);
  }
}

async function loadCorridors() {
  const includeArchived = $("includeArchivedChk")?.checked !== false;
  const corridors = await api(`/api/corridors?include_archived=${includeArchived}`);
  corridorsById = new Map(corridors.map((c) => [Number(c.id), c]));
  const sel = $("corridorSelect");
  sel.innerHTML = "";
  corridors.forEach((c) => {
    const o = document.createElement("option");
    o.value = c.id;
    const status = c.active ? "ACTIVE" : "ARCHIVED";
    o.textContent = `${c.name} (${status}) [${Number(c.xmin).toFixed(2)},${Number(c.ymin).toFixed(2)} -> ${Number(
      c.xmax
    ).toFixed(2)},${Number(c.ymax).toFixed(2)}]`;
    sel.appendChild(o);
  });
  if (!selectedCorridorId && corridors.length) {
    selectedCorridorId = Number(corridors[0].id);
  }
  if (selectedCorridorId && corridors.length && !corridors.find((c) => Number(c.id) === selectedCorridorId)) {
    selectedCorridorId = Number(corridors[0].id);
  }
  sel.value = String(selectedCorridorId || "");
}

function setStatus(msg) {
  $("statusText").textContent = msg;
}

function renderKpis(run) {
  latestRun = run;
  if (!run) return;
  $("kpiScore").textContent = Number(run.fused_score || 0).toFixed(1);
  $("kpiScore").className = `v ${scoreColor(Number(run.fused_score || 0))}`;
  $("kpiForecast").textContent = `${Number(run.predicted_next_score_p50 || 0).toFixed(1)} / ${Number(
    run.predicted_next_score_p90 || 0
  ).toFixed(1)}`;
  $("kpiConf").textContent = `${Number(run.prediction_confidence || 0).toFixed(1)}%`;
  $("kpiAlert").textContent = run.alert_state || "NORMAL";
  $("kpiWeatherRisk").textContent = `${Number(run.weather_risk_score || 0).toFixed(1)}`;
}

function renderScoreChart(ts) {
  const labels = ts.map((r) => new Date(r.run_ts).toLocaleTimeString());
  const scores = ts.map((r) => Number(r.fused_score || 0));
  const p50 = ts.map((r) => Number(r.predicted_next_score_p50 || 0));
  const p90 = ts.map((r) => Number(r.predicted_next_score_p90 || 0));
  if (scoreChart) scoreChart.destroy();
  scoreChart = new Chart($("scoreLineChart"), {
    type: "line",
    data: {
      labels,
      datasets: [
        { label: "Observed Score", data: scores, borderColor: "#6cb6ff", backgroundColor: "rgba(108,182,255,0.15)", fill: true, tension: 0.25 },
        { label: "Forecast P50", data: p50, borderColor: "#28d39f", borderDash: [6, 4], fill: false, tension: 0.2 },
        { label: "Forecast P90", data: p90, borderColor: "#ffbe57", borderDash: [2, 3], fill: false, tension: 0.2 }
      ]
    },
    options: {
      maintainAspectRatio: false,
      plugins: { legend: { labels: { color: "#e8f0ff" } } },
      scales: {
        y: { min: 0, max: 100, ticks: { color: "#d5dff3" }, grid: { color: "rgba(255,255,255,0.1)" } },
        x: { ticks: { color: "#d5dff3", maxTicksLimit: 8 }, grid: { color: "rgba(255,255,255,0.06)" } }
      }
    }
  });
}

function renderComponents(run) {
  if (!run) return;
  const vals = [
    Number(run.traffic_component || 0),
    Number(run.image_component || 0),
    Number(run.incident_component || 0),
    Number(run.closure_component || 0),
    Number(run.weather_component || 0)
  ];
  if (componentChart) componentChart.destroy();
  componentChart = new Chart($("componentBarChart"), {
    type: "bar",
    data: {
      labels: ["Traffic", "Image", "Incidents", "Closures", "Weather"],
      datasets: [{ data: vals, backgroundColor: ["#66b0ff", "#29d99e", "#ffbe57", "#ff6b72", "#8b9bff"] }]
    },
    options: {
      maintainAspectRatio: false,
      plugins: { legend: { display: false } },
      scales: {
        y: { min: 0, max: 100, ticks: { color: "#d5dff3" }, grid: { color: "rgba(255,255,255,0.1)" } },
        x: { ticks: { color: "#d5dff3" }, grid: { display: false } }
      }
    }
  });
}

function renderCameraRows(rows) {
  latestCameras = rows || [];
  const tb = $("cameraRows");
  tb.innerHTML = "";
  rows.forEach((r) => {
    const tr = document.createElement("tr");
    tr.style.cursor = "pointer";
    tr.innerHTML = `
      <td>${r.camera_location || "-"}</td>
      <td>${r.camera_direction || "-"}</td>
      <td>${Number(r.snapshot_bytes || 0)}</td>
      <td>${Number(r.age_minutes || 0).toFixed(1)}</td>
      <td>${Number(r.image_score || 0).toFixed(2)}</td>
      <td>${r.snapshot_url ? `<a href="${r.snapshot_url}" target="_blank">open</a>` : "-"}</td>`;
    tr.addEventListener("click", () => setCameraPreview(r));
    tb.appendChild(tr);
  });
  if (rows.length && !selectedCamera) {
    setCameraPreview(rows[0]);
  }
}

function setCameraPreview(cam) {
  selectedCamera = cam;
  const img = $("cameraPreviewImg");
  const meta = $("cameraPreviewMeta");
  if (!cam || !cam.snapshot_url) {
    img.removeAttribute("src");
    meta.textContent = "No preview available.";
    return;
  }
  img.src = `${cam.snapshot_url}${cam.snapshot_url.includes("?") ? "&" : "?"}ts=${Date.now()}`;
  const runTs = cam.run_ts ? new Date(cam.run_ts).toLocaleString() : "unknown";
  meta.textContent =
    `${cam.camera_location || "-"} | dir=${cam.camera_direction || "-"} | age=${Number(cam.age_minutes || 0).toFixed(
      1
    )} min | observed=${runTs} | image_score=${Number(cam.image_score || 0).toFixed(2)}`;
}

function refreshCameraPreview() {
  if (!selectedCamera) return;
  setCameraPreview(selectedCamera);
}

function renderTsRows(ts) {
  const tb = $("tsRows");
  tb.innerHTML = "";
  [...ts].reverse().slice(0, 40).forEach((r) => {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${new Date(r.run_ts).toLocaleString()}</td>
      <td>${Number(r.fused_score || 0).toFixed(1)}</td>
      <td>${Number(r.predicted_next_score_p50 || 0).toFixed(1)}</td>
      <td>${Number(r.predicted_next_score_p90 || 0).toFixed(1)}</td>
      <td>${Number(r.prediction_confidence || 0).toFixed(1)}%</td>
      <td>${Number(r.drift_score || 0).toFixed(1)}</td>
      <td><span class="pill">${r.alert_state || "-"}</span></td>`;
    tb.appendChild(tr);
  });
}

function normalizeRadarTimes(values) {
  const out = [];
  const seen = new Set();
  for (const v of Array.isArray(values) ? values : []) {
    const d = new Date(v);
    if (!Number.isFinite(d.getTime())) continue;
    const iso = d.toISOString();
    if (seen.has(iso)) continue;
    seen.add(iso);
    out.push(iso);
  }
  return out.sort();
}

function updateRadarFrameLabel() {
  const el = $("radarFrameLabel");
  if (!el) return;
  if (!radarFrameTimes.length || radarFrameIndex < 0) {
    el.textContent = "Frame: live";
    return;
  }
  const ts = new Date(radarFrameTimes[radarFrameIndex]);
  const shown = Number.isFinite(ts.getTime()) ? ts.toLocaleTimeString() : radarFrameTimes[radarFrameIndex];
  el.textContent = `Frame ${radarFrameIndex + 1}/${radarFrameTimes.length} | ${shown}`;
}

function applyRadarFrameToLayers() {
  const frame = radarFrameTimes.length && radarFrameIndex >= 0 ? radarFrameTimes[radarFrameIndex] : "";
  if (radarWmsLayer) {
    radarWmsLayer.setParams({ time: frame, _ts: Date.now() });
  }
  const radarImg = $("radarImg");
  if (radarImg && radarImg.style.display !== "none" && latestWeather?.radar_image_url) {
    const sep = latestWeather.radar_image_url.includes("?") ? "&" : "?";
    const t = frame ? `&time=${encodeURIComponent(frame)}` : "";
    radarImg.src = `${latestWeather.radar_image_url}${sep}ts=${Date.now()}${t}`;
  }
  updateRadarFrameLabel();
}

function setRadarFrameIndex(nextIndex) {
  if (!radarFrameTimes.length) return;
  const n = radarFrameTimes.length;
  radarFrameIndex = ((nextIndex % n) + n) % n;
  applyRadarFrameToLayers();
}

function stopRadarAnimation() {
  radarAnimPlaying = false;
  if (radarAnimTimer) {
    clearInterval(radarAnimTimer);
    radarAnimTimer = null;
  }
  const btn = $("radarPlayBtn");
  if (btn) btn.textContent = "Play Loop";
}

function startRadarAnimation() {
  if (!radarFrameTimes.length) return;
  stopRadarAnimation();
  radarAnimPlaying = true;
  const btn = $("radarPlayBtn");
  if (btn) btn.textContent = "Pause Loop";
  radarAnimTimer = setInterval(() => {
    setRadarFrameIndex(radarFrameIndex + 1);
  }, Math.max(250, radarAnimSpeedMs));
}

function toggleRadarAnimation() {
  if (!radarFrameTimes.length) return;
  if (radarAnimPlaying) stopRadarAnimation();
  else startRadarAnimation();
}

function setRadarFramesFromPayload(weather) {
  const times = normalizeRadarTimes(weather?.radar_frame_times || []);
  const signature = times.join("|");
  const changed = signature !== radarFrameSignature;
  if (changed) {
    radarFrameTimes = times;
    radarFrameSignature = signature;
    if (times.length) {
      let idx = -1;
      if (weather?.radar_time_default_utc) {
        const d = new Date(weather.radar_time_default_utc);
        if (Number.isFinite(d.getTime())) idx = times.lastIndexOf(d.toISOString());
      }
      if (idx < 0) idx = times.length - 1;
      radarFrameIndex = idx;
    } else {
      radarFrameIndex = -1;
      stopRadarAnimation();
    }
  }
  updateRadarFrameLabel();
}

function renderWeather(w) {
  latestWeather = w;
  if (!w) return;
  const summary = [];
  summary.push(`Risk=${Number(w.weather_risk_score || 0).toFixed(1)} / 100`);
  summary.push(`Precip=${Number(w.precip_probability_pct || 0).toFixed(0)}%`);
  summary.push(`Wind=${Number(w.wind_mph || 0).toFixed(1)} mph`);
  if (w.visibility_miles != null) summary.push(`Visibility=${Number(w.visibility_miles).toFixed(1)} mi`);
  summary.push(`Alerts=${Number(w.alerts_count || 0)}`);
  if (w.condition_text) summary.push(`Conditions: ${w.condition_text}`);
  $("weatherSummary").textContent = summary.join(" | ");

  const link = $("radarStationLink");
  if (w.radar_station_url) {
    link.href = w.radar_station_url;
    link.textContent = `Open NWS Station Radar (${w.radar_station || "station"})`;
  } else {
    link.removeAttribute("href");
    link.textContent = "";
  }

  const staticLink = $("radarStaticLink");
  if (w.radar_image_url) {
    staticLink.href = w.radar_image_url;
    staticLink.textContent = "Open static radar image";
    setRadarFramesFromPayload(w);
    renderRadarMap(w);
  } else {
    staticLink.removeAttribute("href");
    staticLink.textContent = "";
    const mapEl = $("radarMap");
    if (mapEl) mapEl.style.display = "none";
    const radarImg = $("radarImg");
    if (radarImg) radarImg.style.display = "none";
  }
  const fetchedAt = w.fetched_at ? new Date(w.fetched_at).toLocaleString() : "unknown";
  const runTs = w.run_ts ? new Date(w.run_ts).toLocaleString() : "";
  $("radarMeta").textContent =
    `Radar source: NOAA OpenGeo MRMS + OSM landmarks | fetched: ${fetchedAt}${runTs ? ` | from run: ${runTs}` : ""}${
      w.from_live_fetch ? " | live fetch" : " | saved run"
    }`;
  updateRadarFrameLabel();
}

function parseBboxFromRadarUrl(url) {
  if (!url) return null;
  try {
    const u = new URL(url);
    const raw = u.searchParams.get("bbox");
    if (!raw) return null;
    const vals = raw.split(",").map(Number);
    if (vals.length !== 4 || vals.some((v) => !Number.isFinite(v))) return null;
    return { xmin: vals[0], ymin: vals[1], xmax: vals[2], ymax: vals[3] };
  } catch {
    return null;
  }
}

function getSelectedCorridorBbox() {
  const c = corridorsById.get(Number(selectedCorridorId));
  if (!c) return null;
  const vals = [Number(c.xmin), Number(c.ymin), Number(c.xmax), Number(c.ymax)];
  if (vals.some((v) => !Number.isFinite(v))) return null;
  return { xmin: vals[0], ymin: vals[1], xmax: vals[2], ymax: vals[3] };
}

function expandBbox(bbox, ratio = 0.42, minPad = 0.2) {
  const w = Math.abs(Number(bbox.xmax) - Number(bbox.xmin));
  const h = Math.abs(Number(bbox.ymax) - Number(bbox.ymin));
  const padX = Math.max(minPad, w * ratio);
  const padY = Math.max(minPad, h * ratio);
  return {
    xmin: Number(bbox.xmin) - padX,
    ymin: Number(bbox.ymin) - padY,
    xmax: Number(bbox.xmax) + padX,
    ymax: Number(bbox.ymax) + padY
  };
}

function ensureRadarMap() {
  if (radarMap || typeof window.L === "undefined") return;
  const mapEl = $("radarMap");
  if (!mapEl) return;
  radarMap = window.L.map("radarMap", {
    zoomControl: true,
    preferCanvas: true
  });
  radarBaseLayer = window.L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
    maxZoom: 19,
    attribution: '&copy; OpenStreetMap contributors'
  }).addTo(radarMap);
  radarWmsLayer = window.L.tileLayer.wms("https://opengeo.ncep.noaa.gov/geoserver/ows", {
    layers: "conus:conus_bref_qcd",
    format: "image/png",
    transparent: true,
    version: "1.1.1",
    styles: "",
    opacity: 0.72,
    attribution: "NOAA MRMS Radar"
  }).addTo(radarMap);
  radarCorridorRect = window.L.rectangle(
    [
      [41.3, -90.8],
      [41.7, -90.2]
    ],
    { color: "#79c0ff", weight: 1.2, dashArray: "6,4", fill: false }
  ).addTo(radarMap);
}

function renderRadarMap(weather) {
  ensureRadarMap();
  if (!radarMap || !radarWmsLayer) {
    const radarImg = $("radarImg");
    radarImg.style.display = "block";
    applyRadarFrameToLayers();
    if (!radarImg.src) {
      radarImg.src = `${weather.radar_image_url}${weather.radar_image_url.includes("?") ? "&" : "?"}ts=${Date.now()}`;
    }
    return;
  }

  $("radarMap").style.display = "block";
  $("radarImg").style.display = "none";

  const bboxRaw = getSelectedCorridorBbox() || parseBboxFromRadarUrl(weather.radar_image_url);
  const bbox = bboxRaw ? expandBbox(bboxRaw) : null;

  applyRadarFrameToLayers();
  if (bboxRaw && radarCorridorRect) {
    radarCorridorRect.setBounds([
      [bboxRaw.ymin, bboxRaw.xmin],
      [bboxRaw.ymax, bboxRaw.xmax]
    ]);
  }

  const shouldFit = radarFittedCorridorId !== Number(selectedCorridorId);
  if (bbox && shouldFit) {
    radarMap.fitBounds(
      [
        [bbox.ymin, bbox.xmin],
        [bbox.ymax, bbox.xmax]
      ],
      { padding: [18, 18], maxZoom: 10 }
    );
    radarFittedCorridorId = Number(selectedCorridorId);
  }
  setTimeout(() => radarMap.invalidateSize(), 120);
}

function renderModelMeta(model) {
  if (!model) {
    $("modelMeta").textContent = "No model snapshot yet.";
    $("predictionDetail").textContent = "-";
    return;
  }
  $("modelMeta").innerHTML =
    `Version: <span class="pill">${model.model_version}</span><br>` +
    `Train rows: ${model.train_rows}<br>` +
    `Residual sigma: ${Number(model.residual_sigma).toFixed(2)}<br>` +
    `R²: ${Number(model.r2).toFixed(3)}<br>` +
    `Drift: ${Number(model.drift_score).toFixed(2)}`;
  if (latestRun) {
    $("predictionDetail").innerHTML =
      `Current score ${Number(latestRun.fused_score).toFixed(1)} -> ` +
      `next p50 ${Number(latestRun.predicted_next_score_p50).toFixed(1)}, ` +
      `p90 ${Number(latestRun.predicted_next_score_p90).toFixed(1)}, ` +
      `confidence ${Number(latestRun.prediction_confidence).toFixed(1)}%.`;
  }
}

async function refreshCheckpointTable() {
  if (!selectedCorridorId) return;
  const cps = await api(`/api/checkpoints?corridor_id=${selectedCorridorId}`);
  const tb = $("checkpointRows");
  tb.innerHTML = "";
  cps.forEach((cp) => {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${cp.name}</td>
      <td>${Number(cp.lat).toFixed(5)}</td>
      <td>${Number(cp.lon).toFixed(5)}</td>
      <td>${Number(cp.radius_km || 0).toFixed(1)}</td>
      <td>${cp.source}</td>`;
    tb.appendChild(tr);
  });
}

async function refreshAll() {
  if (!selectedCorridorId) return;
  setStatus("Refreshing...");
  const hours = Number($("hoursSelect").value || 24);
  const [latest, ts, cams, model, insights, weather] = await Promise.all([
    api(`/api/runs/latest?corridor_id=${selectedCorridorId}`),
    api(`/api/runs/timeseries?corridor_id=${selectedCorridorId}&hours=${hours}`),
    api(`/api/runs/recent-cameras?corridor_id=${selectedCorridorId}`),
    api(`/api/models/latest?corridor_id=${selectedCorridorId}`),
    api(`/api/analysis/insights?corridor_id=${selectedCorridorId}&hours=${hours}`),
    api(`/api/weather/current?corridor_id=${selectedCorridorId}`)
  ]);
  renderKpis(latest);
  renderScoreChart(ts);
  renderComponents(latest);
  renderCameraRows(cams);
  renderTsRows(ts);
  renderModelMeta(model);
  renderInsights(insights);
  renderWeather(weather);
  await refreshCheckpointTable();
  setStatus(`Updated ${new Date().toLocaleTimeString()}`);
}

async function saveCorridor() {
  const body = {
    name: $("corridorName").value.trim(),
    city: $("corridorCity").value.trim() || undefined,
    bbox: $("corridorBbox").value.trim() || undefined,
    radius_km: Number($("corridorRadius").value || 20)
  };
  await api("/api/corridors", { method: "POST", body: JSON.stringify(body) });
  await loadCorridors();
  await refreshAll();
}

async function archiveSelectedCorridor(activeValue) {
  if (!selectedCorridorId) return;
  await api(`/api/corridors/${selectedCorridorId}`, {
    method: "PATCH",
    body: JSON.stringify({ active: activeValue })
  });
  await loadCorridors();
  await refreshAll();
  setStatus(activeValue ? "Corridor restored." : "Corridor archived.");
}

async function saveCheckpoint() {
  const body = {
    corridor_id: selectedCorridorId,
    name: $("cpName").value.trim(),
    radius_km: Number($("cpRadius").value || 8)
  };
  if ($("cpLat").value && $("cpLon").value) {
    body.lat = Number($("cpLat").value);
    body.lon = Number($("cpLon").value);
  } else if ($("cpCity").value.trim()) {
    body.city = $("cpCity").value.trim();
  }
  await api("/api/checkpoints", { method: "POST", body: JSON.stringify(body) });
  await refreshCheckpointTable();
  setStatus("Checkpoint saved.");
}

async function clearCheckpoints() {
  if (!selectedCorridorId) return;
  const ok = confirm("Delete all checkpoints for this corridor?");
  if (!ok) return;
  const out = await api(`/api/checkpoints?corridor_id=${selectedCorridorId}`, { method: "DELETE" });
  await refreshCheckpointTable();
  setStatus(`Deleted ${out.deleted} checkpoints.`);
}

async function autogenCheckpoints() {
  if (!selectedCorridorId) return;
  const body = {
    corridor_id: selectedCorridorId,
    from_query: $("autoFrom").value.trim(),
    to_query: $("autoTo").value.trim(),
    spacing_km: Number($("autoSpacingKm").value || 16),
    checkpoint_radius_km: Number($("autoCpRadiusKm").value || 12),
    max_points: Number($("autoMaxPoints").value || 16),
    use_roads: $("autoUseRoads").checked
  };
  if (!body.from_query || !body.to_query) {
    $("autoGenStatus").textContent = "Enter both from and to city/query.";
    return;
  }
  $("autoGenStatus").textContent = "Generating checkpoints...";
  const out = await api("/api/checkpoints/autogen", {
    method: "POST",
    body: JSON.stringify(body)
  });
  $("autoGenStatus").textContent =
    `Generated ${out.generated_count} (${out.route_source}) from ${out.from.label} to ${out.to.label}.`;
  await refreshCheckpointTable();
  setStatus("Auto-generated checkpoints saved.");
}

async function askQuestion() {
  const q = $("chatInput").value.trim();
  if (!q) return;
  $("chatAnswer").textContent = "Thinking...";
  const out = await api("/api/chat/query", {
    method: "POST",
    body: JSON.stringify({ corridor_id: selectedCorridorId, question: q })
  });
  $("chatAnswer").innerHTML = `<span class="pill">${out.modelUsed}</span><br><br>${out.answer}`;
}

function renderSettingsForm(settings) {
  SETTINGS_KEYS.forEach((k) => {
    const el = $(`set_${k}`);
    if (el) el.value = settings[k] ?? "";
  });
}

async function loadSettings() {
  const settings = await api("/api/settings");
  renderSettingsForm(settings);
  $("settingsStatus").textContent = "Settings loaded.";
}

async function saveSettings() {
  const body = {};
  SETTINGS_KEYS.forEach((k) => {
    const el = $(`set_${k}`);
    if (!el) return;
    const n = Number(el.value);
    if (Number.isFinite(n)) body[k] = n;
  });
  const out = await api("/api/settings", { method: "PUT", body: JSON.stringify(body) });
  renderSettingsForm(out);
  $("settingsStatus").textContent = "Settings saved. New values applied on next poll cycle.";
}

function renderInsights(ins) {
  if (!ins || !ins.summary) return;
  $("insAvgScore").textContent = Number(ins.summary.avg_score || 0).toFixed(1);
  $("insP90Score").textContent = Number(ins.summary.p90_score || 0).toFixed(1);
  $("insReliability").textContent = `${Number(ins.summary.reliability_tight_pct || 0).toFixed(1)}%`;
  $("insWeatherRisk").textContent = Number(ins.summary.avg_weather_risk || 0).toFixed(1);

  const labels = (ins.hourly_profile || []).map((x) => `${String(x.hour_local).padStart(2, "0")}:00`);
  const vals = (ins.hourly_profile || []).map((x) => Number(x.avg_score || 0));
  if (hourlyProfileChart) hourlyProfileChart.destroy();
  hourlyProfileChart = new Chart($("hourlyProfileChart"), {
    type: "bar",
    data: {
      labels,
      datasets: [{ label: "Avg Score by UTC Hour", data: vals, backgroundColor: "#66b0ff" }]
    },
    options: {
      maintainAspectRatio: false,
      plugins: { legend: { labels: { color: "#e8f0ff" } } },
      scales: {
        y: { min: 0, max: 100, ticks: { color: "#d5dff3" }, grid: { color: "rgba(255,255,255,0.1)" } },
        x: { ticks: { color: "#d5dff3" }, grid: { color: "rgba(255,255,255,0.06)" } }
      }
    }
  });

  const rec = $("insRecommendations");
  rec.innerHTML = "";
  (ins.recommendations || []).forEach((r) => {
    const li = document.createElement("li");
    li.textContent = r;
    rec.appendChild(li);
  });

  const hot = $("hotspotRows");
  hot.innerHTML = "";
  (ins.hotspots || []).forEach((h) => {
    const tr = document.createElement("tr");
    tr.innerHTML = `<td>${h.camera_location}</td>
      <td>${Number(h.avg_image_score || 0).toFixed(2)}</td>
      <td>${Number(h.max_image_score || 0).toFixed(2)}</td>
      <td>${Number(h.observations || 0)}</td>`;
    hot.appendChild(tr);
  });
}

async function runScenario() {
  if (!selectedCorridorId) return;
  const body = {
    corridor_id: selectedCorridorId,
    traffic_segments: Number($("simTrafficSegments").value || 0),
    delay_pct: Number($("simDelayPct").value || 0),
    image_score: Number($("simImageScore").value || 0),
    incidents_count: Number($("simIncidents").value || 0),
    closures_count: Number($("simClosures").value || 0),
    weather_risk_score: Number($("simWeatherRisk").value || 0)
  };
  const out = await api("/api/analysis/simulate", { method: "POST", body: JSON.stringify(body) });
  $("scenarioOut").innerHTML =
    `Fused score=${Number(out.fused.fused || 0).toFixed(1)} ` +
    `(traffic=${Number(out.fused.trafficComponent || 0).toFixed(1)}, image=${Number(out.fused.imageComponent || 0).toFixed(
      1
    )}, incidents=${Number(out.fused.incidentComponent || 0).toFixed(1)}, closures=${Number(
      out.fused.closureComponent || 0
    ).toFixed(1)}, weather=${Number(out.fused.weatherComponent || 0).toFixed(1)}). ` +
    `Forecast p50=${Number(out.prediction.p50 || 0).toFixed(1)}, p90=${Number(out.prediction.p90 || 0).toFixed(
      1
    )}, confidence=${Number(out.prediction.confidence || 0).toFixed(1)}%.`;
}

async function refreshWeatherLive() {
  if (!selectedCorridorId) return;
  const out = await api(`/api/weather/live?corridor_id=${selectedCorridorId}`);
  renderWeather(out);
  setStatus(`Weather refreshed ${new Date().toLocaleTimeString()}`);
}

async function pollNow() {
  setStatus("Polling...");
  await api("/api/poll-now", { method: "POST" });
  await refreshAll();
}

async function init() {
  document.querySelectorAll(".tab-btn").forEach((b) => b.addEventListener("click", () => activateTab(b.dataset.tab)));
  $("refreshBtn").addEventListener("click", refreshAll);
  $("pollNowBtn").addEventListener("click", pollNow);
  $("saveCorridorBtn").addEventListener("click", saveCorridor);
  $("archiveCorridorBtn").addEventListener("click", () => archiveSelectedCorridor(false));
  $("restoreCorridorBtn").addEventListener("click", () => archiveSelectedCorridor(true));
  $("saveCheckpointBtn").addEventListener("click", saveCheckpoint);
  $("clearCheckpointBtn").addEventListener("click", clearCheckpoints);
  $("autoGenerateBtn").addEventListener("click", autogenCheckpoints);
  $("refreshPreviewBtn").addEventListener("click", refreshCameraPreview);
  $("refreshWeatherBtn").addEventListener("click", refreshWeatherLive);
  $("radarPlayBtn").addEventListener("click", toggleRadarAnimation);
  $("radarPrevBtn").addEventListener("click", () => {
    stopRadarAnimation();
    setRadarFrameIndex(radarFrameIndex - 1);
  });
  $("radarNextBtn").addEventListener("click", () => {
    stopRadarAnimation();
    setRadarFrameIndex(radarFrameIndex + 1);
  });
  $("radarSpeedSelect").addEventListener("change", (e) => {
    const ms = Number(e.target.value || 800);
    radarAnimSpeedMs = Number.isFinite(ms) ? ms : 800;
    if (radarAnimPlaying) startRadarAnimation();
  });
  $("runScenarioBtn").addEventListener("click", runScenario);
  $("askBtn").addEventListener("click", askQuestion);
  $("reloadSettingsBtn").addEventListener("click", loadSettings);
  $("saveSettingsBtn").addEventListener("click", saveSettings);
  $("includeArchivedChk").addEventListener("change", async () => {
    await loadCorridors();
    await refreshAll();
  });
  $("corridorSelect").addEventListener("change", (e) => {
    selectedCorridorId = Number(e.target.value);
    selectedCamera = null;
    refreshAll();
  });
  $("hoursSelect").addEventListener("change", refreshAll);

  await loadCorridors();
  await loadSettings();
  await refreshAll();
  setInterval(refreshAll, 30000);
}

init().catch((e) => setStatus(`Error: ${e.message}`));
