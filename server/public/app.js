let selectedCorridorId = null;
let scoreChart;
let componentChart;
let hourlyProfileChart;
let latestRun = null;
let latestCameras = [];
let selectedCamera = null;
let hoverPreviewCameraKey = "";
let corridorsById = new Map();
let latestWeather = null;
let latestStrategiesPayload = null;
let selectedStrategyId = "";
let activeStrategyId = "";
let methodPresets = [];
let activeMethodPresetId = "";
let feedProfiles = [];
let radarMap = null;
let radarBaseLayer = null;
let radarWmsLayer = null;
let radarCorridorRect = null;
let radarFittedKey = "";
let radarFrameTimes = [];
let radarFrameIndex = -1;
let radarFrameSignature = "";
let radarStationFrames = [];
let radarStationSignature = "";
let radarAnimTimer = null;
let radarAnimPlaying = false;
let radarAnimSpeedMs = 800;
let radarProduct = "bref";
let radarViewMode = "corridor";
let radarSourceMode = "auto";
let radarResolvedMode = "wms";
let radarStationZoom = 1;
const RADAR_STATION_ZOOM_MIN = 1;
const RADAR_STATION_ZOOM_MAX = 5;
const RADAR_STATION_ZOOM_STEP = 0.2;
const RADAR_PRODUCTS = {
  bref: { layer: "conus:conus_bref_qcd", label: "Base Reflectivity" },
  cref: { layer: "conus:conus_cref_qcd", label: "Composite Reflectivity" }
};
let routeMap = null;
let routeForwardSegmentsLayer = null;
let routeReverseSegmentsLayer = null;
let routeForwardLineLayer = null;
let routeReverseLineLayer = null;
let latestRouteMapPayload = null;
let directionCompareVisible = false;
let refreshInFlight = false;
let refreshQueued = false;
let refreshTicket = 0;
let radiusTunePollTimer = null;
const SETTINGS_KEYS = [
  "poll_seconds",
  "sample_limit",
  "sample_limit_max",
  "baseline_alpha",
  "min_scoped_cameras_for_strict",
  "adaptive_camera_sampling_enabled",
  "adaptive_sample_min",
  "adaptive_sample_max",
  "adaptive_conf_k",
  "adaptive_conf_gain_threshold",
  "camera_hard_drop_enabled",
  "camera_drop_too_old_flag",
  "camera_hard_max_age_minutes",
  "camera_missing_snapshot_penalty",
  "camera_score_weight_fresh",
  "camera_score_weight_proximity",
  "camera_score_weight_scope",
  "camera_non_scoped_penalty",
  "camera_fresh_ref_minutes",
  "camera_proximity_scale_km",
  "camera_diversity_ref_km",
  "camera_diversity_bonus_weight",
  "cv_enabled",
  "cv_provider_mode",
  "cv_max_cameras_per_poll",
  "cv_target_coverage_pct",
  "cv_timeout_ms",
  "cv_min_confidence",
  "cv_blend_strength",
  "cv_temporal_alpha",
  "cv_stopped_persist_bonus",
  "weight_traffic",
  "weight_image_with_traffic",
  "weight_image_no_traffic",
  "image_signal_mode",
  "queue_stop_radius_km",
  "queue_stop_slow_speed_mph",
  "queue_stop_stop_speed_mph",
  "queue_stop_min_segments",
  "queue_stop_delay_weight",
  "queue_stop_slow_weight",
  "queue_stop_stop_weight",
  "hybrid_confidence_floor",
  "hybrid_confidence_gain",
  "hybrid_camera_weight",
  "hybrid_traffic_weight",
  "incident_relevance_mode",
  "incident_route_buffer_km_relaxed",
  "incident_route_buffer_km_strict",
  "incident_relaxed_direction_penalty",
  "incident_saturation_max",
  "incident_saturation_k",
  "closure_saturation_max",
  "closure_saturation_k",
  "dynamic_weights_enabled",
  "auto_learn_weights_enabled",
  "auto_learn_window_points",
  "auto_learn_min_rows",
  "auto_learn_ridge_lambda",
  "auto_learn_strength",
  "dynamic_weight_floor",
  "dynamic_weight_cap",
  "reliability_gamma",
  "camera_coverage_ref",
  "traffic_missing_penalty",
  "weather_missing_penalty",
  "regime_rush_traffic",
  "regime_rush_image",
  "regime_rush_incident",
  "regime_rush_closure",
  "regime_rush_weather",
  "regime_overnight_traffic",
  "regime_overnight_image",
  "regime_overnight_incident",
  "regime_overnight_closure",
  "regime_overnight_weather",
  "regime_weekend_traffic",
  "regime_weekend_image",
  "regime_weekend_incident",
  "regime_weekend_closure",
  "regime_weekend_weather",
  "regime_severe_weather_traffic",
  "regime_severe_weather_image",
  "regime_severe_weather_incident",
  "regime_severe_weather_closure",
  "regime_severe_weather_weather",
  "lock_mult_traffic",
  "lock_mult_image",
  "lock_mult_incident",
  "lock_mult_closure",
  "lock_mult_weather",
  "locked_mult_traffic",
  "locked_mult_image",
  "locked_mult_incident",
  "locked_mult_closure",
  "locked_mult_weather",
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
  if (name === "route") {
    if (latestRouteMapPayload) {
      setTimeout(() => routeMap?.invalidateSize(), 120);
    } else {
      refreshRouteMapView().catch(() => {});
    }
  }
}

function selectedCorridor() {
  return corridorsById.get(Number(selectedCorridorId)) || null;
}

function normalizeFeedProfileLabel(profileId) {
  const id = String(profileId || "").trim();
  if (!id) return "il_arcgis";
  return id;
}

function syncCorridorFormFromSelected() {
  const c = selectedCorridor();
  if (!c) return;
  const profile = normalizeFeedProfileLabel(c.feed_profile || "il_arcgis");
  if ($("corridorFeedProfile")) $("corridorFeedProfile").value = profile;
  const districts =
    Array.isArray(c.feed_config?.districts) && c.feed_config.districts.length
      ? c.feed_config.districts.join(",")
      : "";
  if ($("corridorCaDistricts")) $("corridorCaDistricts").value = districts;
}

function renderFeedProfileSelect() {
  const sel = $("corridorFeedProfile");
  if (!sel) return;
  sel.innerHTML = "";
  const items = Array.isArray(feedProfiles) && feedProfiles.length
    ? feedProfiles
    : [
        { id: "il_arcgis", name: "Illinois ArcGIS" },
        { id: "ca_cwwp2", name: "California CWWP2" }
      ];
  items.forEach((p) => {
    const o = document.createElement("option");
    o.value = p.id;
    o.textContent = `${p.name} (${p.id})`;
    sel.appendChild(o);
  });
}

async function loadFeedProfiles() {
  try {
    const out = await api("/api/feed-profiles");
    feedProfiles = Array.isArray(out?.profiles) ? out.profiles : [];
  } catch {
    feedProfiles = [
      { id: "il_arcgis", name: "Illinois ArcGIS" },
      { id: "ca_cwwp2", name: "California CWWP2" }
    ];
  }
  renderFeedProfileSelect();
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
    const profile = normalizeFeedProfileLabel(c.feed_profile || "il_arcgis");
    o.textContent = `${c.name} (${status}) [${profile}] [${Number(c.xmin).toFixed(2)},${Number(c.ymin).toFixed(2)} -> ${Number(
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
  syncCorridorFormFromSelected();
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

function cameraProxyUrl(snapshotUrl) {
  const raw = String(snapshotUrl || "").trim();
  if (!raw) return "";
  try {
    const u = new URL(raw);
    if (u.protocol !== "http:" && u.protocol !== "https:") return "";
    return `/api/camera/preview?url=${encodeURIComponent(u.toString())}`;
  } catch {
    return "";
  }
}

function setPreviewImageSource(img, snapshotUrl) {
  if (!img) return;
  const raw = String(snapshotUrl || "").trim();
  if (!raw) {
    img.removeAttribute("src");
    return;
  }
  const proxy = cameraProxyUrl(raw);
  const ts = Date.now();
  const proxySrc = proxy ? `${proxy}${proxy.includes("?") ? "&" : "?"}ts=${ts}` : "";
  const rawSrc = `${raw}${raw.includes("?") ? "&" : "?"}ts=${ts}`;
  let triedRaw = false;
  img.onerror = () => {
    if (!triedRaw && proxySrc) {
      triedRaw = true;
      img.src = rawSrc;
      return;
    }
    img.onerror = null;
  };
  img.src = proxySrc || rawSrc;
}

function positionCameraHoverCard(evt) {
  const card = $("cameraHoverCard");
  if (!card || !evt) return;
  const pad = 14;
  const gap = 18;
  const rect = card.getBoundingClientRect();
  const vw = window.innerWidth || 1200;
  const vh = window.innerHeight || 900;
  let left = evt.clientX + gap;
  let top = evt.clientY - rect.height * 0.35;
  if (left + rect.width + pad > vw) left = Math.max(pad, evt.clientX - rect.width - gap);
  if (top + rect.height + pad > vh) top = vh - rect.height - pad;
  if (top < pad) top = pad;
  card.style.left = `${Math.round(left)}px`;
  card.style.top = `${Math.round(top)}px`;
}

function hideCameraHoverPreview() {
  const card = $("cameraHoverCard");
  if (!card) return;
  card.style.display = "none";
  card.style.left = "-9999px";
  card.style.top = "-9999px";
  hoverPreviewCameraKey = "";
}

function showCameraHoverPreview(cam, evt) {
  if (!cam || !cam.snapshot_url) return;
  if (window.matchMedia && window.matchMedia("(max-width: 1100px)").matches) return;
  const card = $("cameraHoverCard");
  const img = $("cameraHoverImg");
  const title = $("cameraHoverTitle");
  const meta = $("cameraHoverMeta");
  if (!card || !img || !title || !meta) return;

  card.style.display = "block";
  title.textContent = cam.camera_location || "Camera";
  const key = `${cam.camera_object_id || ""}|${cam.snapshot_url || ""}`;
  if (key !== hoverPreviewCameraKey) {
    hoverPreviewCameraKey = key;
    setPreviewImageSource(img, cam.snapshot_url);
  }
  meta.textContent =
    `Dir=${cam.camera_direction || "-"} | age=${Number(cam.age_minutes || 0).toFixed(1)} min | ` +
    `selected=${Number(cam.image_score || 0).toFixed(2)} | queue_stop=${Number(cam.queue_stop_score || 0).toFixed(2)} | ` +
    `hybrid=${Number(cam.hybrid_score || 0).toFixed(2)} (${(Number(cam.hybrid_confidence || 0) * 100).toFixed(1)}%)`;
  positionCameraHoverCard(evt);
}

function renderCameraRows(rows) {
  latestCameras = rows || [];
  const tb = $("cameraRows");
  tb.innerHTML = "";
  hideCameraHoverPreview();
  const addTextCell = (tr, val) => {
    const td = document.createElement("td");
    td.textContent = val == null || val === "" ? "-" : String(val);
    tr.appendChild(td);
  };
  rows.forEach((r) => {
    const tr = document.createElement("tr");
    tr.style.cursor = "pointer";
    addTextCell(tr, r.camera_location || "-");
    addTextCell(tr, r.camera_direction || "-");
    addTextCell(tr, Number(r.snapshot_bytes || 0));
    addTextCell(tr, Number(r.age_minutes || 0).toFixed(1));
    addTextCell(tr, Number(r.image_score || 0).toFixed(2));
    addTextCell(tr, Number(r.proxy_score || 0).toFixed(2));
    addTextCell(tr, Number(r.queue_stop_score || 0).toFixed(2));
    addTextCell(tr, Number(r.hybrid_score || 0).toFixed(2));
    addTextCell(tr, (Number(r.hybrid_confidence || 0) * 100).toFixed(1));
    addTextCell(tr, Number(r.local_delay_pct || 0).toFixed(1));
    const snapshotTd = document.createElement("td");
    const snap = String(r.snapshot_url || "").trim();
    if (snap) {
      try {
        const u = new URL(snap);
        if (u.protocol === "http:" || u.protocol === "https:") {
          const a = document.createElement("a");
          a.href = cameraProxyUrl(u.toString()) || u.toString();
          a.target = "_blank";
          a.rel = "noreferrer";
          a.textContent = "open";
          snapshotTd.appendChild(a);
        } else {
          snapshotTd.textContent = "-";
        }
      } catch {
        snapshotTd.textContent = "-";
      }
    } else {
      snapshotTd.textContent = "-";
    }
    tr.appendChild(snapshotTd);
    tr.addEventListener("mouseenter", (e) => {
      setCameraPreview(r);
      showCameraHoverPreview(r, e);
    });
    tr.addEventListener("mousemove", (e) => showCameraHoverPreview(r, e));
    tr.addEventListener("mouseleave", hideCameraHoverPreview);
    tr.addEventListener("click", () => setCameraPreview(r));
    tb.appendChild(tr);
  });
  if (
    selectedCamera &&
    !rows.some((r) => Number(r.camera_object_id || 0) === Number(selectedCamera.camera_object_id || 0))
  ) {
    selectedCamera = null;
  }
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
  setPreviewImageSource(img, cam.snapshot_url);
  const runTs = cam.run_ts ? new Date(cam.run_ts).toLocaleString() : "unknown";
  const cvPart =
    cam.cv_provider && cam.cv_provider !== "disabled"
      ? ` | cv_provider=${cam.cv_provider} | cv_queue=${Number(cam.cv_queue_score || 0).toFixed(2)} | cv_conf=${Number(
          cam.cv_confidence || 0
        ).toFixed(2)} | vehicles=${Number(cam.cv_vehicle_count || 0)} | stopped=${Number(cam.cv_stopped_count || 0)}`
      : "";
  meta.textContent =
    `${cam.camera_location || "-"} | dir=${cam.camera_direction || "-"} | age=${Number(cam.age_minutes || 0).toFixed(
      1
    )} min | observed=${runTs} | selected=${Number(cam.image_score || 0).toFixed(2)} | proxy=${Number(
      cam.proxy_score || 0
    ).toFixed(2)} | queue_stop=${Number(cam.queue_stop_score || 0).toFixed(2)} | hybrid=${Number(
      cam.hybrid_score || 0
    ).toFixed(2)} | conf=${(Number(cam.hybrid_confidence || 0) * 100).toFixed(1)}%${cvPart}`;
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
    const td = (val) => {
      const c = document.createElement("td");
      c.textContent = String(val);
      return c;
    };
    tr.appendChild(td(new Date(r.run_ts).toLocaleString()));
    tr.appendChild(td(Number(r.fused_score || 0).toFixed(1)));
    tr.appendChild(td(Number(r.predicted_next_score_p50 || 0).toFixed(1)));
    tr.appendChild(td(Number(r.predicted_next_score_p90 || 0).toFixed(1)));
    tr.appendChild(td(`${Number(r.prediction_confidence || 0).toFixed(1)}%`));
    tr.appendChild(td(Number(r.drift_score || 0).toFixed(1)));
    const alertTd = document.createElement("td");
    const pill = document.createElement("span");
    pill.className = "pill";
    pill.textContent = r.alert_state || "-";
    alertTd.appendChild(pill);
    tr.appendChild(alertTd);
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

function normalizeRadarFrameUrls(values) {
  const out = [];
  const seen = new Set();
  for (const v of Array.isArray(values) ? values : []) {
    const s = String(v || "").trim();
    if (!s) continue;
    try {
      const u = new URL(s);
      if (u.protocol !== "http:" && u.protocol !== "https:") continue;
      const key = u.toString();
      if (seen.has(key)) continue;
      seen.add(key);
      out.push(key);
    } catch {
      continue;
    }
  }
  return out;
}

function resolveRadarMode(weather) {
  const hasStation = normalizeRadarFrameUrls(weather?.radar_station_frames || []).length > 0;
  if (radarSourceMode === "station") return hasStation ? "station" : "wms";
  if (radarSourceMode === "wms") return "wms";
  return hasStation ? "station" : "wms";
}

function activeRadarFrames() {
  return radarResolvedMode === "station" ? radarStationFrames : radarFrameTimes;
}

function radarImgWrapVisible() {
  const wrap = $("radarImgWrap");
  return !!(wrap && wrap.style.display !== "none");
}

function applyRadarStationZoom() {
  const img = $("radarImg");
  if (!img) return;
  img.style.transform = `scale(${radarStationZoom.toFixed(2)})`;
}

function updateRadarZoomLabel() {
  const el = $("radarZoomLabel");
  if (!el) return;
  if (radarResolvedMode === "station") {
    el.textContent = `Zoom ${radarStationZoom.toFixed(2)}x`;
    return;
  }
  if (radarMap) {
    el.textContent = `Zoom z${Number(radarMap.getZoom() || 0)}`;
    return;
  }
  el.textContent = "Zoom -";
}

function setRadarStationZoom(nextZoom) {
  const next = Number(nextZoom);
  if (!Number.isFinite(next)) return;
  radarStationZoom = Math.max(RADAR_STATION_ZOOM_MIN, Math.min(RADAR_STATION_ZOOM_MAX, next));
  applyRadarStationZoom();
  updateRadarZoomLabel();
}

function radarZoomIn() {
  if (radarResolvedMode === "station") {
    setRadarStationZoom(radarStationZoom + RADAR_STATION_ZOOM_STEP);
    return;
  }
  if (radarMap) {
    radarMap.zoomIn();
    updateRadarZoomLabel();
  }
}

function radarZoomOut() {
  if (radarResolvedMode === "station") {
    setRadarStationZoom(radarStationZoom - RADAR_STATION_ZOOM_STEP);
    return;
  }
  if (radarMap) {
    radarMap.zoomOut();
    updateRadarZoomLabel();
  }
}

function radarZoomReset() {
  if (radarResolvedMode === "station") {
    setRadarStationZoom(1);
    return;
  }
  if (latestWeather) {
    radarFittedKey = "";
    renderRadarMap(latestWeather);
  } else if (radarMap) {
    updateRadarZoomLabel();
  }
}

function updateRadarFrameLabel() {
  const el = $("radarFrameLabel");
  if (!el) return;
  const frames = activeRadarFrames();
  if (!frames.length || radarFrameIndex < 0) {
    el.textContent = radarResolvedMode === "station" ? "Frame: station live" : "Frame: live";
    return;
  }
  if (radarResolvedMode === "station") {
    el.textContent = `Station frame ${radarFrameIndex + 1}/${frames.length}`;
    return;
  }
  const ts = new Date(frames[radarFrameIndex]);
  const shown = Number.isFinite(ts.getTime()) ? ts.toLocaleTimeString() : frames[radarFrameIndex];
  el.textContent = `Frame ${radarFrameIndex + 1}/${frames.length} | ${shown}`;
}

function applyRadarFrameToLayers() {
  const frames = activeRadarFrames();
  const frame = frames.length && radarFrameIndex >= 0 ? frames[radarFrameIndex] : "";
  const radarImg = $("radarImg");
  if (radarResolvedMode === "station") {
    if (radarImg && radarImgWrapVisible()) {
      const chosen = frame || String(latestWeather?.radar_station_loop_gif_url || "");
      if (chosen) {
        radarImg.src = `${chosen}${chosen.includes("?") ? "&" : "?"}ts=${Date.now()}`;
      }
    }
    updateRadarFrameLabel();
    return;
  }
  if (radarWmsLayer) {
    radarWmsLayer.setParams({ time: frame, _ts: Date.now() });
  }
  if (radarImg && radarImgWrapVisible() && latestWeather?.radar_image_url) {
    const baseUrl = radarImageUrlForProduct(latestWeather.radar_image_url);
    const sep = baseUrl.includes("?") ? "&" : "?";
    const t = frame ? `&time=${encodeURIComponent(frame)}` : "";
    radarImg.src = `${baseUrl}${sep}ts=${Date.now()}${t}`;
  }
  updateRadarFrameLabel();
}

function setRadarFrameIndex(nextIndex) {
  const frames = activeRadarFrames();
  if (!frames.length) return;
  const n = frames.length;
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
  if (!activeRadarFrames().length) return;
  stopRadarAnimation();
  radarAnimPlaying = true;
  const btn = $("radarPlayBtn");
  if (btn) btn.textContent = "Pause Loop";
  radarAnimTimer = setInterval(() => {
    setRadarFrameIndex(radarFrameIndex + 1);
  }, Math.max(250, radarAnimSpeedMs));
}

function toggleRadarAnimation() {
  if (!activeRadarFrames().length) return;
  if (radarAnimPlaying) stopRadarAnimation();
  else startRadarAnimation();
}

function setRadarFramesFromPayload(weather) {
  const times = normalizeRadarTimes(weather?.radar_frame_times || []);
  const wmsSignature = times.join("|");
  const wmsChanged = wmsSignature !== radarFrameSignature;
  if (wmsChanged) {
    radarFrameTimes = times;
    radarFrameSignature = wmsSignature;
  }

  const stationFrames = normalizeRadarFrameUrls(weather?.radar_station_frames || []);
  const stationSignature = stationFrames.join("|");
  const stationChanged = stationSignature !== radarStationSignature;
  if (stationChanged) {
    radarStationFrames = stationFrames;
    radarStationSignature = stationSignature;
  }

  const frames = activeRadarFrames();
  if (!frames.length) {
    radarFrameIndex = -1;
    stopRadarAnimation();
    updateRadarFrameLabel();
    return;
  }

  const activeChanged = radarResolvedMode === "station" ? stationChanged : wmsChanged;
  if (activeChanged || radarFrameIndex < 0 || radarFrameIndex >= frames.length) {
    if (radarResolvedMode === "station") {
      radarFrameIndex = Math.max(0, frames.length - 1);
    } else {
      let idx = -1;
      if (weather?.radar_time_default_utc) {
        const d = new Date(weather.radar_time_default_utc);
        if (Number.isFinite(d.getTime())) idx = frames.lastIndexOf(d.toISOString());
      }
      if (idx < 0) idx = frames.length - 1;
      radarFrameIndex = idx;
    }
  }
  updateRadarFrameLabel();
}

function renderWeather(w) {
  latestWeather = w;
  if (!w) return;
  radarResolvedMode = resolveRadarMode(w);
  const sourceSel = $("radarSourceSelect");
  if (sourceSel) sourceSel.value = radarSourceMode;

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

  setRadarFramesFromPayload(w);

  const staticLink = $("radarStaticLink");
  if (radarResolvedMode === "station" && w.radar_station_loop_gif_url) {
    staticLink.href = w.radar_station_loop_gif_url;
    staticLink.textContent = "Open station GIF loop";
  } else if (w.radar_image_url) {
    const productUrl = radarImageUrlForProduct(w.radar_image_url);
    staticLink.href = productUrl;
    staticLink.textContent = "Open static radar image";
  } else {
    staticLink.removeAttribute("href");
    staticLink.textContent = "";
  }

  const hintedLayer = String(w.radar_wms_layer || "");
  if (hintedLayer.includes("cref")) radarProduct = "cref";
  else if (hintedLayer.includes("bref")) radarProduct = "bref";
  const productSel = $("radarProductSelect");
  if (productSel) productSel.value = radarProduct;

  if (radarResolvedMode === "station" && (radarStationFrames.length || w.radar_station_loop_gif_url)) {
    renderRadarStation(w);
  } else if (w.radar_image_url) {
    renderRadarMap(w);
  } else {
    const mapEl = $("radarMap");
    if (mapEl) mapEl.style.display = "none";
    const wrap = $("radarImgWrap");
    if (wrap) wrap.style.display = "none";
  }

  const fetchedAt = w.fetched_at ? new Date(w.fetched_at).toLocaleString() : "unknown";
  const runTs = w.run_ts ? new Date(w.run_ts).toLocaleString() : "";
  const sourceName =
    radarResolvedMode === "station" ? "NWS Station GIF loop" : "NOAA OpenGeo MRMS map + OSM landmarks";
  const fallbackMsg =
    radarSourceMode === "station" && radarResolvedMode !== "station"
      ? " | station GIF unavailable at this corridor center, using MRMS"
      : "";
  $("radarMeta").textContent =
    `Radar source: ${sourceName}${fallbackMsg} | fetched: ${fetchedAt}${runTs ? ` | from run: ${runTs}` : ""}${
      w.from_live_fetch ? " | live fetch" : " | saved run"
    }`;
  const hintEl = $("radarLoopHint");
  if (hintEl) hintEl.textContent = radarLoopHint(w);
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

function radarImageUrlForProduct(url) {
  if (!url) return "";
  try {
    const u = new URL(url);
    const layer = getRadarLayerName();
    const existing = u.searchParams.get("layers") || "";
    if (existing.includes("geopolitical")) {
      u.searchParams.set("layers", `geopolitical,${layer}`);
    } else {
      u.searchParams.set("layers", layer);
    }
    return u.toString();
  } catch {
    return url;
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

function getRadarLayerName() {
  return (RADAR_PRODUCTS[radarProduct] || RADAR_PRODUCTS.bref).layer;
}

function applyRadarProductToLayer() {
  if (!radarWmsLayer) return;
  radarWmsLayer.setParams({
    layers: getRadarLayerName(),
    _ts: Date.now()
  });
}

function radarLoopHint(weather) {
  if (radarResolvedMode === "station") {
    return "Animating NWS station GIF frames (RIDGE). Updates are controlled by NWS publish cadence.";
  }
  const precip = Number(weather?.precip_probability_pct || 0);
  const cond = String(weather?.condition_text || "").toLowerCase();
  const foggy = /fog|mist|haze|smoke/.test(cond);
  if (foggy && precip <= 20) {
    return "Loop may look static: reflectivity radar tracks precip echoes, not fog visibility drops.";
  }
  if (precip <= 10) {
    return "Low precip probability: radar frames can appear nearly unchanged in dry periods.";
  }
  return "";
}

function renderRadarStation(weather) {
  const mapEl = $("radarMap");
  const wrap = $("radarImgWrap");
  const imgEl = $("radarImg");
  if (mapEl) mapEl.style.display = "none";
  if (wrap) wrap.style.display = "block";
  if (!imgEl) return;
  applyRadarStationZoom();

  if (!radarStationFrames.length) {
    const loop = String(weather?.radar_station_loop_gif_url || "");
    if (loop) {
      imgEl.src = `${loop}${loop.includes("?") ? "&" : "?"}ts=${Date.now()}`;
    }
    return;
  }
  if (radarFrameIndex < 0 || radarFrameIndex >= radarStationFrames.length) {
    radarFrameIndex = Math.max(0, radarStationFrames.length - 1);
  }
  applyRadarFrameToLayers();
  updateRadarZoomLabel();
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
    layers: getRadarLayerName(),
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
  radarMap.on("zoomend", () => {
    if (radarResolvedMode !== "station") updateRadarZoomLabel();
  });
}

function renderRadarMap(weather) {
  ensureRadarMap();
  if (!radarMap || !radarWmsLayer) {
    const radarImg = $("radarImg");
    const wrap = $("radarImgWrap");
    if (wrap) wrap.style.display = "block";
    applyRadarFrameToLayers();
    if (!radarImg.src) {
      const productUrl = radarImageUrlForProduct(weather.radar_image_url);
      radarImg.src = `${productUrl}${productUrl.includes("?") ? "&" : "?"}ts=${Date.now()}`;
    }
    return;
  }

  $("radarMap").style.display = "block";
  const wrap = $("radarImgWrap");
  if (wrap) wrap.style.display = "none";

  applyRadarProductToLayer();

  const bboxRaw = getSelectedCorridorBbox() || parseBboxFromRadarUrl(weather.radar_image_url);
  const bbox =
    bboxRaw && radarViewMode === "regional"
      ? expandBbox(bboxRaw, 1.35, 1.1)
      : bboxRaw
        ? expandBbox(bboxRaw, 0.42, 0.2)
        : null;

  applyRadarFrameToLayers();
  if (bboxRaw && radarCorridorRect) {
    radarCorridorRect.setBounds([
      [bboxRaw.ymin, bboxRaw.xmin],
      [bboxRaw.ymax, bboxRaw.xmax]
    ]);
  }

  const fitKey = `${Number(selectedCorridorId)}:${radarViewMode}`;
  const shouldFit = radarFittedKey !== fitKey;
  if (bbox && shouldFit) {
    radarMap.fitBounds(
      [
        [bbox.ymin, bbox.xmin],
        [bbox.ymax, bbox.xmax]
      ],
      { padding: [18, 18], maxZoom: 10 }
    );
    radarFittedKey = fitKey;
  }
  setTimeout(() => radarMap.invalidateSize(), 120);
  updateRadarZoomLabel();
}

function renderModelMeta(model) {
  if (!model) {
    $("modelMeta").textContent = "No model snapshot yet.";
    $("predictionDetail").textContent = "-";
    return;
  }
  const meta = $("modelMeta");
  meta.innerHTML = "";
  const line = (txt) => {
    const div = document.createElement("div");
    div.textContent = txt;
    meta.appendChild(div);
  };
  line(`Version: ${model.model_version}`);
  line(`Train rows: ${model.train_rows}`);
  line(`Residual sigma: ${Number(model.residual_sigma).toFixed(2)}`);
  line(`R²: ${Number(model.r2).toFixed(3)}`);
  line(`Drift: ${Number(model.drift_score).toFixed(2)}`);
  if (latestRun) {
    $("predictionDetail").textContent =
      `Current score ${Number(latestRun.fused_score).toFixed(1)} -> ` +
      `next p50 ${Number(latestRun.predicted_next_score_p50).toFixed(1)}, ` +
      `p90 ${Number(latestRun.predicted_next_score_p90).toFixed(1)}, ` +
      `confidence ${Number(latestRun.prediction_confidence).toFixed(1)}%.`;
  }
}

function renderWeights(payload) {
  const statusEl = $("weightStatus");
  const regimeEl = $("weightRegime");
  const fitEl = $("weightFit");
  const rowsEl = $("weightRows");
  const explainEl = $("weightExplain");

  if (!payload || !payload.weighting) {
    if (statusEl) statusEl.textContent = "No weighting metadata available yet. Run Poll Now first.";
    if (regimeEl) regimeEl.textContent = "-";
    if (fitEl) fitEl.textContent = "-";
    if (rowsEl) rowsEl.innerHTML = "";
    if (explainEl) explainEl.textContent = "No weighting explanation yet.";
    return;
  }

  const w = payload.weighting || {};
  const eff = w.effective_multipliers || {};
  const shares = w.weighted_shares_pct || {};
  const fit = payload.auto_learn_fit || w.auto_learn_fit || {};
  const img = payload.image_signals || {};
  const sel = payload.camera_selection || {};
  const profile = w.profile_multipliers || {};
  const reliability = w.reliability_multipliers || {};
  const learned = w.learned_multipliers || {};

  if (statusEl) {
    statusEl.textContent = `Source=${payload.weighting_source || "unknown"} | dynamic=${
      w.dynamic_enabled ? "ON" : "OFF"
    } | fused=${Number(payload.fused_score || 0).toFixed(1)}`;
  }
  if (regimeEl) regimeEl.textContent = w.regime || "normal";
  if (fitEl) {
    fitEl.textContent = `rows=${Number(fit.rows || 0)}, r2=${Number(fit.r2 || 0).toFixed(3)}, rmse=${Number(
      fit.rmse || 0
    ).toFixed(3)}`;
  }

  if (rowsEl) {
    rowsEl.innerHTML = "";
    [
      ["Traffic", "traffic"],
      ["Image", "image"],
      ["Incidents", "incident"],
      ["Closures", "closure"],
      ["Weather", "weather"]
    ].forEach(([label, key]) => {
      const tr = document.createElement("tr");
      tr.innerHTML = `<td>${label}</td>
        <td>${Number(eff[key] ?? 1).toFixed(2)}</td>
        <td>${Number(shares[key] ?? 0).toFixed(1)}%</td>`;
      rowsEl.appendChild(tr);
    });
  }

  if (explainEl) {
    explainEl.innerHTML =
      `Image signals: mode=${img.mode_label || "hybrid_auto"}, selected=${Number(img.selected_score || 0).toFixed(
        1
      )}, proxy=${Number(img.proxy_score || 0).toFixed(1)}, queue_stop=${Number(img.queue_stop_score || 0).toFixed(
        1
      )}, hybrid=${Number(img.hybrid_score || 0).toFixed(1)}, confidence=${Number(
        img.hybrid_confidence_pct || 0
      ).toFixed(1)}%.<br>` +
      `Camera selection: fallback=${sel.using_scoped_fallback ? "yes" : "no"}, selected=${Number(
        sel.selected_count || 0
      )}, target=${Number(sel.target_count || 0)}, fresh=${Number(sel.fresh_pct || 0).toFixed(1)}%, dropped=${escapeHtml(
        JSON.stringify(sel.dropped || {})
      )}.<br>` +
      `Profile multipliers: traffic=${Number(profile.traffic ?? 1).toFixed(2)}, image=${Number(
        profile.image ?? 1
      ).toFixed(2)}, incidents=${Number(profile.incident ?? 1).toFixed(2)}, closures=${Number(
        profile.closure ?? 1
      ).toFixed(2)}, weather=${Number(profile.weather ?? 1).toFixed(2)}.<br>` +
      `Reliability multipliers: traffic=${Number(reliability.traffic ?? 1).toFixed(2)}, image=${Number(
        reliability.image ?? 1
      ).toFixed(2)}, incidents=${Number(reliability.incident ?? 1).toFixed(2)}, closures=${Number(
        reliability.closure ?? 1
      ).toFixed(2)}, weather=${Number(reliability.weather ?? 1).toFixed(2)}.<br>` +
      `Auto-learn multipliers: traffic=${Number(learned.traffic ?? 1).toFixed(2)}, image=${Number(
        learned.image ?? 1
      ).toFixed(2)}, incidents=${Number(learned.incident ?? 1).toFixed(2)}, closures=${Number(
        learned.closure ?? 1
      ).toFixed(2)}, weather=${Number(learned.weather ?? 1).toFixed(2)}.`;
  }
}

async function refreshWeights() {
  if (!selectedCorridorId) return;
  const out = await api(`/api/weights/current?corridor_id=${selectedCorridorId}`);
  renderWeights(out);
}

async function refreshCheckpointTable(corridorId = selectedCorridorId) {
  if (!corridorId) return;
  const cps = await api(`/api/checkpoints?corridor_id=${corridorId}`);
  const tb = $("checkpointRows");
  tb.innerHTML = "";
  cps.forEach((cp) => {
    const tr = document.createElement("tr");
    const td = (v) => {
      const cell = document.createElement("td");
      cell.textContent = v == null ? "-" : String(v);
      return cell;
    };
    tr.appendChild(td(cp.name || "-"));
    tr.appendChild(td(Number(cp.lat).toFixed(5)));
    tr.appendChild(td(Number(cp.lon).toFixed(5)));
    tr.appendChild(td(Number(cp.radius_km || 0).toFixed(1)));
    tr.appendChild(td(cp.source || "-"));
    tb.appendChild(tr);
  });
}

async function refreshAll() {
  if (!selectedCorridorId) return;
  if (refreshInFlight) {
    refreshQueued = true;
    return;
  }
  refreshInFlight = true;
  const ticket = ++refreshTicket;
  const corridorId = Number(selectedCorridorId);
  const hours = Number($("hoursSelect").value || 24);
  setStatus("Refreshing...");
  try {
    const primaryPromise = Promise.all([
      api(`/api/runs/latest?corridor_id=${corridorId}`),
      api(`/api/runs/timeseries?corridor_id=${corridorId}&hours=${hours}`),
      api(`/api/runs/recent-cameras?corridor_id=${corridorId}`),
      api(`/api/models/latest?corridor_id=${corridorId}`),
      api(`/api/analysis/insights?corridor_id=${corridorId}&hours=${hours}`),
      api(`/api/weather/current?corridor_id=${corridorId}`),
      api(`/api/weights/current?corridor_id=${corridorId}`)
    ]);
    const strategyPromise = api(`/api/analysis/strategy-presets?corridor_id=${corridorId}&hours=${hours}`).catch(() => null);
    const [[latest, ts, cams, model, insights, weather, weights], strategies] = await Promise.all([
      primaryPromise,
      strategyPromise
    ]);

    if (ticket !== refreshTicket || corridorId !== Number(selectedCorridorId)) return;

    renderKpis(latest);
    renderScoreChart(ts);
    renderComponents(latest);
    renderCameraRows(cams);
    renderTsRows(ts);
    renderModelMeta(model);
    renderInsights(insights);
    if (strategies) renderStrategies(strategies);
    renderWeather(weather);
    renderWeights(weights);
    await refreshCheckpointTable(corridorId);
    await refreshRadiusTuneStatus();
    const routeTabActive = $("tab-route")?.classList?.contains("active");
    if (routeTabActive || directionCompareVisible) {
      try {
        await refreshRouteMapView();
      } catch (e) {
        const routeStatus = $("routeMapStatus");
        if (routeStatus) routeStatus.textContent = `Route map refresh failed: ${e.message || e}`;
      }
    }
    if (ticket !== refreshTicket || corridorId !== Number(selectedCorridorId)) return;
    setStatus(`Updated ${new Date().toLocaleTimeString()}`);
  } catch (e) {
    if (ticket === refreshTicket) {
      setStatus(`Refresh failed: ${e.message || e}`);
    }
  } finally {
    refreshInFlight = false;
    if (refreshQueued) {
      refreshQueued = false;
      setTimeout(() => {
        refreshAll();
      }, 0);
    }
  }
}

async function saveCorridor() {
  const feedProfile = $("corridorFeedProfile")?.value || "il_arcgis";
  const caDistrictsCsv = $("corridorCaDistricts")?.value?.trim() || "";
  const body = {
    name: $("corridorName").value.trim(),
    city: $("corridorCity").value.trim() || undefined,
    bbox: $("corridorBbox").value.trim() || undefined,
    radius_km: Number($("corridorRadius").value || 20),
    feed_profile: feedProfile,
    ca_districts_csv: caDistrictsCsv || undefined
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
    use_roads: $("autoUseRoads").checked,
    create_reverse: $("autoCreateReverse")?.checked === true,
    reverse_corridor_name: $("autoReverseName")?.value?.trim() || undefined
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
  const reverseText = out.reverse
    ? ` Reverse corridor: ${out.reverse.corridor_name} (${out.reverse.generated_count} checkpoints).`
    : "";
  $("autoGenStatus").textContent =
    `Generated ${out.generated_count} (${out.route_source}) from ${out.from.label} to ${out.to.label}.${reverseText}`;
  await refreshCheckpointTable();
  await loadCorridors();
  setStatus("Auto-generated checkpoints saved.");
}

function renderRadiusTuneStatus(payload) {
  const statusEl = $("tuneRadiusStatus");
  const rowsEl = $("tuneRadiusRows");
  if (!statusEl || !rowsEl) return;
  const p = payload || {};
  const state = String(p.status || "idle");
  const pct = Number(p.progress_pct || 0);
  const best = p.best || null;
  const runningText = state === "running" ? ` | ${pct.toFixed(1)}%` : "";
  const bestText = best ? ` | best=${Number(best.radius_km || 0).toFixed(1)} km (score ${Number(best.score_avg || 0).toFixed(1)})` : "";
  const doneText = p.applied ? ` | applied=${Number(p.applied_radius_km || 0).toFixed(1)} km` : "";
  const errText = p.error ? ` | error=${p.error}` : "";
  statusEl.textContent = `State=${state}${runningText}${bestText}${doneText}${errText}`;

  rowsEl.innerHTML = "";
  const ranking = Array.isArray(p.ranking) ? p.ranking : [];
  ranking.slice(0, 10).forEach((r) => {
    const tr = document.createElement("tr");
    tr.innerHTML = `<td>${Number(r.radius_km || 0).toFixed(1)}</td>
      <td>${Number(r.score_avg || 0).toFixed(2)}</td>
      <td>${Number(r.eval_count || 0)}</td>
      <td>${Number(r.traffic_segments_avg || 0).toFixed(1)}</td>
      <td>${Number(r.camera_fresh_pct_avg || 0).toFixed(1)}</td>
      <td>${Number(r.camera_total_avg || 0).toFixed(1)}</td>`;
    rowsEl.appendChild(tr);
  });
}

function stopRadiusTunePolling() {
  if (radiusTunePollTimer) {
    clearInterval(radiusTunePollTimer);
    radiusTunePollTimer = null;
  }
}

async function refreshRadiusTuneStatus() {
  if (!selectedCorridorId) return;
  const out = await api(`/api/corridors/${selectedCorridorId}/autotune-radius/status`);
  renderRadiusTuneStatus(out);
  const state = String(out.status || "idle");
  if (state === "running" || state === "queued") {
    if (!radiusTunePollTimer) {
      radiusTunePollTimer = setInterval(async () => {
        try {
          const next = await api(`/api/corridors/${selectedCorridorId}/autotune-radius/status`);
          renderRadiusTuneStatus(next);
          const s = String(next.status || "idle");
          if (s !== "running" && s !== "queued") {
            stopRadiusTunePolling();
            if (s === "done" && next.applied) {
              $("corridorRadius").value = Number(next.applied_radius_km || 0).toFixed(1);
              await loadCorridors();
              await refreshAll();
              setStatus(`Auto-tune done. Applied radius ${Number(next.applied_radius_km || 0).toFixed(1)} km.`);
            }
          }
        } catch {
          // polling failures should not break UI loop permanently
        }
      }, 2500);
    }
  } else {
    stopRadiusTunePolling();
  }
}

async function startRadiusTune() {
  if (!selectedCorridorId) return;
  const body = {
    duration_seconds: Number($("tuneDurationSec").value || 90),
    min_radius_km: Number($("tuneMinRadiusKm").value || 20),
    max_radius_km: Number($("tuneMaxRadiusKm").value || 110),
    step_km: Number($("tuneStepKm").value || 5)
  };
  const out = await api(`/api/corridors/${selectedCorridorId}/autotune-radius/start`, {
    method: "POST",
    body: JSON.stringify(body)
  });
  renderRadiusTuneStatus(out.job || out);
  setStatus(out.already_running ? "Auto-tune already running." : "Auto-tune started.");
  await refreshRadiusTuneStatus();
}

function routeDelayColor(delayPct) {
  const d = Number(delayPct || 0);
  if (d < 10) return "#23c55e";
  if (d < 25) return "#facc15";
  if (d < 40) return "#fb923c";
  if (d < 60) return "#ef4444";
  return "#8b5cf6";
}

function ensureRouteMap() {
  if (routeMap || typeof window.L === "undefined") return;
  const el = $("routeTrafficMap");
  if (!el) return;
  routeMap = window.L.map("routeTrafficMap", { zoomControl: true, preferCanvas: true });
  window.L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
    maxZoom: 19,
    attribution: "&copy; OpenStreetMap contributors"
  }).addTo(routeMap);
}

function resetRouteLayers() {
  [routeForwardSegmentsLayer, routeReverseSegmentsLayer, routeForwardLineLayer, routeReverseLineLayer].forEach((layer) => {
    if (layer && routeMap) routeMap.removeLayer(layer);
  });
  routeForwardSegmentsLayer = null;
  routeReverseSegmentsLayer = null;
  routeForwardLineLayer = null;
  routeReverseLineLayer = null;
}

function routeStatsText(block, label) {
  if (!block || !block.route_available) return `${label}: route not available. Run auto-generate checkpoints first.`;
  const s = block.stats || {};
  return (
    `${label}: segments=${Number(s.segment_count || 0)}, avg delay=${Number(s.avg_delay_pct || 0).toFixed(1)}%, ` +
    `p90=${Number(s.p90_delay_pct || 0).toFixed(1)}%, heavy=${Number(s.heavy_delay_count || 0)}, score=${Number(
      s.score || 0
    ).toFixed(1)}`
  );
}

function renderDirectionCompare(payload) {
  const wrap = $("directionCompareWrap");
  const statusEl = $("directionCompareStatus");
  const rowsEl = $("directionCompareRows");
  if (!wrap || !statusEl || !rowsEl) return;
  wrap.style.display = directionCompareVisible ? "block" : "none";
  if (!directionCompareVisible) return;

  const forward = payload?.forward || null;
  const reverse = payload?.reverse || null;
  statusEl.textContent = "TO/FROM stats from route-scoped traffic segments.";
  rowsEl.innerHTML = "";
  const pushRow = (label, block) => {
    const tr = document.createElement("tr");
    const s = block?.stats || {};
    tr.innerHTML = `<td>${label}</td>
      <td>${Number(s.segment_count || 0)}</td>
      <td>${Number(s.avg_delay_pct || 0).toFixed(1)}</td>
      <td>${Number(s.p90_delay_pct || 0).toFixed(1)}</td>
      <td>${Number(s.heavy_delay_count || 0)}</td>
      <td>${Number(s.score || 0).toFixed(1)}</td>`;
    rowsEl.appendChild(tr);
  };
  if (forward) pushRow("TO (forward)", forward);
  if (reverse) pushRow("FROM (reverse)", reverse);
  if (!forward && !reverse) {
    statusEl.textContent = "No compare data yet.";
  }
}

function renderRouteMap(payload) {
  latestRouteMapPayload = payload || null;
  ensureRouteMap();
  const statusEl = $("routeMapStatus");
  const fStats = $("routeForwardStats");
  const rStats = $("routeReverseStats");

  if (!routeMap || !payload) {
    if (statusEl) statusEl.textContent = "Route map unavailable.";
    if (fStats) fStats.textContent = "-";
    if (rStats) rStats.textContent = "-";
    renderDirectionCompare(payload);
    return;
  }

  resetRouteLayers();
  const bounds = [];
  const forward = payload.forward || null;
  const reverse = payload.reverse || null;
  if (fStats) fStats.textContent = routeStatsText(forward, "TO (forward)");
  if (rStats) rStats.textContent = reverse ? routeStatsText(reverse, "FROM (reverse)") : "FROM (reverse): disabled or unavailable.";

  if (forward?.route_available && forward.route_line_geojson) {
    routeForwardLineLayer = window.L.geoJSON(forward.route_line_geojson, {
      style: { color: "#7dd3fc", weight: 6, opacity: 0.9 }
    }).addTo(routeMap);
    routeForwardSegmentsLayer = window.L.geoJSON(forward.segments_geojson, {
      style: (f) => ({
        color: routeDelayColor(f?.properties?.delay_pct),
        weight: 11,
        opacity: 0.82,
        lineCap: "round"
      })
    }).addTo(routeMap);
    if (routeForwardLineLayer.getBounds().isValid()) bounds.push(routeForwardLineLayer.getBounds());
  }

  if (reverse?.route_available && reverse.route_line_geojson) {
    routeReverseLineLayer = window.L.geoJSON(reverse.route_line_geojson, {
      style: { color: "#f9a8d4", weight: 4, opacity: 0.9, dashArray: "10,8" }
    }).addTo(routeMap);
    routeReverseSegmentsLayer = window.L.geoJSON(reverse.segments_geojson, {
      style: (f) => ({
        color: routeDelayColor(f?.properties?.delay_pct),
        weight: 8,
        opacity: 0.68,
        dashArray: "10,8",
        lineCap: "round"
      })
    }).addTo(routeMap);
    if (routeReverseLineLayer.getBounds().isValid()) bounds.push(routeReverseLineLayer.getBounds());
  }

  if (bounds.length) {
    const merged = bounds[0].extend(bounds.length > 1 ? bounds[1] : bounds[0]);
    routeMap.fitBounds(merged, { padding: [18, 18], maxZoom: 12 });
  } else {
    routeMap.setView([41.6, -90.5], 8);
  }
  setTimeout(() => routeMap.invalidateSize(), 120);

  const reverseLabel = reverse ? ` | reverse=${reverse.corridor_name || reverse.corridor_id}` : "";
  if (statusEl) {
    statusEl.textContent =
      `Forward=${forward?.corridor_name || "-"}${reverseLabel} | buffer=${Number(payload.route_buffer_km || 0).toFixed(
        1
      )} km | fetched ${new Date(payload.fetched_at || Date.now()).toLocaleTimeString()}`;
  }
  renderDirectionCompare(payload);
}

async function refreshRouteMapView() {
  if (!selectedCorridorId) return;
  const includeReverse = $("routeShowReverseChk")?.checked === true;
  const bufferKm = Number($("routeBufferKm")?.value || 8);
  const out = await api(
    `/api/route/traffic-map?corridor_id=${selectedCorridorId}&include_reverse=${includeReverse}&route_buffer_km=${bufferKm}`
  );
  renderRouteMap(out);
}

function toggleDirectionCompare() {
  directionCompareVisible = !directionCompareVisible;
  const btn = $("toggleDirectionCompareBtn");
  if (btn) btn.textContent = directionCompareVisible ? "Hide TO/FROM" : "Show TO/FROM";
  renderDirectionCompare(latestRouteMapPayload);
  if (directionCompareVisible && !latestRouteMapPayload) {
    refreshRouteMapView().catch(() => {});
  }
}

function escapeHtml(text) {
  return String(text || "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;");
}

function extractMermaidBlocks(text) {
  const src = String(text || "");
  const blocks = [];
  const cleaned = src.replace(/```mermaid\s*([\s\S]*?)```/gi, (_, code) => {
    const c = String(code || "").trim();
    if (c) blocks.push(c);
    return "\n\n[Diagram rendered below]\n\n";
  });
  return { cleaned, blocks };
}

function normalizeMermaidCode(code, strictPass = false) {
  let out = String(code || "")
    .replace(/\r\n/g, "\n")
    .replace(/<br\s*\/?>/gi, " ")
    .trim();
  if (!out) return "";
  if (!/^\s*(flowchart|graph|sequenceDiagram|classDiagram|stateDiagram|erDiagram|gantt|journey|pie|mindmap|timeline|gitGraph)\b/m.test(out)) {
    out = `flowchart LR\n${out}`;
  }
  out = out
    .replace(/;[ \t]*/g, "\n")
    .replace(/([\]\)\}"])([A-Za-z][A-Za-z0-9_]*)\s*-->/g, "$1\n$2 -->")
    .replace(/%/g, " pct");
  if (strictPass) {
    out = out
      .replace(/([A-Za-z][A-Za-z0-9_]*)\[(.*?)\]/g, (_, id, label) => `${id}["${String(label || "").replace(/"/g, "'")}"]`)
      .replace(/([A-Za-z][A-Za-z0-9_]*)\((.*?)\)/g, (_, id, label) => `${id}("${String(label || "").replace(/"/g, "'")}")`);
  }
  return out;
}

function renderTextAsHtml(text) {
  const raw = window.marked?.parse ? window.marked.parse(String(text || "")) : escapeHtml(text).replaceAll("\n", "<br>");
  return String(raw)
    .replace(/<script[\s\S]*?>[\s\S]*?<\/script>/gi, "")
    .replace(/<\/?(iframe|object|embed|link|meta)[^>]*>/gi, "")
    .replace(/\son[a-z]+\s*=\s*(['"]).*?\1/gi, "")
    .replace(/\son[a-z]+\s*=\s*[^\s>]+/gi, "")
    .replace(/href\s*=\s*(['"])\s*javascript:[\s\S]*?\1/gi, 'href="#"')
    .replace(/src\s*=\s*(['"])\s*javascript:[\s\S]*?\1/gi, 'src=""');
}

async function renderMermaidBlocks(blocks, holderId) {
  const holder = $(holderId);
  if (!holder) return;
  holder.innerHTML = "";
  if (!blocks.length) return;
  if (!window.mermaid) {
    holder.innerHTML = `<pre>${escapeHtml(blocks.join("\n\n---\n\n"))}</pre>`;
    return;
  }
  try {
    if (!window.__mermaidInit) {
      window.mermaid.initialize({ startOnLoad: false, theme: "dark", securityLevel: "loose" });
      window.__mermaidInit = true;
    }
    blocks.forEach((code) => {
      const wrap = document.createElement("div");
      wrap.className = "mermaid";
      wrap.textContent = normalizeMermaidCode(code, false);
      holder.appendChild(wrap);
    });
    const nodes = holder.querySelectorAll(".mermaid");
    await window.mermaid.run({ nodes });
  } catch (e) {
    try {
      holder.innerHTML = "";
      blocks.forEach((code) => {
        const wrap = document.createElement("div");
        wrap.className = "mermaid";
        wrap.textContent = normalizeMermaidCode(code, true);
        holder.appendChild(wrap);
      });
      const nodes = holder.querySelectorAll(".mermaid");
      await window.mermaid.run({ nodes });
    } catch (e2) {
      holder.innerHTML = `<div class="status">Diagram render failed. Showing raw diagram text instead.</div><pre>${escapeHtml(
        blocks.join("\n\n---\n\n")
      )}</pre>`;
    }
  }
}

async function renderAnswerPanel(out, { answerId, diagramId, contextId }) {
  const answerEl = $(answerId);
  const contextEl = contextId ? $(contextId) : null;
  if (!answerEl) return;
  const aiErr = out?.aiError ? `<div class="status">AI error: ${escapeHtml(out.aiError)}</div>` : "";
  const model = `<span class="pill">${escapeHtml(out?.modelUsed || "heuristic")}</span>`;
  const { cleaned, blocks } = extractMermaidBlocks(out?.answer || "");
  const body = renderTextAsHtml(cleaned);
  answerEl.innerHTML = `${model}${aiErr}<div>${body}</div>`;
  await renderMermaidBlocks(blocks, diagramId);
  if (contextEl) {
    const cov = Array.isArray(out?.contextCoverage) ? out.contextCoverage.join(", ") : "";
    contextEl.textContent = cov ? `Context coverage: ${cov}` : "";
  }
}

async function askQuestionFrom(inputId, answerId, diagramId, contextId = "") {
  const q = $(inputId)?.value?.trim();
  if (!q) return;
  const answer = $(answerId);
  const diagram = $(diagramId);
  const ctx = contextId ? $(contextId) : null;
  if (answer) answer.textContent = "Thinking...";
  if (diagram) diagram.innerHTML = "";
  if (ctx) ctx.textContent = "";
  try {
    const out = await api("/api/chat/query", {
      method: "POST",
      body: JSON.stringify({ corridor_id: selectedCorridorId, question: q })
    });
    await renderAnswerPanel(out, { answerId, diagramId, contextId });
  } catch (e) {
    if (answer) answer.innerHTML = `<div class="status">Chat query failed: ${escapeHtml(e.message || e)}</div>`;
  }
}

async function askQuestion() {
  await askQuestionFrom("chatInput", "chatAnswer", "chatDiagram");
}

async function askAnalyst() {
  await askQuestionFrom("aiInput", "aiAnswer", "aiDiagram", "aiContext");
}

function selectedMethodPreset() {
  const id = String($("methodPresetSelect")?.value || "");
  return methodPresets.find((p) => p.id === id) || null;
}

function renderMethodPresetSelect() {
  const sel = $("methodPresetSelect");
  if (!sel) return;
  sel.innerHTML = "";
  methodPresets.forEach((p) => {
    const o = document.createElement("option");
    o.value = p.id;
    o.textContent = `${p.name} (${Number(p.override_count || 0)} keys)`;
    sel.appendChild(o);
  });
  if (activeMethodPresetId && methodPresets.some((p) => p.id === activeMethodPresetId)) {
    sel.value = activeMethodPresetId;
  } else if (methodPresets.length) {
    sel.value = methodPresets[0].id;
  }
  const p = selectedMethodPreset();
  if ($("methodPresetDesc")) {
    $("methodPresetDesc").textContent = p
      ? `${p.description || ""}${p.id === activeMethodPresetId ? " [ACTIVE]" : ""}`
      : "";
  }
}

async function loadMethodPresets() {
  const out = await api("/api/settings/presets");
  methodPresets = Array.isArray(out?.presets) ? out.presets : [];
  activeMethodPresetId = String(out?.active?.id || "");
  renderMethodPresetSelect();
  if ($("methodPresetStatus")) {
    $("methodPresetStatus").textContent = activeMethodPresetId
      ? `Active preset: ${activeMethodPresetId}`
      : "No active preset yet.";
  }
}

async function applyMethodPreset() {
  const preset = selectedMethodPreset();
  if (!preset) {
    if ($("methodPresetStatus")) $("methodPresetStatus").textContent = "Select a preset first.";
    return;
  }
  const out = await api("/api/settings/presets/activate", {
    method: "PUT",
    body: JSON.stringify({ preset_id: preset.id })
  });
  renderSettingsForm(out.settings || {});
  activeMethodPresetId = String(out?.active?.id || preset.id);
  renderMethodPresetSelect();
  if ($("methodPresetStatus")) {
    $("methodPresetStatus").textContent = `Applied preset: ${preset.name}`;
  }
  $("settingsStatus").textContent = "Preset applied and saved.";
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
  if ($("insP90Score")) $("insP90Score").textContent = Number(ins.summary.p90_score || 0).toFixed(1);
  if ($("insStdScore")) $("insStdScore").textContent = Number(ins.summary.std_score || 0).toFixed(1);
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
    const c1 = document.createElement("td");
    const c2 = document.createElement("td");
    const c3 = document.createElement("td");
    const c4 = document.createElement("td");
    c1.textContent = String(h.camera_location || "-");
    c2.textContent = Number(h.avg_image_score || 0).toFixed(2);
    c3.textContent = Number(h.max_image_score || 0).toFixed(2);
    c4.textContent = String(Number(h.observations || 0));
    tr.appendChild(c1);
    tr.appendChild(c2);
    tr.appendChild(c3);
    tr.appendChild(c4);
    hot.appendChild(tr);
  });
}

function strategyById(id) {
  const rows = latestStrategiesPayload?.strategies || [];
  return rows.find((s) => s.id === id) || null;
}

function formatStrategySummary(s) {
  if (!s) return "No selection.";
  const i = s.inputs || {};
  const p = s.projection || {};
  return `${s.name}: traffic=${Number(i.traffic_segments || 0).toFixed(0)}, delay=${Number(
    i.delay_pct || 0
  ).toFixed(1)}%, image=${Number(i.image_score || 0).toFixed(1)}, incidents=${Number(i.incidents_count || 0).toFixed(
    0
  )}, closures=${Number(i.closures_count || 0).toFixed(0)}, weather=${Number(i.weather_risk_score || 0).toFixed(
    1
  )}, fresh=${Number(i.camera_fresh_pct || 0).toFixed(1)}% -> fused=${Number(p.fused_score || 0).toFixed(
    1
  )}, p50=${Number(p.p50 || 0).toFixed(1)}, p90=${Number(p.p90 || 0).toFixed(1)}, conf=${Number(
    p.confidence || 0
  ).toFixed(1)}%.`;
}

function updateStrategySelectionUi() {
  document.querySelectorAll("#strategyRows tr").forEach((tr) => {
    tr.classList.toggle("strategy-selected", tr.dataset.strategyId === selectedStrategyId);
    tr.classList.toggle("strategy-active", tr.dataset.strategyId === activeStrategyId);
  });
  const selected = strategyById(selectedStrategyId);
  if ($("strategySelectedSummary")) {
    const prefix = selected?.id && selected.id === activeStrategyId ? "[ACTIVE] " : "";
    $("strategySelectedSummary").textContent = `${prefix}${formatStrategySummary(selected)}`;
  }
}

function selectStrategy(id) {
  if (!id) return;
  selectedStrategyId = String(id);
  updateStrategySelectionUi();
}

function renderStrategies(payload) {
  latestStrategiesPayload = payload || null;
  activeStrategyId = String(payload?.active_strategy?.strategy_id || "");
  const tb = $("strategyRows");
  if (!tb) return;
  tb.innerHTML = "";
  const strategies = Array.isArray(payload?.strategies) ? payload.strategies : [];
  if (!strategies.length) {
    if ($("strategyStatus")) $("strategyStatus").textContent = "No strategy candidates yet. Run Poll Now and refresh.";
    if ($("strategyRecommendation")) $("strategyRecommendation").textContent = "";
    if ($("strategySelectedSummary")) $("strategySelectedSummary").textContent = "No selection.";
    selectedStrategyId = "";
    return;
  }

  strategies.forEach((s) => {
    const i = s.inputs || {};
    const p = s.projection || {};
    const activeTag = s.id === activeStrategyId ? ` <span class="pill">ACTIVE</span>` : "";
    const tr = document.createElement("tr");
    tr.dataset.strategyId = s.id;
    tr.style.cursor = "pointer";
    tr.innerHTML = `<td>${escapeHtml(s.name || s.id || "-")}${activeTag}</td>
      <td>${escapeHtml(s.goal || "-")}</td>
      <td>${Number(i.traffic_segments || 0).toFixed(0)}</td>
      <td>${Number(i.delay_pct || 0).toFixed(1)}</td>
      <td>${Number(i.image_score || 0).toFixed(1)}</td>
      <td>${Number(i.incidents_count || 0).toFixed(0)}</td>
      <td>${Number(i.closures_count || 0).toFixed(0)}</td>
      <td>${Number(i.weather_risk_score || 0).toFixed(1)}</td>
      <td>${Number(i.camera_fresh_pct || 0).toFixed(1)}</td>
      <td>${Number(p.fused_score || 0).toFixed(1)}</td>
      <td>${Number(p.p50 || 0).toFixed(1)} / ${Number(p.p90 || 0).toFixed(1)}</td>
      <td>${Number(p.confidence || 0).toFixed(1)}</td>
      <td>${escapeHtml(p.risk_band || "-")}</td>
      <td><button data-action="apply" data-strategy-id="${escapeHtml(s.id)}">Apply</button></td>`;
    tb.appendChild(tr);
  });

  if ($("strategyStatus")) {
    $("strategyStatus").textContent = `Generated ${strategies.length} strategies from ${Number(
      payload?.sample_points || 0
    )} points over ${Number(payload?.window_hours || 0)}h.${
      activeStrategyId ? ` Active=${activeStrategyId}.` : ""
    }`;
  }
  if ($("strategyRecommendation")) {
    const low = payload?.recommendation?.lowest_risk;
    const high = payload?.recommendation?.highest_risk;
    $("strategyRecommendation").textContent = `Recommended: lowest-risk = ${low?.name || "-"} | highest-risk stress case = ${
      high?.name || "-"
    }.`;
  }

  const preferred =
    (selectedStrategyId && strategies.find((s) => s.id === selectedStrategyId)?.id) ||
    (activeStrategyId && strategies.find((s) => s.id === activeStrategyId)?.id) ||
    payload?.recommendation?.lowest_risk?.id ||
    strategies.find((s) => s.id === "typical_commute")?.id ||
    strategies[0].id;
  selectStrategy(preferred);
}

async function refreshStrategies() {
  if (!selectedCorridorId) return;
  const hours = Number($("hoursSelect").value || 24);
  const out = await api(`/api/analysis/strategy-presets?corridor_id=${selectedCorridorId}&hours=${hours}`);
  renderStrategies(out);
}

async function persistActiveStrategy(strategy) {
  if (!selectedCorridorId || !strategy) return null;
  const out = await api("/api/analysis/active-strategy", {
    method: "PUT",
    body: JSON.stringify({
      corridor_id: selectedCorridorId,
      source: "ui_strategy_apply",
      strategy
    })
  });
  if (out?.active_strategy) {
    activeStrategyId = String(out.active_strategy.strategy_id || strategy.id || "");
    if (latestStrategiesPayload && typeof latestStrategiesPayload === "object") {
      latestStrategiesPayload.active_strategy = out.active_strategy;
    }
    updateStrategySelectionUi();
  }
  return out;
}

function applyStrategyInputsToScenario(inputs = {}) {
  $("simTrafficSegments").value = Number(inputs.traffic_segments || 0).toFixed(0);
  $("simDelayPct").value = Number(inputs.delay_pct || 0).toFixed(1);
  $("simImageScore").value = Number(inputs.image_score || 0).toFixed(1);
  $("simIncidents").value = Number(inputs.incidents_count || 0).toFixed(0);
  $("simClosures").value = Number(inputs.closures_count || 0).toFixed(0);
  $("simWeatherRisk").value = Number(inputs.weather_risk_score || 0).toFixed(1);
}

async function applySelectedStrategy(runNow = false) {
  const s = strategyById(selectedStrategyId);
  if (!s) {
    if ($("strategyApplyStatus")) $("strategyApplyStatus").textContent = "Select a strategy first.";
    return;
  }
  applyStrategyInputsToScenario(s.inputs);
  try {
    await persistActiveStrategy(s);
  } catch (e) {
    if ($("strategyApplyStatus")) {
      $("strategyApplyStatus").textContent = `Applied inputs, but active strategy save failed: ${e.message || e}`;
    }
  }
  activateTab("analysis");
  if (runNow) await runScenario();
  if ($("strategyApplyStatus")) {
    $("strategyApplyStatus").textContent = runNow
      ? `Applied and simulated: ${s.name}`
      : `Applied to Scenario Lab: ${s.name}`;
  }
  setStatus(runNow ? `Scenario run from ${s.name}` : `Scenario inputs loaded from ${s.name}`);
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
  $("scenarioOut").textContent =
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
  try {
    await api("/api/poll-now", { method: "POST" });
    await refreshAll();
  } catch (e) {
    setStatus(`Poll failed: ${e.message || e}`);
  }
}

async function init() {
  document.querySelectorAll(".tab-btn").forEach((b) => b.addEventListener("click", () => activateTab(b.dataset.tab)));
  radarSourceMode = String($("radarSourceSelect")?.value || "auto");
  if ($("tuneDurationSec")) $("tuneDurationSec").value = "90";
  if ($("tuneMinRadiusKm")) $("tuneMinRadiusKm").value = "20";
  if ($("tuneMaxRadiusKm")) $("tuneMaxRadiusKm").value = "110";
  if ($("tuneStepKm")) $("tuneStepKm").value = "5";
  if ($("routeBufferKm")) $("routeBufferKm").value = "8";
  $("refreshBtn").addEventListener("click", refreshAll);
  $("pollNowBtn").addEventListener("click", pollNow);
  $("saveCorridorBtn").addEventListener("click", saveCorridor);
  $("corridorFeedProfile").addEventListener("change", (e) => {
    const p = String(e.target.value || "il_arcgis");
    if (p !== "ca_cwwp2" && $("corridorCaDistricts")) {
      $("corridorCaDistricts").value = "";
    }
  });
  $("archiveCorridorBtn").addEventListener("click", () => archiveSelectedCorridor(false));
  $("restoreCorridorBtn").addEventListener("click", () => archiveSelectedCorridor(true));
  $("saveCheckpointBtn").addEventListener("click", saveCheckpoint);
  $("clearCheckpointBtn").addEventListener("click", clearCheckpoints);
  $("autoGenerateBtn").addEventListener("click", autogenCheckpoints);
  $("toggleDirectionCompareBtn").addEventListener("click", toggleDirectionCompare);
  $("refreshDirectionCompareBtn").addEventListener("click", async () => {
    await refreshRouteMapView();
  });
  $("refreshRouteMapBtn").addEventListener("click", refreshRouteMapView);
  $("routeShowReverseChk").addEventListener("change", refreshRouteMapView);
  $("routeBufferKm").addEventListener("change", refreshRouteMapView);
  $("startTuneRadiusBtn").addEventListener("click", startRadiusTune);
  $("refreshTuneRadiusBtn").addEventListener("click", refreshRadiusTuneStatus);
  $("refreshPreviewBtn").addEventListener("click", refreshCameraPreview);
  $("refreshWeatherBtn").addEventListener("click", refreshWeatherLive);
  $("refreshWeightsBtn").addEventListener("click", refreshWeights);
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
  $("radarZoomInBtn").addEventListener("click", radarZoomIn);
  $("radarZoomOutBtn").addEventListener("click", radarZoomOut);
  $("radarZoomResetBtn").addEventListener("click", radarZoomReset);
  $("radarSourceSelect").addEventListener("change", (e) => {
    radarSourceMode = String(e.target.value || "auto");
    stopRadarAnimation();
    if (!latestWeather) return;
    radarResolvedMode = resolveRadarMode(latestWeather);
    setRadarFramesFromPayload(latestWeather);
    if (radarResolvedMode === "station" && (radarStationFrames.length || latestWeather.radar_station_loop_gif_url)) {
      renderRadarStation(latestWeather);
    } else {
      renderRadarMap(latestWeather);
    }
    const hintEl = $("radarLoopHint");
    if (hintEl) hintEl.textContent = radarLoopHint(latestWeather);
    updateRadarFrameLabel();
    updateRadarZoomLabel();
  });
  $("radarProductSelect").addEventListener("change", (e) => {
    radarProduct = String(e.target.value || "bref");
    applyRadarProductToLayer();
    applyRadarFrameToLayers();
    if (latestWeather) {
      if (radarResolvedMode === "station") renderRadarStation(latestWeather);
      else renderRadarMap(latestWeather);
    }
  });
  $("radarViewSelect").addEventListener("change", (e) => {
    radarViewMode = String(e.target.value || "corridor");
    radarFittedKey = "";
    if (latestWeather) {
      if (radarResolvedMode === "station") renderRadarStation(latestWeather);
      else renderRadarMap(latestWeather);
    }
  });
  $("runScenarioBtn").addEventListener("click", runScenario);
  $("refreshStrategiesBtn").addEventListener("click", async () => {
    try {
      await refreshStrategies();
      setStatus("Strategies refreshed.");
    } catch (e) {
      if ($("strategyStatus")) $("strategyStatus").textContent = `Strategy refresh failed: ${e.message || e}`;
    }
  });
  $("applyBestStrategyBtn").addEventListener("click", async () => {
    const low = latestStrategiesPayload?.recommendation?.lowest_risk?.id;
    if (low) selectStrategy(low);
    await applySelectedStrategy(false);
  });
  $("applyTypicalStrategyBtn").addEventListener("click", async () => {
    const typical = strategyById("typical_commute")?.id || (latestStrategiesPayload?.strategies || [])[0]?.id;
    if (typical) selectStrategy(typical);
    await applySelectedStrategy(false);
  });
  $("applySelectedStrategyBtn").addEventListener("click", async () => {
    await applySelectedStrategy(false);
  });
  $("applyRunSelectedStrategyBtn").addEventListener("click", async () => {
    await applySelectedStrategy(true);
  });
  $("strategyRows").addEventListener("click", async (e) => {
    const btn = e.target.closest("button[data-action]");
    const tr = e.target.closest("tr[data-strategy-id]");
    if (!tr) return;
    selectStrategy(tr.dataset.strategyId || "");
    if (btn?.dataset?.action === "apply") {
      await applySelectedStrategy(false);
    }
  });
  $("askBtn").addEventListener("click", askQuestion);
  $("aiAskBtn").addEventListener("click", askAnalyst);
  $("aiUseQuick1").addEventListener("click", () => {
    $("aiInput").value = "Why did the score move in the last 2 hours? show the driver breakdown.";
    askAnalyst();
  });
  $("aiUseQuick2").addEventListener("click", () => {
    $("aiInput").value = "What are the best and worst local departure windows for this corridor?";
    askAnalyst();
  });
  $("aiUseQuick3").addEventListener("click", () => {
    $("aiInput").value = "Draw a mermaid flowchart of current risk drivers and recommended action.";
    askAnalyst();
  });
  $("reloadMethodPresetsBtn").addEventListener("click", loadMethodPresets);
  $("applyMethodPresetBtn").addEventListener("click", applyMethodPreset);
  $("methodPresetSelect").addEventListener("change", () => {
    const p = selectedMethodPreset();
    if ($("methodPresetDesc")) {
      $("methodPresetDesc").textContent = p
        ? `${p.description || ""}${p.id === activeMethodPresetId ? " [ACTIVE]" : ""}`
        : "";
    }
  });
  $("reloadSettingsBtn").addEventListener("click", loadSettings);
  $("saveSettingsBtn").addEventListener("click", saveSettings);
  $("includeArchivedChk").addEventListener("change", async () => {
    await loadCorridors();
    await refreshAll();
  });
  $("corridorSelect").addEventListener("change", (e) => {
    selectedCorridorId = Number(e.target.value);
    selectedCamera = null;
    radarFittedKey = "";
    stopRadiusTunePolling();
    latestRouteMapPayload = null;
    renderDirectionCompare(null);
    syncCorridorFormFromSelected();
    refreshAll();
  });
  $("hoursSelect").addEventListener("change", refreshAll);

  await loadFeedProfiles();
  await loadCorridors();
  await loadMethodPresets();
  await loadSettings();
  await refreshAll();
  setInterval(refreshAll, 30000);
}

init().catch((e) => setStatus(`Error: ${e.message}`));
