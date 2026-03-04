let selectedCorridorId = null;
let scoreChart;
let componentChart;
let latestRun = null;

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
}

async function loadCorridors() {
  const corridors = await api("/api/corridors");
  const sel = $("corridorSelect");
  sel.innerHTML = "";
  corridors.forEach((c) => {
    const o = document.createElement("option");
    o.value = c.id;
    o.textContent = `${c.name} [${c.xmin.toFixed(2)},${c.ymin.toFixed(2)} -> ${c.xmax.toFixed(2)},${c.ymax.toFixed(2)}]`;
    sel.appendChild(o);
  });
  if (!selectedCorridorId && corridors.length) {
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
    Number(run.closure_component || 0)
  ];
  if (componentChart) componentChart.destroy();
  componentChart = new Chart($("componentBarChart"), {
    type: "bar",
    data: {
      labels: ["Traffic", "Image", "Incidents", "Closures"],
      datasets: [{ data: vals, backgroundColor: ["#66b0ff", "#29d99e", "#ffbe57", "#ff6b72"] }]
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
  const tb = $("cameraRows");
  tb.innerHTML = "";
  rows.forEach((r) => {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${r.camera_location || "-"}</td>
      <td>${r.camera_direction || "-"}</td>
      <td>${Number(r.snapshot_bytes || 0)}</td>
      <td>${Number(r.age_minutes || 0).toFixed(1)}</td>
      <td>${Number(r.image_score || 0).toFixed(2)}</td>
      <td>${r.snapshot_url ? `<a href="${r.snapshot_url}" target="_blank">open</a>` : "-"}</td>`;
    tb.appendChild(tr);
  });
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
  const [latest, ts, cams, model] = await Promise.all([
    api(`/api/runs/latest?corridor_id=${selectedCorridorId}`),
    api(`/api/runs/timeseries?corridor_id=${selectedCorridorId}&hours=${hours}`),
    api(`/api/runs/recent-cameras?corridor_id=${selectedCorridorId}`),
    api(`/api/models/latest?corridor_id=${selectedCorridorId}`)
  ]);
  renderKpis(latest);
  renderScoreChart(ts);
  renderComponents(latest);
  renderCameraRows(cams);
  renderTsRows(ts);
  renderModelMeta(model);
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
  $("saveCheckpointBtn").addEventListener("click", saveCheckpoint);
  $("clearCheckpointBtn").addEventListener("click", clearCheckpoints);
  $("autoGenerateBtn").addEventListener("click", autogenCheckpoints);
  $("askBtn").addEventListener("click", askQuestion);
  $("corridorSelect").addEventListener("change", (e) => {
    selectedCorridorId = Number(e.target.value);
    refreshAll();
  });
  $("hoursSelect").addEventListener("change", refreshAll);

  await loadCorridors();
  await refreshAll();
  setInterval(refreshAll, 30000);
}

init().catch((e) => setStatus(`Error: ${e.message}`));
