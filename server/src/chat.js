import { pool } from "./db.js";
import { getActiveStrategy, getRuntimeSettings } from "./settings.js";
import { fetchCorridorWeather } from "./weather.js";

function containsAny(text, words) {
  const t = text.toLowerCase();
  return words.some((w) => t.includes(w));
}

function toNum(v, fallback = 0) {
  const n = Number(v);
  return Number.isFinite(n) ? n : fallback;
}

function round1(v) {
  return Math.round(toNum(v) * 10) / 10;
}

function parseMaybeJson(v) {
  if (!v) return null;
  if (typeof v === "object") return v;
  if (typeof v !== "string") return null;
  try {
    return JSON.parse(v);
  } catch {
    return null;
  }
}

function withTimeout(promise, ms) {
  return Promise.race([
    promise,
    new Promise((_, reject) => setTimeout(() => reject(new Error(`timeout_${ms}ms`)), ms))
  ]);
}

function summarizeRun(runRow) {
  if (!runRow) return null;
  return {
    run_ts: runRow.run_ts,
    fused_score: toNum(runRow.fused_score),
    traffic_segments: toNum(runRow.traffic_segments),
    delay_pct: toNum(runRow.delay_pct),
    incidents_count: toNum(runRow.incidents_count),
    closures_count: toNum(runRow.closures_count),
    camera_total: toNum(runRow.camera_total),
    camera_fresh_pct: toNum(runRow.camera_fresh_pct),
    image_score: toNum(runRow.image_score),
    image_proxy_score: toNum(runRow.image_proxy_score),
    image_queue_stop_score: toNum(runRow.image_queue_stop_score),
    image_hybrid_score: toNum(runRow.image_hybrid_score),
    image_signal_confidence: toNum(runRow.image_signal_confidence),
    image_signal_mode: runRow.image_signal_mode || "",
    cv_analyzed_count: toNum(runRow.cv_analyzed_count),
    cv_coverage_pct: toNum(runRow.cv_coverage_pct),
    cv_avg_confidence: toNum(runRow.cv_avg_confidence),
    cv_provider: runRow.cv_provider || "",
    cv_error_count: toNum(runRow.cv_error_count),
    traffic_component: toNum(runRow.traffic_component),
    image_component: toNum(runRow.image_component),
    incident_component: toNum(runRow.incident_component),
    closure_component: toNum(runRow.closure_component),
    weather_risk_score: toNum(runRow.weather_risk_score),
    weather_component: toNum(runRow.weather_component),
    predicted_next_score_p50: toNum(runRow.predicted_next_score_p50),
    predicted_next_score_p90: toNum(runRow.predicted_next_score_p90),
    prediction_confidence: toNum(runRow.prediction_confidence),
    model_version: runRow.model_version || "",
    drift_score: toNum(runRow.drift_score),
    alert_state: runRow.alert_state || "",
    alert_reason: runRow.alert_reason || ""
  };
}

function computeDrivers(latest) {
  if (!latest) return [];
  const total = Math.max(0.001, toNum(latest.fused_score, 0));
  const parts = [
    { key: "traffic", value: toNum(latest.traffic_component, 0) },
    { key: "image", value: toNum(latest.image_component, 0) },
    { key: "incidents", value: toNum(latest.incident_component, 0) },
    { key: "closures", value: toNum(latest.closure_component, 0) },
    { key: "weather", value: toNum(latest.weather_component, 0) }
  ].sort((a, b) => b.value - a.value);
  return parts.map((p) => ({
    ...p,
    share_pct: round1((p.value / total) * 100)
  }));
}

function deriveBestWorstHours(hourlyRows = []) {
  const usable = (hourlyRows || []).filter((h) => toNum(h.points, 0) >= 2);
  const best = [...usable]
    .sort((a, b) => toNum(a.avg_score, 0) - toNum(b.avg_score, 0))
    .slice(0, 3)
    .map((h) => toNum(h.hour_local, 0));
  const worst = [...usable]
    .sort((a, b) => toNum(b.avg_score, 0) - toNum(a.avg_score, 0))
    .slice(0, 3)
    .map((h) => toNum(h.hour_local, 0));
  return { best, worst };
}

function methodologySummary(settings = {}) {
  const modeCode = Number(settings.image_signal_mode ?? 2);
  const imageMode = modeCode <= 0 ? "proxy_only" : modeCode === 1 ? "queue_stop" : "hybrid_auto";
  return {
    score_formula:
      "fused = traffic_component + image_component + incident_component + closure_component + weather_component (0..100 clamp)",
    image_signal_modes: {
      proxy_only: "camera anomaly proxy from snapshot bytes vs EWMA baseline",
      queue_stop: "queue/stopped proxy from local traffic speed/delay near each camera",
      hybrid_auto: "blends proxy and queue/stop by camera+traffic confidence"
    },
    configured_image_mode: imageMode,
    dynamic_weighting: Number(settings.dynamic_weights_enabled ?? 0) >= 0.5 ? "enabled" : "disabled",
    auto_learn_weighting: Number(settings.auto_learn_weights_enabled ?? 0) >= 0.5 ? "enabled" : "disabled"
  };
}

async function corridorMeta(corridorId) {
  const { rows } = await pool.query(
    `SELECT id, name,
            bbox_xmin AS xmin, bbox_ymin AS ymin, bbox_xmax AS xmax, bbox_ymax AS ymax,
            active, created_at, updated_at
       FROM corridors
      WHERE id = $1`,
    [corridorId]
  );
  return rows[0] || null;
}

async function latestRuns(corridorId, limit = 2) {
  const { rows } = await pool.query(
    `SELECT run_ts, fused_score, traffic_segments, delay_pct, incidents_count, closures_count, camera_total, camera_fresh_pct,
            image_score, image_proxy_score, image_queue_stop_score, image_hybrid_score, image_signal_mode, image_signal_confidence,
            cv_analyzed_count, cv_coverage_pct, cv_avg_confidence, cv_provider, cv_error_count,
            traffic_component, image_component, incident_component, closure_component,
            weather_risk_score, weather_component,
            predicted_next_score_p50, predicted_next_score_p90, prediction_confidence,
            model_version, drift_score, alert_state, alert_reason,
            raw_json
       FROM monitor_runs
      WHERE corridor_id = $1
      ORDER BY run_ts DESC
      LIMIT $2`,
    [corridorId, limit]
  );
  return rows;
}

async function recentRunSeries(corridorId, limit = 72) {
  const { rows } = await pool.query(
    `SELECT run_ts, fused_score, predicted_next_score_p50, predicted_next_score_p90,
            prediction_confidence, drift_score, alert_state, weather_risk_score,
            incidents_count, closures_count
       FROM monitor_runs
      WHERE corridor_id = $1
      ORDER BY run_ts DESC
      LIMIT $2`,
    [corridorId, limit]
  );
  return rows.reverse();
}

async function trendStats(corridorId, hours = 24) {
  const { rows } = await pool.query(
    `SELECT AVG(fused_score) AS avg_score,
            MIN(fused_score) AS min_score,
            MAX(fused_score) AS max_score,
            COUNT(*) AS points,
            AVG(predicted_next_score_p50) AS avg_p50,
            AVG(prediction_confidence) AS avg_confidence,
            STDDEV_SAMP(fused_score) AS std_score,
            AVG(weather_risk_score) AS avg_weather_risk,
            MAX(weather_risk_score) AS max_weather_risk,
            AVG((fused_score <= 35)::int) AS reliability_tight,
            AVG((fused_score <= 50)::int) AS reliability_relaxed
       FROM monitor_runs
      WHERE corridor_id = $1
        AND run_ts >= NOW() - ($2::text || ' hours')::interval`,
    [corridorId, hours]
  );
  return rows[0] || null;
}

async function hourlyProfile(corridorId, hours = 168) {
  const { rows } = await pool.query(
    `SELECT EXTRACT(HOUR FROM run_ts AT TIME ZONE 'America/Chicago')::int AS hour_local,
            AVG(fused_score) AS avg_score,
            AVG(weather_risk_score) AS avg_weather_risk,
            COUNT(*) AS points
       FROM monitor_runs
      WHERE corridor_id = $1
        AND run_ts >= NOW() - ($2::text || ' hours')::interval
      GROUP BY 1
      ORDER BY 1`,
    [corridorId, hours]
  );
  return rows;
}

async function latestModelSnapshot(corridorId) {
  const { rows } = await pool.query(
    `SELECT created_at, model_version, train_rows, residual_sigma, r2, drift_score
       FROM model_snapshots
      WHERE corridor_id = $1
      ORDER BY created_at DESC
      LIMIT 1`,
    [corridorId]
  );
  return rows[0] || null;
}

async function activeCheckpoints(corridorId, limit = 100) {
  const { rows } = await pool.query(
    `SELECT id, name, lat, lon, radius_km, source
       FROM checkpoints
      WHERE corridor_id = $1
        AND active = true
      ORDER BY id
      LIMIT $2`,
    [corridorId, limit]
  );
  return rows;
}

async function topCameras(corridorId, limit = 10) {
  const { rows } = await pool.query(
    `SELECT o.camera_location,
            AVG(o.image_score) AS avg_image_score,
            MAX(o.image_score) AS max_image_score,
            AVG(o.proxy_score) AS avg_proxy_score,
            AVG(o.queue_stop_score) AS avg_queue_stop_score,
            AVG(o.hybrid_score) AS avg_hybrid_score,
            AVG(o.cv_queue_score) AS avg_cv_queue_score,
            AVG(o.cv_confidence) AS avg_cv_confidence,
            AVG(o.age_minutes) AS avg_age_minutes,
            COUNT(*) AS observations,
            MAX(r.run_ts) AS latest_seen
       FROM camera_observations o
       JOIN monitor_runs r ON r.id = o.run_id
      WHERE r.corridor_id = $1
        AND r.run_ts >= NOW() - INTERVAL '24 hours'
      GROUP BY o.camera_location
      ORDER BY AVG(o.image_score) DESC
      LIMIT $2`,
    [corridorId, limit]
  );
  return rows;
}

async function recentCameras(corridorId, limit = 12) {
  const { rows } = await pool.query(
    `SELECT *
       FROM (
         SELECT DISTINCT ON (o.camera_object_id)
                o.camera_object_id,
                o.camera_location,
                o.camera_direction,
                o.snapshot_url,
                o.age_minutes,
                o.image_score,
                o.proxy_score,
                o.queue_stop_score,
                o.hybrid_score,
                o.hybrid_confidence,
                o.local_delay_pct,
                o.local_slow_pct,
                o.local_stop_pct,
                o.local_segment_count,
                r.run_ts
           FROM camera_observations o
           JOIN monitor_runs r ON r.id = o.run_id
          WHERE r.corridor_id = $1
          ORDER BY o.camera_object_id, r.run_ts DESC
       ) x
      ORDER BY x.image_score DESC
      LIMIT $2`,
    [corridorId, limit]
  );
  return rows;
}

async function buildContext(corridorId) {
  const [corridor, settings, runs, series, trend24, trend72, hourly, model, checkpoints, hotspots, cameras, activeStrategy] =
    await Promise.all([
      corridorMeta(corridorId),
      getRuntimeSettings(),
      latestRuns(corridorId, 2),
      recentRunSeries(corridorId, 96),
      trendStats(corridorId, 24),
      trendStats(corridorId, 72),
      hourlyProfile(corridorId, 168),
      latestModelSnapshot(corridorId),
      activeCheckpoints(corridorId, 100),
      topCameras(corridorId, 10),
      recentCameras(corridorId, 12),
      getActiveStrategy(corridorId)
    ]);

  const latest = summarizeRun(runs[0] || null);
  const previous = summarizeRun(runs[1] || null);
  const latestRaw = parseMaybeJson(runs[0]?.raw_json) || {};
  const weatherFromRun = latestRaw.weather || null;
  const feedCounts = latestRaw.feedCounts || null;
  const weightingFromRun = latestRaw.weighting || null;
  const imageSignalsFromRun = latestRaw.imageSignals || null;
  const cameraSelectionFromRun = latestRaw.cameraSelection || null;
  const drivers = computeDrivers(latest);
  const hours = deriveBestWorstHours(hourly);
  const nowMs = Date.now();
  const latestAgeMin = latest?.run_ts ? (nowMs - new Date(latest.run_ts).getTime()) / 60000 : null;

  let weatherLive = null;
  if (corridor) {
    try {
      weatherLive = await withTimeout(fetchCorridorWeather(corridor, settings, { useCache: true }), 4500);
    } catch {
      weatherLive = null;
    }
  }

  const deltaVsPrev = latest && previous
    ? {
        fused_delta: round1(toNum(latest.fused_score) - toNum(previous.fused_score)),
        weather_risk_delta: round1(toNum(latest.weather_risk_score) - toNum(previous.weather_risk_score)),
        incidents_delta: toNum(latest.incidents_count) - toNum(previous.incidents_count),
        closures_delta: toNum(latest.closures_count) - toNum(previous.closures_count)
      }
    : null;

  return {
    generated_at: new Date().toISOString(),
    corridor,
    latest_run: latest,
    previous_run: previous,
    delta_vs_previous: deltaVsPrev,
    latest_run_age_minutes: latestAgeMin == null ? null : round1(latestAgeMin),
    primary_drivers: drivers,
    trend_24h: trend24,
    trend_72h: trend72,
    hourly_profile_local: hourly,
    best_hours_local: hours.best,
    worst_hours_local: hours.worst,
    recent_run_series: series,
    model_snapshot: model,
    checkpoints,
    checkpoint_count: checkpoints.length,
    camera_hotspots_24h: hotspots,
    recent_cameras_latest: cameras,
    weather_from_latest_run: weatherFromRun,
    weighting_from_latest_run: weightingFromRun,
    image_signals_from_latest_run: imageSignalsFromRun,
    camera_selection_from_latest_run: cameraSelectionFromRun,
    latest_run_raw_json: latestRaw,
    weather_live: weatherLive,
    active_strategy: activeStrategy,
    source_feed_counts: feedCounts,
    runtime_settings: settings,
    methodology: methodologySummary(settings),
    context_coverage: [
      "corridor_meta",
      "latest_run+delta",
      "trend_24h_72h+hourly_profile",
      "recent_run_series",
      "model_snapshot",
      "checkpoints",
      "camera_hotspots_24h",
      "recent_cameras_latest",
      "weather_from_latest_run",
      "weighting_from_latest_run",
      "image_signals_from_latest_run",
      "camera_selection_from_latest_run",
      "latest_run_raw_json",
      "weather_live",
      "active_strategy",
      "runtime_settings",
      "methodology"
    ]
  };
}

async function maybeAskOpenAI(question, context) {
  const key = process.env.OPENAI_API_KEY;
  if (!key) return { text: null, error: "OPENAI_API_KEY missing" };
  const model = process.env.OPENAI_MODEL || "gpt-5.2";
  const prompt = `Question: ${question}\nContext JSON:\n${JSON.stringify(context)}`;
  const instructions =
    "You are a corridor operations analyst copilot. Use only provided context values, cite key numbers, and give practical next actions. If data is missing, say exactly what is missing. When asked for visuals, include a concise Mermaid diagram in a ```mermaid fenced block plus a short interpretation.";

  function extractTextFromResponses(data) {
    if (!data) return "";
    if (typeof data.output_text === "string" && data.output_text.trim()) {
      return data.output_text.trim();
    }
    const chunks = [];
    const outputs = Array.isArray(data.output) ? data.output : [];
    for (const out of outputs) {
      const content = Array.isArray(out?.content) ? out.content : [];
      for (const c of content) {
        if (typeof c?.text === "string" && c.text.trim()) chunks.push(c.text.trim());
      }
    }
    return chunks.join("\n").trim();
  }

  async function callResponses() {
    const res = await fetch("https://api.openai.com/v1/responses", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${key}`
      },
      body: JSON.stringify({
        model,
        instructions,
        input: prompt
      })
    });
    if (!res.ok) {
      const errText = await res.text();
      throw new Error(`responses ${res.status}: ${errText.slice(0, 600)}`);
    }
    const data = await res.json();
    return extractTextFromResponses(data);
  }

  async function callChatCompletionsFallback() {
    const res = await fetch("https://api.openai.com/v1/chat/completions", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${key}`
      },
      body: JSON.stringify({
        model,
        messages: [
          { role: "system", content: instructions },
          { role: "user", content: prompt }
        ]
      })
    });
    if (!res.ok) {
      const errText = await res.text();
      throw new Error(`chat.completions ${res.status}: ${errText.slice(0, 600)}`);
    }
    const data = await res.json();
    const msg = data?.choices?.[0]?.message?.content;
    const text = typeof msg === "string" ? msg : Array.isArray(msg) ? msg.map((x) => x?.text || "").join(" ") : "";
    return text.trim();
  }

  try {
    const text = await callResponses();
    if (text) return { text, error: null };
    throw new Error("OpenAI empty response");
  } catch (e) {
    const primaryErr = String(e.message || e);
    try {
      const text = await callChatCompletionsFallback();
      if (text) return { text, error: null };
      return { text: null, error: `OpenAI fallback empty response (primary error: ${primaryErr})` };
    } catch (e2) {
      return {
        text: null,
        error: `OpenAI request failed (${primaryErr}); fallback failed: ${String(e2.message || e2)}`
      };
    }
  }
}

export async function answerAnalyticsQuestion({ corridorId, question }) {
  const q = (question || "").trim();
  if (!q) return { answer: "Please enter a question.", modelUsed: "heuristic" };

  const context = await buildContext(corridorId);
  const latest = context.latest_run;
  const trend = context.trend_24h || {};
  const cameras = context.camera_hotspots_24h || [];
  if (!latest) return { answer: "No run data yet for this corridor.", modelUsed: "heuristic", contextCoverage: context.context_coverage };

  const llm = await maybeAskOpenAI(q, context);
  if (llm?.text) {
    return {
      answer: llm.text,
      modelUsed: "openai",
      aiError: null,
      contextCoverage: context.context_coverage
    };
  }

  let answer = "";
  if (containsAny(q, ["predict", "forecast", "next"])) {
    answer = `Next-interval forecast: p50=${Number(latest.predicted_next_score_p50 || 0).toFixed(1)}, p90=${Number(
      latest.predicted_next_score_p90 || 0
    ).toFixed(1)}, confidence=${Number(latest.prediction_confidence || 0).toFixed(1)}%.`;
  } else if (containsAny(q, ["trend", "average", "last 24", "today"])) {
    answer = `Last 24h: avg score=${Number(trend.avg_score || 0).toFixed(1)}, max score=${Number(
      trend.max_score || 0
    ).toFixed(1)}, points=${Number(trend.points || 0)}, avg weather risk=${Number(trend.avg_weather_risk || 0).toFixed(
      1
    )}.`;
  } else if (containsAny(q, ["settings", "weights", "threshold", "tune", "parameter"])) {
    const s = context.runtime_settings || {};
    const w = context.weighting_from_latest_run || {};
    const eff = w.effective_multipliers || {};
    const regime = w.regime || "normal";
    answer =
      `Runtime settings loaded: dynamic_weights_enabled=${Number(s.dynamic_weights_enabled || 0)}, ` +
      `auto_learn_weights_enabled=${Number(s.auto_learn_weights_enabled || 0)}, ` +
      `weight_traffic=${Number(s.weight_traffic || 0)}, weight_image_with_traffic=${Number(
        s.weight_image_with_traffic || 0
      )}, incident_saturation_max=${Number(s.incident_saturation_max || 0)}, alert_high_score_threshold=${Number(
        s.alert_high_score_threshold || 0
      )}. ` +
      `Latest regime=${regime}, effective multipliers: ` +
      `traffic=${Number(eff.traffic || 1).toFixed(2)}, image=${Number(eff.image || 1).toFixed(2)}, incidents=${Number(
        eff.incident || 1
      ).toFixed(2)}, closures=${Number(eff.closure || 1).toFixed(2)}, weather=${Number(eff.weather || 1).toFixed(2)}.`;
  } else if (containsAny(q, ["image mode", "proxy", "queue", "cv", "hybrid"])) {
    const img = context.image_signals_from_latest_run || {};
    const cv = img.cv || {};
    answer =
      `Image signal mode=${img.mode_label || context.methodology?.configured_image_mode || "hybrid_auto"}. ` +
      `selected=${Number(img.selected_score || latest.image_score || 0).toFixed(1)}, ` +
      `proxy=${Number(img.proxy_score || 0).toFixed(1)}, queue_stop=${Number(img.queue_stop_score || 0).toFixed(1)}, ` +
      `hybrid=${Number(img.hybrid_score || 0).toFixed(1)}, confidence=${Number(img.hybrid_confidence_pct || 0).toFixed(1)}%. ` +
      `CV provider=${cv.provider || latest.cv_provider || "disabled"}, analyzed=${Number(
        cv.analyzed_count || latest.cv_analyzed_count || 0
      )}, coverage=${Number(cv.coverage_pct || latest.cv_coverage_pct || 0).toFixed(1)}%, avg_conf=${Number(
        cv.avg_confidence || latest.cv_avg_confidence || 0
      ).toFixed(2)}.`;
  } else if (containsAny(q, ["checkpoint", "corridor", "route"])) {
    answer =
      `Corridor ${context.corridor?.name || corridorId} has ${Number(context.checkpoint_count || 0)} active checkpoints. ` +
      `Best historical local hours: ${(context.best_hours_local || []).join(", ") || "insufficient data"}, ` +
      `worst: ${(context.worst_hours_local || []).join(", ") || "insufficient data"}.`;
  } else if (containsAny(q, ["strategy", "preset", "scenario"])) {
    const a = context.active_strategy || null;
    if (a) {
      answer =
        `Active strategy="${a.strategy_name || a.strategy_id}" (id=${a.strategy_id || "n/a"}, source=${a.source || "unknown"}). ` +
        `Inputs: traffic=${Number(a.inputs?.traffic_segments || 0).toFixed(0)}, delay=${Number(
          a.inputs?.delay_pct || 0
        ).toFixed(1)}%, image=${Number(a.inputs?.image_score || 0).toFixed(1)}, incidents=${Number(
          a.inputs?.incidents_count || 0
        ).toFixed(0)}, closures=${Number(a.inputs?.closures_count || 0).toFixed(0)}, weather=${Number(
          a.inputs?.weather_risk_score || 0
        ).toFixed(1)}.`;
    } else {
      answer = "No active saved strategy for this corridor yet. Use Auto Strategy tab -> Apply to set one.";
    }
  } else if (containsAny(q, ["camera", "hotspot", "where"])) {
    const top = cameras.map((c) => `${c.camera_location} (${Number(c.avg_image_score).toFixed(1)})`).join("; ");
    answer = top ? `Top camera hotspots (24h): ${top}.` : "No recent camera hotspot data.";
  } else if (containsAny(q, ["weather", "rain", "snow", "storm", "wind", "radar"])) {
    const weather = context.weather_live || context.weather_from_latest_run || {};
    answer =
      `Weather risk=${Number(latest.weather_risk_score || 0).toFixed(1)} (component=${Number(
        latest.weather_component || 0
      ).toFixed(1)}). ` +
      `Conditions="${weather.condition_text || "unknown"}". ` +
      `Alerts=${Number(weather.alerts_count || 0)}, precip=${Number(weather.precip_probability_pct || 0).toFixed(
        0
      )}%, wind=${Number(weather.wind_mph || 0).toFixed(1)} mph.`;
  } else if (containsAny(q, ["why", "driver", "cause", "explain"])) {
    const top3 = (context.primary_drivers || [])
      .slice(0, 3)
      .map((d) => `${d.key}:${d.value.toFixed(1)} (${d.share_pct.toFixed(1)}%)`)
      .join(", ");
    answer =
      `Top score drivers now: ${top3}. Latest score=${latest.fused_score.toFixed(1)} ` +
      `(weather=${latest.weather_risk_score.toFixed(1)}, incidents=${latest.incidents_count}, closures=${latest.closures_count}).`;
  } else {
    answer =
      `Latest score=${Number(latest.fused_score || 0).toFixed(1)} ` +
      `(alert=${latest.alert_state}, incidents=${latest.incidents_count}, closures=${latest.closures_count}, weather=${Number(
        latest.weather_risk_score || 0
      ).toFixed(1)}). ` +
      `Forecast p50=${Number(latest.predicted_next_score_p50 || 0).toFixed(1)}.`;
  }
  return {
    answer,
    modelUsed: "heuristic",
    aiError: llm?.error || "OpenAI unavailable",
    contextCoverage: context.context_coverage
  };
}
