import express from "express";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { config } from "./config.js";
import { pool, runMigrations } from "./db.js";
import { runPollCycle } from "./acquisition.js";
import {
  bboxFromCenter,
  fetchRoadRoutePoints,
  geocodeCity,
  samplePointsAlongPolyline,
  straightLinePoints
} from "./geocode.js";
import { answerAnalyticsQuestion } from "./chat.js";
import { getRuntimeSettings, upsertRuntimeSettings } from "./settings.js";
import { computeFused } from "./scoring.js";
import { predictNextScore, trainNextScoreModel } from "./modeling.js";
import { fetchCorridorWeather } from "./weather.js";

const app = express();
app.use(express.json({ limit: "1mb" }));

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
app.use("/", express.static(path.join(__dirname, "..", "public")));

function parseBbox(input) {
  if (!input) return null;
  if (typeof input === "string") {
    const parts = input.split(",").map(Number);
    if (parts.length !== 4 || parts.some(Number.isNaN)) return null;
    return { xmin: parts[0], ymin: parts[1], xmax: parts[2], ymax: parts[3] };
  }
  const vals = [input.xmin, input.ymin, input.xmax, input.ymax].map(Number);
  if (vals.some(Number.isNaN)) return null;
  return { xmin: vals[0], ymin: vals[1], xmax: vals[2], ymax: vals[3] };
}

async function getCorridorById(corridorId) {
  const { rows } = await pool.query(
    `SELECT id, name,
            bbox_xmin AS xmin, bbox_ymin AS ymin, bbox_xmax AS xmax, bbox_ymax AS ymax,
            active
       FROM corridors
      WHERE id = $1`,
    [corridorId]
  );
  return rows[0] || null;
}

app.get("/api/health", async (_, res) => {
  const db = await pool.query("SELECT NOW() AS now");
  res.json({
    ok: true,
    now: db.rows[0].now,
    poll_seconds: config.pollSeconds
  });
});

app.get("/api/data/options", async (_, res) => {
  res.json({
    corridor_input_modes: ["bbox", "city_query + radius_km", "center(lat/lon) + radius_km"],
    checkpoint_input_modes: ["gps(lat/lon)+radius_km", "city_query+radius_km"],
    prediction_outputs: ["predicted_next_score_p50", "predicted_next_score_p90", "prediction_confidence", "drift_score"],
    chat_modes: ["heuristic_db_parser", "openai_if_configured"],
    weather_sources: ["api.weather.gov points/forecast/alerts/observations", "opengeo.ncep.noaa.gov MRMS radar WMS"]
  });
});

app.get("/api/settings", async (_, res) => {
  const settings = await getRuntimeSettings();
  res.json(settings);
});

app.put("/api/settings", async (req, res) => {
  const updated = await upsertRuntimeSettings(req.body || {});
  res.json(updated);
});

app.get("/api/corridors", async (req, res) => {
  const includeArchived = String(req.query.include_archived || "true") !== "false";
  const { rows } = await pool.query(
    `SELECT c.id, c.name,
            bbox_xmin AS xmin, bbox_ymin AS ymin, bbox_xmax AS xmax, bbox_ymax AS ymax,
            c.active, c.created_at, c.updated_at,
            MAX(r.run_ts) AS last_run_ts,
            COUNT(r.id) AS run_count
       FROM corridors c
  LEFT JOIN monitor_runs r ON r.corridor_id = c.id
      WHERE ($1::boolean = true OR c.active = true)
   GROUP BY c.id
   ORDER BY c.id`,
    [includeArchived]
  );
  res.json(rows);
});

app.patch("/api/corridors/:id", async (req, res) => {
  const id = Number(req.params.id);
  if (!id) return res.status(400).json({ error: "invalid corridor id" });
  const active = req.body?.active;
  const name = req.body?.name ? String(req.body.name).trim() : null;
  if (typeof active === "undefined" && !name) {
    return res.status(400).json({ error: "provide active and/or name" });
  }
  const current = await pool.query("SELECT id, name, active FROM corridors WHERE id = $1", [id]);
  if (!current.rows.length) return res.status(404).json({ error: "corridor not found" });
  const finalName = name || current.rows[0].name;
  const finalActive = typeof active === "boolean" ? active : current.rows[0].active;
  const out = await pool.query(
    `UPDATE corridors
        SET name = $2,
            active = $3,
            updated_at = NOW()
      WHERE id = $1
      RETURNING id, name, active`,
    [id, finalName, finalActive]
  );
  res.json(out.rows[0]);
});

app.post("/api/corridors", async (req, res) => {
  const { name, bbox, city, radius_km, center } = req.body || {};
  if (!name || !String(name).trim()) {
    return res.status(400).json({ error: "name is required" });
  }

  let resolved = parseBbox(bbox);
  let source = "bbox";
  let geocodeLabel = "";

  if (!resolved && center && Number.isFinite(Number(center.lat)) && Number.isFinite(Number(center.lon))) {
    resolved = bboxFromCenter(Number(center.lat), Number(center.lon), Number(radius_km || 20));
    source = "center";
  }

  if (!resolved && city) {
    const hit = await geocodeCity(city);
    if (!hit) return res.status(400).json({ error: "could not geocode city/query" });
    resolved = bboxFromCenter(hit.lat, hit.lon, Number(radius_km || 20));
    source = "city";
    geocodeLabel = hit.label;
  }

  if (!resolved) {
    return res.status(400).json({
      error: "provide one of: bbox, center{lat,lon}, or city/query"
    });
  }

  const q = await pool.query(
    `INSERT INTO corridors(name, bbox_xmin, bbox_ymin, bbox_xmax, bbox_ymax, active, updated_at)
     VALUES($1,$2,$3,$4,$5,true,NOW())
     ON CONFLICT(name) DO UPDATE
       SET bbox_xmin = EXCLUDED.bbox_xmin,
           bbox_ymin = EXCLUDED.bbox_ymin,
           bbox_xmax = EXCLUDED.bbox_xmax,
           bbox_ymax = EXCLUDED.bbox_ymax,
           active = true,
           updated_at = NOW()
     RETURNING id, name,
               bbox_xmin AS xmin, bbox_ymin AS ymin, bbox_xmax AS xmax, bbox_ymax AS ymax,
               active, created_at, updated_at`,
    [name.trim(), resolved.xmin, resolved.ymin, resolved.xmax, resolved.ymax]
  );
  res.json({ corridor: q.rows[0], source, geocode_label: geocodeLabel });
});

app.post("/api/checkpoints", async (req, res) => {
  const { corridor_id, name, lat, lon, city, radius_km } = req.body || {};
  if (!corridor_id) return res.status(400).json({ error: "corridor_id is required" });
  if (!name || !String(name).trim()) return res.status(400).json({ error: "checkpoint name is required" });

  let finalLat = Number(lat);
  let finalLon = Number(lon);
  let source = "gps";
  if ((!Number.isFinite(finalLat) || !Number.isFinite(finalLon)) && city) {
    const hit = await geocodeCity(city);
    if (!hit) return res.status(400).json({ error: "could not geocode city/query" });
    finalLat = hit.lat;
    finalLon = hit.lon;
    source = "city";
  }
  if (!Number.isFinite(finalLat) || !Number.isFinite(finalLon)) {
    return res.status(400).json({ error: "provide lat/lon or city/query" });
  }

  const q = await pool.query(
    `INSERT INTO checkpoints(corridor_id, name, lat, lon, radius_km, source)
     VALUES($1,$2,$3,$4,$5,$6)
     ON CONFLICT(corridor_id, name) DO UPDATE
       SET lat = EXCLUDED.lat,
           lon = EXCLUDED.lon,
           radius_km = EXCLUDED.radius_km,
           source = EXCLUDED.source,
           active = true
     RETURNING *`,
    [corridor_id, name.trim(), finalLat, finalLon, Number(radius_km || 8), source]
  );
  res.json(q.rows[0]);
});

app.get("/api/checkpoints", async (req, res) => {
  const corridorId = Number(req.query.corridor_id);
  if (!corridorId) return res.status(400).json({ error: "corridor_id is required" });
  const { rows } = await pool.query(
    `SELECT id, corridor_id, name, lat, lon, radius_km, source, active, created_at
       FROM checkpoints
      WHERE corridor_id = $1
      ORDER BY id`,
    [corridorId]
  );
  res.json(rows);
});

app.delete("/api/checkpoints", async (req, res) => {
  const corridorId = Number(req.query.corridor_id || req.body?.corridor_id);
  if (!corridorId) return res.status(400).json({ error: "corridor_id is required" });
  const out = await pool.query(
    `DELETE FROM checkpoints
      WHERE corridor_id = $1
      RETURNING id`,
    [corridorId]
  );
  res.json({ ok: true, deleted: out.rowCount });
});

app.post("/api/checkpoints/autogen", async (req, res) => {
  const corridorId = Number(req.body?.corridor_id);
  const fromQuery = String(req.body?.from_query || "").trim();
  const toQuery = String(req.body?.to_query || "").trim();
  const spacingKm = Math.max(5, Math.min(80, Number(req.body?.spacing_km || 16)));
  const radiusKm = Math.max(3, Math.min(30, Number(req.body?.checkpoint_radius_km || 10)));
  const maxPoints = Math.max(2, Math.min(40, Number(req.body?.max_points || 16)));
  const useRoads = req.body?.use_roads !== false;

  if (!corridorId) return res.status(400).json({ error: "corridor_id is required" });
  if (!fromQuery || !toQuery) return res.status(400).json({ error: "from_query and to_query are required" });

  const [fromHit, toHit] = await Promise.all([geocodeCity(fromQuery), geocodeCity(toQuery)]);
  if (!fromHit || !toHit) {
    return res.status(400).json({ error: "could not geocode from/to query" });
  }

  let routePoints = null;
  let routeSource = "straight_line";
  if (useRoads) {
    routePoints = await fetchRoadRoutePoints(fromHit.lat, fromHit.lon, toHit.lat, toHit.lon);
    if (routePoints?.length) routeSource = "osrm_road";
  }
  if (!routePoints || routePoints.length < 2) {
    routePoints = straightLinePoints(fromHit.lat, fromHit.lon, toHit.lat, toHit.lon, 28);
    routeSource = "straight_line";
  }

  let sampled = samplePointsAlongPolyline(routePoints, spacingKm);
  if (sampled.length > maxPoints) {
    const step = (sampled.length - 1) / (maxPoints - 1);
    const reduced = [];
    for (let i = 0; i < maxPoints; i++) {
      reduced.push(sampled[Math.round(i * step)]);
    }
    sampled = reduced;
  }

  const inserted = [];
  for (let i = 0; i < sampled.length; i++) {
    const p = sampled[i];
    const cpName =
      i === 0 ? "Auto_Start" : i === sampled.length - 1 ? "Auto_End" : `Auto_${String(i).padStart(2, "0")}`;
    const q = await pool.query(
      `INSERT INTO checkpoints(corridor_id, name, lat, lon, radius_km, source)
       VALUES($1,$2,$3,$4,$5,$6)
       ON CONFLICT(corridor_id, name) DO UPDATE
         SET lat = EXCLUDED.lat,
             lon = EXCLUDED.lon,
             radius_km = EXCLUDED.radius_km,
             source = EXCLUDED.source,
             active = true
       RETURNING id, corridor_id, name, lat, lon, radius_km, source, active`,
      [corridorId, cpName, p.lat, p.lon, radiusKm, `autogen_${routeSource}`]
    );
    inserted.push(q.rows[0]);
  }

  res.json({
    ok: true,
    route_source: routeSource,
    from: { query: fromQuery, label: fromHit.label, lat: fromHit.lat, lon: fromHit.lon },
    to: { query: toQuery, label: toHit.label, lat: toHit.lat, lon: toHit.lon },
    generated_count: inserted.length,
    checkpoints: inserted
  });
});

app.get("/api/runs/latest", async (req, res) => {
  const corridorId = Number(req.query.corridor_id);
  if (!corridorId) return res.status(400).json({ error: "corridor_id is required" });
  const { rows } = await pool.query(
    `SELECT r.*, c.name as corridor_name
       FROM monitor_runs r
       JOIN corridors c ON c.id = r.corridor_id
      WHERE corridor_id = $1
      ORDER BY run_ts DESC
      LIMIT 1`,
    [corridorId]
  );
  if (!rows.length) return res.json(null);
  res.json(rows[0]);
});

app.get("/api/runs/timeseries", async (req, res) => {
  const corridorId = Number(req.query.corridor_id);
  const hours = Math.min(168, Math.max(1, Number(req.query.hours || 24)));
  if (!corridorId) return res.status(400).json({ error: "corridor_id is required" });
  const { rows } = await pool.query(
    `SELECT run_ts, fused_score, image_score, delay_pct, incidents_count, closures_count,
            weather_risk_score, weather_component, alert_state,
            predicted_next_score_p50, predicted_next_score_p90, prediction_confidence, drift_score
       FROM monitor_runs
      WHERE corridor_id = $1
        AND run_ts >= NOW() - ($2::text || ' hours')::interval
      ORDER BY run_ts ASC`,
    [corridorId, hours]
  );
  res.json(rows);
});

app.get("/api/models/latest", async (req, res) => {
  const corridorId = Number(req.query.corridor_id);
  if (!corridorId) return res.status(400).json({ error: "corridor_id is required" });
  const { rows } = await pool.query(
    `SELECT created_at, model_version, train_rows, residual_sigma, r2, drift_score, coefficients, feature_stats
       FROM model_snapshots
      WHERE corridor_id = $1
      ORDER BY created_at DESC
      LIMIT 1`,
    [corridorId]
  );
  res.json(rows[0] || null);
});

app.post("/api/chat/query", async (req, res) => {
  const corridorId = Number(req.body?.corridor_id);
  const question = String(req.body?.question || "");
  if (!corridorId) return res.status(400).json({ error: "corridor_id is required" });
  const out = await answerAnalyticsQuestion({ corridorId, question });
  await pool.query(
    `INSERT INTO chat_logs(corridor_id, question, response, model_used)
     VALUES($1,$2,$3,$4)`,
    [corridorId, question, out.answer, out.modelUsed]
  );
  res.json(out);
});

app.get("/api/analysis/insights", async (req, res) => {
  const corridorId = Number(req.query.corridor_id);
  const hours = Math.min(720, Math.max(1, Number(req.query.hours || 24)));
  if (!corridorId) return res.status(400).json({ error: "corridor_id is required" });

  const [summaryQ, hourlyQ, hotspotsQ] = await Promise.all([
    pool.query(
      `SELECT
          COUNT(*)::int AS points,
          AVG(fused_score)::float8 AS avg_score,
          STDDEV_SAMP(fused_score)::float8 AS std_score,
          COALESCE(PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY fused_score), 0)::float8 AS p90_score,
          AVG(weather_risk_score)::float8 AS avg_weather_risk,
          COALESCE(PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY weather_risk_score), 0)::float8 AS p90_weather_risk,
          AVG((fused_score <= 35)::int)::float8 AS reliability_tight,
          AVG((fused_score <= 50)::int)::float8 AS reliability_relaxed,
          COALESCE(regr_slope(fused_score, EXTRACT(EPOCH FROM run_ts)), 0)::float8 AS slope_per_sec
         FROM monitor_runs
        WHERE corridor_id = $1
          AND run_ts >= NOW() - ($2::text || ' hours')::interval`,
      [corridorId, hours]
    ),
    pool.query(
      `SELECT EXTRACT(HOUR FROM run_ts AT TIME ZONE 'America/Chicago')::int AS hour_local,
              AVG(fused_score)::float8 AS avg_score,
              AVG(weather_risk_score)::float8 AS avg_weather_risk,
              COUNT(*)::int AS points
         FROM monitor_runs
        WHERE corridor_id = $1
          AND run_ts >= NOW() - ($2::text || ' hours')::interval
        GROUP BY 1
        ORDER BY 1`,
      [corridorId, hours]
    ),
    pool.query(
      `SELECT o.camera_location,
              AVG(o.image_score)::float8 AS avg_image_score,
              MAX(o.image_score)::float8 AS max_image_score,
              COUNT(*)::int AS observations
         FROM camera_observations o
         JOIN monitor_runs r ON r.id = o.run_id
        WHERE r.corridor_id = $1
          AND r.run_ts >= NOW() - ($2::text || ' hours')::interval
        GROUP BY o.camera_location
        ORDER BY AVG(o.image_score) DESC
        LIMIT 10`,
      [corridorId, hours]
    )
  ]);

  const s = summaryQ.rows[0] || {};
  const slopePerHour = Number(s.slope_per_sec || 0) * 3600;
  const reliabilityTight = Number(s.reliability_tight || 0) * 100;
  const reliabilityRelaxed = Number(s.reliability_relaxed || 0) * 100;
  const stdScore = Number(s.std_score || 0);
  const variabilityBand = stdScore < 6 ? "stable" : stdScore < 12 ? "moderate" : "volatile";
  const avgWeatherRisk = Number(s.avg_weather_risk || 0);
  const p90WeatherRisk = Number(s.p90_weather_risk || 0);

  const rankedHours = (hourlyQ.rows || []).filter((h) => Number(h.points || 0) >= 2);
  const bestHours = [...rankedHours]
    .sort((a, b) => Number(a.avg_score || 0) - Number(b.avg_score || 0))
    .slice(0, 3)
    .map((h) => Number(h.hour_local));
  const worstHours = [...rankedHours]
    .sort((a, b) => Number(b.avg_score || 0) - Number(a.avg_score || 0))
    .slice(0, 3)
    .map((h) => Number(h.hour_local));

  const fmtHour = (h) => `${String(Math.max(0, Math.min(23, Number(h)))).padStart(2, "0")}:00`;
  const bestHoursLabel = bestHours.length ? bestHours.map(fmtHour).join(", ") : "insufficient history";
  const worstHoursLabel = worstHours.length ? worstHours.map(fmtHour).join(", ") : "insufficient history";

  const recommendations = [];
  if (reliabilityTight < 65) recommendations.push("Tight reliability is low; widen checkpoint radii or increase sample_limit.");
  if (variabilityBand === "volatile") recommendations.push("High score volatility detected; inspect top hotspot cameras for unstable scenes.");
  if (slopePerHour > 1.8) recommendations.push("Rising trend detected; prepare pre-emptive alert thresholds.");
  if (avgWeatherRisk >= 60) recommendations.push("Weather risk is elevated; prioritize recent radar trend before departure decisions.");
  recommendations.push(`Best local travel windows (historical): ${bestHoursLabel}.`);
  recommendations.push(`Most volatile windows (historical): ${worstHoursLabel}.`);
  if (!recommendations.length) recommendations.push("Conditions look stable; continue collecting data for stronger model confidence.");

  res.json({
    window_hours: hours,
    summary: {
      points: Number(s.points || 0),
      avg_score: Number(s.avg_score || 0),
      p90_score: Number(s.p90_score || 0),
      std_score: stdScore,
      variability_band: variabilityBand,
      reliability_tight_pct: reliabilityTight,
      reliability_relaxed_pct: reliabilityRelaxed,
      avg_weather_risk: avgWeatherRisk,
      p90_weather_risk: p90WeatherRisk,
      trend_slope_per_hour: slopePerHour
    },
    hourly_profile: hourlyQ.rows,
    best_hours_local: bestHours,
    worst_hours_local: worstHours,
    hotspots: hotspotsQ.rows,
    recommendations
  });
});

app.post("/api/analysis/simulate", async (req, res) => {
  const corridorId = Number(req.body?.corridor_id);
  if (!corridorId) return res.status(400).json({ error: "corridor_id is required" });

  const latestQ = await pool.query(
    `SELECT fused_score, traffic_segments, image_score, delay_pct, incidents_count, closures_count, camera_fresh_pct, weather_risk_score
       FROM monitor_runs
      WHERE corridor_id = $1
      ORDER BY run_ts DESC
      LIMIT 1`,
    [corridorId]
  );
  if (!latestQ.rows.length) return res.status(400).json({ error: "no historical run data for corridor" });
  const latest = latestQ.rows[0];
  const runtimeSettings = await getRuntimeSettings();

  const scenario = {
    traffic_segments: Math.max(0, Number(req.body?.traffic_segments ?? latest.traffic_segments ?? 0)),
    delay_pct: Math.max(0, Number(req.body?.delay_pct ?? latest.delay_pct ?? 0)),
    image_score: Math.max(0, Number(req.body?.image_score ?? latest.image_score ?? 0)),
    incidents_count: Math.max(0, Number(req.body?.incidents_count ?? latest.incidents_count ?? 0)),
    closures_count: Math.max(0, Number(req.body?.closures_count ?? latest.closures_count ?? 0)),
    weather_risk_score: Math.max(0, Math.min(100, Number(req.body?.weather_risk_score ?? latest.weather_risk_score ?? 0))),
    camera_fresh_pct: Math.max(0, Math.min(100, Number(req.body?.camera_fresh_pct ?? latest.camera_fresh_pct ?? 100)))
  };

  const fused = computeFused(
    {
      trafficSegments: scenario.traffic_segments,
      delayPct: scenario.delay_pct,
      imageScore: scenario.image_score,
      incidents: scenario.incidents_count,
      closures: scenario.closures_count,
      weatherRiskScore: scenario.weather_risk_score
    },
    runtimeSettings
  );

  const trainQ = await pool.query(
    `SELECT run_ts, fused_score, image_score, incidents_count, closures_count, camera_fresh_pct
       FROM monitor_runs
      WHERE corridor_id = $1
      ORDER BY run_ts DESC
      LIMIT 600`,
    [corridorId]
  );
  const model = trainNextScoreModel(trainQ.rows.reverse(), runtimeSettings);
  const prediction = predictNextScore(
    model,
    {
      fused_score: fused.fused,
      image_score: scenario.image_score,
      incidents_count: scenario.incidents_count,
      closures_count: scenario.closures_count,
      camera_fresh_pct: scenario.camera_fresh_pct
    },
    runtimeSettings
  );

  res.json({
    scenario,
    fused,
    prediction,
    model_version: model?.version || "fallback_v1"
  });
});

app.get("/api/runs/recent-cameras", async (req, res) => {
  const corridorId = Number(req.query.corridor_id);
  if (!corridorId) return res.status(400).json({ error: "corridor_id is required" });
  const { rows } = await pool.query(
    `SELECT *
       FROM (
         SELECT DISTINCT ON (o.camera_object_id)
                o.camera_object_id,
                o.camera_location,
                o.camera_direction,
                o.snapshot_url,
                o.snapshot_bytes,
                o.age_minutes,
                o.image_score,
                r.run_ts
           FROM camera_observations o
           JOIN monitor_runs r ON r.id = o.run_id
          WHERE r.corridor_id = $1
          ORDER BY o.camera_object_id, r.run_ts DESC
       ) x
      ORDER BY x.image_score DESC
      LIMIT 30`,
    [corridorId]
  );
  res.json(rows);
});

app.get("/api/weather/live", async (req, res) => {
  const corridorId = Number(req.query.corridor_id);
  if (!corridorId) return res.status(400).json({ error: "corridor_id is required" });
  const corridor = await getCorridorById(corridorId);
  if (!corridor) return res.status(404).json({ error: "corridor not found" });
  const runtimeSettings = await getRuntimeSettings();
  const weather = await fetchCorridorWeather(corridor, runtimeSettings, { useCache: true });
  res.json({
    corridor_id: corridor.id,
    corridor_name: corridor.name,
    from_live_fetch: true,
    ...weather
  });
});

app.get("/api/weather/current", async (req, res) => {
  const corridorId = Number(req.query.corridor_id);
  if (!corridorId) return res.status(400).json({ error: "corridor_id is required" });
  const corridor = await getCorridorById(corridorId);
  if (!corridor) return res.status(404).json({ error: "corridor not found" });

  const latestQ = await pool.query(
    `SELECT run_ts, weather_risk_score, weather_component, raw_json
       FROM monitor_runs
      WHERE corridor_id = $1
      ORDER BY run_ts DESC
      LIMIT 1`,
    [corridorId]
  );
  const latest = latestQ.rows[0] || null;
  let weatherFromRun = latest?.raw_json?.weather || null;
  if (!weatherFromRun && typeof latest?.raw_json === "string") {
    try {
      weatherFromRun = JSON.parse(latest.raw_json || "{}")?.weather || null;
    } catch {
      weatherFromRun = null;
    }
  }

  if (weatherFromRun) {
    return res.json({
      corridor_id: corridor.id,
      corridor_name: corridor.name,
      from_live_fetch: false,
      run_ts: latest.run_ts,
      weather_risk_score: Number(latest.weather_risk_score || weatherFromRun.weather_risk_score || 0),
      weather_component: Number(latest.weather_component || 0),
      ...weatherFromRun
    });
  }

  const runtimeSettings = await getRuntimeSettings();
  const live = await fetchCorridorWeather(corridor, runtimeSettings, { useCache: true });
  res.json({
    corridor_id: corridor.id,
    corridor_name: corridor.name,
    from_live_fetch: true,
    weather_component: 0,
    ...live
  });
});

app.post("/api/poll-now", async (_, res) => {
  const runtimeSettings = await getRuntimeSettings();
  const result = await runPollCycle({
    sampleLimit: runtimeSettings.sample_limit,
    baselineAlpha: runtimeSettings.baseline_alpha,
    runtimeSettings
  });
  res.json({ ok: true, result });
});

async function ensureSeedCorridors() {
  await pool.query(
    `INSERT INTO corridors(name, bbox_xmin, bbox_ymin, bbox_xmax, bbox_ymax, active)
     VALUES
     ('quad_cities', -90.80, 41.30, -90.20, 41.70, true),
     ('chicago_metro', -88.50, 41.40, -87.30, 42.30, true),
     ('i55_central', -90.00, 39.50, -88.80, 40.30, true)
     ON CONFLICT(name) DO NOTHING`
  );
}

async function bootstrap() {
  await runMigrations();
  await ensureSeedCorridors();

  const loop = async () => {
    try {
      const runtimeSettings = await getRuntimeSettings();
      await runPollCycle({
        sampleLimit: runtimeSettings.sample_limit,
        baselineAlpha: runtimeSettings.baseline_alpha,
        runtimeSettings
      });
      const waitMs = Number(runtimeSettings.poll_seconds || config.pollSeconds) * 1000;
      setTimeout(loop, Math.max(waitMs, 30000));
    } catch (e) {
      console.error("poll cycle failed", e);
      setTimeout(loop, 60000);
    }
  };
  loop().catch((e) => console.error("initial poll loop failed", e));

  app.listen(config.port, () => {
    console.log(`server listening on http://localhost:${config.port}`);
  });
}

bootstrap().catch((e) => {
  console.error(e);
  process.exit(1);
});
