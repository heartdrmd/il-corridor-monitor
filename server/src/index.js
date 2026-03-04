import express from "express";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { config } from "./config.js";
import { pool, runMigrations } from "./db.js";
import { runPollCycle } from "./acquisition.js";
import { bboxFromCenter, geocodeCity } from "./geocode.js";
import { answerAnalyticsQuestion } from "./chat.js";

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
    chat_modes: ["heuristic_db_parser", "openai_if_configured"]
  });
});

app.get("/api/corridors", async (_, res) => {
  const { rows } = await pool.query(
    `SELECT id, name, xmin, ymin, xmax, ymax, active, created_at, updated_at
       FROM corridors ORDER BY id`
  );
  res.json(rows);
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
    `INSERT INTO corridors(name, xmin, ymin, xmax, ymax, active, updated_at)
     VALUES($1,$2,$3,$4,$5,true,NOW())
     ON CONFLICT(name) DO UPDATE
       SET xmin = EXCLUDED.xmin,
           ymin = EXCLUDED.ymin,
           xmax = EXCLUDED.xmax,
           ymax = EXCLUDED.ymax,
           active = true,
           updated_at = NOW()
     RETURNING *`,
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
    `SELECT run_ts, fused_score, image_score, delay_pct, incidents_count, closures_count, alert_state,
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

app.get("/api/runs/recent-cameras", async (req, res) => {
  const corridorId = Number(req.query.corridor_id);
  if (!corridorId) return res.status(400).json({ error: "corridor_id is required" });
  const { rows } = await pool.query(
    `SELECT o.camera_object_id, o.camera_location, o.camera_direction, o.snapshot_url, o.snapshot_bytes, o.age_minutes, o.image_score
       FROM camera_observations o
       JOIN monitor_runs r ON r.id = o.run_id
      WHERE r.corridor_id = $1
      ORDER BY r.run_ts DESC, o.image_score DESC
      LIMIT 30`,
    [corridorId]
  );
  res.json(rows);
});

app.post("/api/poll-now", async (_, res) => {
  const result = await runPollCycle({
    sampleLimit: config.sampleLimit,
    baselineAlpha: config.baselineAlpha
  });
  res.json({ ok: true, result });
});

async function ensureSeedCorridors() {
  await pool.query(
    `INSERT INTO corridors(name, xmin, ymin, xmax, ymax, active)
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
  runPollCycle({
    sampleLimit: config.sampleLimit,
    baselineAlpha: config.baselineAlpha
  }).catch((e) => console.error("initial poll cycle failed", e));

  setInterval(() => {
    runPollCycle({
      sampleLimit: config.sampleLimit,
      baselineAlpha: config.baselineAlpha
    }).catch((e) => console.error("poll cycle failed", e));
  }, config.pollSeconds * 1000);

  app.listen(config.port, () => {
    console.log(`server listening on http://localhost:${config.port}`);
  });
}

bootstrap().catch((e) => {
  console.error(e);
  process.exit(1);
});
