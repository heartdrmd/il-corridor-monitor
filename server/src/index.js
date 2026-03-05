import express from "express";
import net from "node:net";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { config, feeds } from "./config.js";
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
import {
  getActiveMethodologyPreset,
  getActiveStrategy,
  getMethodologyPresetById,
  getRuntimeSettings,
  listMethodologyPresets,
  setActiveMethodologyPreset,
  upsertActiveStrategy,
  upsertRuntimeSettings
} from "./settings.js";
import { computeFused, trainDynamicComponentMultipliers } from "./scoring.js";
import { predictNextScore, trainNextScoreModel } from "./modeling.js";
import { fetchCorridorWeather } from "./weather.js";
import { fetchNormalizedFeeds, listFeedProfiles, normalizeCorridorFeedProfile } from "./feedProviders.js";

const app = express();
app.use(express.json({ limit: "1mb" }));
let activePollPromise = null;
let activePollStartedAt = 0;
let activePollSource = "";

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

function normalizeFeedProfileId(raw) {
  return normalizeCorridorFeedProfile({ feed_profile: raw });
}

function parseDistrictCsv(raw) {
  const txt = String(raw || "").trim();
  if (!txt) return [];
  const out = txt
    .split(",")
    .map((x) => Number(x.trim()))
    .filter((n) => Number.isInteger(n) && n >= 1 && n <= 12);
  return [...new Set(out)];
}

function normalizeFeedConfig(feedProfile, rawFeedConfig = {}, caDistrictsCsv = "") {
  const profile = normalizeFeedProfileId(feedProfile);
  const cfg = rawFeedConfig && typeof rawFeedConfig === "object" ? { ...rawFeedConfig } : {};
  if (profile === "ca_cwwp2") {
    const fromCsv = parseDistrictCsv(caDistrictsCsv);
    const fromCfg = Array.isArray(cfg.districts) ? cfg.districts : [];
    const districts = fromCsv.length ? fromCsv : fromCfg;
    if (districts.length) cfg.districts = [...new Set(districts.map((n) => Number(n)).filter((n) => Number.isInteger(n) && n >= 1 && n <= 12))];
    else delete cfg.districts;
  } else {
    delete cfg.districts;
  }
  return cfg;
}

async function getCorridorById(corridorId) {
  const { rows } = await pool.query(
    `SELECT id, name,
            bbox_xmin AS xmin, bbox_ymin AS ymin, bbox_xmax AS xmax, bbox_ymax AS ymax,
            feed_profile, feed_config,
            active
       FROM corridors
      WHERE id = $1`,
    [corridorId]
  );
  return rows[0] || null;
}

async function getAutoLearnMultipliers(corridorId, runtimeSettings) {
  const autoLearnEnabled = Number(runtimeSettings.auto_learn_weights_enabled ?? 1) >= 0.5;
  if (!autoLearnEnabled) return { multipliers: null, fit: { rows: 0, r2: 0, rmse: 0 } };
  const windowPoints = Math.max(24, Number(runtimeSettings.auto_learn_window_points ?? 240));
  const minRows = Math.max(20, Number(runtimeSettings.auto_learn_min_rows ?? 60));
  const { rows } = await pool.query(
    `SELECT run_ts, fused_score, traffic_component, image_component, incident_component, closure_component, weather_component
       FROM monitor_runs
      WHERE corridor_id = $1
      ORDER BY run_ts DESC
      LIMIT $2`,
    [corridorId, windowPoints]
  );
  const asc = rows.reverse();
  const samples = [];
  for (let i = 0; i + 1 < asc.length; i++) {
    samples.push({
      traffic_component: Number(asc[i].traffic_component || 0),
      image_component: Number(asc[i].image_component || 0),
      incident_component: Number(asc[i].incident_component || 0),
      closure_component: Number(asc[i].closure_component || 0),
      weather_component: Number(asc[i].weather_component || 0),
      target_fused: Number(asc[i + 1].fused_score || 0)
    });
  }
  if (samples.length < minRows) return { multipliers: null, fit: { rows: samples.length, r2: 0, rmse: 0 } };
  const learned = trainDynamicComponentMultipliers(samples, runtimeSettings);
  if (!learned?.fit?.rows || learned.fit.rows < minRows) {
    return { multipliers: null, fit: learned?.fit || { rows: samples.length, r2: 0, rmse: 0 } };
  }
  return { multipliers: learned.multipliers, fit: learned.fit };
}

async function runPollCycleGuarded({ sampleLimit, baselineAlpha, runtimeSettings, source = "unknown" }) {
  if (activePollPromise) return { alreadyRunning: true, source: activePollSource, startedAt: activePollStartedAt, promise: activePollPromise };
  activePollStartedAt = Date.now();
  activePollSource = source;
  activePollPromise = runPollCycle({
    sampleLimit,
    baselineAlpha,
    runtimeSettings
  });
  try {
    const result = await activePollPromise;
    return { alreadyRunning: false, source, startedAt: activePollStartedAt, result };
  } finally {
    activePollPromise = null;
    activePollStartedAt = 0;
    activePollSource = "";
  }
}

function clampNum(v, lo, hi) {
  const n = Number(v);
  if (!Number.isFinite(n)) return lo;
  return Math.max(lo, Math.min(hi, n));
}

function isPrivateHost(hostname) {
  const h = String(hostname || "").trim().toLowerCase();
  if (!h) return true;
  if (h === "localhost" || h.endsWith(".local")) return true;

  const ipType = net.isIP(h);
  if (ipType === 4) {
    if (h.startsWith("10.")) return true;
    if (h.startsWith("127.")) return true;
    if (h.startsWith("169.254.")) return true;
    if (h.startsWith("192.168.")) return true;
    if (h.startsWith("0.")) return true;
    const parts = h.split(".").map((x) => Number(x));
    if (parts.length === 4 && parts[0] === 172 && parts[1] >= 16 && parts[1] <= 31) return true;
    return false;
  }
  if (ipType === 6) {
    if (h === "::1" || h.startsWith("fe80:") || h.startsWith("fc") || h.startsWith("fd")) return true;
    return false;
  }
  return false;
}

function quantile(values, q, fallback = 0) {
  const arr = (Array.isArray(values) ? values : [])
    .map((v) => Number(v))
    .filter((v) => Number.isFinite(v))
    .sort((a, b) => a - b);
  if (!arr.length) return Number(fallback || 0);
  const p = clampNum(Number(q), 0, 1);
  const idx = (arr.length - 1) * p;
  const lo = Math.floor(idx);
  const hi = Math.ceil(idx);
  if (lo === hi) return arr[lo];
  const mix = idx - lo;
  return arr[lo] * (1 - mix) + arr[hi] * mix;
}

function quantilePack(values, fallback = 0) {
  return {
    p10: quantile(values, 0.1, fallback),
    p20: quantile(values, 0.2, fallback),
    p35: quantile(values, 0.35, fallback),
    p50: quantile(values, 0.5, fallback),
    p60: quantile(values, 0.6, fallback),
    p70: quantile(values, 0.7, fallback),
    p80: quantile(values, 0.8, fallback),
    p90: quantile(values, 0.9, fallback)
  };
}

function normalizeScenarioInputs(raw = {}, fallback = {}) {
  return {
    traffic_segments: Math.max(0, Number(raw.traffic_segments ?? fallback.traffic_segments ?? 0)),
    delay_pct: clampNum(raw.delay_pct ?? fallback.delay_pct ?? 0, 0, 100),
    image_score: clampNum(raw.image_score ?? fallback.image_score ?? 0, 0, 100),
    incidents_count: Math.max(0, Math.round(Number(raw.incidents_count ?? fallback.incidents_count ?? 0))),
    closures_count: Math.max(0, Math.round(Number(raw.closures_count ?? fallback.closures_count ?? 0))),
    weather_risk_score: clampNum(raw.weather_risk_score ?? fallback.weather_risk_score ?? 0, 0, 100),
    camera_fresh_pct: clampNum(raw.camera_fresh_pct ?? fallback.camera_fresh_pct ?? 100, 0, 100)
  };
}

const radiusTuneJobs = new Map();

function radiusTuneJobKey(corridorId) {
  return `radius_tune_corridor_${Number(corridorId)}`;
}

function corridorCenterFromBbox(corridor) {
  return {
    lat: (Number(corridor.ymin) + Number(corridor.ymax)) / 2,
    lon: (Number(corridor.xmin) + Number(corridor.xmax)) / 2
  };
}

function bboxString(c) {
  return `${c.xmin},${c.ymin},${c.xmax},${c.ymax}`;
}

function cameraTooOldLikeAcquisition(attrs = {}, maxAgeMin = 16) {
  const age = Number(attrs.AgeInMinutes);
  const tooOldRaw = String(attrs.TooOld || "").toLowerCase();
  const tooOld = tooOldRaw === "true" || tooOldRaw === "1" || tooOldRaw === "yes";
  if (tooOld) return true;
  if (Number.isFinite(age) && age > Number(maxAgeMin || 16)) return true;
  return false;
}

function cameraSignalScore({ trafficSegments, freshCameras, totalCameras, freshPct, settings }) {
  const targetPool = Math.max(20, Number(settings.sample_limit_max || 140) * 1.35);
  const trafficScore = clampNum(Math.log1p(trafficSegments) / Math.log1p(120), 0, 1);
  const freshCamScore = clampNum(Math.log1p(freshCameras) / Math.log1p(targetPool), 0, 1);
  const freshPctScore = clampNum(Number(freshPct || 0) / 100, 0, 1);
  const cameraDensityBalance = Math.exp(-Math.abs(Number(totalCameras || 0) - targetPool) / Math.max(1, targetPool));
  const compositeCam = (0.7 * freshCamScore + 0.3 * freshPctScore) * cameraDensityBalance;
  let score = 100 * (0.56 * trafficScore + 0.44 * compositeCam);

  if (trafficSegments <= 0) score -= 30;
  if (freshCameras < Math.max(6, Number(settings.min_scoped_cameras_for_strict || 8))) score -= 18;
  if (Number(totalCameras || 0) < 8) score -= 12;
  return clampNum(score, 0, 100);
}

async function evaluateCorridorRadiusCandidate(corridor, center, radiusKm, runtimeSettings) {
  const bbox = bboxFromCenter(center.lat, center.lon, radiusKm);
  const feedSnapshot = await fetchNormalizedFeeds(corridor, {
    bboxOverride: bbox,
    needTraffic: true,
    needIncidents: true,
    needClosures: true,
    needCameras: true,
    timeoutMs: 12000
  });
  const traffic = feedSnapshot.trafficFeatures || [];
  const incidents = feedSnapshot.incidentFeatures || [];
  const closures = feedSnapshot.closureFeatures || [];
  const cameras = feedSnapshot.cameraFeatures || [];

  const validTrafficSegments = traffic.filter((f) => {
    const a = f?.attributes || {};
    return Number(a.SPEED) >= 0 && Number(a.SPEED_FF) > 0;
  }).length;
  const cameraAttrs = cameras.map((f) => f?.attributes || {});
  const maxAge = Math.max(3, Number(runtimeSettings.camera_hard_max_age_minutes || 16));
  const freshCameras = cameraAttrs.filter((a) => {
    const hasSnapshot = String(a.SnapShot || "").trim().length > 0;
    return hasSnapshot && !cameraTooOldLikeAcquisition(a, maxAge);
  }).length;
  const totalCameras = cameraAttrs.length;
  const freshPct = totalCameras > 0 ? (freshCameras * 100) / totalCameras : 0;
  const incidentCount = incidents.length;
  const closureCount = closures.length;
  const signalScore = cameraSignalScore({
    trafficSegments: validTrafficSegments,
    freshCameras,
    totalCameras,
    freshPct,
    settings: runtimeSettings
  });
  const incidentAdj = clampNum(Math.log1p(incidentCount + closureCount) / Math.log1p(40), 0, 1) * 10;
  const score = clampNum(signalScore + incidentAdj, 0, 100);
  return {
    radius_km: Number(radiusKm),
    score,
    traffic_segments: validTrafficSegments,
    camera_total: totalCameras,
    camera_fresh: freshCameras,
    camera_fresh_pct: freshPct,
    incidents_count: incidentCount,
    closures_count: closureCount,
    bbox
  };
}

function summarizeRadiusTuneJob(job) {
  const rows = Object.values(job.candidateStats || {})
    .map((r) => ({
      radius_km: Number(r.radius_km),
      score_avg: Number(r.score_sum || 0) / Math.max(1, Number(r.eval_count || 0)),
      eval_count: Number(r.eval_count || 0),
      latest_score: Number(r.latest_score || 0),
      traffic_segments_avg: Number(r.traffic_sum || 0) / Math.max(1, Number(r.eval_count || 0)),
      camera_fresh_pct_avg: Number(r.fresh_pct_sum || 0) / Math.max(1, Number(r.eval_count || 0)),
      camera_total_avg: Number(r.camera_total_sum || 0) / Math.max(1, Number(r.eval_count || 0)),
      incidents_avg: Number(r.incident_sum || 0) / Math.max(1, Number(r.eval_count || 0)),
      closures_avg: Number(r.closure_sum || 0) / Math.max(1, Number(r.eval_count || 0))
    }))
    .sort((a, b) => b.score_avg - a.score_avg);
  const best = rows[0] || null;
  return { ranking: rows, best };
}

async function persistRadiusTuneResult(corridorId, payload) {
  const key = `radius_tune_last_corridor_${Number(corridorId)}`;
  await pool.query(
    `INSERT INTO app_state(key, value, updated_at)
     VALUES($1, $2::jsonb, NOW())
     ON CONFLICT(key) DO UPDATE
       SET value = EXCLUDED.value,
           updated_at = NOW()`,
    [key, JSON.stringify(payload)]
  );
}

async function runRadiusTuneJob(job) {
  const corridorId = Number(job.corridor_id);
  try {
    job.status = "running";
    const runtimeSettings = await getRuntimeSettings();
    const corridor = await getCorridorById(corridorId);
    if (!corridor) throw new Error("corridor not found");
    const center = corridorCenterFromBbox(corridor);
    const durationMs = Math.max(15, Number(job.duration_seconds || 90)) * 1000;
    const startTs = Date.now();
    const endTs = startTs + durationMs;
    job.started_at = new Date(startTs).toISOString();
    job.eta_done_at = new Date(endTs).toISOString();
    job.progress_pct = 0;

    const candidates = [];
    const minR = Number(job.min_radius_km);
    const maxR = Number(job.max_radius_km);
    const step = Number(job.step_km);
    for (let r = minR; r <= maxR + 1e-9; r += step) {
      candidates.push(Number(r.toFixed(2)));
    }
    if (!candidates.length) throw new Error("no candidates");
    job.candidates = candidates;
    job.candidateStats = {};
    candidates.forEach((r) => {
      job.candidateStats[String(r)] = {
        radius_km: r,
        eval_count: 0,
        score_sum: 0,
        latest_score: 0,
        traffic_sum: 0,
        fresh_pct_sum: 0,
        camera_total_sum: 0,
        incident_sum: 0,
        closure_sum: 0
      };
    });

    while (Date.now() < endTs) {
      for (const radiusKm of candidates) {
        if (Date.now() >= endTs) break;
        const row = await evaluateCorridorRadiusCandidate(corridor, center, radiusKm, runtimeSettings);
        const stat = job.candidateStats[String(radiusKm)];
        stat.eval_count += 1;
        stat.score_sum += Number(row.score || 0);
        stat.latest_score = Number(row.score || 0);
        stat.traffic_sum += Number(row.traffic_segments || 0);
        stat.fresh_pct_sum += Number(row.camera_fresh_pct || 0);
        stat.camera_total_sum += Number(row.camera_total || 0);
        stat.incident_sum += Number(row.incidents_count || 0);
        stat.closure_sum += Number(row.closures_count || 0);
        job.last_eval = {
          at: new Date().toISOString(),
          ...row
        };
        job.progress_pct = clampNum(((Date.now() - startTs) / durationMs) * 100, 0, 100);
        const { best } = summarizeRadiusTuneJob(job);
        job.best_so_far = best;
      }
    }

    const summary = summarizeRadiusTuneJob(job);
    job.ranking = summary.ranking;
    job.best = summary.best;
    if (!job.best) throw new Error("no best candidate found");

    const bestRadius = Number(job.best.radius_km);
    const bestBbox = bboxFromCenter(center.lat, center.lon, bestRadius);
    await pool.query(
      `UPDATE corridors
          SET bbox_xmin = $2,
              bbox_ymin = $3,
              bbox_xmax = $4,
              bbox_ymax = $5,
              updated_at = NOW()
        WHERE id = $1`,
      [corridorId, bestBbox.xmin, bestBbox.ymin, bestBbox.xmax, bestBbox.ymax]
    );
    job.applied = true;
    job.applied_radius_km = bestRadius;
    job.applied_at = new Date().toISOString();
    job.status = "done";
    job.progress_pct = 100;
    await persistRadiusTuneResult(corridorId, {
      corridor_id: corridorId,
      done_at: job.applied_at,
      best: job.best,
      ranking: job.ranking.slice(0, 10),
      duration_seconds: Number(job.duration_seconds || 90)
    });
  } catch (e) {
    job.status = "failed";
    job.error = String(e.message || e);
    job.progress_pct = clampNum(Number(job.progress_pct || 0), 0, 100);
  } finally {
    job.finished_at = new Date().toISOString();
  }
}

function routeAutogenKey(corridorId) {
  return `route_autogen_corridor_${Number(corridorId)}`;
}

function routePairKey(corridorId) {
  return `route_pair_corridor_${Number(corridorId)}`;
}

async function upsertAppStateJson(key, value) {
  await pool.query(
    `INSERT INTO app_state(key, value, updated_at)
     VALUES($1, $2::jsonb, NOW())
     ON CONFLICT(key) DO UPDATE
       SET value = EXCLUDED.value,
           updated_at = NOW()`,
    [key, JSON.stringify(value || {})]
  );
}

async function getAppStateJson(key) {
  const { rows } = await pool.query(`SELECT value FROM app_state WHERE key = $1`, [key]);
  return rows[0]?.value || null;
}

async function saveRouteAutogenConfig(corridorId, payload = {}) {
  await upsertAppStateJson(routeAutogenKey(corridorId), {
    corridor_id: Number(corridorId),
    ...payload,
    updated_at: new Date().toISOString()
  });
}

async function getRoutePair(corridorId) {
  return getAppStateJson(routePairKey(corridorId));
}

async function setRoutePair(aCorridorId, bCorridorId) {
  const now = new Date().toISOString();
  await Promise.all([
    upsertAppStateJson(routePairKey(aCorridorId), {
      corridor_id: Number(aCorridorId),
      peer_corridor_id: Number(bCorridorId),
      updated_at: now
    }),
    upsertAppStateJson(routePairKey(bCorridorId), {
      corridor_id: Number(bCorridorId),
      peer_corridor_id: Number(aCorridorId),
      updated_at: now
    })
  ]);
}

async function upsertCorridorByName(name, bbox, feedProfile = "il_arcgis", feedConfig = {}) {
  const n = String(name || "").trim();
  if (!n) throw new Error("corridor name is required");
  const profile = normalizeFeedProfileId(feedProfile);
  const cfg = normalizeFeedConfig(profile, feedConfig);
  const out = await pool.query(
    `INSERT INTO corridors(name, bbox_xmin, bbox_ymin, bbox_xmax, bbox_ymax, feed_profile, feed_config, active, updated_at)
     VALUES($1,$2,$3,$4,$5,$6,$7::jsonb,true,NOW())
     ON CONFLICT(name) DO UPDATE
       SET bbox_xmin = EXCLUDED.bbox_xmin,
           bbox_ymin = EXCLUDED.bbox_ymin,
           bbox_xmax = EXCLUDED.bbox_xmax,
           bbox_ymax = EXCLUDED.bbox_ymax,
           feed_profile = EXCLUDED.feed_profile,
           feed_config = EXCLUDED.feed_config,
           active = true,
           updated_at = NOW()
     RETURNING id, name, bbox_xmin AS xmin, bbox_ymin AS ymin, bbox_xmax AS xmax, bbox_ymax AS ymax,
               feed_profile, feed_config`,
    [n, Number(bbox.xmin), Number(bbox.ymin), Number(bbox.xmax), Number(bbox.ymax), profile, JSON.stringify(cfg)]
  );
  return out.rows[0];
}

async function upsertAutogenCheckpoints(corridorId, sampledPoints, radiusKm, sourceLabel) {
  const inserted = [];
  for (let i = 0; i < sampledPoints.length; i++) {
    const p = sampledPoints[i];
    const cpName =
      i === 0 ? "Auto_Start" : i === sampledPoints.length - 1 ? "Auto_End" : `Auto_${String(i).padStart(2, "0")}`;
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
      [corridorId, cpName, Number(p.lat), Number(p.lon), Number(radiusKm), String(sourceLabel || "autogen")]
    );
    inserted.push(q.rows[0]);
  }
  return inserted;
}

function checkpointSortRank(name = "") {
  const n = String(name || "").trim();
  if (n === "Auto_Start") return 0;
  if (n === "Auto_End") return 1_000_000;
  const m = n.match(/^Auto_(\d+)$/);
  if (m) return Number(m[1]);
  return 100_000;
}

function orderedRoutePointsFromCheckpoints(rows = []) {
  const pts = (rows || [])
    .filter((r) => Number.isFinite(Number(r.lat)) && Number.isFinite(Number(r.lon)))
    .map((r) => ({
      id: Number(r.id || 0),
      name: String(r.name || ""),
      lat: Number(r.lat),
      lon: Number(r.lon)
    }));
  if (pts.length < 2) return [];
  const hasAuto = pts.some((p) => p.name.startsWith("Auto_"));
  if (hasAuto) {
    pts.sort((a, b) => {
      const ra = checkpointSortRank(a.name);
      const rb = checkpointSortRank(b.name);
      if (ra !== rb) return ra - rb;
      return a.id - b.id;
    });
  } else {
    pts.sort((a, b) => a.id - b.id);
  }
  return pts;
}

function haversineKm(aLat, aLon, bLat, bLon) {
  const toRad = (d) => (d * Math.PI) / 180;
  const dLat = toRad(bLat - aLat);
  const dLon = toRad(bLon - aLon);
  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(toRad(aLat)) * Math.cos(toRad(bLat)) * Math.sin(dLon / 2) ** 2;
  return 6371 * (2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a)));
}

function trafficFeatureCoords4326(feature) {
  const g = feature?.geometry || {};
  if (Array.isArray(g.paths) && g.paths.length && Array.isArray(g.paths[0])) {
    const firstPath = g.paths[0]
      .map((p) => [Number(p[0]), Number(p[1])])
      .filter((p) => Number.isFinite(p[0]) && Number.isFinite(p[1]));
    if (firstPath.length >= 2) return firstPath;
  }
  const x = Number(g.x);
  const y = Number(g.y);
  if (Number.isFinite(x) && Number.isFinite(y)) return [[x, y], [x, y]];
  return [];
}

function lineMidpoint(coords) {
  if (!Array.isArray(coords) || !coords.length) return null;
  const i = Math.floor((coords.length - 1) / 2);
  const c = coords[i];
  if (!Array.isArray(c) || c.length < 2) return null;
  return { lon: Number(c[0]), lat: Number(c[1]) };
}

function nearestRouteDistanceKm(point, routePoints) {
  if (!point || !Array.isArray(routePoints) || routePoints.length < 2) return Number.POSITIVE_INFINITY;
  let best = Number.POSITIVE_INFINITY;
  for (const rp of routePoints) {
    const d = haversineKm(Number(point.lat), Number(point.lon), Number(rp.lat), Number(rp.lon));
    if (d < best) best = d;
  }
  return best;
}

async function buildRouteTrafficData(corridorId, opts = {}) {
  const corridor = await getCorridorById(corridorId);
  if (!corridor) throw new Error("corridor not found");
  const bufferKm = Math.max(2, Math.min(30, Number(opts.buffer_km || 8)));
  const maxSegments = Math.max(100, Math.min(2000, Number(opts.max_segments || 900)));
  const checkpointsQ = await pool.query(
    `SELECT id, name, lat, lon, radius_km, source
       FROM checkpoints
      WHERE corridor_id = $1
        AND active = true`,
    [corridorId]
  );
  const routePoints = orderedRoutePointsFromCheckpoints(checkpointsQ.rows || []);
  if (routePoints.length < 2) {
    return {
      corridor_id: Number(corridor.id),
      corridor_name: corridor.name,
      route_available: false,
      reason: "Need at least 2 active checkpoints (use Auto-Generate Route Checkpoints).",
      stats: {
        segment_count: 0,
        avg_delay_pct: 0,
        p90_delay_pct: 0,
        heavy_delay_count: 0,
        score: 0
      },
      route_line_geojson: null,
      segments_geojson: { type: "FeatureCollection", features: [] },
      checkpoints: checkpointsQ.rows || []
    };
  }

  const feedSnapshot = await fetchNormalizedFeeds(corridor, {
    needTraffic: true,
    needIncidents: false,
    needClosures: false,
    needCameras: false,
    timeoutMs: 12000
  });
  const features = Array.isArray(feedSnapshot?.trafficFeatures) ? feedSnapshot.trafficFeatures : [];
  const kept = [];
  const delays = [];

  for (const f of features) {
    const attrs = f?.attributes || {};
    const speed = Number(attrs.SPEED);
    const speedFf = Number(attrs.SPEED_FF);
    if (!Number.isFinite(speedFf) || speedFf <= 0 || !Number.isFinite(speed) || speed < 0) continue;
    const coords = trafficFeatureCoords4326(f);
    if (coords.length < 2) continue;
    const mid = lineMidpoint(coords);
    if (!mid) continue;
    const distKm = nearestRouteDistanceKm(mid, routePoints);
    if (!Number.isFinite(distKm) || distKm > bufferKm) continue;
    const delayPct = clampNum(((speedFf - speed) / speedFf) * 100, 0, 100);
    delays.push(delayPct);
    kept.push({
      type: "Feature",
      geometry: {
        type: "LineString",
        coordinates: coords
      },
      properties: {
        object_id: Number(attrs.OBJECTID || 0),
        speed_mph: speed,
        speed_ff_mph: speedFf,
        jam_factor: Number(attrs.JAM_FACTOR || 0),
        delay_pct: delayPct,
        route_distance_km: distKm
      }
    });
  }

  kept.sort((a, b) => Number(b.properties.delay_pct || 0) - Number(a.properties.delay_pct || 0));
  const sliced = kept.slice(0, maxSegments);
  const usedDelays = sliced.map((f) => Number(f.properties.delay_pct || 0));
  const avgDelay = usedDelays.length ? usedDelays.reduce((s, v) => s + v, 0) / usedDelays.length : 0;
  const p90 = quantile(usedDelays, 0.9, 0);
  const heavy = usedDelays.filter((d) => d >= 40).length;
  const score = clampNum((avgDelay * 0.6 + p90 * 0.4) * (1 + heavy / Math.max(1, usedDelays.length * 2)), 0, 100);

  return {
    corridor_id: Number(corridor.id),
    corridor_name: corridor.name,
    route_available: true,
    buffer_km: bufferKm,
    route_line_geojson: {
      type: "Feature",
      geometry: {
        type: "LineString",
        coordinates: routePoints.map((p) => [Number(p.lon), Number(p.lat)])
      },
      properties: {
        corridor_id: Number(corridor.id),
        corridor_name: corridor.name
      }
    },
    segments_geojson: {
      type: "FeatureCollection",
      features: sliced
    },
    checkpoints: routePoints,
    stats: {
      segment_count: sliced.length,
      avg_delay_pct: avgDelay,
      p90_delay_pct: p90,
      heavy_delay_count: heavy,
      score
    }
  };
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
  const feedProfiles = listFeedProfiles();
  res.json({
    feed_profiles: feedProfiles,
    corridor_input_modes: ["bbox", "city_query + radius_km", "center(lat/lon) + radius_km"],
    checkpoint_input_modes: ["gps(lat/lon)+radius_km", "city_query+radius_km"],
    prediction_outputs: ["predicted_next_score_p50", "predicted_next_score_p90", "prediction_confidence", "drift_score"],
    chat_modes: ["heuristic_db_parser", "openai_if_configured"],
    weather_sources: ["api.weather.gov points/forecast/alerts/observations", "opengeo.ncep.noaa.gov MRMS radar WMS"],
    incidents_definition:
      "incidents_count = route-scoped, direction-aware incident relevance count from active feed profile",
    incidents_feed_url: feeds.incidents,
    closures_feed_url: feeds.closures,
    incident_relevance_modes: {
      0: "legacy_bbox (no route/direction filtering)",
      1: "relaxed_near_route (buffer + soft direction mismatch penalty)",
      2: "strict_directional (buffer + hard direction matching when available)"
    },
    weighting_modes: ["dynamic_regime_reliability_weighting", "auto_learn_multiplier_ridge", "manual_lock_overrides"],
    weighting_endpoint: "/api/weights/current",
    radius_autotune_endpoints: {
      start: "/api/corridors/:id/autotune-radius/start",
      status: "/api/corridors/:id/autotune-radius/status"
    },
    route_traffic_map_endpoint: "/api/route/traffic-map?corridor_id={id}&include_reverse={true|false}",
    methodology_presets_endpoint: "/api/settings/presets",
    image_signal_modes: ["proxy_only", "queue_stop", "hybrid_auto"],
    cv_modes: ["disabled", "openai_vision", "http_json_endpoint"],
    ai_outputs: ["markdown_explanation", "mermaid_diagram_on_request", "context_coverage_disclosure"]
  });
});

app.get("/api/feed-profiles", async (_, res) => {
  res.json({
    profiles: listFeedProfiles()
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

app.get("/api/settings/presets", async (_, res) => {
  const presets = listMethodologyPresets();
  const active = await getActiveMethodologyPreset();
  res.json({ presets, active });
});

app.put("/api/settings/presets/activate", async (req, res) => {
  const presetId = String(req.body?.preset_id || "").trim();
  if (!presetId) return res.status(400).json({ error: "preset_id is required" });
  const preset = getMethodologyPresetById(presetId);
  if (!preset) return res.status(404).json({ error: "preset not found" });
  const settings = await upsertRuntimeSettings(preset.overrides || {});
  const active = await setActiveMethodologyPreset({
    id: preset.id,
    name: preset.name,
    description: preset.description,
    override_count: Object.keys(preset.overrides || {}).length
  });
  res.json({
    ok: true,
    preset: {
      id: preset.id,
      name: preset.name,
      description: preset.description
    },
    active,
    settings
  });
});

app.get("/api/corridors", async (req, res) => {
  const includeArchived = String(req.query.include_archived || "true") !== "false";
  const { rows } = await pool.query(
    `SELECT c.id, c.name,
            bbox_xmin AS xmin, bbox_ymin AS ymin, bbox_xmax AS xmax, bbox_ymax AS ymax,
            c.feed_profile, c.feed_config,
            c.active, c.created_at, c.updated_at,
            r.last_run_ts,
            COALESCE(r.run_count, 0) AS run_count
       FROM corridors c
  LEFT JOIN (
           SELECT corridor_id, MAX(run_ts) AS last_run_ts, COUNT(*)::bigint AS run_count
             FROM monitor_runs
            GROUP BY corridor_id
         ) r ON r.corridor_id = c.id
      WHERE ($1::boolean = true OR c.active = true)
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
  const feedProfileInput = typeof req.body?.feed_profile === "string" ? req.body.feed_profile : null;
  const feedConfigInput = req.body?.feed_config && typeof req.body.feed_config === "object" ? req.body.feed_config : null;
  const caDistrictsCsv = String(req.body?.ca_districts_csv || "").trim();
  if (typeof active === "undefined" && !name && !feedProfileInput && !feedConfigInput && !caDistrictsCsv) {
    return res.status(400).json({ error: "provide active, name, feed_profile, or feed_config" });
  }
  const current = await pool.query("SELECT id, name, active, feed_profile, feed_config FROM corridors WHERE id = $1", [id]);
  if (!current.rows.length) return res.status(404).json({ error: "corridor not found" });
  const finalName = name || current.rows[0].name;
  const finalActive = typeof active === "boolean" ? active : current.rows[0].active;
  const finalFeedProfile = normalizeFeedProfileId(feedProfileInput || current.rows[0].feed_profile || "il_arcgis");
  const finalFeedConfig = normalizeFeedConfig(
    finalFeedProfile,
    feedConfigInput || current.rows[0].feed_config || {},
    caDistrictsCsv
  );
  const out = await pool.query(
    `UPDATE corridors
        SET name = $2,
            active = $3,
            feed_profile = $4,
            feed_config = $5::jsonb,
            updated_at = NOW()
      WHERE id = $1
      RETURNING id, name, active, feed_profile, feed_config`,
    [id, finalName, finalActive, finalFeedProfile, JSON.stringify(finalFeedConfig)]
  );
  res.json(out.rows[0]);
});

app.post("/api/corridors", async (req, res) => {
  const { name, bbox, city, radius_km, center } = req.body || {};
  const feedProfile = normalizeFeedProfileId(req.body?.feed_profile || "il_arcgis");
  const feedConfig = normalizeFeedConfig(feedProfile, req.body?.feed_config || {}, req.body?.ca_districts_csv || "");
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
    `INSERT INTO corridors(name, bbox_xmin, bbox_ymin, bbox_xmax, bbox_ymax, feed_profile, feed_config, active, updated_at)
     VALUES($1,$2,$3,$4,$5,$6,$7::jsonb,true,NOW())
     ON CONFLICT(name) DO UPDATE
       SET bbox_xmin = EXCLUDED.bbox_xmin,
           bbox_ymin = EXCLUDED.bbox_ymin,
           bbox_xmax = EXCLUDED.bbox_xmax,
           bbox_ymax = EXCLUDED.bbox_ymax,
           feed_profile = EXCLUDED.feed_profile,
           feed_config = EXCLUDED.feed_config,
           active = true,
           updated_at = NOW()
     RETURNING id, name,
               bbox_xmin AS xmin, bbox_ymin AS ymin, bbox_xmax AS xmax, bbox_ymax AS ymax,
               feed_profile, feed_config,
               active, created_at, updated_at`,
    [name.trim(), resolved.xmin, resolved.ymin, resolved.xmax, resolved.ymax, feedProfile, JSON.stringify(feedConfig)]
  );
  res.json({ corridor: q.rows[0], source, geocode_label: geocodeLabel });
});

app.post("/api/corridors/:id/autotune-radius/start", async (req, res) => {
  const corridorId = Number(req.params.id);
  if (!corridorId) return res.status(400).json({ error: "valid corridor id is required" });
  const corridor = await getCorridorById(corridorId);
  if (!corridor) return res.status(404).json({ error: "corridor not found" });

  const key = radiusTuneJobKey(corridorId);
  const existing = radiusTuneJobs.get(key);
  if (existing && existing.status === "running") {
    return res.json({
      ok: true,
      already_running: true,
      job: existing
    });
  }

  const durationSeconds = Math.max(30, Math.min(900, Number(req.body?.duration_seconds || 90)));
  const minRadiusKm = Math.max(5, Math.min(300, Number(req.body?.min_radius_km || 20)));
  const maxRadiusKm = Math.max(minRadiusKm + 2, Math.min(350, Number(req.body?.max_radius_km || 110)));
  const stepKm = Math.max(1, Math.min(30, Number(req.body?.step_km || 5)));

  const job = {
    corridor_id: corridorId,
    corridor_name: corridor.name,
    status: "queued",
    created_at: new Date().toISOString(),
    duration_seconds: durationSeconds,
    min_radius_km: minRadiusKm,
    max_radius_km: maxRadiusKm,
    step_km: stepKm,
    progress_pct: 0,
    recommended_duration_seconds: 90
  };
  radiusTuneJobs.set(key, job);
  setTimeout(() => {
    runRadiusTuneJob(job).catch(() => {});
  }, 0);
  res.json({
    ok: true,
    already_running: false,
    job
  });
});

app.get("/api/corridors/:id/autotune-radius/status", async (req, res) => {
  const corridorId = Number(req.params.id);
  if (!corridorId) return res.status(400).json({ error: "valid corridor id is required" });
  const corridor = await getCorridorById(corridorId);
  if (!corridor) return res.status(404).json({ error: "corridor not found" });
  const key = radiusTuneJobKey(corridorId);
  const job = radiusTuneJobs.get(key);
  if (!job) {
    return res.json({
      corridor_id: corridorId,
      corridor_name: corridor.name,
      status: "idle",
      progress_pct: 0,
      recommended_duration_seconds: 90
    });
  }
  const summary = summarizeRadiusTuneJob(job);
  res.json({
    ...job,
    ranking: (job.ranking || summary.ranking || []).slice(0, 12),
    best: job.best || summary.best || null
  });
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
  const createReverse = req.body?.create_reverse === true || String(req.body?.create_reverse || "") === "1";
  const reverseNameRaw = String(req.body?.reverse_corridor_name || "").trim();

  if (!corridorId) return res.status(400).json({ error: "corridor_id is required" });
  if (!fromQuery || !toQuery) return res.status(400).json({ error: "from_query and to_query are required" });
  const baseCorridor = await getCorridorById(corridorId);
  if (!baseCorridor) return res.status(404).json({ error: "corridor not found" });

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

  const inserted = await upsertAutogenCheckpoints(corridorId, sampled, radiusKm, `autogen_${routeSource}`);
  await saveRouteAutogenConfig(corridorId, {
    from_query: fromQuery,
    to_query: toQuery,
    from_label: fromHit.label,
    to_label: toHit.label,
    spacing_km: spacingKm,
    checkpoint_radius_km: radiusKm,
    max_points: maxPoints,
    use_roads: useRoads,
    route_source: routeSource
  });

  let reverse = null;
  if (createReverse) {
    let reverseName = reverseNameRaw || `${baseCorridor.name} (reverse)`;
    if (reverseName === baseCorridor.name) reverseName = `${baseCorridor.name} (reverse)`;
    const reverseCorridor = await upsertCorridorByName(reverseName, {
      xmin: Number(baseCorridor.xmin),
      ymin: Number(baseCorridor.ymin),
      xmax: Number(baseCorridor.xmax),
      ymax: Number(baseCorridor.ymax)
    }, baseCorridor.feed_profile, baseCorridor.feed_config || {});
    const reversePoints = [...sampled].reverse();
    const reverseCheckpoints = await upsertAutogenCheckpoints(
      Number(reverseCorridor.id),
      reversePoints,
      radiusKm,
      `autogen_${routeSource}_reverse`
    );
    await saveRouteAutogenConfig(Number(reverseCorridor.id), {
      from_query: toQuery,
      to_query: fromQuery,
      from_label: toHit.label,
      to_label: fromHit.label,
      spacing_km: spacingKm,
      checkpoint_radius_km: radiusKm,
      max_points: maxPoints,
      use_roads: useRoads,
      route_source: routeSource
    });
    await setRoutePair(corridorId, Number(reverseCorridor.id));
    reverse = {
      corridor_id: Number(reverseCorridor.id),
      corridor_name: reverseCorridor.name,
      generated_count: reverseCheckpoints.length
    };
  }

  return res.json({
    ok: true,
    route_source: routeSource,
    from: { query: fromQuery, label: fromHit.label, lat: fromHit.lat, lon: fromHit.lon },
    to: { query: toQuery, label: toHit.label, lat: toHit.lat, lon: toHit.lon },
    generated_count: inserted.length,
    checkpoints: inserted,
    reverse
  });
});

app.get("/api/route/traffic-map", async (req, res) => {
  const corridorId = Number(req.query.corridor_id);
  if (!corridorId) return res.status(400).json({ error: "corridor_id is required" });
  const includeReverse = String(req.query.include_reverse || "false") === "true";
  const bufferKm = Math.max(2, Math.min(30, Number(req.query.route_buffer_km || 8)));

  const forward = await buildRouteTrafficData(corridorId, {
    buffer_km: bufferKm,
    max_segments: 900
  });

  let reverse = null;
  let pair = null;
  if (includeReverse) {
    pair = await getRoutePair(corridorId);
    const peerId = Number(pair?.peer_corridor_id || 0);
    if (peerId) {
      try {
        reverse = await buildRouteTrafficData(peerId, {
          buffer_km: bufferKm,
          max_segments: 900
        });
      } catch {
        reverse = null;
      }
    }
  }

  res.json({
    corridor_id: corridorId,
    include_reverse: includeReverse,
    route_buffer_km: bufferKm,
    forward,
    reverse,
    pair,
    fetched_at: new Date().toISOString()
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
    `SELECT run_ts, fused_score, image_score, image_proxy_score, image_queue_stop_score, image_hybrid_score,
            image_signal_mode, image_signal_confidence, cv_analyzed_count, cv_coverage_pct, cv_avg_confidence,
            delay_pct, incidents_count, closures_count,
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

app.get("/api/analysis/active-strategy", async (req, res) => {
  const corridorId = Number(req.query.corridor_id);
  if (!corridorId) return res.status(400).json({ error: "corridor_id is required" });
  const strategy = await getActiveStrategy(corridorId);
  res.json({
    corridor_id: corridorId,
    active_strategy: strategy
  });
});

app.put("/api/analysis/active-strategy", async (req, res) => {
  const corridorId = Number(req.body?.corridor_id);
  const strategy = req.body?.strategy;
  if (!corridorId) return res.status(400).json({ error: "corridor_id is required" });
  if (!strategy || typeof strategy !== "object") return res.status(400).json({ error: "strategy object is required" });
  const corridor = await getCorridorById(corridorId);
  if (!corridor) return res.status(404).json({ error: "corridor not found" });
  const saved = await upsertActiveStrategy(corridorId, {
    ...strategy,
    source: req.body?.source || "ui_apply"
  });
  res.json({
    ok: true,
    corridor_id: corridorId,
    active_strategy: saved
  });
});

app.get("/api/analysis/strategy-presets", async (req, res) => {
  const corridorId = Number(req.query.corridor_id);
  const hours = Math.min(720, Math.max(6, Number(req.query.hours || 72)));
  if (!corridorId) return res.status(400).json({ error: "corridor_id is required" });

  const [latestQ, historyQ, trainQ, runtimeSettings, activeStrategy] = await Promise.all([
    pool.query(
      `SELECT run_ts, fused_score, traffic_segments, delay_pct, image_score, incidents_count, closures_count,
              weather_risk_score, camera_fresh_pct, camera_total
         FROM monitor_runs
        WHERE corridor_id = $1
        ORDER BY run_ts DESC
        LIMIT 1`,
      [corridorId]
    ),
    pool.query(
      `SELECT run_ts, fused_score, traffic_segments, delay_pct, image_score, incidents_count, closures_count,
              weather_risk_score, camera_fresh_pct
         FROM monitor_runs
        WHERE corridor_id = $1
          AND run_ts >= NOW() - ($2::text || ' hours')::interval
        ORDER BY run_ts ASC`,
      [corridorId, hours]
    ),
    pool.query(
      `SELECT run_ts, fused_score, image_score, incidents_count, closures_count, camera_fresh_pct
         FROM monitor_runs
        WHERE corridor_id = $1
        ORDER BY run_ts DESC
        LIMIT 600`,
      [corridorId]
    ),
    getRuntimeSettings(),
    getActiveStrategy(corridorId)
  ]);

  if (!latestQ.rows.length) {
    return res.json({
      corridor_id: corridorId,
      window_hours: hours,
      sample_points: 0,
      active_strategy: activeStrategy,
      strategies: []
    });
  }

  const latest = latestQ.rows[0];
  const history = historyQ.rows.length ? historyQ.rows : [latest];
  const pick = (k) => history.map((r) => Number(r[k] || 0)).filter((v) => Number.isFinite(v));
  const stats = {
    traffic: quantilePack(pick("traffic_segments"), Number(latest.traffic_segments || 0)),
    delay: quantilePack(pick("delay_pct"), Number(latest.delay_pct || 0)),
    image: quantilePack(pick("image_score"), Number(latest.image_score || 0)),
    incidents: quantilePack(pick("incidents_count"), Number(latest.incidents_count || 0)),
    closures: quantilePack(pick("closures_count"), Number(latest.closures_count || 0)),
    weather: quantilePack(pick("weather_risk_score"), Number(latest.weather_risk_score || 0)),
    freshness: quantilePack(pick("camera_fresh_pct"), Number(latest.camera_fresh_pct || 100))
  };

  const baseInputs = normalizeScenarioInputs({
    traffic_segments: latest.traffic_segments,
    delay_pct: latest.delay_pct,
    image_score: latest.image_score,
    incidents_count: latest.incidents_count,
    closures_count: latest.closures_count,
    weather_risk_score: latest.weather_risk_score,
    camera_fresh_pct: latest.camera_fresh_pct
  });

  const autoLearn = await getAutoLearnMultipliers(corridorId, runtimeSettings);
  const model = trainNextScoreModel(trainQ.rows.reverse(), runtimeSettings);
  const cameraCountForReliability = Math.max(1, Number(latest.camera_total || 1));

  function evaluateScenario(inputs, note = "") {
    const norm = normalizeScenarioInputs(inputs, baseInputs);
    const fused = computeFused(
      {
        trafficSegments: norm.traffic_segments,
        delayPct: norm.delay_pct,
        imageScore: norm.image_score,
        incidents: norm.incidents_count,
        closures: norm.closures_count,
        weatherRiskScore: norm.weather_risk_score
      },
      runtimeSettings,
      {
        referenceTs: new Date(),
        cameraFreshPct: norm.camera_fresh_pct,
        cameraCountUsed: cameraCountForReliability,
        trafficFeedOk: true,
        incidentFeedOk: true,
        closureFeedOk: true,
        weatherAvailable: true,
        learnedMultipliers: autoLearn.multipliers
      }
    );
    const prediction = predictNextScore(
      model,
      {
        fused_score: fused.fused,
        image_score: norm.image_score,
        incidents_count: norm.incidents_count,
        closures_count: norm.closures_count,
        camera_fresh_pct: norm.camera_fresh_pct
      },
      runtimeSettings
    );
    const riskBand =
      fused.fused >= 70 ? "high" : fused.fused >= 50 ? "elevated" : fused.fused >= 35 ? "watch" : "normal";
    return {
      inputs: norm,
      note,
      projection: {
        fused_score: fused.fused,
        p50: prediction.p50,
        p90: prediction.p90,
        confidence: prediction.confidence,
        drift_score: prediction.driftScore,
        risk_band: riskBand,
        components: {
          traffic: fused.trafficComponent,
          image: fused.imageComponent,
          incidents: fused.incidentComponent,
          closures: fused.closureComponent,
          weather: fused.weatherComponent
        }
      }
    };
  }

  const templates = [
    {
      id: "baseline_now",
      name: "Baseline Now",
      goal: "Use latest observed conditions as reference",
      build: () => ({ ...baseInputs }),
      note: "Anchors all other scenarios."
    },
    {
      id: "best_window_candidate",
      name: "Best Window Candidate",
      goal: "Lower-risk departure mix from recent history",
      build: () => ({
        ...baseInputs,
        traffic_segments: Math.max(1, stats.traffic.p35),
        delay_pct: stats.delay.p20,
        image_score: stats.image.p20,
        incidents_count: Math.round(stats.incidents.p20),
        closures_count: Math.round(stats.closures.p20),
        weather_risk_score: stats.weather.p20,
        camera_fresh_pct: Math.max(baseInputs.camera_fresh_pct, stats.freshness.p80)
      }),
      note: "Lower quantile blend for smoother travel."
    },
    {
      id: "typical_commute",
      name: "Typical Commute",
      goal: "Median expected run state",
      build: () => ({
        ...baseInputs,
        traffic_segments: Math.max(1, stats.traffic.p50),
        delay_pct: stats.delay.p50,
        image_score: stats.image.p50,
        incidents_count: Math.round(stats.incidents.p50),
        closures_count: Math.round(stats.closures.p50),
        weather_risk_score: stats.weather.p50,
        camera_fresh_pct: stats.freshness.p50
      }),
      note: "Balanced baseline for normal planning."
    },
    {
      id: "rush_hour_push",
      name: "Rush-Hour Push",
      goal: "Upper congestion stress test",
      build: () => ({
        ...baseInputs,
        traffic_segments: Math.max(1, Math.max(baseInputs.traffic_segments, stats.traffic.p80)),
        delay_pct: Math.max(baseInputs.delay_pct, stats.delay.p80),
        image_score: Math.max(baseInputs.image_score, stats.image.p70),
        incidents_count: Math.max(1, Math.round(Math.max(baseInputs.incidents_count, stats.incidents.p70))),
        closures_count: Math.round(Math.max(baseInputs.closures_count, stats.closures.p60)),
        weather_risk_score: Math.max(baseInputs.weather_risk_score, stats.weather.p50)
      }),
      note: "Commute-heavy pressure case."
    },
    {
      id: "incident_spike",
      name: "Incident Spike",
      goal: "Incident/closure escalation what-if",
      build: () => ({
        ...baseInputs,
        traffic_segments: Math.max(1, Math.max(baseInputs.traffic_segments, stats.traffic.p70)),
        delay_pct: Math.max(baseInputs.delay_pct, stats.delay.p70),
        image_score: Math.max(baseInputs.image_score, stats.image.p60),
        incidents_count: Math.max(2, Math.round(stats.incidents.p90 + 1)),
        closures_count: Math.max(0, Math.round(stats.closures.p80 + 1)),
        weather_risk_score: Math.max(baseInputs.weather_risk_score, stats.weather.p60)
      }),
      note: "Use when incidents start stacking in feed."
    },
    {
      id: "weather_degradation",
      name: "Weather Degradation",
      goal: "Fog/rain/storm risk stress test",
      build: () => ({
        ...baseInputs,
        traffic_segments: Math.max(1, stats.traffic.p60),
        delay_pct: Math.max(baseInputs.delay_pct, stats.delay.p70),
        image_score: Math.max(baseInputs.image_score, stats.image.p70),
        incidents_count: Math.max(baseInputs.incidents_count, Math.round(stats.incidents.p60)),
        closures_count: Math.max(baseInputs.closures_count, Math.round(stats.closures.p60)),
        weather_risk_score: Math.max(65, Math.max(baseInputs.weather_risk_score, stats.weather.p90))
      }),
      note: "Strong weather dominates weighting regime."
    },
    {
      id: "camera_quality_drop",
      name: "Camera Quality Drop",
      goal: "Data quality resilience check",
      build: () => ({
        ...baseInputs,
        delay_pct: Math.max(baseInputs.delay_pct, stats.delay.p60),
        image_score: Math.max(baseInputs.image_score, stats.image.p60),
        incidents_count: Math.round(stats.incidents.p50),
        closures_count: Math.round(stats.closures.p50),
        weather_risk_score: Math.max(baseInputs.weather_risk_score, stats.weather.p50),
        camera_fresh_pct: Math.min(55, stats.freshness.p20)
      }),
      note: "Tests stability when camera freshness degrades."
    }
  ];

  const evaluated = templates.map((t) => {
    const out = evaluateScenario(t.build(), t.note);
    return {
      id: t.id,
      name: t.name,
      goal: t.goal,
      note: out.note,
      inputs: out.inputs,
      projection: out.projection
    };
  });

  const baseline = evaluated.find((s) => s.id === "baseline_now") || evaluated[0];
  const baselineFused = Number(baseline?.projection?.fused_score || 0);
  const strategies = evaluated.map((s) => ({
    ...s,
    delta_vs_baseline: Number(s.projection.fused_score || 0) - baselineFused
  }));

  const ranked = [...strategies].sort((a, b) => {
    const aRisk = Number(a.projection.p90 || 0) * 0.7 + Number(a.projection.fused_score || 0) * 0.3;
    const bRisk = Number(b.projection.p90 || 0) * 0.7 + Number(b.projection.fused_score || 0) * 0.3;
    return aRisk - bRisk;
  });

  res.json({
    corridor_id: corridorId,
    window_hours: hours,
    sample_points: history.length,
    latest_run_ts: latest.run_ts,
    model_version: model?.version || "fallback_v1",
    auto_learn_fit: autoLearn.fit,
    baseline_strategy_id: baseline?.id || null,
    active_strategy: activeStrategy,
    recommendation: {
      lowest_risk: ranked[0] ? { id: ranked[0].id, name: ranked[0].name } : null,
      highest_risk: ranked.length ? { id: ranked[ranked.length - 1].id, name: ranked[ranked.length - 1].name } : null
    },
    strategies
  });
});

app.post("/api/analysis/simulate", async (req, res) => {
  const corridorId = Number(req.body?.corridor_id);
  if (!corridorId) return res.status(400).json({ error: "corridor_id is required" });

  const latestQ = await pool.query(
    `SELECT fused_score, traffic_segments, image_score, image_signal_confidence, camera_total,
            delay_pct, incidents_count, closures_count, camera_fresh_pct, weather_risk_score
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

  const autoLearn = await getAutoLearnMultipliers(corridorId, runtimeSettings);
  const fused = computeFused(
    {
      trafficSegments: scenario.traffic_segments,
      delayPct: scenario.delay_pct,
      imageScore: scenario.image_score,
      incidents: scenario.incidents_count,
      closures: scenario.closures_count,
      weatherRiskScore: scenario.weather_risk_score
    },
    runtimeSettings,
    {
      referenceTs: new Date(),
      cameraFreshPct: scenario.camera_fresh_pct * (Number(latest.image_signal_confidence || 100) / 100),
      cameraCountUsed: Number(latest.camera_total || 0),
      trafficFeedOk: true,
      incidentFeedOk: true,
      closureFeedOk: true,
      weatherAvailable: true,
      learnedMultipliers: autoLearn.multipliers
    }
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
    model_version: model?.version || "fallback_v1",
    auto_learn: autoLearn.fit
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
                o.proxy_score,
                o.queue_stop_score,
                o.hybrid_score,
                o.hybrid_confidence,
                o.local_delay_pct,
                o.local_slow_pct,
                o.local_stop_pct,
                o.local_segment_count,
                o.cv_provider,
                o.cv_vehicle_count,
                o.cv_stopped_count,
                o.cv_queue_score,
                o.cv_lane_occupancy_pct,
                o.cv_visibility_quality,
                o.cv_confidence,
                o.cv_blend_weight,
                o.cv_notes,
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

app.get("/api/camera/preview", async (req, res) => {
  const raw = String(req.query.url || "").trim();
  if (!raw) return res.status(400).json({ error: "url is required" });
  let u;
  try {
    u = new URL(raw);
  } catch {
    return res.status(400).json({ error: "invalid url" });
  }
  if (!(u.protocol === "http:" || u.protocol === "https:")) {
    return res.status(400).json({ error: "only http/https allowed" });
  }
  if (isPrivateHost(u.hostname)) {
    return res.status(400).json({ error: "private hosts are not allowed" });
  }

  try {
    const upstream = await fetch(u.toString(), {
      signal: AbortSignal.timeout(12000),
      headers: {
        Accept: "image/*,*/*",
        "User-Agent": "il-corridor-monitor/1.0 camera-preview-proxy"
      }
    });
    if (!upstream.ok) {
      return res.status(502).json({ error: `upstream ${upstream.status}` });
    }
    const contentType = String(upstream.headers.get("content-type") || "image/jpeg").toLowerCase();
    if (!contentType.startsWith("image/")) {
      return res.status(415).json({ error: "upstream is not image content" });
    }
    const bytes = Buffer.from(await upstream.arrayBuffer());
    res.setHeader("Content-Type", contentType);
    res.setHeader("Cache-Control", "no-store");
    res.status(200).send(bytes);
  } catch (e) {
    res.status(502).json({ error: `preview fetch failed: ${String(e.message || e)}` });
  }
});

app.get("/api/weights/current", async (req, res) => {
  const corridorId = Number(req.query.corridor_id);
  if (!corridorId) return res.status(400).json({ error: "corridor_id is required" });
  const runtimeSettings = await getRuntimeSettings();
  const latestQ = await pool.query(
    `SELECT run_ts, fused_score, traffic_segments, delay_pct, incidents_count, closures_count, camera_total, camera_fresh_pct,
            image_score, image_proxy_score, image_queue_stop_score, image_hybrid_score, image_signal_mode, image_signal_confidence,
            cv_analyzed_count, cv_coverage_pct, cv_avg_confidence, cv_provider, cv_error_count,
            weather_risk_score, raw_json
       FROM monitor_runs
      WHERE corridor_id = $1
      ORDER BY run_ts DESC
      LIMIT 1`,
    [corridorId]
  );
  if (!latestQ.rows.length) return res.json(null);
  const latest = latestQ.rows[0];
  let rawJson = latest.raw_json || {};
  if (typeof rawJson === "string") {
    try {
      rawJson = JSON.parse(rawJson || "{}");
    } catch {
      rawJson = {};
    }
  }
  const fromRun = rawJson?.weighting || null;
  const imageSignalsFromRun = rawJson?.imageSignals || null;
  const cameraSelectionFromRun = rawJson?.cameraSelection || null;

  let weighting = fromRun;
  let autoLearnFit = fromRun?.auto_learn_fit || { rows: 0, r2: 0, rmse: 0 };
  if (!weighting) {
    const autoLearn = await getAutoLearnMultipliers(corridorId, runtimeSettings);
    autoLearnFit = autoLearn.fit;
    const recomputed = computeFused(
      {
        trafficSegments: Number(latest.traffic_segments || 0),
        delayPct: Number(latest.delay_pct || 0),
        imageScore: Number(latest.image_score || 0),
        incidents: Number(latest.incidents_count || 0),
        closures: Number(latest.closures_count || 0),
        weatherRiskScore: Number(latest.weather_risk_score || 0)
      },
      runtimeSettings,
      {
        referenceTs: latest.run_ts,
        cameraFreshPct: Number(latest.camera_fresh_pct || 100),
        cameraCountUsed: Number(latest.camera_total || 0),
        trafficFeedOk: true,
        incidentFeedOk: true,
        closureFeedOk: true,
        weatherAvailable: Number(latest.weather_risk_score || 0) > 0,
        learnedMultipliers: autoLearn.multipliers
      }
    );
    weighting = {
      ...(recomputed.weighting || {}),
      auto_learn_enabled: Number(runtimeSettings.auto_learn_weights_enabled ?? 1) >= 0.5,
      auto_learn_fit: autoLearn.fit
    };
  }

  res.json({
    corridor_id: corridorId,
    run_ts: latest.run_ts,
    fused_score: Number(latest.fused_score || 0),
    weighting_source: fromRun ? "saved_run" : "recomputed",
    weighting,
    image_signals: imageSignalsFromRun || {
      mode_label: latest.image_signal_mode || "hybrid_auto",
      selected_score: Number(latest.image_score || 0),
      proxy_score: Number(latest.image_proxy_score || 0),
      queue_stop_score: Number(latest.image_queue_stop_score || 0),
      hybrid_score: Number(latest.image_hybrid_score || 0),
      hybrid_confidence_pct: Number(latest.image_signal_confidence || 0),
      cv: {
        provider: latest.cv_provider || "disabled",
        analyzed_count: Number(latest.cv_analyzed_count || 0),
        coverage_pct: Number(latest.cv_coverage_pct || 0),
        avg_confidence: Number(latest.cv_avg_confidence || 0),
        error_count: Number(latest.cv_error_count || 0)
      }
    },
    camera_selection: cameraSelectionFromRun || null,
    auto_learn_fit: autoLearnFit
  });
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
  const runtimeSettings = await getRuntimeSettings();

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
    let merged = { ...weatherFromRun };
    const hasFrames = Array.isArray(merged.radar_frame_times) && merged.radar_frame_times.length > 0;
    const hasStationSchema =
      Object.prototype.hasOwnProperty.call(merged, "radar_station_frames") ||
      Object.prototype.hasOwnProperty.call(merged, "radar_station_loop_gif_url");
    if (!hasFrames || !hasStationSchema) {
      const liveForFrames = await fetchCorridorWeather(corridor, runtimeSettings, { useCache: true });
      merged = {
        ...liveForFrames,
        ...merged,
        radar_frame_times: liveForFrames.radar_frame_times || [],
        radar_time_default_utc: liveForFrames.radar_time_default_utc || merged.radar_time_default_utc || "",
        radar_wms_base_url: liveForFrames.radar_wms_base_url || merged.radar_wms_base_url,
        radar_wms_layer: liveForFrames.radar_wms_layer || merged.radar_wms_layer,
        radar_station: merged.radar_station || liveForFrames.radar_station || "",
        radar_station_url: merged.radar_station_url || liveForFrames.radar_station_url || "",
        radar_station_loop_gif_url: merged.radar_station_loop_gif_url || liveForFrames.radar_station_loop_gif_url || "",
        radar_station_frames:
          Array.isArray(merged.radar_station_frames) && merged.radar_station_frames.length
            ? merged.radar_station_frames
            : liveForFrames.radar_station_frames || [],
        radar_station_frame_count: Number(
          merged.radar_station_frame_count ||
            liveForFrames.radar_station_frame_count ||
            (liveForFrames.radar_station_frames || []).length
        ),
        radar_station_source: merged.radar_station_source || liveForFrames.radar_station_source || ""
      };
    }
    return res.json({
      corridor_id: corridor.id,
      corridor_name: corridor.name,
      from_live_fetch: false,
      run_ts: latest.run_ts,
      weather_risk_score: Number(latest.weather_risk_score || weatherFromRun.weather_risk_score || 0),
      weather_component: Number(latest.weather_component || 0),
      ...merged
    });
  }

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
  const out = await runPollCycleGuarded({
    sampleLimit: runtimeSettings.sample_limit,
    baselineAlpha: runtimeSettings.baseline_alpha,
    runtimeSettings,
    source: "api_poll_now"
  });
  res.json({
    ok: true,
    already_running: out.alreadyRunning,
    active_source: out.source,
    active_started_at: out.startedAt ? new Date(out.startedAt).toISOString() : null,
    result: out.result || null
  });
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
      await runPollCycleGuarded({
        sampleLimit: runtimeSettings.sample_limit,
        baselineAlpha: runtimeSettings.baseline_alpha,
        runtimeSettings,
        source: "background_loop"
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
