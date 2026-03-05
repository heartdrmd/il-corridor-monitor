import { feeds } from "./config.js";
import { pool, withTx } from "./db.js";
import { computeFused, detectAlert, imageScoreFromBaseline, median, p75, trainDynamicComponentMultipliers } from "./scoring.js";
import { predictNextScore, trainNextScoreModel } from "./modeling.js";
import { fetchCorridorWeather } from "./weather.js";
import { analyzeCvForObservations } from "./cv.js";

function arcgisParams(base = {}) {
  return new URLSearchParams({
    where: "1=1",
    f: "pjson",
    ...base
  });
}

async function arcgisQuery(url, params) {
  const res = await fetch(`${url}?${params.toString()}`);
  if (!res.ok) throw new Error(`ArcGIS query failed: ${res.status}`);
  return res.json();
}

function bboxString(c) {
  return `${c.xmin},${c.ymin},${c.xmax},${c.ymax}`;
}

function distanceKm(lat1, lon1, lat2, lon2) {
  const toRad = (d) => (d * Math.PI) / 180;
  const dLat = toRad(lat2 - lat1);
  const dLon = toRad(lon2 - lon1);
  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLon / 2) ** 2;
  return 6371 * (2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a)));
}

function clamp(v, lo, hi) {
  return Math.max(lo, Math.min(hi, v));
}

function featurePoint(feature) {
  if (!feature) return null;
  const attrs = feature.attributes || feature;
  const g = feature.geometry || {};
  const x = Number(
    attrs.x ??
      attrs.X ??
      attrs.lon ??
      attrs.LON ??
      attrs.Longitude ??
      attrs.LONGITUDE ??
      g.x ??
      g.X
  );
  const y = Number(
    attrs.y ??
      attrs.Y ??
      attrs.lat ??
      attrs.LAT ??
      attrs.Latitude ??
      attrs.LATITUDE ??
      g.y ??
      g.Y
  );
  if (Number.isFinite(y) && Number.isFinite(x)) return { lat: y, lon: x };

  const paths = g.paths;
  if (Array.isArray(paths) && paths.length && Array.isArray(paths[0]) && paths[0].length) {
    const first = paths[0][0];
    const last = paths[0][paths[0].length - 1];
    if (Array.isArray(first) && Array.isArray(last)) {
      const lon = (Number(first[0]) + Number(last[0])) / 2;
      const lat = (Number(first[1]) + Number(last[1])) / 2;
      if (Number.isFinite(lat) && Number.isFinite(lon)) return { lat, lon };
    }
  }
  return null;
}

function cameraRowFromFeature(f) {
  const attrs = f.attributes || {};
  const pt = featurePoint(f);
  return {
    ...attrs,
    __lat: pt?.lat,
    __lon: pt?.lon
  };
}

function cameraWithinAnyCheckpoint(camera, checkpoints) {
  const lat = Number(camera.__lat ?? camera.y ?? camera.lat);
  const lon = Number(camera.__lon ?? camera.x ?? camera.lon);
  if (!Number.isFinite(lat) || !Number.isFinite(lon)) return false;
  return checkpoints.some((cp) => distanceKm(lat, lon, cp.lat, cp.lon) <= cp.radiusKm);
}

function trafficSegmentsForLocalStats(features = []) {
  const out = [];
  for (const f of features) {
    const a = f.attributes || {};
    const speed = Number(a.SPEED);
    const speedFF = Number(a.SPEED_FF);
    if (!Number.isFinite(speed) || !Number.isFinite(speedFF) || speed < 0 || speedFF <= 0) continue;
    const pt = featurePoint(f);
    if (!pt) continue;
    const delayPct = clamp(((speedFF - speed) / speedFF) * 100, 0, 100);
    out.push({ speed, speedFF, delayPct, lat: pt.lat, lon: pt.lon });
  }
  return out;
}

function localTrafficStats(cam, segments, cfg = {}) {
  const radiusKm = Math.max(0.5, Number(cfg.queue_stop_radius_km ?? 4));
  const slowSpeed = Math.max(1, Number(cfg.queue_stop_slow_speed_mph ?? 25));
  const stopSpeed = Math.max(0.5, Number(cfg.queue_stop_stop_speed_mph ?? 8));
  const lat = Number(cam.__lat ?? cam.y ?? cam.lat);
  const lon = Number(cam.__lon ?? cam.x ?? cam.lon);
  if (!Number.isFinite(lat) || !Number.isFinite(lon)) {
    return { segmentCount: 0, avgDelayPct: 0, slowPct: 0, stopPct: 0 };
  }
  const nearby = [];
  for (const s of segments) {
    const d = distanceKm(lat, lon, s.lat, s.lon);
    if (d <= radiusKm) nearby.push(s);
  }
  if (!nearby.length) return { segmentCount: 0, avgDelayPct: 0, slowPct: 0, stopPct: 0 };
  const avgDelayPct = nearby.reduce((sum, s) => sum + s.delayPct, 0) / nearby.length;
  const slowPct = (nearby.filter((s) => s.speed <= slowSpeed).length * 100) / nearby.length;
  const stopPct = (nearby.filter((s) => s.speed <= stopSpeed).length * 100) / nearby.length;
  return { segmentCount: nearby.length, avgDelayPct, slowPct, stopPct };
}

function queueStopScoreFromLocal(stats, cfg = {}) {
  const delayWeight = clamp(Number(cfg.queue_stop_delay_weight ?? 0.5), 0, 1);
  const slowWeight = clamp(Number(cfg.queue_stop_slow_weight ?? 0.25), 0, 1);
  const stopWeight = clamp(Number(cfg.queue_stop_stop_weight ?? 0.25), 0, 1);
  const weightSum = Math.max(0.001, delayWeight + slowWeight + stopWeight);
  const delayNorm = clamp(Number(stats.avgDelayPct || 0) / 70, 0, 1) * 100;
  const slowNorm = clamp(Number(stats.slowPct || 0) / 60, 0, 1) * 100;
  const stopNorm = clamp(Number(stats.stopPct || 0) / 35, 0, 1) * 100;
  const raw = (delayWeight * delayNorm + slowWeight * slowNorm + stopWeight * stopNorm) / weightSum;
  return clamp(raw, 0, 100);
}

function hybridConfidenceFromLocal({ tooOld, cameraTotalUsed, localSegmentCount }, cfg = {}) {
  const floor = clamp(Number(cfg.hybrid_confidence_floor ?? 0.15), 0, 1);
  const gain = clamp(Number(cfg.hybrid_confidence_gain ?? 0.85), 0, 1);
  const cameraWeight = clamp(Number(cfg.hybrid_camera_weight ?? 0.65), 0, 1);
  const trafficWeight = clamp(Number(cfg.hybrid_traffic_weight ?? 0.35), 0, 1);
  const coverageRef = Math.max(1, Number(cfg.camera_coverage_ref ?? 10));
  const minSeg = Math.max(1, Number(cfg.queue_stop_min_segments ?? 3));
  const cameraCoverage = clamp(Number(cameraTotalUsed || 0) / coverageRef, 0, 1);
  const cameraFreshFactor = tooOld ? 0.35 : 1;
  const cameraQuality = clamp(cameraCoverage * cameraFreshFactor, 0, 1);
  const trafficQuality = clamp(Number(localSegmentCount || 0) / minSeg, 0, 1);
  const denom = Math.max(0.001, cameraWeight + trafficWeight);
  const blended = (cameraWeight * cameraQuality + trafficWeight * trafficQuality) / denom;
  return clamp(floor + gain * blended, 0, 1);
}

function imageModeLabel(modeCode) {
  const m = Number(modeCode ?? 2);
  if (m <= 0) return "proxy_only";
  if (m === 1) return "queue_stop";
  return "hybrid_auto";
}

function selectImageSignal(modeCode, proxyScore, queueStopScore, hybridScore, localSegmentCount) {
  const m = Number(modeCode ?? 2);
  if (m <= 0) return proxyScore;
  if (m === 1) return localSegmentCount > 0 ? queueStopScore : proxyScore;
  return hybridScore;
}

function cameraTooOldFlag(camera) {
  const raw = String(camera?.TooOld ?? "").toLowerCase();
  return raw === "true" || raw === "1" || raw === "yes";
}

function cameraAgeMinutes(camera) {
  const age = Number(camera?.AgeInMinutes);
  return Number.isFinite(age) ? age : null;
}

function nearestCheckpointDistanceKm(camera, checkpoints) {
  const lat = Number(camera.__lat ?? camera.y ?? camera.lat);
  const lon = Number(camera.__lon ?? camera.x ?? camera.lon);
  if (!Number.isFinite(lat) || !Number.isFinite(lon) || !Array.isArray(checkpoints) || !checkpoints.length) return null;
  let best = Number.POSITIVE_INFINITY;
  for (const cp of checkpoints) {
    const d = distanceKm(lat, lon, cp.lat, cp.lon);
    if (d < best) best = d;
  }
  return Number.isFinite(best) ? best : null;
}

function cameraFreshnessScore(camera, cfg = {}) {
  const age = cameraAgeMinutes(camera);
  const tooOld = cameraTooOldFlag(camera);
  const freshRefMin = Math.max(1, Number(cfg.camera_fresh_ref_minutes ?? 5));
  if (age == null) return tooOld ? 0.2 : 0.55;
  let score = clamp(1 - age / freshRefMin, 0, 1);
  if (tooOld) score *= 0.35;
  return score;
}

function cameraHardDropReason(camera, cfg = {}) {
  const enabled = Number(cfg.camera_hard_drop_enabled ?? 1) >= 0.5;
  if (!enabled) return "";
  const snapshot = String(camera?.SnapShot || "").trim();
  if (!snapshot) return "missing_snapshot";
  const age = cameraAgeMinutes(camera);
  const tooOld = cameraTooOldFlag(camera);
  const dropTooOldFlag = Number(cfg.camera_drop_too_old_flag ?? 1) >= 0.5;
  const maxAge = Math.max(3, Number(cfg.camera_hard_max_age_minutes ?? 16));
  if (dropTooOldFlag && tooOld) return "too_old_flag";
  if (age != null && age > maxAge) return "age_exceeds_max";
  return "";
}

function baseRankScore(camera, checkpoints, cfg = {}) {
  const wFresh = Math.max(0, Number(cfg.camera_score_weight_fresh ?? 0.55));
  const wProx = Math.max(0, Number(cfg.camera_score_weight_proximity ?? 0.3));
  const wScope = Math.max(0, Number(cfg.camera_score_weight_scope ?? 0.15));
  const wSum = Math.max(0.001, wFresh + wProx + wScope);
  const fresh = cameraFreshnessScore(camera, cfg);
  const dist = nearestCheckpointDistanceKm(camera, checkpoints);
  const proxScale = Math.max(1, Number(cfg.camera_proximity_scale_km ?? 14));
  const prox = checkpoints.length ? Math.exp(-Math.max(0, Number(dist ?? proxScale * 2)) / proxScale) : 0.65;
  const scope = camera.__scoped ? 1 : 0;
  const nonScopedPenalty = clamp(Number(cfg.camera_non_scoped_penalty ?? 0.25), 0, 1);
  const missingPenalty = clamp(Number(cfg.camera_missing_snapshot_penalty ?? 0.35), 0, 1);
  const snapshot = String(camera?.SnapShot || "").trim();
  let score = (wFresh * fresh + wProx * prox + wScope * scope) / wSum;
  if (!camera.__scoped) score -= nonScopedPenalty;
  if (!snapshot) score -= missingPenalty;
  return clamp(score, 0, 1.2);
}

function chooseAdaptiveSampleCount(sortedCandidates, sampleLimit, cfg = {}) {
  const nAvail = sortedCandidates.length;
  if (nAvail <= 0) return 0;
  const maxHard = Math.max(10, Number(cfg.sample_limit_max ?? 140));
  const adaptiveMin = clamp(Number(cfg.adaptive_sample_min ?? 20), 1, maxHard);
  const baseRaw = clamp(Number(sampleLimit ?? 40), 1, maxHard);
  const base = Math.max(baseRaw, adaptiveMin);
  const adaptiveOn = Number(cfg.adaptive_camera_sampling_enabled ?? 1) >= 0.5;
  if (!adaptiveOn) return Math.min(nAvail, base);

  const startN = Math.min(nAvail, base);
  const maxN = Math.min(nAvail, clamp(Number(cfg.adaptive_sample_max ?? 140), base, maxHard));
  const k = Math.max(2, Number(cfg.adaptive_conf_k ?? 24));
  const gainThreshold = Math.max(0.0005, Number(cfg.adaptive_conf_gain_threshold ?? 0.012));

  const prefix = [0];
  for (const c of sortedCandidates) {
    prefix.push(prefix[prefix.length - 1] + Number(c.__baseRankScore || 0));
  }
  const conf = (n) => {
    if (n <= 0) return 0;
    const avgQ = prefix[n] / n;
    const coverage = 1 - Math.exp(-n / k);
    return avgQ * coverage;
  };

  let n = startN;
  while (n < maxN) {
    const delta = conf(n + 1) - conf(n);
    if (delta < gainThreshold) break;
    n += 1;
  }
  return n;
}

function selectWithDiversity(candidates, targetCount, cfg = {}) {
  const out = [];
  const remaining = [...candidates];
  const diversityRefKm = Math.max(1, Number(cfg.camera_diversity_ref_km ?? 18));
  const diversityWeight = clamp(Number(cfg.camera_diversity_bonus_weight ?? 0.22), 0, 1);
  const pointOf = (c) => {
    const lat = Number(c.__lat ?? c.y ?? c.lat);
    const lon = Number(c.__lon ?? c.x ?? c.lon);
    if (!Number.isFinite(lat) || !Number.isFinite(lon)) return null;
    return { lat, lon };
  };

  while (out.length < targetCount && remaining.length) {
    let bestIdx = 0;
    let bestScore = -1;
    for (let i = 0; i < remaining.length; i++) {
      const cand = remaining[i];
      const base = Number(cand.__baseRankScore || 0);
      let diversityBonus = 0;
      if (out.length) {
        const p = pointOf(cand);
        if (p) {
          let minD = Number.POSITIVE_INFINITY;
          for (const picked of out) {
            const pp = pointOf(picked);
            if (!pp) continue;
            const d = distanceKm(p.lat, p.lon, pp.lat, pp.lon);
            if (d < minD) minD = d;
          }
          if (Number.isFinite(minD)) diversityBonus = clamp(minD / diversityRefKm, 0, 1) * diversityWeight;
        }
      }
      const total = base + diversityBonus;
      if (total > bestScore) {
        bestScore = total;
        bestIdx = i;
      }
    }
    out.push(remaining.splice(bestIdx, 1)[0]);
  }
  return out;
}

function buildCameraSelection(allCameraAttrs, checkpoints, sampleLimit, cfg = {}) {
  const strictMin = Number(cfg.min_scoped_cameras_for_strict ?? 8);
  const hasCheckpoints = Array.isArray(checkpoints) && checkpoints.length > 0;
  const scopedRaw = hasCheckpoints ? allCameraAttrs.filter((c) => cameraWithinAnyCheckpoint(c, checkpoints)) : allCameraAttrs;
  const usingFallback = hasCheckpoints && scopedRaw.length < strictMin;
  const sourcePool = usingFallback ? allCameraAttrs : scopedRaw;

  const candidates = [];
  const dropped = {
    missing_snapshot: 0,
    too_old_flag: 0,
    age_exceeds_max: 0
  };
  for (const c of sourcePool) {
    const row = { ...c };
    row.__scoped = !hasCheckpoints || cameraWithinAnyCheckpoint(c, checkpoints);
    row.__nearestCpKm = nearestCheckpointDistanceKm(c, checkpoints);
    const reason = cameraHardDropReason(row, cfg);
    if (reason) {
      if (reason in dropped) dropped[reason] += 1;
      continue;
    }
    row.__baseRankScore = baseRankScore(row, checkpoints, cfg);
    candidates.push(row);
  }

  candidates.sort((a, b) => Number(b.__baseRankScore || 0) - Number(a.__baseRankScore || 0));
  const targetCount = chooseAdaptiveSampleCount(candidates, sampleLimit, cfg);
  const selected = selectWithDiversity(candidates, targetCount, cfg);
  const selectedFresh = selected.filter((c) => !cameraTooOldFlag(c)).length;
  const freshPct = selected.length ? (selectedFresh * 100) / selected.length : 0;

  return {
    selected,
    freshPct,
    scopedRawCount: scopedRaw.length,
    sourcePoolCount: sourcePool.length,
    candidateCount: candidates.length,
    usingFallback,
    dropped,
    targetCount,
    availableCount: allCameraAttrs.length
  };
}

async function fetchSnapshotBytes(url) {
  try {
    const res = await fetch(url, { redirect: "follow" });
    if (!res.ok) return null;
    const buf = await res.arrayBuffer();
    return buf.byteLength;
  } catch {
    return null;
  }
}

async function getPrevStats(client, corridorId) {
  const { rows } = await client.query(
    `SELECT fused_score
       FROM monitor_runs
      WHERE corridor_id = $1
      ORDER BY run_ts DESC
      LIMIT 12`,
    [corridorId]
  );
  const vals = rows.map((r) => Number(r.fused_score));
  return { prevCount: vals.length, prevMedian: median(vals) };
}

async function getTrainingRuns(client, corridorId, limit = 420) {
  const { rows } = await client.query(
    `SELECT run_ts, fused_score, image_score, incidents_count, closures_count, camera_fresh_pct
       FROM monitor_runs
      WHERE corridor_id = $1
      ORDER BY run_ts DESC
      LIMIT $2`,
    [corridorId, limit]
  );
  return rows.reverse();
}

async function getBaseline(client, corridorId, cameraObjectId, dow, hour) {
  const { rows } = await client.query(
    `SELECT ewma_bytes, sample_count
       FROM baseline_profiles
      WHERE corridor_id = $1
        AND camera_object_id = $2
        AND dow = $3
        AND hour = $4`,
    [corridorId, cameraObjectId, dow, hour]
  );
  if (!rows.length) return null;
  return {
    ewmaBytes: Number(rows[0].ewma_bytes),
    sampleCount: Number(rows[0].sample_count)
  };
}

async function upsertBaseline(client, corridorId, cameraObjectId, dow, hour, ewmaBytes, sampleCount) {
  await client.query(
    `INSERT INTO baseline_profiles(corridor_id, camera_object_id, dow, hour, ewma_bytes, sample_count)
     VALUES($1,$2,$3,$4,$5,$6)
     ON CONFLICT (corridor_id, camera_object_id, dow, hour)
     DO UPDATE SET ewma_bytes = EXCLUDED.ewma_bytes,
                   sample_count = EXCLUDED.sample_count,
                   updated_at = NOW()`,
    [corridorId, cameraObjectId, dow, hour, ewmaBytes, sampleCount]
  );
}

async function getDynamicWeightSamples(client, corridorId, limit = 240) {
  const { rows } = await client.query(
    `SELECT run_ts, fused_score, traffic_component, image_component, incident_component, closure_component, weather_component
       FROM monitor_runs
      WHERE corridor_id = $1
      ORDER BY run_ts DESC
      LIMIT $2`,
    [corridorId, limit]
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
  return samples;
}

async function getCvStateMap(client, cameraIds = []) {
  const ids = [...new Set((cameraIds || []).map((x) => Number(x)).filter((x) => Number.isFinite(x) && x > 0))];
  if (!ids.length) return new Map();
  const { rows } = await client.query(
    `SELECT camera_object_id, last_vehicle_count, last_stopped_count, last_queue_index, last_scene_confidence,
            last_snapshot_bytes, last_snapshot_url, updated_at
       FROM camera_cv_state
      WHERE camera_object_id = ANY($1::bigint[])`,
    [ids]
  );
  return new Map(rows.map((r) => [Number(r.camera_object_id), r]));
}

async function upsertCvState(client, observation) {
  const cameraObjectId = Number(observation.cameraObjectId);
  if (!Number.isFinite(cameraObjectId) || cameraObjectId <= 0) return;
  await client.query(
    `INSERT INTO camera_cv_state(
       camera_object_id, last_vehicle_count, last_stopped_count, last_queue_index, last_scene_confidence,
       last_snapshot_bytes, last_snapshot_url, updated_at
     )
     VALUES($1,$2,$3,$4,$5,$6,$7,NOW())
     ON CONFLICT(camera_object_id) DO UPDATE
       SET last_vehicle_count = EXCLUDED.last_vehicle_count,
           last_stopped_count = EXCLUDED.last_stopped_count,
           last_queue_index = EXCLUDED.last_queue_index,
           last_scene_confidence = EXCLUDED.last_scene_confidence,
           last_snapshot_bytes = EXCLUDED.last_snapshot_bytes,
           last_snapshot_url = EXCLUDED.last_snapshot_url,
           updated_at = NOW()`,
    [
      cameraObjectId,
      Number(observation.cvVehicleCount || 0),
      Number(observation.cvStoppedCount || 0),
      Number(observation.cvQueueScore || 0),
      Number(observation.cvConfidence || 0),
      Number(observation.snapshotBytes || 0),
      String(observation.snapshotUrl || "")
    ]
  );
}

export async function runCorridorPoll(corridor, { sampleLimit = 40, baselineAlpha = 0.2, runtimeSettings = {} } = {}) {
  const bbox = bboxString(corridor);
  const common = {
    geometry: bbox,
    geometryType: "esriGeometryEnvelope",
    inSR: "4326",
    spatialRel: "esriSpatialRelIntersects"
  };

  const weatherPromise = fetchCorridorWeather(corridor, runtimeSettings, { useCache: true });
  const [traffic, incidents, closures, cameras, weather] = await Promise.all([
    arcgisQuery(
      feeds.traffic,
      arcgisParams({ ...common, outFields: "SPEED,SPEED_FF,JAM_FACTOR", outSR: "4326", returnGeometry: "true" })
    ),
    arcgisQuery(feeds.incidents, arcgisParams({ ...common, returnCountOnly: "true" })),
    arcgisQuery(feeds.closures, arcgisParams({ ...common, returnCountOnly: "true" })),
    arcgisQuery(
      feeds.cameras,
      arcgisParams({
        ...common,
        outFields: "OBJECTID,CameraLocation,CameraDirection,SnapShot,AgeInMinutes,TooOld",
        outSR: "4326",
        returnGeometry: "true"
      })
    ),
    weatherPromise
  ]);

  const validTraffic = (traffic.features || []).filter((f) => {
    const a = f.attributes || {};
    return Number(a.SPEED) >= 0 && Number(a.SPEED_FF) > 0;
  });
  const trafficSegments = validTraffic.length;
  const delayPct =
    trafficSegments === 0
      ? 0
      : (validTraffic.reduce((sum, f) => {
          const a = f.attributes;
          return sum + ((Number(a.SPEED_FF) - Number(a.SPEED)) / Number(a.SPEED_FF)) * 100;
        }, 0) /
          trafficSegments);

  const checkpointsRes = await pool.query(
    `SELECT lat, lon, radius_km
       FROM checkpoints
      WHERE corridor_id = $1
        AND active = true`,
    [corridor.id]
  );
  const checkpoints = checkpointsRes.rows.map((r) => ({
    lat: Number(r.lat),
    lon: Number(r.lon),
    radiusKm: Number(r.radius_km || 8)
  }));

  const allCameraAttrs = (cameras.features || []).map((f) => cameraRowFromFeature(f));
  const selection = buildCameraSelection(allCameraAttrs, checkpoints, sampleLimit, runtimeSettings);
  const cameraRows = selection.selected;
  const trafficForLocal = trafficSegmentsForLocalStats(validTraffic);
  const freshPct = selection.freshPct;

  const now = new Date();
  const dow = ((now.getUTCDay() + 6) % 7) + 1; // 1..7 (Mon..Sun)
  const hour = now.getUTCHours();

  return withTx(async (client) => {
    const imageSignalMode = Number(runtimeSettings.image_signal_mode ?? 2);
    const imageSignalModeLabel = imageModeLabel(imageSignalMode);
    const observations = [];
    for (const c of cameraRows) {
      const cameraObjectId = Number(c.OBJECTID);
      const bytes = await fetchSnapshotBytes(c.SnapShot);
      if (!bytes) continue;

      const baseline = await getBaseline(client, corridor.id, cameraObjectId, dow, hour);
      const proxyScore = imageScoreFromBaseline(bytes, baseline?.ewmaBytes || 0);
      const local = localTrafficStats(c, trafficForLocal, runtimeSettings);
      const queueStopScore = queueStopScoreFromLocal(local, runtimeSettings);
      const tooOld = String(c.TooOld) === "true";
      const hybridConfidence = hybridConfidenceFromLocal(
        {
          tooOld,
          cameraTotalUsed: cameraRows.length,
          localSegmentCount: local.segmentCount
        },
        runtimeSettings
      );
      const hybridScore = clamp(hybridConfidence * queueStopScore + (1 - hybridConfidence) * proxyScore, 0, 100);
      const selectedImageScore = selectImageSignal(
        imageSignalMode,
        proxyScore,
        queueStopScore,
        hybridScore,
        local.segmentCount
      );

      const prevEwma = baseline?.ewmaBytes || bytes;
      const prevCount = baseline?.sampleCount || 0;
      const nextEwma = prevCount === 0 ? bytes : baselineAlpha * bytes + (1 - baselineAlpha) * prevEwma;
      await upsertBaseline(client, corridor.id, cameraObjectId, dow, hour, nextEwma, prevCount + 1);

      observations.push({
        cameraObjectId,
        location: c.CameraLocation || "",
        direction: c.CameraDirection || "",
        snapshotUrl: c.SnapShot || "",
        snapshotBytes: bytes,
        ageMinutes: Number(c.AgeInMinutes || 0),
        tooOld,
        imageScore: selectedImageScore,
        proxyScore,
        queueStopScore,
        hybridScore,
        hybridConfidence,
        localDelayPct: local.avgDelayPct,
        localSlowPct: local.slowPct,
        localStopPct: local.stopPct,
        localSegmentCount: local.segmentCount
      });
    }

    const cvEnabled = Number(runtimeSettings.cv_enabled ?? 0) >= 0.5;
    let cvRun = {
      enabled: cvEnabled,
      provider: "disabled",
      analyzedCount: 0,
      coveragePct: 0,
      avgConfidence: 0,
      byCamera: new Map(),
      errors: []
    };
    if (cvEnabled && observations.length) {
      const cvState = await getCvStateMap(
        client,
        observations.map((o) => o.cameraObjectId)
      );
      cvRun = await analyzeCvForObservations(observations, runtimeSettings, cvState);
      for (const o of observations) {
        const cv = cvRun.byCamera.get(Number(o.cameraObjectId));
        if (!cv) continue;
        o.cvProvider = cv.provider;
        o.cvVehicleCount = Number(cv.vehicleCount || 0);
        o.cvStoppedCount = Number(cv.stoppedVehicleCount || 0);
        o.cvQueueScore = Number(cv.cvQueueScore || 0);
        o.cvLaneOccupancyPct = Number(cv.laneOccupancyPct || 0);
        o.cvVisibilityQuality = Number(cv.visibilityQuality || 0);
        o.cvConfidence = Number(cv.sceneConfidence || 0);
        o.cvBlendWeight = Number(cv.blendWeight || 0);
        o.cvNotes = String(cv.notes || "");

        o.queueStopScore = Number(cv.blendedQueueStopScore || o.queueStopScore || 0);
        const boostedHybridConf = clamp(o.hybridConfidence + o.cvBlendWeight * 0.18, 0, 1);
        o.hybridConfidence = boostedHybridConf;
        o.hybridScore = clamp(boostedHybridConf * o.queueStopScore + (1 - boostedHybridConf) * o.proxyScore, 0, 100);
        o.imageScore = selectImageSignal(
          imageSignalMode,
          o.proxyScore,
          o.queueStopScore,
          o.hybridScore,
          o.localSegmentCount
        );
        await upsertCvState(client, o);
      }
    }

    const proxyImageScore = p75(observations.map((o) => o.proxyScore));
    const queueStopImageScore = p75(observations.map((o) => o.queueStopScore));
    const hybridImageScore = p75(observations.map((o) => o.hybridScore));
    const imageSignalConfidencePct = observations.length
      ? (observations.reduce((sum, o) => sum + Number(o.hybridConfidence || 0), 0) / observations.length) * 100
      : 0;
    const queueSupportCameras = observations.filter((o) => Number(o.localSegmentCount || 0) > 0).length;
    const imageScore = p75(observations.map((o) => o.imageScore));
    const weatherRiskScore = Number(weather?.weather_risk_score || 0);
    const autoLearnEnabled = Number(runtimeSettings.auto_learn_weights_enabled ?? 1) >= 0.5;
    const autoLearnWindow = Math.max(24, Number(runtimeSettings.auto_learn_window_points ?? 240));
    const autoLearnMinRows = Math.max(20, Number(runtimeSettings.auto_learn_min_rows ?? 60));
    let learnedMultipliers = null;
    let learnFit = { rows: 0, r2: 0, rmse: 0 };
    if (autoLearnEnabled) {
      const samples = await getDynamicWeightSamples(client, corridor.id, autoLearnWindow);
      if (samples.length >= autoLearnMinRows) {
        const learned = trainDynamicComponentMultipliers(samples, runtimeSettings);
        if (learned?.fit?.rows >= autoLearnMinRows) {
          learnedMultipliers = learned.multipliers;
          learnFit = learned.fit;
        }
      }
    }

    const fusedCalc = computeFused({
      trafficSegments,
      delayPct,
      imageScore,
      incidents: Number(incidents.count || 0),
      closures: Number(closures.count || 0),
      weatherRiskScore
    }, runtimeSettings, {
      referenceTs: now,
      cameraFreshPct: freshPct,
      cameraCountUsed: cameraRows.length,
      trafficFeedOk: true,
      incidentFeedOk: true,
      closureFeedOk: true,
      weatherAvailable: !!weather,
      learnedMultipliers
    });
    const prev = await getPrevStats(client, corridor.id);
    const trainingRuns = await getTrainingRuns(client, corridor.id);
    const model = trainNextScoreModel(trainingRuns, runtimeSettings);
    const prediction = predictNextScore(model, {
      fused_score: fusedCalc.fused,
      image_score: imageScore,
      incidents_count: Number(incidents.count || 0),
      closures_count: Number(closures.count || 0),
      camera_fresh_pct: freshPct
    }, runtimeSettings);

    const alert = detectAlert({
      fusedScore: Math.max(fusedCalc.fused, prediction.p50),
      freshPct,
      prevMedian: prev.prevMedian,
      prevCount: prev.prevCount
    }, runtimeSettings);

    const runInsert = await client.query(
      `INSERT INTO monitor_runs(
         corridor_id, traffic_segments, delay_pct, incidents_count, closures_count, camera_total, camera_fresh_pct,
         image_score, traffic_component, image_component, incident_component, closure_component, fused_score,
         image_proxy_score, image_queue_stop_score, image_hybrid_score, image_signal_mode, image_signal_confidence,
         cv_analyzed_count, cv_coverage_pct, cv_avg_confidence, cv_provider, cv_error_count,
         weather_risk_score, weather_component,
         predicted_next_score_p50, predicted_next_score_p90, prediction_confidence, model_version, drift_score,
         alert_state, alert_reason, raw_json
       )
       VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33)
       RETURNING id`,
      [
        corridor.id,
        trafficSegments,
        delayPct,
        Number(incidents.count || 0),
        Number(closures.count || 0),
        cameraRows.length,
        freshPct,
        imageScore,
        fusedCalc.trafficComponent,
        fusedCalc.imageComponent,
        fusedCalc.incidentComponent,
        fusedCalc.closureComponent,
        fusedCalc.fused,
        proxyImageScore,
        queueStopImageScore,
        hybridImageScore,
        imageSignalModeLabel,
        imageSignalConfidencePct,
        Number(cvRun.analyzedCount || 0),
        Number(cvRun.coveragePct || 0),
        Number(cvRun.avgConfidence || 0),
        String(cvRun.provider || "disabled"),
        Number((cvRun.errors || []).length || 0),
        weatherRiskScore,
        fusedCalc.weatherComponent,
        prediction.p50,
        prediction.p90,
        prediction.confidence,
        model?.version || "fallback_v1",
        prediction.driftScore,
        alert.state,
        alert.reason,
        JSON.stringify({
          corridor,
          weather,
          imageSignals: {
            mode_code: imageSignalMode,
            mode_label: imageSignalModeLabel,
            selected_score: imageScore,
            proxy_score: proxyImageScore,
            queue_stop_score: queueStopImageScore,
            hybrid_score: hybridImageScore,
            hybrid_confidence_pct: imageSignalConfidencePct,
            cv: {
              enabled: cvRun.enabled,
              provider: cvRun.provider,
              analyzed_count: Number(cvRun.analyzedCount || 0),
              coverage_pct: Number(cvRun.coveragePct || 0),
              avg_confidence: Number(cvRun.avgConfidence || 0),
              error_count: Number((cvRun.errors || []).length || 0),
              errors: (cvRun.errors || []).slice(0, 20)
            },
            queue_support_cameras: queueSupportCameras,
            cameras_sampled: observations.length,
            camera_details: observations.slice(0, 40).map((o) => ({
              camera_object_id: o.cameraObjectId,
              location: o.location,
              direction: o.direction,
              selected_score: Number(o.imageScore || 0),
              proxy_score: Number(o.proxyScore || 0),
              queue_stop_score: Number(o.queueStopScore || 0),
              hybrid_score: Number(o.hybridScore || 0),
              hybrid_confidence: Number(o.hybridConfidence || 0),
              local_delay_pct: Number(o.localDelayPct || 0),
              local_slow_pct: Number(o.localSlowPct || 0),
              local_stop_pct: Number(o.localStopPct || 0),
              local_segment_count: Number(o.localSegmentCount || 0),
              cv_provider: o.cvProvider || "",
              cv_vehicle_count: Number(o.cvVehicleCount || 0),
              cv_stopped_count: Number(o.cvStoppedCount || 0),
              cv_queue_score: Number(o.cvQueueScore || 0),
              cv_lane_occupancy_pct: Number(o.cvLaneOccupancyPct || 0),
              cv_visibility_quality: Number(o.cvVisibilityQuality || 0),
              cv_confidence: Number(o.cvConfidence || 0),
              cv_blend_weight: Number(o.cvBlendWeight || 0),
              cv_notes: o.cvNotes || "",
              snapshot_url: o.snapshotUrl
            }))
          },
          weighting: {
            ...(fusedCalc.weighting || {}),
            auto_learn_enabled: autoLearnEnabled,
            auto_learn_fit: learnFit
          },
          prev,
          prediction,
          model: model
            ? {
                version: model.version,
                trainRows: model.trainRows,
                sigma: model.sigma,
                r2: model.r2
              }
            : null,
          feedCounts: {
            traffic: traffic.features?.length || 0,
            cameras: cameras.features?.length || 0,
            cameras_scoped_raw: selection.scopedRawCount,
            cameras_source_pool: selection.sourcePoolCount,
            cameras_candidates_after_drop: selection.candidateCount,
            cameras_scoped_used: cameraRows.length
          },
          cameraSelection: {
            using_scoped_fallback: selection.usingFallback,
            target_count: selection.targetCount,
            selected_count: cameraRows.length,
            fresh_pct: freshPct,
            dropped: selection.dropped
          }
        })
      ]
    );

    const runId = Number(runInsert.rows[0].id);

    if (model) {
      await client.query(
        `INSERT INTO model_snapshots(
           corridor_id, model_version, train_rows, residual_sigma, r2, drift_score, coefficients, feature_stats
         ) VALUES($1,$2,$3,$4,$5,$6,$7,$8)`,
        [
          corridor.id,
          model.version,
          model.trainRows,
          model.sigma,
          model.r2,
          prediction.driftScore,
          JSON.stringify(model.beta),
          JSON.stringify(model.featureStats)
        ]
      );
    }

    for (const o of observations) {
      await client.query(
        `INSERT INTO camera_observations(
           run_id, camera_object_id, camera_location, camera_direction, snapshot_url, snapshot_bytes, age_minutes, too_old, image_score,
           proxy_score, queue_stop_score, hybrid_score, hybrid_confidence, local_delay_pct, local_slow_pct, local_stop_pct, local_segment_count
           , cv_provider, cv_vehicle_count, cv_stopped_count, cv_queue_score, cv_lane_occupancy_pct, cv_visibility_quality, cv_confidence, cv_blend_weight, cv_notes
         ) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26)`,
        [
          runId,
          o.cameraObjectId,
          o.location,
          o.direction,
          o.snapshotUrl,
          o.snapshotBytes,
          o.ageMinutes,
          o.tooOld,
          o.imageScore,
          o.proxyScore,
          o.queueStopScore,
          o.hybridScore,
          o.hybridConfidence,
          o.localDelayPct,
          o.localSlowPct,
          o.localStopPct,
          o.localSegmentCount,
          o.cvProvider || "",
          Number(o.cvVehicleCount || 0),
          Number(o.cvStoppedCount || 0),
          Number(o.cvQueueScore || 0),
          Number(o.cvLaneOccupancyPct || 0),
          Number(o.cvVisibilityQuality || 0),
          Number(o.cvConfidence || 0),
          Number(o.cvBlendWeight || 0),
          String(o.cvNotes || "")
        ]
      );
    }

    return {
      runId,
      corridor: corridor.name,
      selectedCameraCount: cameraRows.length,
      cameraSelectionFallback: selection.usingFallback,
      fusedScore: fusedCalc.fused,
      imageSignalMode: imageSignalModeLabel,
      imageScore,
      proxyImageScore,
      queueStopImageScore,
      hybridImageScore,
      weatherRiskScore,
      predictedNextP50: prediction.p50,
      alert: alert.state
    };
  });
}

export async function runPollCycle(opts = {}) {
  const runtimeSettings = opts.runtimeSettings || {};
  const sampleLimit = Number(opts.sampleLimit ?? runtimeSettings.sample_limit ?? 40);
  const baselineAlpha = Number(opts.baselineAlpha ?? runtimeSettings.baseline_alpha ?? 0.2);
  const { rows } = await pool.query(
    `SELECT id, name,
            bbox_xmin AS xmin, bbox_ymin AS ymin, bbox_xmax AS xmax, bbox_ymax AS ymax
       FROM corridors
      WHERE active = true
      ORDER BY id`
  );
  const out = [];
  for (const c of rows) {
    try {
      out.push(await runCorridorPoll(c, { sampleLimit, baselineAlpha, runtimeSettings }));
    } catch (e) {
      out.push({ corridor: c.name, error: String(e.message || e) });
    }
  }
  return out;
}
