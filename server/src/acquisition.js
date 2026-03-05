import { feeds } from "./config.js";
import { pool, withTx } from "./db.js";
import { computeFused, detectAlert, imageScoreFromBaseline, median, p75 } from "./scoring.js";
import { predictNextScore, trainNextScoreModel } from "./modeling.js";
import { fetchCorridorWeather } from "./weather.js";

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

function cameraWithinAnyCheckpoint(camera, checkpoints) {
  const lat = Number(camera.y);
  const lon = Number(camera.x);
  if (!Number.isFinite(lat) || !Number.isFinite(lon)) return false;
  return checkpoints.some((cp) => distanceKm(lat, lon, cp.lat, cp.lon) <= cp.radiusKm);
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
      arcgisParams({ ...common, outFields: "SPEED,SPEED_FF,JAM_FACTOR", returnGeometry: "false" })
    ),
    arcgisQuery(feeds.incidents, arcgisParams({ ...common, returnCountOnly: "true" })),
    arcgisQuery(feeds.closures, arcgisParams({ ...common, returnCountOnly: "true" })),
    arcgisQuery(
      feeds.cameras,
      arcgisParams({
        ...common,
        outFields: "OBJECTID,CameraLocation,CameraDirection,SnapShot,AgeInMinutes,TooOld",
        returnGeometry: "false"
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

  const allCameraAttrs = (cameras.features || []).map((f) => f.attributes || {});
  const scopedCameraAttrsRaw =
    checkpoints.length > 0 ? allCameraAttrs.filter((c) => cameraWithinAnyCheckpoint(c, checkpoints)) : allCameraAttrs;
  const strictMin = Number(runtimeSettings.min_scoped_cameras_for_strict ?? 8);
  const scopedCameraAttrs =
    checkpoints.length > 0 && scopedCameraAttrsRaw.length < strictMin ? allCameraAttrs : scopedCameraAttrsRaw;
  const cameraRows = scopedCameraAttrs.slice(0, sampleLimit);
  const fresh = cameraRows.filter((c) => String(c.TooOld) === "false").length;
  const freshPct = cameraRows.length ? (fresh * 100) / cameraRows.length : 0;

  const now = new Date();
  const dow = ((now.getUTCDay() + 6) % 7) + 1; // 1..7 (Mon..Sun)
  const hour = now.getUTCHours();

  return withTx(async (client) => {
    const observations = [];
    for (const c of cameraRows) {
      const cameraObjectId = Number(c.OBJECTID);
      const bytes = await fetchSnapshotBytes(c.SnapShot);
      if (!bytes) continue;

      const baseline = await getBaseline(client, corridor.id, cameraObjectId, dow, hour);
      const imageScore = imageScoreFromBaseline(bytes, baseline?.ewmaBytes || 0);

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
        tooOld: String(c.TooOld) === "true",
        imageScore
      });
    }

    const imageScore = p75(observations.map((o) => o.imageScore));
    const weatherRiskScore = Number(weather?.weather_risk_score || 0);
    const fusedCalc = computeFused({
      trafficSegments,
      delayPct,
      imageScore,
      incidents: Number(incidents.count || 0),
      closures: Number(closures.count || 0),
      weatherRiskScore
    }, runtimeSettings);
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
         weather_risk_score, weather_component,
         predicted_next_score_p50, predicted_next_score_p90, prediction_confidence, model_version, drift_score,
         alert_state, alert_reason, raw_json
       )
       VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23)
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
            cameras_scoped_raw: scopedCameraAttrsRaw.length,
            cameras_scoped_used: cameraRows.length
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
           run_id, camera_object_id, camera_location, camera_direction, snapshot_url, snapshot_bytes, age_minutes, too_old, image_score
         ) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
        [runId, o.cameraObjectId, o.location, o.direction, o.snapshotUrl, o.snapshotBytes, o.ageMinutes, o.tooOld, o.imageScore]
      );
    }

    return {
      runId,
      corridor: corridor.name,
      fusedScore: fusedCalc.fused,
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
