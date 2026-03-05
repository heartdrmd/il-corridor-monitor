function clamp(v, lo, hi) {
  return Math.max(lo, Math.min(hi, v));
}

function toNum(v, fallback = 0) {
  const n = Number(v);
  return Number.isFinite(n) ? n : fallback;
}

function withTimeout(promise, ms) {
  return Promise.race([
    promise,
    new Promise((_, reject) => setTimeout(() => reject(new Error(`cv_timeout_${ms}ms`)), ms))
  ]);
}

function extractFirstJsonObject(text) {
  const src = String(text || "");
  const i = src.indexOf("{");
  const j = src.lastIndexOf("}");
  if (i < 0 || j <= i) return null;
  try {
    return JSON.parse(src.slice(i, j + 1));
  } catch {
    return null;
  }
}

function parseResponseText(data) {
  if (!data) return "";
  if (typeof data.output_text === "string" && data.output_text.trim()) return data.output_text.trim();
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

function cameraFreshPenalty(obs) {
  const age = toNum(obs.ageMinutes, 0);
  return clamp(1 - age / 25, 0, 1);
}

function observationPriority(obs) {
  const risk = Math.max(toNum(obs.imageScore), toNum(obs.proxyScore), toNum(obs.queueStopScore));
  const freshness = cameraFreshPenalty(obs);
  const localSupport = clamp(toNum(obs.localSegmentCount, 0) / 6, 0, 1);
  return risk * 0.6 + freshness * 30 + localSupport * 20;
}

function pickTargets(observations, cfg = {}) {
  const enabled = Number(cfg.cv_enabled ?? 0) >= 0.5;
  if (!enabled) return [];
  const withImage = (observations || []).filter((o) => String(o.snapshotUrl || "").trim());
  if (!withImage.length) return [];
  const maxPerPoll = Math.max(0, Math.floor(Number(cfg.cv_max_cameras_per_poll ?? 10)));
  if (maxPerPoll <= 0) return [];
  const coveragePct = clamp(Number(cfg.cv_target_coverage_pct ?? 55), 0, 100);
  const targetByCoverage = Math.ceil((coveragePct / 100) * withImage.length);
  const target = Math.min(withImage.length, Math.max(1, Math.min(maxPerPoll, targetByCoverage)));
  return [...withImage]
    .sort((a, b) => observationPriority(b) - observationPriority(a))
    .slice(0, target);
}

function normalizeCvJson(parsed) {
  const p = parsed || {};
  const sceneConfidenceRaw = p.scene_confidence ?? p.sceneConfidence;
  const visibilityQualityRaw = p.visibility_quality ?? p.visibilityQuality;
  const laneOccRaw = p.lane_occupancy_pct ?? p.laneOccupancyPct;
  const queueRaw = p.queue_index ?? p.queueIndex;
  const vehRaw = p.vehicle_count ?? p.vehicleCount;
  const stoppedRaw = p.stopped_vehicle_count ?? p.stoppedVehicleCount;
  return {
    vehicleCount: Math.max(0, Math.round(toNum(vehRaw, 0))),
    stoppedVehicleCount: Math.max(0, Math.round(toNum(stoppedRaw, 0))),
    queueIndex: clamp(toNum(queueRaw, 0), 0, 100),
    laneOccupancyPct: clamp(toNum(laneOccRaw, 0), 0, 100),
    visibilityQuality: clamp(toNum(visibilityQualityRaw, 0.5), 0, 1),
    sceneConfidence: clamp(toNum(sceneConfidenceRaw, 0.5), 0, 1),
    notes: String(p.notes || "")
  };
}

async function callOpenAIVision(snapshotUrl, cfg = {}) {
  const key = process.env.OPENAI_API_KEY;
  if (!key) throw new Error("OPENAI_API_KEY missing");
  const model = process.env.CV_OPENAI_MODEL || process.env.OPENAI_MODEL || "gpt-5.2";
  const prompt =
    "You are a strict traffic camera CV parser. Inspect this roadway image and return only JSON with keys: " +
    "vehicle_count (integer), stopped_vehicle_count (integer), queue_index (0-100), lane_occupancy_pct (0-100), " +
    "visibility_quality (0-1), scene_confidence (0-1), notes (short). No markdown.";
  const timeoutMs = Math.max(2500, Number(cfg.cv_timeout_ms ?? 9000));

  const req = fetch("https://api.openai.com/v1/responses", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${key}`
    },
    body: JSON.stringify({
      model,
      input: [
        {
          role: "user",
          content: [
            { type: "input_text", text: prompt },
            { type: "input_image", image_url: snapshotUrl }
          ]
        }
      ]
    })
  });

  const res = await withTimeout(req, timeoutMs);
  if (!res.ok) {
    const err = await res.text();
    throw new Error(`cv_openai_${res.status}:${err.slice(0, 300)}`);
  }
  const data = await res.json();
  const txt = parseResponseText(data);
  const parsed = extractFirstJsonObject(txt);
  if (!parsed) throw new Error("cv_openai_parse_failed");
  return normalizeCvJson(parsed);
}

async function callHttpVision(snapshotUrl, cfg = {}) {
  const endpoint = process.env.CV_HTTP_ENDPOINT || "";
  if (!endpoint) throw new Error("CV_HTTP_ENDPOINT missing");
  const token = process.env.CV_HTTP_TOKEN || "";
  const timeoutMs = Math.max(2500, Number(cfg.cv_timeout_ms ?? 9000));
  const req = fetch(endpoint, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      ...(token ? { Authorization: `Bearer ${token}` } : {})
    },
    body: JSON.stringify({ image_url: snapshotUrl })
  });
  const res = await withTimeout(req, timeoutMs);
  if (!res.ok) {
    const err = await res.text();
    throw new Error(`cv_http_${res.status}:${err.slice(0, 300)}`);
  }
  const data = await res.json();
  return normalizeCvJson(data);
}

function providerName(cfg = {}) {
  const mode = Number(cfg.cv_provider_mode ?? 1);
  if (mode === 2) return "http_json";
  if (mode === 1) return "openai_vision";
  return "disabled";
}

function calcCvQueueScore(metrics, prevState, cfg = {}) {
  const stopped = toNum(metrics.stoppedVehicleCount);
  const vehicles = Math.max(1, toNum(metrics.vehicleCount));
  const stopRatio = clamp((stopped / vehicles) * 100, 0, 100);
  const queue = clamp(toNum(metrics.queueIndex), 0, 100);
  const occ = clamp(toNum(metrics.laneOccupancyPct), 0, 100);
  const alpha = clamp(Number(cfg.cv_temporal_alpha ?? 0.55), 0, 1);
  const persistBonus = clamp(Number(cfg.cv_stopped_persist_bonus ?? 8), 0, 40);

  const raw = clamp(queue * 0.55 + stopRatio * 0.3 + occ * 0.15, 0, 100);
  const prevQ = prevState ? clamp(toNum(prevState.last_queue_index), 0, 100) : raw;
  let smooth = alpha * raw + (1 - alpha) * prevQ;
  if (prevState && toNum(prevState.last_stopped_count) > 0 && stopped > 0) {
    smooth = clamp(smooth + persistBonus, 0, 100);
  }
  return smooth;
}

function blendScores(baseQueueStop, cvQueueScore, cvConfidence, cfg = {}) {
  const minConf = clamp(Number(cfg.cv_min_confidence ?? 0.45), 0, 1);
  const strength = clamp(Number(cfg.cv_blend_strength ?? 0.72), 0, 1);
  const usable = cvConfidence <= minConf ? 0 : (cvConfidence - minConf) / Math.max(0.05, 1 - minConf);
  const w = clamp(usable * strength, 0, 1);
  return {
    blended: clamp((1 - w) * baseQueueStop + w * cvQueueScore, 0, 100),
    weight: w
  };
}

export async function analyzeCvForObservations(observations, runtimeSettings, prevStateByCamera = new Map()) {
  const provider = providerName(runtimeSettings);
  const targets = pickTargets(observations, runtimeSettings);
  if (provider === "disabled" || !targets.length) {
    return {
      enabled: false,
      provider,
      analyzedCount: 0,
      coveragePct: 0,
      avgConfidence: 0,
      byCamera: new Map(),
      errors: []
    };
  }

  const byCamera = new Map();
  const errors = [];
  for (const obs of targets) {
    try {
      const metrics =
        provider === "http_json"
          ? await callHttpVision(obs.snapshotUrl, runtimeSettings)
          : await callOpenAIVision(obs.snapshotUrl, runtimeSettings);
      const prevState = prevStateByCamera.get(Number(obs.cameraObjectId));
      const cvQueueScore = calcCvQueueScore(metrics, prevState, runtimeSettings);
      const conf = clamp(toNum(metrics.sceneConfidence, 0.5), 0, 1);
      const blended = blendScores(toNum(obs.queueStopScore), cvQueueScore, conf, runtimeSettings);
      byCamera.set(Number(obs.cameraObjectId), {
        provider,
        vehicleCount: toNum(metrics.vehicleCount),
        stoppedVehicleCount: toNum(metrics.stoppedVehicleCount),
        laneOccupancyPct: toNum(metrics.laneOccupancyPct),
        visibilityQuality: toNum(metrics.visibilityQuality, 0.5),
        sceneConfidence: conf,
        cvQueueScore,
        blendedQueueStopScore: blended.blended,
        blendWeight: blended.weight,
        notes: String(metrics.notes || "")
      });
    } catch (e) {
      errors.push({
        cameraObjectId: Number(obs.cameraObjectId),
        error: String(e.message || e)
      });
    }
  }

  const analyzedCount = byCamera.size;
  const coveragePct = observations.length ? (analyzedCount * 100) / observations.length : 0;
  const avgConfidence =
    analyzedCount > 0
      ? [...byCamera.values()].reduce((sum, r) => sum + toNum(r.sceneConfidence), 0) / analyzedCount
      : 0;

  return {
    enabled: true,
    provider,
    analyzedCount,
    coveragePct,
    avgConfidence,
    byCamera,
    errors
  };
}
