function clamp(v, min = 0, max = 100) {
  return Math.max(min, Math.min(max, v));
}

const COMPONENT_KEYS = ["traffic", "image", "incident", "closure", "weather"];

export function p75(values) {
  if (!values.length) return 0;
  const sorted = [...values].sort((a, b) => a - b);
  const i = Math.max(0, Math.ceil(sorted.length * 0.75) - 1);
  return sorted[i];
}

export function median(values) {
  if (!values.length) return 0;
  const sorted = [...values].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
}

export function imageScoreFromBaseline(snapshotBytes, ewmaBytes) {
  if (!ewmaBytes || ewmaBytes <= 0) return 50;
  const z = (snapshotBytes - ewmaBytes) / ewmaBytes;
  return clamp(50 + 120 * z);
}

export function incidentComponent(incidents, cfg = {}) {
  const incMax = Number(cfg.incident_saturation_max ?? 28);
  const incK = Number(cfg.incident_saturation_k ?? 35);
  return clamp(incMax * (1 - Math.exp(-Math.max(0, Number(incidents || 0)) / Math.max(1, incK))), 0, incMax);
}

export function closureComponent(closures, cfg = {}) {
  const cloMax = Number(cfg.closure_saturation_max ?? 20);
  const cloK = Number(cfg.closure_saturation_k ?? 3);
  return clamp(cloMax * (1 - Math.exp(-Math.max(0, Number(closures || 0)) / Math.max(0.5, cloK))), 0, cloMax);
}

export function weatherComponentFromRisk(weatherRiskScore, cfg = {}) {
  const maxComponent = Number(cfg.weather_component_max ?? 18);
  const exponent = Number(cfg.weather_component_exponent ?? 1.05);
  const normalized = clamp(Number(weatherRiskScore || 0) / 100, 0, 1);
  return clamp(maxComponent * normalized ** exponent, 0, maxComponent);
}

function clockChicagoParts(ts = new Date()) {
  const d = ts instanceof Date ? ts : new Date(ts);
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone: "America/Chicago",
    hour: "2-digit",
    weekday: "short",
    hour12: false
  }).formatToParts(d);
  const hour = Number(parts.find((p) => p.type === "hour")?.value || 0);
  const weekday = String(parts.find((p) => p.type === "weekday")?.value || "Mon");
  return { hour, weekday };
}

function inferRegime({ referenceTs, weatherRiskScore }) {
  const { hour, weekday } = clockChicagoParts(referenceTs || new Date());
  const isWeekend = weekday === "Sat" || weekday === "Sun";
  if (Number(weatherRiskScore || 0) >= 65) return "severe_weather";
  if (isWeekend) return "weekend";
  if (hour < 5 || hour >= 22) return "overnight";
  if ((hour >= 6 && hour <= 9) || (hour >= 15 && hour <= 19)) return "rush";
  return "normal";
}

function profileMap(regime, cfg = {}) {
  if (regime === "rush") {
    return {
      traffic: Number(cfg.regime_rush_traffic ?? 1.25),
      image: Number(cfg.regime_rush_image ?? 1.05),
      incident: Number(cfg.regime_rush_incident ?? 1.1),
      closure: Number(cfg.regime_rush_closure ?? 1.1),
      weather: Number(cfg.regime_rush_weather ?? 0.95)
    };
  }
  if (regime === "overnight") {
    return {
      traffic: Number(cfg.regime_overnight_traffic ?? 0.7),
      image: Number(cfg.regime_overnight_image ?? 1.2),
      incident: Number(cfg.regime_overnight_incident ?? 1.0),
      closure: Number(cfg.regime_overnight_closure ?? 1.0),
      weather: Number(cfg.regime_overnight_weather ?? 1.1)
    };
  }
  if (regime === "weekend") {
    return {
      traffic: Number(cfg.regime_weekend_traffic ?? 0.85),
      image: Number(cfg.regime_weekend_image ?? 1.1),
      incident: Number(cfg.regime_weekend_incident ?? 0.95),
      closure: Number(cfg.regime_weekend_closure ?? 1.0),
      weather: Number(cfg.regime_weekend_weather ?? 1.05)
    };
  }
  if (regime === "severe_weather") {
    return {
      traffic: Number(cfg.regime_severe_weather_traffic ?? 0.9),
      image: Number(cfg.regime_severe_weather_image ?? 0.9),
      incident: Number(cfg.regime_severe_weather_incident ?? 1.05),
      closure: Number(cfg.regime_severe_weather_closure ?? 1.1),
      weather: Number(cfg.regime_severe_weather_weather ?? 1.5)
    };
  }
  return { traffic: 1, image: 1, incident: 1, closure: 1, weather: 1 };
}

function reliabilityMap(
  { trafficSegments, weatherRiskScore },
  { cameraFreshPct, cameraCountUsed, trafficFeedOk = true, incidentFeedOk = true, closureFeedOk = true, weatherAvailable = true },
  cfg = {}
) {
  const gamma = Math.max(0.2, Number(cfg.reliability_gamma ?? 1.15));
  const coverageRef = Math.max(1, Number(cfg.camera_coverage_ref ?? 10));
  const cameraCoverage = clamp(Number(cameraCountUsed || 0) / coverageRef, 0, 1);
  const freshNorm = clamp(Number(cameraFreshPct || 0) / 100, 0, 1);
  const cameraSignalQuality = Math.max(0.05, freshNorm * Math.max(0.2, cameraCoverage));
  const trafficMissingPenalty = clamp(Number(cfg.traffic_missing_penalty ?? 0.35), 0, 1);
  const weatherMissingPenalty = clamp(Number(cfg.weather_missing_penalty ?? 0.45), 0, 1);

  return {
    traffic: trafficFeedOk ? (Number(trafficSegments || 0) > 0 ? 1 : trafficMissingPenalty) : trafficMissingPenalty,
    image: clamp(cameraSignalQuality ** gamma, 0.08, 1),
    incident: incidentFeedOk ? 1 : 0.55,
    closure: closureFeedOk ? 1 : 0.55,
    weather: weatherAvailable || Number(weatherRiskScore || 0) > 0 ? 1 : weatherMissingPenalty
  };
}

function learnedMap(learned, cfg = {}) {
  const strength = clamp(Number(cfg.auto_learn_strength ?? 0.55), 0, 1);
  const out = { traffic: 1, image: 1, incident: 1, closure: 1, weather: 1 };
  if (!learned || typeof learned !== "object") return out;
  for (const key of COMPONENT_KEYS) {
    const raw = Number(learned[key] ?? 1);
    if (!Number.isFinite(raw)) continue;
    out[key] = 1 + (raw - 1) * strength;
  }
  return out;
}

function lockedMap(cfg = {}) {
  const out = {};
  for (const key of COMPONENT_KEYS) {
    const lock = Number(cfg[`lock_mult_${key}`] ?? 0) >= 0.5;
    if (!lock) continue;
    out[key] = Number(cfg[`locked_mult_${key}`] ?? 1);
  }
  return out;
}

function combineMultipliers(profile, reliability, learned, locked, cfg = {}) {
  const floor = Number(cfg.dynamic_weight_floor ?? 0.45);
  const cap = Number(cfg.dynamic_weight_cap ?? 1.9);
  const out = {};
  for (const key of COMPONENT_KEYS) {
    const p = Number(profile[key] ?? 1);
    const r = Number(reliability[key] ?? 1);
    const l = Number(learned[key] ?? 1);
    const raw = Number.isFinite(locked[key]) ? Number(locked[key]) : p * r * l;
    out[key] = clamp(raw, floor, cap);
  }
  return out;
}

function scoreShares(components, fused) {
  const denom = Math.max(0.001, Number(fused || 0));
  return {
    traffic: clamp((Number(components.traffic || 0) / denom) * 100, 0, 100),
    image: clamp((Number(components.image || 0) / denom) * 100, 0, 100),
    incident: clamp((Number(components.incident || 0) / denom) * 100, 0, 100),
    closure: clamp((Number(components.closure || 0) / denom) * 100, 0, 100),
    weather: clamp((Number(components.weather || 0) / denom) * 100, 0, 100)
  };
}

export function computeFused(
  { trafficSegments, delayPct, imageScore, incidents, closures, weatherRiskScore },
  cfg = {},
  dynamicContext = {}
) {
  const weightTraffic = Number(cfg.weight_traffic ?? 0.5);
  const weightImageWithTraffic = Number(cfg.weight_image_with_traffic ?? 0.3);
  const weightImageNoTraffic = Number(cfg.weight_image_no_traffic ?? 0.55);

  const baseComponents = {
    traffic: trafficSegments > 0 ? weightTraffic * Number(delayPct || 0) : 0,
    image:
      trafficSegments > 0
        ? weightImageWithTraffic * Number(imageScore || 0)
        : weightImageNoTraffic * Number(imageScore || 0),
    incident: incidentComponent(incidents, cfg),
    closure: closureComponent(closures, cfg),
    weather: weatherComponentFromRisk(weatherRiskScore, cfg)
  };

  const dynamicEnabled = Number(cfg.dynamic_weights_enabled ?? 0) >= 0.5;
  const regime = inferRegime({
    referenceTs: dynamicContext.referenceTs || new Date(),
    weatherRiskScore: Number(weatherRiskScore || 0)
  });
  const profile = dynamicEnabled ? profileMap(regime, cfg) : profileMap("normal", cfg);
  const reliability = dynamicEnabled
    ? reliabilityMap(
        { trafficSegments, weatherRiskScore },
        {
          cameraFreshPct: Number(dynamicContext.cameraFreshPct ?? 100),
          cameraCountUsed: Number(dynamicContext.cameraCountUsed ?? 0),
          trafficFeedOk: dynamicContext.trafficFeedOk !== false,
          incidentFeedOk: dynamicContext.incidentFeedOk !== false,
          closureFeedOk: dynamicContext.closureFeedOk !== false,
          weatherAvailable: dynamicContext.weatherAvailable !== false
        },
        cfg
      )
    : { traffic: 1, image: 1, incident: 1, closure: 1, weather: 1 };
  const learned = dynamicEnabled ? learnedMap(dynamicContext.learnedMultipliers, cfg) : learnedMap(null, cfg);
  const locked = lockedMap(cfg);
  const multipliers = combineMultipliers(profile, reliability, learned, locked, cfg);

  const trafficComponent = baseComponents.traffic * multipliers.traffic;
  const imageComponent = baseComponents.image * multipliers.image;
  const incidentComp = baseComponents.incident * multipliers.incident;
  const closureComp = baseComponents.closure * multipliers.closure;
  const weatherComp = baseComponents.weather * multipliers.weather;
  const fused = clamp(trafficComponent + imageComponent + incidentComp + closureComp + weatherComp);

  const weighted = {
    traffic: trafficComponent,
    image: imageComponent,
    incident: incidentComp,
    closure: closureComp,
    weather: weatherComp
  };

  return {
    trafficComponent,
    imageComponent,
    incidentComponent: incidentComp,
    closureComponent: closureComp,
    weatherComponent: weatherComp,
    fused,
    weighting: {
      dynamic_enabled: dynamicEnabled,
      regime,
      profile_multipliers: profile,
      reliability_multipliers: reliability,
      learned_multipliers: learned,
      locked_multipliers: locked,
      effective_multipliers: multipliers,
      base_components: baseComponents,
      weighted_components: weighted,
      weighted_shares_pct: scoreShares(weighted, fused)
    }
  };
}

export function detectAlert({ fusedScore, freshPct, prevMedian, prevCount }, cfg = {}) {
  const highThreshold = Number(cfg.alert_high_score_threshold ?? 70);
  const spikeDelta = Number(cfg.alert_spike_delta ?? 15);
  const spikeMinScore = Number(cfg.alert_spike_min_score ?? 40);
  const freshnessMinPct = Number(cfg.alert_freshness_min_pct ?? 70);

  if (fusedScore >= highThreshold) return { state: "HIGH", reason: `fused_score>=${highThreshold}` };
  if (prevCount >= 6 && fusedScore - prevMedian >= spikeDelta && fusedScore >= spikeMinScore) {
    return { state: "SPIKE", reason: "fused jump >=15 over rolling median" };
  }
  if (freshPct < freshnessMinPct) return { state: "DATA_STALE", reason: `camera freshness below ${freshnessMinPct}%` };
  return { state: "NORMAL", reason: "" };
}

function solveLinearSystem(aIn, bIn) {
  const n = aIn.length;
  const a = aIn.map((row) => [...row]);
  const b = [...bIn];

  for (let col = 0; col < n; col++) {
    let pivot = col;
    for (let r = col + 1; r < n; r++) {
      if (Math.abs(a[r][col]) > Math.abs(a[pivot][col])) pivot = r;
    }
    if (Math.abs(a[pivot][col]) < 1e-9) return null;
    if (pivot !== col) {
      [a[col], a[pivot]] = [a[pivot], a[col]];
      [b[col], b[pivot]] = [b[pivot], b[col]];
    }
    const diag = a[col][col];
    for (let j = col; j < n; j++) a[col][j] /= diag;
    b[col] /= diag;
    for (let r = 0; r < n; r++) {
      if (r === col) continue;
      const f = a[r][col];
      if (Math.abs(f) < 1e-12) continue;
      for (let j = col; j < n; j++) a[r][j] -= f * a[col][j];
      b[r] -= f * b[col];
    }
  }
  return b;
}

export function trainDynamicComponentMultipliers(samples = [], cfg = {}) {
  const rows = Array.isArray(samples) ? samples : [];
  if (!rows.length) {
    return {
      multipliers: { traffic: 1, image: 1, incident: 1, closure: 1, weather: 1 },
      fit: { rows: 0, r2: 0, rmse: 0 }
    };
  }

  const x = [];
  const y = [];
  for (const s of rows) {
    const traffic = Number(s.traffic_component ?? s.traffic ?? 0) / 100;
    const image = Number(s.image_component ?? s.image ?? 0) / 100;
    const incident = Number(s.incident_component ?? s.incident ?? 0) / 100;
    const closure = Number(s.closure_component ?? s.closure ?? 0) / 100;
    const weather = Number(s.weather_component ?? s.weather ?? 0) / 100;
    const target = Number(s.target_fused ?? s.target ?? s.next_fused_score ?? 0) / 100;
    if (![traffic, image, incident, closure, weather, target].every((n) => Number.isFinite(n))) continue;
    x.push([traffic, image, incident, closure, weather]);
    y.push(target);
  }
  const n = y.length;
  if (n < 8) {
    return {
      multipliers: { traffic: 1, image: 1, incident: 1, closure: 1, weather: 1 },
      fit: { rows: n, r2: 0, rmse: 0 }
    };
  }

  const k = 5;
  const lambda = Math.max(0.01, Number(cfg.auto_learn_ridge_lambda ?? 1.8));
  const a = Array.from({ length: k }, () => Array.from({ length: k }, () => 0));
  const b = Array.from({ length: k }, () => 0);
  for (let i = 0; i < n; i++) {
    const row = x[i];
    for (let p = 0; p < k; p++) {
      b[p] += row[p] * y[i];
      for (let q = 0; q < k; q++) {
        a[p][q] += row[p] * row[q];
      }
    }
  }
  for (let i = 0; i < k; i++) a[i][i] += lambda;

  const beta = solveLinearSystem(a, b);
  if (!beta) {
    return {
      multipliers: { traffic: 1, image: 1, incident: 1, closure: 1, weather: 1 },
      fit: { rows: n, r2: 0, rmse: 0, failed: true }
    };
  }

  const clippedBeta = beta.map((v) => Math.max(0, Number(v || 0)));
  const betaMean = clippedBeta.reduce((sum, v) => sum + v, 0) / clippedBeta.length;
  if (betaMean <= 1e-9) {
    return {
      multipliers: { traffic: 1, image: 1, incident: 1, closure: 1, weather: 1 },
      fit: { rows: n, r2: 0, rmse: 0 }
    };
  }

  const floor = Number(cfg.dynamic_weight_floor ?? 0.45);
  const cap = Number(cfg.dynamic_weight_cap ?? 1.9);
  const multipliers = {
    traffic: clamp(clippedBeta[0] / betaMean, floor, cap),
    image: clamp(clippedBeta[1] / betaMean, floor, cap),
    incident: clamp(clippedBeta[2] / betaMean, floor, cap),
    closure: clamp(clippedBeta[3] / betaMean, floor, cap),
    weather: clamp(clippedBeta[4] / betaMean, floor, cap)
  };

  let ssRes = 0;
  let ssTot = 0;
  const yMean = y.reduce((sum, v) => sum + v, 0) / y.length;
  for (let i = 0; i < y.length; i++) {
    const pred =
      clippedBeta[0] * x[i][0] +
      clippedBeta[1] * x[i][1] +
      clippedBeta[2] * x[i][2] +
      clippedBeta[3] * x[i][3] +
      clippedBeta[4] * x[i][4];
    const e = y[i] - pred;
    ssRes += e * e;
    const d = y[i] - yMean;
    ssTot += d * d;
  }
  const r2 = ssTot <= 1e-9 ? 0 : 1 - ssRes / ssTot;
  const rmse = Math.sqrt(ssRes / Math.max(1, y.length));

  return {
    multipliers,
    fit: {
      rows: y.length,
      r2,
      rmse,
      beta: clippedBeta
    }
  };
}
