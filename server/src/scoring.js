function clamp(v, min = 0, max = 100) {
  return Math.max(min, Math.min(max, v));
}

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

export function incidentComponent(incidents) {
  return clamp(28 * (1 - Math.exp(-incidents / 35)), 0, 28);
}

export function closureComponent(closures) {
  return clamp(20 * (1 - Math.exp(-closures / 3)), 0, 20);
}

export function weatherComponentFromRisk(weatherRiskScore, cfg = {}) {
  const maxComponent = Number(cfg.weather_component_max ?? 18);
  const exponent = Number(cfg.weather_component_exponent ?? 1.05);
  const normalized = clamp(Number(weatherRiskScore || 0) / 100, 0, 1);
  return clamp(maxComponent * normalized ** exponent, 0, maxComponent);
}

export function computeFused({ trafficSegments, delayPct, imageScore, incidents, closures, weatherRiskScore }, cfg = {}) {
  const weightTraffic = Number(cfg.weight_traffic ?? 0.5);
  const weightImageWithTraffic = Number(cfg.weight_image_with_traffic ?? 0.3);
  const weightImageNoTraffic = Number(cfg.weight_image_no_traffic ?? 0.55);
  const incMax = Number(cfg.incident_saturation_max ?? 28);
  const incK = Number(cfg.incident_saturation_k ?? 35);
  const cloMax = Number(cfg.closure_saturation_max ?? 20);
  const cloK = Number(cfg.closure_saturation_k ?? 3);
  const weatherComp = weatherComponentFromRisk(weatherRiskScore, cfg);

  const trafficComponent = trafficSegments > 0 ? weightTraffic * delayPct : 0;
  const imageComponent = trafficSegments > 0 ? weightImageWithTraffic * imageScore : weightImageNoTraffic * imageScore;
  const incidentComp = clamp(incMax * (1 - Math.exp(-incidents / incK)), 0, incMax);
  const closureComp = clamp(cloMax * (1 - Math.exp(-closures / cloK)), 0, cloMax);
  const fused = clamp(trafficComponent + imageComponent + incidentComp + closureComp + weatherComp);
  return {
    trafficComponent,
    imageComponent,
    incidentComponent: incidentComp,
    closureComponent: closureComp,
    weatherComponent: weatherComp,
    fused
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
