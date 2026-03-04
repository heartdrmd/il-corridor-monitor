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

export function computeFused({ trafficSegments, delayPct, imageScore, incidents, closures }) {
  const trafficComponent = trafficSegments > 0 ? 0.5 * delayPct : 0;
  const imageComponent = trafficSegments > 0 ? 0.3 * imageScore : 0.55 * imageScore;
  const incidentComp = incidentComponent(incidents);
  const closureComp = closureComponent(closures);
  const fused = clamp(trafficComponent + imageComponent + incidentComp + closureComp);
  return {
    trafficComponent,
    imageComponent,
    incidentComponent: incidentComp,
    closureComponent: closureComp,
    fused
  };
}

export function detectAlert({ fusedScore, freshPct, prevMedian, prevCount }) {
  if (fusedScore >= 70) return { state: "HIGH", reason: "fused_score>=70" };
  if (prevCount >= 6 && fusedScore - prevMedian >= 15 && fusedScore >= 40) {
    return { state: "SPIKE", reason: "fused jump >=15 over rolling median" };
  }
  if (freshPct < 70) return { state: "DATA_STALE", reason: "camera freshness below 70%" };
  return { state: "NORMAL", reason: "" };
}

