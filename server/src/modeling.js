function clamp(v, lo = 0, hi = 100) {
  return Math.max(lo, Math.min(hi, v));
}

function mean(arr) {
  if (!arr.length) return 0;
  return arr.reduce((s, x) => s + x, 0) / arr.length;
}

function std(arr, m = mean(arr)) {
  if (arr.length < 2) return 1;
  const v = arr.reduce((s, x) => s + (x - m) ** 2, 0) / (arr.length - 1);
  return Math.sqrt(v) || 1;
}

function median(arr) {
  if (!arr.length) return 0;
  const s = [...arr].sort((a, b) => a - b);
  const mid = Math.floor(s.length / 2);
  return s.length % 2 ? s[mid] : (s[mid - 1] + s[mid]) / 2;
}

function mad(arr, med = median(arr)) {
  return median(arr.map((x) => Math.abs(x - med)));
}

function hourCyclic(date) {
  const h = date.getUTCHours();
  const ang = (2 * Math.PI * h) / 24;
  return { sin: Math.sin(ang), cos: Math.cos(ang) };
}

export function featureVector(runLike, date = new Date()) {
  const cyc = hourCyclic(date);
  return [
    1,
    Number(runLike.fused_score || 0),
    Number(runLike.image_score || 0),
    Number(runLike.incidents_count || 0),
    Number(runLike.closures_count || 0),
    Number(runLike.camera_fresh_pct || 0),
    cyc.sin,
    cyc.cos
  ];
}

function solveLinearSystem(A, b) {
  const n = A.length;
  const M = A.map((row, i) => [...row, b[i]]);
  for (let i = 0; i < n; i++) {
    let maxRow = i;
    for (let k = i + 1; k < n; k++) {
      if (Math.abs(M[k][i]) > Math.abs(M[maxRow][i])) maxRow = k;
    }
    [M[i], M[maxRow]] = [M[maxRow], M[i]];
    if (Math.abs(M[i][i]) < 1e-10) continue;
    for (let k = i + 1; k < n; k++) {
      const f = M[k][i] / M[i][i];
      for (let j = i; j <= n; j++) M[k][j] -= f * M[i][j];
    }
  }
  const x = new Array(n).fill(0);
  for (let i = n - 1; i >= 0; i--) {
    let s = M[i][n];
    for (let j = i + 1; j < n; j++) s -= M[i][j] * x[j];
    x[i] = Math.abs(M[i][i]) < 1e-10 ? 0 : s / M[i][i];
  }
  return x;
}

function ridgeFit(X, y, lambda = 2.0) {
  const n = X.length;
  const d = X[0].length;
  const XtX = Array.from({ length: d }, () => Array(d).fill(0));
  const Xty = Array(d).fill(0);
  for (let i = 0; i < n; i++) {
    for (let j = 0; j < d; j++) {
      Xty[j] += X[i][j] * y[i];
      for (let k = 0; k < d; k++) XtX[j][k] += X[i][j] * X[i][k];
    }
  }
  for (let j = 0; j < d; j++) XtX[j][j] += lambda;
  return solveLinearSystem(XtX, Xty);
}

function dot(a, b) {
  let s = 0;
  for (let i = 0; i < a.length; i++) s += a[i] * b[i];
  return s;
}

export function trainNextScoreModel(runs, cfg = {}) {
  const minRows = Number(cfg.model_min_train_rows ?? 30);
  const ridgeLambda = Number(cfg.model_ridge_lambda ?? 2.5);
  if (!runs || runs.length < minRows) return null;
  const rows = [...runs].sort((a, b) => new Date(a.run_ts) - new Date(b.run_ts));
  const X = [];
  const y = [];
  for (let i = 0; i < rows.length - 1; i++) {
    X.push(featureVector(rows[i], new Date(rows[i].run_ts)));
    y.push(Number(rows[i + 1].fused_score || 0));
  }
  const beta = ridgeFit(X, y, ridgeLambda);
  const preds = X.map((x) => clamp(dot(beta, x)));
  const residuals = preds.map((p, i) => y[i] - p);
  const yMean = mean(y);
  const ssRes = residuals.reduce((s, r) => s + r * r, 0);
  const ssTot = y.reduce((s, v) => s + (v - yMean) ** 2, 0) || 1;
  const r2 = 1 - ssRes / ssTot;
  const sigma = 1.4826 * mad(residuals);

  const cols = X[0].length;
  const mu = Array(cols).fill(0);
  const sd = Array(cols).fill(1);
  for (let j = 0; j < cols; j++) {
    const col = X.map((r) => r[j]);
    mu[j] = mean(col);
    sd[j] = std(col, mu[j]);
  }

  return {
    version: "ridge_v2_nextscore",
    beta,
    sigma: sigma || 8,
    r2: Number.isFinite(r2) ? r2 : 0,
    trainRows: X.length,
    featureStats: { mu, sd }
  };
}

export function predictNextScore(model, runLike, cfg = {}) {
  if (!model) {
    const base = Number(runLike.fused_score || 0);
    return {
      p50: base,
      p90: clamp(base + 12),
      confidence: 35,
      driftScore: 1
    };
  }
  const x = featureVector(runLike, new Date());
  const raw = dot(model.beta, x);
  const p50 = clamp(raw);
  const p90Z = Number(cfg.model_p90_z ?? 1.2816);
  const p90 = clamp(p50 + p90Z * model.sigma);

  const zScores = x.map((v, i) => Math.abs((v - model.featureStats.mu[i]) / (model.featureStats.sd[i] || 1)));
  const driftNormDivisor = Number(cfg.model_drift_norm_divisor ?? 3.5);
  const confSigmaFactor = Number(cfg.model_conf_sigma_factor ?? 2.2);
  const confDriftFactor = Number(cfg.model_conf_drift_factor ?? 35);
  const driftScore = mean(zScores.slice(1)) / driftNormDivisor;
  const confidence = clamp(100 - model.sigma * confSigmaFactor - driftScore * confDriftFactor, 5, 99);

  return {
    p50,
    p90,
    confidence,
    driftScore: clamp(driftScore * 100, 0, 100)
  };
}
