import { pool } from "./db.js";

export const DEFAULT_SETTINGS = {
  poll_seconds: 300,
  sample_limit: 40,
  baseline_alpha: 0.2,
  min_scoped_cameras_for_strict: 8,

  weight_traffic: 0.5,
  weight_image_with_traffic: 0.3,
  weight_image_no_traffic: 0.55,
  incident_saturation_max: 28,
  incident_saturation_k: 35,
  closure_saturation_max: 20,
  closure_saturation_k: 3,

  alert_high_score_threshold: 70,
  alert_spike_min_score: 40,
  alert_spike_delta: 15,
  alert_freshness_min_pct: 70,

  model_min_train_rows: 30,
  model_ridge_lambda: 2.5,
  model_p90_z: 1.2816,
  model_conf_sigma_factor: 2.2,
  model_conf_drift_factor: 35,
  model_drift_norm_divisor: 3.5
};

const BOUNDS = {
  poll_seconds: [30, 3600],
  sample_limit: [5, 300],
  baseline_alpha: [0.01, 0.95],
  min_scoped_cameras_for_strict: [0, 200],
  weight_traffic: [0, 1],
  weight_image_with_traffic: [0, 1],
  weight_image_no_traffic: [0, 1],
  incident_saturation_max: [0, 100],
  incident_saturation_k: [1, 500],
  closure_saturation_max: [0, 100],
  closure_saturation_k: [0.5, 100],
  alert_high_score_threshold: [1, 100],
  alert_spike_min_score: [1, 100],
  alert_spike_delta: [1, 100],
  alert_freshness_min_pct: [1, 100],
  model_min_train_rows: [10, 5000],
  model_ridge_lambda: [0.01, 100],
  model_p90_z: [0.1, 5],
  model_conf_sigma_factor: [0.1, 10],
  model_conf_drift_factor: [1, 100],
  model_drift_norm_divisor: [0.5, 20]
};

function clamp(v, lo, hi) {
  return Math.max(lo, Math.min(hi, v));
}

export function sanitizeSettings(input = {}) {
  const out = { ...DEFAULT_SETTINGS };
  for (const [k, v] of Object.entries(input || {})) {
    if (!(k in DEFAULT_SETTINGS)) continue;
    const n = Number(v);
    if (!Number.isFinite(n)) continue;
    const [lo, hi] = BOUNDS[k];
    out[k] = clamp(n, lo, hi);
  }
  return out;
}

export async function getRuntimeSettings() {
  const { rows } = await pool.query(`SELECT value FROM app_state WHERE key = 'runtime_settings'`);
  if (!rows.length) return { ...DEFAULT_SETTINGS };
  return sanitizeSettings(rows[0].value || {});
}

export async function upsertRuntimeSettings(partial) {
  const current = await getRuntimeSettings();
  const merged = sanitizeSettings({ ...current, ...(partial || {}) });
  await pool.query(
    `INSERT INTO app_state(key, value, updated_at)
     VALUES('runtime_settings', $1::jsonb, NOW())
     ON CONFLICT(key) DO UPDATE
       SET value = EXCLUDED.value,
           updated_at = NOW()`,
    [JSON.stringify(merged)]
  );
  return merged;
}
