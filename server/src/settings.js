import { pool } from "./db.js";

export const DEFAULT_SETTINGS = {
  poll_seconds: 300,
  sample_limit: 40,
  sample_limit_max: 140,
  baseline_alpha: 0.2,
  min_scoped_cameras_for_strict: 8,
  adaptive_camera_sampling_enabled: 1,
  adaptive_sample_min: 20,
  adaptive_sample_max: 140,
  adaptive_conf_k: 24,
  adaptive_conf_gain_threshold: 0.012,
  camera_hard_drop_enabled: 1,
  camera_drop_too_old_flag: 1,
  camera_hard_max_age_minutes: 16,
  camera_missing_snapshot_penalty: 0.35,
  camera_score_weight_fresh: 0.55,
  camera_score_weight_proximity: 0.3,
  camera_score_weight_scope: 0.15,
  camera_non_scoped_penalty: 0.25,
  camera_fresh_ref_minutes: 5,
  camera_proximity_scale_km: 14,
  camera_diversity_ref_km: 18,
  camera_diversity_bonus_weight: 0.22,
  cv_enabled: 0,
  cv_provider_mode: 1,
  cv_max_cameras_per_poll: 10,
  cv_target_coverage_pct: 55,
  cv_timeout_ms: 9000,
  cv_min_confidence: 0.45,
  cv_blend_strength: 0.72,
  cv_temporal_alpha: 0.55,
  cv_stopped_persist_bonus: 8,

  weight_traffic: 0.5,
  weight_image_with_traffic: 0.3,
  weight_image_no_traffic: 0.55,
  image_signal_mode: 2,
  queue_stop_radius_km: 4,
  queue_stop_slow_speed_mph: 25,
  queue_stop_stop_speed_mph: 8,
  queue_stop_min_segments: 3,
  queue_stop_delay_weight: 0.5,
  queue_stop_slow_weight: 0.25,
  queue_stop_stop_weight: 0.25,
  hybrid_confidence_floor: 0.15,
  hybrid_confidence_gain: 0.85,
  hybrid_camera_weight: 0.65,
  hybrid_traffic_weight: 0.35,
  incident_saturation_max: 28,
  incident_saturation_k: 35,
  closure_saturation_max: 20,
  closure_saturation_k: 3,
  dynamic_weights_enabled: 1,
  auto_learn_weights_enabled: 1,
  auto_learn_window_points: 240,
  auto_learn_min_rows: 60,
  auto_learn_ridge_lambda: 1.8,
  auto_learn_strength: 0.55,
  dynamic_weight_floor: 0.45,
  dynamic_weight_cap: 1.9,
  reliability_gamma: 1.15,
  camera_coverage_ref: 10,
  traffic_missing_penalty: 0.35,
  weather_missing_penalty: 0.45,
  regime_rush_traffic: 1.25,
  regime_rush_image: 1.05,
  regime_rush_incident: 1.1,
  regime_rush_closure: 1.1,
  regime_rush_weather: 0.95,
  regime_overnight_traffic: 0.7,
  regime_overnight_image: 1.2,
  regime_overnight_incident: 1.0,
  regime_overnight_closure: 1.0,
  regime_overnight_weather: 1.1,
  regime_weekend_traffic: 0.85,
  regime_weekend_image: 1.1,
  regime_weekend_incident: 0.95,
  regime_weekend_closure: 1.0,
  regime_weekend_weather: 1.05,
  regime_severe_weather_traffic: 0.9,
  regime_severe_weather_image: 0.9,
  regime_severe_weather_incident: 1.05,
  regime_severe_weather_closure: 1.1,
  regime_severe_weather_weather: 1.5,
  lock_mult_traffic: 0,
  lock_mult_image: 0,
  lock_mult_incident: 0,
  lock_mult_closure: 0,
  lock_mult_weather: 0,
  locked_mult_traffic: 1,
  locked_mult_image: 1,
  locked_mult_incident: 1,
  locked_mult_closure: 1,
  locked_mult_weather: 1,
  weather_component_max: 18,
  weather_component_exponent: 1.05,
  weather_precip_weight: 1.1,
  weather_wind_weight: 0.9,
  weather_visibility_weight: 1.0,
  weather_alert_weight: 1.4,
  weather_wind_ref_mph: 45,
  weather_visibility_ref_miles: 10,
  weather_text_boost_rain: 6,
  weather_text_boost_storm: 14,
  weather_text_boost_snow: 18,
  weather_text_boost_fog: 10,
  weather_alert_major_bonus: 12,

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
  sample_limit_max: [20, 300],
  baseline_alpha: [0.01, 0.95],
  min_scoped_cameras_for_strict: [0, 200],
  adaptive_camera_sampling_enabled: [0, 1],
  adaptive_sample_min: [5, 250],
  adaptive_sample_max: [10, 300],
  adaptive_conf_k: [4, 80],
  adaptive_conf_gain_threshold: [0.001, 0.08],
  camera_hard_drop_enabled: [0, 1],
  camera_drop_too_old_flag: [0, 1],
  camera_hard_max_age_minutes: [3, 180],
  camera_missing_snapshot_penalty: [0, 1],
  camera_score_weight_fresh: [0, 1],
  camera_score_weight_proximity: [0, 1],
  camera_score_weight_scope: [0, 1],
  camera_non_scoped_penalty: [0, 1],
  camera_fresh_ref_minutes: [1, 60],
  camera_proximity_scale_km: [1, 80],
  camera_diversity_ref_km: [1, 80],
  camera_diversity_bonus_weight: [0, 1],
  cv_enabled: [0, 1],
  cv_provider_mode: [0, 2],
  cv_max_cameras_per_poll: [1, 80],
  cv_target_coverage_pct: [5, 100],
  cv_timeout_ms: [1500, 30000],
  cv_min_confidence: [0, 1],
  cv_blend_strength: [0, 1],
  cv_temporal_alpha: [0, 1],
  cv_stopped_persist_bonus: [0, 40],
  weight_traffic: [0, 1],
  weight_image_with_traffic: [0, 1],
  weight_image_no_traffic: [0, 1],
  image_signal_mode: [0, 2],
  queue_stop_radius_km: [0.5, 12],
  queue_stop_slow_speed_mph: [5, 40],
  queue_stop_stop_speed_mph: [1, 20],
  queue_stop_min_segments: [1, 40],
  queue_stop_delay_weight: [0, 1],
  queue_stop_slow_weight: [0, 1],
  queue_stop_stop_weight: [0, 1],
  hybrid_confidence_floor: [0, 1],
  hybrid_confidence_gain: [0, 1],
  hybrid_camera_weight: [0, 1],
  hybrid_traffic_weight: [0, 1],
  incident_saturation_max: [0, 100],
  incident_saturation_k: [1, 500],
  closure_saturation_max: [0, 100],
  closure_saturation_k: [0.5, 100],
  dynamic_weights_enabled: [0, 1],
  auto_learn_weights_enabled: [0, 1],
  auto_learn_window_points: [24, 2000],
  auto_learn_min_rows: [20, 1000],
  auto_learn_ridge_lambda: [0.01, 50],
  auto_learn_strength: [0, 1],
  dynamic_weight_floor: [0.1, 1],
  dynamic_weight_cap: [1, 4],
  reliability_gamma: [0.2, 3],
  camera_coverage_ref: [1, 100],
  traffic_missing_penalty: [0, 1],
  weather_missing_penalty: [0, 1],
  regime_rush_traffic: [0.2, 3],
  regime_rush_image: [0.2, 3],
  regime_rush_incident: [0.2, 3],
  regime_rush_closure: [0.2, 3],
  regime_rush_weather: [0.2, 3],
  regime_overnight_traffic: [0.2, 3],
  regime_overnight_image: [0.2, 3],
  regime_overnight_incident: [0.2, 3],
  regime_overnight_closure: [0.2, 3],
  regime_overnight_weather: [0.2, 3],
  regime_weekend_traffic: [0.2, 3],
  regime_weekend_image: [0.2, 3],
  regime_weekend_incident: [0.2, 3],
  regime_weekend_closure: [0.2, 3],
  regime_weekend_weather: [0.2, 3],
  regime_severe_weather_traffic: [0.2, 3],
  regime_severe_weather_image: [0.2, 3],
  regime_severe_weather_incident: [0.2, 3],
  regime_severe_weather_closure: [0.2, 3],
  regime_severe_weather_weather: [0.2, 3],
  lock_mult_traffic: [0, 1],
  lock_mult_image: [0, 1],
  lock_mult_incident: [0, 1],
  lock_mult_closure: [0, 1],
  lock_mult_weather: [0, 1],
  locked_mult_traffic: [0.2, 4],
  locked_mult_image: [0.2, 4],
  locked_mult_incident: [0.2, 4],
  locked_mult_closure: [0.2, 4],
  locked_mult_weather: [0.2, 4],
  weather_component_max: [0, 50],
  weather_component_exponent: [0.2, 3],
  weather_precip_weight: [0, 3],
  weather_wind_weight: [0, 3],
  weather_visibility_weight: [0, 3],
  weather_alert_weight: [0, 3],
  weather_wind_ref_mph: [10, 120],
  weather_visibility_ref_miles: [1, 20],
  weather_text_boost_rain: [0, 30],
  weather_text_boost_storm: [0, 40],
  weather_text_boost_snow: [0, 40],
  weather_text_boost_fog: [0, 30],
  weather_alert_major_bonus: [0, 40],
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

const METHODOLOGY_PRESETS = [
  {
    id: "balanced_hybrid",
    name: "Balanced Hybrid",
    description: "Recommended default. Hybrid image signal + dynamic and auto-learn weighting.",
    overrides: {
      image_signal_mode: 2,
      cv_enabled: 0,
      dynamic_weights_enabled: 1,
      auto_learn_weights_enabled: 1,
      queue_stop_radius_km: 4,
      queue_stop_slow_speed_mph: 25,
      queue_stop_stop_speed_mph: 8,
      queue_stop_delay_weight: 0.5,
      queue_stop_slow_weight: 0.25,
      queue_stop_stop_weight: 0.25,
      hybrid_confidence_floor: 0.15,
      hybrid_confidence_gain: 0.85
    }
  },
  {
    id: "queue_stop_heavy",
    name: "Queue/Stop Heavy",
    description: "Emphasizes near-camera traffic slowdown and stopped-vehicle proxies.",
    overrides: {
      image_signal_mode: 1,
      cv_enabled: 0,
      queue_stop_radius_km: 5,
      queue_stop_slow_speed_mph: 30,
      queue_stop_stop_speed_mph: 10,
      queue_stop_min_segments: 4,
      queue_stop_delay_weight: 0.35,
      queue_stop_slow_weight: 0.35,
      queue_stop_stop_weight: 0.3,
      hybrid_confidence_floor: 0.2,
      hybrid_confidence_gain: 0.8
    }
  },
  {
    id: "cv_vision_fusion",
    name: "CV Vision Fusion",
    description: "Turns on camera CV counting and blends CV queue estimates into the score.",
    overrides: {
      image_signal_mode: 2,
      cv_enabled: 1,
      cv_provider_mode: 1,
      cv_max_cameras_per_poll: 24,
      cv_target_coverage_pct: 85,
      cv_timeout_ms: 12000,
      cv_min_confidence: 0.4,
      cv_blend_strength: 0.88,
      cv_temporal_alpha: 0.65,
      cv_stopped_persist_bonus: 12
    }
  },
  {
    id: "proxy_lightweight",
    name: "Proxy Lightweight",
    description: "Fast and cheap mode using snapshot-byte anomaly only (no CV, no queue-stop).",
    overrides: {
      image_signal_mode: 0,
      cv_enabled: 0,
      dynamic_weights_enabled: 1,
      auto_learn_weights_enabled: 1,
      sample_limit: 28,
      adaptive_camera_sampling_enabled: 0,
      weight_image_no_traffic: 0.65,
      weight_image_with_traffic: 0.25
    }
  },
  {
    id: "weather_priority",
    name: "Weather Priority",
    description: "Aggressively raises weather impact during fog/rain/storm periods.",
    overrides: {
      image_signal_mode: 2,
      cv_enabled: 0,
      weather_component_max: 24,
      weather_component_exponent: 1.15,
      weather_alert_major_bonus: 18,
      regime_severe_weather_traffic: 0.75,
      regime_severe_weather_image: 0.75,
      regime_severe_weather_weather: 1.9,
      alert_high_score_threshold: 65
    }
  }
];

const METHODOLOGY_PRESET_ACTIVE_KEY = "methodology_preset_active";

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

export function listMethodologyPresets() {
  return METHODOLOGY_PRESETS.map((p) => ({
    id: p.id,
    name: p.name,
    description: p.description,
    override_count: Object.keys(p.overrides || {}).length
  }));
}

export function getMethodologyPresetById(presetId) {
  const id = String(presetId || "").trim();
  return METHODOLOGY_PRESETS.find((p) => p.id === id) || null;
}

export async function getActiveMethodologyPreset() {
  const { rows } = await pool.query(`SELECT value FROM app_state WHERE key = $1`, [METHODOLOGY_PRESET_ACTIVE_KEY]);
  if (!rows.length) return null;
  return rows[0].value || null;
}

export async function setActiveMethodologyPreset(preset) {
  const p = preset && typeof preset === "object" ? preset : {};
  const normalized = {
    id: String(p.id || "").trim(),
    name: String(p.name || "").trim(),
    description: String(p.description || "").trim(),
    override_count: Math.max(0, Number(p.override_count || 0)),
    activated_at: new Date().toISOString()
  };
  await pool.query(
    `INSERT INTO app_state(key, value, updated_at)
     VALUES($1, $2::jsonb, NOW())
     ON CONFLICT(key) DO UPDATE
       SET value = EXCLUDED.value,
           updated_at = NOW()`,
    [METHODOLOGY_PRESET_ACTIVE_KEY, JSON.stringify(normalized)]
  );
  return normalized;
}

function activeStrategyKey(corridorId) {
  return `active_strategy_corridor_${Number(corridorId)}`;
}

function sanitizeActiveStrategyPayload(payload = {}, corridorId) {
  const p = payload && typeof payload === "object" ? payload : {};
  const strategy = p.strategy && typeof p.strategy === "object" ? p.strategy : p;
  const normalized = {
    corridor_id: Number(corridorId),
    strategy_id: String(strategy.strategy_id || strategy.id || "").trim(),
    strategy_name: String(strategy.strategy_name || strategy.name || "").trim(),
    goal: String(strategy.goal || "").trim(),
    note: String(strategy.note || "").trim(),
    source: String(p.source || "ui").trim() || "ui",
    updated_at: new Date().toISOString(),
    inputs: strategy.inputs && typeof strategy.inputs === "object" ? strategy.inputs : {},
    projection: strategy.projection && typeof strategy.projection === "object" ? strategy.projection : {}
  };
  if (!normalized.strategy_id) normalized.strategy_id = "custom";
  if (!normalized.strategy_name) normalized.strategy_name = normalized.strategy_id;
  return normalized;
}

export async function getActiveStrategy(corridorId) {
  const key = activeStrategyKey(corridorId);
  const { rows } = await pool.query(`SELECT value FROM app_state WHERE key = $1`, [key]);
  if (!rows.length) return null;
  return rows[0].value || null;
}

export async function upsertActiveStrategy(corridorId, payload = {}) {
  const key = activeStrategyKey(corridorId);
  const value = sanitizeActiveStrategyPayload(payload, corridorId);
  await pool.query(
    `INSERT INTO app_state(key, value, updated_at)
     VALUES($1, $2::jsonb, NOW())
     ON CONFLICT(key) DO UPDATE
       SET value = EXCLUDED.value,
           updated_at = NOW()`,
    [key, JSON.stringify(value)]
  );
  return value;
}
