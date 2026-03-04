CREATE TABLE IF NOT EXISTS corridors (
  id BIGSERIAL PRIMARY KEY,
  name TEXT NOT NULL UNIQUE,
  xmin DOUBLE PRECISION NOT NULL,
  ymin DOUBLE PRECISION NOT NULL,
  xmax DOUBLE PRECISION NOT NULL,
  ymax DOUBLE PRECISION NOT NULL,
  active BOOLEAN NOT NULL DEFAULT TRUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS checkpoints (
  id BIGSERIAL PRIMARY KEY,
  corridor_id BIGINT NOT NULL REFERENCES corridors(id) ON DELETE CASCADE,
  name TEXT NOT NULL,
  lat DOUBLE PRECISION NOT NULL,
  lon DOUBLE PRECISION NOT NULL,
  radius_km DOUBLE PRECISION NOT NULL DEFAULT 8,
  source TEXT NOT NULL DEFAULT 'manual',
  active BOOLEAN NOT NULL DEFAULT TRUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE(corridor_id, name)
);

ALTER TABLE checkpoints
  ADD COLUMN IF NOT EXISTS radius_km DOUBLE PRECISION NOT NULL DEFAULT 8;

CREATE TABLE IF NOT EXISTS baseline_profiles (
  corridor_id BIGINT NOT NULL REFERENCES corridors(id) ON DELETE CASCADE,
  camera_object_id BIGINT NOT NULL,
  dow SMALLINT NOT NULL,
  hour SMALLINT NOT NULL,
  ewma_bytes DOUBLE PRECISION NOT NULL,
  sample_count INTEGER NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (corridor_id, camera_object_id, dow, hour)
);

CREATE TABLE IF NOT EXISTS monitor_runs (
  id BIGSERIAL PRIMARY KEY,
  corridor_id BIGINT NOT NULL REFERENCES corridors(id) ON DELETE CASCADE,
  run_ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  traffic_segments INTEGER NOT NULL,
  delay_pct DOUBLE PRECISION NOT NULL,
  incidents_count INTEGER NOT NULL,
  closures_count INTEGER NOT NULL,
  camera_total INTEGER NOT NULL,
  camera_fresh_pct DOUBLE PRECISION NOT NULL,
  image_score DOUBLE PRECISION NOT NULL,
  traffic_component DOUBLE PRECISION NOT NULL,
  image_component DOUBLE PRECISION NOT NULL,
  incident_component DOUBLE PRECISION NOT NULL,
  closure_component DOUBLE PRECISION NOT NULL,
  fused_score DOUBLE PRECISION NOT NULL,
  predicted_next_score_p50 DOUBLE PRECISION,
  predicted_next_score_p90 DOUBLE PRECISION,
  prediction_confidence DOUBLE PRECISION,
  model_version TEXT,
  drift_score DOUBLE PRECISION,
  alert_state TEXT NOT NULL,
  alert_reason TEXT NOT NULL DEFAULT '',
  raw_json JSONB NOT NULL DEFAULT '{}'::jsonb
);

ALTER TABLE monitor_runs ADD COLUMN IF NOT EXISTS predicted_next_score_p50 DOUBLE PRECISION;
ALTER TABLE monitor_runs ADD COLUMN IF NOT EXISTS predicted_next_score_p90 DOUBLE PRECISION;
ALTER TABLE monitor_runs ADD COLUMN IF NOT EXISTS prediction_confidence DOUBLE PRECISION;
ALTER TABLE monitor_runs ADD COLUMN IF NOT EXISTS model_version TEXT;
ALTER TABLE monitor_runs ADD COLUMN IF NOT EXISTS drift_score DOUBLE PRECISION;

CREATE INDEX IF NOT EXISTS idx_monitor_runs_corridor_ts
  ON monitor_runs(corridor_id, run_ts DESC);

CREATE TABLE IF NOT EXISTS camera_observations (
  id BIGSERIAL PRIMARY KEY,
  run_id BIGINT NOT NULL REFERENCES monitor_runs(id) ON DELETE CASCADE,
  camera_object_id BIGINT NOT NULL,
  camera_location TEXT NOT NULL,
  camera_direction TEXT NOT NULL,
  snapshot_url TEXT NOT NULL,
  snapshot_bytes INTEGER NOT NULL,
  age_minutes DOUBLE PRECISION,
  too_old BOOLEAN,
  image_score DOUBLE PRECISION NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_camera_obs_run_id ON camera_observations(run_id);

CREATE TABLE IF NOT EXISTS app_state (
  key TEXT PRIMARY KEY,
  value JSONB NOT NULL DEFAULT '{}'::jsonb,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS model_snapshots (
  id BIGSERIAL PRIMARY KEY,
  corridor_id BIGINT NOT NULL REFERENCES corridors(id) ON DELETE CASCADE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  model_version TEXT NOT NULL,
  train_rows INTEGER NOT NULL,
  residual_sigma DOUBLE PRECISION NOT NULL,
  r2 DOUBLE PRECISION NOT NULL,
  drift_score DOUBLE PRECISION NOT NULL,
  coefficients JSONB NOT NULL,
  feature_stats JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_model_snapshots_corridor_created
  ON model_snapshots(corridor_id, created_at DESC);

CREATE TABLE IF NOT EXISTS chat_logs (
  id BIGSERIAL PRIMARY KEY,
  corridor_id BIGINT REFERENCES corridors(id) ON DELETE SET NULL,
  question TEXT NOT NULL,
  response TEXT NOT NULL,
  model_used TEXT NOT NULL DEFAULT 'heuristic',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
