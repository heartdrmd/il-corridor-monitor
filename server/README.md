# Corridor Intelligence Server

Server app for live corridor delay analytics using public Illinois camera/incident feeds.

## What this includes
- Express API + acquisition worker
- Sophisticated live HTML command center (`/`) with tabs:
  - Overview
  - Predictive Model
  - Data Ops
  - Chat Bridge
- PostgreSQL storage for runs, camera observations, baselines
- Advanced predictive layer:
  - Ridge next-interval score model
  - p50/p90 forecast
  - confidence + drift score
  - model snapshots persisted in DB
- Restart-safe resume behavior:
  - Polling resumes on process start
  - Baselines and run history persist in Postgres

## Why data survives reboots and upgrades
- Data is in PostgreSQL tables, not in-memory files.
- In `docker-compose.yml`, Postgres uses a named volume (`corridor_pgdata`).
- App redeploy/restart does not delete DB unless you explicitly remove the volume.

## Start locally (Docker)
```bash
cd /Users/nadalmaker/Documents/NEWP/server
docker compose up -d
```

Open:
- App: http://localhost:8080

## Start locally (without Docker)
Prereqs: Node 20+, PostgreSQL running.

```bash
cd /Users/nadalmaker/Documents/NEWP/server
npm install
export DATABASE_URL="postgres://postgres:postgres@localhost:5432/corridor_monitor"
npm start
```

## Flexible corridor/checkpoint input
- Corridor creation supports:
  - `bbox` (`xmin,ymin,xmax,ymax`)
  - `city/query` + radius
  - center `lat/lon` + radius (API)
- Checkpoint creation supports:
  - exact `lat/lon`
  - city/query geocode fallback

## Key API endpoints
- `GET /api/health`
- `GET /api/data/options`
- `GET /api/corridors`
- `POST /api/corridors`
- `POST /api/checkpoints`
- `GET /api/runs/latest?corridor_id=...`
- `GET /api/runs/timeseries?corridor_id=...&hours=24`
- `GET /api/runs/recent-cameras?corridor_id=...`
- `GET /api/models/latest?corridor_id=...`
- `POST /api/chat/query`
- `POST /api/poll-now`

## Optional GPT bridge
Set these env vars if you want GPT-assisted narrative answers in chat:
- `OPENAI_API_KEY`
- `OPENAI_MODEL` (default `gpt-4.1-mini`)

Without API key, chat uses deterministic DB-driven analytics responses.

## Upgrade safety practices
- Keep DB in separate managed service/volume.
- Never use destructive commands on DB volume in deployments.
- Add periodic backups:
  - `pg_dump` daily
  - WAL/archive strategy for point-in-time recovery if needed.
