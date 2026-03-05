#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

echo "[Phase I] Syntax checks"
for f in src/*.js public/app.js; do
  node --check "$f"
done
echo "[Phase I] Syntax OK"

echo "[Phase I] Dependency vulnerability audit"
npm audit --omit=dev --audit-level=moderate
echo "[Phase I] Audit OK"

echo "[Phase I] COMPLETE"
