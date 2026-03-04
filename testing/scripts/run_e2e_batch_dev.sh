#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

if [[ -x "$ROOT/.venv/bin/activate" ]]; then
  # shellcheck disable=SC1091
  source "$ROOT/.venv/bin/activate"
fi

PYTHON_BIN="${PYTHON_BIN:-$ROOT/.venv/bin/python3}"
if [[ ! -x "$PYTHON_BIN" ]]; then
  PYTHON_BIN="python3"
fi

INPUT_JSONL="${INPUT_JSONL:-testing/requests/eval_requests_dev_v0_1.jsonl}"
SENSOR_MODE="${SENSOR_MODE:-STUB}"             # STUB|LIVE
SENSOR_BASE_URL="${SENSOR_BASE_URL:-http://127.0.0.1:9000}"
SENSOR_TIMEOUT_MS="${SENSOR_TIMEOUT_MS:-1200}"
BRMS_MODE="${BRMS_MODE:-STUB}"                 # STUB|LIVE|NONE
BRMS_STUB="${BRMS_STUB:-tools/smoke/fixtures/brms_all_pass.json}"
BRMS_URL="${BRMS_URL:-http://localhost:8090/bridge/brms_flags}"
MAX_ROWS="${MAX_ROWS:-50}"

mkdir -p testing/_logs testing/runs
TS="$(date +%Y%m%d_%H%M%S)"
RUN_DIR="testing/runs/eval_requests_dev_v0_1_run_${TS}"
PACK_DIR="$RUN_DIR/packs"
REPORT_DIR="$RUN_DIR/reports"
RESULTS_JSONL="$RUN_DIR/results.jsonl"
SUMMARY_JSON="$RUN_DIR/summary.json"
LOG_FILE="testing/_logs/run_e2e_batch_dev_${TS}.log"

mkdir -p "$PACK_DIR" "$REPORT_DIR"

exec > >(tee -a "$LOG_FILE") 2>&1

echo "[BATCH_DEV] root=$ROOT"
echo "[BATCH_DEV] python=$PYTHON_BIN"
echo "[BATCH_DEV] input=$INPUT_JSONL"
echo "[BATCH_DEV] run_dir=$RUN_DIR"
echo "[BATCH_DEV] sensor_mode=$SENSOR_MODE brms_mode=$BRMS_MODE max_rows=$MAX_ROWS"
echo "[BATCH_DEV] log=$LOG_FILE"

INPUT_JSONL="$INPUT_JSONL" \
PYTHON_BIN="$PYTHON_BIN" \
SENSOR_MODE="$SENSOR_MODE" \
SENSOR_BASE_URL="$SENSOR_BASE_URL" \
SENSOR_TIMEOUT_MS="$SENSOR_TIMEOUT_MS" \
BRMS_MODE="$BRMS_MODE" \
BRMS_STUB="$BRMS_STUB" \
BRMS_URL="$BRMS_URL" \
MAX_ROWS="$MAX_ROWS" \
PACK_DIR="$PACK_DIR" \
REPORT_DIR="$REPORT_DIR" \
RESULTS_JSONL="$RESULTS_JSONL" \
SUMMARY_JSON="$SUMMARY_JSON" \
"$PYTHON_BIN" - << 'PY'
import json
import os
import subprocess
import time
from pathlib import Path

input_jsonl = Path(os.environ["INPUT_JSONL"])
python_bin = os.environ["PYTHON_BIN"]
sensor_mode = os.environ["SENSOR_MODE"]
sensor_base_url = os.environ["SENSOR_BASE_URL"]
sensor_timeout_ms = str(os.environ["SENSOR_TIMEOUT_MS"])
brms_mode = os.environ["BRMS_MODE"].upper()
brms_stub = os.environ["BRMS_STUB"]
brms_url = os.environ["BRMS_URL"]
max_rows = int(os.environ["MAX_ROWS"])
pack_dir = Path(os.environ["PACK_DIR"])
report_dir = Path(os.environ["REPORT_DIR"])
results_jsonl = Path(os.environ["RESULTS_JSONL"])
summary_json = Path(os.environ["SUMMARY_JSON"])

required = {
    "request_id",
    "client_id",
    "seed",
    "product_type",
    "requested_amount",
    "term_months",
    "age",
    "employment_status",
    "declared_income_monthly",
    "is_existing_customer",
    "as_of_ts",
}

rows = []
with input_jsonl.open("r", encoding="utf-8") as f:
    for line in f:
        line = line.strip()
        if not line:
            continue
        rows.append(json.loads(line))

rows = rows[:max_rows]

ok = 0
fail = 0
status_counts = {"APPROVED": 0, "REJECTED": 0, "REVIEW_REQUIRED": 0}
outcome_counts = {"APPROVE": 0, "REVIEW": 0, "REJECT": 0}
results = []

for i, row in enumerate(rows, start=1):
    row_req = str(row.get("request_id", "")).strip()
    missing = sorted(k for k in required if k not in row)
    if not row_req or missing:
        fail += 1
        results.append({
            "row_index": i,
            "request_id": row_req or f"missing_req_{i}",
            "status": "ERROR",
            "error": f"missing_required={missing} or blank request_id",
        })
        continue

    client_id = str(row["client_id"])
    seed = str(row["seed"])
    channel = str(row.get("channel", "web"))
    safe_req = "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in row_req)
    pack_path = pack_dir / f"{i:04d}_{safe_req}.json"
    report_path = report_dir / f"{i:04d}_{safe_req}.json"

    wf_cmd = [
        python_bin,
        "runners/runner_workflow_eligibility.py",
        "--client-id", client_id,
        "--seed", seed,
        "--request-id", row_req,
        "--channel", channel,
        "--as-of-ts", str(row.get("as_of_ts")),
        "--age", str(row.get("age")),
        "--employment-status", str(row.get("employment_status")),
        "--declared-income-monthly", str(row.get("declared_income_monthly")),
        "--is-existing-customer", str(row.get("is_existing_customer")).lower(),
        "--requested-amount", str(row.get("requested_amount")),
        "--term-months", str(row.get("term_months")),
        "--product-type", str(row.get("product_type")),
        "--sensor-mode", sensor_mode,
        "--sensor-base-url", sensor_base_url,
        "--sensor-timeout-ms", sensor_timeout_ms,
    ]
    if row.get("declared_dti") is not None:
        wf_cmd.extend(["--declared-dti", str(row.get("declared_dti"))])
    if row.get("declared_credit_score") is not None:
        wf_cmd.extend(["--declared-credit-score", str(row.get("declared_credit_score"))])

    if brms_mode == "NONE":
        wf_cmd.append("--no-brms")
    elif brms_mode == "LIVE":
        wf_cmd.extend(["--brms-url", brms_url])
    else:
        wf_cmd.extend(["--brms-stub", brms_stub])

    t0 = time.time()
    try:
        pack_out = subprocess.check_output(wf_cmd, stderr=subprocess.STDOUT, text=True)
        pack_path.write_text(pack_out, encoding="utf-8")

        rep_cmd = [
            python_bin,
            "runners/runner_reporter.py",
            "--decision-pack-json",
            str(pack_path),
        ]
        rep_out = subprocess.check_output(rep_cmd, stderr=subprocess.STDOUT, text=True)
        report_path.write_text(rep_out, encoding="utf-8")

        pack = json.loads(pack_out)
        report = json.loads(rep_out)
        decisions = pack.get("decisions", {}) or {}
        elig = decisions.get("eligibility", {}) or {}
        final_decision = decisions.get("final_decision", {}) or {}

        elig_status = str(elig.get("eligibility_status", ""))
        outcome = str(final_decision.get("final_outcome", ""))
        reason = str(final_decision.get("final_reason_code", ""))
        mode = str(elig.get("meta_sensor_mode_used", ""))
        elapsed_ms = int((time.time() - t0) * 1000)

        status_counts[elig_status] = status_counts.get(elig_status, 0) + 1
        outcome_counts[outcome] = outcome_counts.get(outcome, 0) + 1
        ok += 1

        results.append({
            "row_index": i,
            "request_id": row_req,
            "client_id": client_id,
            "seed": int(seed),
            "eligibility_status": elig_status,
            "final_outcome": outcome,
            "final_reason_code": reason,
            "sensor_mode_used": mode,
            "pack_path": str(pack_path),
            "report_path": str(report_path),
            "report_schema": report.get("meta_schema_version"),
            "elapsed_ms": elapsed_ms,
            "status": "OK",
        })
    except subprocess.CalledProcessError as e:
        fail += 1
        results.append({
            "row_index": i,
            "request_id": row_req,
            "client_id": client_id,
            "seed": int(seed),
            "status": "ERROR",
            "error": e.output[-1200:] if e.output else str(e),
        })

with results_jsonl.open("w", encoding="utf-8") as f:
    for r in results:
        f.write(json.dumps(r, ensure_ascii=True) + "\n")

summary = {
    "schema_version": "e2e_batch_run_summary_v0_1",
    "input_path": str(input_jsonl),
    "run_size": len(rows),
    "ok_count": ok,
    "error_count": fail,
    "eligibility_status_counts": status_counts,
    "final_outcome_counts": outcome_counts,
    "results_jsonl": str(results_jsonl),
}
summary_json.write_text(json.dumps(summary, indent=2), encoding="utf-8")

print(f"[BATCH_DEV] run_size={len(rows)} ok={ok} error={fail}")
print(f"[BATCH_DEV] results={results_jsonl}")
print(f"[BATCH_DEV] summary={summary_json}")

if fail > 0:
    raise SystemExit(3)
PY

echo "[BATCH_DEV] done"
echo "[BATCH_DEV] results=$RESULTS_JSONL"
echo "[BATCH_DEV] summary=$SUMMARY_JSON"
echo "[BATCH_DEV] log=$LOG_FILE"
