#!/usr/bin/env python3
import argparse
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from statistics import median


def percentile(values, p):
    if not values:
        return None
    s = sorted(values)
    if p <= 0:
        return s[0]
    if p >= 100:
        return s[-1]
    k = (len(s) - 1) * (p / 100.0)
    f = int(k)
    c = min(f + 1, len(s) - 1)
    if f == c:
        return s[f]
    return s[f] + (s[c] - s[f]) * (k - f)


def load_jsonl(path: Path):
    rows = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def detect_latest_run(runs_root: Path):
    candidates = sorted([p for p in runs_root.glob("eval_requests_dev_v0_1_run_*") if p.is_dir()])
    if not candidates:
        raise FileNotFoundError("No batch run directories found under testing/runs")
    return candidates[-1]


def build_report(run_dir: Path, summary: dict, results: list):
    total = int(summary.get("run_size", len(results)))
    ok_count = int(summary.get("ok_count", 0))
    err_count = int(summary.get("error_count", 0))

    reason_counts = Counter()
    elig_counts = Counter()
    outcome_counts = Counter()
    latencies = []

    for r in results:
        if r.get("status") != "OK":
            continue
        elig = str(r.get("eligibility_status", "UNKNOWN"))
        out = str(r.get("final_outcome", "UNKNOWN"))
        reason = str(r.get("final_reason_code", "UNKNOWN"))
        elig_counts[elig] += 1
        outcome_counts[out] += 1
        reason_counts[reason] += 1
        if isinstance(r.get("elapsed_ms"), int):
            latencies.append(r["elapsed_ms"])

    p50 = percentile(latencies, 50)
    p90 = percentile(latencies, 90)
    p99 = percentile(latencies, 99)

    ts = datetime.now(timezone.utc).isoformat()
    lines = []
    lines.append("# E2E Batch Dev Report v0.1")
    lines.append("")
    lines.append(f"- Generated at (UTC): {ts}")
    lines.append(f"- Run dir: `{run_dir}`")
    lines.append(f"- Input path: `{summary.get('input_path', 'unknown')}`")
    lines.append(f"- Results path: `{run_dir / 'results.jsonl'}`")
    lines.append(f"- Summary path: `{run_dir / 'summary.json'}`")
    lines.append("")
    lines.append("## Health")
    lines.append(f"- Total requests: {total}")
    lines.append(f"- OK: {ok_count}")
    lines.append(f"- Errors: {err_count}")
    lines.append(f"- Success rate: {((ok_count / total) * 100.0) if total else 0.0:.2f}%")
    lines.append(f"- Error rate: {((err_count / total) * 100.0) if total else 0.0:.2f}%")
    lines.append("")
    lines.append("## Eligibility Distribution")
    for k, v in sorted(elig_counts.items()):
        lines.append(f"- {k}: {v}")
    lines.append("")
    lines.append("## Final Outcome Distribution")
    for k, v in sorted(outcome_counts.items()):
        lines.append(f"- {k}: {v}")
    lines.append("")
    lines.append("## Top Final Reason Codes")
    top_reasons = reason_counts.most_common(10)
    if not top_reasons:
        lines.append("- none")
    else:
        for code, cnt in top_reasons:
            lines.append(f"- {code}: {cnt}")
    lines.append("")
    lines.append("## Latency (elapsed_ms)")
    if not latencies:
        lines.append("- p50: n/a")
        lines.append("- p90: n/a")
        lines.append("- p99: n/a")
    else:
        lines.append(f"- p50: {p50:.1f}")
        lines.append(f"- p90: {p90:.1f}")
        lines.append(f"- p99: {p99:.1f}")
        lines.append(f"- median: {median(latencies):.1f}")

    return "\n".join(lines) + "\n"


def main():
    ap = argparse.ArgumentParser(description="Build markdown report from batch E2E run artifacts")
    ap.add_argument("--run-dir", default=None, help="Path to run directory (testing/runs/eval_requests_dev_v0_1_run_*)")
    ap.add_argument("--out", default=None, help="Optional output markdown path")
    args = ap.parse_args()

    runs_root = Path("testing/runs")
    run_dir = Path(args.run_dir) if args.run_dir else detect_latest_run(runs_root)

    summary_path = run_dir / "summary.json"
    results_path = run_dir / "results.jsonl"
    if not summary_path.exists() or not results_path.exists():
        raise FileNotFoundError(f"Missing summary/results in run_dir: {run_dir}")

    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    results = load_jsonl(results_path)

    content = build_report(run_dir, summary, results)

    if args.out:
        out_path = Path(args.out)
    else:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_path = Path("testing/reports") / f"e2e_batch_dev_report_{ts}.md"

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(content, encoding="utf-8")
    print(out_path)


if __name__ == "__main__":
    main()
