#!/usr/bin/env python3

import argparse
import json
from pathlib import Path


def find_peak(peaks, prefix):
    for name, value in peaks.items():
        if name == prefix or name.startswith(prefix + "-"):
            return value
    return {}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--result-dir", required=True)
    parser.add_argument("--deploy-result-md", required=True)
    parser.add_argument("--run-date", required=True)
    parser.add_argument("rung_dirs", nargs="+")
    args = parser.parse_args()

    result_dir = Path(args.result_dir)
    result_dir.mkdir(parents=True, exist_ok=True)
    deploy_md_path = Path(args.deploy_result_md)
    deploy_md_path.parent.mkdir(parents=True, exist_ok=True)

    rungs = []
    highest_passing = None
    first_failed = None
    for rung_dir in args.rung_dirs:
        summary_path = Path(rung_dir) / "summary.json"
        if not summary_path.exists():
            continue
        payload = json.loads(summary_path.read_text(encoding="utf-8"))
        rate = payload["run"]["aggregate_target_rate"]
        rung = {
            "rate": rate,
            "status": payload["status"],
            "requested": payload["counts"]["requested"],
            "accepted": payload["counts"]["accepted"],
            "failed": payload["counts"]["failed"],
            "postgres_delta": payload["counts"]["postgres_delta"],
            "cassandra_delta": payload["counts"]["cassandra_delta"],
            "missing": payload["counts"]["missing"],
            "duplicates": payload["counts"]["duplicates"],
            "lag_at_end": payload["kafka"]["lag_at_end"],
            "lag_settled_to_zero": payload["kafka"]["lag_settled_to_zero"],
            "lag_zero_seconds": payload["kafka"]["lag_zero_seconds"],
            "source_ips": payload["run"]["source_ip_count"],
            "per_pod_target": payload["run"].get("per_pod_rate_label", f"{payload['run']['per_pod_rate']} msg/sec"),
            "result_dir": str(Path(rung_dir)),
            "summary_md": str(Path(rung_dir) / "summary.md"),
            "processor_peak": find_peak(payload["resource_observations"]["peak_sampled_during_run"], "messages-processor"),
            "gateway_peak": find_peak(payload["resource_observations"]["peak_sampled_during_run"], "gateway"),
        }
        rungs.append(rung)
        if payload["status"] == "passed":
            highest_passing = rate
        elif first_failed is None:
            first_failed = rate

    status = "inconclusive"
    if highest_passing is not None:
        status = "partial_pass"
    if first_failed is not None:
        status = "bounded_failure"

    summary = {
        "scenario": "pipeline-diagnostic-ladder",
        "run_date": args.run_date,
        "status": status,
        "highest_fully_reconciled_rate": highest_passing,
        "first_failed_rate": first_failed,
        "rungs": rungs,
    }

    summary_json_path = result_dir / "summary.json"
    summary_md_path = result_dir / "summary.md"

    lines = [
        f"# Pipeline Diagnostic Ladder - {args.run_date}",
        "",
        f"- Overall status: `{status}`",
        f"- Highest fully reconciled rate: `{highest_passing if highest_passing is not None else 'none'}`",
        f"- First failed rate: `{first_failed if first_failed is not None else 'none'}`",
        "",
        "## Rungs",
        "",
    ]
    for rung in rungs:
        lines.extend([
            f"### {rung['rate']} msg/sec",
            "",
            f"- Status: `{rung['status']}`",
            f"- Requested / accepted / failed: `{rung['requested']}` / `{rung['accepted']}` / `{rung['failed']}`",
            f"- Postgres / Cassandra delta: `{rung['postgres_delta']}` / `{rung['cassandra_delta']}`",
            f"- Missing / duplicates: `{rung['missing']}` / `{rung['duplicates']}`",
            f"- Kafka lag settled / end: `{rung['lag_settled_to_zero']}` / `{rung['lag_at_end']}`",
            f"- Lag zero seconds: `{rung['lag_zero_seconds']}`",
            f"- Source IPs: `{rung['source_ips']}`",
            f"- Per-pod target: `{rung['per_pod_target']}`",
            f"- Processor peak: `{json.dumps(rung['processor_peak'])}`",
            f"- Gateway peak: `{json.dumps(rung['gateway_peak'])}`",
            f"- Artifact: [{Path(rung['summary_md']).parent.name}]({Path(rung['summary_md']).as_posix()})",
            "",
        ])

    summary_json_path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    summary_md_path.write_text("\n".join(lines), encoding="utf-8")
    deploy_md_path.write_text("\n".join(lines), encoding="utf-8")


if __name__ == "__main__":
    main()
