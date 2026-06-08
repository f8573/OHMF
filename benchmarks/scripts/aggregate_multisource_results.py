#!/usr/bin/env python3

import argparse
import json
import math
import re
import subprocess
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path


def run_command(args):
    completed = subprocess.run(args, check=True, capture_output=True, text=True)
    return completed.stdout


def parse_trailing_int(raw):
    for line in reversed(raw.strip().splitlines()):
        line = line.strip()
        if not line:
            continue
        match = re.search(r"(-?\d+)\s*$", line)
        if match:
            return int(match.group(1))
    raise ValueError(f"could not parse trailing integer from output: {raw!r}")


def parse_summary_from_log(raw):
    begin = "SHARD_SUMMARY_JSON_BEGIN"
    end = "SHARD_SUMMARY_JSON_END"
    start = raw.find(begin)
    finish = raw.find(end)
    if start == -1 or finish == -1 or finish <= start:
        raise ValueError("summary markers not found in shard log")
    payload = raw[start + len(begin):finish].strip()
    return json.loads(payload)


def percentile(values, pct):
    if not values:
        return 0.0
    if len(values) == 1:
        return float(values[0])
    values = sorted(values)
    rank = (len(values) - 1) * (pct / 100.0)
    lower = math.floor(rank)
    upper = math.ceil(rank)
    if lower == upper:
        return float(values[lower])
    lower_value = values[lower]
    upper_value = values[upper]
    return float(lower_value + (upper_value - lower_value) * (rank - lower))


def postgres_count(namespace, conversations):
    if not conversations:
        return 0
    total = 0
    chunk_size = 40
    for start in range(0, len(conversations), chunk_size):
        chunk = conversations[start:start + chunk_size]
        ids = ",".join(f"'{conversation}'::uuid" for conversation in chunk)
        query = f"SELECT COUNT(*) FROM messages WHERE conversation_id IN ({ids});"
        raw = run_command([
            "kubectl", "-n", namespace, "exec", "deploy/postgres", "--", "sh", "-lc",
            f'psql -U ohmf -d ohmf -t -A -c "{query}"',
        ])
        total += parse_trailing_int(raw)
    return total


def cassandra_count(namespace, conversations, keyspace):
    if not conversations:
        return 0
    bucket = datetime.now(timezone.utc).strftime("%Y%m%d")
    total = 0
    for conversation in conversations:
        query = (
            f"CONSISTENCY ONE; SELECT COUNT(*) FROM {keyspace}.messages_by_conversation "
            f"WHERE conversation_id = {conversation} AND bucket_yyyymmdd = '{bucket}';"
        )
        raw = run_command([
            "kubectl", "-n", namespace, "exec", "deploy/cassandra", "--", "sh", "-lc",
            f"/opt/cassandra/bin/cqlsh -e \"{query}\"",
        ])
        total += parse_trailing_int(raw)
    return total


def kafka_lag(namespace, consumer_group, topic):
    raw = run_command([
        "kubectl", "-n", namespace, "exec", "deploy/kafka", "--", "sh", "-lc",
        f"/usr/bin/kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group {consumer_group}",
    ])
    total = 0
    for line in raw.splitlines():
        fields = line.split()
        if len(fields) < 6:
            continue
        if fields[0] != consumer_group or fields[1] != topic:
            continue
        try:
            total += int(fields[5])
        except ValueError:
            continue
    return total, raw


def wait_for_kafka_zero(namespace, consumer_group, topic, observations_dir, timeout_seconds, poll_seconds):
    started = time.time()
    poll_index = 0
    last_lag = 0
    while True:
        lag, raw = kafka_lag(namespace, consumer_group, topic)
        timestamp = datetime.now(timezone.utc).strftime("%H%M%S")
        (observations_dir / f"kafka-lag-poll-{poll_index:02d}-{timestamp}.txt").write_text(raw, encoding="utf-8")
        last_lag = lag
        if lag == 0:
            return {
                "lag_settled_to_zero": True,
                "lag_zero_seconds": round(time.time() - started, 6),
                "lag_at_end": 0,
            }
        if time.time() - started >= timeout_seconds:
            return {
                "lag_settled_to_zero": False,
                "lag_zero_seconds": round(time.time() - started, 6),
                "lag_at_end": lag,
            }
        poll_index += 1
        time.sleep(poll_seconds)


def parse_restart_counts(pods_json_path):
    payload = json.loads(pods_json_path.read_text(encoding="utf-8"))
    counts = {}
    for item in payload.get("items", []):
        name = item["metadata"]["name"]
        base = re.sub(r"-[a-z0-9]{5,}$", "", name)
        container_statuses = item.get("status", {}).get("containerStatuses", [])
        restart_count = sum(status.get("restartCount", 0) for status in container_statuses)
        counts[base] = restart_count
    return counts


def parse_hpa_state(hpa_path):
    if not hpa_path.exists():
        return {"present": False, "raw": ""}
    raw = hpa_path.read_text(encoding="utf-8").strip()
    lines = [line for line in raw.splitlines() if line.strip()]
    if len(lines) <= 1:
        return {"present": False, "raw": raw}
    return {"present": True, "raw": raw}


def parse_peak_resources(top_paths):
    peaks = defaultdict(lambda: {"cpu_m": 0, "memory_mib": 0})
    for path in top_paths:
        lines = [line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
        if len(lines) <= 1:
            continue
        for line in lines[1:]:
            parts = re.split(r"\s+", line)
            if len(parts) < 3:
                continue
            name, cpu_raw, mem_raw = parts[:3]
            base = re.sub(r"-[a-z0-9]{5,}$", "", name)
            peaks[base]["cpu_m"] = max(peaks[base]["cpu_m"], parse_cpu_m(cpu_raw))
            peaks[base]["memory_mib"] = max(peaks[base]["memory_mib"], parse_memory_mib(mem_raw))
    result = {}
    for base, values in sorted(peaks.items()):
        result[base] = {
            "cpu": f"{values['cpu_m']}m",
            "memory": f"{values['memory_mib']}Mi",
        }
    return result


def parse_cpu_m(raw):
    raw = raw.strip().lower()
    if raw.endswith("m"):
        return int(raw[:-1])
    return int(float(raw) * 1000)


def parse_memory_mib(raw):
    raw = raw.strip()
    if raw.endswith("Mi"):
        return int(float(raw[:-2]))
    if raw.endswith("Gi"):
        return int(float(raw[:-2]) * 1024)
    if raw.endswith("Ki"):
        return int(float(raw[:-2]) / 1024)
    return int(float(raw))


def merge_phase_data(shard_summaries):
    merged = {}
    for summary in shard_summaries:
        for phase in summary["phases"]:
            entry = merged.setdefault(phase["name"], {
                "name": phase["name"],
                "user_count": 0,
                "per_user_rate": phase["per_user_rate"],
                "aggregate_target_rate": 0,
                "duration_seconds": phase["duration_seconds"],
                "requested": 0,
                "accepted": 0,
                "failed_sends_by_reason": Counter(),
            })
            entry["user_count"] += phase["user_count"]
            entry["aggregate_target_rate"] += phase["aggregate_target_rate"]
            entry["requested"] += phase["requested"]
            entry["accepted"] += phase["accepted"]
            entry["failed_sends_by_reason"].update(phase.get("send_failures", {}))
    output = []
    for name in sorted(merged.keys()):
        phase = merged[name]
        phase["failed_sends_by_reason"] = dict(sorted(phase["failed_sends_by_reason"].items()))
        output.append(phase)
    return output


def build_supported_claim(status, aggregate_rate, source_ip_count, requested, accepted, pg_delta, cass_delta, lag_info):
    if status == "passed":
        return (
            f"Sustained {aggregate_rate} aggregate msg/sec through the local Kubernetes full pipeline across "
            f"{source_ip_count} source IPs, with {accepted}/{requested} accepted messages reconciling to "
            f"Postgres ({pg_delta}) and Cassandra ({cass_delta}) and Kafka lag returning to zero "
            f"in {lag_info['lag_zero_seconds']:.2f} seconds."
        )
    return (
        f"The multisource local Kubernetes run did not establish a passing {aggregate_rate} msg/sec aggregate claim. "
        f"It remains useful only as bounded evidence for the accepted subset: {accepted}/{requested} accepted, "
        f"Postgres delta {pg_delta}, Cassandra delta {cass_delta}, Kafka lag settled_to_zero={lag_info['lag_settled_to_zero']}."
    )


def render_summary_markdown(summary):
    per_pod_rate_label = summary["run"].get("per_pod_rate_label", f"{summary['run']['per_pod_rate']} msg/sec")
    lines = [
        f"# {summary['scenario']}",
        "",
        "## Outcome",
        "",
        f"- Status: `{summary['status']}`",
        f"- Loadgen pods: `{summary['run']['loadgen_pods']}`",
        f"- Unique source IPs: `{summary['run']['source_ip_count']}`",
        f"- Per-pod target: `{per_pod_rate_label}`",
        f"- Aggregate target rate: `{summary['run']['aggregate_target_rate']} msg/sec`",
        f"- Main-phase duration: `{summary['run']['main_duration_seconds']}s`",
        "",
        "## Counts",
        "",
        f"- Requested: `{summary['counts']['requested']}`",
        f"- Sent attempts: `{summary['counts']['sent_attempts']}`",
        f"- Accepted: `{summary['counts']['accepted']}`",
        f"- Failed: `{summary['counts']['failed']}`",
        f"- Duplicates: `{summary['counts']['duplicates']}`",
        f"- Missing: `{summary['counts']['missing']}`",
        f"- Postgres delta: `{summary['counts']['postgres_delta']}`",
        f"- Cassandra delta: `{summary['counts']['cassandra_delta']}`",
        "",
        "## Latency",
        "",
        f"- Label: `{summary['latency']['label']}`",
        f"- Sample count: `{summary['latency']['sample_count']}`",
        f"- p50: `{summary['latency']['p50_ms']:.3f} ms`",
        f"- p95: `{summary['latency']['p95_ms']:.3f} ms`",
        f"- p99: `{summary['latency']['p99_ms']:.3f} ms`",
        "",
        "## Kafka",
        "",
        f"- Lag settled to zero: `{summary['kafka']['lag_settled_to_zero']}`",
        f"- Lag zero seconds: `{summary['kafka']['lag_zero_seconds']:.3f}`",
        f"- Lag at end: `{summary['kafka']['lag_at_end']}`",
        "",
        "## Supported claim",
        "",
        summary["supported_claim"],
        "",
    ]
    if summary["counts"]["failed_sends_by_reason"]:
        lines.extend(["## Failures", ""])
        for reason, value in summary["counts"]["failed_sends_by_reason"].items():
            lines.append(f"- `{reason}`: `{value}`")
        lines.append("")
    lines.extend(["## Unsupported claims", ""])
    for claim in summary["unsupported_claims"]:
        lines.append(f"- {claim}")
    lines.append("")
    return "\n".join(lines)


def render_deploy_markdown(summary, benchmark_summary_path, title):
    per_pod_rate_label = summary["run"].get("per_pod_rate_label", f"{summary['run']['per_pod_rate']} msg/sec")
    lines = [
        f"# {title}",
        "",
        "## Outcome",
        "",
        f"Stage B1 multisource **{summary['status']}**.",
        "",
        f"- Loadgen pods / source IPs observed: `{summary['run']['loadgen_pods']}` / `{summary['run']['source_ip_count']}`",
        f"- Per-pod target: `{per_pod_rate_label}`",
        f"- Aggregate target: `{summary['run']['aggregate_target_rate']} msg/sec`",
        f"- Main-phase duration: `{summary['run']['main_duration_seconds']}s`",
        f"- Requested / sent / accepted: `{summary['counts']['requested']}` / `{summary['counts']['sent_attempts']}` / `{summary['counts']['accepted']}`",
        f"- Postgres / Cassandra delta: `{summary['counts']['postgres_delta']}` / `{summary['counts']['cassandra_delta']}`",
        f"- Missing / duplicates: `{summary['counts']['missing']}` / `{summary['counts']['duplicates']}`",
        f"- Kafka lag settled to zero: `{summary['kafka']['lag_settled_to_zero']}` in `{summary['kafka']['lag_zero_seconds']:.3f}s`",
        "",
        "## Claim boundary",
        "",
        summary["supported_claim"],
        "",
        "Unsupported:",
        "",
    ]
    for claim in summary["unsupported_claims"]:
        lines.append(f"- {claim}")
    lines.extend([
        "",
        "## Paired artifact",
        "",
        f"- Benchmark summary: [{benchmark_summary_path.name}]({benchmark_summary_path.as_posix()})",
        "",
    ])
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--result-dir", required=True)
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--job-name", required=True)
    parser.add_argument("--loadgen-pods", required=True, type=int)
    parser.add_argument("--per-pod-rate", required=True, type=int)
    parser.add_argument("--per-pod-rate-label")
    parser.add_argument("--main-duration-seconds", required=True, type=int)
    parser.add_argument("--aggregate-target-rate", required=True, type=int)
    parser.add_argument("--overlay", required=True)
    parser.add_argument("--cluster-name", required=True)
    parser.add_argument("--cluster-context", required=True)
    parser.add_argument("--system-under-test-commit", required=True)
    parser.add_argument("--artifact-head", required=True)
    parser.add_argument("--run-id-prefix", required=True)
    parser.add_argument("--run-date", required=True)
    parser.add_argument("--principal-provisioning-mode", required=True)
    parser.add_argument("--scenario-name", default="sustained-120msgsec-multisource")
    parser.add_argument("--deploy-result-md")
    parser.add_argument("--deploy-title")
    parser.add_argument("--kafka-consumer-group", default="messages-processor-v1")
    parser.add_argument("--kafka-topic", default="msg.ingress.v1")
    parser.add_argument("--cassandra-keyspace", default="ohmf_messages")
    parser.add_argument("--kafka-timeout-seconds", default=180, type=int)
    parser.add_argument("--kafka-poll-seconds", default=2, type=int)
    args = parser.parse_args()

    result_dir = Path(args.result_dir)
    observations_dir = result_dir / "observations"
    shards_dir = result_dir / "shards"
    observations_dir.mkdir(parents=True, exist_ok=True)
    shards_dir.mkdir(parents=True, exist_ok=True)

    shard_summaries = []
    latency_samples = []
    all_conversations = []
    for log_path in sorted(shards_dir.glob("*.log")):
        raw = log_path.read_text(encoding="utf-8")
        summary = parse_summary_from_log(raw)
        shard_name = log_path.stem
        (shards_dir / f"{shard_name}.summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
        shard_summaries.append(summary)
        latency_samples.extend(summary.get("latency_samples_ms", []))
        all_conversations.extend(conversation["conversation_id"] for conversation in summary.get("conversations", []))

    if len(shard_summaries) != args.loadgen_pods:
        raise SystemExit(f"expected {args.loadgen_pods} shard summaries, found {len(shard_summaries)}")

    all_conversations = sorted(set(all_conversations))
    phase_data = merge_phase_data(shard_summaries)
    failures = Counter()
    requested = 0
    sent_attempts = 0
    accepted = 0
    accepted_responses = 0
    duplicates = 0
    for summary in shard_summaries:
        counts = summary["counts"]
        requested += counts["requested"]
        sent_attempts += counts["sent_attempts"]
        accepted += counts["accepted"]
        accepted_responses += counts["accepted_responses"]
        duplicates += counts["duplicates"]
        failures.update(counts.get("send_failures", {}))

    postgres_delta = postgres_count(args.namespace, all_conversations)
    cassandra_delta = cassandra_count(args.namespace, all_conversations, args.cassandra_keyspace)
    lag_info = wait_for_kafka_zero(
        args.namespace,
        args.kafka_consumer_group,
        args.kafka_topic,
        observations_dir,
        args.kafka_timeout_seconds,
        args.kafka_poll_seconds,
    )

    missing = accepted - postgres_delta
    loadgen_pods_json = json.loads((observations_dir / "loadgen-pods.json").read_text(encoding="utf-8"))
    source_ips = sorted({
        item.get("status", {}).get("podIP", "")
        for item in loadgen_pods_json.get("items", [])
        if item.get("status", {}).get("podIP")
    })

    top_paths = sorted(observations_dir.glob("top-pods-*.txt"))
    peaks = parse_peak_resources(top_paths)
    restarts_before = parse_restart_counts(observations_dir / "pods-before.json")
    restarts_after = parse_restart_counts(observations_dir / "pods-after.json")
    hpa_state = parse_hpa_state(observations_dir / "hpa-after.txt")

    latency = {
        "label": "client-observed HTTP accept latency",
        "calculation": "exact from merged shard accept samples",
        "sample_count": len(latency_samples),
        "p50_ms": round(percentile(latency_samples, 50), 6),
        "p95_ms": round(percentile(latency_samples, 95), 6),
        "p99_ms": round(percentile(latency_samples, 99), 6),
    }

    status = "passed"
    if failures:
        if all(reason.startswith("http_429") for reason in failures):
            status = "failed_limited"
        else:
            status = "failed"
    if accepted != requested or postgres_delta != accepted or cassandra_delta != accepted or missing != 0 or duplicates != 0:
        if status == "passed":
            status = "failed"
    if not lag_info["lag_settled_to_zero"]:
        status = "failed"

    supported_claim = build_supported_claim(
        status,
        args.aggregate_target_rate,
        len(source_ips),
        requested,
        accepted,
        postgres_delta,
        cassandra_delta,
        lag_info,
    )

    deploy_md_path = Path(args.deploy_result_md) if args.deploy_result_md else Path("deploy") / "k8s" / "results" / f"{args.run_date}-throughput-120msgsec-multisource.md"
    deploy_title = args.deploy_title or f"Local k3s throughput result - {args.aggregate_target_rate} msg/sec multisource - {args.run_date}"
    per_pod_rate_label = args.per_pod_rate_label or f"{args.per_pod_rate} msg/sec"

    unsupported_claims = [
        "Any p95 or p99 delivery latency claim",
        "3,100 concurrent clients",
        "Production readiness",
        "HA or failover",
        "Any cloud or production benchmark interpretation",
    ]
    if status != "passed":
        unsupported_claims.insert(0, f"OHMF can sustain {args.aggregate_target_rate} accepted messages/sec aggregate in this local configuration")

    summary = {
        "scenario": args.scenario_name,
        "stage": "B1",
        "status": status,
        "scope": "local single-node Kubernetes only",
        "result_dir": str(result_dir),
        "run": {
            "run_id_prefix": args.run_id_prefix,
            "run_date": args.run_date,
            "system_under_test_commit": args.system_under_test_commit,
            "artifact_generation_head": args.artifact_head,
            "overlay": args.overlay,
            "cluster_name": args.cluster_name,
            "cluster_context": args.cluster_context,
            "driver_location": "in-cluster multisource",
            "principal_provisioning_mode": args.principal_provisioning_mode,
            "loadgen_pods": args.loadgen_pods,
            "source_ip_count": len(source_ips),
            "source_ips": source_ips,
            "per_pod_rate": args.per_pod_rate,
            "per_pod_rate_label": per_pod_rate_label,
            "aggregate_target_rate": args.aggregate_target_rate,
            "main_duration_seconds": args.main_duration_seconds,
        },
        "phases": phase_data,
        "counts": {
            "requested": requested,
            "sent_attempts": sent_attempts,
            "accepted": accepted,
            "accepted_responses": accepted_responses,
            "failed": sent_attempts - accepted_responses,
            "failed_sends_by_reason": dict(sorted(failures.items())),
            "duplicates": duplicates,
            "missing": missing,
            "postgres_delta": postgres_delta,
            "cassandra_delta": cassandra_delta,
        },
        "latency": latency,
        "kafka": {
            "consumer_group": args.kafka_consumer_group,
            "lag_settled_to_zero": lag_info["lag_settled_to_zero"],
            "lag_zero_seconds": lag_info["lag_zero_seconds"],
            "lag_at_end": lag_info["lag_at_end"],
            "limitation": "Kafka lag reconciliation is consumer-group scoped, not run scoped.",
        },
        "pods": {
            "restarts_before": restarts_before,
            "restarts_after": restarts_after,
            "hpa_present": hpa_state["present"],
            "hpa_state_raw": hpa_state["raw"],
        },
        "resource_observations": {
            "peak_sampled_during_run": peaks,
        },
        "reconciliation": {
            "mode": "run-scoped by fresh test conversation IDs aggregated across shards",
            "conversation_count": len(all_conversations),
        },
        "supported_claim": supported_claim,
        "unsupported_claims": unsupported_claims,
        "artifacts": {
            "human_summary_md": str(result_dir / "summary.md"),
            "deploy_result_md": str(deploy_md_path),
            "observations_dir": str(observations_dir),
            "shards_dir": str(shards_dir),
        },
    }

    summary_path = result_dir / "summary.json"
    summary_md_path = result_dir / "summary.md"

    summary_path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    summary_md_path.write_text(render_summary_markdown(summary), encoding="utf-8")
    deploy_md_path.write_text(render_deploy_markdown(summary, summary_md_path, deploy_title), encoding="utf-8")


if __name__ == "__main__":
    try:
        main()
    except subprocess.CalledProcessError as exc:
        sys.stderr.write(exc.stdout or "")
        sys.stderr.write(exc.stderr or "")
        raise
