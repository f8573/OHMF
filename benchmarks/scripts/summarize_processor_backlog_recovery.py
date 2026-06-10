#!/usr/bin/env python3

import argparse
import json
import math
import re
import subprocess
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path


METRIC_LINE = re.compile(r'^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{[^}]*\})?\s+(-?[0-9.eE+-]+)$')
STAGE_FAILURE_LINE = re.compile(
    r"stage failure stage=(?P<stage>\S+) target=(?P<target>\S+) "
    r"partial_persistence=(?P<partial>\w+) event_id=(?P<event_id>\S+) "
    r"message_id=(?P<message_id>\S+) conversation_id=(?P<conversation_id>\S+) "
    r"sender_user_id=(?P<sender_user_id>\S+) idempotency_key=(?P<idempotency_key>\S+) "
    r"client_generated_id=(?P<client_generated_id>\S+) run_scope=(?P<run_scope>\S+) err=(?P<err>.*)$"
)
PROCESS_FAILED_LINE = re.compile(r"process failed: (?P<err>.*)$")
COMMIT_FAILED_LINE = re.compile(r"commit failed: (?P<err>.*)$")


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


def parse_metrics_text(path):
    metrics = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        match = METRIC_LINE.match(line)
        if not match:
            continue
        name, label_blob, value_raw = match.groups()
        labels = {}
        if label_blob:
            for item in label_blob.strip("{}").split(","):
                if not item:
                    continue
                key, value = item.split("=", 1)
                labels[key] = value.strip('"')
        metrics[(name, tuple(sorted(labels.items())))] = float(value_raw)
    return metrics


def metric_value(metrics, name, **labels):
    key = (name, tuple(sorted(labels.items())))
    return int(round(metrics.get(key, 0.0)))


def extract_stage_counters(metrics):
    stages = [
        ("consumed", dict(stage="kafka_consume", outcome="succeeded", target="")),
        ("decoded", dict(stage="decode", outcome="succeeded", target="")),
        ("deduped", dict(stage="dedupe", outcome="skipped", target="")),
        ("postgres_write_succeeded", dict(stage="postgres_write", outcome="succeeded", target="")),
        ("postgres_write_failed", dict(stage="postgres_write", outcome="failed", target="")),
        ("cassandra_write_succeeded", dict(stage="cassandra_write", outcome="succeeded", target="")),
        ("cassandra_write_failed", dict(stage="cassandra_write", outcome="failed", target="")),
        ("redis_ack_set_succeeded", dict(stage="redis_ack", outcome="succeeded", target="set")),
        ("redis_ack_set_failed", dict(stage="redis_ack", outcome="failed", target="set")),
        ("persisted_publish_succeeded", dict(stage="downstream_publish", outcome="succeeded", target="persisted")),
        ("microservice_publish_succeeded", dict(stage="downstream_publish", outcome="succeeded", target="microservice")),
        ("offset_commit_succeeded", dict(stage="kafka_offset_commit", outcome="succeeded", target="")),
        ("handler_success", dict(stage="handler_return", outcome="success", target="")),
        ("handler_error", dict(stage="handler_return", outcome="error", target="")),
    ]
    return {
        name: metric_value(metrics, "ohmf_messages_processor_stage_events_total", **labels)
        for name, labels in stages
    }


def parse_snapshot_metrics(observations_dir, prefix):
    snapshots = {}
    for path in observations_dir.glob(f"messages-processor-metrics-{prefix}-*.txt"):
        pod_name = path.name[len(f"messages-processor-metrics-{prefix}-"):-4]
        snapshots[pod_name] = parse_metrics_text(path)
    return snapshots


def parse_restart_counts(pods_json_path):
    payload = json.loads(pods_json_path.read_text(encoding="utf-8"))
    counts = {}
    for item in payload.get("items", []):
        name = item["metadata"]["name"]
        container_statuses = item.get("status", {}).get("containerStatuses", [])
        restart_count = sum(status.get("restartCount", 0) for status in container_statuses)
        counts[name] = restart_count
    return counts


def compute_restart_deltas(before_counts, settled_counts):
    deltas = {}
    for pod_name in sorted(set(before_counts) | set(settled_counts)):
        delta = settled_counts.get(pod_name, 0) - before_counts.get(pod_name, 0)
        if delta != 0:
            deltas[pod_name] = delta
    return deltas


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
            peaks[name]["cpu_m"] = max(peaks[name]["cpu_m"], parse_cpu_m(cpu_raw))
            peaks[name]["memory_mib"] = max(peaks[name]["memory_mib"], parse_memory_mib(mem_raw))
    result = {}
    for name, values in sorted(peaks.items()):
        result[name] = {
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


def parse_consumer_group(path):
    raw = path.read_text(encoding="utf-8")
    assignments = {}
    total = 0
    assigned = 0
    for raw_line in raw.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("GROUP") or line.startswith("Warning:"):
            continue
        fields = re.split(r"\s+", line)
        if len(fields) < 9 or fields[0] != "messages-processor-v1" or fields[1] != "msg.ingress.v1":
            continue
        total += 1
        member = fields[6]
        if member == "-":
            continue
        assigned += 1
        assignments.setdefault(member, 0)
        assignments[member] += 1
    return {
        "raw": raw,
        "total_partitions": total,
        "assigned_partitions": assigned,
        "unique_members": sorted(assignments.keys()),
        "member_partition_counts": dict(sorted(assignments.items())),
    }


def parse_lag_from_consumer_group_text(raw):
    lag = 0
    for raw_line in raw.splitlines():
        fields = re.split(r"\s+", raw_line.strip())
        if len(fields) < 6 or fields[0] != "messages-processor-v1" or fields[1] != "msg.ingress.v1":
            continue
        try:
            lag += int(fields[5])
        except ValueError:
            continue
    return lag


def parse_lag_timeline(observations_dir):
    samples = []
    for path in sorted(observations_dir.glob("kafka-consumer-group-poll-*.txt")):
        match = re.match(r"kafka-consumer-group-poll-(\d+)-(\d{6})\.txt$", path.name)
        stamp = match.group(2) if match else ""
        group = parse_consumer_group(path)
        lag = parse_lag_from_consumer_group_text(group["raw"])
        samples.append({
            "file": path.name,
            "stamp": stamp,
            "lag": lag,
            "consumer_group": group,
        })
    return samples


def extract_lag_phases(lag_samples, scale_up_offset_seconds, main_duration_seconds):
    if not lag_samples:
        return {"peak_during_outage": 0, "lag_at_scaleup_approx": 0}
    lags = [s["lag"] for s in lag_samples]
    # Samples are ordered chronologically; approximate scale-up position
    total_samples = len(lag_samples)
    if total_samples > 1 and main_duration_seconds > 0:
        scaleup_fraction = scale_up_offset_seconds / main_duration_seconds
        scaleup_index = min(int(total_samples * scaleup_fraction), total_samples - 1)
        outage_lags = lags[:scaleup_index] if scaleup_index > 0 else lags[:1]
        peak_during_outage = max(outage_lags) if outage_lags else 0
        lag_at_scaleup = lags[scaleup_index]
    else:
        peak_during_outage = max(lags)
        lag_at_scaleup = lags[-1]
    return {
        "peak_during_outage": peak_during_outage,
        "lag_at_scaleup_approx": lag_at_scaleup,
    }


def parse_processor_diagnostics(observations_dir):
    stage_failures = Counter()
    process_failures = Counter()
    commit_failures = Counter()
    idempotency_index = {}

    for path in sorted(observations_dir.glob("messages-processor-logs-*.txt")):
        raw = path.read_text(encoding="utf-8")
        for line in raw.splitlines():
            stage_match = STAGE_FAILURE_LINE.search(line)
            if stage_match:
                stage = stage_match.group("stage")
                target = stage_match.group("target")
                err = stage_match.group("err").strip()
                key = stage_match.group("idempotency_key")
                stage_failures[(stage, target, err)] += 1
                idempotency_index[key] = {
                    "stage": stage,
                    "target": target,
                    "partial_persistence": stage_match.group("partial") == "true",
                    "err": err,
                    "message_id": stage_match.group("message_id"),
                    "event_id": stage_match.group("event_id"),
                    "conversation_id": stage_match.group("conversation_id"),
                    "sender_user_id": stage_match.group("sender_user_id"),
                    "client_generated_id": stage_match.group("client_generated_id"),
                    "run_scope": stage_match.group("run_scope"),
                }
                continue
            process_match = PROCESS_FAILED_LINE.search(line)
            if process_match:
                process_failures[process_match.group("err").strip()] += 1
                continue
            commit_match = COMMIT_FAILED_LINE.search(line)
            if commit_match:
                commit_failures[commit_match.group("err").strip()] += 1

    return {
        "stage_failures": stage_failures,
        "process_failures": process_failures,
        "commit_failures": commit_failures,
        "idempotency_index": idempotency_index,
    }


def load_json_if_exists(path):
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def count_by_run_prefix(namespace, run_prefix):
    query = f"SELECT COUNT(*) FROM messages WHERE client_generated_id LIKE '{run_prefix}-%';"
    raw = run_command([
        "kubectl", "-n", namespace, "exec", "deploy/postgres", "--", "sh", "-lc",
        f'psql -U ohmf -d ohmf -t -A -c "{query}"',
    ])
    return parse_trailing_int(raw)


def cassandra_count_by_run_prefix(namespace, conversations, keyspace, run_prefix):
    bucket = datetime.now(timezone.utc).strftime("%Y%m%d")
    total = 0
    for conversation_id in conversations:
        query = (
            "PAGING OFF; CONSISTENCY ONE; "
            f"SELECT content_json FROM {keyspace}.messages_by_conversation "
            f"WHERE conversation_id = {conversation_id} AND bucket_yyyymmdd = '{bucket}';"
        )
        raw = run_command([
            "kubectl", "-n", namespace, "exec", "deploy/cassandra", "--", "sh", "-lc",
            f"/opt/cassandra/bin/cqlsh -e \"{query}\"",
        ])
        total += sum(1 for line in raw.splitlines() if run_prefix in line)
    return total


def render_stage_table(stage_by_pod, aggregate):
    order = [
        "consumed",
        "decoded",
        "deduped",
        "postgres_write_succeeded",
        "postgres_write_failed",
        "cassandra_write_succeeded",
        "cassandra_write_failed",
        "redis_ack_set_succeeded",
        "redis_ack_set_failed",
        "persisted_publish_succeeded",
        "microservice_publish_succeeded",
        "offset_commit_succeeded",
        "handler_success",
        "handler_error",
    ]
    lines = [
        "| Pod | " + " | ".join(order) + " |",
        "| --- | " + " | ".join(["---:"] * len(order)) + " |",
    ]
    for pod_name in sorted(stage_by_pod.keys()):
        counters = stage_by_pod[pod_name]
        lines.append("| " + pod_name + " | " + " | ".join(str(counters.get(name, 0)) for name in order) + " |")
    lines.append("| aggregate | " + " | ".join(str(aggregate.get(name, 0)) for name in order) + " |")
    return "\n".join(lines)


def build_supported_claim(status, aggregate_target_rate):
    if status == "passed":
        return (
            f"Validated {aggregate_target_rate} msg/sec local Kubernetes backlog recovery: "
            "Kafka consumer group scaled to 0, backlog accumulated, processors restored to 4 replicas, "
            "Kafka lag drained to zero, and exact full-pipeline reconciliation confirmed."
        )
    return "The backlog recovery run did not establish the supported full-pipeline claim."


def render_summary_markdown(summary):
    sd = summary["scale_events"]["scale_down"]
    su = summary["scale_events"]["scale_up"]
    lines = [
        f"# {summary['scenario']}",
        "",
        "## Outcome",
        "",
        f"- Status: `{summary['status']}`",
        f"- Scale-down timestamp: `{sd.get('scale_down_at_utc', 'n/a')}`",
        f"- Scale-up timestamp: `{su.get('scale_up_at_utc', 'n/a')}`",
        f"- Scale-up offset: `{su.get('scale_up_offset_seconds', 0)}s` into loadgen",
        f"- Loadgen pods: `{summary['run']['loadgen_pods']}`",
        f"- Per-pod target: `{summary['run']['per_pod_rate_label']}`",
        f"- Aggregate target rate: `{summary['run']['aggregate_target_rate']} msg/sec`",
        f"- Main-phase duration: `{summary['run']['main_duration_seconds']}s`",
        "",
        "## Counts",
        "",
        f"- Requested: `{summary['counts']['requested']}`",
        f"- Sent attempts: `{summary['counts']['sent_attempts']}`",
        f"- Accepted: `{summary['counts']['accepted']}`",
        f"- Failed: `{summary['counts']['failed']}`",
        f"- Postgres exact: `{summary['counts']['postgres_exact']}`",
        f"- Cassandra exact: `{summary['counts']['cassandra_exact']}`",
        f"- Missing: `{summary['counts']['missing']}`",
        f"- Duplicates: `{summary['counts']['duplicates']}`",
        "",
        "## Kafka backlog and recovery",
        "",
        f"- Peak lag during processor outage: `{summary['kafka']['peak_lag_during_outage']}`",
        f"- Lag at approximate scale-up time: `{summary['kafka']['lag_at_scaleup_approx']}`",
        f"- Lag settled to zero: `{summary['kafka']['lag_settled_to_zero']}`",
        f"- Lag zero seconds (from post-loadgen drain call): `{summary['kafka']['lag_zero_seconds']:.3f}`",
        f"- Lag at end: `{summary['kafka']['lag_at_end']}`",
        "",
        "## Consumer group",
        "",
        f"- Before scale-down (normal): `{summary['assignments']['before']['unique_members']}`",
        f"- During processor outage: `{summary['assignments']['during_outage']['unique_members']}`",
        f"- After scale-up (recovery): `{summary['assignments']['after_scaleup']['unique_members']}`",
        f"- After full drain (settled): `{summary['assignments']['settled']['unique_members']}`",
        "",
        "## Restarts",
        "",
        f"- Before: `{summary['pods']['restarts_before']}`",
        f"- After (settled): `{summary['pods']['restarts_after']}`",
        f"- New restarts delta: `{summary['pods']['restarts_delta']}`",
        "",
        "## Resources",
        "",
        f"- Peak sampled CPU / RAM: `{summary['pods']['peak_sampled_during_run']}`",
        "",
        "## Stage counters",
        "",
        render_stage_table(summary["stage_counters"]["by_pod"], summary["stage_counters"]["aggregate"]),
        "",
        "## Diagnostics",
        "",
        f"- Gateway 500 cause: `{summary['diagnostics']['gateway_500_cause']}`",
        f"- Processor log correlation sample (tail-limited): `{summary['diagnostics']['failed_sends_with_message_id']}` / `{summary['counts']['failed']}`",
        f"- Failed sends durably persisted lower bound: `{summary['diagnostics']['failed_sends_durably_persisted_lower_bound']}`",
        f"- Processor handler error reason: `{summary['diagnostics']['handler_error_reason']}`",
        f"- Redis ack failure reason: `{summary['diagnostics']['redis_ack_failure_reason']}`",
        f"- Kafka offset commit failure reason: `{summary['diagnostics']['kafka_offset_commit_failure_reason']}`",
        f"- Snapshot labels captured: `{summary['diagnostics']['snapshot_labels']}`",
        "",
        "## Artifact paths",
        "",
        f"- `kubectl get pods -o wide`: `{summary['artifacts']['pods_wide_after']}`",
        f"- `kubectl top pods`: `{summary['artifacts']['top_pods_after']}`",
        f"- Processor logs: `{summary['artifacts']['processor_logs_dir']}`",
        f"- Gateway logs: `{summary['artifacts']['gateway_logs_dir']}`",
        f"- Kubernetes events: `{summary['artifacts']['events_dir']}`",
        "",
        "## Supported claim",
        "",
        summary["supported_claim"],
        "",
        "## Unsupported claims",
        "",
    ]
    for claim in summary["unsupported_claims"]:
        lines.append(f"- {claim}")
    lines.append("")
    return "\n".join(lines)


def render_deploy_markdown(summary, benchmark_summary_path, title):
    sd = summary["scale_events"]["scale_down"]
    su = summary["scale_events"]["scale_up"]
    lines = [
        f"# {title}",
        "",
        "## Outcome",
        "",
        f"Backlog recovery run **{summary['status']}**.",
        "",
        f"- Scale-down timestamp: `{sd.get('scale_down_at_utc', 'n/a')}`",
        f"- Scale-up timestamp: `{su.get('scale_up_at_utc', 'n/a')}`",
        f"- Scale-up offset into loadgen: `{su.get('scale_up_offset_seconds', 0)}s`",
        f"- Requested / sent / accepted: `{summary['counts']['requested']}` / `{summary['counts']['sent_attempts']}` / `{summary['counts']['accepted']}`",
        f"- Postgres / Cassandra exact: `{summary['counts']['postgres_exact']}` / `{summary['counts']['cassandra_exact']}`",
        f"- Missing / duplicates: `{summary['counts']['missing']}` / `{summary['counts']['duplicates']}`",
        f"- Peak Kafka lag during outage: `{summary['kafka']['peak_lag_during_outage']}`",
        f"- Kafka lag settled to zero: `{summary['kafka']['lag_settled_to_zero']}` in `{summary['kafka']['lag_zero_seconds']:.3f}s` after loadgen",
        "",
        "## Consumer group",
        "",
        f"- Before scale-down: `{summary['assignments']['before']['unique_members']}`",
        f"- During outage: `{summary['assignments']['during_outage']['unique_members']}`",
        f"- After scale-up: `{summary['assignments']['after_scaleup']['unique_members']}`",
        f"- Settled: `{summary['assignments']['settled']['unique_members']}`",
        "",
        "## Pod restarts",
        "",
        f"- Before: `{summary['pods']['restarts_before']}`",
        f"- After (settled): `{summary['pods']['restarts_after']}`",
        f"- New restarts delta: `{summary['pods']['restarts_delta']}`",
        "",
        "## CPU / RAM",
        "",
        f"- Peak sampled resources: `{summary['pods']['peak_sampled_during_run']}`",
        "",
        "## Claim boundary",
        "",
        summary["supported_claim"],
        "",
        "## Stage counters",
        "",
        render_stage_table(summary["stage_counters"]["by_pod"], summary["stage_counters"]["aggregate"]),
        "",
        "## Diagnostics",
        "",
        f"- Gateway 500 cause: `{summary['diagnostics']['gateway_500_cause']}`",
        f"- Processor handler error reason: `{summary['diagnostics']['handler_error_reason']}`",
        f"- Redis ack failure reason: `{summary['diagnostics']['redis_ack_failure_reason']}`",
        f"- Kafka offset commit failure reason: `{summary['diagnostics']['kafka_offset_commit_failure_reason']}`",
        f"- Snapshot labels captured: `{summary['diagnostics']['snapshot_labels']}`",
        "",
        "## Paired artifact",
        "",
        f"- Benchmark summary: [{benchmark_summary_path.name}]({benchmark_summary_path.as_posix()})",
        "",
    ]
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--result-dir", required=True)
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--job-name", required=True)
    parser.add_argument("--loadgen-pods", required=True, type=int)
    parser.add_argument("--per-pod-rate", required=True, type=int)
    parser.add_argument("--per-pod-rate-label", required=True)
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
    parser.add_argument("--scenario-name", required=True)
    parser.add_argument("--deploy-result-md", required=True)
    parser.add_argument("--deploy-title", required=True)
    parser.add_argument("--kafka-timeout-seconds", required=True, type=int)
    parser.add_argument("--kafka-poll-seconds", required=True, type=int)
    parser.add_argument("--scale-up-offset-seconds", required=True, type=int)
    parser.add_argument("--run-status", required=True)
    parser.add_argument("--cassandra-keyspace", default="ohmf_messages")
    parser.add_argument("--kafka-consumer-group", default="messages-processor-v1")
    parser.add_argument("--kafka-topic", default="msg.ingress.v1")
    args = parser.parse_args()

    result_dir = Path(args.result_dir)
    observations_dir = result_dir / "observations"
    shards_dir = result_dir / "shards"
    observations_dir.mkdir(parents=True, exist_ok=True)
    shards_dir.mkdir(parents=True, exist_ok=True)

    shard_summaries = []
    latency_samples = []
    conversations = []
    failed_sends = []
    for log_path in sorted(shards_dir.glob("*.log")):
        raw = log_path.read_text(encoding="utf-8")
        summary = parse_summary_from_log(raw)
        (shards_dir / f"{log_path.stem}.summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
        shard_summaries.append(summary)
        latency_samples.extend(summary.get("latency_samples_ms", []))
        conversations.extend(item["conversation_id"] for item in summary.get("conversations", []))
        failed_sends.extend(summary.get("failed_sends", []))

    if len(shard_summaries) != args.loadgen_pods:
        raise SystemExit(f"expected {args.loadgen_pods} shard summaries, found {len(shard_summaries)}")

    conversations = sorted(set(conversations))
    requested = sum(s["counts"]["requested"] for s in shard_summaries)
    sent_attempts = sum(s["counts"]["sent_attempts"] for s in shard_summaries)
    accepted = sum(s["counts"]["accepted"] for s in shard_summaries)
    accepted_responses = sum(s["counts"]["accepted_responses"] for s in shard_summaries)
    duplicates = sum(s["counts"]["duplicates"] for s in shard_summaries)
    failed_reasons = Counter()
    for s in shard_summaries:
        failed_reasons.update(s["counts"].get("send_failures", {}))

    before_snapshot = parse_snapshot_metrics(observations_dir, "before")
    settled_snapshot = parse_snapshot_metrics(observations_dir, "settled")
    post_scaleup_snapshot = parse_snapshot_metrics(observations_dir, "post-scaleup")

    before_pods = sorted(before_snapshot.keys())
    settled_pods = sorted(set(settled_snapshot.keys()) | set(post_scaleup_snapshot.keys()))
    created_pods = [pod for pod in settled_pods if pod not in before_snapshot]

    processor_diagnostics = parse_processor_diagnostics(observations_dir)

    stage_names = [
        {"name": "consumed", "stage": "kafka_consume", "outcome": "succeeded", "target": ""},
        {"name": "decoded", "stage": "decode", "outcome": "succeeded", "target": ""},
        {"name": "deduped", "stage": "dedupe", "outcome": "skipped", "target": ""},
        {"name": "postgres_write_succeeded", "stage": "postgres_write", "outcome": "succeeded", "target": ""},
        {"name": "postgres_write_failed", "stage": "postgres_write", "outcome": "failed", "target": ""},
        {"name": "cassandra_write_succeeded", "stage": "cassandra_write", "outcome": "succeeded", "target": ""},
        {"name": "cassandra_write_failed", "stage": "cassandra_write", "outcome": "failed", "target": ""},
        {"name": "redis_ack_set_succeeded", "stage": "redis_ack", "outcome": "succeeded", "target": "set"},
        {"name": "redis_ack_set_failed", "stage": "redis_ack", "outcome": "failed", "target": "set"},
        {"name": "persisted_publish_succeeded", "stage": "downstream_publish", "outcome": "succeeded", "target": "persisted"},
        {"name": "microservice_publish_succeeded", "stage": "downstream_publish", "outcome": "succeeded", "target": "microservice"},
        {"name": "offset_commit_succeeded", "stage": "kafka_offset_commit", "outcome": "succeeded", "target": ""},
        {"name": "handler_success", "stage": "handler_return", "outcome": "success", "target": ""},
        {"name": "handler_error", "stage": "handler_return", "outcome": "error", "target": ""},
    ]
    aggregate_stage = Counter()
    stage_by_pod = {}
    for pod in before_pods:
        before_metrics = before_snapshot[pod]
        end_metrics = settled_snapshot.get(pod, post_scaleup_snapshot.get(pod, before_metrics))
        pod_counts = {}
        for stage in stage_names:
            delta = (
                metric_value(end_metrics, "ohmf_messages_processor_stage_events_total",
                             stage=stage["stage"], outcome=stage["outcome"], target=stage["target"])
                - metric_value(before_metrics, "ohmf_messages_processor_stage_events_total",
                               stage=stage["stage"], outcome=stage["outcome"], target=stage["target"])
            )
            pod_counts[stage["name"]] = delta
            aggregate_stage[stage["name"]] += delta
        stage_by_pod[pod] = pod_counts
    for pod in created_pods:
        end_metrics = settled_snapshot.get(pod, post_scaleup_snapshot.get(pod, {}))
        pod_counts = {}
        for stage in stage_names:
            delta = metric_value(end_metrics, "ohmf_messages_processor_stage_events_total",
                                 stage=stage["stage"], outcome=stage["outcome"], target=stage["target"])
            pod_counts[stage["name"]] = delta
            aggregate_stage[stage["name"]] += delta
        stage_by_pod[pod] = pod_counts

    stage_counters = dict(aggregate_stage)
    for required in ("offset_commit_succeeded", "handler_error"):
        stage_counters.setdefault(required, 0)

    lag_samples = parse_lag_timeline(observations_dir)
    lag_phases = extract_lag_phases(lag_samples, args.scale_up_offset_seconds, args.main_duration_seconds)
    lag_zero_seconds = 0.0
    lag_at_end = 0
    lag_settled_to_zero = False
    lag_recovery = load_json_if_exists(observations_dir / "kafka-lag-recovery.json")
    if lag_recovery:
        lag_settled_to_zero = bool(lag_recovery.get("settled_to_zero"))
        lag_zero_seconds = float(lag_recovery.get("lag_zero_seconds", 0.0))
        lag_at_end = int(lag_recovery.get("lag_at_end", 0))
    elif lag_samples:
        lag_at_end = lag_samples[-1]["lag"]
        lag_settled_to_zero = lag_at_end == 0

    postgres_exact = count_by_run_prefix(args.namespace, args.run_id_prefix)
    cassandra_exact = cassandra_count_by_run_prefix(args.namespace, conversations, args.cassandra_keyspace, args.run_id_prefix)
    missing = accepted - postgres_exact

    loadgen_pods_json = json.loads(
        (observations_dir / "loadgen-pods.json").read_text(encoding="utf-8")
    ) if (observations_dir / "loadgen-pods.json").exists() else {"items": []}
    source_ips = sorted({
        item.get("status", {}).get("podIP", "")
        for item in loadgen_pods_json.get("items", [])
        if item.get("status", {}).get("podIP")
    })

    top_paths = sorted(observations_dir.glob("top-pods-*.txt"))
    peaks = parse_peak_resources(top_paths)
    pods_before = parse_restart_counts(observations_dir / "pods-before.json") if (observations_dir / "pods-before.json").exists() else {}
    pods_settled = parse_restart_counts(observations_dir / "pods-settled.json") if (observations_dir / "pods-settled.json").exists() else {}
    restart_deltas = compute_restart_deltas(pods_before, pods_settled)

    total_failed = sent_attempts - accepted_responses
    status = args.run_status
    if failed_reasons and status == "passed":
        if all(reason.startswith("http_429") for reason in failed_reasons):
            status = "failed_limited"
        elif total_failed or missing != 0 or duplicates != 0 or not lag_settled_to_zero:
            status = "failed"
    if not lag_settled_to_zero or accepted != requested or postgres_exact != accepted or cassandra_exact != accepted or missing != 0 or duplicates != 0 or stage_counters["handler_error"] != 0:
        if status == "passed":
            status = "failed"

    if any(delta > 0 for delta in restart_deltas.values()):
        if status == "passed":
            status = "failed"

    scale_down_event = load_json_if_exists(observations_dir / "scale-down-event.json")
    scale_up_event = load_json_if_exists(observations_dir / "scale-up-event.json")

    def _cg(label):
        path = observations_dir / f"kafka-consumer-group-{label}.txt"
        if path.exists():
            return parse_consumer_group(path)
        return {"raw": "", "total_partitions": 0, "assigned_partitions": 0, "unique_members": [], "member_partition_counts": {}}

    assignments = {
        "before": _cg("before"),
        "during_outage": _cg("scale-down"),
        "after_scaleup": _cg("post-scaleup"),
        "settled": _cg("settled"),
    }

    if (
        len(assignments["before"]["unique_members"]) != 4
        or len(assignments["settled"]["unique_members"]) != 4
        or assignments["before"]["assigned_partitions"] != assignments["before"]["total_partitions"]
        or assignments["settled"]["assigned_partitions"] != assignments["settled"]["total_partitions"]
    ):
        if status == "passed":
            status = "failed"

    enriched_failed_sends = []
    for item in failed_sends:
        enriched = dict(item)
        correlated = processor_diagnostics["idempotency_index"].get(item["idempotency_key"])
        if correlated:
            enriched["message_id"] = correlated.get("message_id", "")
            enriched["processor_stage"] = correlated.get("stage", "")
            enriched["processor_target"] = correlated.get("target", "")
            enriched["processor_error"] = correlated.get("err", "")
            enriched["partial_persistence"] = correlated.get("partial_persistence", False)
        enriched_failed_sends.append(enriched)

    gateway_500_cause = "none recorded"
    if enriched_failed_sends:
        try:
            first_failure = json.loads(enriched_failed_sends[0].get("response_body", "{}"))
            gateway_500_cause = first_failure.get("message", gateway_500_cause)
        except json.JSONDecodeError:
            pass

    failed_sends_with_message_id = sum(1 for item in enriched_failed_sends if item.get("message_id"))
    failed_sends_durably_persisted_lower_bound = max(0, max(postgres_exact, cassandra_exact) - accepted)
    processor_handler_error_reason = (
        processor_diagnostics["process_failures"].most_common(1)[0][0]
        if processor_diagnostics["process_failures"]
        else "none recorded"
    )
    redis_ack_failure_reason = (
        next(
            (
                reason
                for (stage, target, reason), count in processor_diagnostics["stage_failures"].most_common()
                if stage == "redis_ack" and target == "set" and count > 0
            ),
            "none recorded",
        )
    )
    kafka_offset_commit_failure_reason = (
        processor_diagnostics["commit_failures"].most_common(1)[0][0]
        if processor_diagnostics["commit_failures"]
        else "none recorded; gaps are retryable handler failures before commit"
    )

    snapshot_labels = ", ".join(
        label for label in ("before", "scale-down", "pre-scaleup", "post-scaleup", "during-recovery", "settled")
        if (observations_dir / f"pods-{label}.json").exists()
    )

    latency = {
        "label": "client-observed HTTP accept latency",
        "calculation": "exact from merged shard accept samples",
        "sample_count": len(latency_samples),
        "p50_ms": round(percentile(latency_samples, 50), 6),
        "p95_ms": round(percentile(latency_samples, 95), 6),
        "p99_ms": round(percentile(latency_samples, 99), 6),
    }

    unsupported_claims = [
        "Any p95 or p99 delivery latency claim",
        "Production readiness",
        "HA or automatic failover",
        "Any cloud or production benchmark interpretation",
    ]
    if args.aggregate_target_rate != 120 or status != "passed":
        unsupported_claims.insert(0, (
            "Validated 120 msg/sec local Kubernetes backlog recovery with exact full-pipeline reconciliation."
        ))

    summary = {
        "scenario": args.scenario_name,
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
            "principal_provisioning_mode": args.principal_provisioning_mode,
            "loadgen_pods": args.loadgen_pods,
            "source_ip_count": len(source_ips),
            "source_ips": source_ips,
            "per_pod_rate": args.per_pod_rate,
            "per_pod_rate_label": args.per_pod_rate_label,
            "aggregate_target_rate": args.aggregate_target_rate,
            "main_duration_seconds": args.main_duration_seconds,
            "scale_up_offset_seconds": args.scale_up_offset_seconds,
        },
        "scale_events": {
            "scale_down": scale_down_event,
            "scale_up": scale_up_event,
        },
        "counts": {
            "requested": requested,
            "sent_attempts": sent_attempts,
            "accepted": accepted,
            "accepted_responses": accepted_responses,
            "failed": total_failed,
            "failed_sends_by_reason": dict(sorted(failed_reasons.items())),
            "duplicates": duplicates,
            "missing": missing,
            "postgres_exact": postgres_exact,
            "cassandra_exact": cassandra_exact,
        },
        "latency": latency,
        "kafka": {
            "consumer_group": args.kafka_consumer_group,
            "peak_lag_during_outage": lag_phases["peak_during_outage"],
            "lag_at_scaleup_approx": lag_phases["lag_at_scaleup_approx"],
            "lag_settled_to_zero": lag_settled_to_zero,
            "lag_zero_seconds": lag_zero_seconds,
            "lag_at_end": lag_at_end,
            "timeline": lag_samples,
        },
        "assignments": assignments,
        "pods": {
            "restarts_before": pods_before,
            "restarts_after": pods_settled,
            "restarts_delta": restart_deltas,
            "peak_sampled_during_run": peaks,
        },
        "stage_counters": {
            "by_pod": stage_by_pod,
            "aggregate": stage_counters,
        },
        "supported_claim": build_supported_claim(status, args.aggregate_target_rate),
        "unsupported_claims": unsupported_claims,
        "diagnostics": {
            "gateway_500_cause": gateway_500_cause,
            "failed_sends_with_message_id": failed_sends_with_message_id,
            "failed_sends_durably_persisted_lower_bound": failed_sends_durably_persisted_lower_bound,
            "handler_error_reason": processor_handler_error_reason,
            "redis_ack_failure_reason": redis_ack_failure_reason,
            "kafka_offset_commit_failure_reason": kafka_offset_commit_failure_reason,
            "snapshot_labels": snapshot_labels,
            "failed_sends": enriched_failed_sends,
        },
        "artifacts": {
            "summary_json": str(result_dir / "summary.json"),
            "summary_md": str(result_dir / "summary.md"),
            "deploy_result_md": args.deploy_result_md,
            "observations_dir": str(observations_dir),
            "shards_dir": str(shards_dir),
            "pods_wide_after": str(observations_dir / "pods-settled.txt"),
            "top_pods_after": str(observations_dir / "top-pods-settled.txt"),
            "processor_logs_dir": str(observations_dir),
            "gateway_logs_dir": str(observations_dir),
            "events_dir": str(observations_dir),
        },
    }

    summary_path = result_dir / "summary.json"
    summary_md_path = result_dir / "summary.md"
    deploy_md_path = Path(args.deploy_result_md)
    deploy_md_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    summary_md_path.write_text(render_summary_markdown(summary), encoding="utf-8")
    deploy_md_path.write_text(render_deploy_markdown(summary, summary_md_path, args.deploy_title), encoding="utf-8")


if __name__ == "__main__":
    try:
        main()
    except subprocess.CalledProcessError as exc:
        sys.stderr.write(exc.stdout or "")
        sys.stderr.write(exc.stderr or "")
        raise
