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


def parse_lag_timeline(observations_dir):
    samples = []
    for path in sorted(observations_dir.glob("kafka-consumer-group-poll-*.txt")):
        match = re.match(r"kafka-consumer-group-poll-(\d+)-(\d{6})\.txt$", path.name)
        stamp = match.group(2) if match else ""
        group = parse_consumer_group(path)
        lag = 0
        for raw_line in group["raw"].splitlines():
            fields = re.split(r"\s+", raw_line.strip())
            if len(fields) < 6 or fields[0] != "messages-processor-v1" or fields[1] != "msg.ingress.v1":
                continue
            try:
                lag += int(fields[5])
            except ValueError:
                continue
        samples.append({
            "file": path.name,
            "stamp": stamp,
            "lag": lag,
            "consumer_group": group,
        })
    return samples


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


def shard_conversations(run_dir):
    conversations = []
    for shard_path in sorted((run_dir / "shards").glob("*.summary.json")):
        payload = json.loads(shard_path.read_text(encoding="utf-8"))
        conversations.extend(item["conversation_id"] for item in payload.get("conversations", []))
    return sorted(set(conversations))


def compute_stage_deltas(before, end, deleted_pod, created_pods, stage_names):
    pod_deltas = {}
    aggregate = Counter()

    for pod_name, before_metrics in before.items():
        if pod_name == deleted_pod:
            end_metrics = end.get(pod_name, before_metrics)
        else:
            end_metrics = end.get(pod_name, before_metrics)
        deltas = {}
        for stage_name in stage_names:
            delta = metric_value(end_metrics, "ohmf_messages_processor_stage_events_total", **stage_name) - metric_value(before_metrics, "ohmf_messages_processor_stage_events_total", **stage_name)
            deltas[stage_name["name"]] = delta
            aggregate[stage_name["name"]] += delta
        pod_deltas[pod_name] = deltas

    for pod_name in created_pods:
        end_metrics = end.get(pod_name, {})
        deltas = {}
        for stage_name in stage_names:
            delta = metric_value(end_metrics, "ohmf_messages_processor_stage_events_total", **stage_name)
            deltas[stage_name["name"]] = delta
            aggregate[stage_name["name"]] += delta
        pod_deltas[pod_name] = deltas

    return dict(aggregate), pod_deltas


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
        lines.append("| " + pod_name + " | " + " | ".join(str(counters[name]) for name in order) + " |")
    lines.append("| aggregate | " + " | ".join(str(aggregate[name]) for name in order) + " |")
    return "\n".join(lines)


def build_supported_claim(status):
    if status == "passed":
        return 'Validated 120 msg/sec local Kubernetes ingress with exact full-pipeline reconciliation during "messages-processor" pod deletion and Kafka consumer-group rebalance.'
    return 'The pod-deletion resilience run did not establish the supported full-pipeline claim.'


def render_summary_markdown(summary):
    lines = [
        f"# {summary['scenario']}",
        "",
        "## Outcome",
        "",
        f"- Status: `{summary['status']}`",
        f"- Deleted pod: `{summary['deletion']['deleted_pod']}`",
        f"- Deletion timestamp: `{summary['deletion']['deletion_requested_at_utc']}`",
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
        "## Kafka",
        "",
        f"- Lag settled to zero: `{summary['kafka']['lag_settled_to_zero']}`",
        f"- Lag zero seconds: `{summary['kafka']['lag_zero_seconds']:.3f}`",
        f"- Lag at end: `{summary['kafka']['lag_at_end']}`",
        "",
        "## Consumer group",
        "",
        f"- Before deletion: `{summary['assignments']['before']['unique_members']}`",
        f"- During rebalance: `{summary['assignments']['during']['unique_members']}`",
        f"- After recovery: `{summary['assignments']['after']['unique_members']}`",
        "",
        "## Restarts",
        "",
        f"- Before: `{summary['pods']['restarts_before']}`",
        f"- During: `{summary['pods']['restarts_during']}`",
        f"- After: `{summary['pods']['restarts_after']}`",
        "",
        "## Resources",
        "",
        f"- Peak sampled CPU / RAM: `{summary['pods']['peak_sampled_during_run']}`",
        "",
        "## Stage counters",
        "",
        render_stage_table(summary["stage_counters"]["by_pod"], summary["stage_counters"]["aggregate"]),
        "",
        "## Artifact paths",
        "",
        f"- `kubectl get pods -o wide`: `{summary['artifacts']['pods_wide_after']}`",
        f"- `kubectl top pods`: `{summary['artifacts']['top_pods_after']}`",
        f"- Processor logs around deletion/rebalance: `{summary['artifacts']['processor_logs_dir']}`",
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
    lines = [
        f"# {title}",
        "",
        "## Outcome",
        "",
        f"Pod-deletion resilience run **{summary['status']}**.",
        "",
        f"- Deleted pod: `{summary['deletion']['deleted_pod']}`",
        f"- Deletion timestamp: `{summary['deletion']['deletion_requested_at_utc']}`",
        f"- Requested / sent / accepted: `{summary['counts']['requested']}` / `{summary['counts']['sent_attempts']}` / `{summary['counts']['accepted']}`",
        f"- Postgres / Cassandra exact: `{summary['counts']['postgres_exact']}` / `{summary['counts']['cassandra_exact']}`",
        f"- Missing / duplicates: `{summary['counts']['missing']}` / `{summary['counts']['duplicates']}`",
        f"- Kafka lag settled to zero: `{summary['kafka']['lag_settled_to_zero']}` in `{summary['kafka']['lag_zero_seconds']:.3f}s`",
        "",
        "## Consumer group",
        "",
        f"- Before / during / after: `{summary['assignments']['before']['unique_members']}` / `{summary['assignments']['during']['unique_members']}` / `{summary['assignments']['after']['unique_members']}`",
        "",
        "## Pod restarts",
        "",
        f"- Before: `{summary['pods']['restarts_before']}`",
        f"- During: `{summary['pods']['restarts_during']}`",
        f"- After: `{summary['pods']['restarts_after']}`",
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
    for log_path in sorted(shards_dir.glob("*.log")):
        raw = log_path.read_text(encoding="utf-8")
        summary = parse_summary_from_log(raw)
        (shards_dir / f"{log_path.stem}.summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
        shard_summaries.append(summary)
        latency_samples.extend(summary.get("latency_samples_ms", []))
        conversations.extend(item["conversation_id"] for item in summary.get("conversations", []))

    if len(shard_summaries) != args.loadgen_pods:
        raise SystemExit(f"expected {args.loadgen_pods} shard summaries, found {len(shard_summaries)}")

    conversations = sorted(set(conversations))
    requested = sum(summary["counts"]["requested"] for summary in shard_summaries)
    sent_attempts = sum(summary["counts"]["sent_attempts"] for summary in shard_summaries)
    accepted = sum(summary["counts"]["accepted"] for summary in shard_summaries)
    accepted_responses = sum(summary["counts"]["accepted_responses"] for summary in shard_summaries)
    duplicates = sum(summary["counts"]["duplicates"] for summary in shard_summaries)
    failed_reasons = Counter()
    for summary in shard_summaries:
        failed_reasons.update(summary["counts"].get("send_failures", {}))

    before_snapshot = parse_snapshot_metrics(observations_dir, "before")
    pre_delete_snapshot = parse_snapshot_metrics(observations_dir, "pre-delete")
    delete_snapshot = parse_snapshot_metrics(observations_dir, "delete")
    during_snapshot = parse_snapshot_metrics(observations_dir, "during-rebalance")
    settled_snapshot = parse_snapshot_metrics(observations_dir, "settled")

    before_pods = sorted(before_snapshot.keys())
    settled_pods = sorted(set(settled_snapshot.keys()) | set(during_snapshot.keys()) | set(delete_snapshot.keys()))
    deleted_event = load_json_if_exists(observations_dir / "deletion-event.json")
    deleted_pod = deleted_event.get("deleted_pod", "")
    created_pods = [pod for pod in settled_pods if pod not in before_snapshot]

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
        if pod == deleted_pod:
            end_metrics = pre_delete_snapshot.get(pod, delete_snapshot.get(pod, before_metrics))
        else:
            end_metrics = settled_snapshot.get(pod, delete_snapshot.get(pod, before_metrics))
        pod_counts = {}
        for stage in stage_names:
            delta = metric_value(end_metrics, "ohmf_messages_processor_stage_events_total", stage=stage["stage"], outcome=stage["outcome"], target=stage["target"]) - metric_value(before_metrics, "ohmf_messages_processor_stage_events_total", stage=stage["stage"], outcome=stage["outcome"], target=stage["target"])
            pod_counts[stage["name"]] = delta
            aggregate_stage[stage["name"]] += delta
        stage_by_pod[pod] = pod_counts
    for pod in created_pods:
        end_metrics = settled_snapshot.get(pod, {})
        pod_counts = {}
        for stage in stage_names:
            delta = metric_value(end_metrics, "ohmf_messages_processor_stage_events_total", stage=stage["stage"], outcome=stage["outcome"], target=stage["target"])
            pod_counts[stage["name"]] = delta
            aggregate_stage[stage["name"]] += delta
        stage_by_pod[pod] = pod_counts

    stage_counters = dict(aggregate_stage)

    # Defensive consistency checks.
    for required in ("offset_commit_succeeded", "handler_error"):
        stage_counters.setdefault(required, 0)

    lag_samples = parse_lag_timeline(observations_dir)
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

    if not lag_recovery and not lag_settled_to_zero and lag_samples:
        lag_settled_to_zero = any(sample["lag"] == 0 for sample in lag_samples)
        if lag_settled_to_zero:
            first_zero = next(sample for sample in lag_samples if sample["lag"] == 0)
            lag_zero_seconds = float(first_zero["stamp"] or 0)

    postgres_exact = count_by_run_prefix(args.namespace, args.run_id_prefix)
    cassandra_exact = cassandra_count_by_run_prefix(args.namespace, conversations, args.cassandra_keyspace, args.run_id_prefix)
    missing = accepted - postgres_exact

    loadgen_pods_json = json.loads((observations_dir / "loadgen-pods.json").read_text(encoding="utf-8")) if (observations_dir / "loadgen-pods.json").exists() else {"items": []}
    source_ips = sorted({
        item.get("status", {}).get("podIP", "")
        for item in loadgen_pods_json.get("items", [])
        if item.get("status", {}).get("podIP")
    })

    top_paths = sorted(observations_dir.glob("top-pods-*.txt"))
    peaks = parse_peak_resources(top_paths)
    pods_before = parse_restart_counts(observations_dir / "pods-before.json") if (observations_dir / "pods-before.json").exists() else {}
    pods_settled = parse_restart_counts(observations_dir / "pods-settled.json") if (observations_dir / "pods-settled.json").exists() else {}
    pods_delete = parse_restart_counts(observations_dir / "pods-delete.json") if (observations_dir / "pods-delete.json").exists() else {}
    pods_during = parse_restart_counts(observations_dir / "pods-during-rebalance.json") if (observations_dir / "pods-during-rebalance.json").exists() else {}

    total_failed = sent_attempts - accepted_responses
    status = args.run_status
    if failed_reasons and status == "passed":
        if all(reason.startswith("http_429") for reason in failed_reasons):
            status = "failed_limited"
        elif total_failed or missing != 0 or duplicates != 0 or not lag_settled_to_zero:
            status = "failed"
    if not lag_settled_to_zero or accepted != requested or postgres_exact != accepted or cassandra_exact != accepted or missing != 0 or duplicates != 0 or stage_counters["handler_error"] != 0 or stage_counters["offset_commit_succeeded"] != accepted:
        if status == "passed":
            status = "failed"

    if deleted_pod and deleted_pod in pods_before and pods_settled.get(deleted_pod, 0) > 0:
        # The original pod should have disappeared; a surviving restart would be unexpected.
        pass

    assignments = {
        "before": parse_consumer_group(observations_dir / "kafka-consumer-group-before.txt") if (observations_dir / "kafka-consumer-group-before.txt").exists() else {"raw": "", "total_partitions": 0, "assigned_partitions": 0, "unique_members": [], "member_partition_counts": {}},
        "delete": parse_consumer_group(observations_dir / "kafka-consumer-group-delete.txt") if (observations_dir / "kafka-consumer-group-delete.txt").exists() else {"raw": "", "total_partitions": 0, "assigned_partitions": 0, "unique_members": [], "member_partition_counts": {}},
        "during": parse_consumer_group(observations_dir / "kafka-consumer-group-during-rebalance.txt") if (observations_dir / "kafka-consumer-group-during-rebalance.txt").exists() else {"raw": "", "total_partitions": 0, "assigned_partitions": 0, "unique_members": [], "member_partition_counts": {}},
        "after": parse_consumer_group(observations_dir / "kafka-consumer-group-settled.txt") if (observations_dir / "kafka-consumer-group-settled.txt").exists() else {"raw": "", "total_partitions": 0, "assigned_partitions": 0, "unique_members": [], "member_partition_counts": {}},
    }

    if (
        len(assignments["before"]["unique_members"]) != 4
        or len(assignments["after"]["unique_members"]) != 4
        or assignments["before"]["assigned_partitions"] != assignments["before"]["total_partitions"]
        or assignments["after"]["assigned_partitions"] != assignments["after"]["total_partitions"]
    ):
        if status == "passed":
            status = "failed"
    if any(count > 0 for count in pods_settled.values()):
        if status == "passed":
            status = "failed"

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
        "3,100 concurrent clients",
        "Production readiness",
        "HA or failover",
        "Any cloud or production benchmark interpretation",
    ]
    if status != "passed":
        unsupported_claims.insert(0, "Validated 120 msg/sec local Kubernetes ingress with exact full-pipeline reconciliation during \"messages-processor\" pod deletion and Kafka consumer-group rebalance.")

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
        },
        "deletion": {
            "deleted_pod": deleted_pod,
            "deletion_requested_at_utc": deleted_event.get("deletion_requested_at_utc", ""),
            "deletion_offset_seconds": deleted_event.get("deletion_offset_seconds", 0),
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
            "lag_settled_to_zero": lag_settled_to_zero,
            "lag_zero_seconds": lag_zero_seconds,
            "lag_at_end": lag_at_end,
            "timeline": lag_samples,
        },
        "assignments": assignments,
        "pods": {
            "restarts_before": pods_before,
            "restarts_delete": pods_delete,
            "restarts_during": pods_during,
            "restarts_after": pods_settled,
            "peak_sampled_during_run": peaks,
        },
        "stage_counters": {
            "by_pod": stage_by_pod,
            "aggregate": stage_counters,
        },
        "supported_claim": build_supported_claim(status),
        "unsupported_claims": unsupported_claims,
        "artifacts": {
            "summary_json": str(result_dir / "summary.json"),
            "summary_md": str(result_dir / "summary.md"),
            "deploy_result_md": args.deploy_result_md,
            "observations_dir": str(observations_dir),
            "shards_dir": str(shards_dir),
            "pods_wide_after": str(observations_dir / "pods-settled.txt"),
            "top_pods_after": str(observations_dir / "top-pods-settled.txt"),
            "processor_logs_dir": str(observations_dir),
        },
    }

    summary_path = result_dir / "summary.json"
    summary_md_path = result_dir / "summary.md"
    deploy_md_path = Path(args.deploy_result_md)
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
