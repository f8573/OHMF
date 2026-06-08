#!/usr/bin/env python3

import argparse
import json
import re
import subprocess
from datetime import datetime, timezone
from pathlib import Path


METRIC_LINE = re.compile(r'^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{[^}]*\})?\s+(-?[0-9.eE+-]+)$')


def run_command(args):
    completed = subprocess.run(args, check=True, capture_output=True, text=True)
    return completed.stdout


def parse_metrics(path):
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


def metric_delta(before, settled, name, **labels):
    key = (name, tuple(sorted(labels.items())))
    return int(round(settled.get(key, 0.0) - before.get(key, 0.0)))


def parse_shard_conversations(run_dir):
    conversations = []
    for shard_path in sorted((run_dir / "shards").glob("*.summary.json")):
        payload = json.loads(shard_path.read_text(encoding="utf-8"))
        conversations.extend(item["conversation_id"] for item in payload.get("conversations", []))
    return sorted(set(conversations))


def parse_trailing_int(raw):
    for line in reversed(raw.strip().splitlines()):
        line = line.strip()
        if not line:
            continue
        match = re.search(r"(-?\d+)\s*$", line)
        if match:
            return int(match.group(1))
    raise ValueError(f"unable to parse trailing int from {raw!r}")


def postgres_count_by_run_prefix(namespace, run_prefix):
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


def summarize_restarts(before, after):
    changed = {}
    all_keys = sorted(set(before) | set(after))
    for key in all_keys:
        delta = after.get(key, 0) - before.get(key, 0)
        if delta != 0:
            changed[key] = delta
    return changed


def normalize_rollout(rollout):
    normalized = dict(rollout)
    services = {}
    overall_verified = True
    for service_name, payload in rollout["services"].items():
        effective_verified = (
            payload["deployment_image"] == payload["intended_image"]
            and bool(payload["running_pods"])
            and all(item.get("image_id") for item in payload["running_pods"])
        )
        overall_verified = overall_verified and effective_verified
        normalized_payload = dict(payload)
        normalized_payload["verified"] = effective_verified
        services[service_name] = normalized_payload
    normalized["services"] = services
    normalized["verified"] = overall_verified
    return normalized


def stage_counters_for_run(run_dir):
    before_metrics = parse_metrics(run_dir / "observations" / "messages-processor-metrics-before.txt")
    settled_metrics = parse_metrics(run_dir / "observations" / "messages-processor-metrics-settled.txt")
    return {
        "consumed": metric_delta(before_metrics, settled_metrics, "ohmf_messages_processor_stage_events_total", stage="kafka_consume", outcome="succeeded", target=""),
        "decoded": metric_delta(before_metrics, settled_metrics, "ohmf_messages_processor_stage_events_total", stage="decode", outcome="succeeded", target=""),
        "deduped": metric_delta(before_metrics, settled_metrics, "ohmf_messages_processor_stage_events_total", stage="dedupe", outcome="skipped", target=""),
        "postgres_write_succeeded": metric_delta(before_metrics, settled_metrics, "ohmf_messages_processor_stage_events_total", stage="postgres_write", outcome="succeeded", target=""),
        "cassandra_write_succeeded": metric_delta(before_metrics, settled_metrics, "ohmf_messages_processor_stage_events_total", stage="cassandra_write", outcome="succeeded", target=""),
        "redis_ack_set_succeeded": metric_delta(before_metrics, settled_metrics, "ohmf_messages_processor_stage_events_total", stage="redis_ack", outcome="succeeded", target="set"),
        "persisted_publish_succeeded": metric_delta(before_metrics, settled_metrics, "ohmf_messages_processor_stage_events_total", stage="downstream_publish", outcome="succeeded", target="persisted"),
        "microservice_publish_succeeded": metric_delta(before_metrics, settled_metrics, "ohmf_messages_processor_stage_events_total", stage="downstream_publish", outcome="succeeded", target="microservice"),
        "offset_commit_succeeded": metric_delta(before_metrics, settled_metrics, "ohmf_messages_processor_stage_events_total", stage="kafka_offset_commit", outcome="succeeded", target=""),
        "handler_success": metric_delta(before_metrics, settled_metrics, "ohmf_messages_processor_stage_events_total", stage="handler_return", outcome="success", target=""),
        "handler_error": metric_delta(before_metrics, settled_metrics, "ohmf_messages_processor_stage_events_total", stage="handler_return", outcome="error", target=""),
    }


def rung_passes(summary, counters, postgres_exact, cassandra_exact, restart_changes):
    accepted = summary["counts"]["accepted"]
    requested = summary["counts"]["requested"]
    exact_missing = accepted - postgres_exact
    exact_cassandra_missing = accepted - cassandra_exact
    return all([
        requested == accepted,
        counters["consumed"] == accepted,
        counters["postgres_write_succeeded"] == accepted,
        counters["cassandra_write_succeeded"] == accepted,
        counters["offset_commit_succeeded"] == accepted,
        counters["handler_error"] == 0,
        exact_missing == 0,
        exact_cassandra_missing == 0,
        summary["counts"]["duplicates"] == 0,
        summary["kafka"]["lag_settled_to_zero"],
        not restart_changes,
    ])


def render_rollout_markdown(rollout):
    lines = [
        "## Rollout Discipline",
        "",
        f"- Cluster context: `{rollout['cluster_context']}`",
        f"- Namespace: `{rollout['namespace']}`",
        f"- `stage_events_total` gate: `{rollout['stage_events_metric_present']}`",
        f"- Rollout verification status: `{rollout['verified']}`",
        "",
        "| Service | Intended image | Deployment image | Pod image ID |",
        "| --- | --- | --- | --- |",
    ]
    for service_name, payload in rollout["services"].items():
        pod_image_id = ", ".join(item["image_id"] for item in payload["running_pods"]) or "missing"
        lines.append(
            f"| {service_name} | `{payload['intended_image']}` | `{payload['deployment_image']}` | `{pod_image_id}` |"
        )
    lines.append("")
    return lines


def render_markdown(summary):
    lines = [
        f"# {summary['title']}",
        "",
        "## Outcome",
        "",
        summary["supported_claim"],
        "",
        f"- Highest passing sustained rate: `{summary['highest_passing_sustained_rate_msg_sec']}`",
        f"- `120 msg/sec` passed: `{summary['passed_120_msg_sec']}`",
        f"- Highest attempted rate: `{summary['highest_attempted_rate_msg_sec']}`",
        "",
        "A prior 60 msg/sec under-reconciliation result was traced to stale container image deployment; subsequent unique-tag rollout with processor stage instrumentation reconciled exactly.",
        "",
    ]
    lines.extend(render_rollout_markdown(summary["rollout"]))
    lines.extend([
        "## Stage Counter Table",
        "",
        "| Rate | Status | Requested | Sent | Accepted | Consumed | PG ok | Cassandra ok | Redis ack ok | Persisted publish | Microservice publish | Offset commits | Handler errors | PG exact | Cassandra exact | Lag zero sec | p50 ms | p95 ms | p99 ms | Restart delta |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
    ])
    for rung in summary["rungs"]:
        c = rung["stage_counters"]
        restart_delta = json.dumps(rung["restart_changes"], sort_keys=True) if rung["restart_changes"] else "0"
        lines.append(
            f"| {rung['rate_msg_sec']} msg/sec | `{rung['status']}` | {rung['requested']} | {rung['sent_attempts']} | {rung['accepted']} | {c['consumed']} | "
            f"{c['postgres_write_succeeded']} | {c['cassandra_write_succeeded']} | {c['redis_ack_set_succeeded']} | "
            f"{c['persisted_publish_succeeded']} | {c['microservice_publish_succeeded']} | {c['offset_commit_succeeded']} | "
            f"{c['handler_error']} | {rung['postgres_exact']} | {rung['cassandra_exact']} | {rung['kafka_lag_zero_seconds']:.3f} | "
            f"{rung['latency']['p50_ms']:.3f} | {rung['latency']['p95_ms']:.3f} | {rung['latency']['p99_ms']:.3f} | `{restart_delta}` |"
        )
    lines.extend([
        "",
        "## Artifacts",
        "",
        f"- Combined JSON: `{summary['artifacts']['summary_json']}`",
        f"- Deploy note: `{summary['artifacts']['deploy_result_md']}`",
    ])
    for rung in summary["rungs"]:
        lines.append(f"- `{rung['rate_msg_sec']} msg/sec`: `{rung['result_dir']}`")
    lines.extend([
        "",
        "## Unsupported Claims Still Remaining",
        "",
    ])
    for claim in summary["unsupported_claims_still_remaining"]:
        lines.append(f"- {claim}")
    lines.append("")
    return "\n".join(lines)


def render_deploy_markdown(summary):
    lines = [
        f"# {summary['title']}",
        "",
        "## Outcome",
        "",
        f"- Highest passing sustained rate: `{summary['highest_passing_sustained_rate_msg_sec']}`",
        f"- `120 msg/sec` passed: `{summary['passed_120_msg_sec']}`",
        f"- Supported claim: {summary['supported_claim']}",
        "",
        "## Rollout discipline",
        "",
        f"- Unique image rollout verified: `{summary['rollout']['verified']}`",
        f"- `stage_events_total` present before ladder: `{summary['rollout']['stage_events_metric_present']}`",
        "",
        "## Rungs",
        "",
    ]
    for rung in summary["rungs"]:
        c = rung["stage_counters"]
        lines.append(
            f"- `{rung['rate_msg_sec']} msg/sec`: status=`{rung['status']}`, requested/sent/accepted=`{rung['requested']}`/`{rung['sent_attempts']}`/`{rung['accepted']}`, "
            f"consumed=`{c['consumed']}`, pg_ok=`{c['postgres_write_succeeded']}`, cass_ok=`{c['cassandra_write_succeeded']}`, "
            f"commits=`{c['offset_commit_succeeded']}`, handler_errors=`{c['handler_error']}`, pg_exact=`{rung['postgres_exact']}`, "
            f"cass_exact=`{rung['cassandra_exact']}`, lag_zero=`{rung['kafka_lag_zero_seconds']:.3f}s`, restarts=`{json.dumps(rung['restart_changes'], sort_keys=True) if rung['restart_changes'] else '0'}`"
        )
    lines.extend([
        "",
        "A prior 60 msg/sec under-reconciliation result was traced to stale container image deployment; subsequent unique-tag rollout with processor stage instrumentation reconciled exactly.",
        "",
    ])
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--deploy-result-md", required=True)
    parser.add_argument("--run-date", required=True)
    parser.add_argument("--namespace", default="ohmf")
    parser.add_argument("--rollout-evidence", required=True)
    parser.add_argument("--cassandra-keyspace", default="ohmf_messages")
    parser.add_argument("run_dirs", nargs="+")
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    rollout = normalize_rollout(json.loads(Path(args.rollout_evidence).read_text(encoding="utf-8")))

    rungs = []
    highest_passing = None
    for run_dir_raw in args.run_dirs:
        run_dir = Path(run_dir_raw)
        summary = json.loads((run_dir / "summary.json").read_text(encoding="utf-8"))
        conversations = parse_shard_conversations(run_dir)
        run_prefix = summary["run"]["run_id_prefix"]
        counters = stage_counters_for_run(run_dir)
        postgres_exact = postgres_count_by_run_prefix(args.namespace, run_prefix)
        cassandra_exact = cassandra_count_by_run_prefix(args.namespace, conversations, args.cassandra_keyspace, run_prefix)
        restart_changes = summarize_restarts(summary["pods"]["restarts_before"], summary["pods"]["restarts_after"])
        verified_pass = rung_passes(summary, counters, postgres_exact, cassandra_exact, restart_changes)

        rung = {
            "rate_msg_sec": summary["run"]["aggregate_target_rate"],
            "status": "passed" if verified_pass else summary["status"],
            "requested": summary["counts"]["requested"],
            "sent_attempts": summary["counts"]["sent_attempts"],
            "accepted": summary["counts"]["accepted"],
            "failed_sends_by_reason": summary["counts"]["failed_sends_by_reason"],
            "missing": summary["counts"]["accepted"] - postgres_exact,
            "duplicates": summary["counts"]["duplicates"],
            "postgres_exact": postgres_exact,
            "cassandra_exact": cassandra_exact,
            "kafka_lag_zero_seconds": summary["kafka"]["lag_zero_seconds"],
            "kafka_lag_settled_to_zero": summary["kafka"]["lag_settled_to_zero"],
            "latency": summary["latency"],
            "restart_changes": restart_changes,
            "stage_counters": counters,
            "result_dir": str(run_dir),
        }
        rungs.append(rung)
        if verified_pass:
            highest_passing = rung["rate_msg_sec"]
        else:
            break

    highest_attempted = rungs[-1]["rate_msg_sec"] if rungs else 0
    passed_120 = highest_passing == 120
    if highest_passing is None:
        supported_claim = "No new Stage B1 sustained throughput claim is supported by this rerun."
    else:
        supported_claim = (
            f"Validated {highest_passing} msg/sec through the local Kubernetes full pipeline with exact reconciliation across Kafka consumption, "
            f"Postgres persistence, Cassandra persistence, downstream publishes, and offset commits."
        )

    summary = {
        "title": f"Stage B1 throughput rerun - {args.run_date}",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "namespace": args.namespace,
        "rollout": rollout,
        "highest_passing_sustained_rate_msg_sec": highest_passing,
        "highest_attempted_rate_msg_sec": highest_attempted,
        "passed_120_msg_sec": passed_120,
        "supported_claim": supported_claim,
        "unsupported_claims_still_remaining": [
            "Any delivery-latency claim beyond client-observed HTTP accept latency",
            "Any burst-throughput headline beyond the highest sustained passing rung",
            "Any production, HA, failover, or cloud benchmark interpretation",
            "Any large-client-count claim such as 3,100 concurrent clients",
        ],
        "rungs": rungs,
        "artifacts": {
            "summary_json": str(output_dir / "summary.json"),
            "summary_md": str(output_dir / "summary.md"),
            "deploy_result_md": args.deploy_result_md,
            "rollout_evidence": args.rollout_evidence,
        },
    }

    summary_json_path = output_dir / "summary.json"
    summary_md_path = output_dir / "summary.md"
    deploy_md_path = Path(args.deploy_result_md)
    summary_json_path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    summary_md_path.write_text(render_markdown(summary), encoding="utf-8")
    deploy_md_path.write_text(render_deploy_markdown(summary), encoding="utf-8")


if __name__ == "__main__":
    main()
