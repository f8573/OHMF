#!/usr/bin/env python3

import argparse
import json
import re
import subprocess
from datetime import datetime, timezone
from pathlib import Path


METRIC_LINE = re.compile(r'^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{[^}]*\})?\s+(-?[0-9.eE+-]+)$')
RUNG_NAME = re.compile(r'(?P<replicas>\d+)replicas-(?P<rate>\d+)msgsec$')


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


def parse_metrics_many(paths):
    totals = {}
    for path in paths:
        metrics = parse_metrics(path)
        for key, value in metrics.items():
            totals[key] = totals.get(key, 0.0) + value
    return totals


def metric_delta(before, settled, name, **labels):
    key = (name, tuple(sorted(labels.items())))
    return int(round(settled.get(key, 0.0) - before.get(key, 0.0)))


def parse_trailing_int(raw):
    for line in reversed(raw.strip().splitlines()):
        line = line.strip()
        if not line:
            continue
        match = re.search(r"(-?\d+)\s*$", line)
        if match:
            return int(match.group(1))
    raise ValueError(f"unable to parse trailing int from {raw!r}")


def parse_shard_conversations(run_dir):
    conversations = []
    for shard_path in sorted((run_dir / "shards").glob("*.summary.json")):
        payload = json.loads(shard_path.read_text(encoding="utf-8"))
        conversations.extend(item["conversation_id"] for item in payload.get("conversations", []))
    return sorted(set(conversations))


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
    obs_dir = run_dir / "observations"
    before_paths = sorted(obs_dir.glob("messages-processor-metrics-before-*.txt"))
    settled_paths = sorted(obs_dir.glob("messages-processor-metrics-settled-*.txt"))
    if before_paths and settled_paths:
        before_metrics = parse_metrics_many(before_paths)
        settled_metrics = parse_metrics_many(settled_paths)
    else:
        before_metrics = parse_metrics(obs_dir / "messages-processor-metrics-before.txt")
        settled_metrics = parse_metrics(obs_dir / "messages-processor-metrics-settled.txt")
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


def parse_rung_identity(run_dir):
    match = RUNG_NAME.match(run_dir.name)
    if not match:
        raise ValueError(f"unable to parse replicas/rate from rung directory name: {run_dir}")
    return int(match.group("replicas")), int(match.group("rate"))


def parse_consumer_group_assignments(path):
    assignments = {}
    total_partitions = 0
    assigned_partitions = 0
    raw = path.read_text(encoding="utf-8")
    for line in raw.splitlines():
        fields = line.split()
        if len(fields) < 9:
            continue
        if fields[0] != "messages-processor-v1" or fields[1] != "msg.ingress.v1":
            continue
        try:
            partition = int(fields[2])
        except ValueError:
            continue
        member_id = fields[6]
        total_partitions += 1
        if member_id == "-":
            continue
        assigned_partitions += 1
        assignments.setdefault(member_id, []).append(partition)
    return {
        "total_partitions": total_partitions,
        "assigned_partitions": assigned_partitions,
        "unique_members": sorted(assignments.keys()),
        "member_partition_counts": {member: len(sorted(parts)) for member, parts in sorted(assignments.items())},
    }


def assignment_summary(run_dir, replicas):
    scale_ready = parse_consumer_group_assignments(run_dir / "observations" / "messages-processor-consumer-group-scale-ready.txt")
    post_run = parse_consumer_group_assignments(run_dir / "observations" / "messages-processor-consumer-group-post-run.txt")
    post_drain = parse_consumer_group_assignments(run_dir / "observations" / "messages-processor-consumer-group-post-drain.txt")
    min_expected_members = min(replicas, scale_ready["total_partitions"] or replicas)
    members_ok = len(scale_ready["unique_members"]) >= min_expected_members
    return {
        "scale_ready": scale_ready,
        "post_run": post_run,
        "post_drain": post_drain,
        "members_ok": members_ok,
        "expected_members": min_expected_members,
    }


def rung_passes(summary, counters, postgres_exact, cassandra_exact, restart_changes):
    accepted = summary["counts"]["accepted"]
    requested = summary["counts"]["requested"]
    return all([
        requested == accepted,
        counters["consumed"] == accepted,
        counters["postgres_write_succeeded"] == accepted,
        counters["cassandra_write_succeeded"] == accepted,
        counters["offset_commit_succeeded"] == accepted,
        counters["handler_error"] == 0,
        postgres_exact == accepted,
        cassandra_exact == accepted,
        summary["kafka"]["lag_settled_to_zero"],
        not restart_changes,
    ])


def rate_control_summary(rungs, rate):
    relevant = [rung for rung in rungs if rung["rate_msg_sec"] == rate]
    relevant.sort(key=lambda item: item["processor_replicas"])
    if not relevant:
        return None
    passing = [rung["processor_replicas"] for rung in relevant if rung["status"] == "passed"]
    return {
        "rate_msg_sec": rate,
        "passing_replicas": passing,
        "all_passed": len(passing) == len(relevant),
    }


def compare_120_rungs(rungs):
    relevant = sorted((r for r in rungs if r["rate_msg_sec"] == 120), key=lambda item: item["processor_replicas"])
    if not relevant:
        return {
            "answer": "No 120 msg/sec rungs were present.",
            "hypothesis_result": "inconclusive",
        }

    baseline = next((r for r in relevant if r["processor_replicas"] == 1), relevant[0])
    scaled = [r for r in relevant if r["processor_replicas"] > 1]

    if any(r["status"] == "passed" for r in scaled):
        return {
            "answer": "Yes. Adding processors made `120 msg/sec` pass strict full-pipeline reconciliation.",
            "hypothesis_result": "validated",
        }

    if any(not r["assignment"]["members_ok"] for r in scaled):
        return {
            "answer": "Inconclusive. At least one scaled `120 msg/sec` rung did not show the expected active consumer-member assignments across the 12 Kafka partitions.",
            "hypothesis_result": "inconclusive",
        }

    improved = False
    for rung in scaled:
        if rung["kafka_lag_settled_to_zero"] and not baseline["kafka_lag_settled_to_zero"]:
            improved = True
        if rung["stage_counters"]["consumed"] > baseline["stage_counters"]["consumed"]:
            improved = True
        if rung["postgres_exact"] > baseline["postgres_exact"]:
            improved = True
        if rung["cassandra_exact"] > baseline["cassandra_exact"]:
            improved = True
        if rung["kafka_lag_at_end"] < baseline["kafka_lag_at_end"]:
            improved = True

    if improved:
        return {
            "answer": "Partially. Adding processors improved `120 msg/sec` drain/reconciliation, but none of the scaled rungs met the strict pass criteria.",
            "hypothesis_result": "not_validated",
        }

    return {
        "answer": "No. Adding processors did not improve `120 msg/sec` drain/reconciliation within the bounded window.",
        "hypothesis_result": "not_validated",
    }


def supported_claim(rungs):
    scaled_120_pass = any(
        rung["processor_replicas"] in (2, 4)
        and rung["rate_msg_sec"] == 120
        and rung["status"] == "passed"
        for rung in rungs
    )
    if scaled_120_pass:
        return "Validated 120 msg/sec local Kubernetes ingress with exact full-pipeline reconciliation after scaling the messages-processor consumer group across Kafka partitions."

    passing_105 = sorted(
        rung["processor_replicas"]
        for rung in rungs
        if rung["rate_msg_sec"] == 105 and rung["status"] == "passed"
    )
    if passing_105:
        joined = ", ".join(str(item) for item in passing_105)
        return f"Validated exact full-pipeline reconciliation at 105 msg/sec with messages-processor replicas `{joined}`; `120 msg/sec` still requires scaled evidence to pass strict criteria."

    return "No new processor-scaling throughput claim is supported by this matrix."


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
        f"- Hypothesis: {summary['hypothesis']}",
        f"- Question answer: {summary['question_answer']}",
        f"- Hypothesis result: `{summary['hypothesis_result']}`",
        f"- Supported claim: {summary['supported_claim']}",
        "",
    ]
    control_105 = summary.get("control_105")
    if control_105:
        lines.append(f"- `105 msg/sec` strict pass across replicas: `{control_105['all_passed']}` ({', '.join(str(item) for item in control_105['passing_replicas']) or 'none'})")
        lines.append("")
    lines.extend(render_rollout_markdown(summary["rollout"]))
    lines.extend([
        "## Matrix",
        "",
        "| Replicas | Rate | Status | Requested | Accepted | Consumed | PG ok | Cassandra ok | Offset commits | Handler errors | PG exact | Cassandra exact | Lag zero sec | Lag at end | Restart delta | Assigned members | Expected members | Assigned partitions |",
        "| ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | ---: | ---: | ---: |",
    ])
    for rung in summary["rungs"]:
        c = rung["stage_counters"]
        restart_delta = json.dumps(rung["restart_changes"], sort_keys=True) if rung["restart_changes"] else "0"
        lines.append(
            f"| {rung['processor_replicas']} | {rung['rate_msg_sec']} | `{rung['status']}` | {rung['requested']} | {rung['accepted']} | {c['consumed']} | "
            f"{c['postgres_write_succeeded']} | {c['cassandra_write_succeeded']} | {c['offset_commit_succeeded']} | {c['handler_error']} | "
            f"{rung['postgres_exact']} | {rung['cassandra_exact']} | {rung['kafka_lag_zero_seconds']:.3f} | {rung['kafka_lag_at_end']} | `{restart_delta}` | "
            f"{len(rung['assignment']['scale_ready']['unique_members'])} | {rung['assignment']['expected_members']} | {rung['assignment']['scale_ready']['assigned_partitions']} |"
        )
    lines.extend([
        "",
        "## Assignment Evidence",
        "",
        "| Replicas | Rate | Scale-ready members | Scale-ready partition counts | Post-run members | Post-drain members |",
        "| ---: | ---: | --- | --- | --- | --- |",
    ])
    for rung in summary["rungs"]:
        lines.append(
            f"| {rung['processor_replicas']} | {rung['rate_msg_sec']} | "
            f"`{', '.join(rung['assignment']['scale_ready']['unique_members']) or 'none'}` | "
            f"`{json.dumps(rung['assignment']['scale_ready']['member_partition_counts'], sort_keys=True)}` | "
            f"`{', '.join(rung['assignment']['post_run']['unique_members']) or 'none'}` | "
            f"`{', '.join(rung['assignment']['post_drain']['unique_members']) or 'none'}` |"
        )
    lines.extend([
        "",
        "## Artifacts",
        "",
        f"- Combined JSON: `{summary['artifacts']['summary_json']}`",
        f"- Deploy note: `{summary['artifacts']['deploy_result_md']}`",
    ])
    for rung in summary["rungs"]:
        lines.append(f"- `{rung['processor_replicas']} replicas @ {rung['rate_msg_sec']} msg/sec`: `{rung['result_dir']}`")
    lines.extend([
        "",
        "Required capture files per rung live under each rung's `observations/` directory:",
        "",
        "- `messages-processor-logs-scale-ready.txt`, `messages-processor-logs-post-run.txt`, `messages-processor-logs-post-drain.txt`",
        "- `messages-processor-consumer-group-scale-ready.txt`, `messages-processor-consumer-group-post-run.txt`, `messages-processor-consumer-group-post-drain.txt`",
        "- `pods-scale-ready.txt`, `pods-post-run.txt`, `pods-post-drain.txt`",
        "",
    ])
    return "\n".join(lines)


def render_deploy_markdown(summary):
    lines = [
        f"# {summary['title']}",
        "",
        "## Outcome",
        "",
        f"- Question answer: {summary['question_answer']}",
        f"- Hypothesis result: `{summary['hypothesis_result']}`",
        f"- Supported claim: {summary['supported_claim']}",
        "",
        "## Matrix",
        "",
    ]
    for rung in summary["rungs"]:
        c = rung["stage_counters"]
        lines.append(
            f"- `{rung['processor_replicas']} replicas @ {rung['rate_msg_sec']} msg/sec`: status=`{rung['status']}`, requested/accepted=`{rung['requested']}`/`{rung['accepted']}`, "
            f"consumed=`{c['consumed']}`, pg_ok=`{c['postgres_write_succeeded']}`, cass_ok=`{c['cassandra_write_succeeded']}`, "
            f"commits=`{c['offset_commit_succeeded']}`, handler_errors=`{c['handler_error']}`, pg_exact=`{rung['postgres_exact']}`, "
            f"cass_exact=`{rung['cassandra_exact']}`, lag_zero=`{rung['kafka_lag_zero_seconds']:.3f}s`, lag_end=`{rung['kafka_lag_at_end']}`, "
            f"assigned_members=`{len(rung['assignment']['scale_ready']['unique_members'])}/{rung['assignment']['expected_members']}`"
        )
    lines.append("")
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
    for run_dir_raw in args.run_dirs:
        run_dir = Path(run_dir_raw)
        summary = json.loads((run_dir / "summary.json").read_text(encoding="utf-8"))
        conversations = parse_shard_conversations(run_dir)
        run_prefix = summary["run"]["run_id_prefix"]
        processor_replicas, rate_msg_sec = parse_rung_identity(run_dir)
        counters = stage_counters_for_run(run_dir)
        postgres_exact = postgres_count_by_run_prefix(args.namespace, run_prefix)
        cassandra_exact = cassandra_count_by_run_prefix(args.namespace, conversations, args.cassandra_keyspace, run_prefix)
        restart_changes = summarize_restarts(summary["pods"]["restarts_before"], summary["pods"]["restarts_after"])
        assignments = assignment_summary(run_dir, processor_replicas)
        verified_pass = rung_passes(summary, counters, postgres_exact, cassandra_exact, restart_changes)

        rungs.append({
            "processor_replicas": processor_replicas,
            "rate_msg_sec": rate_msg_sec,
            "status": "passed" if verified_pass else "failed",
            "requested": summary["counts"]["requested"],
            "accepted": summary["counts"]["accepted"],
            "postgres_exact": postgres_exact,
            "cassandra_exact": cassandra_exact,
            "kafka_lag_zero_seconds": summary["kafka"]["lag_zero_seconds"],
            "kafka_lag_settled_to_zero": summary["kafka"]["lag_settled_to_zero"],
            "kafka_lag_at_end": summary["kafka"]["lag_at_end"],
            "latency": summary["latency"],
            "restart_changes": restart_changes,
            "stage_counters": counters,
            "assignment": assignments,
            "result_dir": str(run_dir),
        })

    rungs.sort(key=lambda item: (item["processor_replicas"], item["rate_msg_sec"]))
    question = compare_120_rungs(rungs)
    control_105 = rate_control_summary(rungs, 105)

    summary = {
        "title": f"Processor scaling matrix - {args.run_date}",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "namespace": args.namespace,
        "hypothesis": "The 120 msg/sec ceiling is caused by single messages-processor throughput, not gateway ingress or Kafka partition count.",
        "question_answer": question["answer"],
        "hypothesis_result": question["hypothesis_result"],
        "rollout": rollout,
        "control_105": control_105,
        "supported_claim": supported_claim(rungs),
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
