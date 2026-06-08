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


def classify_failure_mode(counters, summary, pg_exact, cass_exact):
    accepted = summary["counts"]["accepted"]
    commits = counters["offset_commit_succeeded"]
    handler_success = counters["handler_success"]
    handler_errors = counters["handler_error"]
    pg_success = counters["postgres_write_succeeded"]
    cass_success = counters["cassandra_write_succeeded"]
    deduped = counters["deduped"]

    if deduped > 0:
        return "dedupe or idempotency collapse"
    if handler_success == accepted and commits == accepted and pg_success == accepted and cass_success == accepted and pg_exact == accepted and cass_exact == accepted:
        return "no failure detected"
    if commits == accepted and (pg_success < accepted or cass_success < accepted):
        return "partial-write behavior with offset commits after handler success"
    if handler_success < accepted and handler_errors > 0:
        return "processor-side failure or retry exhaustion before success"
    if handler_success == accepted and commits == accepted and pg_exact < accepted and cass_exact < accepted:
        return "reconciliation undercount or delayed visibility"
    if handler_success == accepted and commits == accepted and pg_exact == accepted and cass_exact < accepted:
        return "Cassandra visibility or reconciliation undercount"
    return "mixed or unresolved partial persistence behavior"


def render_markdown(summary):
    lines = [
        f"# {summary['title']}",
        "",
        "## Conclusion",
        "",
        summary["conclusion"],
        "",
        "Kafka offsets do not commit after a retryable partial-persistence failure. The code commits offsets only after `processMessage` returns success; PostgreSQL can commit before Cassandra/Redis/downstream side effects, but those later failures leave the offset uncommitted for retry.",
        "",
        "## Stage counters",
        "",
        "| Rate | Accepted | Consumed | Decoded | Deduped | PG ok | PG fail | Cassandra ok | Cassandra fail | Redis ack ok | Redis ack fail | Persisted topic ok | Microservice topic ok | Offset commits | Handler success | Handler error | PG exact | Cassandra exact | Failure mode |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
    ]
    for run in summary["runs"]:
        c = run["stage_counters"]
        lines.append(
            f"| {run['rate_msg_sec']} msg/sec | {run['accepted']} | {c['consumed']} | {c['decoded']} | {c['deduped']} | "
            f"{c['postgres_write_succeeded']} | {c['postgres_write_failed']} | {c['cassandra_write_succeeded']} | {c['cassandra_write_failed']} | "
            f"{c['redis_ack_set_succeeded']} | {c['redis_ack_set_failed']} | {c['persisted_publish_succeeded']} | {c['microservice_publish_succeeded']} | "
            f"{c['offset_commit_succeeded']} | {c['handler_success']} | {c['handler_error']} | {run['postgres_exact']} | {run['cassandra_exact']} | {run['failure_mode']} |"
        )
    lines.extend([
        "",
        "## Artifacts",
        "",
    ])
    for run in summary["runs"]:
        lines.append(f"- `{run['rate_msg_sec']} msg/sec`: `{run['result_dir']}`")
    lines.append(f"- Combined summary JSON: `{summary['artifacts']['summary_json']}`")
    return "\n".join(lines) + "\n"


def render_deploy_markdown(summary):
    lines = [
        f"# {summary['title']}",
        "",
        "## Outcome",
        "",
        summary["conclusion"],
        "",
        "## Failure semantics",
        "",
        "Kafka offsets commit only after handler success. PostgreSQL commits can happen before Cassandra/Redis/downstream side effects, so partial persistence is possible inside an attempt, but retryable failures should leave the Kafka offset uncommitted.",
        "",
        "## Stage counter snapshot",
        "",
    ]
    for run in summary["runs"]:
        c = run["stage_counters"]
        lines.append(
            f"- `{run['rate_msg_sec']} msg/sec`: accepted=`{run['accepted']}`, consumed=`{c['consumed']}`, pg_ok=`{c['postgres_write_succeeded']}`, "
            f"cass_ok=`{c['cassandra_write_succeeded']}`, commits=`{c['offset_commit_succeeded']}`, handler_error=`{c['handler_error']}`, "
            f"pg_exact=`{run['postgres_exact']}`, cass_exact=`{run['cassandra_exact']}`, mode=`{run['failure_mode']}`"
        )
    lines.append("")
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--deploy-result-md", required=True)
    parser.add_argument("--run-date", required=True)
    parser.add_argument("--namespace", default="ohmf")
    parser.add_argument("--cassandra-keyspace", default="ohmf_messages")
    parser.add_argument("run_dirs", nargs="+")
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    runs = []
    for run_dir_raw in args.run_dirs:
        run_dir = Path(run_dir_raw)
        summary = json.loads((run_dir / "summary.json").read_text(encoding="utf-8"))
        before_metrics = parse_metrics(run_dir / "observations" / "messages-processor-metrics-before.txt")
        settled_metrics = parse_metrics(run_dir / "observations" / "messages-processor-metrics-settled.txt")
        conversations = parse_shard_conversations(run_dir)
        run_prefix = summary["run"]["run_id_prefix"]

        counters = {
            "consumed": metric_delta(before_metrics, settled_metrics, "ohmf_messages_processor_stage_events_total", stage="kafka_consume", outcome="succeeded", target=""),
            "decoded": metric_delta(before_metrics, settled_metrics, "ohmf_messages_processor_stage_events_total", stage="decode", outcome="succeeded", target=""),
            "deduped": metric_delta(before_metrics, settled_metrics, "ohmf_messages_processor_stage_events_total", stage="dedupe", outcome="skipped", target=""),
            "postgres_write_attempted": metric_delta(before_metrics, settled_metrics, "ohmf_messages_processor_stage_events_total", stage="postgres_write", outcome="attempted", target=""),
            "postgres_write_succeeded": metric_delta(before_metrics, settled_metrics, "ohmf_messages_processor_stage_events_total", stage="postgres_write", outcome="succeeded", target=""),
            "postgres_write_failed": metric_delta(before_metrics, settled_metrics, "ohmf_messages_processor_stage_events_total", stage="postgres_write", outcome="failed", target=""),
            "cassandra_write_attempted": metric_delta(before_metrics, settled_metrics, "ohmf_messages_processor_stage_events_total", stage="cassandra_write", outcome="attempted", target=""),
            "cassandra_write_succeeded": metric_delta(before_metrics, settled_metrics, "ohmf_messages_processor_stage_events_total", stage="cassandra_write", outcome="succeeded", target=""),
            "cassandra_write_failed": metric_delta(before_metrics, settled_metrics, "ohmf_messages_processor_stage_events_total", stage="cassandra_write", outcome="failed", target=""),
            "redis_ack_set_attempted": metric_delta(before_metrics, settled_metrics, "ohmf_messages_processor_stage_events_total", stage="redis_ack", outcome="attempted", target="set"),
            "redis_ack_set_succeeded": metric_delta(before_metrics, settled_metrics, "ohmf_messages_processor_stage_events_total", stage="redis_ack", outcome="succeeded", target="set"),
            "redis_ack_set_failed": metric_delta(before_metrics, settled_metrics, "ohmf_messages_processor_stage_events_total", stage="redis_ack", outcome="failed", target="set"),
            "persisted_publish_succeeded": metric_delta(before_metrics, settled_metrics, "ohmf_messages_processor_stage_events_total", stage="downstream_publish", outcome="succeeded", target="persisted"),
            "microservice_publish_succeeded": metric_delta(before_metrics, settled_metrics, "ohmf_messages_processor_stage_events_total", stage="downstream_publish", outcome="succeeded", target="microservice"),
            "offset_commit_succeeded": metric_delta(before_metrics, settled_metrics, "ohmf_messages_processor_stage_events_total", stage="kafka_offset_commit", outcome="succeeded", target=""),
            "handler_success": metric_delta(before_metrics, settled_metrics, "ohmf_messages_processor_stage_events_total", stage="handler_return", outcome="success", target=""),
            "handler_error": metric_delta(before_metrics, settled_metrics, "ohmf_messages_processor_stage_events_total", stage="handler_return", outcome="error", target=""),
        }

        postgres_exact = postgres_count_by_run_prefix(args.namespace, run_prefix)
        cassandra_exact = cassandra_count_by_run_prefix(args.namespace, conversations, args.cassandra_keyspace, run_prefix)
        failure_mode = classify_failure_mode(counters, summary, postgres_exact, cassandra_exact)

        runs.append({
            "rate_msg_sec": summary["run"]["aggregate_target_rate"],
            "accepted": summary["counts"]["accepted"],
            "result_dir": str(run_dir),
            "run_id_prefix": run_prefix,
            "postgres_exact": postgres_exact,
            "cassandra_exact": cassandra_exact,
            "failure_mode": failure_mode,
            "stage_counters": counters,
        })

    runs.sort(key=lambda item: item["rate_msg_sec"])
    failing = next((run for run in runs if run["accepted"] != run["postgres_exact"] or run["accepted"] != run["cassandra_exact"]), None)
    if failing is None:
        conclusion = "The instrumented ladder did not show an under-reconciliation failure in these runs."
    else:
        conclusion = (
            f"The {failing['rate_msg_sec']} msg/sec run under-reconciled after settled drain. "
            f"Postgres exact count reached {failing['postgres_exact']}/{failing['accepted']} and Cassandra exact count reached "
            f"{failing['cassandra_exact']}/{failing['accepted']}. Classified mode: {failing['failure_mode']}."
        )

    summary = {
        "title": f"Processor stage instrumentation diagnostic - {args.run_date}",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "namespace": args.namespace,
        "conclusion": conclusion,
        "runs": runs,
        "artifacts": {
            "summary_json": str(output_dir / "summary.json"),
            "summary_md": str(output_dir / "summary.md"),
            "deploy_result_md": args.deploy_result_md,
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
