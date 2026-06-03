# 19.1 — Gateway: Message Ingress & Transformation

Mapping: OHMF spec sections 19 (Gateway) and 7 (Protocol).

Purpose
- Transform inbound client messages into platform canonical envelope, validate content, augment metadata, persist minimal audit record, and publish to internal event bus.

Expected behavior
- Accept validated message requests from the gateway HTTP handlers.
- Apply canonicalization: normalize phone numbers, enforce content-length, attach receipt metadata.
- Persist ingest audit record and publish MessageIngress event to bus.
- Async send waits for a processor persistence ack via Redis. If that ack times out, the gateway returns a provisional queued response with `server_order = 0`; clients reconcile the canonical persisted message through the normal sync/list path.

Full specification details
- Input: JSON matching GatewayMessageRequest (see gateway README).
- Output: publish `message.ingress` event with envelope proto / JSON.
- Invariants:
	- Normalized phone numbers must be E.164 when possible.
	- Duplicate detection: if a message with the same client-supplied message_id and conversation_id was ingested less than 5 minutes ago, mark duplicate and return existing message id.

Event payload (JSON Schema)
```json
{
	"$schema":"https://json-schema.org/draft/2020-12/schema",
	"$id":"https://ohmf.example/schemas/message.ingress.json",
	"title":"MessageIngressEvent",
	"type":"object",
	"required":["message_id","conversation_id","from","to","received_at","payload"],
	"properties":{
		"message_id":{"type":"string"},
		"conversation_id":{"type":"string"},
		"from":{"type":"string"},
		"to":{"type":"string"},
		"received_at":{"type":"string","format":"date-time"},
		"payload":{"type":"object"}
	}
}
```

Protocol buffer snippet (canonical envelope)
```proto
syntax = "proto3";
package ohmf.protocol;

message Envelope {
	string message_id = 1;
	string conversation_id = 2;
	string from = 3;
	string to = 4;
	bytes body = 5;
	map<string,string> metadata = 6;
	int64 received_at = 7; // epoch ms
}
```

Implementation constraints
- Use idempotent DB writes for ingest audit.
- Use consistent normalization library for phone numbers.
- Validate body size before buffering.

Security considerations
- Strip sensitive metadata fields not allowed to be stored (per privacy policy).
- Ensure inbound attachments are quarantined and scanned before inclusion.

Observability and operational notes
- Emit metric `gateway.messages.ingress.total` and `gateway.messages.ingress.duplicates`.
- Log structured event with `ingest_status` and `request_id`.
- Trace span "messages.ingest" covering normalization, persistence, publish.

Testing requirements
- Unit tests for normalization rules and duplicate detection.
- Integration test that validates publish to event bus and that downstream services receive expected envelope.

SQL persistence example
```sql
CREATE TABLE message_ingest_audit (
	id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
	message_id TEXT NOT NULL UNIQUE,
	conversation_id TEXT NOT NULL,
	from_number TEXT NOT NULL,
	to_number TEXT NOT NULL,
	payload JSONB NOT NULL,
	status TEXT NOT NULL,
	created_at TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX ON message_ingest_audit (conversation_id);
```

References
- Gateway README (routing and auth).
- packages/protocol proto definitions.
