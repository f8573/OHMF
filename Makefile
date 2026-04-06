.PHONY: run-contacts run-apps run-media test-services test-unit test-integration test-web test-e2e test-live test-perf test-staging

run-contacts:
	@echo "Running contacts service on :18085"
	go run ./ohmf/services/contacts

run-apps:
	@echo "Running apps service on :18086"
	go run ./ohmf/services/apps

run-media:
	@echo "Running media service on :18087"
	go run ./ohmf/services/media

test-services:
	@echo "Running tests for services..."
	go test ./ohmf/services/... ./ohmf/pkg/observability -v

test-unit:
	node ./scripts/test-gates.js unit

test-integration:
	node ./scripts/test-gates.js integration

test-web:
	node ./scripts/test-gates.js web

test-e2e:
	node ./scripts/test-gates.js e2e

test-live:
	node ./scripts/test-gates.js live

test-perf:
	node ./scripts/test-gates.js perf

test-staging:
	node ./scripts/test-gates.js staging
