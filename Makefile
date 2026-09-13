# Local Go dev (run these from your workstation).
build:
	go build ./...

test:
	go test ./...

fmt:
	goimports -w .

lint:
	golangci-lint run ./...

run-worker:
	go run ./cmd/heimdall-server

run-controller:
	go run ./cmd/heimdall-ui

# --- TrueNAS deploy targets (run these on the TrueNAS host, in the repo root) ---

# Quick iteration: rebuild only what changed, keep containers/volumes.
up:
	docker compose up -d --build

# Full hard-clean rebuild: no cache, guarantees you're running exactly
# what's in the repo. Use this whenever `make up` doesn't seem to reflect
# your latest changes.
redeploy:
	./scripts/redeploy.sh

# Same as redeploy, but also wipes heimdall-data/ollama-data volumes.
# Destroys your database and pulled models — confirm before running.
redeploy-clean:
	./scripts/redeploy.sh --wipe-data

logs:
	docker compose logs -f worker controller

ps:
	docker compose ps

.PHONY: build test fmt lint run-worker run-controller up redeploy redeploy-clean logs ps