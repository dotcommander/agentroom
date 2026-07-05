set shell := ["bash", "-uc"]

default:
    just --list

test:
    go test ./...

test-short:
    go test -short ./...

test-ask:
    go test ./cmd/agentroom -run 'TestAskReplyAcrossCLI|TestBeginAskRejectsConcurrentAsk|TestWaitForReplyTimeout|TestResolveLiveTargetRejectsMissingAndAmbiguous' -count=1

build:
    go build ./...

vet:
    go vet ./cmd/agentroom ./agentroom

check: test build vet
    git diff --check

cli:
    go build -o cmd/agentroom/agentroom ./cmd/agentroom

help:
    go run ./cmd/agentroom --help
    go run ./cmd/agentroom ask --help
    go run ./cmd/agentroom reply --help

# Requires a reachable Redis at REDIS_ADDR (default localhost:6379).
smoke-ask redis_addr="localhost:6379":
    #!/usr/bin/env bash
    set -euo pipefail
    bin="${TMPDIR:-/tmp}/agentroom-smoke"
    repo="agentroom-smoke-$$(date +%s)"
    branch="test"
    session="agentroom-smoke-session"
    go build -o "$bin" ./cmd/agentroom
    env CLAUDE_SESSION_ID="$session" "$bin" --addr "{{redis_addr}}" --repo "$repo" --branch "$branch" post AGENT_JOINED '{"role":"reviewer","working_on":"ask smoke"}' --agent reviewer-smoke
    "$bin" --addr "{{redis_addr}}" --repo "$repo" --branch "$branch" who
    env CLAUDE_SESSION_ID="$session" "$bin" --addr "{{redis_addr}}" --repo "$repo" --branch "$branch" ask "can you confirm the smoke?" --agent asker-smoke --to reviewer-smoke --timeout 1s &
    ask_pid=$!
    trap 'kill "$ask_pid" 2>/dev/null || true' EXIT
    ask_id=""
    for _ in {1..50}; do
        ask_id="$("$bin" --addr "{{redis_addr}}" --repo "$repo" --branch "$branch" tail --count 5 | awk '$3 == "ASK" {print $1}' | tail -1)"
        if [[ -n "$ask_id" ]]; then
            break
        fi
        sleep 0.05
    done
    if [[ -z "$ask_id" ]]; then
        echo "ASK event did not appear" >&2
        exit 1
    fi
    env CLAUDE_SESSION_ID="$session" "$bin" --addr "{{redis_addr}}" --repo "$repo" --branch "$branch" reply "$ask_id" '{"status":"ok"}' --agent reviewer-smoke
    wait "$ask_pid"
    trap - EXIT
