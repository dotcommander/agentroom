# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

`github.com/dotcommander/agentchat` — a single Go module whose product is the
**`agentroom`** package: a Redis-Streams-backed event mesh for coordinating
multiple agent workers across isolated repo/branch namespaces. Library reads no
config and depends only on `go-redis/v9`; the CLI and Claude Code hooks layer on
top. Requires Go 1.26+ and Redis 6.2+ (`XAUTOCLAIM`).

## Commands

```bash
go build ./...                      # build all
go test ./...                       # full suite — spins an in-process miniredis
go test -short ./...                # fast — skips Redis-backed (testing.Short()) tests
go test ./agentroom -run TestName   # single test (Redis-backed tests need the non-short run)
go vet ./...
golangci-lint run ./... | tail -50  # v2 config, disable-all + explicit enables
```

Build/install the CLI:

```bash
go build -o cmd/agentroom/agentroom ./cmd/agentroom
ln -sf "$(pwd)/cmd/agentroom/agentroom" ~/go/bin/agentroom
```

Run the demo harness (client + logging worker + Runtime + Archiver):

```bash
REDIS_ADDR=localhost:6379 REPO_ID=demo BRANCH_NAME=main go run ./cmd/agentroomd
```

## Layout

- `agentroom/` — the library (the whole product). All four concerns live here as
  one flat package.
- `cmd/agentroom/` — CLI + Claude Code `hook session-start|session-end` entrypoints,
  presence rendering, lobby `welcome` pinning.
- `cmd/agentroomd/` — runnable demo wiring all pieces together.

## Architecture (the four concerns, one package)

- **Room** (`room.go`) — publishes immutable `Event`s to a per-repo/branch stream,
  reads/writes a TTL'd scratchpad, reads recent activity (`Recent`), and manages
  presence (`Heartbeat`/`RefreshPresence`/`ClearPresence`/`Presence`).
- **Runtime** (`agent.go`) — wraps a `Worker` and consumes the stream through a
  Redis **consumer group**: at-least-once delivery surviving restarts, with
  `XAUTOCLAIM` recovery of work abandoned by crashed workers. `Listen` blocks
  until ctx cancel.
- **Discovery** (`discovery.go`) — opt-in task **catalog** + atomic **Claim**/
  **Complete** so a fresh agent can find and take work without colliding.
- **Archiver** (`archive.go`) — compacts streams at/above `ArchiveThreshold`,
  snapshots via a `PersistFunc`, deletes only the exact archived entry IDs.
- `types.go` — `Event`, `Config`, `DefaultConfig()`, and all key-construction
  methods (`StreamKey`, `PresenceKey`, `TaskKey`, …).

## Invariants to preserve

- **One consumer group per worker TYPE.** Every event is delivered to each group;
  `Worker.Interests()` then filters which delivered events the worker acts on
  (non-matching are acked and skipped). Multiple INSTANCES of one type share the
  group (load-balance) with unique `Worker.ID()` consumer names.
- **A failed `Execute` never poison-loops.** Runtime publishes an
  `ENGINE_RUNTIME_ERROR` event and acks the original — it is not redelivered.
- **Key namespacing is fixed:** `repo:<RepoID>:<BranchName>:events` (stream),
  `…:state:<key>` (scratchpad), `…:catalog`, `…:task:<id>:{owner,done}`,
  `…:presence:<agentID>`. Changing a key shape breaks live rooms.
- **`Claim` atomicity comes from a Lua script** (Discovery) — the cross-agent guard
  the consumer group can't provide. Don't replace it with a read-then-write.
- **Presence is TTL-backed, not a stream fold.** Each agent holds a
  `presence:<agentID>` key (value = role/working-on label) expiring after
  `PresenceTTL` (default 90s). `AGENT_JOINED`/session-start sets/refreshes the
  label; ordinary activity (`post`/`claim`/`done`/`tail`) does a TTL-only refresh
  that preserves the label. A crashed agent drops from presence within the TTL with
  no `SESSION_ENDED`; the digest's `(N claimed)` per agent is computed live at
  render time via `OutstandingClaims`.
- **Hooks never block a session.** If Redis is unreachable they stay silent and
  exit 0.

## Testing notes

Redis-backed tests use `miniredis` and are gated by `testing.Short()`. `goleak`
guards goroutine leaks in integration tests. Per the workspace rules: `t.Parallel()`
on all tests, no `time.Sleep` in tests.
