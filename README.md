# agentroom

A low-overhead, Redis-Streams-backed event mesh for coordinating multiple agent
workers across isolated repo/branch namespaces. Three small concerns in one Go
package:

- **Room** — publishes immutable events to a per-repo/branch stream and reads/writes a TTL'd scratchpad for heavy transient data.
- **Runtime** — wraps a `Worker` and consumes the stream through a Redis **consumer group**: at-least-once delivery that survives restarts, with `XAUTOCLAIM` recovery of work abandoned by crashed workers.
- **Archiver** — compacts streams that grow past a length threshold, snapshotting to cold storage and deleting only the exact archived entries (events appended mid-sweep are preserved).

The library reads no configuration itself and depends only on
[`go-redis/v9`](https://github.com/redis/go-redis).

## Install

```bash
go get github.com/dotcommander/agentchat/agentroom
```

```go
import "github.com/dotcommander/agentchat/agentroom"
```

Requires Go 1.26+ and Redis 6.2+ (consumer groups need 5.0; `XAUTOCLAIM` needs 6.2).

## Quick start

### Publish an event

```go
package main

import (
	"context"
	"encoding/json"
	"log"

	"github.com/dotcommander/agentchat/agentroom"
	"github.com/redis/go-redis/v9"
)

func main() {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer rdb.Close()

	cfg := agentroom.DefaultConfig()
	cfg.RepoID = "auth-service"
	cfg.BranchName = "main"

	room := agentroom.NewRoom(rdb, cfg)

	payload, _ := json.Marshal(map[string]string{"file": "parser.go"})
	ev := &agentroom.Event{Type: "AST_PARSED", AgentID: "parser-1", Payload: payload}
	if err := room.Publish(context.Background(), ev); err != nil {
		log.Fatal(err)
	}
	log.Printf("published entry %s to %s", ev.ID, cfg.StreamKey())
}
```

`Publish` assigns the stream entry ID back to `ev.ID`, defaults `ev.Timestamp` to
the current time if unset, and refreshes the stream's idle-expiry lease (the
stream auto-expires `Config.StreamTTL` after the last publish).

### React to events with a Worker

A `Worker` is your execution engine. `Interests` selects the event types it acts
on (`"*"` matches everything); `Execute` runs once per matching event and may
publish follow-up events through the `Room`.

```go
type TestRunner struct{}

func (TestRunner) ID() string          { return "test-runner-1" }
func (TestRunner) Interests() []string { return []string{"AST_PARSED"} }

func (TestRunner) Execute(ctx context.Context, ev agentroom.Event, room *agentroom.Room) error {
	log.Printf("running tests for %s", ev.AgentID)
	// ... run the engine ...
	return room.Publish(ctx, &agentroom.Event{
		Type:    "TESTS_PASSED",
		AgentID: "test-runner-1",
	})
}
```

### Run the Runtime

```go
cfg := agentroom.DefaultConfig()
cfg.RepoID = "auth-service"
cfg.BranchName = "main"
cfg.Group = "test-runners" // one consumer group per worker TYPE

room := agentroom.NewRoom(rdb, cfg)
rt := agentroom.NewRuntime(room, TestRunner{})

ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
defer stop()

// Listen blocks until ctx is canceled; it returns ctx.Err() on shutdown.
if err := rt.Listen(ctx); err != nil && ctx.Err() == nil {
	log.Fatal(err)
}
```

If `Execute` returns an error, the Runtime publishes an `ENGINE_RUNTIME_ERROR`
event (the failure as JSON) and acks the original, so a poison message is never
redelivered.

## Delivery model (important)

Every event on a stream is delivered to **each consumer group**; `Interests`
then filters which delivered events a worker acts on (non-matching events are
acked and skipped).

- Give each distinct **worker type** its own `Config.Group`.
- Run multiple **instances** of one type with the **same** `Config.Group` (they
  load-balance) and **unique** `Worker.ID()` consumer names.

Because the group persists its position in Redis, events published while a worker
is down are delivered on reconnect, and entries left pending by a crashed worker
are reclaimed by another instance via `XAUTOCLAIM`.

## Configuration

`Config` carries the namespace and tunables. `DefaultConfig()` supplies the
fallbacks below; override per environment (e.g. from viper/env). The library
never reads config on its own.

| Field | `DefaultConfig()` | Purpose |
|-------|-------------------|---------|
| `RedisAddr` | `localhost:6379` | Redis address (used by your client, not the library) |
| `RepoID` | `""` | Namespace component — set per repo |
| `BranchName` | `""` | Namespace component — set per branch |
| `StreamTTL` | `48h` | Idle expiry refreshed on each publish |
| `ArchiveThreshold` | `10000` | Stream length at/above which the Archiver compacts |
| `Group` | `agents` | Consumer-group name (set one per worker type) |

Keys are namespaced as `repo:<RepoID>:<BranchName>:events` (stream) and
`repo:<RepoID>:<BranchName>:state:<key>` (scratchpad).

## Scratchpad

Store heavy transient payloads out-of-band so the event stream stays lightweight.
A missing or expired key surfaces as a wrapped `redis.Nil`.

```go
import "errors"

if err := room.WriteScratchpad(ctx, "diff:123", bigBlob, 10*time.Minute); err != nil {
	log.Fatal(err)
}

data, err := room.ReadScratchpad(ctx, "diff:123")
switch {
case errors.Is(err, redis.Nil):
	// absent or expired
case err != nil:
	log.Fatal(err)
default:
	use(data)
}
```

## Archiver

Compacts every stream at/above the threshold. It discovers streams with `SCAN`
(non-blocking), snapshots each one, hands the batch to your `PersistFunc`, then
deletes only the archived entry IDs — anything appended during the sweep
survives. Run it on a daily ticker.

```go
archiver := agentroom.NewArchiver(rdb, cfg.ArchiveThreshold,
	func(stream string, events []redis.XMessage) error {
		return saveToColdStorage(stream, events) // S3, Postgres, file, ...
	})

ticker := time.NewTicker(24 * time.Hour)
defer ticker.Stop()
for {
	select {
	case <-ctx.Done():
		return
	case <-ticker.C:
		if err := archiver.RunDailySweep(ctx); err != nil {
			log.Printf("sweep: %v", err) // per-stream errors are joined, not fatal
		}
	}
}
```

## Demo harness

`cmd/agentroomd` wires a client, a logging worker, the Runtime, a sample publish,
and the Archiver. Namespace/address come from env vars.

```bash
REDIS_ADDR=localhost:6379 REPO_ID=demo BRANCH_NAME=main go run ./cmd/agentroomd
```

## Testing

```bash
go test ./agentroom/         # full: spins an in-process miniredis
go test -short ./agentroom/  # fast: skips Redis-backed tests
```

Redis-backed tests use [`miniredis`](https://github.com/alicebob/miniredis) and
are gated by `testing.Short()`.
