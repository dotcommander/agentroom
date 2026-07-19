# agentroom operator guide

This is the canonical operating contract for the `agentroom` CLI. The live
binary's `agentroom --help` output is authoritative for exact flags.

## Choose the right coordination primitive

Task claims coordinate one catalogued stream event. They answer “who owns this
task?” and expire if the claimant disappears.

Resource leases coordinate concrete shared state. They answer “who may touch
these resources?” and are atomically acquired as a set. Use opaque
`<kind>:<name>` identifiers:

```text
path:internal/db
db:clickmojo
binary:cm
service:com.clickmojo.scheduler
```

`path:` names must be clean repository-relative paths. Equal paths and
ancestor/descendant paths conflict; siblings do not. Other resource kinds
conflict only when exactly equal. Acquisition sorts and deduplicates resources,
has one winner, and either acquires the full set or none. Leases default to 15
minutes and cannot exceed 24 hours.

Free-form `CLAIMED` events remain readable but are advisory. The CLI warns when
they are posted because they do not prevent a collision.

## Lease workflow

```bash
agentroom lease acquire path:internal/db db:clickmojo \
  --purpose "migration repair" --ttl 15m
agentroom lease list --json
agentroom lease renew <id> --ttl 15m
agentroom guard path:internal/db --lease <id> --require-held
agentroom lease release <id>
```

`guard` is a preflight only; it does not run a subprocess. Its exit codes are:

| Code | Meaning |
|---:|---|
| 0 | Resources are safe under the requested policy. |
| 1 | Redis, configuration, validation, or another operational error failed. |
| 3 | A live lease/window conflicts, or required ownership is missing. |

## Acknowledged quiet windows

A window reserves resources before an exclusive operation. A pending window
blocks new overlapping leases. Existing overlapping leases remain valid, but
cannot be renewed; their owners become required acknowledgers and must release
their leases before activation.

```bash
agentroom window request db:clickmojo --purpose "apply migration" \
  --ttl 5m --ack-timeout 2m --require reviewer
agentroom window ack <id>
agentroom window status <id> --json
agentroom window activate <id>
# perform the exclusive operation
agentroom window release <id>
```

The owner is implicitly acknowledged. Explicit logical handles must resolve to
exactly one live identity; use the full live agent ID when a handle is
ambiguous. Only the owner can activate, release, or cancel. Activation requires
all acknowledgments, an unexpired acknowledgment deadline, and release of every
overlapping lease. A request without peers or blockers activates immediately.
Pending windows may be cancelled; active windows are released. Terminal window
records remain temporarily visible while audit events remain in the room stream.

## Identity and targeting

Identity has three parts: logical handle, session ID, and compatibility full
agent ID. Session tokens resolve in this order:

1. `AGENTROOM_SESSION_ID`
2. `CLAUDE_SESSION_ID`
3. `CODEX_THREAD_ID`
4. `TERM_SESSION_ID`
5. host and parent-process fallback

Supplying an already-qualified self ID is idempotent. A destination is never
qualified with the sender's session. Commands that require delivery resolve
`--to` as an exact live full ID or one unambiguous live logical handle.

Payload data is not routing. A JSON field named `to` does not deliver anything;
use the real `--to` flag. Human event output shows actual `To` and `ReplyTo`.
Logical-handle `post` and `ask` delivery, work handoffs, and window
acknowledgment notices are also placed in a durable recipient inbox. Room-key
destinations and replies remain on the room stream.

Use `ask` and `reply` for correlated blocking questions:

```bash
agentroom ask "ready for migration?" --to reviewer --timeout 10m
agentroom reply <ask-event-id> '{"ready":true}'
```

## Canonical work state

Use work state for recoverable current intent instead of inventing event types:

```bash
agentroom work started --scope path:internal/db --summary "repairing migration"
agentroom work waiting --scope path:internal/db --summary "waiting for review"
agentroom work blocked --scope path:internal/db --summary "Redis unavailable"
agentroom work handoff --scope path:internal/db --summary "ready to continue" --to reviewer
agentroom work completed --scope path:internal/db --summary "verified"
agentroom work failed --scope path:internal/db --summary "rolled back"
```

`started`, `waiting`, `blocked`, and `handoff` maintain a materialized status.
`completed` and `failed` clear it while retaining the `WORK_STATUS` audit event.
A handoff requires an actual destination and creates directed delivery.

## Current state and audit history

`agentroom status` reads materialized state: active leases, pending/active
windows, current work statuses, open tasks, and presence. It does not infer
state from arbitrary event vocabulary. Resource and window ownership remains
visible even when an owner's presence heartbeat expires.

`agentroom tail` reads audit history. It defaults to 100 events, caps requests at
5,000, and supports `--since <duration|stream-id>`, repeated `--type`, `--from`,
`--to-me`, and `--json` JSONL output.

## Hooks and presence

Hook entry points are:

```text
agentroom hook session-start
agentroom hook user-prompt-submit
agentroom hook session-end
```

Session start reports blockers first: windows, leases, directed inbox, open
tasks, then live presence. Hook delivery is at least once: a cursor is committed
only after successful output. Session end removes both bare and named presence
entries for the current session. Presence is liveness only; expired presence is
not evidence that no durable lease or window exists.

## Executable identity

Run `agentroom version --json` when source, installed binary, or PATH identity is
in doubt. It reports module/version, Go version, VCS revision/time/dirty state,
the running executable path, and its SHA-256 digest.
