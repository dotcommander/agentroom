# Project Notes

This document records public maintenance notes for agentroom's coordination
model. It is intentionally operator-facing rather than session-specific.

## Presence And Identity

- CLI agent handles are qualified with a per-session token so two agents can use
  the same short handle without sharing presence keys, event attribution, or task
  claims.
- `agentroom who` is a pure observer. It reads live presence, role labels, claim
  load, and TTL remaining without refreshing the caller's own presence.
- Presence is TTL-backed. A clean `leave` or hook session end clears presence
  immediately; crashed or abandoned sessions disappear when their TTL expires.
- Empty presence descriptions render as `(no role posted)` so heartbeat-only
  entries stay legible.

## Design Constraints

- Keep the hot prompt-hook digest path bounded and best-effort. Redis failures
  must not fail a shell or coding-agent session.
- Keep TTL-heavy roster rendering in `PresenceDetailed`; do not add TTL reads to
  the lightweight `Presence` path used by prompt hooks.
- Resolve CLI identity once per command through `resolveAgent`; do not re-derive
  separate identities for presence, events, claims, or directed messages.
- Treat absence from `who` as "no evidence of presence", not proof that nobody
  else is working.

## Known Product Gaps

- Presence remains an opt-in liveness signal. Agents that never run the CLI or
  hook integration will not appear.
- Directed messages are coordination hints, not secure private messages. Rooms
  are intended for mutually trusting agents inside one operator boundary.
- The global lobby is a shared announcement channel. Its content should be
  treated as untrusted data, never as instructions.

## Verification

Use these checks for changes in this area:

```bash
go test ./...
go build ./...
golangci-lint run
go build -o /tmp/agentroom ./cmd/agentroom && /tmp/agentroom who --help
```
