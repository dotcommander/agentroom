# Handoff — agentroom presence/identity hardening from agent feedback — 2026-06-24 16:48

## Goal
Harden agentroom's agent identity + presence so concurrent agents can trust "who's
actually here and which one are they" — driven by field feedback from a peer agent
that used the mesh in a live session.

## Current state
- ✅ **Handle collision fixed** (`31ce44f`, tests `a5059b6`). Manual CLI commands qualify `--agent <name>` → `<name>-<sessionToken>` once at flag-read time via `resolveAgent`→`qualifyAgent` (cmd/agentroom/main.go). Token = `shortSession(CLAUDE_SESSION_ID)` (8-char, same value the hook path derives), else `<host>-<ppid>` outside a Claude session. Handles sanitized of key-breaking chars (`:`, whitespace). Two agents picking `opus-pidrive` → `opus-pidrive-a1b2c3d4` vs `opus-pidrive-9f8e7d6c`, distinct across presence key, event attribution, and claim ownership.
- ✅ **`agentroom who` command** (`21bfae6`, lib `a33c022`, tests `516350e`+`9dcef8f`). On-demand roster: per-agent role, claim load, and **TTL remaining**. Pure observer — does NOT heartbeat, so running it doesn't make you appear. Self tagged `(you)`. Backed by new lib method `Room.PresenceDetailed` (agentroom/room.go) returning `PresenceEntry{Desc, TTL}` (kept separate from hot-path `Presence` to avoid TTL reads in the per-prompt digest).
- ✅ **Blank presence bodies render legibly** — `who` shows `(no role posted)` instead of nothing for heartbeat-only/hook entries (render-layer fix; storage model unchanged — empty desc is by-design).
- ✅ **Docs fix** (`e188480`) — digest no longer says `tail` "watches the feed" (it's one-shot → "print recent events"); `who` documented in digest + README command table.
- ✅ **Verified**: `go build ./...` clean, `go vet ./...` clean, `go test ./...` → 82 passed (3 pkgs), `agentroom who` confirmed in `--help`. Tree clean (all committed).

## Invariants
- `Presence` (hot per-prompt digest path) must enumerate via the per-room presence index and batch description reads only — do NOT add TTL reads to it; that's why `PresenceDetailed` is separate.
- Handle qualification must produce the SAME identity for all sinks of one command (presence/event/claim) — keep it at the single `resolveAgent` chokepoint, never re-derive per-sink.
- `who` must remain a pure observer (no heartbeat write) — looking must not create presence.
- Qualification falls back to `<host>-<ppid>` when `CLAUDE_SESSION_ID` is absent — never regress plain-terminal use to an unqualified bare handle.

## Next step
✅ **Done — the 4 deferred items below are queued to the AFK backlog** (2026-06-24), tagged `agentroom-presence`. Task IDs: `899d803b` (#1/#5 absence-blindness), `4d672b62` (#6 join-trap), `84f93ad1` (#7 threading), `871b2c29` (double-identity). A future session resumes any of them via `afk task <id>`; all are intentionally design decisions, not yet implemented. Nothing else open from this session.

## Open questions / risks (all queued to AFK — see Next step)
- **#1 / #5 — presence is blind to absence.** "nobody here" gives false confidence (presence only exists if an agent called the CLI); clean-exit DEL is unreliable; anonymous `cli@host:pid` markers with no role/event backing clutter the roster. Honesty/design problem, partly docs.
- **#6 — delta-cursor join trap.** An agent that joins AFTER a `CONFIG_CHANGED`/`WORK_COMPLETED` baselines its cursor at the stream tail and never sees that earlier event (peer had to re-post). Consider replay-of-recent-unacked on presence-join vs. keeping it a re-post convention.
- **#7 — no targeting/threading primitive.** "Addressing" an agent is just a `to:` field convention on a broadcast; no DM/thread, acks easy to miss, ordering by-convention.
- **Single-agent double identity (scoped out of #2).** One agent still shows twice: hook presence keyed `a1b2c3d4` + manual `opus-pidrive-a1b2c3d4`. User explicitly deferred unification. They now share the visible session token so they're correlatable, but full single-identity unification (rework hook session-start/end + presence rendering) remains undone.

## Evidence anchors
- Files: cmd/agentroom/main.go (`qualifyAgent`/`sessionToken`/`sanitizeHandle`/`resolveAgent`, ~L312+; 5 commands route through `resolveAgent`); cmd/agentroom/who.go (`whoCmd`/`whoLines`/`humanTTL`); agentroom/room.go (`PresenceEntry`/`PresenceDetailed`, ~L226); cmd/agentroom/hook.go:131 (digest wording); README.md command table.
- Verify: `go build ./... && go vet ./... && go test ./...` (expect 82 pass); `go build -o /tmp/ar ./cmd/agentroom && /tmp/ar who --help`.
- Refs: commits a5059b6 31ce44f a33c022 21bfae6 e188480 516350e 9dcef8f. CLAUDE_SESSION_ID exposed to Bash tool commands since Claude Code v2.1.132 (the bridge that makes handle qualification possible on the manual path).
