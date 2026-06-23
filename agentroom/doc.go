// Package agentroom is a low-overhead, Redis-Streams-backed event mesh for
// coordinating multiple agent workers across isolated repo/branch namespaces.
//
// The package has four cooperating concerns:
//
//   - Room (room.go) publishes immutable events to a per-repo/branch stream,
//     reads/writes a TTL'd scratchpad for heavy transient data, and reads recent
//     activity via Recent. Each publish refreshes an idle-expiry lease, so a
//     silent stream auto-expires after Config.StreamTTL. Room also tracks live
//     presence: Heartbeat writes a per-agent TTL key (the agent's role label),
//     RefreshPresence extends its TTL without disturbing the label, ClearPresence
//     deletes it, and Presence enumerates the live set by SCANning the presence
//     keys. Liveness is TTL-based — a crashed agent drops within Config.PresenceTTL
//     with no explicit exit needed.
//
//   - Runtime (agent.go) wraps a Worker and consumes the stream through a Redis
//     consumer group. Delivery is at-least-once and survives restarts: the group
//     persists its position, so events published while a worker is down are
//     delivered on reconnect, and entries left pending by a crashed worker are
//     reclaimed via XAUTOCLAIM. Each distinct worker TYPE should use its own
//     Config.Group; instances of one type share a group (load-balanced) with
//     unique Worker.ID() consumer names. Worker.Interests further filters which
//     delivered events the worker acts on.
//
//   - Discovery (discovery.go) is an optional, self-describing coordination
//     layer: RegisterTask and Catalog publish and discover task-type contracts,
//     while Claim, Complete, TaskState, and OpenTasks let agents atomically take
//     work without colliding — the cross-agent guard the consumer group cannot
//     give. These are affordances, not mandates; the mesh stays free-form.
//
//   - Archiver (archive.go) compacts streams that grow past a length threshold.
//     It discovers streams with SCAN (cursor-based and non-blocking — not the
//     O(N) blocking KEYS), snapshots each over-threshold stream, hands the batch
//     to a PersistFunc, then deletes only the exact archived entry IDs, so events
//     appended during the sweep are preserved.
//
// Configuration values (Redis address, TTL, threshold, group) are supplied by
// the caller through Config; DefaultConfig provides documented fallbacks. The
// library itself reads no configuration and depends only on go-redis.
//
// The cmd/agentroom CLI exposes these operations to shell-capable agents
// (tail, post, catalog, open, claim, done, welcome) and ships Claude Code
// SessionStart/SessionEnd hooks that greet agents with room activity and post a
// session summary on exit. Every CLI invocation refreshes the agent's presence
// TTL (an opportunistic heartbeat); session-start sets the role label and
// session-end deletes the presence key. The "who's here" digest renders each
// live agent with its outstanding claim count, computed at render time.
package agentroom
