// Package agentroom is a low-overhead, Redis-Streams-backed event mesh for
// coordinating multiple agent workers across isolated repo/branch namespaces.
//
// The package has three cooperating concerns:
//
//   - Room (room.go) publishes immutable events to a per-repo/branch stream and
//     reads/writes a TTL'd scratchpad for heavy transient data. Each publish
//     refreshes an idle-expiry lease, so a silent stream auto-expires after
//     Config.StreamTTL.
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
//   - Archiver (archive.go) compacts streams that grow past a length threshold.
//     It discovers streams with SCAN (cursor-based and non-blocking — not the
//     O(N) blocking KEYS), snapshots each over-threshold stream, hands the batch
//     to a PersistFunc, then deletes only the exact archived entry IDs, so events
//     appended during the sweep are preserved.
//
// Configuration values (Redis address, TTL, threshold, group) are supplied by
// the caller through Config; DefaultConfig provides documented fallbacks. The
// library itself reads no configuration and depends only on go-redis.
package agentroom
