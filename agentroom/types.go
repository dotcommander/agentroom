// Package agentroom implements a low-overhead, Redis-Streams-backed event mesh
// for coordinating multiple agent workers across isolated repo/branch namespaces.
package agentroom

import (
	"encoding/base64"
	"encoding/json"
	"time"
)

// EventEngineRuntimeError is published by a Runtime when its Worker.Execute
// returns an error, surfacing the failure as a durable, cross-agent event.
const EventEngineRuntimeError = "ENGINE_RUNTIME_ERROR"

// Event is an immutable, timestamped marker in a repo/branch stream.
type Event struct {
	ID        string          `json:"id"`                 // stream-assigned entry ID
	Type      string          `json:"type"`               // e.g. "AST_PARSED", "TESTS_FAILED"
	AgentID   string          `json:"agent_id"`           // initiating engine
	To        string          `json:"to,omitempty"`       // directed recipient agent id ("" = broadcast)
	ReplyTo   string          `json:"reply_to,omitempty"` // stream id this threads under ("" = top-level)
	Payload   json.RawMessage `json:"payload"`            // small metadata or scratchpad refs
	Timestamp int64           `json:"timestamp"`          // unix-nano
}

// Config isolates one room to a repo + branch namespace and carries the mesh
// tunables. Values come from the caller (viper/env in the consuming app); the
// library never reads config itself. DefaultConfig supplies documented fallbacks.
type Config struct {
	RedisAddr        string
	RepoID           string
	BranchName       string
	StreamTTL        time.Duration // stream auto-expires this long after the last publish
	StreamMaxLen     int64         // approximate XADD MAXLEN cap; <=0 disables (applies even to persisted/no-TTL streams)
	MaxPayloadBytes  int64         // max Event.Payload size accepted by Publish; <=0 disables
	InboxMaxLen      int64         // approximate per-recipient inbox stream cap; <=0 disables
	InboxTTL         time.Duration // inbox stream auto-expires this long after the last enqueue
	InboxCursorTTL   time.Duration // per-recipient inbox cursor expiry after last delivery
	ArchiveThreshold int64         // stream length that triggers compaction
	SweepInterval    time.Duration // how often agentroomd re-runs the archive sweep
	Group            string        // consumer-group name for Runtime delivery
	WorkerReceiptTTL time.Duration // completed worker receipts expire after this duration; non-positive uses the default and values above the safety cap are clamped
	PresenceTTL      time.Duration // per-agent presence key auto-expires this long after the last CLI activity (opportunistic heartbeat)
	CursorTTL        time.Duration // per-session read-cursor key auto-expires this long after the last refresh; a lost/expired cursor simply re-baselines to the stream tail
	JoinReplayWindow time.Duration // a freshly-joined session seeds its read cursor this far back instead of the bare tail, so it replays a peer's just-landed events (the join-trap); non-positive disables replay
}

// defaultGroup is the fallback consumer-group name used when Config.Group is
// unset; single source of truth for DefaultConfig and Runtime.group.
const defaultGroup = "agents"

const (
	defaultWorkerReceiptTTL = 7 * 24 * time.Hour
	maxWorkerReceiptTTL     = 30 * 24 * time.Hour
)

const streamStartID = "0-0"

// DefaultConfig returns the documented fallback tunables. This is the single
// sanctioned home for these literals; callers override per environment.
func DefaultConfig() Config {
	return Config{
		RedisAddr:        "localhost:6379",
		StreamTTL:        48 * time.Hour,
		StreamMaxLen:     10000,
		MaxPayloadBytes:  16 * 1024,
		InboxMaxLen:      1000,
		InboxTTL:         30 * 24 * time.Hour,
		InboxCursorTTL:   30 * 24 * time.Hour,
		ArchiveThreshold: 10000,
		SweepInterval:    24 * time.Hour,
		Group:            defaultGroup,
		WorkerReceiptTTL: defaultWorkerReceiptTTL,
		PresenceTTL:      15 * time.Minute,
		CursorTTL:        24 * time.Hour,
		JoinReplayWindow: 10 * time.Minute,
	}
}

func (c Config) workerReceiptTTL() time.Duration {
	if c.WorkerReceiptTTL <= 0 {
		return defaultWorkerReceiptTTL
	}
	if c.WorkerReceiptTTL > maxWorkerReceiptTTL {
		return maxWorkerReceiptTTL
	}
	return c.WorkerReceiptTTL
}

// roomPrefix is the "repo:<id>:<branch>" namespace every room key is built on —
// the single source of truth for the key layout that stream archiving and room
// coordination keys depend on.
func (c Config) roomPrefix() string {
	return "repo:" + c.RepoID + ":" + c.BranchName
}

// StreamKey is the Redis Streams key for this room's event log.
func (c Config) StreamKey() string {
	return c.roomPrefix() + ":events"
}

// workerReceiptKey identifies successful handling of one stream event by one
// consumer group. Event IDs cannot contain colons, but group names can, so the
// group is encoded to keep the key tuple unambiguous.
func (c Config) workerReceiptKey(group, eventID string) string {
	return c.roomPrefix() + ":worker-receipt:" + base64.RawURLEncoding.EncodeToString([]byte(group)) + ":" + eventID
}

// ScratchpadPrefix is the key prefix for this room's transient KV scratchpad.
func (c Config) ScratchpadPrefix() string {
	return c.roomPrefix() + ":state:"
}

// PresencePrefix is the key prefix for this room's live per-agent presence
// records.
func (c Config) PresencePrefix() string {
	return c.roomPrefix() + ":presence:"
}

// PresenceIndexKey is the per-room sorted set of active agent IDs scored by
// presence expiry time in Unix milliseconds. It lets roster reads enumerate the
// room directly instead of SCANning the Redis database for PresencePrefix.
func (c Config) PresenceIndexKey() string {
	return c.roomPrefix() + ":presence:index"
}

// PresenceKey is the TTL key holding one agent's presence description (role /
// working_on). Written on join and refreshed on each CLI invocation; it expires
// PresenceTTL after the last activity, so a crashed agent drops automatically.
func (c Config) PresenceKey(agentID string) string {
	return c.PresencePrefix() + agentID
}

// CursorKey is the per-session TTL key holding the last stream entry ID a
// session has already seen. The UserPromptSubmit hook reads events after this ID
// and advances it, so each prompt injects only the delta. It expires CursorTTL
// after the last refresh, so a dead session's cursor self-evicts.
func (c Config) CursorKey(sessionID string) string {
	return c.roomPrefix() + ":cursor:" + sessionID
}

// InboxKey is the durable per-recipient stream for directed messages addressed
// to recipient. Unlike the room stream cursor, inbox reads start at 0-0 when no
// cursor exists so offline directed messages are not silently skipped.
func (c Config) InboxKey(recipient string) string {
	return c.roomPrefix() + ":inbox:" + recipient
}

// InboxCursorKey stores the last delivered entry ID for one recipient inbox.
func (c Config) InboxCursorKey(recipient string) string {
	return c.roomPrefix() + ":inboxcursor:" + recipient
}

// PendingAskKey is the singleton waiter lease for one asking agent. It prevents
// one agent from starting multiple blocking asks in the same room and losing the
// reply correlation contract.
func (c Config) PendingAskKey(agentID string) string {
	return c.roomPrefix() + ":ask:" + agentID
}

// CatalogKey is the Redis hash holding this room's self-describing task catalog
// (task type -> TaskDef). Agents read it to discover what work the room knows.
func (c Config) CatalogKey() string {
	return c.roomPrefix() + ":catalog"
}

// TaskKey is the base key for one task's coordination state, suffixed with
// ":owner" (the claim lease) and ":done" (the completion record).
func (c Config) TaskKey(id string) string {
	return c.roomPrefix() + ":task:" + id
}

// Task coordination states reported by TaskState.
const (
	TaskOpen    = "open"
	TaskClaimed = "claimed"
	TaskDone    = "done"
)

// TaskDef is the self-describing contract for a task type: what it means, what
// an agent should emit on success, and what capability it needs. Agents publish
// these to the catalog so others can discover how to participate.
type TaskDef struct {
	Type         string `json:"type"`                   // task/event type, e.g. "TESTS_FAILED"
	Description  string `json:"description"`            // what it means and when it applies
	Produces     string `json:"produces"`               // event type emitted on success
	Requires     string `json:"requires"`               // capability an agent needs to handle it
	Prerequisite string `json:"prerequisite,omitempty"` // event type that must exist in the stream before this task may be claimed; "" disables gating
}

// Task is a unit of work derived from a catalogued stream event, identified by
// the triggering event's stream entry ID.
type Task struct {
	ID      string          `json:"id"`
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

// InboxEvent is one durable recipient-inbox entry. ID is the inbox stream entry
// ID; SourceID is the original room-stream entry ID for dedupe against normal
// room deltas.
type InboxEvent struct {
	ID           string
	SourceStream string
	SourceID     string
	Event        Event
}

// TaskStatus is the coordination state of a task: TaskOpen (claimable),
// TaskClaimed (Owner set), or TaskDone (Result set).
type TaskStatus struct {
	State  string          `json:"state"`
	Owner  string          `json:"owner,omitempty"`
	Result json.RawMessage `json:"result,omitempty"`
}

// matches reports whether a worker subscribed to interests should receive an
// event of eventType. "*" subscribes to everything.
func matches(interests []string, eventType string) bool {
	for _, interest := range interests {
		if interest == "*" || interest == eventType {
			return true
		}
	}
	return false
}
