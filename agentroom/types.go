// Package agentroom implements a low-overhead, Redis-Streams-backed event mesh
// for coordinating multiple agent workers across isolated repo/branch namespaces.
package agentroom

import (
	"encoding/json"
	"time"
)

// EventEngineRuntimeError is published by a Runtime when its Worker.Execute
// returns an error, surfacing the failure as a durable, cross-agent event.
const EventEngineRuntimeError = "ENGINE_RUNTIME_ERROR"

// Event is an immutable, timestamped marker in a repo/branch stream.
type Event struct {
	ID        string          `json:"id"`        // stream-assigned entry ID
	Type      string          `json:"type"`      // e.g. "AST_PARSED", "TESTS_FAILED"
	AgentID   string          `json:"agent_id"`  // initiating engine
	Payload   json.RawMessage `json:"payload"`   // small metadata or scratchpad refs
	Timestamp int64           `json:"timestamp"` // unix-nano
}

// Config isolates one room to a repo + branch namespace and carries the mesh
// tunables. Values come from the caller (viper/env in the consuming app); the
// library never reads config itself. DefaultConfig supplies documented fallbacks.
type Config struct {
	RedisAddr        string
	RepoID           string
	BranchName       string
	StreamTTL        time.Duration // stream auto-expires this long after the last publish
	ArchiveThreshold int64         // stream length that triggers compaction
	Group            string        // consumer-group name for Runtime delivery
	PresenceTTL      time.Duration // per-agent presence key auto-expires this long after the last CLI activity (opportunistic heartbeat)
}

// defaultGroup is the fallback consumer-group name used when Config.Group is
// unset; single source of truth for DefaultConfig and Runtime.group.
const defaultGroup = "agents"

// DefaultConfig returns the documented fallback tunables. This is the single
// sanctioned home for these literals; callers override per environment.
func DefaultConfig() Config {
	return Config{
		RedisAddr:        "localhost:6379",
		StreamTTL:        48 * time.Hour,
		ArchiveThreshold: 10000,
		Group:            defaultGroup,
		PresenceTTL:      90 * time.Second,
	}
}

// StreamKey is the Redis Streams key for this room's event log.
func (c Config) StreamKey() string {
	return "repo:" + c.RepoID + ":" + c.BranchName + ":events"
}

// ScratchpadPrefix is the key prefix for this room's transient KV scratchpad.
func (c Config) ScratchpadPrefix() string {
	return "repo:" + c.RepoID + ":" + c.BranchName + ":state:"
}

// PresencePrefix is the key prefix for this room's live per-agent presence
// records. SCAN this prefix to enumerate agents active within PresenceTTL.
func (c Config) PresencePrefix() string {
	return "repo:" + c.RepoID + ":" + c.BranchName + ":presence:"
}

// PresenceKey is the TTL key holding one agent's presence description (role /
// working_on). Written on join and refreshed on each CLI invocation; it expires
// PresenceTTL after the last activity, so a crashed agent drops automatically.
func (c Config) PresenceKey(agentID string) string {
	return c.PresencePrefix() + agentID
}

// CatalogKey is the Redis hash holding this room's self-describing task catalog
// (task type -> TaskDef). Agents read it to discover what work the room knows.
func (c Config) CatalogKey() string {
	return "repo:" + c.RepoID + ":" + c.BranchName + ":catalog"
}

// TaskKey is the base key for one task's coordination state, suffixed with
// ":owner" (the claim lease) and ":done" (the completion record).
func (c Config) TaskKey(id string) string {
	return "repo:" + c.RepoID + ":" + c.BranchName + ":task:" + id
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
	Type        string `json:"type"`        // task/event type, e.g. "TESTS_FAILED"
	Description string `json:"description"` // what it means and when it applies
	Produces    string `json:"produces"`    // event type emitted on success
	Requires    string `json:"requires"`    // capability an agent needs to handle it
}

// Task is a unit of work derived from a catalogued stream event, identified by
// the triggering event's stream entry ID.
type Task struct {
	ID      string          `json:"id"`
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
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
