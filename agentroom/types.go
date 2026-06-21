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
