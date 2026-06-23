package agentroom

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// Room is one repo/branch namespace's view of the mesh: it publishes events to
// the room's stream and reads/writes the transient scratchpad. A Room holds no
// goroutines and is safe for concurrent use.
type Room struct {
	rdb *redis.Client
	cfg Config
}

// NewRoom binds a Room to an existing redis client and config. The client is
// injected so callers may reuse a pooled client built elsewhere.
func NewRoom(rdb *redis.Client, cfg Config) *Room {
	return &Room{rdb: rdb, cfg: cfg}
}

// Config returns the room's namespace and tunable configuration.
func (r *Room) Config() Config {
	return r.cfg
}

// Publish appends an immutable event to the room stream and refreshes the
// stream's idle-expiry lease (the stream auto-expires cfg.StreamTTL after the
// last publish). The stream-assigned entry ID is written back to ev.ID.
func (r *Room) Publish(ctx context.Context, ev *Event) error {
	if ev.Timestamp == 0 {
		ev.Timestamp = time.Now().UnixNano()
	}
	pipe := r.rdb.Pipeline()
	add := pipe.XAdd(ctx, &redis.XAddArgs{
		Stream: r.cfg.StreamKey(),
		Values: map[string]any{
			"type":      ev.Type,
			"agent_id":  ev.AgentID,
			"payload":   []byte(ev.Payload),
			"timestamp": ev.Timestamp,
		},
	})
	if r.cfg.StreamTTL > 0 {
		pipe.Expire(ctx, r.cfg.StreamKey(), r.cfg.StreamTTL)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("agentroom: publish to %s: %w", r.cfg.StreamKey(), err)
	}
	ev.ID = add.Val()
	return nil
}

// WriteScratchpad stores a transient payload under the room's scratchpad prefix
// with an explicit TTL. Use it for heavy data that should not bloat the stream.
func (r *Room) WriteScratchpad(ctx context.Context, key string, val []byte, ttl time.Duration) error {
	if err := r.rdb.Set(ctx, r.cfg.ScratchpadPrefix()+key, val, ttl).Err(); err != nil {
		return fmt.Errorf("agentroom: write scratchpad %s: %w", key, err)
	}
	return nil
}

// ReadScratchpad returns the value stored under key. A missing or expired key
// surfaces as a wrapped redis.Nil (test with errors.Is(err, redis.Nil)).
func (r *Room) ReadScratchpad(ctx context.Context, key string) ([]byte, error) {
	b, err := r.rdb.Get(ctx, r.cfg.ScratchpadPrefix()+key).Bytes()
	if err != nil {
		return nil, fmt.Errorf("agentroom: read scratchpad %s: %w", key, err)
	}
	return b, nil
}

// Recent returns up to count of the most recent events on the room stream, in
// chronological order (oldest first). It is the read-side counterpart to Publish
// and the primitive behind "what is happening in this room right now".
func (r *Room) Recent(ctx context.Context, count int64) ([]Event, error) {
	msgs, err := r.rdb.XRevRangeN(ctx, r.cfg.StreamKey(), "+", "-", count).Result()
	if err != nil {
		return nil, fmt.Errorf("agentroom: recent: %w", err)
	}
	events := make([]Event, len(msgs))
	for i, msg := range msgs {
		events[len(msgs)-1-i] = decodeEvent(msg)
	}
	return events, nil
}

// Heartbeat writes (or refreshes) this agent's live-presence record: a TTL key
// holding desc (role / working_on). It is called on join and on every CLI
// invocation, so the key auto-expires ttl after the agent's last activity — a
// crashed agent drops from presence with no SESSION_ENDED needed.
func (r *Room) Heartbeat(ctx context.Context, agentID, desc string, ttl time.Duration) error {
	if err := r.rdb.Set(ctx, r.cfg.PresenceKey(agentID), desc, ttl).Err(); err != nil {
		return fmt.Errorf("agentroom: heartbeat %s: %w", agentID, err)
	}
	return nil
}

// refreshPresenceScript refreshes a presence key's TTL without disturbing its
// description; if the key is absent it creates a label-less record. This keeps
// non-join activity (claim/tail/non-JOINED post) from clobbering a role label
// set at sign-in while still registering liveness.
var refreshPresenceScript = redis.NewScript(`
if redis.call('pexpire', KEYS[1], ARGV[1]) == 0 then
	redis.call('set', KEYS[1], '', 'PX', ARGV[1])
end
return 1
`)

// RefreshPresence extends agentID's presence TTL, preserving any existing
// description, and creates an empty record if none exists.
func (r *Room) RefreshPresence(ctx context.Context, agentID string, ttl time.Duration) error {
	if err := refreshPresenceScript.Run(ctx, r.rdb, []string{r.cfg.PresenceKey(agentID)}, ttl.Milliseconds()).Err(); err != nil {
		return fmt.Errorf("agentroom: refresh presence %s: %w", agentID, err)
	}
	return nil
}

// ClearPresence deletes this agent's presence record for a clean fast exit
// (called on SESSION_ENDED). Absence of the key is not an error.
func (r *Room) ClearPresence(ctx context.Context, agentID string) error {
	if err := r.rdb.Del(ctx, r.cfg.PresenceKey(agentID)).Err(); err != nil {
		return fmt.Errorf("agentroom: clear presence %s: %w", agentID, err)
	}
	return nil
}

// Presence returns the live presence set as agentID -> description, by SCANning
// the room's presence prefix (cursor-based, non-blocking — never KEYS) and
// reading each key. Expired keys are simply absent. This is the liveness-backed
// replacement for folding AGENT_JOINED/SESSION_ENDED off the event stream.
func (r *Room) Presence(ctx context.Context) (map[string]string, error) {
	prefix := r.cfg.PresencePrefix()
	out := make(map[string]string)
	iter := r.rdb.Scan(ctx, 0, prefix+"*", 0).Iterator()
	for iter.Next(ctx) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		key := iter.Val()
		desc, err := r.rdb.Get(ctx, key).Result()
		if err != nil {
			continue // key expired between SCAN and GET — treat as not present
		}
		out[strings.TrimPrefix(key, prefix)] = desc
	}
	if err := iter.Err(); err != nil {
		return nil, fmt.Errorf("agentroom: scan presence: %w", err)
	}
	return out, nil
}
