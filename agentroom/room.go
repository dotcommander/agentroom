package agentroom

import (
	"context"
	"fmt"
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
