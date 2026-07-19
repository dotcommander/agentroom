package agentroom

import (
	"context"
	"errors"
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
	if r.cfg.MaxPayloadBytes > 0 && int64(len(ev.Payload)) > r.cfg.MaxPayloadBytes {
		return fmt.Errorf("agentroom: payload is %d bytes, max %d bytes", len(ev.Payload), r.cfg.MaxPayloadBytes)
	}
	if ev.Timestamp == 0 {
		ev.Timestamp = time.Now().UnixNano()
	}
	pipe := r.rdb.Pipeline()
	args := &redis.XAddArgs{
		Stream: r.cfg.StreamKey(),
		Values: map[string]any{
			"type":         ev.Type,
			"agent_id":     ev.AgentID,
			"agent_handle": ev.AgentHandle,
			"session_id":   ev.SessionID,
			"to":           ev.To,
			"reply_to":     ev.ReplyTo,
			"payload":      []byte(ev.Payload),
			"timestamp":    ev.Timestamp,
		},
	}
	if r.cfg.StreamMaxLen > 0 {
		args = &redis.XAddArgs{
			Stream: r.cfg.StreamKey(),
			MaxLen: r.cfg.StreamMaxLen,
			Approx: true, // MAXLEN ~ N: cheap trimming, bounds even Persist'd lobby/welcome streams
			Values: args.Values,
		}
	}
	add := pipe.XAdd(ctx, args)
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

// EventByID returns the exact stream event id when it still exists in the room
// stream. The bool is false when the id is absent, expired, or trimmed.
func (r *Room) EventByID(ctx context.Context, id string) (Event, bool, error) {
	msgs, err := r.rdb.XRangeN(ctx, r.cfg.StreamKey(), id, id, 1).Result()
	if err != nil {
		return Event{}, false, fmt.Errorf("agentroom: event %s: %w", id, err)
	}
	if len(msgs) == 0 {
		return Event{}, false, nil
	}
	return decodeEvent(msgs[0]), true, nil
}

// BeginAsk creates the singleton blocking-ask lease for agentID with token. It
// returns false when the agent already has an outstanding ask in this room.
func (r *Room) BeginAsk(ctx context.Context, agentID, token string, ttl time.Duration) (bool, error) {
	ok, err := r.rdb.SetNX(ctx, r.cfg.PendingAskKey(agentID), token, ttl).Result()
	if err != nil {
		return false, fmt.Errorf("agentroom: begin ask %s: %w", agentID, err)
	}
	return ok, nil
}

var endAskScript = redis.NewScript(`
if redis.call('get', KEYS[1]) == ARGV[1] then
	return redis.call('del', KEYS[1])
end
return 0
`)

// EndAsk clears agentID's singleton blocking-ask lease only when it still points
// at token, so an expired-and-replaced lease is not deleted by a stale waiter.
func (r *Room) EndAsk(ctx context.Context, agentID, token string) error {
	if err := endAskScript.Run(ctx, r.rdb, []string{r.cfg.PendingAskKey(agentID)}, token).Err(); err != nil {
		return fmt.Errorf("agentroom: end ask %s: %w", agentID, err)
	}
	return nil
}

// directedScanWindow bounds how far back Directed scans for messages addressed
// to an agent — directed messages are sparse among broadcasts, so it reads a
// wide recent slice and filters rather than maintaining a per-recipient index.
const directedScanWindow = 200

// Directed returns up to count of the most recent stream events addressed to
// agentID (Event.To == agentID), newest-first. Broadcast events (empty To) are
// excluded. This is a compatibility/read-recent helper: it scans only the
// bounded recent window, so durable offline delivery should use EnqueueInbox and
// InboxSince instead.
func (r *Room) Directed(ctx context.Context, agentID string, count int64) ([]Event, error) {
	if agentID == "" {
		return nil, nil
	}
	recent, err := r.Recent(ctx, directedScanWindow)
	if err != nil {
		return nil, err
	}
	out := make([]Event, 0, count)
	for i := len(recent) - 1; i >= 0; i-- { // Recent is oldest-first; walk back for newest-first
		if recent[i].To == agentID {
			out = append(out, recent[i])
			if int64(len(out)) >= count {
				break
			}
		}
	}
	return out, nil
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

// Since returns up to count events with stream IDs strictly after lastID, in
// chronological order — the delta counterpart to Recent ("what landed since I
// last looked"). An empty lastID returns no events (the caller has no cursor
// yet). XRANGE treats lastID inclusively, so the matching entry is skipped.
func (r *Room) Since(ctx context.Context, lastID string, count int64) ([]Event, error) {
	if lastID == "" {
		return nil, nil
	}
	msgs, err := r.rdb.XRangeN(ctx, r.cfg.StreamKey(), lastID, "+", count+1).Result()
	if err != nil {
		return nil, fmt.Errorf("agentroom: since %s: %w", lastID, err)
	}
	events := make([]Event, 0, len(msgs))
	for _, msg := range msgs {
		if msg.ID == lastID {
			continue
		}
		events = append(events, decodeEvent(msg))
	}
	if int64(len(events)) > count {
		events = events[:count]
	}
	return events, nil
}

// RecentSince returns the most recent events strictly after lastID, preserving
// chronological output order while bounding the reverse scan.
func (r *Room) RecentSince(ctx context.Context, lastID string, count int64) ([]Event, error) {
	msgs, err := r.rdb.XRevRangeN(ctx, r.cfg.StreamKey(), "+", "("+lastID, count).Result()
	if err != nil {
		return nil, fmt.Errorf("agentroom: recent since %s: %w", lastID, err)
	}
	events := make([]Event, len(msgs))
	for i, msg := range msgs {
		events[len(msgs)-1-i] = decodeEvent(msg)
	}
	return events, nil
}

// Wait blocks until the stream has entries after lastID, then returns them in
// chronological order. It is an interactive/read-only primitive, not a consumer
// group read: it does not claim or ack entries, so it cannot interfere with
// Runtime/agentroomd delivery. A Redis block timeout returns no events and no
// error; context cancellation or deadline expiry returns the context error.
func (r *Room) Wait(ctx context.Context, lastID string, block time.Duration, count int64) ([]Event, error) {
	if lastID == "" {
		lastID = "$"
	}
	if count <= 0 {
		count = 1
	}
	res, err := r.rdb.XRead(ctx, &redis.XReadArgs{
		Streams: []string{r.cfg.StreamKey(), lastID},
		Count:   count,
		Block:   block,
	}).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("agentroom: wait after %s: %w", lastID, err)
	}
	var events []Event
	for _, st := range res {
		for _, msg := range st.Messages {
			events = append(events, decodeEvent(msg))
		}
	}
	return events, nil
}

// LastID returns the stream's most recent entry ID, or "0-0" when the stream is
// empty. It seeds a session's read cursor so the first delta read covers only
// events published after the cursor was set (never the full history).
func (r *Room) LastID(ctx context.Context) (string, error) {
	msgs, err := r.rdb.XRevRangeN(ctx, r.cfg.StreamKey(), "+", "-", 1).Result()
	if err != nil {
		return "", fmt.Errorf("agentroom: last id: %w", err)
	}
	if len(msgs) == 0 {
		return streamStartID, nil
	}
	return msgs[0].ID, nil
}

// ReadCursor returns the last stream entry ID session sessionID has seen, or ""
// when no cursor exists yet (treat as "baseline from the stream tail").
func (r *Room) ReadCursor(ctx context.Context, sessionID string) (string, error) {
	id, err := r.rdb.Get(ctx, r.cfg.CursorKey(sessionID)).Result()
	if errors.Is(err, redis.Nil) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("agentroom: read cursor %s: %w", sessionID, err)
	}
	return id, nil
}

// WriteCursor stores the last stream entry ID seen by session sessionID with a
// TTL, so a dead session's cursor self-evicts.
func (r *Room) WriteCursor(ctx context.Context, sessionID, id string, ttl time.Duration) error {
	if err := r.rdb.Set(ctx, r.cfg.CursorKey(sessionID), id, ttl).Err(); err != nil {
		return fmt.Errorf("agentroom: write cursor %s: %w", sessionID, err)
	}
	return nil
}

// ReplayCursorFrom returns the read cursor a freshly-joined session should start
// from so its first delta replays events from the last window — instead of
// baselining to the bare stream tail and missing a peer's just-landed
// CONFIG_CHANGED/WORK_COMPLETED (the join-trap). The result is a
// millisecond-timestamp stream ID floor ("<ms>-0"); Since(cursor) then yields
// events newer than now-window, still bounded by the caller's count cap. `now`
// is a parameter for testability.
func (r *Room) ReplayCursorFrom(now time.Time, window time.Duration) string {
	return fmt.Sprintf("%d-0", now.Add(-window).UnixMilli())
}
