package agentroom

import (
	"context"
	"errors"
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
			"to":        ev.To,
			"reply_to":  ev.ReplyTo,
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

// directedScanWindow bounds how far back Directed scans for messages addressed
// to an agent — directed messages are sparse among broadcasts, so it reads a
// wide recent slice and filters rather than maintaining a per-recipient index.
const directedScanWindow = 200

// Directed returns up to count of the most recent events addressed to agentID
// (Event.To == agentID), newest-first — the read side of directed messaging.
// Broadcast events (empty To) are excluded. It scans the recent window (bounded
// by directedScanWindow) and filters; a recipient with no directed messages in
// that window gets an empty slice.
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

// LastID returns the stream's most recent entry ID, or "0-0" when the stream is
// empty. It seeds a session's read cursor so the first delta read covers only
// events published after the cursor was set (never the full history).
func (r *Room) LastID(ctx context.Context) (string, error) {
	msgs, err := r.rdb.XRevRangeN(ctx, r.cfg.StreamKey(), "+", "-", 1).Result()
	if err != nil {
		return "", fmt.Errorf("agentroom: last id: %w", err)
	}
	if len(msgs) == 0 {
		return "0-0", nil
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

// RefreshSessionPresence refreshes the TTL of every "<handle>-<sessionToken>"
// presence key — the named records a manual `AGENT_JOINED --agent <handle>`
// created — preserving each description via RefreshPresence. The per-prompt hook
// calls this so a named roster entry stays live alongside the bare session key
// instead of expiring after PresenceTTL while only the anonymous session line is
// refreshed (the presence identity-split bug). The bare "<sessionToken>" key has
// no "-" separator and is refreshed separately by the hook's own heartbeat.
func (r *Room) RefreshSessionPresence(ctx context.Context, sessionToken string, ttl time.Duration) error {
	if sessionToken == "" {
		return nil
	}
	prefix := r.cfg.PresencePrefix()
	iter := r.rdb.Scan(ctx, 0, prefix+"*-"+sessionToken, 0).Iterator()
	for iter.Next(ctx) {
		if err := ctx.Err(); err != nil {
			return err
		}
		agentID := strings.TrimPrefix(iter.Val(), prefix)
		if err := r.RefreshPresence(ctx, agentID, ttl); err != nil {
			return err
		}
	}
	if err := iter.Err(); err != nil {
		return fmt.Errorf("agentroom: scan session presence %s: %w", sessionToken, err)
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
		if errors.Is(err, redis.Nil) {
			continue // key expired between SCAN and GET — not present
		}
		if err != nil {
			return nil, fmt.Errorf("agentroom: read presence %s: %w", key, err)
		}
		out[strings.TrimPrefix(key, prefix)] = desc
	}
	if err := iter.Err(); err != nil {
		return nil, fmt.Errorf("agentroom: scan presence: %w", err)
	}
	return out, nil
}

// PresenceEntry is one live roster record: the agent's free-form description and
// the time left on its presence TTL before it drops absent a refresh. Surfaced
// by the `who` command; Presence (the hot digest path) omits the TTL to skip the
// extra PTTL round-trip per key.
type PresenceEntry struct {
	Desc string
	TTL  time.Duration
}

// PresenceDetailed is Presence plus each key's remaining TTL — the on-demand
// roster view behind `who`. Same cursor-based SCAN (never KEYS); additionally
// reads PTTL per key. Keys that expire mid-scan are skipped. Kept separate from
// Presence so the per-prompt digest path stays a single GET per key.
func (r *Room) PresenceDetailed(ctx context.Context) (map[string]PresenceEntry, error) {
	prefix := r.cfg.PresencePrefix()
	out := make(map[string]PresenceEntry)
	iter := r.rdb.Scan(ctx, 0, prefix+"*", 0).Iterator()
	for iter.Next(ctx) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		key := iter.Val()
		desc, err := r.rdb.Get(ctx, key).Result()
		if errors.Is(err, redis.Nil) {
			continue // expired between SCAN and GET
		}
		if err != nil {
			return nil, fmt.Errorf("agentroom: read presence %s: %w", key, err)
		}
		ttl, err := r.rdb.PTTL(ctx, key).Result()
		if err != nil {
			return nil, fmt.Errorf("agentroom: ttl presence %s: %w", key, err)
		}
		out[strings.TrimPrefix(key, prefix)] = PresenceEntry{Desc: desc, TTL: ttl}
	}
	if err := iter.Err(); err != nil {
		return nil, fmt.Errorf("agentroom: scan presence detailed: %w", err)
	}
	return out, nil
}
