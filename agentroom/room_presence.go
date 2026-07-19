package agentroom

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// Heartbeat writes (or refreshes) this agent's live-presence record: a TTL key
// holding desc (role / working_on). It is called on join and on every CLI
// invocation, so the key auto-expires ttl after the agent's last activity — a
// crashed agent drops from presence with no SESSION_ENDED needed.
func (r *Room) Heartbeat(ctx context.Context, agentID, desc string, ttl time.Duration) error {
	return r.HeartbeatIdentity(ctx, AgentIdentity{AgentID: agentID}, desc, ttl)
}

// HeartbeatIdentity writes a versioned presence record while retaining the
// legacy Heartbeat entry point for callers that only know the full agent ID.
func (r *Room) HeartbeatIdentity(ctx context.Context, identity AgentIdentity, desc string, ttl time.Duration) error {
	if identity.AgentID == "" {
		return errors.New("agentroom: presence agent id is required")
	}
	value, err := json.Marshal(presenceRecord{SchemaVersion: 1, Identity: identity, Desc: desc})
	if err != nil {
		return fmt.Errorf("agentroom: marshal presence %s: %w", identity.AgentID, err)
	}
	pipe := r.rdb.Pipeline()
	pipe.Set(ctx, r.cfg.PresenceKey(identity.AgentID), value, ttl)
	pipe.ZAdd(ctx, r.cfg.PresenceIndexKey(), redis.Z{
		Score:  presenceExpiryScore(ttl),
		Member: identity.AgentID,
	})
	if r.cfg.StreamTTL > 0 {
		pipe.Expire(ctx, r.cfg.PresenceIndexKey(), r.cfg.StreamTTL)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("agentroom: heartbeat %s: %w", identity.AgentID, err)
	}
	return nil
}

// refreshPresenceScript refreshes a presence key's TTL without disturbing its
// description; if the key is absent it creates a label-less record. This keeps
// liveness-only activity (claim/tail/non-JOINED post) from clobbering a role
// label or hook-inferred working-on label while still registering liveness.
var refreshPresenceScript = redis.NewScript(`
if redis.call('pexpire', KEYS[1], ARGV[1]) == 0 then
	redis.call('set', KEYS[1], ARGV[4], 'PX', ARGV[1])
end
redis.call('zadd', KEYS[2], ARGV[3], ARGV[2])
return 1
`)

// RefreshPresence extends agentID's presence TTL, preserving any existing
// description, and creates an empty record if none exists.
func (r *Room) RefreshPresence(ctx context.Context, agentID string, ttl time.Duration) error {
	return r.RefreshPresenceIdentity(ctx, AgentIdentity{AgentID: agentID}, ttl)
}

// RefreshPresenceIdentity refreshes liveness without replacing an existing
// description and seeds a structured identity when the record is absent.
func (r *Room) RefreshPresenceIdentity(ctx context.Context, identity AgentIdentity, ttl time.Duration) error {
	agentID := identity.AgentID
	keys := []string{r.cfg.PresenceKey(agentID), r.cfg.PresenceIndexKey()}
	empty, err := json.Marshal(presenceRecord{SchemaVersion: 1, Identity: identity})
	if err != nil {
		return fmt.Errorf("agentroom: marshal presence %s: %w", agentID, err)
	}
	if err := refreshPresenceScript.Run(ctx, r.rdb, keys, ttl.Milliseconds(), agentID, presenceExpiryScore(ttl), empty).Err(); err != nil {
		return fmt.Errorf("agentroom: refresh presence %s: %w", agentID, err)
	}
	return nil
}

// ClearSessionPresence removes both the bare session presence entry and every
// named handle entry qualified with that session suffix.
func (r *Room) ClearSessionPresence(ctx context.Context, sessionID string) error {
	if sessionID == "" {
		return nil
	}
	agentIDs, err := r.activePresenceAgentIDs(ctx)
	if err != nil {
		return fmt.Errorf("agentroom: read session presence %s: %w", sessionID, err)
	}
	pipe := r.rdb.Pipeline()
	for _, agentID := range agentIDs {
		if agentID != sessionID && !strings.HasSuffix(agentID, "-"+sessionID) {
			continue
		}
		pipe.Del(ctx, r.cfg.PresenceKey(agentID))
		pipe.ZRem(ctx, r.cfg.PresenceIndexKey(), agentID)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("agentroom: clear session presence %s: %w", sessionID, err)
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
	agentIDs, err := r.activePresenceAgentIDs(ctx)
	if err != nil {
		return fmt.Errorf("agentroom: read session presence %s: %w", sessionToken, err)
	}
	for _, agentID := range agentIDs {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !strings.HasSuffix(agentID, "-"+sessionToken) {
			continue
		}
		if err := r.RefreshPresence(ctx, agentID, ttl); err != nil {
			return err
		}
	}
	return nil
}

// ClearPresence deletes this agent's presence record for a clean fast exit
// (called on SESSION_ENDED). Absence of the key is not an error.
func (r *Room) ClearPresence(ctx context.Context, agentID string) error {
	pipe := r.rdb.Pipeline()
	pipe.Del(ctx, r.cfg.PresenceKey(agentID))
	pipe.ZRem(ctx, r.cfg.PresenceIndexKey(), agentID)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("agentroom: clear presence %s: %w", agentID, err)
	}
	return nil
}

// Presence returns the live presence set as agentID -> description from the
// room's indexed presence roster. Expired keys are simply absent. This is the
// liveness-backed replacement for folding AGENT_JOINED/SESSION_ENDED off the
// event stream.
func (r *Room) Presence(ctx context.Context) (map[string]string, error) {
	entries, err := r.presenceScan(ctx, false)
	if err != nil {
		return nil, err
	}
	out := make(map[string]string, len(entries))
	for id, e := range entries {
		out[id] = e.Desc
	}
	return out, nil
}

// PresenceEntry is one live roster record: the agent's free-form description and
// the time left on its presence TTL before it drops absent a refresh. Surfaced
// by the `who` command; Presence (the hot digest path) omits the TTL so it only
// batches description reads.
type PresenceEntry struct {
	Identity AgentIdentity `json:"identity"`
	Desc     string        `json:"description"`
	TTL      time.Duration `json:"ttl"`
}

type presenceRecord struct {
	SchemaVersion int           `json:"schema_version"`
	Identity      AgentIdentity `json:"identity"`
	Desc          string        `json:"description"`
}

// PresenceDetailed is Presence plus each key's remaining TTL — the on-demand
// roster view behind `who`. It uses the same indexed roster read and additionally
// batches PTTL reads. Keys that expire mid-read are skipped. Kept separate from
// Presence so the per-prompt digest path does not read TTLs.
func (r *Room) PresenceDetailed(ctx context.Context) (map[string]PresenceEntry, error) {
	return r.presenceScan(ctx, true)
}

// presenceScan is the shared indexed read behind Presence and PresenceDetailed:
// it trims expired roster index entries, reads active agent IDs from the room's
// ZSET, then batches description reads and — only when withTTL — TTL reads. Keys
// that expire between the index read and batched reads are skipped.
func (r *Room) presenceScan(ctx context.Context, withTTL bool) (map[string]PresenceEntry, error) {
	prefix := r.cfg.PresencePrefix()
	agentIDs, err := r.activePresenceAgentIDs(ctx)
	if err != nil {
		return nil, err
	}
	if len(agentIDs) == 0 {
		return map[string]PresenceEntry{}, nil
	}
	pipe := r.rdb.Pipeline()
	entries := make([]presenceReadCmds, 0, len(agentIDs))
	for _, agentID := range agentIDs {
		key := prefix + agentID
		cmds := presenceReadCmds{
			key:  key,
			desc: pipe.Get(ctx, key),
		}
		if withTTL {
			cmds.ttl = pipe.PTTL(ctx, key)
		}
		entries = append(entries, cmds)
	}
	_, _ = pipe.Exec(ctx)
	return presenceEntries(prefix, entries, withTTL)
}

func (r *Room) activePresenceAgentIDs(ctx context.Context) ([]string, error) {
	now := time.Now().UnixMilli()
	pipe := r.rdb.Pipeline()
	pipe.ZRemRangeByScore(ctx, r.cfg.PresenceIndexKey(), "-inf", strconv.FormatInt(now, 10))
	active := pipe.ZRangeByScore(ctx, r.cfg.PresenceIndexKey(), &redis.ZRangeBy{
		Min: strconv.FormatInt(now, 10),
		Max: "+inf",
	})
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, fmt.Errorf("agentroom: read presence index: %w", err)
	}
	return active.Result()
}

func presenceExpiryScore(ttl time.Duration) float64 {
	return float64(time.Now().Add(ttl).UnixMilli())
}

type presenceReadCmds struct {
	key  string
	desc *redis.StringCmd
	ttl  *redis.DurationCmd
}

func presenceEntries(prefix string, entries []presenceReadCmds, withTTL bool) (map[string]PresenceEntry, error) {
	out := make(map[string]PresenceEntry, len(entries))
	for _, cmds := range entries {
		desc, err := cmds.desc.Result()
		if errors.Is(err, redis.Nil) {
			continue // key expired between SCAN and GET — not present
		}
		if err != nil {
			return nil, fmt.Errorf("agentroom: read presence %s: %w", cmds.key, err)
		}
		entry := decodePresenceEntry(desc)
		if entry.Identity.AgentID == "" {
			entry.Identity.AgentID = strings.TrimPrefix(cmds.key, prefix)
		}
		if withTTL {
			ttl, err := cmds.ttl.Result()
			if errors.Is(err, redis.Nil) {
				continue // key expired between GET and PTTL — not present
			}
			if err != nil {
				return nil, fmt.Errorf("agentroom: ttl presence %s: %w", cmds.key, err)
			}
			entry.TTL = ttl
		}
		out[strings.TrimPrefix(cmds.key, prefix)] = entry
	}
	return out, nil
}

func decodePresenceEntry(raw string) PresenceEntry {
	var record presenceRecord
	if json.Unmarshal([]byte(raw), &record) == nil && record.SchemaVersion == 1 {
		return PresenceEntry{Identity: record.Identity, Desc: record.Desc}
	}
	return PresenceEntry{Desc: raw}
}
