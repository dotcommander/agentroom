package agentroom

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	WorkStarted   = "started"
	WorkWaiting   = "waiting"
	WorkBlocked   = "blocked"
	WorkCompleted = "completed"
	WorkHandoff   = "handoff"
	WorkFailed    = "failed"

	defaultWorkStatusTTL = 24 * time.Hour
)

var setWorkStatusScript = redis.NewScript(`
local nowp = redis.call('TIME')
local now = tonumber(nowp[1]) * 1000 + math.floor(tonumber(nowp[2]) / 1000)
local status = cjson.decode(ARGV[1])
local streamType = redis.call('TYPE', KEYS[1]).ok
if streamType ~= 'none' and streamType ~= 'stream' then return redis.error_reply('audit key is not a stream') end
local recipientIndexType = redis.call('TYPE', KEYS[5]).ok
if recipientIndexType ~= 'none' and recipientIndexType ~= 'zset' then return redis.error_reply('inbox recipient index is not a sorted set') end
if status.state == 'handoff' then local inboxType = redis.call('TYPE', KEYS[4]).ok;if inboxType ~= 'none' and inboxType ~= 'stream' then return redis.error_reply('inbox key is not a stream') end end
status.updated_at = now
status.expires_at = now + tonumber(ARGV[2])
local raw = cjson.encode(status)
local eventID
if tonumber(ARGV[3]) > 0 then
  eventID = redis.call('XADD', KEYS[1], 'MAXLEN', '~', ARGV[3], '*',
    'type', 'WORK_STATUS', 'agent_id', status.agent_id, 'to', status.to or '',
    'reply_to', '', 'payload', raw, 'timestamp', now * 1000000)
else
  eventID = redis.call('XADD', KEYS[1], '*', 'type', 'WORK_STATUS',
    'agent_id', status.agent_id, 'to', status.to or '', 'reply_to', '',
    'payload', raw, 'timestamp', now * 1000000)
end
if tonumber(ARGV[4]) > 0 then redis.call('PEXPIRE', KEYS[1], ARGV[4]) end
if status.state == 'completed' or status.state == 'failed' then
  redis.call('DEL', KEYS[3])
  redis.call('ZREM', KEYS[2], status.id)
else
  redis.call('SET', KEYS[3], raw, 'PX', ARGV[2])
  redis.call('ZADD', KEYS[2], now, status.id)
end
if status.state == 'handoff' and KEYS[4] ~= '' then
  redis.call('XADD', KEYS[4], '*', 'source_stream', KEYS[1], 'source_id', eventID,
    'type', 'WORK_STATUS', 'agent_id', status.agent_id, 'to', status.to,
    'reply_to', '', 'payload', raw, 'timestamp', now * 1000000)
	  if tonumber(ARGV[5]) > 0 then redis.call('PEXPIRE', KEYS[4], ARGV[5]) end
	  local inboxExpiry = 9223372036854775807
	  if tonumber(ARGV[5]) > 0 then inboxExpiry = now + tonumber(ARGV[5]) end
	  redis.call('ZREMRANGEBYSCORE', KEYS[5], '-inf', now)
	  redis.call('ZADD', KEYS[5], inboxExpiry, status.to)
	  if tonumber(ARGV[5]) > 0 then redis.call('PEXPIRE', KEYS[5], ARGV[5]) end
end
return raw
`)

// SetWorkStatus records one canonical WORK_STATUS event and updates the
// materialized current-state view in the same Redis transaction.
func (r *Room) SetWorkStatus(ctx context.Context, status WorkStatus, ttl time.Duration) (WorkStatus, error) {
	if status.AgentID == "" || status.Scope == "" || status.Summary == "" {
		return WorkStatus{}, errors.New("agentroom: work agent, scope, and summary are required")
	}
	if !validWorkState(status.State) {
		return WorkStatus{}, fmt.Errorf("agentroom: invalid work state %q", status.State)
	}
	if status.State == WorkHandoff && status.To == "" {
		return WorkStatus{}, errors.New("agentroom: handoff requires a destination")
	}
	if ttl <= 0 {
		ttl = defaultWorkStatusTTL
	}
	status.SchemaVersion = 1
	if status.ID == "" {
		status.ID = workStatusID(status.AgentID, status.Scope)
	}
	raw, err := json.Marshal(status)
	if err != nil {
		return WorkStatus{}, fmt.Errorf("agentroom: marshal work status: %w", err)
	}
	inboxKey := ""
	if status.State == WorkHandoff {
		inboxKey = r.cfg.InboxKey(status.To)
	}
	keys := []string{r.cfg.StreamKey(), r.cfg.WorkStatusIndexKey(), r.cfg.WorkStatusKey(status.ID), inboxKey, r.cfg.InboxRecipientIndexKey()}
	result, err := setWorkStatusScript.Run(ctx, r.rdb, keys, raw, ttl.Milliseconds(), r.cfg.StreamMaxLen, r.cfg.StreamTTL.Milliseconds(), r.cfg.InboxTTL.Milliseconds()).Result()
	if err != nil {
		return WorkStatus{}, fmt.Errorf("agentroom: set work status: %w", err)
	}
	if err := json.Unmarshal([]byte(stringValue(result)), &status); err != nil {
		return WorkStatus{}, fmt.Errorf("agentroom: decode work status: %w", err)
	}
	return status, nil
}

// ClearWorkStatus removes one materialized status without deleting its audit
// history. It is idempotent.
func (r *Room) ClearWorkStatus(ctx context.Context, id string) error {
	pipe := r.rdb.Pipeline()
	pipe.Del(ctx, r.cfg.WorkStatusKey(id))
	pipe.ZRem(ctx, r.cfg.WorkStatusIndexKey(), id)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("agentroom: clear work status %s: %w", id, err)
	}
	return nil
}

// WorkStatuses returns current nonterminal work ordered by most recent update.
func (r *Room) WorkStatuses(ctx context.Context) ([]WorkStatus, error) {
	query := indexedRecordQuery{indexKey: r.cfg.WorkStatusIndexKey(), keyFor: r.cfg.WorkStatusKey, reverse: true, label: "work status"}
	statuses, err := indexedRecords[WorkStatus](ctx, r.rdb, query)
	if err != nil {
		return nil, err
	}
	slices.SortStableFunc(statuses, func(a, b WorkStatus) int { return int(b.UpdatedAt - a.UpdatedAt) })
	return statuses, nil
}

func validWorkState(state string) bool {
	switch state {
	case WorkStarted, WorkWaiting, WorkBlocked, WorkCompleted, WorkHandoff, WorkFailed:
		return true
	default:
		return false
	}
}

func workStatusID(agentID, scope string) string {
	sum := sha256.Sum256([]byte(strings.Join([]string{agentID, scope}, "\x00")))
	return hex.EncodeToString(sum[:16])
}
