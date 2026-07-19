package agentroom

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

var requestWindowScript = redis.NewScript(`
local tp=redis.call('TIME'); local now=tonumber(tp[1])*1000+math.floor(tonumber(tp[2])/1000)
local resources=cjson.decode(ARGV[5]); local required=cjson.decode(ARGV[6]); local blockers={}
local function overlap(a,b) if a==b then return true end; if string.sub(a,1,5)=='path:' and string.sub(b,1,5)=='path:' then return string.sub(a,1,string.len(b)+1)==b..'/' or string.sub(b,1,string.len(a)+1)==a..'/' end; return false end
local function conflict(ares,bres) for _,a in ipairs(ares) do for _,b in ipairs(bres or {}) do if overlap(a,b) then return a,b end end end end
for _,id in ipairs(redis.call('ZRANGE',KEYS[2],0,-1)) do local raw=redis.call('GET',ARGV[8]..id); if raw then local w=cjson.decode(raw); local live=(w.state=='pending' and w.ack_deadline>now) or (w.state=='active' and w.expires_at>now); if live then local a,b=conflict(resources,w.resources);if a then return {'CONFLICT',a,b,w.owner or '',id} end end else redis.call('ZREM',KEYS[2],id) end end
local seen={}; for _,a in ipairs(required) do seen[a]=true end; seen[ARGV[2]]=true
for _,id in ipairs(redis.call('ZRANGE',KEYS[1],0,-1)) do local raw=redis.call('GET',ARGV[7]..id); if raw then local l=cjson.decode(raw); if l.expires_at>now then local a=conflict(resources,l.resources);if a then blockers[#blockers+1]=id;if l.owner and not seen[l.owner] then required[#required+1]=l.owner;seen[l.owner]=true end end end end end
table.sort(required); local state='pending'; local deadline=now+tonumber(ARGV[4]); local expires=deadline
if #required==1 and #blockers==0 then state='active'; expires=now+tonumber(ARGV[3]) end
local streamType=redis.call('TYPE',KEYS[3]).ok;if streamType~='none' and streamType~='stream' then return redis.error_reply('audit key is not a stream') end
local recipientIndexType=redis.call('TYPE',KEYS[4]).ok;if recipientIndexType~='none' and recipientIndexType~='zset' then return redis.error_reply('inbox recipient index is not a sorted set') end
for _,agent in ipairs(required) do if agent~=ARGV[2] then local inboxType=redis.call('TYPE',ARGV[11]..agent).ok;if inboxType~='none' and inboxType~='stream' then return redis.error_reply('inbox key is not a stream') end end end
local w={id=ARGV[1],owner=ARGV[2],purpose=ARGV[7+2],resources=resources,state=state,required=required,acknowledged={ARGV[2]},created_at=now,ack_deadline=deadline,expires_at=expires,ttl_millis=tonumber(ARGV[3])}
local keep=tonumber(ARGV[4])+tonumber(ARGV[10]); if state=='active' then keep=tonumber(ARGV[3])+tonumber(ARGV[10]) end
local raw=cjson.encode(w); redis.call('SET',ARGV[8]..ARGV[1],raw,'PX',keep); redis.call('DEL',ARGV[8]..ARGV[1]..':acks');redis.call('SADD',ARGV[8]..ARGV[1]..':acks',ARGV[2]);redis.call('PEXPIRE',ARGV[8]..ARGV[1]..':acks',keep);redis.call('ZADD',KEYS[2],expires,ARGV[1])
local payload=cjson.encode({schema_version=1,window=w})
local function addEvent(eventType,target)
  local eventID;if tonumber(ARGV[12])>0 then eventID=redis.call('XADD',KEYS[3],'MAXLEN','~',ARGV[12],'*','type',eventType,'agent_id',w.owner,'to',target,'reply_to','','payload',payload,'timestamp',now*1000000) else eventID=redis.call('XADD',KEYS[3],'*','type',eventType,'agent_id',w.owner,'to',target,'reply_to','','payload',payload,'timestamp',now*1000000) end
	  if target~='' then local inbox=ARGV[11]..target;redis.call('XADD',inbox,'*','source_stream',KEYS[3],'source_id',eventID,'type',eventType,'agent_id',w.owner,'to',target,'reply_to','','payload',payload,'timestamp',now*1000000);if tonumber(ARGV[14])>0 then redis.call('PEXPIRE',inbox,ARGV[14]) end end
	  if target~='' then local inboxExpiry=9223372036854775807;if tonumber(ARGV[14])>0 then inboxExpiry=now+tonumber(ARGV[14]) end;redis.call('ZREMRANGEBYSCORE',KEYS[4],'-inf',now);redis.call('ZADD',KEYS[4],inboxExpiry,target);if tonumber(ARGV[14])>0 then redis.call('PEXPIRE',KEYS[4],ARGV[14]) end end
end
addEvent('WINDOW_REQUESTED','');for _,agent in ipairs(required) do if agent~=w.owner then addEvent('WINDOW_ACK_REQUIRED',agent) end end
if tonumber(ARGV[13])>0 then redis.call('PEXPIRE',KEYS[3],ARGV[13]) end
return {'OK',raw}
`)

// requestWindowScript argument positions 9 and 10 are purpose and terminal retention.
// The unusual explicit positions keep the key prefixes adjacent for the Lua loops.

var acknowledgeWindowScript = redis.NewScript(`
local tp=redis.call('TIME'); local now=tonumber(tp[1])*1000+math.floor(tonumber(tp[2])/1000);local raw=redis.call('GET',ARGV[1]..ARGV[2]);if not raw then return {'MISSING'} end;local w=cjson.decode(raw);if w.state~='pending' then return {'STATE',w.state} end;if now>=w.ack_deadline then return {'STATE','expired'} end
local streamType=redis.call('TYPE',KEYS[1]).ok;if streamType~='none' and streamType~='stream' then return redis.error_reply('audit key is not a stream') end
local allowed=false;for _,id in ipairs(w.required or {}) do if id==ARGV[3] then allowed=true end end;if not allowed then return {'OWNER','required agent',ARGV[3]} end
redis.call('SADD',ARGV[1]..ARGV[2]..':acks',ARGV[3]); local acks=redis.call('SMEMBERS',ARGV[1]..ARGV[2]..':acks');table.sort(acks);w.acknowledged=acks;raw=cjson.encode(w);local keep=w.ack_deadline-now+tonumber(ARGV[4]);redis.call('SET',ARGV[1]..ARGV[2],raw,'PX',keep);redis.call('PEXPIRE',ARGV[1]..ARGV[2]..':acks',keep)
local payload=cjson.encode({schema_version=1,window=w});if tonumber(ARGV[5])>0 then redis.call('XADD',KEYS[1],'MAXLEN','~',ARGV[5],'*','type','WINDOW_ACKNOWLEDGED','agent_id',ARGV[3],'to','','reply_to','','payload',payload,'timestamp',now*1000000) else redis.call('XADD',KEYS[1],'*','type','WINDOW_ACKNOWLEDGED','agent_id',ARGV[3],'to','','reply_to','','payload',payload,'timestamp',now*1000000) end;if tonumber(ARGV[6])>0 then redis.call('PEXPIRE',KEYS[1],ARGV[6]) end;return {'OK',raw}
`)

var activateWindowScript = redis.NewScript(`
local tp=redis.call('TIME');local now=tonumber(tp[1])*1000+math.floor(tonumber(tp[2])/1000);local raw=redis.call('GET',ARGV[1]..ARGV[2]);if not raw then return {'MISSING'} end;local w=cjson.decode(raw);if w.owner~=ARGV[3] then return {'OWNER',w.owner or ''} end;if w.state~='pending' then return {'STATE',w.state} end;if now>=w.ack_deadline then return {'STATE','expired'} end
local streamType=redis.call('TYPE',KEYS[3]).ok;if streamType~='none' and streamType~='stream' then return redis.error_reply('audit key is not a stream') end
for _,id in ipairs(w.required or {}) do if redis.call('SISMEMBER',ARGV[1]..ARGV[2]..':acks',id)==0 then return {'ACK',id} end end
local function overlap(a,b) if a==b then return true end;if string.sub(a,1,5)=='path:' and string.sub(b,1,5)=='path:' then return string.sub(a,1,string.len(b)+1)==b..'/' or string.sub(b,1,string.len(a)+1)==a..'/' end;return false end
for _,id in ipairs(redis.call('ZRANGE',KEYS[1],0,-1)) do local lr=redis.call('GET',ARGV[4]..id);if lr then local l=cjson.decode(lr);if l.expires_at>now then for _,a in ipairs(w.resources) do for _,b in ipairs(l.resources) do if overlap(a,b) then return {'LEASE',a,b,l.owner or '',id} end end end end end end
w.state='active';w.expires_at=now+w.ttl_millis;raw=cjson.encode(w);redis.call('SET',ARGV[1]..ARGV[2],raw,'PX',w.ttl_millis+tonumber(ARGV[5]));redis.call('PEXPIRE',ARGV[1]..ARGV[2]..':acks',w.ttl_millis+tonumber(ARGV[5]));redis.call('ZADD',KEYS[2],w.expires_at,ARGV[2])
local payload=cjson.encode({schema_version=1,window=w});if tonumber(ARGV[6])>0 then redis.call('XADD',KEYS[3],'MAXLEN','~',ARGV[6],'*','type','WINDOW_ACTIVATED','agent_id',w.owner,'to','','reply_to','','payload',payload,'timestamp',now*1000000) else redis.call('XADD',KEYS[3],'*','type','WINDOW_ACTIVATED','agent_id',w.owner,'to','','reply_to','','payload',payload,'timestamp',now*1000000) end;if tonumber(ARGV[7])>0 then redis.call('PEXPIRE',KEYS[3],ARGV[7]) end;return {'OK',raw}
`)

var finishWindowScript = redis.NewScript(`
local tp=redis.call('TIME');local now=tonumber(tp[1])*1000+math.floor(tonumber(tp[2])/1000);local raw=redis.call('GET',ARGV[1]..ARGV[2]);if not raw then return {'MISSING'} end;local w=cjson.decode(raw);if w.owner~=ARGV[3] then return {'OWNER',w.owner or ''} end;if w.state~=ARGV[4] then return {'STATE',w.state} end
local streamType=redis.call('TYPE',KEYS[2]).ok;if streamType~='none' and streamType~='stream' then return redis.error_reply('audit key is not a stream') end
w.state=ARGV[5];w.expires_at=now;raw=cjson.encode(w);redis.call('SET',ARGV[1]..ARGV[2],raw,'PX',ARGV[6]);redis.call('PEXPIRE',ARGV[1]..ARGV[2]..':acks',ARGV[6]);redis.call('ZADD',KEYS[1],now+tonumber(ARGV[6]),ARGV[2])
local payload=cjson.encode({schema_version=1,window=w});if tonumber(ARGV[8])>0 then redis.call('XADD',KEYS[2],'MAXLEN','~',ARGV[8],'*','type',ARGV[7],'agent_id',w.owner,'to','','reply_to','','payload',payload,'timestamp',now*1000000) else redis.call('XADD',KEYS[2],'*','type',ARGV[7],'agent_id',w.owner,'to','','reply_to','','payload',payload,'timestamp',now*1000000) end;if tonumber(ARGV[9])>0 then redis.call('PEXPIRE',KEYS[2],ARGV[9]) end;return {'OK',raw}
`)

// RequestWindow atomically reserves resources and discovers owners of blocking leases.
func (r *Room) RequestWindow(ctx context.Context, req WindowRequest) (CoordinationWindow, error) {
	resources, err := normalizeResources(req.Resources)
	if err != nil {
		return CoordinationWindow{}, err
	}
	windowTTL := req.TTL
	if windowTTL <= 0 {
		windowTTL = defaultWindowTTL
	}
	ttl, err := coordinationTTL(windowTTL)
	if err != nil {
		return CoordinationWindow{}, err
	}
	if req.Owner == "" {
		return CoordinationWindow{}, errors.New("agentroom: window owner is required")
	}
	ack := req.AckTimeout
	if ack <= 0 {
		ack = 2 * time.Minute
	}
	if ack > maxResourceLeaseTTL {
		return CoordinationWindow{}, fmt.Errorf("agentroom: acknowledgment timeout exceeds maximum %s", maxResourceLeaseTTL)
	}
	required := append([]string(nil), req.Required...)
	required = append(required, req.Owner)
	slices.Sort(required)
	required = slices.Compact(required)
	id, err := randomCoordinationID()
	if err != nil {
		return CoordinationWindow{}, err
	}
	resJSON, _ := json.Marshal(resources)
	reqJSON, _ := json.Marshal(required)
	keys := []string{r.cfg.ResourceLeaseIndexKey(), r.cfg.CoordinationWindowIndexKey(), r.cfg.StreamKey(), r.cfg.InboxRecipientIndexKey()}
	result, err := requestWindowScript.Run(ctx, r.rdb, keys, id, req.Owner, ttl.Milliseconds(), ack.Milliseconds(), string(resJSON), string(reqJSON), r.cfg.ResourceLeasePrefix(), r.cfg.CoordinationWindowPrefix(), req.Purpose, terminalWindowTTL.Milliseconds(), r.cfg.InboxKey(""), r.cfg.StreamMaxLen, r.cfg.StreamTTL.Milliseconds(), r.cfg.InboxTTL.Milliseconds()).Slice()
	if err != nil {
		return CoordinationWindow{}, fmt.Errorf("agentroom: request window: %w", err)
	}
	if stringValue(result[0]) == coordinationResultConflict {
		return CoordinationWindow{}, &LeaseConflictError{Resource: stringValue(result[1]), Conflicts: stringValue(result[2]), Owner: stringValue(result[3]), WindowID: stringValue(result[4])}
	}
	w, err := decodeWindowResult(result)
	if err != nil {
		return CoordinationWindow{}, err
	}
	return w, nil
}

// AcknowledgeWindow records an authorized acknowledgment idempotently.
func (r *Room) AcknowledgeWindow(ctx context.Context, id, agentID string) (CoordinationWindow, error) {
	result, err := acknowledgeWindowScript.Run(ctx, r.rdb, []string{r.cfg.StreamKey()}, r.cfg.CoordinationWindowPrefix(), id, agentID, terminalWindowTTL.Milliseconds(), r.cfg.StreamMaxLen, r.cfg.StreamTTL.Milliseconds()).Slice()
	if err != nil {
		return CoordinationWindow{}, fmt.Errorf("agentroom: acknowledge window: %w", err)
	}
	if err := windowResultError(result, "acknowledge", agentID, id); err != nil {
		return CoordinationWindow{}, err
	}
	return decodeWindowResult(result)
}

// ActivateWindow atomically verifies acknowledgments and released blocker leases.
func (r *Room) ActivateWindow(ctx context.Context, id, owner string) (CoordinationWindow, error) {
	keys := []string{r.cfg.ResourceLeaseIndexKey(), r.cfg.CoordinationWindowIndexKey(), r.cfg.StreamKey()}
	result, err := activateWindowScript.Run(ctx, r.rdb, keys, r.cfg.CoordinationWindowPrefix(), id, owner, r.cfg.ResourceLeasePrefix(), terminalWindowTTL.Milliseconds(), r.cfg.StreamMaxLen, r.cfg.StreamTTL.Milliseconds()).Slice()
	if err != nil {
		return CoordinationWindow{}, fmt.Errorf("agentroom: activate window: %w", err)
	}
	switch stringValue(result[0]) {
	case "ACK":
		return CoordinationWindow{}, fmt.Errorf("%w: %s", ErrAcknowledgmentDue, stringValue(result[1]))
	case "LEASE":
		return CoordinationWindow{}, &LeaseConflictError{Resource: stringValue(result[1]), Conflicts: stringValue(result[2]), Owner: stringValue(result[3]), LeaseID: stringValue(result[4])}
	}
	if err := windowResultError(result, "activate", owner, id); err != nil {
		return CoordinationWindow{}, err
	}
	return decodeWindowResult(result)
}

// ReleaseWindow releases an active window and retains its terminal record.
func (r *Room) ReleaseWindow(ctx context.Context, id, owner string) error {
	return r.finishWindow(ctx, id, owner, windowTransition{from: windowStateActive, to: "released", eventType: "WINDOW_RELEASED"})
}

// CancelWindow cancels a pending window and retains its terminal record.
func (r *Room) CancelWindow(ctx context.Context, id, owner string) error {
	return r.finishWindow(ctx, id, owner, windowTransition{from: windowStatePending, to: "cancelled", eventType: "WINDOW_CANCELLED"})
}

type windowTransition struct{ from, to, eventType string }

func (r *Room) finishWindow(ctx context.Context, id, owner string, transition windowTransition) error {
	keys := []string{r.cfg.CoordinationWindowIndexKey(), r.cfg.StreamKey()}
	result, err := finishWindowScript.Run(ctx, r.rdb, keys, r.cfg.CoordinationWindowPrefix(), id, owner, transition.from, transition.to, terminalWindowTTL.Milliseconds(), transition.eventType, r.cfg.StreamMaxLen, r.cfg.StreamTTL.Milliseconds()).Slice()
	if err != nil {
		return fmt.Errorf("agentroom: finish window: %w", err)
	}
	if err := windowResultError(result, transition.to, owner, id); err != nil {
		return err
	}
	_, err = decodeWindowResult(result)
	return err
}

// Windows returns pending, active, and retained terminal records.
func (r *Room) Windows(ctx context.Context) ([]CoordinationWindow, error) {
	ids, err := r.rdb.ZRange(ctx, r.cfg.CoordinationWindowIndexKey(), 0, -1).Result()
	if err != nil {
		return nil, fmt.Errorf("agentroom: list window index: %w", err)
	}
	out := make([]CoordinationWindow, 0, len(ids))
	serverNow, err := r.rdb.Time(ctx).Result()
	if err != nil {
		return nil, fmt.Errorf("agentroom: read Redis time: %w", err)
	}
	now := serverNow.UnixMilli()
	for _, id := range ids {
		raw, err := r.rdb.Get(ctx, r.cfg.CoordinationWindowKey(id)).Bytes()
		if errors.Is(err, redis.Nil) {
			_ = r.rdb.ZRem(ctx, r.cfg.CoordinationWindowIndexKey(), id).Err()
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("agentroom: read window %s: %w", id, err)
		}
		var w CoordinationWindow
		if err := json.Unmarshal(raw, &w); err != nil {
			return nil, fmt.Errorf("agentroom: decode window %s: %w", id, err)
		}
		acks, _ := r.rdb.SMembers(ctx, r.cfg.CoordinationWindowAcksKey(id)).Result()
		slices.Sort(acks)
		w.Acknowledged = acks
		if w.State == "pending" && w.AckDeadline <= now {
			w.State = windowStateExpired
		}
		if w.State == windowStateActive && w.ExpiresAt <= now {
			w.State = windowStateExpired
		}
		out = append(out, w)
	}
	slices.SortFunc(out, func(a, b CoordinationWindow) int { return strings.Compare(a.ID, b.ID) })
	return out, nil
}

func decodeWindowResult(result []any) (CoordinationWindow, error) {
	if len(result) < 2 {
		return CoordinationWindow{}, errors.New("agentroom: malformed window result")
	}
	var w CoordinationWindow
	if err := json.Unmarshal([]byte(stringValue(result[1])), &w); err != nil {
		return CoordinationWindow{}, fmt.Errorf("agentroom: decode window: %w", err)
	}
	return w, nil
}
func windowResultError(result []any, operation, actual, id string) error {
	switch stringValue(result[0]) {
	case "OK":
		return nil
	case "MISSING":
		return &ExpiryError{Kind: "window", ID: id}
	case coordinationResultOwner:
		return &OwnershipError{Expected: stringValue(result[1]), Actual: actual}
	case "STATE":
		if stringValue(result[1]) == windowStateExpired {
			return &ExpiryError{Kind: "window", ID: id}
		}
		return &StateError{State: stringValue(result[1]), Operation: operation}
	}
	return fmt.Errorf("agentroom: unexpected window result %q", stringValue(result[0]))
}
