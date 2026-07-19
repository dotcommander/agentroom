package agentroom

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"slices"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	coordinationResultConflict = "CONFLICT"
	coordinationResultOwner    = "OWNER"
	windowStateActive          = "active"
	windowStateExpired         = "expired"
	windowStatePending         = "pending"
)

var acquireResourcesScript = redis.NewScript(`
local nowp = redis.call('TIME')
local now = tonumber(nowp[1]) * 1000 + math.floor(tonumber(nowp[2]) / 1000)
local streamType=redis.call('TYPE',KEYS[3]).ok;if streamType~='none' and streamType~='stream' then return redis.error_reply('audit key is not a stream') end
local requested = cjson.decode(ARGV[5])
local function overlap(a, b)
  if a == b then return true end
  if string.sub(a, 1, 5) == 'path:' and string.sub(b, 1, 5) == 'path:' then
    return string.sub(a, 1, string.len(b) + 1) == b .. '/' or string.sub(b, 1, string.len(a) + 1) == a .. '/'
  end
  return false
end
local function conflict(resources)
  for _, a in ipairs(requested) do
    for _, b in ipairs(resources or {}) do if overlap(a, b) then return a, b end end
  end
end
local expired = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now)
for _, id in ipairs(expired) do redis.call('DEL', ARGV[6] .. id); redis.call('ZREM', KEYS[1], id) end
for _, id in ipairs(redis.call('ZRANGE', KEYS[1], 0, -1)) do
  local raw = redis.call('GET', ARGV[6] .. id)
  if raw then
    local lease = cjson.decode(raw); local a,b = conflict(lease.resources)
    if a then return {'CONFLICT', a, b, lease.owner or '', id, ''} end
  else redis.call('ZREM', KEYS[1], id) end
end
for _, id in ipairs(redis.call('ZRANGE', KEYS[2], 0, -1)) do
  local raw = redis.call('GET', ARGV[7] .. id)
  if raw then
    local w = cjson.decode(raw)
    if (w.state == 'pending' and w.ack_deadline > now) or (w.state == 'active' and w.expires_at > now) then
      local a,b = conflict(w.resources)
      if a then return {'CONFLICT', a, b, w.owner or '', '', id} end
    end
  else redis.call('ZREM', KEYS[2], id)
  end
end
local lease = {id=ARGV[1], owner=ARGV[2], purpose=ARGV[3], resources=requested, created_at=now, expires_at=now+tonumber(ARGV[4])}
local raw = cjson.encode(lease)
redis.call('SET', ARGV[6] .. ARGV[1], raw, 'PX', tonumber(ARGV[4]))
redis.call('ZADD', KEYS[1], lease.expires_at, ARGV[1])
local payload=cjson.encode({schema_version=1,lease=lease})
if tonumber(ARGV[8])>0 then redis.call('XADD',KEYS[3],'MAXLEN','~',ARGV[8],'*','type','LEASE_ACQUIRED','agent_id',lease.owner,'to','','reply_to','','payload',payload,'timestamp',now*1000000) else redis.call('XADD',KEYS[3],'*','type','LEASE_ACQUIRED','agent_id',lease.owner,'to','','reply_to','','payload',payload,'timestamp',now*1000000) end
if tonumber(ARGV[9])>0 then redis.call('PEXPIRE',KEYS[3],ARGV[9]) end
return {'OK', raw}
`)

var renewResourcesScript = redis.NewScript(`
local nowp = redis.call('TIME'); local now = tonumber(nowp[1])*1000 + math.floor(tonumber(nowp[2])/1000)
local streamType=redis.call('TYPE',KEYS[3]).ok;if streamType~='none' and streamType~='stream' then return redis.error_reply('audit key is not a stream') end
local raw = redis.call('GET', ARGV[1] .. ARGV[2]); if not raw then return {'MISSING'} end
local lease = cjson.decode(raw); if lease.owner ~= ARGV[3] then return {'OWNER', lease.owner or ''} end
local function overlap(a,b)
  if a == b then return true end
  if string.sub(a,1,5)=='path:' and string.sub(b,1,5)=='path:' then return string.sub(a,1,string.len(b)+1)==b..'/' or string.sub(b,1,string.len(a)+1)==a..'/' end
  return false
end
for _, wid in ipairs(redis.call('ZRANGE', KEYS[2], 0, -1)) do
  local wr = redis.call('GET', ARGV[4] .. wid)
  if wr then local w=cjson.decode(wr); if w.state=='pending' and w.ack_deadline>now then
    for _,a in ipairs(lease.resources) do for _,b in ipairs(w.resources) do if overlap(a,b) then return {'WINDOW',a,b,w.owner or '',wid} end end end
  end else redis.call('ZREM', KEYS[2], wid) end
end
lease.expires_at=now+tonumber(ARGV[5]); raw=cjson.encode(lease)
redis.call('SET',ARGV[1]..ARGV[2],raw,'PX',tonumber(ARGV[5])); redis.call('ZADD',KEYS[1],lease.expires_at,ARGV[2])
local payload=cjson.encode({schema_version=1,lease=lease})
if tonumber(ARGV[6])>0 then redis.call('XADD',KEYS[3],'MAXLEN','~',ARGV[6],'*','type','LEASE_RENEWED','agent_id',lease.owner,'to','','reply_to','','payload',payload,'timestamp',now*1000000) else redis.call('XADD',KEYS[3],'*','type','LEASE_RENEWED','agent_id',lease.owner,'to','','reply_to','','payload',payload,'timestamp',now*1000000) end
if tonumber(ARGV[7])>0 then redis.call('PEXPIRE',KEYS[3],ARGV[7]) end
return {'OK',raw}
`)

var releaseResourcesScript = redis.NewScript(`
local streamType=redis.call('TYPE',KEYS[2]).ok;if streamType~='none' and streamType~='stream' then return redis.error_reply('audit key is not a stream') end
local raw=redis.call('GET',ARGV[1]..ARGV[2]); if not raw then redis.call('ZREM',KEYS[1],ARGV[2]); return {'OK'} end
local lease=cjson.decode(raw); if lease.owner~=ARGV[3] then return {'OWNER',lease.owner or ''} end
local tp=redis.call('TIME');local now=tonumber(tp[1])*1000+math.floor(tonumber(tp[2])/1000)
redis.call('DEL',ARGV[1]..ARGV[2]); redis.call('ZREM',KEYS[1],ARGV[2])
local payload=cjson.encode({schema_version=1,lease=lease})
if tonumber(ARGV[4])>0 then redis.call('XADD',KEYS[2],'MAXLEN','~',ARGV[4],'*','type','LEASE_RELEASED','agent_id',lease.owner,'to','','reply_to','','payload',payload,'timestamp',now*1000000) else redis.call('XADD',KEYS[2],'*','type','LEASE_RELEASED','agent_id',lease.owner,'to','','reply_to','','payload',payload,'timestamp',now*1000000) end
if tonumber(ARGV[5])>0 then redis.call('PEXPIRE',KEYS[2],ARGV[5]) end
return {'OK',raw}
`)

var guardResourcesScript = redis.NewScript(`
local tp=redis.call('TIME');local now=tonumber(tp[1])*1000+math.floor(tonumber(tp[2])/1000)
local requested=cjson.decode(ARGV[1]);local held={}
local function overlap(a,b) if a==b then return true end;if string.sub(a,1,5)=='path:' and string.sub(b,1,5)=='path:' then return string.sub(a,1,string.len(b)+1)==b..'/' or string.sub(b,1,string.len(a)+1)==a..'/' end;return false end
local function covers(heldResource,requestedResource) if heldResource==requestedResource then return true end;if string.sub(heldResource,1,5)=='path:' and string.sub(requestedResource,1,5)=='path:' then return string.sub(requestedResource,1,string.len(heldResource)+1)==heldResource..'/' end;return false end
for _,id in ipairs(redis.call('ZRANGEBYSCORE',KEYS[1],'-inf',now)) do redis.call('DEL',ARGV[4]..id);redis.call('ZREM',KEYS[1],id) end
for _,id in ipairs(redis.call('ZRANGE',KEYS[1],0,-1)) do local raw=redis.call('GET',ARGV[4]..id);if raw then local lease=cjson.decode(raw);for _,a in ipairs(requested) do for _,b in ipairs(lease.resources or {}) do if overlap(a,b) then if id==ARGV[3] and lease.owner==ARGV[2] and covers(b,a) then held[a]=true elseif id==ARGV[3] and lease.owner==ARGV[2] then return {'NEEDED'} else return {'CONFLICT',a,b,lease.owner or '',id,''} end end end end else redis.call('ZREM',KEYS[1],id) end end
for _,id in ipairs(redis.call('ZRANGE',KEYS[2],0,-1)) do local raw=redis.call('GET',ARGV[5]..id);if raw then local w=cjson.decode(raw);local live=(w.state=='pending' and w.ack_deadline>now) or (w.state=='active' and w.expires_at>now);if live then for _,a in ipairs(requested) do for _,b in ipairs(w.resources or {}) do if overlap(a,b) then return {'CONFLICT',a,b,w.owner or '','',id} end end end end else redis.call('ZREM',KEYS[2],id) end end
if ARGV[6]=='1' then for _,a in ipairs(requested) do if not held[a] then return {'NEEDED'} end end end
return {'OK'}
`)

func normalizeResources(resources []string) ([]string, error) {
	if len(resources) == 0 {
		return nil, errors.New("agentroom: at least one resource is required")
	}
	out := make([]string, 0, len(resources))
	seen := make(map[string]struct{}, len(resources))
	for _, resource := range resources {
		if err := validateResource(resource); err != nil {
			return nil, err
		}
		if _, ok := seen[resource]; !ok {
			seen[resource] = struct{}{}
			out = append(out, resource)
		}
	}
	slices.Sort(out)
	return out, nil
}

func validateResource(resource string) error {
	kind, name, ok := strings.Cut(resource, ":")
	if !ok || kind == "" || name == "" {
		return fmt.Errorf("agentroom: invalid resource %q", resource)
	}
	if kind != "path" {
		return nil
	}
	if strings.HasPrefix(name, "/") || path.Clean(name) != name || name == "." || strings.ContainsAny(name, "*?[]{}") {
		return fmt.Errorf("agentroom: invalid path resource %q", resource)
	}
	for segment := range strings.SplitSeq(name, "/") {
		if segment == "" || segment == ".." {
			return fmt.Errorf("agentroom: invalid path resource %q", resource)
		}
	}
	return nil
}

func coordinationTTL(ttl time.Duration) (time.Duration, error) {
	if ttl <= 0 {
		ttl = defaultResourceLeaseTTL
	}
	if ttl > maxResourceLeaseTTL {
		return 0, fmt.Errorf("agentroom: ttl %s exceeds maximum %s", ttl, maxResourceLeaseTTL)
	}
	return ttl, nil
}

func randomCoordinationID() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("agentroom: generate coordination id: %w", err)
	}
	return hex.EncodeToString(raw[:]), nil
}

// AcquireResources atomically acquires every requested resource or none.
func (r *Room) AcquireResources(ctx context.Context, req ResourceLeaseRequest) (ResourceLease, error) {
	resources, err := normalizeResources(req.Resources)
	if err != nil {
		return ResourceLease{}, err
	}
	ttl, err := coordinationTTL(req.TTL)
	if err != nil {
		return ResourceLease{}, err
	}
	if req.Owner == "" {
		return ResourceLease{}, errors.New("agentroom: lease owner is required")
	}
	id, err := randomCoordinationID()
	if err != nil {
		return ResourceLease{}, err
	}
	encoded, _ := json.Marshal(resources)
	keys := []string{r.cfg.ResourceLeaseIndexKey(), r.cfg.CoordinationWindowIndexKey(), r.cfg.StreamKey()}
	result, err := acquireResourcesScript.Run(ctx, r.rdb, keys, id, req.Owner, req.Purpose, ttl.Milliseconds(), string(encoded), r.cfg.ResourceLeasePrefix(), r.cfg.CoordinationWindowPrefix(), r.cfg.StreamMaxLen, r.cfg.StreamTTL.Milliseconds()).Slice()
	if err != nil {
		return ResourceLease{}, fmt.Errorf("agentroom: acquire resources: %w", err)
	}
	if stringValue(result[0]) == coordinationResultConflict {
		return ResourceLease{}, &LeaseConflictError{Resource: stringValue(result[1]), Conflicts: stringValue(result[2]), Owner: stringValue(result[3]), LeaseID: stringValue(result[4]), WindowID: stringValue(result[5])}
	}
	var lease ResourceLease
	if err := json.Unmarshal([]byte(stringValue(result[1])), &lease); err != nil {
		return ResourceLease{}, fmt.Errorf("agentroom: decode acquired lease: %w", err)
	}
	return lease, nil
}

// RenewResources extends a lease using Redis server time.
func (r *Room) RenewResources(ctx context.Context, id, owner string, ttl time.Duration) (ResourceLease, error) {
	ttl, err := coordinationTTL(ttl)
	if err != nil {
		return ResourceLease{}, err
	}
	keys := []string{r.cfg.ResourceLeaseIndexKey(), r.cfg.CoordinationWindowIndexKey(), r.cfg.StreamKey()}
	result, err := renewResourcesScript.Run(ctx, r.rdb, keys, r.cfg.ResourceLeasePrefix(), id, owner, r.cfg.CoordinationWindowPrefix(), ttl.Milliseconds(), r.cfg.StreamMaxLen, r.cfg.StreamTTL.Milliseconds()).Slice()
	if err != nil {
		return ResourceLease{}, fmt.Errorf("agentroom: renew resources: %w", err)
	}
	switch stringValue(result[0]) {
	case "MISSING":
		return ResourceLease{}, &ExpiryError{Kind: "lease", ID: id}
	case coordinationResultOwner:
		return ResourceLease{}, &OwnershipError{Expected: stringValue(result[1]), Actual: owner}
	case "WINDOW":
		return ResourceLease{}, &LeaseConflictError{Resource: stringValue(result[1]), Conflicts: stringValue(result[2]), Owner: stringValue(result[3]), WindowID: stringValue(result[4])}
	}
	var lease ResourceLease
	if err := json.Unmarshal([]byte(stringValue(result[1])), &lease); err != nil {
		return ResourceLease{}, fmt.Errorf("agentroom: decode renewed lease: %w", err)
	}
	return lease, nil
}

// ReleaseResources releases a lease owned by owner. Missing leases are idempotent.
func (r *Room) ReleaseResources(ctx context.Context, id, owner string) error {
	keys := []string{r.cfg.ResourceLeaseIndexKey(), r.cfg.StreamKey()}
	result, err := releaseResourcesScript.Run(ctx, r.rdb, keys, r.cfg.ResourceLeasePrefix(), id, owner, r.cfg.StreamMaxLen, r.cfg.StreamTTL.Milliseconds()).Slice()
	if err != nil {
		return fmt.Errorf("agentroom: release resources: %w", err)
	}
	if stringValue(result[0]) == coordinationResultOwner {
		return &OwnershipError{Expected: stringValue(result[1]), Actual: owner}
	}
	return nil
}

// ResourceLeases returns unexpired durable leases independently of presence.
func (r *Room) ResourceLeases(ctx context.Context) ([]ResourceLease, error) {
	query := indexedRecordQuery{indexKey: r.cfg.ResourceLeaseIndexKey(), keyFor: r.cfg.ResourceLeaseKey, label: "lease"}
	leases, err := indexedRecords[ResourceLease](ctx, r.rdb, query)
	if err != nil {
		return nil, err
	}
	slices.SortFunc(leases, func(a, b ResourceLease) int { return strings.Compare(a.ID, b.ID) })
	return leases, nil
}

// GuardResources reports conflicts, and optionally requires ownership by leaseID.
func (r *Room) GuardResources(ctx context.Context, resources []string, owner, leaseID string, requireHeld bool) error {
	normalized, err := normalizeResources(resources)
	if err != nil {
		return err
	}
	raw, _ := json.Marshal(normalized)
	required := "0"
	if requireHeld {
		required = "1"
	}
	result, err := guardResourcesScript.Run(ctx, r.rdb, []string{r.cfg.ResourceLeaseIndexKey(), r.cfg.CoordinationWindowIndexKey()}, raw, owner, leaseID, r.cfg.ResourceLeasePrefix(), r.cfg.CoordinationWindowPrefix(), required).Slice()
	if err != nil {
		return fmt.Errorf("agentroom: guard resources: %w", err)
	}
	switch stringValue(result[0]) {
	case "OK":
		return nil
	case "NEEDED":
		return ErrResourceLeaseNeeded
	case coordinationResultConflict:
		return &LeaseConflictError{Resource: stringValue(result[1]), Conflicts: stringValue(result[2]), Owner: stringValue(result[3]), LeaseID: stringValue(result[4]), WindowID: stringValue(result[5])}
	default:
		return fmt.Errorf("agentroom: unexpected guard result %q", stringValue(result[0]))
	}
}

func stringValue(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case []byte:
		return string(x)
	default:
		return fmt.Sprint(v)
	}
}
