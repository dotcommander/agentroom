package agentroom

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestHeartbeatWritesPresenceKey(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("requires redis (miniredis)")
	}
	room, mr := newTestRoom(t)
	if err := room.Heartbeat(context.Background(), "agent-1", "role=fixer", time.Minute); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	got, err := mr.Get(room.cfg.PresenceKey("agent-1"))
	if err != nil {
		t.Fatalf("get presence key: %v", err)
	}
	if got != "role=fixer" {
		t.Fatalf("presence desc = %q, want %q", got, "role=fixer")
	}
}

func TestHeartbeatRefreshesTTL(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("requires redis (miniredis)")
	}
	room, mr := newTestRoom(t)
	key := room.cfg.PresenceKey("agent-1")
	ctx := context.Background()
	if err := room.Heartbeat(ctx, "agent-1", "x", time.Minute); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	mr.FastForward(30 * time.Second)
	if err := room.Heartbeat(ctx, "agent-1", "x", time.Minute); err != nil {
		t.Fatalf("heartbeat refresh: %v", err)
	}
	if ttl := mr.TTL(key); ttl <= 30*time.Second {
		t.Fatalf("ttl after refresh = %v, want > 30s", ttl)
	}
}

func TestPresenceListsLiveAgentsAndExpires(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("requires redis (miniredis)")
	}
	room, mr := newTestRoom(t)
	ctx := context.Background()
	if err := room.Heartbeat(ctx, "long-lived", "a", time.Minute); err != nil {
		t.Fatalf("heartbeat long: %v", err)
	}
	if err := room.Heartbeat(ctx, "short-lived", "b", 10*time.Second); err != nil {
		t.Fatalf("heartbeat short: %v", err)
	}
	live, err := room.Presence(ctx)
	if err != nil {
		t.Fatalf("presence: %v", err)
	}
	if len(live) != 2 {
		t.Fatalf("presence count = %d, want 2 (%v)", len(live), live)
	}
	mr.FastForward(11 * time.Second)
	live, err = room.Presence(ctx)
	if err != nil {
		t.Fatalf("presence after expiry: %v", err)
	}
	if _, ok := live["short-lived"]; ok {
		t.Fatalf("short-lived agent should have expired: %v", live)
	}
	if live["long-lived"] != "a" {
		t.Fatalf("long-lived desc = %q, want %q", live["long-lived"], "a")
	}
}

func TestPresenceEmptyRoom(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("requires redis (miniredis)")
	}
	room, _ := newTestRoom(t)
	live, err := room.Presence(context.Background())
	if err != nil {
		t.Fatalf("presence: %v", err)
	}
	if len(live) != 0 {
		t.Fatalf("presence on empty room = %v, want empty", live)
	}
}

func TestClearPresenceRemovesAgent(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("requires redis (miniredis)")
	}
	room, _ := newTestRoom(t)
	ctx := context.Background()
	if err := room.Heartbeat(ctx, "agent-1", "x", time.Minute); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	if err := room.ClearPresence(ctx, "agent-1"); err != nil {
		t.Fatalf("clear presence: %v", err)
	}
	live, err := room.Presence(ctx)
	if err != nil {
		t.Fatalf("presence: %v", err)
	}
	if _, ok := live["agent-1"]; ok {
		t.Fatalf("agent-1 should be cleared: %v", live)
	}
}

func TestRefreshPresencePreservesDesc(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("requires redis (miniredis)")
	}
	room, mr := newTestRoom(t)
	ctx := context.Background()
	key := room.cfg.PresenceKey("a1")
	if err := room.Heartbeat(ctx, "a1", "role=x", time.Minute); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	mr.FastForward(30 * time.Second)
	if err := room.RefreshPresence(ctx, "a1", time.Minute); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	got, err := mr.Get(key)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got != "role=x" {
		t.Fatalf("desc after refresh = %q, want %q", got, "role=x")
	}
	if ttl := mr.TTL(key); ttl <= 30*time.Second {
		t.Fatalf("ttl after refresh = %v, want > 30s", ttl)
	}
}

func TestPresenceErrorsOnOutage(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("requires redis (miniredis)")
	}
	room, mr := newTestRoom(t)
	ctx := context.Background()

	// Seed a presence record.
	if err := room.Heartbeat(ctx, "agent-1", "role=fixer", time.Minute); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}

	// Force the next Redis command to error.
	mr.SetError("forced error")

	_, err := room.Presence(ctx)
	if err == nil {
		t.Fatal("Presence() should return error during redis outage, got nil")
	}
}

func TestRefreshPresenceCreatesEmptyWhenAbsent(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("requires redis (miniredis)")
	}
	room, _ := newTestRoom(t)
	ctx := context.Background()
	if err := room.RefreshPresence(ctx, "ghost", time.Minute); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	live, err := room.Presence(ctx)
	if err != nil {
		t.Fatalf("presence: %v", err)
	}
	desc, ok := live["ghost"]
	if !ok {
		t.Fatalf("ghost not registered: %v", live)
	}
	if desc != "" {
		t.Fatalf("ghost desc = %q, want empty", desc)
	}
}

func TestPresenceDetailedIncludesDescAndTTL(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("requires redis (miniredis)")
	}
	room, _ := newTestRoom(t)
	ctx := context.Background()
	if err := room.Heartbeat(ctx, "agent-1", "role=fixer", time.Minute); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	det, err := room.PresenceDetailed(ctx)
	if err != nil {
		t.Fatalf("presence detailed: %v", err)
	}
	e, ok := det["agent-1"]
	if !ok {
		t.Fatalf("agent-1 absent from detailed presence: %v", det)
	}
	if e.Desc != "role=fixer" {
		t.Fatalf("desc = %q, want %q", e.Desc, "role=fixer")
	}
	if e.TTL <= 0 || e.TTL > time.Minute {
		t.Fatalf("ttl = %v, want in (0, 1m]", e.TTL)
	}
}

func TestPresenceErrorsOnGetFailure(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("requires redis (miniredis)")
	}
	room, mr := newTestRoom(t)
	ctx := context.Background()

	// Poison a key under the presence prefix with the WRONG type (a hash):
	// SCAN still returns it, but GET returns a non-Nil WRONGTYPE error, which
	// isolates the GET-error branch (not the SCAN/iter.Err path).
	mr.HSet(room.cfg.PresenceKey("poison"), "field", "value")

	_, err := room.Presence(ctx)
	if err == nil {
		t.Fatal("Presence() should error when GET on a presence key fails, got nil")
	}
	if !strings.Contains(err.Error(), "read presence") {
		t.Fatalf("expected GET-branch error (read presence), got: %v", err)
	}
}
