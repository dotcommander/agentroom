package agentroom

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newTestRoom(t *testing.T) (*Room, *miniredis.Miniredis) {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	t.Cleanup(mr.Close)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	cfg := Config{RepoID: "auth", BranchName: "main", StreamTTL: 48 * time.Hour}
	return NewRoom(rdb, cfg), mr
}

func TestPublishWritesEventAndTTL(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("requires redis (miniredis)")
	}
	room, mr := newTestRoom(t)
	ctx := context.Background()

	ev := &Event{Type: "AST_PARSED", AgentID: "engine-1", Payload: []byte(`{"file":"a.go"}`)}
	if err := room.Publish(ctx, ev); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if ev.ID == "" {
		t.Fatal("expected stream-assigned ID to be set")
	}
	if ev.Timestamp == 0 {
		t.Fatal("expected timestamp to be defaulted")
	}

	msgs, err := room.rdb.XRange(ctx, room.cfg.StreamKey(), "-", "+").Result()
	if err != nil {
		t.Fatalf("xrange: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("got %d msgs, want 1", len(msgs))
	}
	if got := msgs[0].Values["type"]; got != "AST_PARSED" {
		t.Errorf("type field = %v, want AST_PARSED", got)
	}
	if _, ok := msgs[0].Values["timestamp"]; !ok {
		t.Error("timestamp field missing from stream entry")
	}

	if ttl := mr.TTL(room.cfg.StreamKey()); ttl <= 0 {
		t.Errorf("expected positive TTL on stream, got %v", ttl)
	}
}

func TestScratchpadRoundTrip(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("requires redis (miniredis)")
	}
	room, _ := newTestRoom(t)
	ctx := context.Background()

	if err := room.WriteScratchpad(ctx, "plan", []byte("payload-bytes"), time.Minute); err != nil {
		t.Fatalf("write: %v", err)
	}
	got, err := room.ReadScratchpad(ctx, "plan")
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(got) != "payload-bytes" {
		t.Errorf("read = %q, want payload-bytes", got)
	}

	_, err = room.ReadScratchpad(ctx, "absent")
	if !errors.Is(err, redis.Nil) {
		t.Errorf("missing key error = %v, want redis.Nil", err)
	}
}

func TestRecentReturnsChronological(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("requires redis (miniredis)")
	}
	room, _ := newTestRoom(t)
	ctx := context.Background()
	for _, typ := range []string{"A", "B", "C"} {
		if err := room.Publish(ctx, &Event{Type: typ, AgentID: "p"}); err != nil {
			t.Fatalf("publish %s: %v", typ, err)
		}
	}
	events, err := room.Recent(ctx, 10)
	if err != nil {
		t.Fatalf("recent: %v", err)
	}
	if len(events) != 3 {
		t.Fatalf("recent = %d events, want 3", len(events))
	}
	if events[0].Type != "A" || events[2].Type != "C" {
		t.Errorf("order = [%s..%s], want A..C (chronological)", events[0].Type, events[2].Type)
	}
}

func TestPresenceHeartbeatExpiry(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("requires redis (miniredis)")
	}
	room, mr := newTestRoom(t)
	ctx := context.Background()
	const ttl = 60 * time.Second

	if err := room.Heartbeat(ctx, "agentA", "builder: refactor auth", ttl); err != nil {
		t.Fatalf("heartbeat agentA: %v", err)
	}
	if err := room.Heartbeat(ctx, "agentB", "", ttl); err != nil {
		t.Fatalf("heartbeat agentB: %v", err)
	}

	pres, err := room.Presence(ctx)
	if err != nil {
		t.Fatalf("presence: %v", err)
	}
	if len(pres) != 2 {
		t.Fatalf("presence size = %d, want 2", len(pres))
	}
	if pres["agentA"] != "builder: refactor auth" {
		t.Errorf("agentA desc = %q, want %q", pres["agentA"], "builder: refactor auth")
	}
	if _, ok := pres["agentB"]; !ok {
		t.Error("agentB missing from presence")
	}

	// Crash simulation: no SESSION_ENDED, just let the TTL lapse.
	mr.FastForward(ttl + time.Second)
	pres, err = room.Presence(ctx)
	if err != nil {
		t.Fatalf("presence after expiry: %v", err)
	}
	if len(pres) != 0 {
		t.Fatalf("presence after expiry = %d, want 0 (both keys should have expired)", len(pres))
	}

	// Clean-exit path: re-join then ClearPresence removes the key immediately.
	if err := room.Heartbeat(ctx, "agentA", "builder", ttl); err != nil {
		t.Fatalf("re-heartbeat agentA: %v", err)
	}
	if err := room.ClearPresence(ctx, "agentA"); err != nil {
		t.Fatalf("clear presence agentA: %v", err)
	}
	pres, err = room.Presence(ctx)
	if err != nil {
		t.Fatalf("presence after clear: %v", err)
	}
	if _, ok := pres["agentA"]; ok {
		t.Error("agentA still present after ClearPresence")
	}
}
