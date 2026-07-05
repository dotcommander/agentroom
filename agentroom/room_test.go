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

func TestPublishBoundsStreamLength(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("requires redis (miniredis)")
	}
	room, _ := newTestRoom(t)
	room.cfg.StreamMaxLen = 5
	ctx := context.Background()

	for i := range 50 {
		if err := room.Publish(ctx, &Event{Type: "EVENT", AgentID: "engine-1"}); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}

	length, err := room.rdb.XLen(ctx, room.cfg.StreamKey()).Result()
	if err != nil {
		t.Fatalf("xlen: %v", err)
	}
	if length != room.cfg.StreamMaxLen {
		t.Fatalf("stream length = %d, want %d", length, room.cfg.StreamMaxLen)
	}
}

func TestPublishRejectsOversizedPayload(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("requires redis (miniredis)")
	}
	room, _ := newTestRoom(t)
	room.cfg.MaxPayloadBytes = 4
	ctx := context.Background()

	err := room.Publish(ctx, &Event{Type: "BIG", AgentID: "engine-1", Payload: []byte("12345")})
	if err == nil {
		t.Fatal("publish succeeded, want oversized payload error")
	}

	length, xerr := room.rdb.XLen(ctx, room.cfg.StreamKey()).Result()
	if xerr != nil {
		t.Fatalf("xlen: %v", xerr)
	}
	if length != 0 {
		t.Fatalf("stream length = %d, want 0", length)
	}
}

func TestInboxRoundTripAndCursor(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("requires redis (miniredis)")
	}
	room, _ := newTestRoom(t)
	ctx := context.Background()
	ev := &Event{Type: "MSG", AgentID: "alice", To: "gary", Payload: []byte(`{"m":1}`)}
	if err := room.Publish(ctx, ev); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if err := room.EnqueueInbox(ctx, "gary", *ev); err != nil {
		t.Fatalf("enqueue inbox: %v", err)
	}

	got, err := room.InboxSince(ctx, "gary", "", 10)
	if err != nil {
		t.Fatalf("inbox since: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("inbox count = %d, want 1", len(got))
	}
	if got[0].SourceID != ev.ID {
		t.Fatalf("SourceID = %q, want %q", got[0].SourceID, ev.ID)
	}
	if got[0].Event.Type != "MSG" || string(got[0].Event.Payload) != `{"m":1}` {
		t.Fatalf("inbox event = %+v", got[0].Event)
	}

	if err := room.WriteInboxCursor(ctx, "gary", got[0].ID, time.Hour); err != nil {
		t.Fatalf("write inbox cursor: %v", err)
	}
	cursor, err := room.ReadInboxCursor(ctx, "gary")
	if err != nil {
		t.Fatalf("read inbox cursor: %v", err)
	}
	if cursor != got[0].ID {
		t.Fatalf("cursor = %q, want %q", cursor, got[0].ID)
	}
	again, err := room.InboxSince(ctx, "gary", cursor, 10)
	if err != nil {
		t.Fatalf("inbox since cursor: %v", err)
	}
	if len(again) != 0 {
		t.Fatalf("inbox after cursor = %d, want 0", len(again))
	}
}

func TestEventByIDReturnsExactEvent(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("requires redis (miniredis)")
	}
	room, _ := newTestRoom(t)
	ctx := context.Background()

	first := &Event{Type: "ASK", AgentID: "asker", To: "reviewer", Payload: []byte("question")}
	if err := room.Publish(ctx, first); err != nil {
		t.Fatalf("publish first: %v", err)
	}
	if err := room.Publish(ctx, &Event{Type: "NOISE", AgentID: "other"}); err != nil {
		t.Fatalf("publish noise: %v", err)
	}

	got, ok, err := room.EventByID(ctx, first.ID)
	if err != nil {
		t.Fatalf("event by id: %v", err)
	}
	if !ok {
		t.Fatal("EventByID returned ok=false for published event")
	}
	if got.ID != first.ID || got.Type != "ASK" || got.AgentID != "asker" || got.To != "reviewer" || string(got.Payload) != "question" {
		t.Fatalf("EventByID = %+v, want first event", got)
	}

	_, ok, err = room.EventByID(ctx, "9999999999999-0")
	if err != nil {
		t.Fatalf("event by missing id: %v", err)
	}
	if ok {
		t.Fatal("EventByID returned ok=true for missing event")
	}
}

func TestBeginAskLeaseRejectsConcurrentAndEndAskIsTokenSafe(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("requires redis (miniredis)")
	}
	room, mr := newTestRoom(t)
	ctx := context.Background()
	const agent = "asker-1"

	ok, err := room.BeginAsk(ctx, agent, "token-a", time.Minute)
	if err != nil {
		t.Fatalf("begin ask: %v", err)
	}
	if !ok {
		t.Fatal("first BeginAsk returned false")
	}
	ok, err = room.BeginAsk(ctx, agent, "token-b", time.Minute)
	if err != nil {
		t.Fatalf("begin concurrent ask: %v", err)
	}
	if ok {
		t.Fatal("second BeginAsk returned true, want singleton lease rejection")
	}

	if err := room.EndAsk(ctx, agent, "wrong-token"); err != nil {
		t.Fatalf("end ask wrong token: %v", err)
	}
	ok, err = room.BeginAsk(ctx, agent, "token-c", time.Minute)
	if err != nil {
		t.Fatalf("begin after wrong-token end: %v", err)
	}
	if ok {
		t.Fatal("wrong-token EndAsk deleted the active lease")
	}

	if err := room.EndAsk(ctx, agent, "token-a"); err != nil {
		t.Fatalf("end ask: %v", err)
	}
	ok, err = room.BeginAsk(ctx, agent, "token-d", time.Minute)
	if err != nil {
		t.Fatalf("begin after end: %v", err)
	}
	if !ok {
		t.Fatal("BeginAsk after matching EndAsk returned false")
	}

	mr.FastForward(time.Minute + time.Second)
	ok, err = room.BeginAsk(ctx, agent, "token-e", time.Minute)
	if err != nil {
		t.Fatalf("begin after ttl expiry: %v", err)
	}
	if !ok {
		t.Fatal("BeginAsk after TTL expiry returned false")
	}
}

func TestWaitReturnsNextEventWithoutConsumerGroup(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("requires redis (miniredis)")
	}
	room, _ := newTestRoom(t)
	ctx := context.Background()
	lastID, err := room.LastID(ctx)
	if err != nil {
		t.Fatalf("last id: %v", err)
	}
	got := make(chan []Event, 1)
	errc := make(chan error, 1)
	go func() {
		events, err := room.Wait(ctx, lastID, 3*time.Second, 1)
		if err != nil {
			errc <- err
			return
		}
		got <- events
	}()

	if err := room.Publish(ctx, &Event{Type: "PING", AgentID: "peer"}); err != nil {
		t.Fatalf("publish: %v", err)
	}

	select {
	case err := <-errc:
		t.Fatalf("wait error: %v", err)
	case events := <-got:
		if len(events) != 1 || events[0].Type != "PING" {
			t.Fatalf("wait events = %+v, want one PING", events)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for Wait")
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
