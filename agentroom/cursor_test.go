package agentroom

import (
	"context"
	"testing"
	"time"
)

func TestCursorKeyShape(t *testing.T) {
	t.Parallel()
	cfg := Config{RepoID: "auth", BranchName: "main"}
	if got, want := cfg.CursorKey("sess1"), "repo:auth:main:cursor:sess1"; got != want {
		t.Errorf("CursorKey = %q, want %q", got, want)
	}
}

func TestSinceEmptyCursorReturnsNothing(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("requires redis (miniredis)")
	}
	room, _ := newTestRoom(t)
	events, err := room.Since(context.Background(), "", 10)
	if err != nil {
		t.Fatalf("since: %v", err)
	}
	if len(events) != 0 {
		t.Errorf("Since empty = %d events, want 0", len(events))
	}
}

func TestSinceReturnsEventsAfterCursor(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("requires redis (miniredis)")
	}
	room, _ := newTestRoom(t)
	ctx := context.Background()
	first := &Event{Type: "A", AgentID: "p"}
	if err := room.Publish(ctx, first); err != nil {
		t.Fatalf("publish A: %v", err)
	}
	for _, typ := range []string{"B", "C"} {
		if err := room.Publish(ctx, &Event{Type: typ, AgentID: "p"}); err != nil {
			t.Fatalf("publish %s: %v", typ, err)
		}
	}
	events, err := room.Since(ctx, first.ID, 10)
	if err != nil {
		t.Fatalf("since: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("Since(first) = %d events, want 2", len(events))
	}
	if events[0].Type != "B" || events[1].Type != "C" {
		t.Errorf("delta = [%s,%s], want [B,C]", events[0].Type, events[1].Type)
	}
}

func TestSinceRespectsCount(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("requires redis (miniredis)")
	}
	room, _ := newTestRoom(t)
	ctx := context.Background()
	first := &Event{Type: "A", AgentID: "p"}
	if err := room.Publish(ctx, first); err != nil {
		t.Fatalf("publish A: %v", err)
	}
	for _, typ := range []string{"B", "C", "D"} {
		if err := room.Publish(ctx, &Event{Type: typ, AgentID: "p"}); err != nil {
			t.Fatalf("publish %s: %v", typ, err)
		}
	}
	events, err := room.Since(ctx, first.ID, 2)
	if err != nil {
		t.Fatalf("since: %v", err)
	}
	if len(events) != 2 {
		t.Errorf("Since count=2 returned %d events, want 2", len(events))
	}
}

func TestSinceFromZeroBaseline(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("requires redis (miniredis)")
	}
	room, _ := newTestRoom(t)
	ctx := context.Background()
	for _, typ := range []string{"A", "B"} {
		if err := room.Publish(ctx, &Event{Type: typ, AgentID: "p"}); err != nil {
			t.Fatalf("publish %s: %v", typ, err)
		}
	}
	events, err := room.Since(ctx, "0-0", 10)
	if err != nil {
		t.Fatalf("since: %v", err)
	}
	if len(events) != 2 {
		t.Errorf("Since zero = %d events, want 2", len(events))
	}
}

func TestLastIDEmptyAndPopulated(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("requires redis (miniredis)")
	}
	room, _ := newTestRoom(t)
	ctx := context.Background()
	id, err := room.LastID(ctx)
	if err != nil {
		t.Fatalf("last id (empty): %v", err)
	}
	if id != "0-0" {
		t.Errorf("LastID empty = %q, want 0-0", id)
	}
	ev := &Event{Type: "A", AgentID: "p"}
	if err := room.Publish(ctx, ev); err != nil {
		t.Fatalf("publish: %v", err)
	}
	id, err = room.LastID(ctx)
	if err != nil {
		t.Fatalf("last id: %v", err)
	}
	if id != ev.ID {
		t.Errorf("LastID = %q, want %q", id, ev.ID)
	}
}

func TestCursorRoundTrip(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("requires redis (miniredis)")
	}
	room, _ := newTestRoom(t)
	ctx := context.Background()
	got, err := room.ReadCursor(ctx, "sess1")
	if err != nil {
		t.Fatalf("read missing cursor: %v", err)
	}
	if got != "" {
		t.Errorf("missing cursor = %q, want empty", got)
	}
	if err := room.WriteCursor(ctx, "sess1", "5-0", time.Hour); err != nil {
		t.Fatalf("write cursor: %v", err)
	}
	got, err = room.ReadCursor(ctx, "sess1")
	if err != nil {
		t.Fatalf("read cursor: %v", err)
	}
	if got != "5-0" {
		t.Errorf("cursor = %q, want 5-0", got)
	}
}

func TestReplayCursorReplaysRecentEvents(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("requires redis (miniredis)")
	}
	room, _ := newTestRoom(t)
	ctx := context.Background()

	// A peer posts an event just before we join.
	if err := room.Publish(ctx, &Event{Type: "CONFIG_CHANGED", AgentID: "peer"}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	last, err := room.LastID(ctx)
	if err != nil {
		t.Fatalf("last id: %v", err)
	}

	// The trap: a session baselining to the bare tail surfaces no prior events.
	trapped, err := room.Since(ctx, last, 20)
	if err != nil {
		t.Fatalf("since tail: %v", err)
	}
	if len(trapped) != 0 {
		t.Fatalf("tail baseline should surface no prior events, got %d", len(trapped))
	}

	// The fix: a replay cursor (now-window) surfaces the just-landed event.
	replay := room.ReplayCursorFrom(time.Now(), 10*time.Minute)
	got, err := room.Since(ctx, replay, 20)
	if err != nil {
		t.Fatalf("since replay: %v", err)
	}
	if len(got) == 0 {
		t.Fatal("replay cursor should surface the recent CONFIG_CHANGED, got none")
	}
}
