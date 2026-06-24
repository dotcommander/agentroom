package agentroom

import (
	"context"
	"testing"
)

func TestDirectedReturnsOnlyAddressedEvents(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("requires redis (miniredis)")
	}
	room, _ := newTestRoom(t)
	ctx := context.Background()

	if err := room.Publish(ctx, &Event{Type: "MSG", AgentID: "alice", To: "bob", Payload: []byte(`{"m":1}`)}); err != nil {
		t.Fatalf("publish directed: %v", err)
	}
	if err := room.Publish(ctx, &Event{Type: "MSG", AgentID: "alice"}); err != nil {
		t.Fatalf("publish broadcast: %v", err)
	}
	if err := room.Publish(ctx, &Event{Type: "MSG", AgentID: "carol", To: "bob", ReplyTo: "1-0"}); err != nil {
		t.Fatalf("publish reply: %v", err)
	}

	got, err := room.Directed(ctx, "bob", 10)
	if err != nil {
		t.Fatalf("directed: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("want 2 events directed to bob (broadcast excluded), got %d", len(got))
	}
	// Newest-first: carol's threaded reply comes first.
	if got[0].AgentID != "carol" {
		t.Fatalf("want newest-first (carol), got %q", got[0].AgentID)
	}
	if got[0].To != "bob" || got[0].ReplyTo != "1-0" {
		t.Fatalf("To/ReplyTo not round-tripped through the stream: %+v", got[0])
	}
}
