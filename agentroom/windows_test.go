package agentroom

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestWindowAcknowledgmentAndLeaseBlockerProtocol(t *testing.T) { //nolint:gocyclo // one lifecycle test asserts every state transition
	room, _ := newTestRoom(t)
	ctx := context.Background()
	lease, err := room.AcquireResources(ctx, ResourceLeaseRequest{Owner: "alice", Resources: []string{"path:internal/db"}})
	if err != nil {
		t.Fatal(err)
	}
	w, err := room.RequestWindow(ctx, WindowRequest{Owner: "operator", Resources: []string{"path:internal"}, Purpose: "migration", Required: []string{"reviewer"}})
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	if w.State != "pending" {
		t.Fatalf("state=%q", w.State)
	}
	recipients, err := room.InboxRecipientsEndingWith(ctx, "reviewer")
	if err != nil || len(recipients) != 1 || recipients[0] != "reviewer" {
		t.Fatalf("window inbox recipients=%v err=%v", recipients, err)
	}
	for _, recipient := range []string{"alice", "reviewer"} {
		inbox, err := room.InboxSince(ctx, recipient, "", 10)
		if err != nil || len(inbox) != 1 || inbox[0].Event.Type != "WINDOW_ACK_REQUIRED" {
			t.Fatalf("inbox %s=%+v err=%v", recipient, inbox, err)
		}
	}
	if _, err := room.ActivateWindow(ctx, w.ID, "operator"); !errors.Is(err, ErrAcknowledgmentDue) {
		t.Fatalf("premature activation=%v", err)
	}
	if _, err := room.AcknowledgeWindow(ctx, w.ID, "stranger"); !errors.Is(err, ErrNotOwner) {
		t.Fatalf("unauthorized ack=%v", err)
	}
	for _, id := range []string{"alice", "reviewer"} {
		if _, err := room.AcknowledgeWindow(ctx, w.ID, id); err != nil {
			t.Fatalf("ack %s: %v", id, err)
		}
		events, err := room.Recent(ctx, 1)
		if err != nil || len(events) != 1 || events[0].Type != "WINDOW_ACKNOWLEDGED" || events[0].AgentID != id {
			t.Fatalf("ack event for %s = %+v, err=%v", id, events, err)
		}
		if _, err := room.AcknowledgeWindow(ctx, w.ID, id); err != nil {
			t.Fatalf("idempotent ack %s: %v", id, err)
		}
	}
	if _, err := room.ActivateWindow(ctx, w.ID, "operator"); !errors.Is(err, ErrLeaseConflict) {
		t.Fatalf("activation with blocker=%v", err)
	}
	if err := room.ReleaseResources(ctx, lease.ID, "alice"); err != nil {
		t.Fatal(err)
	}
	active, err := room.ActivateWindow(ctx, w.ID, "operator")
	if err != nil {
		t.Fatalf("activate: %v", err)
	}
	if active.State != "active" {
		t.Fatalf("active state=%q", active.State)
	}
	if _, err := room.AcquireResources(ctx, ResourceLeaseRequest{Owner: "bob", Resources: []string{"path:internal/api"}}); !errors.Is(err, ErrLeaseConflict) {
		t.Fatalf("lease during window=%v", err)
	}
	if err := room.ReleaseWindow(ctx, w.ID, "operator"); err != nil {
		t.Fatalf("release: %v", err)
	}
	windows, err := room.Windows(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(windows) != 1 || windows[0].State != "released" {
		t.Fatalf("windows=%+v", windows)
	}
}

func TestWindowConcurrentActivationSingleWinner(t *testing.T) {
	room, _ := newTestRoom(t)
	ctx := context.Background()
	w, err := room.RequestWindow(ctx, WindowRequest{Owner: "alice", Resources: []string{"service:deploy"}, Required: []string{"bob"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := room.AcknowledgeWindow(ctx, w.ID, "bob"); err != nil {
		t.Fatal(err)
	}
	results := make(chan error, 2)
	var wg sync.WaitGroup
	for range 2 {
		wg.Go(func() { _, err := room.ActivateWindow(ctx, w.ID, "alice"); results <- err })
	}
	wg.Wait()
	close(results)
	successes, rejected := 0, 0
	for err := range results {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, ErrInvalidState):
			rejected++
		default:
			t.Fatalf("activation error=%v", err)
		}
	}
	if successes != 1 || rejected != 1 {
		t.Fatalf("successes=%d rejected=%d", successes, rejected)
	}
}

func TestWindowAuditFailureRollsBackState(t *testing.T) {
	room, _ := newTestRoom(t)
	ctx := context.Background()
	if err := room.rdb.Set(ctx, room.cfg.StreamKey(), "not-a-stream", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := room.RequestWindow(ctx, WindowRequest{Owner: "alice", Resources: []string{"service:deploy"}}); err == nil {
		t.Fatal("request succeeded despite audit failure")
	}
	windows, err := room.Windows(ctx)
	if err != nil || len(windows) != 0 {
		t.Fatalf("window mutation survived failed atomic audit: %+v err=%v", windows, err)
	}
}

func TestWindowImmediateActivationConflictAndCancel(t *testing.T) {
	room, _ := newTestRoom(t)
	ctx := context.Background()
	active, err := room.RequestWindow(ctx, WindowRequest{Owner: "alice", Resources: []string{"service:deploy"}, TTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if active.State != "active" {
		t.Fatalf("state=%q", active.State)
	}
	if _, err := room.RequestWindow(ctx, WindowRequest{Owner: "bob", Resources: []string{"service:deploy"}}); !errors.Is(err, ErrLeaseConflict) {
		t.Fatalf("overlap request=%v", err)
	}
	if err := room.ReleaseWindow(ctx, active.ID, "alice"); err != nil {
		t.Fatal(err)
	}
	pending, err := room.RequestWindow(ctx, WindowRequest{Owner: "bob", Resources: []string{"service:deploy"}, Required: []string{"reviewer"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := room.CancelWindow(ctx, pending.ID, "alice"); !errors.Is(err, ErrNotOwner) {
		t.Fatalf("cancel owner error=%v", err)
	}
	if err := room.CancelWindow(ctx, pending.ID, "bob"); err != nil {
		t.Fatal(err)
	}
}

func TestWindowAckTimeout(t *testing.T) {
	room, mr := newTestRoom(t)
	ctx := context.Background()
	start := time.Unix(2_000_000_000, 0)
	mr.SetTime(start)
	w, err := room.RequestWindow(ctx, WindowRequest{Owner: "alice", Resources: []string{"binary:cm"}, Required: []string{"bob"}, AckTimeout: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	mr.SetTime(start.Add(2 * time.Minute))
	mr.FastForward(2 * time.Minute)
	if _, err := room.AcknowledgeWindow(ctx, w.ID, "bob"); err == nil {
		t.Fatal("ack succeeded after deadline")
	}
	windows, err := room.Windows(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(windows) != 1 || windows[0].State != "expired" {
		t.Fatalf("windows=%+v", windows)
	}
}
