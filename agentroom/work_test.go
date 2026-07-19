package agentroom

import (
	"context"
	"testing"
	"time"
)

func TestWorkStatusTransitionsAndHandoffInbox(t *testing.T) { //nolint:gocyclo // one lifecycle test asserts event, materialized-state, and inbox transitions
	room, _ := newTestRoom(t)
	ctx := context.Background()
	started, err := room.SetWorkStatus(ctx, WorkStatus{AgentID: "alice-1", State: WorkStarted, Scope: "path:cmd", Summary: "editing"}, time.Hour)
	if err != nil {
		t.Fatalf("start work: %v", err)
	}
	active, err := room.WorkStatuses(ctx)
	if err != nil || len(active) != 1 || active[0].ID != started.ID {
		t.Fatalf("active work = %+v, err=%v", active, err)
	}
	if _, err := room.SetWorkStatus(ctx, WorkStatus{AgentID: "alice-1", State: WorkHandoff, Scope: "path:cmd", Summary: "ready", To: "bob-2"}, time.Hour); err != nil {
		t.Fatalf("handoff: %v", err)
	}
	inbox, err := room.InboxSince(ctx, "bob-2", "", 10)
	if err != nil || len(inbox) != 1 || inbox[0].Event.Type != "WORK_STATUS" {
		t.Fatalf("handoff inbox = %+v, err=%v", inbox, err)
	}
	recipients, err := room.InboxRecipientsEndingWith(ctx, "bob-2")
	if err != nil || len(recipients) != 1 || recipients[0] != "bob-2" {
		t.Fatalf("work inbox recipients=%v err=%v", recipients, err)
	}
	if _, err := room.SetWorkStatus(ctx, WorkStatus{AgentID: "alice-1", State: WorkCompleted, Scope: "path:cmd", Summary: "done"}, time.Hour); err != nil {
		t.Fatalf("complete: %v", err)
	}
	active, err = room.WorkStatuses(ctx)
	if err != nil || len(active) != 0 {
		t.Fatalf("active after complete = %+v, err=%v", active, err)
	}
	events, err := room.Recent(ctx, 10)
	if err != nil || len(events) != 3 {
		t.Fatalf("work audit events = %+v, err=%v", events, err)
	}
}
