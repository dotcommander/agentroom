package agentroom

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestRegisterStoresPrerequisite verifies the additive Prerequisite field
// round-trips through the catalog (acceptance criterion 1).
func TestRegisterStoresPrerequisite(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("requires redis (miniredis)")
	}
	room, _ := newTestRoom(t)
	ctx := context.Background()
	if err := room.RegisterTask(ctx, TaskDef{
		Type:         "BUILD",
		Description:  "build artifacts",
		Prerequisite: "SOURCE_READY",
	}); err != nil {
		t.Fatalf("register: %v", err)
	}
	defs, err := room.Catalog(ctx)
	if err != nil {
		t.Fatalf("catalog: %v", err)
	}
	if defs["BUILD"].Prerequisite != "SOURCE_READY" {
		t.Errorf("Prerequisite = %q, want SOURCE_READY", defs["BUILD"].Prerequisite)
	}
}

// TestPrerequisiteMetDetectsEvent covers the three states of PrerequisiteMet:
// empty (gating disabled), declared-but-absent, and satisfied-by-a-recent-event.
func TestPrerequisiteMetDetectsEvent(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("requires redis (miniredis)")
	}
	room, _ := newTestRoom(t)
	ctx := context.Background()
	def := TaskDef{Type: "BUILD", Prerequisite: "SOURCE_READY"}

	if met, err := room.PrerequisiteMet(ctx, TaskDef{Type: "X"}); err != nil || !met {
		t.Fatalf("empty prereq should be met: met=%v err=%v", met, err)
	}
	if met, err := room.PrerequisiteMet(ctx, def); err != nil || met {
		t.Fatalf("absent prereq should be unmet: met=%v err=%v", met, err)
	}
	if err := room.Publish(ctx, &Event{Type: "SOURCE_READY", AgentID: "ci"}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if met, err := room.PrerequisiteMet(ctx, def); err != nil || !met {
		t.Fatalf("present prereq should be met: met=%v err=%v", met, err)
	}
}

// TestClaimCheckedRefusesUntilPrerequisitePosted is the core behavior: a task
// whose declared prerequisite is absent is refused with ErrPrerequisiteUnmet,
// and becomes claimable once the prerequisite event is posted (criteria 2, 3).
func TestClaimCheckedRefusesUntilPrerequisitePosted(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("requires redis (miniredis)")
	}
	room, _ := newTestRoom(t)
	ctx := context.Background()
	if err := room.RegisterTask(ctx, TaskDef{
		Type:         "BUILD",
		Description:  "build",
		Prerequisite: "SOURCE_READY",
	}); err != nil {
		t.Fatalf("register: %v", err)
	}
	taskEv := &Event{Type: "BUILD", AgentID: "ci"}
	if err := room.Publish(ctx, taskEv); err != nil {
		t.Fatalf("publish task: %v", err)
	}

	ok, err := room.ClaimChecked(ctx, taskEv.ID, "agent-a", time.Minute)
	if ok {
		t.Fatal("claim before prerequisite should be refused")
	}
	if !errors.Is(err, ErrPrerequisiteUnmet) {
		t.Fatalf("err = %v, want ErrPrerequisiteUnmet", err)
	}

	if err := room.Publish(ctx, &Event{Type: "SOURCE_READY", AgentID: "ci"}); err != nil {
		t.Fatalf("publish prerequisite: %v", err)
	}
	ok2, err := room.ClaimChecked(ctx, taskEv.ID, "agent-a", time.Minute)
	if err != nil || !ok2 {
		t.Fatalf("claim after prerequisite: ok=%v err=%v", ok2, err)
	}
}

// TestClaimCheckedUnchangedWithoutPrerequisite is the regression guard: a task
// with no declared prerequisite (here, not even catalogued) behaves exactly
// like Claim — first caller wins, second loses the race (criterion 4).
func TestClaimCheckedUnchangedWithoutPrerequisite(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("requires redis (miniredis)")
	}
	room, _ := newTestRoom(t)
	ctx := context.Background()
	taskEv := &Event{Type: "ADHOC", AgentID: "ci"}
	if err := room.Publish(ctx, taskEv); err != nil {
		t.Fatalf("publish: %v", err)
	}
	ok, err := room.ClaimChecked(ctx, taskEv.ID, "agent-a", time.Minute)
	if err != nil || !ok {
		t.Fatalf("first claim: ok=%v err=%v", ok, err)
	}
	ok2, err := room.ClaimChecked(ctx, taskEv.ID, "agent-b", time.Minute)
	if err != nil {
		t.Fatalf("second claim: %v", err)
	}
	if ok2 {
		t.Fatal("second claim should lose the race, matching Claim")
	}
}
