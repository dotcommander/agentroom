package agentroom

import (
	"context"
	"testing"
	"time"
)

const (
	evFailed = "TESTS_FAILED"
	idClaim  = "task-c1"
	idDone   = "task-d1"
)

func TestCatalogRegisterAndList(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("requires redis (miniredis)")
	}
	room, _ := newTestRoom(t)
	ctx := context.Background()
	if err := room.RegisterTask(ctx, TaskDef{Type: evFailed, Description: "a test run failed", Produces: "FIX_APPLIED", Requires: "fixer"}); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := room.RegisterTask(ctx, TaskDef{Type: "FILE_CHANGED", Description: "source changed", Produces: "AST_PARSED", Requires: "parser"}); err != nil {
		t.Fatalf("register: %v", err)
	}
	defs, err := room.Catalog(ctx)
	if err != nil {
		t.Fatalf("catalog: %v", err)
	}
	if len(defs) != 2 {
		t.Fatalf("catalog has %d defs, want 2", len(defs))
	}
	if defs[evFailed].Produces != "FIX_APPLIED" {
		t.Errorf("%s.Produces = %q, want FIX_APPLIED", evFailed, defs[evFailed].Produces)
	}
	if defs["FILE_CHANGED"].Requires != "parser" {
		t.Errorf("FILE_CHANGED.Requires = %q, want parser", defs["FILE_CHANGED"].Requires)
	}
}

func TestClaimIsExclusive(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("requires redis (miniredis)")
	}
	room, _ := newTestRoom(t)
	ctx := context.Background()
	ok, err := room.Claim(ctx, idClaim, "agent-a", time.Minute)
	if err != nil || !ok {
		t.Fatalf("first claim: ok=%v err=%v", ok, err)
	}
	ok2, err := room.Claim(ctx, idClaim, "agent-b", time.Minute)
	if err != nil {
		t.Fatalf("second claim: %v", err)
	}
	if ok2 {
		t.Error("second claim should fail — task already owned")
	}
	st, err := room.TaskState(ctx, idClaim)
	if err != nil {
		t.Fatalf("state: %v", err)
	}
	if st.State != TaskClaimed || st.Owner != "agent-a" {
		t.Errorf("state = %+v, want claimed by agent-a", st)
	}
}

func TestCompleteMarksDone(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("requires redis (miniredis)")
	}
	room, _ := newTestRoom(t)
	ctx := context.Background()
	if _, err := room.Claim(ctx, idDone, "agent-x", time.Minute); err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := room.Complete(ctx, idDone, []byte(`{"ok":true}`)); err != nil {
		t.Fatalf("complete: %v", err)
	}
	st, err := room.TaskState(ctx, idDone)
	if err != nil {
		t.Fatalf("state: %v", err)
	}
	if st.State != TaskDone {
		t.Errorf("state = %q, want done", st.State)
	}
	if string(st.Result) != `{"ok":true}` {
		t.Errorf("result = %q, want {\"ok\":true}", st.Result)
	}
	ok, err := room.Claim(ctx, idDone, "agent-c", time.Minute)
	if err != nil {
		t.Fatalf("reclaim: %v", err)
	}
	if ok {
		t.Error("claiming a done task should fail")
	}
}

func TestOpenTasks(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("requires redis (miniredis)")
	}
	room, _ := newTestRoom(t)
	ctx := context.Background()
	if err := room.RegisterTask(ctx, TaskDef{Type: evFailed, Description: "failed"}); err != nil {
		t.Fatalf("register: %v", err)
	}
	t1 := &Event{Type: evFailed, AgentID: "ci"}
	t2 := &Event{Type: evFailed, AgentID: "ci"}
	noise := &Event{Type: "HEARTBEAT", AgentID: "ci"}
	for _, e := range []*Event{t1, t2, noise} {
		if err := room.Publish(ctx, e); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}
	if ok, err := room.Claim(ctx, t1.ID, "fixer-1", time.Minute); err != nil || !ok {
		t.Fatalf("claim t1: ok=%v err=%v", ok, err)
	}
	open, err := room.OpenTasks(ctx, 10)
	if err != nil {
		t.Fatalf("open tasks: %v", err)
	}
	if len(open) != 1 {
		t.Fatalf("open tasks = %d, want 1 (t2 only)", len(open))
	}
	if open[0].ID != t2.ID {
		t.Errorf("open task id = %s, want t2 %s", open[0].ID, t2.ID)
	}
	if open[0].Type != evFailed {
		t.Errorf("open task type = %q, want %s", open[0].Type, evFailed)
	}
}

func TestOutstandingClaims(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("requires redis (miniredis)")
	}
	room, mr := newTestRoom(t)
	ctx := context.Background()
	const lease = 60 * time.Second

	// agentA claims two tasks; a different agent claims a third.
	for _, id := range []string{"oc-1", "oc-2"} {
		ok, err := room.Claim(ctx, id, "agentA", lease)
		if err != nil {
			t.Fatalf("claim %s: %v", id, err)
		}
		if !ok {
			t.Fatalf("claim %s not granted", id)
		}
	}
	if ok, err := room.Claim(ctx, "oc-3", "agentB", lease); err != nil || !ok {
		t.Fatalf("claim oc-3 as agentB: ok=%v err=%v", ok, err)
	}

	n, err := room.OutstandingClaims(ctx, "agentA")
	if err != nil {
		t.Fatalf("outstanding agentA: %v", err)
	}
	if n != 2 {
		t.Fatalf("agentA outstanding = %d, want 2", n)
	}
	if n, err := room.OutstandingClaims(ctx, "agentB"); err != nil || n != 1 {
		t.Fatalf("agentB outstanding = %d (err %v), want 1", n, err)
	}

	// Completing one of agentA's tasks releases its owner lease → count drops.
	if err := room.Complete(ctx, "oc-1", nil); err != nil {
		t.Fatalf("complete oc-1: %v", err)
	}
	n, err = room.OutstandingClaims(ctx, "agentA")
	if err != nil {
		t.Fatalf("outstanding after complete: %v", err)
	}
	if n != 1 {
		t.Fatalf("agentA outstanding after complete = %d, want 1", n)
	}

	// Crash simulation: let the remaining lease expire (no Complete) → count → 0.
	mr.FastForward(lease + time.Second)
	n, err = room.OutstandingClaims(ctx, "agentA")
	if err != nil {
		t.Fatalf("outstanding after expiry: %v", err)
	}
	if n != 0 {
		t.Fatalf("agentA outstanding after lease expiry = %d, want 0", n)
	}
}
