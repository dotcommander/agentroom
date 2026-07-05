package main

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/dotcommander/agentroom/agentroom"
	"github.com/redis/go-redis/v9"
)

func TestResolveLiveTargetRejectsMissingAndAmbiguous(t *testing.T) {
	if testing.Short() {
		t.Skip("requires redis (miniredis)")
	}
	room := newCLITestRoom(t)
	ctx := context.Background()
	for _, id := range []string{"reviewer-abc", "reviewer-def"} {
		if err := room.Heartbeat(ctx, id, "", time.Minute); err != nil {
			t.Fatalf("heartbeat %s: %v", id, err)
		}
	}

	if _, err := resolveLiveTarget(ctx, room, "nobody"); err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("missing target error = %v, want no live agent match", err)
	}
	if _, err := resolveLiveTarget(ctx, room, "reviewer"); err == nil || !strings.Contains(err.Error(), "ambiguous") {
		t.Fatalf("ambiguous target error = %v, want ambiguity", err)
	}
}

func TestAskReplyAcrossCLI(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test: requires miniredis, exercises CLI ask/reply")
	}
	mr, room := newSharedCLIRoom(t)
	ctx := context.Background()
	const asker = "asker"
	const replier = "replier"
	if err := room.Heartbeat(ctx, qualifyAgent(replier), "reviewer", time.Minute); err != nil {
		t.Fatalf("heartbeat replier: %v", err)
	}

	beforeAsk, err := room.LastID(ctx)
	if err != nil {
		t.Fatalf("last id: %v", err)
	}
	errc := make(chan error, 1)
	go func() {
		errc <- runCLI(ctx, mr.Addr(), "ask", "can you review?", "--agent", asker, "--to", replier, "--timeout", "2s")
	}()

	ask := waitForMatchingAfter(t, ctx, room, beforeAsk, func(ev agentroom.Event) bool {
		return ev.Type == eventAsk && ev.AgentID == qualifyAgent(asker)
	})
	if ask.To != qualifyAgent(replier) {
		t.Fatalf("ask target = %q, want %q", ask.To, qualifyAgent(replier))
	}

	if err := runCLI(ctx, mr.Addr(), "reply", ask.ID, "done", "--agent", replier); err != nil {
		t.Fatalf("reply: %v", err)
	}

	select {
	case err := <-errc:
		if err != nil {
			t.Fatalf("ask returned error: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("ask did not unblock after reply")
	}

	reply := waitForMatchingAfter(t, ctx, room, ask.ID, func(ev agentroom.Event) bool {
		return ev.Type == eventReply && ev.ReplyTo == ask.ID
	})
	if reply.AgentID != qualifyAgent(replier) || reply.To != qualifyAgent(asker) {
		t.Fatalf("reply correlation = %+v, want from replier to asker", reply)
	}
}

func TestWaitForReplyTimeout(t *testing.T) {
	if testing.Short() {
		t.Skip("requires redis (miniredis)")
	}
	room := newCLITestRoom(t)
	ctx := context.Background()
	ask := &agentroom.Event{Type: eventAsk, AgentID: "asker", To: "replier"}
	if err := room.Publish(ctx, ask); err != nil {
		t.Fatalf("publish ask: %v", err)
	}

	_, err := waitForReply(ctx, room, replyWait{
		AskID:             ask.ID,
		ExpectedSender:    "replier",
		ExpectedRecipient: "asker",
		Timeout:           time.Millisecond,
	})
	if err == nil || !strings.Contains(err.Error(), "ask timed out") {
		t.Fatalf("waitForReply error = %v, want timeout", err)
	}
}

func TestBeginAskRejectsConcurrentAsk(t *testing.T) {
	if testing.Short() {
		t.Skip("requires redis (miniredis)")
	}
	mr, room := newSharedCLIRoom(t)
	ctx := context.Background()
	const asker = "asker"
	const replier = "replier"
	qAsker := qualifyAgent(asker)
	if err := room.Heartbeat(ctx, qualifyAgent(replier), "reviewer", time.Minute); err != nil {
		t.Fatalf("heartbeat replier: %v", err)
	}
	ok, err := room.BeginAsk(ctx, qAsker, "existing", time.Minute)
	if err != nil {
		t.Fatalf("begin ask: %v", err)
	}
	if !ok {
		t.Fatal("first BeginAsk returned false")
	}

	before, err := room.Recent(ctx, 10)
	if err != nil {
		t.Fatalf("recent before: %v", err)
	}
	err = runCLI(ctx, mr.Addr(), "ask", "second?", "--agent", asker, "--to", replier, "--timeout", "100ms")
	if err == nil || !strings.Contains(err.Error(), "already has a pending ask") {
		t.Fatalf("ask error = %v, want pending ask rejection", err)
	}
	after, err := room.Recent(ctx, 10)
	if err != nil {
		t.Fatalf("recent after: %v", err)
	}
	if len(after) != len(before) {
		t.Fatalf("rejected ask emitted an event: before=%d after=%d", len(before), len(after))
	}
}

func newSharedCLIRoom(t *testing.T) (*miniredis.Miniredis, *agentroom.Room) {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	t.Cleanup(mr.Close)

	orig := newRedisClient
	newRedisClient = func(string) *redis.Client {
		return redis.NewClient(&redis.Options{Addr: mr.Addr()})
	}
	t.Cleanup(func() { newRedisClient = orig })

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return mr, agentroom.NewRoom(rdb, roomCfg(mr.Addr(), defaultRepo(), defaultBranch))
}

func waitForMatchingAfter(t *testing.T, ctx context.Context, room *agentroom.Room, lastID string, match func(agentroom.Event) bool) agentroom.Event {
	t.Helper()
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	for {
		events, err := room.Wait(ctx, lastID, 50*time.Millisecond, 10)
		if err != nil {
			t.Fatalf("wait: %v", err)
		}
		for _, ev := range events {
			lastID = ev.ID
			if match(ev) {
				return ev
			}
		}
	}
}
