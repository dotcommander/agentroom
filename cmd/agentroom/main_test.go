package main

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/dotcommander/agentchat/agentroom"
	"github.com/redis/go-redis/v9"
)

func TestRootCmdHasSubcommands(t *testing.T) {
	t.Parallel()
	root := rootCmd()
	got := map[string]bool{}
	for _, c := range root.Commands() {
		got[c.Name()] = true
	}
	for _, name := range []string{"tail", "post", "wait", "ask", "reply", "catalog", "register", "open", "claim", "done", "leave", "hook", "welcome"} {
		if !got[name] {
			t.Errorf("missing subcommand %q", name)
		}
	}
}

func newCLITestRoom(t *testing.T) *agentroom.Room {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	t.Cleanup(mr.Close)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	cfg := roomCfg(mr.Addr(), "testrepo", "main")
	return agentroom.NewRoom(rdb, cfg)
}

func TestResolveTargetAgainstRoster(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("requires redis (miniredis)")
	}
	room := newCLITestRoom(t)
	ctx := context.Background()
	if err := room.Heartbeat(ctx, "gary-abc", "reviewer", time.Minute); err != nil {
		t.Fatalf("heartbeat gary: %v", err)
	}
	if err := room.Heartbeat(ctx, "sam-def", "builder", time.Minute); err != nil {
		t.Fatalf("heartbeat sam: %v", err)
	}

	got, err := resolveTarget(ctx, room, "gary")
	if err != nil {
		t.Fatalf("resolve target: %v", err)
	}
	if got != "gary-abc" {
		t.Fatalf("resolve target = %q, want gary-abc", got)
	}

	roomKey, err := resolveTarget(ctx, room, "repo:branch")
	if err != nil {
		t.Fatalf("resolve room target: %v", err)
	}
	if roomKey != "repo:branch" {
		t.Fatalf("room target = %q, want repo:branch", roomKey)
	}
}

func TestResolveTargetRejectsAmbiguousRosterPrefix(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("requires redis (miniredis)")
	}
	room := newCLITestRoom(t)
	ctx := context.Background()
	for _, id := range []string{"gary-abc", "gary-def"} {
		if err := room.Heartbeat(ctx, id, "", time.Minute); err != nil {
			t.Fatalf("heartbeat %s: %v", id, err)
		}
	}

	_, err := resolveTarget(ctx, room, "gary")
	if err == nil {
		t.Fatal("resolve target succeeded, want ambiguity error")
	}
	if !strings.Contains(err.Error(), "gary-abc") || !strings.Contains(err.Error(), "gary-def") {
		t.Fatalf("ambiguity error missing candidates: %v", err)
	}
}

func TestPostToEnqueuesDurableInbox(t *testing.T) {
	if testing.Short() {
		t.Skip("requires redis (miniredis)")
	}
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

	ctx := context.Background()
	if err := runCLI(ctx, mr.Addr(), "post", "MSG", `{"m":1}`, "--agent", "alice", "--to", "gary"); err != nil {
		t.Fatalf("post --to: %v", err)
	}

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	wd, _ := os.Getwd()
	repo, branch := resolveRoom(ctx, wd)
	room := agentroom.NewRoom(rdb, roomCfg(mr.Addr(), repo, branch))
	got, err := room.InboxSince(ctx, "gary", "", 10)
	if err != nil {
		t.Fatalf("inbox since: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("inbox count = %d, want 1", len(got))
	}
	if got[0].Event.Type != "MSG" || got[0].Event.To != "gary" {
		t.Fatalf("inbox event = %+v", got[0].Event)
	}
}
