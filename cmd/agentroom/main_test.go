package main

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/dotcommander/agentroom/agentroom"
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

func TestCoreCLICommandsAgainstMiniredis(t *testing.T) {
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
	runCoreCLICommandSequence(t, ctx, mr.Addr())
	runCoreCLIWaitSmoke(t, ctx, mr.Addr())
}

func runCoreCLICommandSequence(t *testing.T, ctx context.Context, addr string) {
	t.Helper()
	if err := runCLI(ctx, addr, "--repo", "cli-core", "--branch", "test", "register", "PATCH_READY", "patch can be reviewed", "--produces", "REVIEWED", "--requires", "reviewer"); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := runCLI(ctx, addr, "--repo", "cli-core", "--branch", "test", "catalog"); err != nil {
		t.Fatalf("catalog: %v", err)
	}
	if err := runCLI(ctx, addr, "--repo", "cli-core", "--branch", "test", "post", "PATCH_READY", `{"pr":42}`, "--agent", "builder"); err != nil {
		t.Fatalf("post task: %v", err)
	}
	if err := runCLI(ctx, addr, "--repo", "cli-core", "--branch", "test", "open"); err != nil {
		t.Fatalf("open: %v", err)
	}

	rdb := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { _ = rdb.Close() })
	room := agentroom.NewRoom(rdb, roomCfg(addr, "cli-core", "test"))
	events, err := room.Recent(ctx, 10)
	if err != nil {
		t.Fatalf("recent: %v", err)
	}
	if len(events) == 0 {
		t.Fatal("no events after post")
	}
	taskID := events[len(events)-1].ID

	if err := runCLI(ctx, addr, "--repo", "cli-core", "--branch", "test", "claim", taskID, "--agent", "reviewer", "--ttl", "1m"); err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := runCLI(ctx, addr, "--repo", "cli-core", "--branch", "test", "done", taskID, `{"status":"ok"}`, "--agent", "reviewer"); err != nil {
		t.Fatalf("done: %v", err)
	}
	if err := runCLI(ctx, addr, "--repo", "cli-core", "--branch", "test", "tail", "--count", "5", "--agent", "observer"); err != nil {
		t.Fatalf("tail: %v", err)
	}
	if err := runCLI(ctx, addr, "--repo", "cli-core", "--branch", "test", "who", "--agent", "observer"); err != nil {
		t.Fatalf("who: %v", err)
	}
	if err := runCLI(ctx, addr, "--repo", "cli-core", "--branch", "test", "welcome"); err != nil {
		t.Fatalf("welcome: %v", err)
	}
}

func runCoreCLIWaitSmoke(t *testing.T, ctx context.Context, addr string) {
	t.Helper()
	errc := make(chan error, 1)
	go func() {
		errc <- runCLI(ctx, addr, "--repo", "cli-core", "--branch", "test", "wait", "--timeout", "2s")
	}()
	time.Sleep(25 * time.Millisecond)
	if err := runCLI(ctx, addr, "--repo", "cli-core", "--branch", "test", "post", "WAKE", `{"ok":true}`, "--agent", "builder"); err != nil {
		t.Fatalf("post wake: %v", err)
	}
	select {
	case err := <-errc:
		if err != nil {
			t.Fatalf("wait: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("wait command did not unblock")
	}
}
