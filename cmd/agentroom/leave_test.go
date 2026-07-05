package main

import (
	"context"
	"os"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/dotcommander/agentroom/agentroom"
	"github.com/redis/go-redis/v9"
)

func TestLeaveCmdClearsPresence(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test: requires miniredis")
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
	wd, _ := os.Getwd()
	repo, branch := resolveRoom(ctx, wd)
	const agent = "qa-leave"
	qAgent := qualifyAgent(agent) // CLI qualifies --agent; presence key carries the session token

	// Seed presence via Heartbeat, matching the mechanism the integration
	// test uses for post-driven presence.
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	cfg := roomCfg(mr.Addr(), repo, branch)
	room := agentroom.NewRoom(rdb, cfg)
	if err := room.Heartbeat(ctx, qAgent, "tester", cfg.PresenceTTL); err != nil {
		t.Fatalf("seed heartbeat: %v", err)
	}
	_ = rdb.Close()

	// Assert agent is present before leave.
	rdb2 := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	room2 := agentroom.NewRoom(rdb2, roomCfg(mr.Addr(), repo, branch))
	pres, err := room2.Presence(ctx)
	if err != nil {
		t.Fatalf("presence before leave: %v", err)
	}
	if _, ok := pres[qAgent]; !ok {
		t.Fatalf("agent %q absent from presence before leave; got %v", qAgent, pres)
	}
	_ = rdb2.Close()

	// Execute leave command via the real CLI path.
	if err := runCLI(ctx, mr.Addr(), "leave", "--agent", agent); err != nil {
		t.Fatalf("first leave: %v", err)
	}

	// Assert agent is gone from presence.
	rdb3 := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	room3 := agentroom.NewRoom(rdb3, roomCfg(mr.Addr(), repo, branch))
	pres, err = room3.Presence(ctx)
	if err != nil {
		t.Fatalf("presence after leave: %v", err)
	}
	if _, ok := pres[qAgent]; ok {
		t.Fatalf("agent %q still present after leave; got %v", qAgent, pres)
	}
	_ = rdb3.Close()

	// Leave again: must be idempotent (no error).
	if err := runCLI(ctx, mr.Addr(), "leave", "--agent", agent); err != nil {
		t.Fatalf("second leave (idempotent) returned error: %v", err)
	}
}
