package main

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestPinWelcomeDedups(t *testing.T) {
	t.Parallel()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	defer mr.Close()
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer func() { _ = rdb.Close() }()
	ctx := context.Background()

	if _, err := pinWelcome(ctx, rdb, mr.Addr()); err != nil {
		t.Fatalf("first pin: %v", err)
	}
	if _, err := pinWelcome(ctx, rdb, mr.Addr()); err != nil {
		t.Fatalf("second pin: %v", err)
	}

	msgs, err := rdb.XRange(ctx, "repo:lobby:main:events", "-", "+").Result()
	if err != nil {
		t.Fatalf("xrange: %v", err)
	}
	welcomes := 0
	for _, m := range msgs {
		if m.Values["type"] == welcomeType {
			welcomes++
		}
	}
	if welcomes != 1 {
		t.Errorf("WELCOME entries after re-pin = %d, want 1", welcomes)
	}
	if ttl := mr.TTL("repo:lobby:main:events"); ttl != 0 {
		t.Errorf("lobby TTL = %v, want 0 (pinned, no expiry)", ttl)
	}
}
