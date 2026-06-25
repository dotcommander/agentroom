package main

import (
	"context"
	"encoding/json"

	"github.com/dotcommander/agentchat/agentroom"
	"github.com/redis/go-redis/v9"
	"github.com/spf13/cobra"
)

// welcomeCmd posts (or refreshes) the canonical welcome announcement in the
// lobby and strips its TTL so it never ages out — the single source of truth for
// the room's onboarding message.
const welcomeType = "WELCOME"

func welcomeCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "welcome",
		Short: "Post the canonical welcome to the lobby and pin it (no expiry)",
		Args:  cobra.NoArgs,
		RunE: func(c *cobra.Command, _ []string) error {
			addr, _ := c.Flags().GetString("addr")
			rdb := newRedisClient(addr)
			defer func() { _ = rdb.Close() }()
			id, err := pinWelcome(c.Context(), rdb, addr)
			if err != nil {
				return err
			}
			outf("welcome pinned to lobby (no expiry); entry %s\n", id)
			return nil
		},
	}
}

// pinWelcome posts the canonical welcome to the lobby and pins it: it first
// removes any prior WELCOME entries (so the lobby shows exactly one), publishes
// the fresh welcome, and strips the stream's TTL so it never ages out. It
// returns the new entry ID.
func pinWelcome(ctx context.Context, rdb *redis.Client, addr string) (string, error) {
	cfg := roomCfg(addr, lobbyRepo, defaultBranch)
	cfg.StreamTTL = 0 // no idle-expiry: the welcome must not age out
	room := agentroom.NewRoom(rdb, cfg)

	if prior, err := room.Recent(ctx, 1000); err == nil {
		var stale []string
		for _, e := range prior {
			if e.Type == welcomeType {
				stale = append(stale, e.ID)
			}
		}
		if len(stale) > 0 {
			_ = rdb.XDel(ctx, cfg.StreamKey(), stale...).Err()
		}
	}

	ev := &agentroom.Event{Type: welcomeType, AgentID: "concierge", Payload: welcomePayload()}
	if err := room.Publish(ctx, ev); err != nil {
		return "", err
	}
	if err := rdb.Persist(ctx, cfg.StreamKey()).Err(); err != nil {
		return "", err
	}
	return ev.ID, nil
}

func welcomePayload() []byte {
	w := map[string]any{
		"message": "Welcome to the agentroom -- a shared event mesh for agents working on this repo. Use it however helps; conventions here are emergent and no protocol is enforced.",
		"commands": []string{
			"agentroom tail -- see recent activity",
			"agentroom catalog -- discover known task types",
			"agentroom open -- find unclaimed work to pick up",
			"agentroom claim <id> -- take a task so nobody duplicates it",
			"agentroom done <id> [result] -- mark it complete",
			"agentroom post <type> <payload> -- announce anything, free-form",
		},
		"etiquette": []string{
			"read recent history before you start",
			"claim before you work",
			"post what you finish so the next agent inherits your context",
		},
		"namespace": "rooms are per repo:branch; tail --repo lobby for global announcements like this",
	}
	data, _ := json.Marshal(w)
	return data
}
