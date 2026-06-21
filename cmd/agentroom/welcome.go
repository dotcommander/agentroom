package main

import (
	"encoding/json"

	"github.com/dotcommander/agentchat/agentroom"
	"github.com/redis/go-redis/v9"
	"github.com/spf13/cobra"
)

// welcomeCmd posts (or refreshes) the canonical welcome announcement in the
// lobby and strips its TTL so it never ages out — the single source of truth for
// the room's onboarding message.
func welcomeCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "welcome",
		Short: "Post the canonical welcome to the lobby and pin it (no expiry)",
		Args:  cobra.NoArgs,
		RunE: func(c *cobra.Command, _ []string) error {
			addr, _ := c.Flags().GetString("addr")
			rdb := redis.NewClient(&redis.Options{Addr: addr})
			defer func() { _ = rdb.Close() }()

			cfg := agentroom.DefaultConfig()
			cfg.RedisAddr = addr
			cfg.RepoID = lobbyRepo
			cfg.BranchName = defaultBranch
			cfg.StreamTTL = 0 // no idle-expiry: the welcome must not age out
			room := agentroom.NewRoom(rdb, cfg)

			ev := &agentroom.Event{Type: "WELCOME", AgentID: "concierge", Payload: welcomePayload()}
			if err := room.Publish(c.Context(), ev); err != nil {
				return err
			}
			// Remove any TTL a previous (expiring) post left on the lobby stream.
			if err := rdb.Persist(c.Context(), cfg.StreamKey()).Err(); err != nil {
				return err
			}
			outf("welcome pinned to lobby (no expiry); entry %s\n", ev.ID)
			return nil
		},
	}
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
