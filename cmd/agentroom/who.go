package main

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/dotcommander/agentchat/agentroom"
	"github.com/spf13/cobra"
)

// whoCmd prints the live roster on demand -- the gap the SessionStart digest
// left, since that digest only renders into the hook JSON channel and shows
// nothing when run by hand. Unlike the digest it is a pure observer: it does NOT
// heartbeat, so looking never makes you appear.
func whoCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "who",
		Short: "List agents currently present (role, claim load, TTL left)",
		Args:  cobra.NoArgs,
		RunE: func(c *cobra.Command, _ []string) error {
			room, rdb := roomFromFlags(c)
			defer func() { _ = rdb.Close() }()
			pres, err := room.PresenceDetailed(c.Context())
			if err != nil {
				return err
			}
			for _, line := range whoLines(pres, resolveAgent(c), claimsCounter(c.Context(), room)) {
				outln(line)
			}
			return nil
		},
	}
	cmd.Flags().String("agent", defaultAgent(), "this agent's id (tagged \"(you)\" in the roster)")
	return cmd
}

// whoLines renders the detailed roster: one line per live agent with its
// description (or "(no role posted)" so heartbeat-only/anonymous entries stay
// legible instead of blank), outstanding-claim count, and the time left on its
// presence TTL before it drops. Ids are column-aligned and sorted; the caller's
// own id is tagged "(you)".
func whoLines(pres map[string]agentroom.PresenceEntry, selfID string, claimsFor func(string) int) []string {
	ids := make([]string, 0, len(pres))
	width := 0
	for id := range pres {
		if isAnonymousIdle(id, pres[id].Desc) {
			continue // hide role-less default-"cli" markers: clutter, no role/event backing
		}
		ids = append(ids, id)
		if len(id) > width {
			width = len(id)
		}
	}
	if len(ids) == 0 {
		return []string{"(nobody here)"}
	}
	sort.Strings(ids)
	lines := make([]string, 0, len(ids))
	for _, id := range ids {
		e := pres[id]
		desc := e.Desc
		if desc == "" {
			desc = "(no role posted)"
		}
		line := fmt.Sprintf("%-*s  %s", width, id, desc)
		if n := claimsFor(id); n > 0 {
			line += fmt.Sprintf("  (%d claimed)", n)
		}
		line += fmt.Sprintf("  [%s left]", humanTTL(e.TTL))
		if id == selfID {
			line += "  (you)"
		}
		lines = append(lines, line)
	}
	return lines
}

// humanTTL renders a presence key's remaining TTL compactly. Non-positive means
// the key has no expiry left (about to drop), shown as "expiring".
func humanTTL(d time.Duration) string {
	if d <= 0 {
		return "expiring"
	}
	return d.Round(time.Second).String()
}

// isAnonymousIdle reports whether id is a default-label "cli" presence marker
// with no role posted — bare CLI activity (claim/tail/post) that registered
// liveness without signing in. These clutter the roster with no role or event
// backing, so `who` hides them. A role-bearing entry (Desc != ""), a live
// bare-session token, or a real chosen handle is never hidden.
func isAnonymousIdle(id, desc string) bool {
	return desc == "" && (strings.HasPrefix(id, "cli@") || strings.HasPrefix(id, "cli-"))
}
