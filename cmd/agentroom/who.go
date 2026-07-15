package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/dotcommander/agentroom/agentroom"
)

const ttlExpiring = "expiring"

// whoCmd prints the live roster on demand -- the gap the SessionStart digest
// left, since that digest only renders into the hook JSON channel and shows
// nothing when run by hand. Unlike the digest it is a pure observer: it does NOT
// heartbeat, so looking never makes you appear.
type whoCommand struct {
	Agent string `help:"This agent's id (tagged (you) in the roster)."`
}

func (c *whoCommand) Run(ctx context.Context, g *globals) error {
	room, rdb := g.room(ctx)
	defer func() { _ = rdb.Close() }()
	pres, err := room.PresenceDetailed(ctx)
	if err != nil {
		return err
	}
	for _, line := range whoLines(pres, resolveAgent(c.Agent), claimsCounter(ctx, room)) {
		outln(line)
	}
	return nil
}

// whoLines renders the detailed roster: one line per live agent with its
// description (or "(no role posted)" so heartbeat-only/anonymous entries stay
// legible instead of blank), outstanding-claim count, and the time left on its
// presence TTL before it drops. Ids are column-aligned and sorted; the caller's
// own id is tagged "(you)".
func whoLines(pres map[string]agentroom.PresenceEntry, selfID string, claimsFor func(string) int) []string {
	ids := visibleRosterIDs(pres, func(id string, e agentroom.PresenceEntry) bool {
		return isAnonymousIdle(id, e.Desc)
	})
	if len(ids) == 0 {
		return []string{"(nobody here)"}
	}
	width := 0
	for _, id := range ids {
		if len(id) > width {
			width = len(id)
		}
	}
	lines := make([]string, 0, len(ids))
	for _, id := range ids {
		lines = append(lines, whoLine(id, pres[id], width, claimsFor(id), id == selfID))
	}
	return lines
}

// whoLine renders one roster row: id left-padded to width, role/description
// (or "(no role posted)" when blank), an optional claim count, the remaining
// presence TTL, and a "(you)" tag when isSelf.
func whoLine(id string, e agentroom.PresenceEntry, width, claims int, isSelf bool) string {
	desc := e.Desc
	if desc == "" {
		desc = "(no role posted)"
	}
	line := fmt.Sprintf("%-*s  %s", width, id, desc)
	if claims > 0 {
		line += fmt.Sprintf("  (%d claimed)", claims)
	}
	line += fmt.Sprintf("  [%s left]", humanTTL(e.TTL))
	if isSelf {
		line += "  (you)"
	}
	return line
}

// humanTTL renders a presence key's remaining TTL compactly. Non-positive means
// the key has no expiry left (about to drop), shown as "expiring".
func humanTTL(d time.Duration) string {
	if d <= 0 {
		return ttlExpiring
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
