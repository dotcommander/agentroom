package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/dotcommander/agentroom/agentroom"
)

const emptyRosterMsg = "(nobody else here)"

// presenceLines renders the live presence set (agentID -> description, from the
// room's TTL presence keys) into the "== who's here ==" block. selfID is the
// calling session's own agent id and is omitted (you are not "someone else
// here"). Output shape is preserved: "  <id> -- <desc>" (or "  <id>" when the
// agent posted no role/working_on), and "(nobody else here)" when empty.
// redundantSessionKeys returns the bare "<sessionToken>" presence ids that are
// ALSO represented by a named "<handle>-<sessionToken>" entry — the same agent
// shown twice (the hook writes the bare session-token key; a manual AGENT_JOINED
// writes the named key). The roster collapses to the named entry, which carries
// the role, hiding the bare duplicate. O(n^2) over the small roster set.
func redundantSessionKeys(ids []string) map[string]struct{} {
	redundant := make(map[string]struct{})
	for _, bare := range ids {
		suffix := "-" + bare
		for _, other := range ids {
			if other != bare && strings.HasSuffix(other, suffix) {
				redundant[bare] = struct{}{}
				break
			}
		}
	}
	return redundant
}

// visibleRosterIDs returns the sorted agent ids to show in a roster: every key
// in pres except those skip reports (the caller's own id, or anonymous-idle
// markers) and the bare session keys collapsed into a named "<handle>-<token>"
// entry. It is the shared selection behind both the SessionStart digest
// (presenceLines) and the `who` command (whoLines), so the dedup/sort policy
// lives in one place instead of drifting between two copies.
func visibleRosterIDs[V any](pres map[string]V, skip func(id string, v V) bool) []string {
	all := make([]string, 0, len(pres))
	for id := range pres {
		all = append(all, id)
	}
	redundant := redundantSessionKeys(all)
	ids := make([]string, 0, len(pres))
	for id := range pres {
		if skip(id, pres[id]) {
			continue
		}
		if _, dup := redundant[id]; dup {
			continue // bare session key collapsed into its named "<handle>-<token>" entry
		}
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func presenceLines(pres map[string]string, selfID string, claimsFor func(agentID string) int) []string {
	ids := visibleRosterIDs(pres, func(id, _ string) bool { return id == selfID })
	lines := make([]string, 0, len(ids))
	for _, id := range ids {
		// claims is computed at render time (not stored) so the peer's
		// label-preserving writer stays untouched; the count is always fresh.
		lines = append(lines, presenceLine(id, pres[id], claimsFor(id)))
	}
	if len(lines) == 0 {
		return []string{emptyRosterMsg}
	}
	return lines
}

// presenceLine renders one roster entry: "  <id>", "  <id> -- <desc>", and a
// trailing " (<N> claimed)" capacity hint when the agent holds outstanding
// claims. The suffix is omitted when N == 0 (or unknown), preserving the
// desc-only and id-only shapes for agents with no current load.
func presenceLine(id, desc string, claims int) string {
	line := "  " + id
	if desc != "" {
		line += " -- " + desc
	}
	if claims > 0 {
		line += fmt.Sprintf(" (%d claimed)", claims)
	}
	return line
}

// joinDesc renders an AGENT_JOINED payload as "role: working_on" (either may be
// absent). Returns "" if neither field is present. It is the presence
// description an agent advertises; reused as the heartbeat value.
func joinDesc(p []byte) string {
	var j struct {
		Role      string `json:"role"`
		WorkingOn string `json:"working_on"`
	}
	if json.Unmarshal(p, &j) != nil {
		return ""
	}
	switch {
	case j.Role != "" && j.WorkingOn != "":
		return clip(j.Role+": "+j.WorkingOn, 160)
	case j.Role != "":
		return clip(j.Role, 160)
	default:
		return clip(j.WorkingOn, 160)
	}
}

// writeHeartbeat best-effort refreshes the agent's TTL presence record with desc
// (role / working_on). Called on join and on every CLI invocation; failures are
// ignored so presence never blocks a command. TTL comes from the room config.
func writeHeartbeat(ctx context.Context, room *agentroom.Room, agentID, desc string) {
	if agentID == "" {
		return
	}
	// Empty desc means "just refresh liveness" (claim/tail/non-JOINED post):
	// refresh the TTL without overwriting a role label or inferred prompt label.
	if desc == "" {
		_ = room.RefreshPresence(ctx, agentID, room.Config().PresenceTTL)
		return
	}
	_ = room.Heartbeat(ctx, agentID, desc, room.Config().PresenceTTL)
}

// claimsCounter returns a render-time claim-count lookup over room: for each
// agent it reports OutstandingClaims (claimed-but-not-done tasks), the live
// load signal shown as "(N claimed)". Errors degrade to 0 — the digest must
// never fail on a count lookup. Kept here (not hook.go) to hold hook.go under
// its line tripwire.
func claimsCounter(ctx context.Context, room *agentroom.Room) func(agentID string) int {
	return func(agentID string) int {
		n, err := room.OutstandingClaims(ctx, agentID)
		if err != nil {
			return 0
		}
		return n
	}
}
