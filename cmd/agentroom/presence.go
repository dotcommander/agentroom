package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/dotcommander/agentchat/agentroom"
)

// presenceValue is the render-time view of one roster entry: the agent's free-form
// description (the raw presence-key value) plus a live claim count. The presence
// key stores only the opaque desc string; Claims is always derived at render time
// (see presenceLines), never read from storage.
const emptyRosterMsg = "(nobody else here)"

type presenceValue struct {
	Desc   string
	Claims int
}

// decodePresence wraps a stored presence value (the opaque role/working-on
// description string, possibly empty) into a presenceValue for rendering. Claims
// is filled in at render time, never parsed from storage. Never errors — presence
// rendering must not break on any value.
func decodePresence(raw string) presenceValue {
	return presenceValue{Desc: raw}
}

// presenceLines renders the live presence set (agentID -> description, from the
// room's TTL presence keys) into the "== who's here ==" block. selfID is the
// calling session's own agent id and is omitted (you are not "someone else
// here"). Output shape is preserved: "  <id> -- <desc>" (or "  <id>" when the
// agent posted no role/working_on), and "(nobody else here)" when empty.
func presenceLines(pres map[string]string, selfID string, claimsFor func(agentID string) int) []string {
	ids := make([]string, 0, len(pres))
	for id := range pres {
		if id == selfID {
			continue
		}
		ids = append(ids, id)
	}
	sort.Strings(ids)
	lines := make([]string, 0, len(ids))
	for _, id := range ids {
		v := decodePresence(pres[id])
		// claims is computed at render time (not stored) so the peer's
		// label-preserving writer stays untouched; the count is always fresh.
		v.Claims = claimsFor(id)
		lines = append(lines, presenceLine(id, v))
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
func presenceLine(id string, v presenceValue) string {
	line := "  " + id
	if v.Desc != "" {
		line += " -- " + v.Desc
	}
	if v.Claims > 0 {
		line += fmt.Sprintf(" (%d claimed)", v.Claims)
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
	// refresh the TTL without overwriting a role label set at sign-in.
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
