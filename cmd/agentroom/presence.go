package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/dotcommander/agentchat/agentroom"
)

// presenceLines renders the live presence set (agentID -> description, from the
// room's TTL presence keys) into the "== who's here ==" block. selfID is the
// calling session's own agent id and is omitted (you are not "someone else
// here"). Output shape is preserved: "  <id> -- <desc>" (or "  <id>" when the
// agent posted no role/working_on), and "(nobody else here)" when empty.
func presenceLines(pres map[string]string, selfID string) []string {
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
		if desc := pres[id]; desc != "" {
			lines = append(lines, fmt.Sprintf("  %s -- %s", id, desc))
		} else {
			lines = append(lines, "  "+id)
		}
	}
	if len(lines) == 0 {
		return []string{"(nobody else here)"}
	}
	return lines
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
	_ = room.Heartbeat(ctx, agentID, desc, room.Config().PresenceTTL)
}
