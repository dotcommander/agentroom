package agentroom

import "strings"

// lobbyExcludedTypes are the low-signal lobby event types the cross-repo delta
// drops: join spam, session teardown, and the pinned onboarding welcome. Every
// other type — including ones agents invent — is treated as potential cross-repo
// signal, preserving the lobby's emergent, protocol-less design.
var lobbyExcludedTypes = map[string]bool{
	"AGENT_JOINED":  true,
	"SESSION_ENDED": true,
	"WELCOME":       true,
}

// FilterLobby reduces raw lobby events to the cross-repo signal relevant to one
// session, preserving chronological (oldest-first) order and capping to max
// (max <= 0 = uncapped; when capped it keeps the most recent max). Three rules,
// in order:
//
//  1. drop low-signal presence/onboarding types (lobbyExcludedTypes);
//  2. suppress this session's own posts — AgentID == selfID, or the
//     "<handle>-<selfID>" form a qualified CLI handle produces;
//  3. keep an event only if it is a broadcast (To == "") or directed at this
//     session: To == ownRoom ("repo:branch"), To == selfID, or the
//     "<handle>-<selfID>" handle form. Directed messages to other agents or
//     rooms are dropped.
//
// The hook cannot know the session's human-chosen handle, but every handle is
// qualified with the session token (== selfID), so the "-"+selfID suffix match
// covers handle-addressed messages without it.
func FilterLobby(events []Event, selfID, ownRoom string, max int) []Event {
	out := make([]Event, 0, len(events))
	for _, ev := range events {
		if lobbyExcludedTypes[ev.Type] {
			continue
		}
		if isSelf(ev.AgentID, selfID) {
			continue
		}
		if ev.To != "" && ev.To != ownRoom && !isSelf(ev.To, selfID) {
			continue // directed at someone else
		}
		out = append(out, ev)
	}
	if max > 0 && len(out) > max {
		out = out[len(out)-max:] // keep the most recent
	}
	return out
}

// isSelf reports whether id refers to this session: an exact match on the session
// token, or a "<handle>-<token>" qualified handle ending in it.
func isSelf(id, selfID string) bool {
	if selfID == "" {
		return false
	}
	return id == selfID || strings.HasSuffix(id, "-"+selfID)
}
