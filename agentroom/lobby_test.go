package agentroom

import "testing"

func TestFilterLobby(t *testing.T) {
	t.Parallel()
	const self = "sess1234"
	const room = "auth:main"

	events := []Event{
		{Type: "AGENT_JOINED", AgentID: "peer-other"},
		{Type: "SESSION_ENDED", AgentID: "peer-other"},
		{Type: "WELCOME", AgentID: "concierge"},
		{Type: "PING", AgentID: "peer-other"},
		{Type: "CONFIG_CHANGED", AgentID: "peer-other"},
		{Type: "MSG", AgentID: self},
		{Type: "MSG", AgentID: "alice-" + self},
		{Type: "MSG", AgentID: "peer-other", To: room},
		{Type: "MSG", AgentID: "peer-other", To: self},
		{Type: "MSG", AgentID: "peer-other", To: "bob-" + self},
		{Type: "MSG", AgentID: "peer-other", To: "other:main"},
		{Type: "MSG", AgentID: "peer-other", To: "bob-othersess"},
	}

	got := FilterLobby(events, self, room, 0) // uncapped
	if len(got) != 5 {
		t.Fatalf("want 5 signal events, got %d: %+v", len(got), got)
	}
	for _, ev := range got {
		if lobbyExcludedTypes[ev.Type] {
			t.Errorf("excluded type leaked through: %s", ev.Type)
		}
		if isSelf(ev.AgentID, self) {
			t.Errorf("own post leaked through: %s", ev.AgentID)
		}
		if ev.To != "" && ev.To != room && !isSelf(ev.To, self) {
			t.Errorf("foreign directed message leaked through: To=%s", ev.To)
		}
	}

	// Cap keeps the most recent.
	capped := FilterLobby(events, self, room, 2)
	if len(capped) != 2 {
		t.Fatalf("want 2 after cap, got %d", len(capped))
	}
	if capped[1].To != "bob-"+self {
		t.Errorf("cap should keep the most recent kept event, got To=%s", capped[1].To)
	}
}

func TestIsSelf(t *testing.T) {
	t.Parallel()
	cases := []struct {
		id, self string
		want     bool
	}{
		{"sess1234", "sess1234", true},
		{"alice-sess1234", "sess1234", true},
		{"sess1234", "other", false},
		{"alice-othersess", "sess1234", false},
		{"anything", "", false},
	}
	for _, c := range cases {
		if got := isSelf(c.id, c.self); got != c.want {
			t.Errorf("isSelf(%q,%q)=%v want %v", c.id, c.self, got, c.want)
		}
	}
}
