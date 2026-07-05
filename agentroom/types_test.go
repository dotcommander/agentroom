package agentroom

import (
	"encoding/json"
	"testing"
	"time"
)

func TestConfigStreamKey(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		cfg  Config
		want string
	}{
		{"basic", Config{RepoID: "auth", BranchName: "main"}, "repo:auth:main:events"},
		{"empty", Config{}, "repo:::events"},
		{"feature", Config{RepoID: "repo-auth-service", BranchName: "feat-login"}, "repo:repo-auth-service:feat-login:events"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := tt.cfg.StreamKey(); got != tt.want {
				t.Errorf("StreamKey() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestConfigScratchpadPrefix(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		cfg  Config
		want string
	}{
		{"basic", Config{RepoID: "auth", BranchName: "main"}, "repo:auth:main:state:"},
		{"empty", Config{}, "repo:::state:"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := tt.cfg.ScratchpadPrefix(); got != tt.want {
				t.Errorf("ScratchpadPrefix() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestMatches(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		interests []string
		eventType string
		want      bool
	}{
		{"exact", []string{"AST_PARSED"}, "AST_PARSED", true},
		{"wildcard", []string{"*"}, "ANYTHING", true},
		{"no match", []string{"AST_PARSED"}, "TESTS_FAILED", false},
		{"empty interests", nil, "AST_PARSED", false},
		{"multiple with match", []string{"A", "B", "TESTS_FAILED"}, "TESTS_FAILED", true},
		{"wildcard among others", []string{"A", "*"}, "Z", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := matches(tt.interests, tt.eventType); got != tt.want {
				t.Errorf("matches(%v, %q) = %v, want %v", tt.interests, tt.eventType, got, tt.want)
			}
		})
	}
}

func TestDefaultConfig(t *testing.T) {
	t.Parallel()
	cfg := DefaultConfig()
	if cfg.RedisAddr != "localhost:6379" {
		t.Errorf("RedisAddr = %q, want localhost:6379", cfg.RedisAddr)
	}
	if cfg.StreamTTL != 48*time.Hour {
		t.Errorf("StreamTTL = %v, want 48h", cfg.StreamTTL)
	}
	if cfg.ArchiveThreshold != 10000 {
		t.Errorf("ArchiveThreshold = %d, want 10000", cfg.ArchiveThreshold)
	}
	if cfg.MaxPayloadBytes != 16*1024 {
		t.Errorf("MaxPayloadBytes = %d, want 16384", cfg.MaxPayloadBytes)
	}
	if cfg.InboxMaxLen != 1000 {
		t.Errorf("InboxMaxLen = %d, want 1000", cfg.InboxMaxLen)
	}
	if cfg.InboxTTL != 30*24*time.Hour {
		t.Errorf("InboxTTL = %v, want 720h", cfg.InboxTTL)
	}
	if cfg.InboxCursorTTL != 30*24*time.Hour {
		t.Errorf("InboxCursorTTL = %v, want 720h", cfg.InboxCursorTTL)
	}
	if cfg.Group != defaultGroup {
		t.Errorf("Group = %q, want agents", cfg.Group)
	}
}

func TestEventJSONRoundTrip(t *testing.T) {
	t.Parallel()
	in := Event{
		ID:        "1-0",
		Type:      "AST_PARSED",
		AgentID:   "engine-7",
		Payload:   json.RawMessage(`{"file":"main.go"}`),
		Timestamp: 1718000000000000000,
	}
	data, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out Event
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out.Type != in.Type || out.AgentID != in.AgentID || out.Timestamp != in.Timestamp {
		t.Errorf("round trip mismatch: got %+v want %+v", out, in)
	}
}

func TestConfigCatalogAndTaskKeys(t *testing.T) {
	t.Parallel()
	cfg := Config{RepoID: "svc", BranchName: "dev"}
	if got, want := cfg.InboxKey("gary"), "repo:svc:dev:inbox:gary"; got != want {
		t.Errorf("InboxKey() = %q, want %q", got, want)
	}
	if got, want := cfg.InboxCursorKey("gary"), "repo:svc:dev:inboxcursor:gary"; got != want {
		t.Errorf("InboxCursorKey() = %q, want %q", got, want)
	}
	if got, want := cfg.CatalogKey(), "repo:svc:dev:catalog"; got != want {
		t.Errorf("CatalogKey() = %q, want %q", got, want)
	}
	if got, want := cfg.TaskKey("t-9"), "repo:svc:dev:task:t-9"; got != want {
		t.Errorf("TaskKey() = %q, want %q", got, want)
	}
}
