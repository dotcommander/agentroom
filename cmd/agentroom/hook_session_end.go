package main

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"os"
	"strings"

	"github.com/dotcommander/agentroom/agentroom"
)

// sessionEndInput is the subset of the SessionEnd hook stdin we use.
type sessionEndInput struct {
	CWD            string `json:"cwd"`
	SessionID      string `json:"session_id"`
	TranscriptPath string `json:"transcript_path"`
}

// sessionEnd posts a SESSION_ENDED event to the local room with a best-effort
// summary (the session's opening ask + transcript size), so the next agent
// inherits what just happened. Best-effort: never fails session teardown.
func sessionEnd(ctx context.Context, addr string) error {
	var in sessionEndInput
	if raw, err := io.ReadAll(io.LimitReader(os.Stdin, 1<<20)); err == nil {
		_ = json.Unmarshal(raw, &in)
	}
	if in.CWD == "" {
		in.CWD, _ = os.Getwd()
	}
	if in.SessionID == "" {
		return nil
	}
	repo, branch := resolveRoom(ctx, in.CWD)
	ask, entries := transcriptSummary(in.TranscriptPath)

	rdb := newRedisClient(addr)
	defer func() { _ = rdb.Close() }()
	room := agentroom.NewRoom(rdb, roomCfg(addr, repo, branch))

	summary := map[string]any{summaryKeySession: shortSession(in.SessionID), "entries": entries}
	if ask != "" {
		summary["worked_on"] = ask
	}
	payload, err := json.Marshal(summary)
	if err != nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, hookOpTimeout)
	defer cancel()
	_ = room.Publish(ctx, &agentroom.Event{
		Type:    eventSessionEnded,
		AgentID: shortSession(in.SessionID),
		Payload: payload,
	})
	_ = room.ClearSessionPresence(ctx, shortSession(in.SessionID))
	return nil
}

func shortSession(id string) string {
	if id == "" {
		return "session"
	}
	if len(id) > 8 {
		return id[:8]
	}
	return id
}

// transcriptSummary returns the session's opening user ask and the number of
// transcript entries. Best-effort: ("", 0) on any problem.
func transcriptSummary(path string) (string, int) {
	if path == "" {
		return "", 0
	}
	f, err := os.Open(path) //nolint:gosec // transcript_path is supplied by the trusted Claude Code runtime
	if err != nil {
		return "", 0
	}
	defer func() { _ = f.Close() }()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	entries, ask := 0, ""
	for sc.Scan() {
		entries++
		if ask == "" {
			ask = firstUserText(sc.Bytes())
		}
	}
	return ask, entries
}

func firstUserText(line []byte) string {
	var e struct {
		Message struct {
			Role    string          `json:"role"`
			Content json.RawMessage `json:"content"`
		} `json:"message"`
	}
	if err := json.Unmarshal(line, &e); err != nil || e.Message.Role != "user" {
		return ""
	}
	return clip(extractText(e.Message.Content), 160)
}

// extractText pulls plain text from a content field that may be a raw string or
// an array of {type,text} blocks.
func extractText(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return s
	}
	var blocks []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}
	if json.Unmarshal(raw, &blocks) == nil {
		for _, b := range blocks {
			if b.Type == "text" && b.Text != "" {
				return b.Text
			}
		}
	}
	return ""
}

func clip(s string, n int) string {
	s = strings.TrimSpace(s)
	r := []rune(s)
	if len(r) > n {
		return string(r[:n]) + "..."
	}
	return s
}
