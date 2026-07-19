package main

import (
	"fmt"
	"io"
	"os"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/dotcommander/agentroom/agentroom"
)

func printEvent(w io.Writer, e agentroom.Event) {
	for _, line := range eventLines(e) {
		_, _ = fmt.Fprintln(w, line)
	}
}

func eventLines(e agentroom.Event) []string {
	ts := ""
	if e.Timestamp > 0 {
		ts = time.Unix(0, e.Timestamp).Format("15:04:05")
	}
	lines := []string{fmt.Sprintf("%-16s %-8s %-16s %s", terminalText(e.ID), ts, terminalText(e.Type), terminalText(e.AgentID))}
	if e.To != "" {
		lines = append(lines, "    To: "+terminalText(e.To))
	}
	if e.ReplyTo != "" {
		lines = append(lines, "    ReplyTo: "+terminalText(e.ReplyTo))
	}
	if len(e.Payload) > 0 {
		lines = append(lines, "    "+terminalText(string(e.Payload)))
	}
	return lines
}

// terminalText removes terminal control sequences and control characters from
// untrusted room fields before they are written to a human's terminal.
func terminalText(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); {
		switch s[i] {
		case 0x1b:
			i = skipEscapeSequence(s, i+1)
			continue
		case 0x9b:
			i = skipCSI(s, i+1)
			continue
		case 0x9d:
			i = skipOSC(s, i+1)
			continue
		}
		if s[i] >= 0x80 && s[i] <= 0x9f {
			i++
			continue
		}
		r, size := rune(s[i]), 1
		if r >= utf8.RuneSelf {
			r, size = utf8.DecodeRuneInString(s[i:])
		}
		if !unicode.IsControl(r) {
			b.WriteString(s[i : i+size])
		}
		i += size
	}
	return b.String()
}

func skipEscapeSequence(s string, i int) int {
	if i >= len(s) {
		return i
	}
	switch s[i] {
	case '[':
		return skipCSI(s, i+1)
	case ']':
		return skipOSC(s, i+1)
	default:
		return i + 1
	}
}

func skipCSI(s string, i int) int {
	for ; i < len(s); i++ {
		if s[i] >= 0x40 && s[i] <= 0x7e {
			return i + 1
		}
	}
	return len(s)
}

func skipOSC(s string, i int) int {
	for ; i < len(s); i++ {
		if s[i] == 0x07 || s[i] == 0x9c {
			return i + 1
		}
		if s[i] == 0x1b && i+1 < len(s) && s[i+1] == '\\' {
			return i + 2
		}
	}
	return len(s)
}

// defaultAgent is the human label manual CLI commands use before qualification.
func defaultAgent() string {
	if v := os.Getenv("AGENTROOM_AGENT"); v != "" {
		return v
	}
	return defaultHandle
}

// sessionToken is the per-session disambiguator appended to every handle.
func sessionToken() string {
	for _, key := range []string{"AGENTROOM_SESSION_ID", "CLAUDE_SESSION_ID", "CODEX_THREAD_ID", "TERM_SESSION_ID"} {
		if id := os.Getenv(key); id != "" {
			return shortSession(id)
		}
	}
	host := "cli"
	if h, err := os.Hostname(); err == nil && h != "" {
		host = h
	}
	return fmt.Sprintf("%s-%d", host, os.Getppid())
}

// sanitizeHandle replaces characters that would corrupt Redis key structure.
func sanitizeHandle(h string) string {
	return strings.Map(func(r rune) rune {
		switch r {
		case ':', '*', '?', '[', ']', ' ', '\t', '\n', '\r':
			return '-'
		}
		return r
	}, h)
}

// qualifyAgent makes a handle collision-proof within concurrent sessions.
func qualifyAgent(handle string) string {
	h := sanitizeHandle(handle)
	tok := sessionToken()
	if h == "" {
		return tok
	}
	if h == tok || strings.HasSuffix(h, "-"+tok) {
		return h
	}
	return h + "-" + tok
}

// resolveAgent is the single source of truth for command actor identity.
func resolveAgent(raw string) string {
	if raw == "" {
		raw = defaultAgent()
	}
	return qualifyAgent(raw)
}

func eventIdentity(agentID string) (string, string) {
	return agentHandle(agentID), sessionToken()
}
