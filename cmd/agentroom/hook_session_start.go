package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/dotcommander/agentroom/agentroom"
	"github.com/redis/go-redis/v9"
)

const emptySectionMsg = "(none)"

// sessionStart reads the SessionStart payload, builds a digest of lobby + local
// room activity, and emits it as additionalContext. It NEVER fails the session:
// any error (redis down, bad input) yields no output and exit 0.
func sessionStart(ctx context.Context, addr string, out io.Writer) error {
	in := readSessionInput()
	if in.SessionID == "" {
		return nil
	}
	repo, branch := resolveRoom(ctx, in.CWD)
	ref := roomRef{Addr: addr, Repo: repo, Branch: branch}
	selfID := shortSession(in.SessionID)
	rdb := newRedisClient(addr)
	defer func() { _ = rdb.Close() }()
	joinLobby(ctx, rdb, ref, in.SessionID)
	writeLocalHeartbeat(ctx, rdb, ref, selfID)
	localSeed := prepareSeedRoomCursor(ctx, agentroom.NewRoom(rdb, roomCfg(ref.Addr, ref.Repo, ref.Branch)), in.SessionID)
	lobbySeed := prepareSeedRoomCursor(ctx, lobbyRoom(rdb, addr), in.SessionID)
	digest := prepareBuildDigest(ctx, rdb, ref, selfID)
	prepared := []preparedSection{localSeed, lobbySeed, digest}
	if digest.text == "" {
		commitHookSections(ctx, prepared...)
		return nil
	}
	if !writeHookOutput(out, "SessionStart", digest.text) {
		return nil
	}
	commitHookSections(ctx, prepared...)
	return nil
}

func readSessionInput() sessionStartInput {
	var in sessionStartInput
	if raw, err := io.ReadAll(io.LimitReader(os.Stdin, 1<<20)); err == nil {
		_ = json.Unmarshal(raw, &in)
	}
	if in.CWD == "" {
		in.CWD, _ = os.Getwd()
	}
	return in
}

// buildDigest reports who is currently present in the local room (live, from TTL
// presence keys) plus open tasks. Returns "" if redis is unreachable
// so the session is never blocked. The noisy lobby and raw recent-activity
// feeds are intentionally omitted — use `agentroom tail` for the full feed.
func buildDigest(ctx context.Context, rdb *redis.Client, ref roomRef, selfID string) string {
	prepared := prepareBuildDigest(ctx, rdb, ref, selfID)
	commitHookSections(ctx, prepared)
	return prepared.text
}

func prepareBuildDigest(ctx context.Context, rdb *redis.Client, ref roomRef, selfID string) preparedSection {
	local := agentroom.NewRoom(rdb, roomCfg(ref.Addr, ref.Repo, ref.Branch))

	ctx, cancel := context.WithTimeout(ctx, hookOpTimeout)
	defer cancel()

	// Presence is liveness-backed: read the live TTL key set, not a fold of the
	// event stream. Crashed agents drop within PresenceTTL with no SESSION_ENDED.
	pres, err := local.Presence(ctx)
	if err != nil {
		return preparedSection{}
	}
	open, _ := local.OpenTasks(ctx, 50)
	windows, _ := local.Windows(ctx)
	leases, _ := local.ResourceLeases(ctx)

	lines := []string{fmt.Sprintf("agentroom -- shared agent mesh (this room: %s:%s)", ref.Repo, ref.Branch)}
	lines = append(lines, "", "== active or pending quiet windows ==")
	lines = append(lines, windowDigestLines(windows)...)
	lines = append(lines, "", "== active resource leases ==")
	lines = append(lines, leaseDigestLines(leases)...)
	recipients := inboxRecipientsForSession(ctx, local, selfID)
	inbox, _ := prepareInboxDelta(ctx, local, recipients)
	if inbox.text != "" {
		lines = append(lines, "", inbox.text)
	}
	lines = append(lines, "", "== claimable tasks ==")
	lines = append(lines, openLines(open)...)
	lines = append(lines, "", "== who's here (live TTL presence; absence is not proof nobody's working) ==")
	lines = append(lines, presenceLines(pres, selfID, claimsCounter(ctx, local))...)
	lines = append(lines, "", "Use `agentroom claim <id>` before editing claimed work; `agentroom done <id>` when finished; `agentroom tail` for the full feed.")
	return preparedSection{
		text:   strings.Join(lines, "\n"),
		commit: inbox.commit,
	}
}

func windowDigestLines(windows []agentroom.CoordinationWindow) []string {
	lines := make([]string, 0, len(windows))
	for _, window := range windows {
		if window.State != coordinationWindowPending && window.State != coordinationWindowActive {
			continue
		}
		lines = append(lines, terminalText(fmt.Sprintf("  %s  %s  %s  %v", window.ID, window.State, window.Owner, window.Resources)))
	}
	if len(lines) == 0 {
		return []string{emptySectionMsg}
	}
	return lines
}

func leaseDigestLines(leases []agentroom.ResourceLease) []string {
	if len(leases) == 0 {
		return []string{emptySectionMsg}
	}
	lines := make([]string, 0, len(leases))
	for _, lease := range leases {
		lines = append(lines, terminalText(fmt.Sprintf("  %s  %s  %v", lease.ID, lease.Owner, lease.Resources)))
	}
	return lines
}

// joinLobby posts a best-effort AGENT_JOINED to the global lobby room so every
// session is visible cross-repo, recording which local room it belongs to. It
// publishes with StreamTTL=0 so it never re-arms expiry on the persistent lobby
// stream (the welcome banner relies on that). Never fails the session.
func joinLobby(ctx context.Context, rdb *redis.Client, ref roomRef, sessionID string) {
	cfg := roomCfg(ref.Addr, lobbyRepo, defaultBranch)
	cfg.StreamTTL = 0
	lobby := agentroom.NewRoom(rdb, cfg)
	payload, err := json.Marshal(map[string]any{"room": fmt.Sprintf("%s:%s", ref.Repo, ref.Branch)})
	if err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, hookOpTimeout)
	defer cancel()
	_ = lobby.Publish(ctx, &agentroom.Event{
		Type:    eventAgentJoined,
		AgentID: shortSession(sessionID),
		Payload: payload,
	})
}

// writeLocalHeartbeat best-effort registers this session's presence in the local
// room with a TTL key, so it appears in "who's here" without depending on the
// event fold. The description starts empty; a later `post AGENT_JOINED` refreshes
// it with role/working_on. Never fails the session.
func writeLocalHeartbeat(ctx context.Context, rdb *redis.Client, ref roomRef, agentID string) {
	local := agentroom.NewRoom(rdb, roomCfg(ref.Addr, ref.Repo, ref.Branch))
	ctx, cancel := context.WithTimeout(ctx, hookOpTimeout)
	defer cancel()
	writeHeartbeat(ctx, local, agentID, "")
}

func openLines(tasks []agentroom.Task) []string {
	if len(tasks) == 0 {
		return []string{emptySectionMsg}
	}
	lines := make([]string, 0, len(tasks))
	for _, t := range tasks {
		lines = append(lines, fmt.Sprintf("  %s  %s", t.ID, t.Type))
	}
	return lines
}

func roomCfg(addr, repo, branch string) agentroom.Config {
	cfg := agentroom.DefaultConfig()
	cfg.RedisAddr = addr
	cfg.RepoID = repo
	cfg.BranchName = branch
	return cfg
}
