// Command agentroom is the interactive CLI for an agentroom event mesh: tail
// recent activity, post events, browse the task catalog, and claim/complete
// tasks. It is the interface an agent (or a human) uses to join a room. Room
// coordinates come from --addr/--repo/--branch or REDIS_ADDR/REPO_ID/BRANCH_NAME.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/alecthomas/kong"
	"github.com/dotcommander/agentroom/agentroom"
	"github.com/redis/go-redis/v9"
)

const (
	lobbyRepo           = "lobby"
	defaultBranch       = "main"
	defaultHandle       = "cli"
	defaultRepoFallback = "demo"
	helpCommandName     = "help"
	addrFlag            = "--addr"
	repoFlag            = "--repo"
	branchFlag          = "--branch"
)

func main() {
	if err := run(); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

// run wires signal-based cancellation and executes the root command. It is split
// from main so os.Exit never runs while a defer (stop) is still pending.
func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	return execute(ctx, os.Args[1:])
}

func outln(args ...any)               { _, _ = fmt.Fprintln(os.Stdout, args...) }
func outf(format string, args ...any) { _, _ = fmt.Fprintf(os.Stdout, format, args...) }

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// newRedisClient is the seam through which every CLI command and hook obtains its
// redis client. Production uses the default (a real client for --addr); tests
// override this var to inject a single shared miniredis-backed client so a
// goleak check sees one client lifecycle, not one pool per invocation.
var newRedisClient = agentroom.NewClient

// defaultRepo is the default room namespace: REPO_ID if set, else the current
// directory's basename -- so ad-hoc CLI use targets this repo's room, matching
// what the SessionStart hook derives.
func defaultRepo() string {
	if v := os.Getenv("REPO_ID"); v != "" {
		return v
	}
	if wd, err := os.Getwd(); err == nil {
		return filepath.Base(wd)
	}
	return defaultRepoFallback
}

type globals struct {
	Addr      string    `help:"Redis address"`
	Repo      string    `help:"Repo id (room namespace)."`
	Branch    string    `help:"Branch name (room namespace)."`
	Out       io.Writer `kong:"-"`
	Err       io.Writer `kong:"-"`
	RepoSet   bool      `kong:"-"`
	BranchSet bool      `kong:"-"`
}

type cli struct {
	globals
	Completion completionCommand `cmd:"" help:"Generate the autocompletion script for the specified shell."`
	Help       helpCommand       `cmd:"" help:"Help about any command."`
	Tail       tailCommand       `cmd:"" help:"Print the most recent events on the room stream."`
	Post       postCommand       `cmd:"" help:"Publish an event to the room (payload is free-form; arg or omitted)."`
	Wait       waitCommand       `cmd:"" help:"Block until the next room event arrives."`
	Ask        askCommand        `cmd:"" help:"Ask one live agent and block until its correlated reply arrives."`
	Reply      replyCommand      `cmd:"" help:"Reply to an ask event, automatically targeting the asker."`
	Catalog    catalogCommand    `cmd:"" help:"List the task types registered in this room."`
	Register   registerCommand   `cmd:"" help:"Advertise a task type in the room catalog."`
	Open       openCommand       `cmd:"" help:"List open (unclaimed, undone) tasks an agent could pick up."`
	Claim      claimCommand      `cmd:"" help:"Atomically claim a task so no other agent duplicates it."`
	Done       doneCommand       `cmd:"" help:"Mark a claimed task complete (releasing it)."`
	Leave      leaveCommand      `cmd:"" help:"Clear this agent's presence (announce you're gone now)."`
	Who        whoCommand        `cmd:"" help:"List agents currently present (role, claim load, TTL left)."`
	Hook       hookCommand       `cmd:"" help:"Handle an agent lifecycle hook event."`
	Welcome    welcomeCommand    `cmd:"" help:"Post the canonical welcome to the lobby and pin it (no expiry)."`
}

func execute(ctx context.Context, args []string) error {
	return executeWithIO(ctx, args, os.Stdout, os.Stderr)
}

func executeWithIO(ctx context.Context, args []string, out, errOut io.Writer) error {
	helpPath, helpCommandRequested := helpCommandPath(args)
	app := cli{globals: globals{
		Addr: envOr("REDIS_ADDR", "localhost:6379"), Repo: defaultRepo(), Branch: envOr("BRANCH_NAME", "main"), Out: out, Err: errOut,
	}}
	agent := defaultAgent()
	app.Tail.Agent, app.Post.Agent, app.Wait.Agent = agent, agent, agent
	app.Ask.Agent, app.Reply.Agent, app.Claim.Agent = agent, agent, agent
	app.Done.Agent, app.Leave.Agent, app.Who.Agent = agent, agent, agent
	parser, err := kong.New(&app, kong.Name("agentroom"), kong.Description("Join an agentroom event mesh: tail, post, and claim work"), kong.Writers(out, errOut), kong.Bind(&app.globals), kong.BindTo(ctx, (*context.Context)(nil)))
	if err != nil {
		return err
	}
	if helpCommandRequested {
		helpCtx, err := commandHelpContext(parser, helpPath)
		if err != nil {
			return err
		}
		return helpCtx.PrintUsage(false)
	}
	if hasHelpFlag(args) {
		helpCtx, err := kong.Trace(parser, args)
		if err != nil {
			return err
		}
		if helpCtx.Selected() == nil && helpCtx.Error != nil {
			return helpCtx.Error
		}
		return helpCtx.PrintUsage(false)
	}
	kctx, err := parser.Parse(args)
	if err != nil {
		return err
	}
	for _, arg := range args {
		app.RepoSet = app.RepoSet || arg == repoFlag || strings.HasPrefix(arg, repoFlag+"=")
		app.BranchSet = app.BranchSet || arg == branchFlag || strings.HasPrefix(arg, branchFlag+"=")
	}
	return kctx.Run(parser)
}

type helpCommand struct {
	Command []string `arg:"" optional:"" name:"command"`
}

func helpCommandPath(args []string) ([]string, bool) {
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if isGlobalValueFlag(arg) {
			i++
			continue
		}
		if isGlobalFlagAssignment(arg) {
			continue
		}
		if arg != helpCommandName {
			return nil, false
		}
		return helpTopics(args[i+1:]), true
	}
	return nil, false
}

func helpTopics(args []string) []string {
	topics := make([]string, 0, len(args))
	for i := 0; i < len(args); i++ {
		if isGlobalValueFlag(args[i]) {
			i++
			continue
		}
		if isGlobalFlagAssignment(args[i]) {
			continue
		}
		topics = append(topics, args[i])
	}
	return topics
}

func isGlobalValueFlag(arg string) bool {
	return arg == addrFlag || arg == repoFlag || arg == branchFlag
}

func isGlobalFlagAssignment(arg string) bool {
	return strings.HasPrefix(arg, addrFlag+"=") || strings.HasPrefix(arg, repoFlag+"=") || strings.HasPrefix(arg, branchFlag+"=")
}

func hasHelpFlag(args []string) bool {
	for _, arg := range args {
		if arg == "-h" || arg == "--help" {
			return true
		}
	}
	return false
}

func commandHelpContext(parser *kong.Kong, names []string) (*kong.Context, error) {
	path := []*kong.Path{{App: parser.Model, Flags: parser.Model.Flags}}
	parent := parser.Model.Node
	for _, name := range names {
		var selected *kong.Node
		for _, child := range parent.Children {
			if child.Name == name || slices.Contains(child.Aliases, name) {
				selected = child
				break
			}
		}
		if selected == nil {
			return nil, fmt.Errorf("unknown help topic %q", strings.Join(names, " "))
		}
		path = append(path, &kong.Path{Parent: parent, Command: selected, Flags: selected.Flags})
		parent = selected
	}
	return &kong.Context{Kong: parser, Path: path}, nil
}

// room builds a Room and its client from the global flags. The
// caller must close the returned client.
func (g *globals) room(ctx context.Context) (*agentroom.Room, *redis.Client) {
	addr, repo, branch := g.Addr, g.Repo, g.Branch
	// Resolve git-aware room identity lazily — never at flag registration, so
	// --help/completion stay offline (only real room commands reach here).
	// Explicit --repo/--branch always win; the registered default
	// (defaultRepo/"main") remains the fallback when git/cwd are unavailable.
	repo, branch = resolveRoomIdentity(ctx, repo, branch, g.RepoSet, g.BranchSet)
	cfg := agentroom.DefaultConfig()
	cfg.RedisAddr = addr
	cfg.RepoID = repo
	cfg.BranchName = branch
	rdb := newRedisClient(cfg.RedisAddr)
	return agentroom.NewRoom(rdb, cfg), rdb
}

func resolveRoomIdentity(ctx context.Context, repo, branch string, repoSet, branchSet bool) (string, string) {
	if repoSet && branchSet {
		return repo, branch
	}
	wd, _ := os.Getwd()
	if wd == "" {
		return repo, branch
	}
	gitRepo, gitBranch := resolveRoom(ctx, wd)
	if !repoSet {
		repo = gitRepo
	}
	if !branchSet {
		branch = gitBranch
	}
	return repo, branch
}

type tailCommand struct {
	Count int64  `default:"20" help:"Number of recent events to show."`
	Agent string `help:"Agent id to attribute presence to."`
}

func (c *tailCommand) Run(ctx context.Context, g *globals) error {
	room, rdb := g.room(ctx)
	defer func() { _ = rdb.Close() }()
	events, err := room.Recent(ctx, c.Count)
	if err != nil {
		return err
	}
	for _, e := range events {
		printEvent(e)
	}
	agent := resolveAgent(c.Agent)
	writeHeartbeat(ctx, room, agent, "")
	return nil
}

type postCommand struct {
	Type    string `arg:""`
	Payload string `arg:"" optional:""`
	Agent   string `help:"Agent id to attribute the event to."`
	To      string `help:"Directed recipient: a room key or agent handle (empty = broadcast)."`
}

func (c *postCommand) Run(ctx context.Context, g *globals) error {
	room, rdb := g.room(ctx)
	defer func() { _ = rdb.Close() }()
	agent := resolveAgent(c.Agent)
	rawTo := c.To
	to, err := resolveTarget(ctx, room, rawTo)
	if err != nil {
		return err
	}
	if rawTo != "" && to == rawTo && !strings.Contains(rawTo, ":") {
		_, _ = fmt.Fprintf(g.Err, "warning: no live agent matches --to %q; posting verbatim\n", rawTo)
	}
	inboxRecipient := durableInboxRecipient(rawTo)
	var payload []byte
	if c.Payload != "" {
		payload = []byte(c.Payload)
	}
	ev := &agentroom.Event{Type: c.Type, AgentID: agent, To: to, Payload: payload}
	if err := room.Publish(ctx, ev); err != nil {
		return err
	}
	if inboxRecipient != "" {
		if err := room.EnqueueInbox(ctx, inboxRecipient, *ev); err != nil {
			return fmt.Errorf("posted %s as %s (entry %s), but durable inbox enqueue failed: %w", ev.Type, agent, ev.ID, err)
		}
	}
	// Opportunistic heartbeat: every CLI call refreshes the agent's
	// presence TTL key — this is the heartbeat in a daemonless CLI.
	writeHeartbeat(ctx, room, agent, joinDesc(payload))
	outf("posted %s as %s (entry %s)\n", ev.Type, agent, ev.ID)
	return nil
}

type waitCommand struct {
	Agent   string        `help:"Agent id to match when --to-me is set."`
	ToMe    bool          `name:"to-me" help:"Only unblock for events directed to this agent."`
	Timeout time.Duration `help:"Maximum time to wait (0 = wait until interrupted)."`
}

func (c *waitCommand) Run(ctx context.Context, g *globals) error {
	room, rdb := g.room(ctx)
	defer func() { _ = rdb.Close() }()
	agent := resolveAgent(c.Agent)
	writeHeartbeat(ctx, room, agent, "")
	ev, err := waitForEvent(ctx, room, agent, c.ToMe, c.Timeout)
	if err != nil {
		return err
	}
	printEvent(ev)
	return nil
}

func waitForEvent(ctx context.Context, room *agentroom.Room, agent string, toMe bool, timeout time.Duration) (agentroom.Event, error) {
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	lastID, err := room.LastID(ctx)
	if err != nil {
		return agentroom.Event{}, err
	}
	for {
		events, err := room.Wait(ctx, lastID, 2*time.Second, 10)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) && timeout > 0 {
				return agentroom.Event{}, fmt.Errorf("wait timed out after %s", timeout)
			}
			return agentroom.Event{}, err
		}
		for _, ev := range events {
			lastID = ev.ID
			if toMe && ev.To != agent {
				continue
			}
			return ev, nil
		}
	}
}

// resolveTarget turns a human --to value into the live qualified roster ID when
// possible. Room keys ("repo:branch") pass through; exact roster IDs win; then a
// unique prefix match wins. Ambiguous prefixes are rejected with candidates so a
// directed post never guesses between two live agents.
func resolveTarget(ctx context.Context, room *agentroom.Room, raw string) (string, error) {
	if raw == "" || strings.Contains(raw, ":") {
		return raw, nil
	}
	target, candidates, err := matchLiveTarget(ctx, room, raw)
	if err != nil {
		return "", err
	}
	switch len(candidates) {
	case 0:
		return raw, nil
	case 1:
		return target, nil
	default:
		return "", fmt.Errorf("--to %q is ambiguous; candidates: %s", raw, strings.Join(candidates, ", "))
	}
}

func matchLiveTarget(ctx context.Context, room *agentroom.Room, raw string) (string, []string, error) {
	pres, err := room.PresenceDetailed(ctx)
	if err != nil {
		return "", nil, err
	}
	if _, ok := pres[raw]; ok {
		return raw, []string{raw}, nil
	}
	candidates := make([]string, 0)
	for id := range pres {
		if strings.HasPrefix(id, raw) {
			candidates = append(candidates, id)
		}
	}
	sort.Strings(candidates)
	if len(candidates) == 1 {
		return candidates[0], candidates, nil
	}
	return "", candidates, nil
}

func durableInboxRecipient(rawTo string) string {
	if rawTo == "" || strings.Contains(rawTo, ":") {
		return ""
	}
	return sanitizeHandle(rawTo)
}

type catalogCommand struct{}

func (*catalogCommand) Run(ctx context.Context, g *globals) error {
	room, rdb := g.room(ctx)
	defer func() { _ = rdb.Close() }()
	defs, err := room.Catalog(ctx)
	if err != nil {
		return err
	}
	if len(defs) == 0 {
		outln("(catalog is empty)")
		return nil
	}
	for _, d := range defs {
		outf("%-16s %s\n", d.Type, d.Description)
		outf("%16s produces=%s requires=%s\n", "", d.Produces, d.Requires)
		if d.Prerequisite != "" {
			outf("%16s prereq=%s\n", "", d.Prerequisite)
		}
	}
	return nil
}

type registerCommand struct {
	Type        string `arg:""`
	Description string `arg:""`
	Produces    string `help:"Event type emitted on success."`
	Requires    string `help:"Capability an agent needs to handle it."`
	Prereq      string `help:"Event type that must exist before this task may be claimed."`
}

func (c *registerCommand) Run(ctx context.Context, g *globals) error {
	room, rdb := g.room(ctx)
	defer func() { _ = rdb.Close() }()
	def := agentroom.TaskDef{Type: c.Type, Description: c.Description, Produces: c.Produces, Requires: c.Requires, Prerequisite: c.Prereq}
	if err := room.RegisterTask(ctx, def); err != nil {
		return err
	}
	outf("registered task type %s\n", def.Type)
	return nil
}

type openCommand struct {
	Count int64 `default:"50" help:"How many recent stream entries to scan (capped at 100)."`
}

func (c *openCommand) Run(ctx context.Context, g *globals) error {
	room, rdb := g.room(ctx)
	defer func() { _ = rdb.Close() }()
	tasks, err := room.OpenTasks(ctx, c.Count)
	if err != nil {
		return err
	}
	if len(tasks) == 0 {
		outln("(no open tasks)")
		return nil
	}
	for _, tk := range tasks {
		outf("%s  %-16s %s\n", tk.ID, tk.Type, tk.Payload)
	}
	return nil
}

type claimCommand struct {
	TaskID string        `arg:"" name:"task-id"`
	Agent  string        `help:"Agent id claiming the task."`
	TTL    time.Duration `default:"5m" help:"Claim lease before another agent may reclaim."`
	Force  bool          `help:"Bypass the declared prerequisite gate and claim unconditionally."`
}

func (c *claimCommand) Run(ctx context.Context, g *globals) error {
	room, rdb := g.room(ctx)
	defer func() { _ = rdb.Close() }()
	agent := resolveAgent(c.Agent)
	var (
		ok  bool
		err error
	)
	if c.Force {
		ok, err = room.Claim(ctx, c.TaskID, agent, c.TTL)
	} else {
		ok, err = room.ClaimChecked(ctx, c.TaskID, agent, c.TTL)
	}
	if err != nil {
		return err
	}
	if !ok {
		outf("task %s is already claimed or done -- skip it\n", c.TaskID)
		return nil
	}
	writeHeartbeat(ctx, room, agent, "")
	outf("claimed task %s as %s (lease %s)\n", c.TaskID, agent, c.TTL)
	return nil
}

type doneCommand struct {
	TaskID string `arg:"" name:"task-id"`
	Result string `arg:"" optional:""`
	Agent  string `help:"Agent id completing the task."`
}

func (c *doneCommand) Run(ctx context.Context, g *globals) error {
	room, rdb := g.room(ctx)
	defer func() { _ = rdb.Close() }()
	agent := resolveAgent(c.Agent)
	var result []byte
	if c.Result != "" {
		result = []byte(c.Result)
	}
	if err := room.Complete(ctx, c.TaskID, result); err != nil {
		return err
	}
	writeHeartbeat(ctx, room, agent, "")
	outf("completed task %s\n", c.TaskID)
	return nil
}

type leaveCommand struct {
	Agent string `help:"Agent id to clear presence for."`
}

func (c *leaveCommand) Run(ctx context.Context, g *globals) error {
	room, rdb := g.room(ctx)
	defer func() { _ = rdb.Close() }()
	agent := resolveAgent(c.Agent)
	if err := room.ClearPresence(ctx, agent); err != nil {
		return err
	}
	outf("cleared presence for %s\n", agent)
	return nil
}

func printEvent(e agentroom.Event) {
	ts := ""
	if e.Timestamp > 0 {
		ts = time.Unix(0, e.Timestamp).Format("15:04:05")
	}
	outf("%-16s %-8s %-16s %s\n", e.ID, ts, e.Type, e.AgentID)
	if len(e.Payload) > 0 {
		outf("    %s\n", e.Payload)
	}
}

// defaultAgent is the human label manual CLI commands attribute presence and
// task claims to BEFORE qualification. AGENTROOM_AGENT pins it explicitly;
// otherwise it is the bare "cli" label. resolveAgent/qualifyAgent then append a
// per-session token so two agents that pick the same label never share a key.
func defaultAgent() string {
	if v := os.Getenv("AGENTROOM_AGENT"); v != "" {
		return v
	}
	return defaultHandle
}

// sessionToken is the per-session disambiguator appended to every handle.
// CLAUDE_SESSION_ID (exposed to Bash tool commands by Claude Code) is preferred:
// it is stable across every call in one session and matches the token the hook
// presence path derives via shortSession. Outside a Claude session it falls back
// to <hostname>-<ppid> -- stable per terminal, distinct between concurrent shells.
func sessionToken() string {
	if id := os.Getenv("CLAUDE_SESSION_ID"); id != "" {
		return shortSession(id)
	}
	host := "cli"
	if h, err := os.Hostname(); err == nil && h != "" {
		host = h
	}
	return fmt.Sprintf("%s-%d", host, os.Getppid())
}

// sanitizeHandle replaces characters that would corrupt Redis key structure
// (':' is the key separator), plus whitespace, with '-'. Alphanumerics and
// '-' '_' '@' '.' pass through unchanged.
func sanitizeHandle(h string) string {
	return strings.Map(func(r rune) rune {
		switch r {
		case ':', '*', '?', '[', ']', ' ', '\t', '\n', '\r':
			return '-'
		}
		return r
	}, h)
}

// qualifyAgent makes a handle collision-proof: sanitize the label, then append
// the session token so two agents that pick the same name never share a key.
func qualifyAgent(handle string) string {
	h := sanitizeHandle(handle)
	tok := sessionToken()
	if h == "" {
		return tok
	}
	return h + "-" + tok
}

// resolveAgent reads the --agent flag and qualifies it: the single source of
// truth for the identity a command acts as (presence key, event attribution,
// claim owner).
func resolveAgent(raw string) string {
	if raw == "" {
		raw = defaultAgent()
	}
	return qualifyAgent(raw)
}
