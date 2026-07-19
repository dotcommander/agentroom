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
	"strings"
	"syscall"

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
		_, _ = fmt.Fprintln(os.Stderr, "error:", terminalText(err.Error()))
		var exitErr interface{ ExitCode() int }
		if errors.As(err, &exitErr) {
			os.Exit(exitErr.ExitCode())
		}
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
	Leave      leaveCommand      `cmd:"" help:"Clear all presence entries for the current session."`
	Who        whoCommand        `cmd:"" help:"List agents currently present (role, claim load, TTL left)."`
	Lease      leaseCommand      `cmd:"" help:"Acquire, renew, release, and list enforceable resource leases."`
	Guard      guardCommand      `cmd:"" help:"Check whether resources are safe to use."`
	Window     windowCommand     `cmd:"" help:"Coordinate acknowledged exclusive quiet windows."`
	Work       workCommand       `cmd:"" help:"Publish canonical current work state."`
	Status     statusCommand     `cmd:"" help:"Show recoverable current coordination state."`
	Version    versionCommand    `cmd:"" help:"Report source and running executable identity."`
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
	app.Lease.Acquire.Agent, app.Lease.Renew.Agent, app.Lease.Release.Agent = agent, agent, agent
	app.Guard.Agent = agent
	app.Window.Request.Agent, app.Window.Ack.Agent, app.Window.Activate.Agent = agent, agent, agent
	app.Window.Release.Agent, app.Window.Cancel.Agent = agent, agent
	app.Work.Agent = agent
	parser, err := kong.New(
		&app,
		kong.Name("agentroom"),
		kong.Description("Join an agentroom event mesh: tail, post, and claim work"),
		kong.Writers(out, errOut),
		kong.Bind(&app.globals),
		kong.BindTo(ctx, (*context.Context)(nil)),
		kong.ConfigureHelp(kong.HelpOptions{
			Compact:   true,
			Tree:      true,
			Summary:   true,
			FlagsLast: true,
		}),
	)
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
