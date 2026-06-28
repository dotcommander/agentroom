package main

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// gitProbeTimeout bounds the one-shot git probes used to derive room identity so
// a slow or hung git never stalls a hook or CLI command.
const gitProbeTimeout = 1 * time.Second

// resolveRoom is the single source of truth for room identity, shared by the
// hook paths and the CLI flag resolution (roomFromFlags). Precedence:
// REPO_ID/BRANCH_NAME env win; else the git repo (toplevel basename + current
// branch); else the cwd basename and "main". Outside a git repo it falls back
// gracefully with no error. git is probed lazily here, never at flag
// registration, so --help/completion stay offline.
func resolveRoom(ctx context.Context, cwd string) (string, string) {
	repo := os.Getenv("REPO_ID")
	if repo == "" {
		if top := gitOutput(ctx, cwd, "rev-parse", "--show-toplevel"); top != "" {
			repo = filepath.Base(top)
		}
	}
	if repo == "" {
		repo = filepath.Base(cwd)
	}
	branch := os.Getenv("BRANCH_NAME")
	if branch == "" {
		branch = gitOutput(ctx, cwd, "rev-parse", "--abbrev-ref", "HEAD")
	}
	if branch == "" {
		branch = defaultBranch
	}
	return repo, branch
}

// gitOutput runs a one-shot git command in cwd and returns its trimmed stdout,
// or "" if git is absent, cwd is not a repo, or the command errors/empties. A
// short timeout bounds it so a hung git never stalls a hook or CLI command.
func gitOutput(ctx context.Context, cwd string, args ...string) string {
	ctx, cancel := context.WithTimeout(ctx, gitProbeTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Dir = cwd
	out, err := cmd.Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}
