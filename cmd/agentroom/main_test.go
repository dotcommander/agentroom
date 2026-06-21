package main

import "testing"

func TestRootCmdHasSubcommands(t *testing.T) {
	t.Parallel()
	root := rootCmd()
	got := map[string]bool{}
	for _, c := range root.Commands() {
		got[c.Name()] = true
	}
	for _, name := range []string{"tail", "post", "catalog", "register", "open", "claim", "done", "hook"} {
		if !got[name] {
			t.Errorf("missing subcommand %q", name)
		}
	}
}
