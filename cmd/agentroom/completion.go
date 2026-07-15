package main

import (
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/alecthomas/kong"
)

type completionCommand struct {
	Bash       bashCompletion       `cmd:"" help:"Generate the autocompletion script for bash."`
	Fish       fishCompletion       `cmd:"" help:"Generate the autocompletion script for fish."`
	PowerShell powershellCompletion `cmd:"" name:"powershell" help:"Generate the autocompletion script for PowerShell."`
	Zsh        zshCompletion        `cmd:"" help:"Generate the autocompletion script for zsh."`
}

type bashCompletion struct{}
type fishCompletion struct{}
type powershellCompletion struct{}
type zshCompletion struct{}

func (*bashCompletion) Run(g *globals, parser *kong.Kong) error {
	return writeBashCompletion(g.Out, completionWords(parser.Model))
}

func (*fishCompletion) Run(g *globals, parser *kong.Kong) error {
	return writeFishCompletion(g.Out, completionWords(parser.Model))
}

func (*powershellCompletion) Run(g *globals, parser *kong.Kong) error {
	return writePowerShellCompletion(g.Out, completionWords(parser.Model))
}

func (*zshCompletion) Run(g *globals, parser *kong.Kong) error {
	return writeZshCompletion(g.Out, completionWords(parser.Model))
}

// completionWords derives candidates from Kong's live command model so the
// generated scripts cannot drift from command and flag definitions.
func completionWords(app *kong.Application) map[string][]string {
	words := map[string][]string{}
	var walk func(*kong.Node, string)
	walk = func(node *kong.Node, path string) {
		candidates := make([]string, 0, len(node.Children)+8)
		for _, child := range node.Children {
			if child.Hidden {
				continue
			}
			candidates = append(candidates, child.Name)
			childPath := strings.TrimSpace(path + " " + child.Name)
			walk(child, childPath)
		}
		for _, group := range node.AllFlags(true) {
			for _, flag := range group {
				candidates = append(candidates, "--"+flag.Name)
				if flag.Short != 0 {
					candidates = append(candidates, "-"+string(flag.Short))
				}
			}
		}
		for _, positional := range node.Positional {
			candidates = append(candidates, positional.EnumSlice()...)
		}
		words[path] = sortedUnique(candidates)
	}
	walk(app.Node, "")
	return words
}

func sortedUnique(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if value != "" {
			seen[value] = struct{}{}
		}
	}
	values = values[:0]
	for value := range seen {
		values = append(values, value)
	}
	sort.Strings(values)
	return values
}

func completionPaths(words map[string][]string) []string {
	paths := make([]string, 0, len(words))
	for path := range words {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	return paths
}

func writeCompletionCases(w io.Writer, words map[string][]string, render func(string, []string) string) error {
	for _, path := range completionPaths(words) {
		if _, err := io.WriteString(w, render(path, words[path])); err != nil {
			return err
		}
	}
	return nil
}

func writeStructuredCompletion(w io.Writer, words map[string][]string, header, footer string, render func(string, []string) string) error {
	if _, err := io.WriteString(w, header); err != nil {
		return err
	}
	if err := writeCompletionCases(w, words, render); err != nil {
		return err
	}
	_, err := io.WriteString(w, footer)
	return err
}

func writeBashCompletion(w io.Writer, words map[string][]string) error {
	if _, err := io.WriteString(w, `# bash completion for agentroom
_agentroom_complete() {
	local cur path candidates word
	local skip=0
  cur="${COMP_WORDS[COMP_CWORD]}"
  path=""
	for ((i=1; i<COMP_CWORD; i++)); do
		word="${COMP_WORDS[i]}"
		if (( skip )); then skip=0; continue; fi
		case "$word" in
			--addr|--repo|--branch) skip=1; continue ;;
			--addr=*|--repo=*|--branch=*|-*) continue ;;
		esac
		if [[ -z "$path" ]]; then path="$word";
		elif [[ "$path" == "completion" ]]; then path="$path $word"; fi
	done
  case "$path" in
`); err != nil {
		return err
	}
	for _, path := range completionPaths(words) {
		if _, err := fmt.Fprintf(w, "    %q) candidates=%q ;;\n", path, strings.Join(words[path], " ")); err != nil {
			return err
		}
	}
	_, err := io.WriteString(w, `  esac
  COMPREPLY=( $(compgen -W "$candidates" -- "$cur") )
}
complete -F _agentroom_complete agentroom
`)
	return err
}

func writeFishCompletion(w io.Writer, words map[string][]string) error {
	if _, err := io.WriteString(w, `# fish completion for agentroom
function __agentroom_complete
  set -l tokens (commandline -opc)
  set -l path ''
	set -l skip 0
	for token in $tokens[2..-1]
		if test $skip -eq 1; set skip 0; continue; end
		if contains -- $token --addr --repo --branch; set skip 1; continue; end
		if string match -qr '^--(addr|repo|branch)=' -- $token; continue; end
		if string match -qr '^-' -- $token; continue; end
		if test -z "$path"; set path $token
		else if test "$path" = completion; set path "$path $token"; end
	end
  switch "$path"
`); err != nil {
		return err
	}
	for _, path := range completionPaths(words) {
		if _, err := fmt.Fprintf(w, "    case %q\n      printf '%%s\\n' %s\n", path, strings.Join(words[path], " ")); err != nil {
			return err
		}
	}
	_, err := io.WriteString(w, `  end
end
complete -c agentroom -f -a '(__agentroom_complete)'
`)
	return err
}

func writePowerShellCompletion(w io.Writer, words map[string][]string) error {
	return writeStructuredCompletion(w, words, `# PowerShell completion for agentroom
Register-ArgumentCompleter -Native -CommandName agentroom -ScriptBlock {
  param($wordToComplete, $commandAst, $cursorPosition)
  $tokens = @($commandAst.CommandElements | ForEach-Object { $_.Extent.Text })
	$path = ''
	$skip = $false
	for ($i = 1; $i -lt $tokens.Count; $i++) {
		$token = $tokens[$i]
		if ($skip) { $skip = $false; continue }
		if ($token -in @('--addr', '--repo', '--branch')) { $skip = $true; continue }
		if ($token -match '^--(addr|repo|branch)=' -or $token.StartsWith('-')) { continue }
		if (-not $path) { $path = $token }
		elseif ($path -eq 'completion') { $path = "$path $token" }
	}
  $candidates = switch ($path) {
`, `  }
  $candidates | Where-Object { $_ -like "$wordToComplete*" } | ForEach-Object {
    [System.Management.Automation.CompletionResult]::new($_, $_, 'ParameterValue', $_)
  }
}
`, func(path string, candidates []string) string {
		quoted := make([]string, len(candidates))
		for i, word := range candidates {
			quoted[i] = "'" + strings.ReplaceAll(word, "'", "''") + "'"
		}
		return fmt.Sprintf("    %q { @(%s) }\n", path, strings.Join(quoted, ", "))
	})
}

func writeZshCompletion(w io.Writer, words map[string][]string) error {
	return writeStructuredCompletion(w, words, `#compdef agentroom
_agentroom() {
  local path
  local -a candidates
	local word
	local skip=0
  path=""
	for ((i=2; i<CURRENT; i++)); do
		word="${words[i]}"
		if (( skip )); then skip=0; continue; fi
		case "$word" in
			--addr|--repo|--branch) skip=1; continue ;;
			--addr=*|--repo=*|--branch=*|-*) continue ;;
		esac
		if [[ -z "$path" ]]; then path="$word";
		elif [[ "$path" == "completion" ]]; then path="$path $word"; fi
	done
  case "$path" in
`, `  esac
  compadd -- $candidates
}
compdef _agentroom agentroom
`, func(path string, candidates []string) string {
		quoted := make([]string, len(candidates))
		for i, word := range candidates {
			quoted[i] = "'" + strings.ReplaceAll(word, "'", "'\\''") + "'"
		}
		return fmt.Sprintf("    %q) candidates=(%s) ;;\n", path, strings.Join(quoted, " "))
	})
}
