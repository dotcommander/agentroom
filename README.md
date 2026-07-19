# agentroom

`agentroom` is a Redis-backed coordination mesh for coding agents. It provides
immutable room events, durable directed inboxes, task claims, enforceable
multi-resource leases, acknowledged quiet windows, materialized work status,
TTL presence, and lifecycle hooks.

## Quick start

Requires Go 1.26+ and Redis 6.2+.

```bash
go build -o /tmp/agentroom ./cmd/agentroom

/tmp/agentroom post AGENT_JOINED \
  '{"role":"reviewer","working_on":"parser tests"}' --agent reviewer
/tmp/agentroom who
/tmp/agentroom status
```

Room coordinates come from `--addr`, `--repo`, and `--branch`, or from
`REDIS_ADDR`, `REPO_ID`, and `BRANCH_NAME`. Git repository and branch discovery
provide the defaults when explicit values are absent.

Install the CLI or library with:

```bash
go install github.com/dotcommander/agentroom/cmd/agentroom@latest
go get github.com/dotcommander/agentroom/agentroom
```

## Collision-safe coordination

Use task claims for one catalogued unit of work. Use resource leases for shared
files, databases, binaries, and services:

```bash
agentroom lease acquire path:internal/db db:clickmojo \
  --purpose "repair migration ordering" --ttl 15m
agentroom guard path:internal/db --lease <lease-id> --require-held
agentroom lease release <lease-id>
```

Use a quiet window when named peers and owners of overlapping leases must
acknowledge before an exclusive operation:

```bash
agentroom window request db:clickmojo --purpose "apply migration" --require reviewer
agentroom window ack <window-id>
agentroom window activate <window-id>
agentroom window release <window-id>
```

`agentroom status --json` is the authoritative current-state snapshot. Use
`agentroom tail` for audit history and `agentroom version --json` to identify the
running executable.

## Documentation

- [Agent guide](docs/agent-guide.md) — tasks versus leases, windows, targeting,
  work states, hooks, status, guard codes, and operational examples.
- [Integration guide](docs/integrations.md) — Claude Code, Codex, Pi, and hook
  setup.
- [Maintenance handoff](docs/handoff.md) — contributor invariants and checks.

Run `agentroom --help` for the live command surface.
