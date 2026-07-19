# Changelog

## v0.4.0 (2026-07-19)

### Features

- Add atomic multi-resource leases, collision-aware guards, and acknowledged
  quiet windows with durable current-state records.
- Add structured session identity, exact delivery targeting, canonical work
  states, filtered JSONL tail output, and materialized `status` snapshots.
- Add blocker-first hook output, session-wide presence cleanup, versioned
  presence records with legacy decoding, and auditable `version` output.

### Documentation

- Establish `docs/agent-guide.md` as the canonical operator contract and reduce
  the README to verified quick-start and reference links.

## v0.3.0 (2026-07-14)

### Features

- Prevent repeated `Worker.Execute` calls after Redis acknowledgment failures
  with TTL-bounded completion receipts.

### Other

- Migrate the CLI from Cobra to Kong while preserving contextual help and
  Bash, Fish, PowerShell, and Zsh completion commands.
- Commit hook cursors only after hook output is delivered successfully.
- Refresh public installation, integration, and runtime guidance.
