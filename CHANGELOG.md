# Changelog

## v0.3.0 (2026-07-14)

### Features

- Prevent repeated `Worker.Execute` calls after Redis acknowledgment failures
  with TTL-bounded completion receipts.

### Other

- Migrate the CLI from Cobra to Kong while preserving contextual help and
  Bash, Fish, PowerShell, and Zsh completion commands.
- Commit hook cursors only after hook output is delivered successfully.
- Refresh public installation, integration, and runtime guidance.
