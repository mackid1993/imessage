#!/usr/bin/env bash
# claude.sh — TPP-aware Claude Code wrapper
# Place this in your project root. The shell function in ~/.zshrc or ~/.bashrc
# will automatically use it when present.

set -euo pipefail

DATE=$(date +%Y-%m-%d)

# Build the system prompt
SYSTEM_PROMPT="$(cat <<SYSTEM
- Current date: $DATE

## TPP Workflow

This project uses Technical Project Plans (TPPs) in \`_todo/*.md\` to document
research, design decisions, implementation progress, and handoff notes between sessions.

### Key behaviors:

1. **Starting a session**: Check \`_todo/\` for active TPPs. If the user references
   a feature that has a TPP, read it first before doing any work.

2. **During work**: Keep the TPP updated as you make progress. Check off completed
   tasks, add discoveries to Lore, and record any failed approaches.

3. **When exiting plan mode**: Write or update a relevant TPP in \`_todo/\` using
   the /handoff skill to capture the plan for future sessions.

4. **When context approaches limits**: Run the /handoff skill to preserve progress
   before the session ends. Do not wait to be asked.

5. **Phase discipline**: Follow the TPP phases in order — Research, Tests, Design,
   Tasks, Implementation, Review, Integration. Do not skip phases.

6. **Tests first**: Write failing tests before implementation code when possible.

7. DO NOT UNDER ANY CIRCUMSTANCES MODIFY VENDORED RUSTPUSH

### TPP file format:
- Location: \`_todo/YYYYMMDD-feature-name.md\`
- Completed TPPs: \`_done/\`
- Guide: \`docs/TPP-GUIDE.md\`
- Max length: 400 lines
SYSTEM
)"

exec command claude --append-system-prompt "$SYSTEM_PROMPT" "$@"
