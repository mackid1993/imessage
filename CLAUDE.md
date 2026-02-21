# mautrix-imessage (v2) — Project Primer

## Stack

- **Go 1.24** + **Rust FFI** (rustpush via uniffi)
- **Framework**: mautrix bridgev2 (`maunium.net/go/mautrix`)
- **Logging**: zerolog (`github.com/rs/zerolog`)
- **Config**: YAML with `upgradeConfig` pattern

## Key Directories

```
cmd/mautrix-imessage/    # Entrypoint (main.go, platform setup)
pkg/connector/           # bridgev2 connector (config, login, send/receive, backfill)
pkg/rustpushgo/          # Rust FFI wrapper (uniffi bindings, Cargo workspace)
rustpush/                # Vendored OpenBubbles/rustpush + open-absinthe NAC emulator
scripts/                 # Build/install/reset helpers
tools/                   # extract-key, nac-relay
imessage/                # macOS chat.db + Contacts reader
docs/                    # Research notes
```

## Build Commands

```bash
make build           # Full build (Rust lib + Go binary/.app)
make rust            # Rust library only
make bindings        # Regenerate Go FFI bindings (needs uniffi-bindgen-go)
make install         # Build + install (self-hosted homeserver)
make install-beeper  # Build + install (Beeper)
make clean           # Remove build artifacts
```

## Key Files

- `pkg/connector/config.go` — bridge config schema
- `pkg/connector/example-config.yaml` — default config template
- `pkg/connector/connector.go` — bridge lifecycle + platform detection
- `pkg/connector/client.go` — send/receive/reactions/edits/typing
- `pkg/connector/login.go` — Apple ID + external key login flows
- `Makefile` — build system (Go + Rust + platform detection)

## Conventions

- Follow existing patterns: bridgev2 interfaces, zerolog structured logging
- Config uses YAML with `upgradeConfig` helper for migrations
- Portal/ghost IDs via helper functions in `pkg/connector/ids.go`
- Rust FFI calls go through `pkg/rustpushgo/` — never call rustpush directly from Go
- Platform-specific code uses `_darwin.go` / `_other.go` build tags

## TPP System

Check `_todo/` for active Technical Project Plans (TPPs). Use `/tpp <path>` to work on one, `/handoff` to save progress before ending a session. See `docs/TPP-GUIDE.md` for the full workflow.
