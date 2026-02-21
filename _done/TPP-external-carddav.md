# TPP: External CardDAV Contact Source

## Status
- **Phase**: Review & Refinement
- **Last updated**: 2026-02-14
- **Session notes**: All implementation complete. Ready for testing and review.

## Goal
Allow users to configure an external CardDAV server (Google with app passwords, Nextcloud, Radicale, Fastmail, etc.) for contact name resolution, as an alternative to iCloud CardDAV. Features: auto-discovery (RFC 6764), encrypted password storage, contact photos, interactive setup in all install scripts.

## Research & Planning
- [x] Read existing CardDAV implementation (`pkg/connector/cloud_contacts.go`)
- [x] Read contact usage sites (`client.go`, `sync_controller.go`, `contact_merge.go`)
- [x] Read config system (`config.go`, `example-config.yaml`)
- [x] Identify all `cloudContacts.` call sites

### Findings
- `cloudContactsClient` is used via two methods: `SyncContacts()` and `GetContactInfo()`
- 7 call sites reference `c.cloudContacts` directly (not through an interface)
- The `imessage.Contact` struct is the shared data type — external CardDAV produces the same type
- The existing vCard parser and phone normalization already handle standard vCard 3.0/4.0
- Config uses `upgradeConfig` helper with `up.Copy()` for schema migration
- Google CardDAV endpoint: `https://www.googleapis.com/carddav/v1/principals/{email}/lists/default/`
- Contact photos flow through `parseVCard()` → `contact.Avatar` (base64 PHOTO fields)

## Design

### `contactSource` interface
```go
type contactSource interface {
    SyncContacts(log zerolog.Logger) error
    GetContactInfo(identifier string) (*imessage.Contact, error)
}
```

### Config
```yaml
carddav:
    email: ""              # for auto-discovery + default username
    url: ""                # leave empty for auto-discovery
    username: ""           # defaults to email
    password_encrypted: "" # AES-256-GCM, set by carddav-setup
```

### Password encryption
- AES-256-GCM with random 32-byte key stored at `$SESSION_DIR/carddav.key`
- `carddav-setup` CLI subcommand handles encryption + auto-discovery

### Auto-discovery (RFC 6764)
1. `.well-known/carddav` (HTTPS, then HTTP)
2. DNS SRV records (`_carddavs._tcp`, `_carddav._tcp`)
3. Special case: Google domains → known URL pattern

### Priority: external > iCloud
If `carddav.email` is configured, use external. Otherwise fall back to iCloud CardDAV.

## Task Breakdown

- [x] Task 1: Extract `contactSource` interface + rename `cloudContacts` → `contacts`
- [x] Task 2: Add `CardDAVConfig` to config + example-config.yaml + crypto helpers
- [x] Task 3: Implement `externalCardDAVClient` with auto-discovery
- [x] Task 3b: Add `carddav-setup` CLI subcommand
- [x] Task 4: Wire up external client in `Connect()` with fallback to iCloud
- [x] Task 5: Add CardDAV prompt to all 4 install scripts

## Files Changed

### New files:
- `pkg/connector/external_carddav.go` — external CardDAV client + RFC 6764 auto-discovery
- `pkg/connector/carddav_crypto.go` — AES-256-GCM encrypt/decrypt for password storage
- `cmd/mautrix-imessage/carddav_setup.go` — CLI subcommand for install scripts

### Modified files:
- `pkg/connector/cloud_contacts.go` — added `contactSource` interface
- `pkg/connector/config.go` — added `CardDAVConfig` struct + upgradeConfig entries
- `pkg/connector/example-config.yaml` — added `carddav:` section
- `pkg/connector/client.go` — renamed `cloudContacts` → `contacts`, external CardDAV wiring
- `pkg/connector/sync_controller.go` — renamed `cloudContacts` → `contacts`
- `pkg/connector/contact_merge.go` — renamed `cloudContacts` → `contacts`
- `scripts/install.sh` — added CardDAV prompt
- `scripts/install-linux.sh` — added CardDAV prompt
- `scripts/install-beeper.sh` — added CardDAV prompt
- `scripts/install-beeper-linux.sh` — added CardDAV prompt
- `cmd/mautrix-imessage/main.go` — added `carddav-setup` subcommand

## Implementation Log

### Session 1 — 2026-02-14
- Research phase completed, design finalized

### Session 2 — 2026-02-14
- All implementation tasks completed:
  1. Extracted `contactSource` interface, renamed field across all 7 call sites
  2. Added `CardDAVConfig` struct with email/url/username/password_encrypted fields
  3. Created `externalCardDAVClient` with HTTP Basic auth, auto-discovery (RFC 6764), contact photos
  4. Created `carddav_crypto.go` with AES-256-GCM encryption using session-dir key file
  5. Created `carddav-setup` CLI subcommand for install script integration
  6. Wired up external client in Connect() with iCloud fallback
  7. Added interactive CardDAV prompt to all 4 install scripts
- All Go code compiles clean (`go vet ./pkg/connector/` passes)
- All bash scripts pass syntax check (`bash -n`)

## Review & Refinement
- [x] Code follows project conventions (zerolog, bridgev2 interfaces, YAML config)
- [ ] No regressions in existing functionality
- [ ] Changes tested manually
- [ ] Contact photos verified working

## Final Integration
- [ ] All tasks complete
- [ ] Push to mackid1993/imessage-test branch carddav
- [ ] Move TPP to `_done/`
