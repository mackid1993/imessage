# TPP: APNs → CloudKit Reliability

## Status
- **Phase**: Phase 4 — Fix remaining splintering (DMs + groups)
- **Last updated**: 2026-02-21 (Session 4 research)
- **Branch**: `master` on `mackid1993/imessage-test`
- **Next step**: Implement 4 fixes below

---

## Tribal Knowledge (READ FIRST)

- **NEVER modify `rustpush/` (vendored)**. All Rust changes go in `pkg/rustpushgo/src/lib.rs`.
- `save_messages()` / `save_chats()` / `save_attachments()` already exist in `rustpush/src/imessage/cloud_messages.rs`.
- `CloudMessage` is PCS-encrypted (`MessageEncryptedV3` record type) in `messageManateeZone`. Encryption/batching is handled automatically by `save_records()`.
- `get_or_init_cloud_messages_client()` in lib.rs lazy-inits the CloudKit client and caches it. Thread-safe.
- Incremental CloudKit sync costs ~100-500ms when no changes (3 FFI calls, continuation tokens).
- Apple epoch for CloudKit timestamps: 2001-01-01 00:00:00 UTC.
- `MessageFlags` bitfield: minimum for a persisted message is `IS_FINISHED | IS_SENT`.
- Stored messages (`is_stored_message=true`) are reconnect re-deliveries already in CloudKit — skip CloudKit write.
- **APNs `sender_guid` ≠ CloudKit `gid`** for the same group. Different UUIDs, same conversation.
- **APNs `sender_guid` is set on DMs TOO** — it's a UUID for every conversation, not just groups.
- **`cv_name` (group name)**: Apple sets `Some("")` on DMs AND some group APNs payloads. Empty string ≠ nil. `makePortalKey` must check `*groupName != ""`, not just `groupName != nil`.
- **Participant matching is unreliable** for portal resolution: Apple uses different identifiers (phone vs email) for the same person across CloudKit and APNs.
- **CloudKit is the source of truth** for group portal creation. The APNs path must NEVER create `gid:` portals.
- **`gidAliases` map is in-memory only** — lost on restart. Must be persisted for cross-restart survival.
- **Session 3 failed approach**: Moving DM sender resolution before UUID in `resolveConversationID` broke groups because group messages also have a sender with a DM portal. Reverted.

---

## Root Cause: Remaining Splintering (Session 4 Finding)

The Rust `message_inst_to_cloud_message()` (lib.rs:1266) uses `sender_guid` as `chat_id` for ALL conversations:
```rust
let chat_id = if let Some(ref sender_guid) = conv.sender_guid {
    sender_guid.clone()  // ← Used for BOTH DMs and groups!
```

When CloudKit sync later processes these messages, `resolveConversationID` (sync_controller.go:1264) calls `getChatPortalID(sender_guid)`. This searches `cloud_chat` by `cloud_chat_id`, `record_name`, and `group_id`. Since `sender_guid ≠ group_id` for both DMs and groups, the lookup fails. The UUID falls through to `gid:<sender_guid>` — creating a splintered portal.

### DM flow (splintered):
1. APNs DM arrives: `sender_guid = UUID_B`
2. Rust writes to CloudKit: `chat_id = UUID_B` (wrong — should be `iMessage;-;+number`)
3. CloudKit sync: `getChatPortalID("UUID_B")` → no match (DM group_id = UUID_A ≠ UUID_B)
4. Falls through UUID pattern → `gid:UUID_B` (DM treated as group!)
5. Portal `gid:UUID_B` created alongside correct `tel:+number` portal → **splinter**

### Group flow (splintered):
1. APNs group msg: `sender_guid = UUID_B`
2. Rust writes to CloudKit: `chat_id = UUID_B` (CloudKit uses `group_id = UUID_A`)
3. CloudKit sync: `getChatPortalID("UUID_B")` → no match
4. Falls through → `gid:UUID_B` (different from correct `gid:UUID_A`)
5. Two portals for same group → **splinter**

---

## Phase 4 Tasks

### 4.1: Rust — DM chat_id fix ☐
**File**: `pkg/rustpushgo/src/lib.rs` — `message_inst_to_cloud_message()` line ~1266

Only use `sender_guid` for groups (>2 participants or non-empty `cv_name`). For DMs, use structured `service;-;remote` format. This matches Apple's native iCloud message format.

```rust
let is_group = conv.participants.len() > 2
    || conv.cv_name.as_ref().map_or(false, |n| !n.is_empty());

let chat_id = if is_group {
    if let Some(ref sg) = conv.sender_guid { sg.clone() } else { return None; }
} else {
    let remote = conv.participants.iter()
        .find(|p| !our_handles.contains(p))
        .or_else(|| conv.participants.first())?;
    format!("{};-;{}", service_str, remote)
};
```

### 4.2: Go — Persistent gid_alias table ☐
**File**: `pkg/connector/cloud_backfill_store.go`

Add `gid_alias` table: `(login_id, sender_guid) → portal_id`. Methods: `saveGidAlias`, `getGidAlias`, `loadAllGidAliases`. Load into `gidAliases` map on startup.

**File**: `pkg/connector/client.go` — `makePortalKey()` line ~4365

When `resolveExistingGroupByGid` maps sender_guid to a different portal, also persist to `gid_alias` DB table (not just in-memory map).

### 4.3: Go — resolveConversationID gidAliases check ☐
**File**: `pkg/connector/sync_controller.go` — `resolveConversationID()` line ~1290

Before UUID→gid: fallback (step 3), check `gidAliases` (in-memory, loaded from DB). If APNs already resolved this sender_guid, use that portal_id.

```go
gidKey := "gid:" + strings.ToLower(msg.CloudChatId)
c.gidAliasesMu.RLock()
aliased, hasAlias := c.gidAliases[gidKey]
c.gidAliasesMu.RUnlock()
if hasAlias {
    return aliased
}
```

### 4.4: Go — Validate gid: portals in createPortalsFromCloudSync ☐
**File**: `pkg/connector/sync_controller.go` — `createPortalsFromCloudSync()`

Before creating a `gid:<uuid>` portal, verify the UUID exists in cloud_chat via `getChatPortalID`. If no match, it's an orphaned sender_guid artifact — skip.

---

## Architecture

```
APNs message received (Rust)
  │
  ├── Write to CloudKit (save_messages)     ← "cache"
  │     └── chat_id = sender_guid (groups) or service;-;remote (DMs)  ← FIX 4.1
  │
  └── Go callback (handleMessage)
        │
        ├── Existing portal? → deliver immediately
        │     └── gidAliases cache hit → persist alias to DB  ← FIX 4.2
        │
        └── New gid: portal? → signal background sync, return
              │
              └── Background sync loop picks up trigger
                    ├── resolveConversationID checks gidAliases  ← FIX 4.3
                    ├── createPortalsFromCloudSync validates gid:  ← FIX 4.4
                    └── Backfill delivers message to Matrix
```

Key invariant: **gid: portals are only created by CloudKit sync, never by APNs.**

---

## Completed Phases

### Phase 1: Periodic CloudKit Poll ✅
### Phase 2: APNs → CloudKit Write ✅
### Phase 3: Portal Splintering Fixes ✅

---

## Key Commits

| Commit | Description |
|--------|-------------|
| `c046eef` | Fix makePortalKey: empty groupName check |
| `e8c568e` | Fix group detection: cloud_chat fallback for ≤2 participants |
| `97b600a` | Status 200 PCS acks → DEBUG |
| `95e3994` | Pre-flush CloudKit sync + resolveConversationID ordering |
| `0855bd1` | getMessagePortalID helper |
| `2eaf224` | Background sync loop, never create gid: from APNs |

---

## Implementation Log

### Session 1 (2026-02-20) — Research + Plan
- Explored CloudKit write primitives, mapped WrappedMessage ↔ CloudMessage fields
- Decision: CloudKit write BEFORE Go callback (survives crashes)
- Decision: periodic poll interval = 5 minutes

### Session 2 (2026-02-20) — Implementation
- Implemented periodic CloudKit poll (Go) and APNs→CloudKit write (Rust)
- Added HasCloudMessageUuid FFI callback

### Session 3 (2026-02-20) — Portal Splintering Fixes
- Root cause: APNs sender_guid ≠ CloudKit gid for same group
- Root cause: makePortalKey treated empty cv_name as non-nil → DMs became groups
- **Failed approach**: DM sender before UUID in resolveConversationID → reverted (broke groups)
- Added pre-flush CloudKit sync + UUID dedup in flushPendingPortalMsgs
- **Key decision**: gid: portals NEVER created from APNs path

### Session 4 (2026-02-21) — Remaining Splintering Research
- **Finding**: Rust `message_inst_to_cloud_message` uses `sender_guid` as `chat_id` for ALL convos
- **Finding**: DMs have `sender_guid` set (UUID) — gets written to CloudKit with UUID chat_id
- **Finding**: `resolveConversationID` UUID→gid fallback creates `gid:<sender_guid>` for DMs
- **Finding**: `gidAliases` in-memory only — lost on restart, can't help CloudKit sync
- Designed 4-part fix: Rust DM chat_id + persistent gid_alias + resolveConversationID + portal validation
- **Key code paths traced**:
  - lib.rs:1266 — `message_inst_to_cloud_message` chat_id derivation
  - sync_controller.go:1264 — `resolveConversationID` UUID fallback (step 3)
  - sync_controller.go:1544 — `createPortalsFromCloudSync` portal loop
  - client.go:4324 — `makePortalKey` isGroup detection + gidAliases caching
  - client.go:4190 — `resolveExistingGroupByGid` participant matching
