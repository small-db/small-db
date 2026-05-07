# Review Questions

Questions / decisions I deferred during the autonomous code+doc consistency pass.
Read these in the morning and settle them.

## 1. `TxnRecord.commit_ts` → `write_ts` (on-disk JSON wire-format change)

The on-disk JSON for `/_txn/<txn_id>` records changed key `commit_ts` → `write_ts`.

- **Impact:** existing data dirs from older runs cannot be read by the new binary.
  `from_json` will throw on missing `write_ts`.
- **Mitigation:** all production-equivalent runners (Jepsen `lein run test-all`,
  integration `scripts/test/test.sh`) wipe the data dir before the run. Unit tests
  open ephemeral `/tmp/small-db-unit-<pid>/` paths and wipe per-pid.
- **Open:** if you have a long-running data dir somewhere (e.g. a VM in
  `small-db-jepsen/vagrant/` that's mid-investigation), that dir's txn records
  will fail to deserialize. Acceptable? If not, a one-shot migration is doable
  (add a fallback `commit_ts` reader in `from_json`).

## 2. `WriteResponse.final_commit_ts` → `final_write_ts` (gRPC wire-format change)

Same family of issue: `final_commit_ts` (proto field 2) was renamed to
`final_write_ts`. Field tag stays 2; on-the-wire bytes are unchanged. Source
compatibility breaks for any pre-rename callers.

- **Impact:** none in this repo (all callers updated). No external consumers known.
- **Open:** any embargoed branch/PR with the old name will conflict on rebase.

## 3. `closed_timestamps.md` and other book chapters: keep `commit_ts` in prose?

Several book chapters (`shadowed_writes.md`, `multi_statement_transactions.md`,
`closed_timestamps.md`, etc.) use `commit_ts` extensively. Per the rename,
many of those references are either:

- (a) the *post-COMMIT* commit timestamp — accurate either way; KEPT
- (b) the *mid-flight* write timestamp — should arguably be `write_ts`

I updated the chapters where the distinction is structurally important
(`write_intents.md`, `lost_intent.md`, `intent_atomicity_race.md`). I did NOT
mass-rename in chapters that talk about distributed-systems concepts in
generic terms (Spanner/Cockroach/HLC etc.) — those use `commit_ts` as the
literature term and the rename would obscure cross-referencing.

- **Open:** want me to keep going (chapter-by-chapter review of every
  `commit_ts` reference)? Or stop here?

## 4. `IntentRow` carries no timestamp

For reference: the on-disk intent record `/<table>/<pk>/INTENT` does NOT carry
a timestamp; it's just `{ value, txn_id, coordinator_addr }`. The intent's
effective `version_ts` is whatever the txn record's `write_ts` says at
read-resolve time, promoted to a numeric chain entry on
half-promote/full-promote. So no rename was needed at the intent layer.

## 5. Reference comment in `closedts/registry.h:55`

The comment says: `// the protocol's invariant ('commit_ts > T_closed') only requires`.

Left as `commit_ts` because the closed-timestamps invariant is genuinely
about post-COMMIT commit timestamps (the protocol guarantees that every
commit committed at `commit_ts > T_closed` is invisible to readers waiting
past `T_closed`). The Register() call takes a `lower_bound` argument — a
lower bound on what the eventually-finalized commit_ts will be — sourced
from the writer's current `write_ts`.

Worth adding a sentence that says exactly this in the chapter, but the
in-code comment is correct as-is.

## 6. (Resolved during the run, noted for completeness)

`Txn::Commit()` does a final bump (`write_ts := max(write_ts, now())` —
"Mechanism A" from `closed_timestamps.md`) before persisting `write_ts` as
the txn record's commit timestamp. This is an additional bump trigger
beyond the per-row push rule. Documented in the in-code comment of
`Txn::Commit`.
