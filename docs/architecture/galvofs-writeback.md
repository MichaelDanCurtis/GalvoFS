# GalvoFS Writeback

## Status

Design note for writeback, covering both directions the word has been used for
in this repo. Written 2026-07-29, before implementation, per the development
rule in `AGENTS.md`: S3-prefix, site-filesystem, writeback, CSI, NFS, and
data-lake behavior are explicit in roadmap or design notes before durable API
contracts exist.

This note closes `ROADMAP.md` Next Work item 2 ("Writeback design before
implementation") and the Needs Brainstorming entry "Writeback semantics and
conflict behavior."

## What "writeback" means here

The word had been carrying two different features:

- **Inward** — committing several files into a workspace as one unit, so a
  caller holding a multi-file change either lands all of it or none of it.
- **Outward** — pushing a workspace's changes back to the external source the
  workspace materialized them from.

They are one feature because they are one guarantee seen from two sides, and
because the outward direction is unimplementable honestly until the inward one
holds. They are designed together and implemented in that order.

## Why the substrate comes first

The inward guarantee cannot currently be delivered, and the reason is not the
absence of a multi-file API. It is three properties of the metadata layer:

1. **Document writes are not crash-atomic.** `write_json` is a bare
   `fs::write` — truncate, then write. A crash partway leaves torn JSON, and
   for a workspace document that loses the *entire* file index rather than one
   file. This is a strictly worse failure than the half-applied patch that
   prompted the work.

2. **There is no lost-update protection.** Every mutation is read-whole-state,
   modify, write-whole-state. There is no version, no compare-and-swap, and no
   lock; the crate depends on no locking library. Two concurrent `write_file`
   calls against one workspace both read, both insert, and the second write
   silently discards the first record. The body object survives — bodies are
   content-addressed and written separately — but it is unreferenced, and
   garbage collection eventually reaps it.

3. **Storage leases do not confer exclusivity.** `authorize_lease` checks
   expiry, access mode, and tenant/project match. Two live write leases on one
   workspace both authorize. Leases are an authorization primitive and are not
   extended into a concurrency primitive by this design.

There are no concurrency tests in the crate today.

An API advertising all-or-nothing on top of that substrate would claim more
than it delivers. Since the motivating consumer is Femto's `apply_patch` —
which is meticulous about refusing rather than overstating — shipping the
overclaim one layer down would reproduce the exact bug being fixed.

## Substrate design

**Crash-atomic documents.** `write_json` becomes write to a sibling temporary
file, fsync the file, rename over the target, fsync the parent directory.
Rename is atomic on POSIX; the fsyncs make the result durable rather than
merely ordered. Because every metadata document in the crate goes through this
one function, workspaces, leases, storage inputs, snapshots, bindings, and
artifacts all gain durability together. The blast radius is wider than the
change description suggests, and that is intended.

**Version field.** `WorkspaceState` gains `version: u64`, incremented on every
mutation and persisted. A write asserts the on-disk version still equals the
version it read; a mismatch raises a new `GalvoFsError::Conflict`. This is the
*detector*.

**Workspace lock.** An `O_EXCL` lockfile at `metadata/workspaces/<id>.lock`,
holding the owning pid and an acquisition timestamp, is taken across the whole
read-modify-write. `O_EXCL` creation is atomic, so no dependency is required.
A lock older than a configured staleness window is broken, and the break is
recorded. This is the *preventer*.

Both exist because a lock is only as good as every call site remembering to
take one. The version field turns a forgotten lock from silent data loss into a
loud, attributable error.

**`commit_files`.** `commit_files(workspace_id, files)` and
`commit_files_with_lease` put every body first — content-addressed and
idempotent, and an orphaned body is already safe because garbage collection
reclaims unreferenced objects — then perform one locked, version-checked state
write inserting all N records. All N records land or none do.

### Error mapping

`GalvoFsError::Conflict` must not reach Femto as `InvalidRequest`.

Femto's protocol doc guarantees that a 422 means the workspace is unchanged,
and `apply_patch` currently makes that true by construction: nothing in its
write-back path can produce `InvalidRequest`, because that variant is
unreachable through `From<GalvoFsError>`. A conflict does leave the workspace
unchanged, so 422 would not be a lie — but routing it there would make the
invariant depend on a coincidence rather than on the type system.

`Conflict` therefore gets its own Femto variant and surfaces as **409**. It is
honest, it is distinguishable from both the refusals (422) and the runtime
failures (502), and it tells the caller the request is retryable.

## Femto adoption

The write-back loop in `crates/femto-core/src/workspace.rs` becomes a single
`commit_files` call. The comment above it, which currently documents that loop
as the point where the all-or-nothing guarantee stops, is rewritten to record
that the guarantee now holds and what makes it hold.

That is the only Femto-side change. GalvoFS gains no knowledge of Femto,
per the independence rule in `AGENTS.md`.

## Baseline for drift detection

Nothing in the crate records what an external source looked like when it was
materialized. `digest_policy` and `verification_status` exist as labels with no
stored digest behind them, and `GalvoFsMaterializationTask` counts files and
bytes without recording any.

On materialization, take a `SnapshotPin` and record `baseline_snapshot_id` on
`GalvoFsStorageInputBinding`. That is the only new persisted field. Drift is
then `diff_workspace` against the baseline — an operation that already exists.
`verification_status` gains a real `Pending -> Verified` transition when the
baseline is taken and digests confirm under `digest_policy: Required`, which
retires one of the decorative labels rather than leaving it.

## The writeback operation

`writeback(workspace_id, binding_id, lease_id?) -> WritebackReport`.

Every refusal happens before anything external is touched:

1. `writeback_policy: None` — refuse, explicitly. A no-op that reports success
   is how a dead policy field survives unnoticed.
2. `source_kind != LocalPath` — refuse as unsupported, naming the kind.
3. Binding access is `ReadOnly` — refuse.
4. Diff the workspace against `baseline_snapshot_id`, scoped to paths under the
   binding's `mount_path`.
5. Re-read the external source and compare against the baseline. **Any drift on
   any path aborts the entire writeback**, naming every drifted path. Nothing
   is pushed.
6. Only now write, each file through its own temp-and-rename.
7. Append a `StorageReceipt` recording binding, baseline snapshot, and per-path
   digests.

Files added in the workspace under `mount_path` are pushed as new files. The
workspace cannot represent a deletion, so writeback cannot express one either;
this is stated rather than silently assumed.

### The honest limit

Step 6 is N writes into a directory this system does not own, and a plain local
filesystem offers no cross-file transaction. A crash midway leaves a partial
result. No design removes that.

What the design does instead: make the pre-flight exhaustive so a step-6 failure
is rare and mechanical rather than a normal outcome; write each file through
temp-and-rename so no individual file is ever observed torn; and return a
`WritebackReport` naming exactly which paths landed. That report is the whole
difference between a partial push and a silent one.

The word "atomic" is not used for the outward direction anywhere in the API,
the docs, or the receipts.

## PinArtifacts

`PinArtifacts` publishes each changed file under `mount_path` through the
existing `publish_artifact`, returning the manifests. It is internal, immutable,
and mutates no external storage — the cheap policy to implement, because the
machinery already exists.

With this note, all three `GalvoFsWritebackPolicy` variants have defined
behavior. Before it, all three were persisted and none was ever read: no
`match`, no `if`, nowhere in the crate.

## Testing

- **Concurrency**, where none exists today: two concurrent `write_file` calls
  on one workspace both survive; `commit_files` racing a `write_file` yields
  either a full commit followed by the write or a `Conflict`, never a partial
  index.
- **Crash**: a kill between temp write and rename leaves the previous document
  valid and parseable.
- **Refusal matrix**: every refusal reason above, each asserting the external
  source is byte-identical afterward.
- **Drift**: mutate the source, assert the refusal names the path and that
  nothing was written.
- **Femto integration**: a forced mid-commit failure leaves the workspace index
  unchanged.

The concurrency and crash tests must be confirmed to fail against the current
implementation before the fix lands. A test that passes against the bug it
targets is not coverage.

## Out of scope

- External atomicity beyond per-file temp-and-rename. Stated above; not
  designed around.
- S3-prefix and site-filesystem writeback. Neither source kind has a working
  materialization path yet, and writeback for a source that cannot yet be read
  in would be dead code that reads as coverage.
- Conflict *resolution*. This design detects and refuses; it does not merge.
- Metadata HA and multi-owner mode. Still in Needs Brainstorming. The version
  field is deliberately the hook that generalizes into a compare-and-swap
  against a shared metadata store when that work happens; an in-process lock
  would have foreclosed it.
