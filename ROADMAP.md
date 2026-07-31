# GalvoFS Roadmap

Last updated: 2026-07-29

GalvoFS is now its own repo after Storage Inputs V1 for local-path imports and
uploaded-object promotion landed in Femto.

## Implemented

- `galvofs-core` library crate.
- Local metadata and object-backed storage.
- OpenDAL filesystem and S3-compatible object body options.
- Workspace create, file read/write/list, snapshots, clones, diffs, rollback,
  artifacts, receipts, integrity checks, and conservative garbage collection.
- Storage leases and mount manifests.
- Typed storage input admission.
- Local-path import behind explicit allowlists.
- Uploaded-object promotion into workspace files.
- Materialization task records for local/uploaded imports.
- `galvofs-connector` plan generation for CSI and kernel `nfsd`/NFSv4 handoff.
- Guarded `galvofs-connector --apply-nfsd` mode that requires
  `FEMTO_GALVOFS_NFSD_CLIENTS`, executes `exportfs` without a shell, and
  supports dry-run command reports.
- NFSD export reconciliation and cleanup support in `galvofs-connector`,
  including `exportfs -v` state parsing, already-configured no-op reports,
  drifted export updates, and `--cleanup-nfsd` unexport reports.
- SHA-256 command digests in NFSD apply/cleanup reports for receipt-ready
  command evidence.
- Crash-atomic metadata documents (temp, fsync, rename) and a workspace write
  lock plus state version, closing a torn-document window and a lost-update
  race that silently discarded concurrent writes.
- `commit_files`: several files installed as one unit, all or none.
- Canonical storage input source URIs, content-derived input identity so a
  retried create is idempotent, and `storage_inputs` for listing — previously
  nothing could list them, so a client whose create timed out could not
  discover whether it had landed.
- Writeback, designed in `docs/architecture/galvofs-writeback.md` and
  implemented for local-path sources: a materialization baseline snapshot,
  explicit invocation only, refusal of the whole push if the source drifted
  since materialization, per-file temp-and-rename, and a report naming exactly
  which paths landed. `PinArtifacts` publishes internally instead. The outward
  push is not atomic across files and does not claim to be — see the design
  note.

## Next Work

1. Storage input policy hardening — **structured errors** are what remains.
   Source URI normalization, idempotency, and duplicate detection all shipped:
   identity is derived from the normalized request content, so a retried create
   resolves to the original rather than minting a second input, and changing the
   `name` is how a caller asks for a genuinely new one. Explicit verification
   transitions were already implemented before this item was written — see the
   correction in `docs/architecture/galvofs-writeback.md`.
2. Writeback for source kinds beyond local-path. The operation shipped for
   `LocalPath` only; every other kind refuses by name. `S3Prefix` and
   `SiteFilesystemReference` cannot be done before their import paths exist
   (items 3 and 4), because writeback for a source that cannot be read in would
   be dead code that reads as support.
3. S3-prefix import/reference/mirror design before implementation.
4. Site filesystem reference design before implementation.
5. Production NFSv4 lease-expiry automation, audit receipts, and site policy.
6. Real CSI driver.
7. Galvo-facing workspace/evidence integration.

## Needs Brainstorming

- S3-prefix default behavior: reference, import, mirror, or mount.
- Site filesystem references for Slurm/HPC and enterprise filesystems.
- Data-lake shims: input source, delegated query engine, manifest catalog, or
  writeback target.
- Metadata HA and multi-owner mode.
