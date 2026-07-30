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

## Next Work

1. Storage input policy hardening: source URI normalization, idempotency,
   duplicate detection, structured errors, and explicit verification
   transitions.
2. Writeback — designed in `docs/architecture/galvofs-writeback.md`, not yet
   implemented. The design starts with metadata substrate integrity, because
   writeback cannot claim all-or-nothing on a substrate whose document writes
   are not crash-atomic and which has no lost-update protection. Those two are
   live defects today, independent of writeback.
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
