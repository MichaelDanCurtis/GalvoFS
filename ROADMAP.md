# GalvoFS Roadmap

Last updated: 2026-06-15

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

## Next Work

1. Storage input policy hardening: source URI normalization, idempotency,
   duplicate detection, structured errors, and explicit verification
   transitions.
2. Writeback design before implementation.
3. S3-prefix import/reference/mirror design before implementation.
4. Site filesystem reference design before implementation.
5. Production NFSv4 apply-plane reconciliation, cleanup, and site policy.
6. Real CSI driver.
7. Galvo-facing workspace/evidence integration.

## Needs Brainstorming

- S3-prefix default behavior: reference, import, mirror, or mount.
- Site filesystem references for Slurm/HPC and enterprise filesystems.
- Writeback semantics and conflict behavior.
- Data-lake shims: input source, delegated query engine, manifest catalog, or
  writeback target.
- Metadata HA and multi-owner mode.
