# GalvoFS

GalvoFS is the shared filesystem substrate for Femto compute and Galvo agents.

It owns the storage namespace, object-backed workspace model, storage inputs,
leases, mount manifests, receipts, snapshots, artifacts, integrity checks, and
access-plane connector contracts. Femto consumes GalvoFS for compute storage.
Galvo consumes GalvoFS for agent workspaces and evidence state.

## Crates

- `galvofs-core`: Rust library for the GalvoFS metadata and object model.
- `galvofs-connector`: connector-side planner for CSI and kernel `nfsd`/NFSv4
  handoff.

## Current V1 Scope

- Local metadata and object-backed storage.
- OpenDAL filesystem and S3-compatible object body backends.
- Workspaces, leases, files, snapshots, clones, diffs, rollback, artifacts,
  receipts, integrity checks, and conservative garbage collection.
- Storage Inputs V1 for local-path imports behind explicit allowlists and
  uploaded-object promotion.
- Connector-ready CSI and NFSv4 endpoint plans.

S3-prefix materialization, site filesystem policy, writeback, production CSI,
NFSD apply mode, and data-lake shims remain roadmap work.
