# GalvoFS Agent Notes

GalvoFS is the shared storage substrate for Femto compute and Galvo agents.

Ownership boundary:

- GalvoFS owns filesystem/storage semantics: workspaces, storage inputs,
  object bodies, leases, mount manifests, snapshots, artifacts, receipts,
  integrity checks, garbage collection, and access-plane connector contracts.
- Femto owns A2E admission, runtime routing, compute adapters, and HTTP
  integration that consumes GalvoFS.
- Galvo owns agent orchestration and agent-facing workspace workflows that
  consume GalvoFS.
- Tessera owns key management and trust/unwrap policy.

Development rules:

- Keep GalvoFS independent of Femto and Galvo crates.
- Prefer narrow shared crates and generated clients over copy-pasted protocol
  structs.
- Keep S3-prefix, site-filesystem, writeback, CSI, NFS, and data-lake behavior
  explicit in roadmap/design notes before implementing durable API contracts.
