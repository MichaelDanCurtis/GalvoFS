# GalvoFS Shared Filesystem Substrate

## Status

Decision note for the shared Femto and Galvo filesystem subsystem.

The current milestone is **GalvoFS foundation plus Storage Inputs V1 local
data path**: Femto has the core local GalvoFS storage model, runtime bindings,
artifact/snapshot records, connector-ready CSI and NFSv4 endpoint metadata, a
GalvoFS connector with plan generation plus guarded NFSD export apply mode,
local-path import behind explicit allowlists, and uploaded-object promotion
into workspaces. Production access-plane reconciliation work, including NFSD
cleanup/reconciliation, a real CSI driver, runtime operators, S3/site input
adapters, writeback, and data-lake shims, remains on the roadmap after the V1
contract is exercised.

Femto needs a storage substrate for execution workspaces, artifacts,
checkpoints, mounted inputs, snapshots, and receipts. Galvo needs the same
substrate for agent workspaces, agent-local state, tool evidence, and shared
context. The subsystem should be shared rather than duplicated because the
boundary between agent work and compute work is intentionally fluid.

The selected direction is to build **GalvoFS** as a Femto/Galvo-owned subsystem
using NoKV-style object-backed filesystem semantics, OpenDAL-backed storage
adapters, optional AgentFS-style per-agent audit state, and optional
Mirage-style virtual mounts for SaaS and context sources.

## Decision

Build GalvoFS as the singular filesystem substrate for Femto and Galvo.

The core should be based on the design shape of
[NoKV](https://github.com/feichai0017/NoKV): a Rust-first filesystem for AI
training and agent workspaces with a metadata service, object-backed file
bodies, FUSE, snapshots, clone, diff, rollback, artifact publish, and
copy-on-write workspace semantics.

GalvoFS should use [Apache OpenDAL](https://github.com/apache/opendal) as the
preferred storage adapter layer wherever it fits. The durable byte plane should
start with S3-compatible object storage such as RustFS, MinIO, Ceph RGW, AWS S3,
or any deployment-specific equivalent.

GalvoFS should treat [AgentFS](https://github.com/tursodatabase/agentfs) as a
useful per-agent state and audit reference, not as the main durable filesystem
substrate. AgentFS is strong for SQLite/Turso-backed agent files, key/value
state, tool-call timelines, FUSE/NFS access, sync, and MCP exposure, but it is
not the best base for large durable workspaces, checkpoints, model artifacts,
or container and microVM roots.

GalvoFS should treat [Mirage](https://github.com/strukto-ai/mirage) as an
optional agent-facing virtual mount layer, not as the authoritative filesystem.
Mirage remains valuable for mounting SaaS, collaboration, repository, database,
and knowledge sources into an agent-friendly namespace. It should not own
Femto's durable object manifests, A2E storage receipts, or destructive boot
semantics.

## Problem

Femto and Galvo need one filesystem vocabulary across several very different
execution situations:

- agent-local scratch state;
- code repositories and generated files;
- container and microVM workspaces;
- function inputs and outputs;
- model artifacts and datasets;
- checkpoints and final artifacts;
- mounted external context;
- execution receipts and storage evidence.

The substrate must be independent of the underlying storage fabric. A Femto
cell may use local disk, RustFS, MinIO, Ceph RGW, AWS S3, Azure Blob, GCS, or a
site-specific object store. Galvo agents should not need to learn those
differences, and Femto runtimes should not directly inherit cloud credentials
or fabric-specific APIs.

HPC hot scratch remains a separate data-plane concern. Large multi-node
training may require Lustre, BeeGFS, GPFS, DAOS, NFS, or site-specific parallel
filesystems inside the Slurm allocation. GalvoFS should stage inputs, record
provenance, verify outputs, and publish/write back artifacts for HPC work, but
it should not replace the native parallel filesystem used for hot scratch inside
the allocation.

## Required Properties

GalvoFS must provide:

- one namespace model for Galvo agents and Femto compute;
- filesystem-shaped access through FUSE, NFS, 9P, virtio-fs, or a native SDK;
- copy-on-write workspaces for isolated agent and compute runs;
- snapshot, clone, diff, rollback, and artifact publish operations;
- object-backed byte durability independent of the local compute node;
- immutable block or object references with digest metadata;
- metadata-first APIs, but object-first write ordering;
- storage leases bound to tenant, project, work order, runtime, and A2E ticket;
- receipt-ready artifact records with digests, snapshot ids, mount ids, and
  retention policy;
- integration with the existing key-management subsystem for wrapping,
  unwrapping, revocation, and audit;
- fail-closed mount policy for secrets and external data sources;
- enough local ergonomics to run containers and microVMs without a production
  storage cluster.

## Selected Upstream Roles

### NoKV: Core Filesystem Direction

NoKV is the closest match to the needed substrate because it owns filesystem
metadata semantics while delegating file body durability to object storage. Its
copy-on-write workspace model maps directly onto agent workspaces and Femto
`workspacelet` runs:

- snapshots pin a stable read version;
- clones create writable zero-copy forks;
- diffs show content-addressed workspace changes;
- rollback returns a workspace to a prior snapshot;
- object blocks are immutable;
- artifact publish uploads bytes first, then commits metadata;
- snapshot pins protect old bodies from garbage collection.

The main gap is distributed metadata. NoKV is single-node metadata today. Its
distributed direction is subtree sharding with owner leases and epoch fencing,
but that is not implemented. GalvoFS v0 should therefore treat each Femto cell
or local development site as a single metadata owner and make HA an explicit
later phase.

### OpenDAL: Storage Adapter Layer

OpenDAL should be used for backend object and blob access where practical. It
keeps GalvoFS from binding its core to one cloud or object-store SDK. The first
supported backend should be S3-compatible storage because it covers RustFS,
MinIO, Ceph RGW, SeaweedFS S3, AWS S3, and many private-cloud deployments.

OpenDAL is not a filesystem. It belongs below GalvoFS as the storage operator
layer.

### AgentFS: Agent State And Audit Reference

AgentFS is useful for small per-agent state:

- SQLite/Turso-backed virtual filesystem state;
- key/value context;
- tool-call timeline;
- MCP serving;
- local encrypted agent sessions;
- FUSE on Linux and NFS on macOS;
- sandboxed `agentfs run` workflows.

GalvoFS can borrow this shape for per-run timelines and agent scratch
databases. It should not use AgentFS as the primary object and checkpoint store.

### Mirage: Agent Virtual Mount Reference

Mirage is useful when an agent needs a single path vocabulary over SaaS,
collaboration, repository, and knowledge sources. GalvoFS should be able to
expose Mirage-like mounts or import Mirage workspaces, but those mounts are
context views. Durable compute payloads and result artifacts still need GalvoFS
object manifests and A2E receipts.

### Infrastructure Candidates

JuiceFS, SeaweedFS, ZeroFS, Ceph, MinIO, RustFS, and similar systems are
possible underlying storage fabrics. They should be treated as deployment
choices beneath the GalvoFS contract, not as the Galvo/Femto semantic
filesystem itself.

## GalvoFS Architecture

GalvoFS should have four layers:

1. **Namespace and metadata service**
   Owns mounts, inodes, paths, snapshots, clones, diffs, rollback, artifact
   publication, storage leases, watch events, and garbage-collection policy.

2. **Object and data plane**
   Stores file bodies as immutable blocks or objects in S3-compatible storage
   first, with OpenDAL adapters for additional providers later. Local NVMe or
   disk cache is a soft hot tier, never the durable truth.

3. **Runtime access plane**
   Presents workspaces through FUSE, NFS, 9P, virtio-fs, bind mounts, or a
   native Rust SDK depending on the runtime. Containers and microVMs should see
   normal filesystem paths. Femto's hot path should use native APIs when it can.

4. **A2E and security control plane**
   Binds mount manifests, storage leases, object digests, snapshot ids, unwrap
   decisions, artifact publication, and retention policy to the A2E ticket and
   the existing key-management subsystem.

## Core Object Model

GalvoFS should define these stable API objects:

| Object | Purpose |
| --- | --- |
| `MountManifest` | Declares named mounts, backend references, access mode, policy, and digest. |
| `WorkspaceRoot` | Names a mutable filesystem root for an agent, work order, or secure entity. |
| `SnapshotPin` | Pins a stable read frontier and protects referenced objects from GC. |
| `WorkspaceClone` | Creates an isolated copy-on-write workspace from a root or snapshot. |
| `StorageLease` | Grants scoped read/write/mount rights to one secure entity or agent run. |
| `AccessEndpoint` | Describes how a runtime should consume a leased workspace, such as native API, CSI, or NFSv4, including connector-ready projection metadata. |
| `ArtifactManifest` | Records published outputs, object digests, content type, size, and retention. |
| `WorkspaceDiff` | Reports added, removed, and modified paths with digest and size deltas. |
| `StorageReceipt` | Evidence record for mount, unwrap, read, write, snapshot, publish, and purge. |

## Femto Integration

Femto should use GalvoFS as the storage authority behind:

- `workspacelet` roots for local directories, containers, and microVMs;
- `fnlet` input and output references when inline payloads are too large;
- `kubenyxlet` staged inputs and published artifacts;
- `slurmlet` staged inputs, checkpoints, logs, final artifacts, provenance
  records, and result writeback policy while the job's hot scratch I/O stays on
  the native HPC parallel filesystem;
- secure compute capsule storage bindings;
- workspace snapshots and artifact receipts;
- model cache and GPU job artifact references.

Femto remains the runtime placement and enforcement layer. GalvoFS does not
decide whether work runs as `instant`, `warm`, `scheduled`, or `batch`.

## Galvo Integration

Galvo should use GalvoFS as the agent workspace and evidence filesystem:

- each agent receives a scoped workspace root;
- sub-agents can receive copy-on-write clones;
- agent changes are visible through `WorkspaceDiff`;
- successful work can publish artifacts back to declared result roots;
- failed work can be rolled back or retained for inspection;
- tool timelines and small state can use AgentFS-style sidecar databases;
- SaaS and context mounts can be exposed through Mirage-style adapters.

Galvo remains the agent negotiation and collaboration layer. GalvoFS does not
decide task semantics, peer discovery, or A2A conversation policy.

## A2E And Key Management

GalvoFS must be wired to the existing key-management subsystem rather than
inventing its own trust store.

The A2E secure compute capsule should bind:

- A2E ticket digest;
- tenant and project;
- Femto cell identity;
- secure entity identity;
- mount manifest digest;
- GalvoFS workspace root;
- snapshot id or clone id;
- artifact manifest digest;
- key unwrap policy;
- retention and purge policy.

Storage unwrap should fail unless the presented ticket, entity identity, cell
identity, policy digest, and key-management decision match. Successful unwraps,
mounts, reads, writes, snapshots, artifact publishes, and purges should emit
storage receipts that Femto can include in secure run receipts.

## V0 Scope

The first implementation should be deliberately small:

- local GalvoFS metadata service with one owner;
- S3-compatible object backend through a simple adapter path;
- local filesystem or RustFS/MinIO for development;
- native Rust client used by Femto;
- workspace create, upload, download, list, exec-mount preparation;
- snapshot, clone, diff, rollback, and publish artifact;
- storage receipts written into Femto's run ledger;
- mock key unwrap integration that matches the current local
  GalvoFS/RustyVault secure capsule flow;
- FUSE or NFS mount only after the native API is stable enough.

The v0 success criterion is not cluster scale. It is that Galvo agents and
Femto runtimes can share the same storage vocabulary, produce snapshots, publish
artifacts, emit receipts, and replay the evidence without knowing whether bytes
live on local disk or S3-compatible storage. Slurm/HPC remains part of that
contract at the staging, provenance, and writeback layer, not as a replacement
for native parallel scratch.

## Current Implementation

The first implementation slice now lives in `femto-core` as the `galvofs`
module and is exposed by `femto-server` through `/v0/galvofs/*` routes.

The implemented v0 surface includes:

- local object-backed storage under a configurable GalvoFS root;
- OpenDAL-backed object storage using either a local filesystem service or an
  S3-compatible service while retaining GalvoFS metadata locally;
- immutable SHA-256 object bodies addressed as `galvofs://objects/sha256/...`;
- workspace create, file write, file read, and file list;
- storage lease issuance bound to workspace, tenant, project, secure entity,
  work order, optional A2E ticket, optional runtime id, access mode, and expiry;
- lease-scoped file list, read, write, and artifact publish routes that fail
  closed on expired or insufficient leases;
- typed storage input admission for `galvofs-workspace`, `uploaded-object`,
  `local-path`, `s3-prefix`, and `site-filesystem-reference` sources with
  materialization mode, access mode, mount path, digest policy, retention
  policy, writeback policy, and verification status;
- raw uploaded-object ingestion with optional expected-digest validation,
  content-type capture, SHA-256 addressing, and upload receipts;
- local-path `import` materialization for files and directories under explicit
  GalvoFS allowlists;
- uploaded-object `import` materialization into a workspace file;
- materialization task records with `running`, `completed`, and `failed`
  states, start/completion/failure receipts, and HTTP task listing by storage
  input;
- workspace storage input attachment and listing, including lease-scoped
  authorization checks and JSONL storage receipts;
- workload `StorageSpec` admission projects declared runtime storage into
  GalvoFS storage input bindings, including explicit `sourceKind`, `sourceUri`,
  materialization, access, digest, retention, and writeback fields, so run
  metadata carries deterministic storage inputs before CSI, NFS, or site
  operators apply them;
- mount manifest creation with deterministic SHA-256 digests, optional
  lease binding, policy digest binding, and named mount declarations for
  workspace, attached storage input, or external context sources;
- workspace snapshot, clone, diff, and rollback;
- workspacelet admission binds each local workspace run to a GalvoFS workspace,
  read-write lease, and mount manifest, then mirrors upload/download/list and
  snapshot flows through that binding;
- fnlet invocation resolves scoped `galvofs://workspaces/.../files/...`
  payloads into the function event and publishes result/stdout/stderr as
  GalvoFS artifact manifests without giving the function a filesystem mount;
- kubenyxlet admission binds each scheduled dispatch to a GalvoFS workspace,
  read-write lease, and `/workspace` mount manifest so KubeNyx receives a stable
  storage projection along with image, command, GPU, receipt metadata, and a
  CSI access endpoint for `galvofs.csi.femto.local` with a Kubernetes inline
  CSI volume and volume mount projection;
- slurmlet admission binds each batch dispatch to a GalvoFS staging workspace,
  read-write lease, and `/femto/galvofs` mount manifest while advertising a
  native parallel scratch policy for Lustre, BeeGFS, GPFS, or DAOS and a
  writeback contract that pins outputs back to GalvoFS; the dispatch metadata
  includes an NFSv4 export descriptor, environment values, and mount command for
  the lease-bound staging workspace;
- secure compute capsule storage resolution can validate real
  `galvofs://workspaces/.../files/...` objects, mount manifest digests, and
  snapshot pins in addition to the local development fixture resolver;
- artifact publish with digest, size, content type, and retention metadata;
- JSONL storage receipts for workspace, lease, file, snapshot, clone, rollback,
  and artifact operations;
- integrity verification that scans workspace, snapshot, and artifact metadata,
  deduplicates referenced object digests, and reports missing or corrupt object
  bodies;
- conservative garbage collection that reports or purges only object bodies no
  longer referenced by workspace, snapshot, or artifact metadata;
- path validation that rejects absolute paths and parent traversal.

The local Femto server uses `FEMTO_GALVOFS_ROOT` when set. Without it, local
development uses a temp-directory GalvoFS root. Object bodies default to
`$FEMTO_GALVOFS_ROOT/objects`, but can be moved behind OpenDAL with:

- `FEMTO_GALVOFS_OBJECT_BACKEND=opendal-fs` plus
  `FEMTO_GALVOFS_OBJECT_ROOT=/path/to/object-root`;
- `FEMTO_GALVOFS_OBJECT_BACKEND=s3` or `opendal-s3` plus
  `FEMTO_GALVOFS_S3_BUCKET`, optional `FEMTO_GALVOFS_S3_ROOT`,
  optional `FEMTO_GALVOFS_S3_ENDPOINT`, optional `FEMTO_GALVOFS_S3_REGION`,
  and optional explicit `FEMTO_GALVOFS_S3_ACCESS_KEY_ID` /
  `FEMTO_GALVOFS_S3_SECRET_ACCESS_KEY`.

Local-path storage inputs are fail-closed unless
`FEMTO_GALVOFS_LOCAL_INPUT_ROOTS` names one or more allowed host roots using
the platform path-list separator. Paths outside those roots and symlink inputs
are rejected before materialization.

The current mode remains native API first. The KubeNyx CSI and Slurm NFSv4
entries are connector-ready access endpoint contracts in dispatch metadata;
production CSI sidecars, NFS export daemons, FUSE, 9P, and virtio-fs mount
adapters remain roadmap phases.

Storage Inputs V1 now covers local files/directories and plain uploaded object
blobs. The remaining storage-input work is external-source design and
hardening: source URI normalization, idempotency, duplicate detection,
structured error codes, reference/live-mount behavior, S3-prefix
scan/stage/reference behavior, site filesystem connector policy, writeback, and
failure semantics when a source cannot be verified. Data-lake shims should wait
until that storage-input contract is clear enough to decide whether GalvoFS
imports, references, mirrors, or delegates to an existing lake.

## Milestone Boundary And Roadmap

### Completed Milestone: GalvoFS Foundation

This milestone is complete when evaluating the current repository state. It
establishes GalvoFS as the shared Femto/Galvo filesystem substrate and proves
the end-to-end control contract without requiring privileged mount or export
mutation.

Completed scope:

- local metadata and object-backed GalvoFS storage;
- OpenDAL-backed filesystem and S3-compatible object body options;
- workspace file, lease, mount manifest, snapshot, clone, diff, rollback,
  artifact, receipt, integrity, and garbage-collection primitives;
- Femto runtime binding for `workspacelet`, `fnlet`, `kubenyxlet`,
  `slurmlet`, and secure compute capsule preflight;
- connector-ready KubeNyx CSI endpoint metadata;
- connector-ready Slurm NFSv4 endpoint metadata;
- `galvofs-connector` plan generation for KubeNyx CSI and kernel `nfsd`/NFSv4
  handoff, plus explicit `--apply-nfsd` export apply mode with dry-run command
  reports and required client allowlists.

### Current Milestone: Storage Inputs V1

Storage inputs should be built before productionizing the CSI or NFSD access
planes. The contract should answer where bytes can originate, how they are
authorized, how they are represented in mount manifests, and how outputs write
back or remain pinned as GalvoFS artifacts.

The storage-input milestone now has the first data path: typed input objects,
workspace attachments, local-path import, uploaded-object promotion, typed
workload `StorageSpec` source fields, workload storage projection into run
bindings, mount-manifest references, materialization task records, and receipts.
The remaining milestone work should add:

- reference, mirror, and mount behavior for source kinds that GalvoFS does not
  copy eagerly;
- policy enforcement for read-only, read-write, copy-on-write, snapshot,
  retention, and writeback behavior;
- verified digest and provenance receipts for every input admitted into a
  workspace;
- connector boundaries and adapters for S3-compatible bucket inputs,
  object/blob provider inputs, and site-managed filesystem references;
- safe handling for large datasets where GalvoFS should reference or stage data
  rather than copy it eagerly;
- explicit failure behavior when an input cannot be verified, mounted, staged,
  or authorized.

### Roadmap After Storage Inputs

After storage inputs are stable, continue with:

- kernel `nfsd` reconciliation with privileged host checks, drift detection,
  export cleanup, lease-expiry handling, and audit receipts;
- a real GalvoFS CSI driver for KubeNyx/Kubernetes consumption;
- Slurm and KubeNyx production connectors/operators that consume GalvoFS plans
  and apply site policy;
- FUSE, 9P, or virtio-fs access backends where they fit containers and
  microVMs better than native APIs;
- quotas, watch streams, richer metrics, backup/restore, and metadata HA;
- data-lake shims once the storage-input model makes it clear whether GalvoFS
  should import, mirror, reference, or delegate to each lake.

## Non-Goals

GalvoFS v0 should not:

- replace Lustre, BeeGFS, GPFS, DAOS, NFS, or site-specific native parallel
  filesystems used for Slurm/HPC hot scratch;
- carry hot MPI, tensor checkpoint, or scratch-heavy Slurm I/O unless a site
  explicitly chooses a GalvoFS-compatible data-plane backend for that workload;
- implement multi-cell distributed metadata;
- expose raw object-store credentials to agents or runtimes;
- turn SaaS sources into durable storage without explicit import or snapshot;
- require Kubernetes, a live CSI deployment, an NFS daemon, or a production
  object store for local development;
- make Mirage, AgentFS, JuiceFS, or any one project the whole subsystem.

## Risks

| Risk | Mitigation |
| --- | --- |
| NoKV metadata is single-node today. | Keep v0 single-cell and design HA as a later explicit phase. Archive metadata checkpoints to object storage. |
| FUSE/NFS behavior can be platform-sensitive. | Use native Rust APIs first; add mount backends only after core semantics pass. |
| Object stores vary in consistency and versioning. | Keep GalvoFS metadata as truth; use object-first publish and fsck-style verification. |
| Agent virtual mounts may leak authority. | Treat SaaS/context mounts as policy-bound views with no ambient credentials. |
| Snapshots can block garbage collection. | Use leased snapshot pins and explicit retention policy. |
| HPC workloads need specialized storage. | Reference HPC storage from GalvoFS instead of replacing it in v0. Stage inputs and pin outputs through GalvoFS while hot scratch remains native. |

## Implementation Phases

### Phase 0: Spec And API Contract

Define GalvoFS object types, receipt schema, storage lease semantics, and the
Femto/Galvo integration boundary. Update A2E storage binding names to match.

### Phase 1: Local Metadata And Object Store

Implement or vendor a NoKV-shaped local metadata service with S3-compatible
object bodies, local development storage, and native Rust APIs.

### Phase 2: Workspace Operations

Add workspace create, snapshot, clone, diff, rollback, artifact publish, and
garbage-collection retention rules.

### Phase 3: Femto Runtime Binding

Replace local workspace manifest snapshots with GalvoFS snapshot and artifact
records. Mount GalvoFS roots into `workspacelet`, attach `/workspace` storage
projections to `kubenyxlet`, and attach staging/writeback projections to
`slurmlet` while preserving native HPC scratch.

### Phase 4: Galvo Agent Binding

Expose scoped agent roots, copy-on-write sub-agent clones, workspace diffs, and
optional AgentFS-style timelines.

### Phase 5: Production Hardening

Add metadata backup/restore, object fsck, quotas, watch streams, mount backends,
OpenDAL expansion, metrics, and later owner-lease/epoch-fenced multi-owner
metadata.

## Research Basis

- [NoKV](https://github.com/feichai0017/NoKV): best fit for the core
  object-backed AI workspace filesystem.
- [AgentFS](https://github.com/tursodatabase/agentfs): strong reference for
  per-agent SQLite/Turso state, audit timelines, and local sandboxing.
- [Apache OpenDAL](https://github.com/apache/opendal): best fit for storage
  backend abstraction.
- [Mirage](https://github.com/strukto-ai/mirage): useful virtual mount and
  agent context layer, not the durable storage authority.
- [JuiceFS](https://github.com/juicedata/juicefs),
  [SeaweedFS](https://github.com/seaweedfs/seaweedfs), and
  [ZeroFS](https://github.com/Barre/zerofs): possible underlying storage
  fabrics or deployment references, not the Femto/Galvo semantic layer.
