# GalvoFS Connector

`galvofs-connector` consumes GalvoFS access endpoint manifests and turns
them into connector actions.

Current scope:

- read `GET /v0/runs/{run_id}/galvofs/access-endpoints` JSON from stdin;
- produce connector plans for KubeNyx CSI handoff;
- produce connector plans for Slurm NFSv4 staging handoff through kernel
  `nfsd`/`exportfs`;
- apply kernel `nfsd` exports only when the operator explicitly passes
  `--apply-nfsd` and supplies `FEMTO_GALVOFS_NFSD_CLIENTS`.

The GalvoFS foundation and Storage Inputs V1 local/uploaded path are in place.
This connector can turn Femto endpoint metadata into concrete CSI and
NFSD/NFSv4 plans. It also has the first guarded NFSD apply mode. A real CSI
driver, runtime operators, lease-expiry cleanup, and site policy controllers
remain roadmap work.

Example:

```bash
curl -s http://127.0.0.1:8787/v0/runs/${RUN_ID}/galvofs/access-endpoints \
  | cargo run -p galvofs-connector
```

Dry-run the concrete `exportfs` apply command without mutating host export
state:

```bash
export FEMTO_GALVOFS_NFSD_CLIENTS='fd00:1234::/64'
curl -s http://127.0.0.1:8787/v0/runs/${RUN_ID}/galvofs/access-endpoints \
  | cargo run -p galvofs-connector -- --apply-nfsd --dry-run
```

Apply the export on a privileged Linux host:

```bash
export FEMTO_GALVOFS_NFSD_CLIENTS='fd00:1234::/64'
curl -s http://127.0.0.1:8787/v0/runs/${RUN_ID}/galvofs/access-endpoints \
  | cargo run -p galvofs-connector -- --apply-nfsd
```

## NFSD Direction

The selected NFSv4 direction is kernel `nfsd`.

### Kernel `nfsd`

Kernel `nfsd` uses the operating system NFS server and export table. It is the
highest-fidelity path for production NFS behavior, especially on Linux, and it
will look familiar to Slurm and HPC operators.

The connector emits a `galvofs-nfsd` plan with:

- a `configureKernelNfsdExport` action;
- an `exportfs` command template;
- an `exportfs -u` cleanup command template;
- a follow-on `mountNfs4` client-side action.

The client selector is intentionally represented as
`${FEMTO_GALVOFS_NFSD_CLIENTS}` until the Slurm site connector supplies the
authorized node range. Apply mode refuses to run unless this value is set to a
non-empty, non-wildcard selector without whitespace.

Tradeoffs and guardrails:

- requires elevated privileges or host-level service management;
- mutates global host export state;
- behaves differently across Linux, macOS, and managed environments;
- needs careful cleanup on lease expiry and cancellation;
- is hard to run safely inside local developer tests.

Therefore, the default connector behavior remains **plan only**. Apply mode is
explicit and narrow:

- `--apply-nfsd --dry-run` returns the concrete `exportfs` command report
  without executing it;
- `--apply-nfsd` executes `exportfs` without a shell;
- only `configureKernelNfsdExport` actions are applied;
- client-side `mountNfs4` actions are reported as skipped because runtime or
  site operators own client mounts.

Lease-expiry cleanup, export reconciliation, and Slurm/KubeNyx operator wiring
are still follow-on work.

### User-Space NFSv4 Service

A user-space service remains a fallback if a deployment cannot grant `nfsd`
control, but it is no longer the primary direction.
