# GalvoFS Connector

`galvofs-connector` consumes GalvoFS access endpoint manifests and turns
them into connector actions.

Current scope:

- read `GET /v0/runs/{run_id}/galvofs/access-endpoints` JSON from stdin;
- produce connector plans for KubeNyx CSI handoff;
- produce connector plans for Slurm NFSv4 staging handoff through kernel
  `nfsd`/`exportfs`;
- keep privileged apply behavior out of scope until storage inputs and
  access-plane policy are fully defined.

The GalvoFS foundation and Storage Inputs V1 local/uploaded path are in place.
This connector can turn Femto endpoint metadata into concrete CSI and
NFSD/NFSv4 plans. NFSD apply mode, a real CSI driver, and runtime operators
remain roadmap work after access-plane policy is settled.

Example:

```bash
curl -s http://127.0.0.1:8787/v0/runs/${RUN_ID}/galvofs/access-endpoints \
  | cargo run -p galvofs-connector
```

## NFSD Direction

The selected NFSv4 direction is kernel `nfsd`.

### Kernel `nfsd`

Kernel `nfsd` uses the operating system NFS server and export table. It is the
highest-fidelity path for production NFS behavior, especially on Linux, and it
will look familiar to Slurm and HPC operators.

The connector currently emits a `galvofs-nfsd` plan with:

- a `configureKernelNfsdExport` action;
- an `exportfs` command template;
- an `exportfs -u` cleanup command template;
- a follow-on `mountNfs4` client-side action.

The client selector is intentionally represented as
`${FEMTO_GALVOFS_NFSD_CLIENTS}` until the Slurm site connector supplies the
authorized node range. This avoids accidentally planning a broad export.

Tradeoffs and guardrails:

- requires elevated privileges or host-level service management;
- mutates global host export state;
- behaves differently across Linux, macOS, and managed environments;
- needs careful cleanup on lease expiry and cancellation;
- is hard to run safely inside local developer tests.

Therefore, the default connector behavior is still **plan only**. A future
`apply` mode should require an explicit flag, a configured
`FEMTO_GALVOFS_NFSD_CLIENTS` value, and lease-expiry cleanup wiring.

### User-Space NFSv4 Service

A user-space service remains a fallback if a deployment cannot grant `nfsd`
control, but it is no longer the primary direction.
