use std::collections::BTreeMap;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use opendal::services;
use opendal::{ErrorKind as OpenDalErrorKind, Operator};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use time::OffsetDateTime;
use tokio::fs;
use tokio::io::AsyncWriteExt;
use uuid::Uuid;

use crate::error::{GalvoFsError, GalvoFsResult};

const SHA256_PREFIX: &str = "sha256:";
const MAX_STORAGE_INPUT_FILES: usize = 10_000;
const MAX_STORAGE_INPUT_BYTES: u64 = 1024 * 1024 * 1024;

/// How long a workspace lock may go untouched before another caller breaks it.
///
/// A process killed while holding the lock leaves the file behind, and nothing
/// else can ever mutate that workspace again unless the lock can be broken.
/// The window is far longer than any legitimate hold, which is a metadata read
/// and a metadata write.
const WORKSPACE_LOCK_STALE_AFTER: std::time::Duration = std::time::Duration::from_secs(30);
const WORKSPACE_LOCK_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(5);
const WORKSPACE_LOCK_ACQUIRE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

#[derive(Clone)]
pub struct GalvoFs {
    root: Arc<PathBuf>,
    object_store: Arc<dyn GalvoFsObjectStore>,
    local_storage_input_roots: Arc<Vec<PathBuf>>,
}

impl std::fmt::Debug for GalvoFs {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("GalvoFs")
            .field("root", &self.root)
            .finish_non_exhaustive()
    }
}

impl GalvoFs {
    pub fn local(root: impl AsRef<Path>) -> Self {
        let root = root.as_ref().to_path_buf();
        Self {
            object_store: Arc::new(LocalGalvoFsObjectStore::new(root.join("objects"))),
            root: Arc::new(root),
            local_storage_input_roots: Arc::new(Vec::new()),
        }
    }

    pub fn local_with_storage_input_roots(
        root: impl AsRef<Path>,
        local_storage_input_roots: impl IntoIterator<Item = impl AsRef<Path>>,
    ) -> Self {
        let root = root.as_ref().to_path_buf();
        Self {
            object_store: Arc::new(LocalGalvoFsObjectStore::new(root.join("objects"))),
            root: Arc::new(root),
            local_storage_input_roots: Arc::new(
                local_storage_input_roots
                    .into_iter()
                    .map(|path| path.as_ref().to_path_buf())
                    .collect(),
            ),
        }
    }

    pub fn with_storage_input_roots(
        mut self,
        local_storage_input_roots: impl IntoIterator<Item = impl AsRef<Path>>,
    ) -> Self {
        self.local_storage_input_roots = Arc::new(
            local_storage_input_roots
                .into_iter()
                .map(|path| path.as_ref().to_path_buf())
                .collect(),
        );
        self
    }

    pub fn opendal_fs(
        metadata_root: impl AsRef<Path>,
        object_root: impl AsRef<Path>,
    ) -> GalvoFsResult<Self> {
        let metadata_root = metadata_root.as_ref().to_path_buf();
        let object_root = object_root.as_ref().to_path_buf();
        let builder = services::Fs::default().root(&object_root.to_string_lossy());
        let operator = Operator::new(builder)
            .map_err(opendal_config_error)?
            .finish();
        Ok(Self {
            object_store: Arc::new(OpenDalGalvoFsObjectStore::new(operator)),
            root: Arc::new(metadata_root),
            local_storage_input_roots: Arc::new(Vec::new()),
        })
    }

    pub fn opendal_s3(
        metadata_root: impl AsRef<Path>,
        config: GalvoFsS3Config,
    ) -> GalvoFsResult<Self> {
        config.validate()?;
        let mut builder = services::S3::default()
            .bucket(&config.bucket)
            .root(&config.root);
        if let Some(endpoint) = &config.endpoint {
            builder = builder.endpoint(endpoint);
        }
        if let Some(region) = &config.region {
            builder = builder.region(region);
        }
        if let Some(access_key_id) = &config.access_key_id {
            builder = builder.access_key_id(access_key_id);
        }
        if let Some(secret_access_key) = &config.secret_access_key {
            builder = builder.secret_access_key(secret_access_key);
        }
        let operator = Operator::new(builder)
            .map_err(opendal_config_error)?
            .finish();
        Ok(Self {
            object_store: Arc::new(OpenDalGalvoFsObjectStore::new(operator)),
            root: Arc::new(metadata_root.as_ref().to_path_buf()),
            local_storage_input_roots: Arc::new(Vec::new()),
        })
    }

    pub async fn create_workspace(
        &self,
        request: CreateGalvoWorkspaceRequest,
    ) -> GalvoFsResult<WorkspaceRoot> {
        request.validate()?;
        self.ensure_metadata_dirs().await?;

        let workspace = WorkspaceRoot {
            workspace_id: format!("ws-{}", Uuid::new_v4()),
            tenant_id: request.tenant_id,
            project_id: request.project_id,
            owner_id: request.owner_id,
            workspace_name: request.workspace_name,
            purpose: request.purpose,
            created_at: OffsetDateTime::now_utc(),
        };
        // A brand new workspace: no prior document to race with, so this is a
        // create rather than a read-modify-write and needs no lock.
        let state = WorkspaceState {
            root: workspace.clone(),
            files: BTreeMap::new(),
            storage_inputs: Vec::new(),
            version: 0,
        };
        self.write_workspace_state(&state).await?;
        self.append_receipt(StorageReceipt::new("workspace.create").for_workspace(&workspace))
            .await?;
        Ok(workspace)
    }

    pub async fn write_file(
        &self,
        workspace_id: &str,
        relative_path: &str,
        bytes: &[u8],
    ) -> GalvoFsResult<GalvoFsFileRecord> {
        let path = normalize_galvo_path(relative_path)?;
        let digest = sha256_digest(bytes);
        // The body goes in before the lock. It is content-addressed and so
        // idempotent, an unreferenced one is already reclaimable, and the
        // object store may be S3 — holding a workspace lock across that would
        // serialize every writer behind a network round trip.
        let object_uri = self.object_store.put_object(&digest, bytes).await?;
        let _lock = self.lock_workspace(workspace_id).await?;
        let mut state = self.read_workspace_state(workspace_id).await?;
        let record = GalvoFsFileRecord {
            path: path.clone(),
            kind: GalvoFsFileKind::File,
            size_bytes: bytes.len() as u64,
            digest,
            object_uri,
        };
        state.files.insert(path, record.clone());
        self.commit_workspace_state(&mut state).await?;
        self.append_receipt(
            StorageReceipt::new("file.write")
                .for_workspace(&state.root)
                .with_path(&record.path)
                .with_object_digest(&record.digest),
        )
        .await?;
        Ok(record)
    }

    pub async fn write_file_with_lease(
        &self,
        lease_id: &str,
        relative_path: &str,
        bytes: &[u8],
    ) -> GalvoFsResult<GalvoFsFileRecord> {
        let lease = self
            .authorize_lease(lease_id, GalvoFsLeaseAction::Write)
            .await?;
        let path = normalize_galvo_path(relative_path)?;
        let digest = sha256_digest(bytes);
        let object_uri = self.object_store.put_object(&digest, bytes).await?;
        let _lock = self.lock_workspace(&lease.workspace_id).await?;
        let mut state = self.read_workspace_state(&lease.workspace_id).await?;
        let record = GalvoFsFileRecord {
            path: path.clone(),
            kind: GalvoFsFileKind::File,
            size_bytes: bytes.len() as u64,
            digest,
            object_uri,
        };
        state.files.insert(path, record.clone());
        self.commit_workspace_state(&mut state).await?;
        self.append_receipt(
            StorageReceipt::new("file.write")
                .for_workspace(&state.root)
                .with_lease(&lease.lease_id)
                .with_path(&record.path)
                .with_object_digest(&record.digest),
        )
        .await?;
        Ok(record)
    }

    /// Write several files as one unit: all of them land, or none of them do.
    ///
    /// `write_file` in a loop cannot offer this. A failure on the third of five
    /// leaves two written and returns an error, so a caller holding a
    /// multi-file change — a patch, most obviously — has no way to apply it
    /// without risking a half-applied result it then has to describe honestly.
    ///
    /// The shape that makes this work is already here: bodies are
    /// content-addressed and stored separately from the index, and the index is
    /// a single document. So every body goes in first, outside the lock, where
    /// a failure costs nothing — an unreferenced body is reclaimable and an
    /// identical one is a no-op — and then a single locked, version-checked
    /// write installs all N records at once.
    ///
    /// Duplicate paths are refused rather than resolved. Two entries for one
    /// path means the caller believes it is writing two files; silently keeping
    /// the last would report N written when N-1 landed.
    pub async fn commit_files(
        &self,
        workspace_id: &str,
        files: &[(&str, &[u8])],
    ) -> GalvoFsResult<Vec<GalvoFsFileRecord>> {
        let records = self.stage_file_bodies(files).await?;

        let _lock = self.lock_workspace(workspace_id).await?;
        let mut state = self.read_workspace_state(workspace_id).await?;
        for record in &records {
            state.files.insert(record.path.clone(), record.clone());
        }
        self.commit_workspace_state(&mut state).await?;

        // After the commit, so a receipt never describes a write that was
        // refused. A receipt-append failure here leaves the commit standing and
        // the audit trail short, which is the lesser of the two.
        for record in &records {
            self.append_receipt(
                StorageReceipt::new("file.commit")
                    .for_workspace(&state.root)
                    .with_path(&record.path)
                    .with_object_digest(&record.digest),
            )
            .await?;
        }
        Ok(records)
    }

    /// `commit_files` against a lease, authorized for write.
    pub async fn commit_files_with_lease(
        &self,
        lease_id: &str,
        files: &[(&str, &[u8])],
    ) -> GalvoFsResult<Vec<GalvoFsFileRecord>> {
        let lease = self
            .authorize_lease(lease_id, GalvoFsLeaseAction::Write)
            .await?;
        let records = self.stage_file_bodies(files).await?;

        let _lock = self.lock_workspace(&lease.workspace_id).await?;
        let mut state = self.read_workspace_state(&lease.workspace_id).await?;
        for record in &records {
            state.files.insert(record.path.clone(), record.clone());
        }
        self.commit_workspace_state(&mut state).await?;

        for record in &records {
            self.append_receipt(
                StorageReceipt::new("file.commit")
                    .for_workspace(&state.root)
                    .with_lease(&lease.lease_id)
                    .with_path(&record.path)
                    .with_object_digest(&record.digest),
            )
            .await?;
        }
        Ok(records)
    }

    /// Validate every path and store every body, before the lock is taken.
    ///
    /// Both validation and body storage happen here so that everything able to
    /// fail for a reason the caller controls has already failed by the time the
    /// workspace document is touched.
    async fn stage_file_bodies(
        &self,
        files: &[(&str, &[u8])],
    ) -> GalvoFsResult<Vec<GalvoFsFileRecord>> {
        if files.is_empty() {
            return Err(GalvoFsError::AdmissionRejected(
                "GalvoFS commit contains no files".into(),
            ));
        }

        let mut normalized = Vec::with_capacity(files.len());
        let mut seen: BTreeMap<String, ()> = BTreeMap::new();
        for (path, bytes) in files {
            let path = normalize_galvo_path(path)?;
            if seen.insert(path.clone(), ()).is_some() {
                return Err(GalvoFsError::AdmissionRejected(format!(
                    "GalvoFS commit names {path} more than once"
                )));
            }
            normalized.push((path, *bytes));
        }

        let mut records = Vec::with_capacity(normalized.len());
        for (path, bytes) in normalized {
            let digest = sha256_digest(bytes);
            let object_uri = self.object_store.put_object(&digest, bytes).await?;
            records.push(GalvoFsFileRecord {
                path,
                kind: GalvoFsFileKind::File,
                size_bytes: bytes.len() as u64,
                digest,
                object_uri,
            });
        }
        Ok(records)
    }

    pub async fn read_file(
        &self,
        workspace_id: &str,
        relative_path: &str,
    ) -> GalvoFsResult<GalvoFsFileData> {
        let path = normalize_galvo_path(relative_path)?;
        let state = self.read_workspace_state(workspace_id).await?;
        let record = state.files.get(&path).cloned().ok_or_else(|| {
            GalvoFsError::ArtifactMissing(format!("GalvoFS file {relative_path} not found"))
        })?;
        let bytes = self.object_store.get_object(&record.digest).await?;
        self.append_receipt(
            StorageReceipt::new("file.read")
                .for_workspace(&state.root)
                .with_path(&record.path)
                .with_object_digest(&record.digest),
        )
        .await?;
        Ok(GalvoFsFileData { record, bytes })
    }

    pub async fn read_file_with_lease(
        &self,
        lease_id: &str,
        relative_path: &str,
    ) -> GalvoFsResult<GalvoFsFileData> {
        let lease = self
            .authorize_lease(lease_id, GalvoFsLeaseAction::Read)
            .await?;
        let path = normalize_galvo_path(relative_path)?;
        let state = self.read_workspace_state(&lease.workspace_id).await?;
        let record = state.files.get(&path).cloned().ok_or_else(|| {
            GalvoFsError::ArtifactMissing(format!("GalvoFS file {relative_path} not found"))
        })?;
        let bytes = self.object_store.get_object(&record.digest).await?;
        self.append_receipt(
            StorageReceipt::new("file.read")
                .for_workspace(&state.root)
                .with_lease(&lease.lease_id)
                .with_path(&record.path)
                .with_object_digest(&record.digest),
        )
        .await?;
        Ok(GalvoFsFileData { record, bytes })
    }

    pub async fn list_files(&self, workspace_id: &str) -> GalvoFsResult<Vec<GalvoFsFileRecord>> {
        let state = self.read_workspace_state(workspace_id).await?;
        Ok(state.files.values().cloned().collect())
    }

    pub async fn list_files_with_lease(
        &self,
        lease_id: &str,
    ) -> GalvoFsResult<Vec<GalvoFsFileRecord>> {
        let lease = self
            .authorize_lease(lease_id, GalvoFsLeaseAction::Read)
            .await?;
        self.list_files(&lease.workspace_id).await
    }

    pub async fn issue_storage_lease(
        &self,
        request: IssueGalvoStorageLeaseRequest,
    ) -> GalvoFsResult<StorageLease> {
        request.validate()?;
        self.ensure_metadata_dirs().await?;

        let state = self.read_workspace_state(&request.workspace_id).await?;
        if state.root.tenant_id != request.tenant_id || state.root.project_id != request.project_id
        {
            return Err(GalvoFsError::AdmissionRejected(format!(
                "GalvoFS storage lease tenant/project does not match workspace {}",
                request.workspace_id
            )));
        }

        let lease = StorageLease {
            lease_id: format!("lease-{}", Uuid::new_v4()),
            tenant_id: request.tenant_id,
            project_id: request.project_id,
            secure_entity_id: request.secure_entity_id,
            work_order_id: request.work_order_id,
            a2e_ticket_id: request.a2e_ticket_id,
            runtime_id: request.runtime_id,
            workspace_id: state.root.workspace_id.clone(),
            access: request.access,
            issued_at: OffsetDateTime::now_utc(),
            expires_at: request.expires_at,
        };
        write_json(&self.lease_path(&lease.lease_id), &lease).await?;
        self.append_receipt(
            StorageReceipt::new("storage-lease.issue")
                .for_workspace(&state.root)
                .with_lease(&lease.lease_id),
        )
        .await?;
        Ok(lease)
    }

    pub async fn create_mount_manifest(
        &self,
        request: CreateGalvoMountManifestRequest,
    ) -> GalvoFsResult<MountManifest> {
        request.validate()?;
        self.ensure_metadata_dirs().await?;

        let state = self.read_workspace_state(&request.workspace_id).await?;
        if let Some(lease_id) = &request.lease_id {
            let required_action = if request
                .mounts
                .iter()
                .any(|mount| mount.access.requires_write())
            {
                GalvoFsLeaseAction::Write
            } else {
                GalvoFsLeaseAction::Read
            };
            let lease = self.authorize_lease(lease_id, required_action).await?;
            if lease.workspace_id != request.workspace_id {
                return Err(GalvoFsError::AdmissionRejected(format!(
                    "GalvoFS lease {} is not scoped to workspace {}",
                    lease.lease_id, request.workspace_id
                )));
            }
        }
        for mount in &request.mounts {
            if let Some(input_id) = &mount.storage_input_id {
                let input = self.read_storage_input(input_id).await?;
                if input.tenant_id != state.root.tenant_id
                    || input.project_id != state.root.project_id
                {
                    return Err(GalvoFsError::AdmissionRejected(format!(
                        "GalvoFS storage input {} tenant/project does not match workspace {}",
                        input.input_id, state.root.workspace_id
                    )));
                }
                if !state
                    .storage_inputs
                    .iter()
                    .any(|binding| binding.input_id == input.input_id)
                {
                    return Err(GalvoFsError::AdmissionRejected(format!(
                        "GalvoFS storage input {} is not attached to workspace {}",
                        input.input_id, state.root.workspace_id
                    )));
                }
            }
        }

        let mut mounts = request.mounts;
        mounts.sort_by(|left, right| {
            left.name
                .cmp(&right.name)
                .then(left.path.cmp(&right.path))
                .then(left.source_uri.cmp(&right.source_uri))
                .then(left.storage_input_id.cmp(&right.storage_input_id))
        });
        let digest = mount_manifest_digest(
            &request.workspace_id,
            request.lease_id.as_deref(),
            &request.policy_digest,
            &mounts,
        )?;
        let manifest = MountManifest {
            mount_manifest_id: format!("mount-manifest-{}", Uuid::new_v4()),
            workspace_id: state.root.workspace_id.clone(),
            lease_id: request.lease_id,
            policy_digest: request.policy_digest,
            digest,
            mounts,
            created_at: OffsetDateTime::now_utc(),
        };
        write_json(
            &self.mount_manifest_path(&manifest.mount_manifest_id),
            &manifest,
        )
        .await?;
        self.append_receipt(
            StorageReceipt::new("mount-manifest.create")
                .for_workspace(&state.root)
                .with_optional_lease(manifest.lease_id.as_deref())
                .with_mount_manifest(&manifest.digest),
        )
        .await?;
        Ok(manifest)
    }

    pub async fn create_storage_input(
        &self,
        request: CreateGalvoStorageInputRequest,
    ) -> GalvoFsResult<GalvoFsStorageInput> {
        request.validate()?;
        self.ensure_metadata_dirs().await?;

        let input = GalvoFsStorageInput {
            input_id: format!("storage-input-{}", Uuid::new_v4()),
            tenant_id: request.tenant_id,
            project_id: request.project_id,
            name: request.name,
            source_kind: request.source_kind,
            source_uri: request.source_uri,
            materialization: request.materialization,
            access: request.access,
            mount_path: request.mount_path,
            digest_policy: request.digest_policy,
            verification_status: storage_input_verification_status(
                request.source_kind,
                request.digest_policy,
            ),
            retention_policy: request.retention_policy,
            writeback_policy: request.writeback_policy,
            created_at: OffsetDateTime::now_utc(),
        };
        write_json(&self.storage_input_path(&input.input_id), &input).await?;
        self.append_receipt(
            StorageReceipt::new("storage-input.create")
                .with_storage_input(&input.input_id)
                .with_source_uri(&input.source_uri)
                .with_tenant_project(&input.tenant_id, &input.project_id),
        )
        .await?;
        Ok(input)
    }

    pub async fn attach_storage_input(
        &self,
        request: AttachGalvoStorageInputRequest,
    ) -> GalvoFsResult<GalvoFsStorageInputBinding> {
        request.validate()?;
        self.ensure_metadata_dirs().await?;

        // Held across materialization, which mutates the state in place and may
        // import up to MAX_STORAGE_INPUT_FILES. An import that outruns
        // WORKSPACE_LOCK_STALE_AFTER can have its lock broken by another
        // writer; the version check then fails the attach with a Conflict
        // rather than letting two writers both commit. Wasteful — the imported
        // bodies are content-addressed and simply become reclaimable — but not
        // corrupting, and a retry succeeds.
        let _lock = self.lock_workspace(&request.workspace_id).await?;
        let mut state = self.read_workspace_state(&request.workspace_id).await?;
        let input = self.read_storage_input(&request.input_id).await?;
        if input.tenant_id != state.root.tenant_id || input.project_id != state.root.project_id {
            return Err(GalvoFsError::AdmissionRejected(format!(
                "GalvoFS storage input {} tenant/project does not match workspace {}",
                input.input_id, state.root.workspace_id
            )));
        }
        if let Some(lease_id) = &request.lease_id {
            let required_action = if request.access.requires_write() {
                GalvoFsLeaseAction::Write
            } else {
                GalvoFsLeaseAction::Read
            };
            let lease = self.authorize_lease(lease_id, required_action).await?;
            if lease.workspace_id != request.workspace_id {
                return Err(GalvoFsError::AdmissionRejected(format!(
                    "GalvoFS lease {} is not scoped to workspace {}",
                    lease.lease_id, request.workspace_id
                )));
            }
        }

        let mut binding = GalvoFsStorageInputBinding {
            binding_id: format!("storage-input-binding-{}", Uuid::new_v4()),
            workspace_id: state.root.workspace_id.clone(),
            input_id: input.input_id.clone(),
            lease_id: request.lease_id,
            name: input.name.clone(),
            source_kind: input.source_kind,
            source_uri: input.source_uri.clone(),
            materialization: request.materialization,
            access: request.access,
            mount_path: request.mount_path,
            digest_policy: input.digest_policy,
            verification_status: input.verification_status,
            retention_policy: input.retention_policy.clone(),
            writeback_policy: request.writeback_policy,
            baseline_snapshot_id: None,
            attached_at: OffsetDateTime::now_utc(),
        };

        if binding.materialization == GalvoFsInputMaterialization::Import {
            self.materialize_storage_input(&mut state, &input, &mut binding)
                .await?;
            // Pinned from the IN-MEMORY state, not through `snapshot_workspace`,
            // which reads from disk and would capture the workspace as it was
            // before this materialization — a baseline that omits exactly the
            // files writeback exists to reason about.
            let baseline = snapshot_from_state(&state, format!("baseline-{}", binding.binding_id));
            write_json(&self.snapshot_path(&baseline.snapshot_id), &baseline).await?;
            binding.baseline_snapshot_id = Some(baseline.snapshot_id.clone());
        }

        state.storage_inputs.retain(|existing| {
            existing.input_id != binding.input_id || existing.mount_path != binding.mount_path
        });
        state.storage_inputs.push(binding.clone());
        state.storage_inputs.sort_by(|left, right| {
            left.mount_path
                .cmp(&right.mount_path)
                .then(left.input_id.cmp(&right.input_id))
        });
        self.commit_workspace_state(&mut state).await?;
        self.append_receipt(
            StorageReceipt::new("storage-input.attach")
                .for_workspace(&state.root)
                .with_optional_lease(binding.lease_id.as_deref())
                .with_storage_input(&binding.input_id)
                .with_source_uri(&binding.source_uri),
        )
        .await?;
        Ok(binding)
    }

    pub async fn workspace_storage_inputs(
        &self,
        workspace_id: &str,
    ) -> GalvoFsResult<Vec<GalvoFsStorageInputBinding>> {
        require_non_empty("workspaceId", workspace_id)?;
        let state = self.read_workspace_state(workspace_id).await?;
        Ok(state.storage_inputs)
    }

    pub async fn upload_storage_object(
        &self,
        request: UploadGalvoStorageObjectRequest,
    ) -> GalvoFsResult<GalvoFsUploadedObject> {
        request.validate()?;
        self.ensure_metadata_dirs().await?;
        let digest = sha256_digest(&request.bytes);
        if let Some(expected_digest) = &request.expected_digest {
            if expected_digest != &digest {
                return Err(GalvoFsError::ManifestInvalid(format!(
                    "GalvoFS uploaded object digest mismatch: expected {expected_digest}, got {digest}"
                )));
            }
        }
        let object_uri = self
            .object_store
            .put_object(&digest, &request.bytes)
            .await?;
        let uploaded = GalvoFsUploadedObject {
            digest,
            object_uri,
            size_bytes: request.bytes.len() as u64,
            content_type: request.content_type,
            uploaded_at: OffsetDateTime::now_utc(),
        };
        self.append_receipt(
            StorageReceipt::new("storage-object.upload")
                .with_object_digest(&uploaded.digest)
                .with_source_uri(&uploaded.object_uri),
        )
        .await?;
        Ok(uploaded)
    }

    pub async fn storage_input_materialization_tasks(
        &self,
        input_id: &str,
    ) -> GalvoFsResult<Vec<GalvoFsMaterializationTask>> {
        require_non_empty("storageInputId", input_id)?;
        let mut tasks = read_json_dir::<GalvoFsMaterializationTask>(
            &self.storage_input_materialization_tasks_root(),
        )
        .await?
        .into_iter()
        .filter(|task| task.input_id == input_id)
        .collect::<Vec<_>>();
        tasks.sort_by(|left, right| {
            left.started_at
                .cmp(&right.started_at)
                .then(left.task_id.cmp(&right.task_id))
        });
        Ok(tasks)
    }

    pub async fn find_mount_manifest_by_digest(
        &self,
        workspace_id: &str,
        digest: &str,
    ) -> GalvoFsResult<Option<MountManifest>> {
        let manifests = read_json_dir::<MountManifest>(&self.mount_manifests_root()).await?;
        Ok(manifests
            .into_iter()
            .find(|manifest| manifest.workspace_id == workspace_id && manifest.digest == digest))
    }

    pub async fn snapshot_workspace(
        &self,
        workspace_id: &str,
        label: impl Into<String>,
    ) -> GalvoFsResult<SnapshotPin> {
        let state = self.read_workspace_state(workspace_id).await?;
        let snapshot = snapshot_from_state(&state, label);
        write_json(&self.snapshot_path(&snapshot.snapshot_id), &snapshot).await?;
        self.append_receipt(
            StorageReceipt::new("workspace.snapshot")
                .for_workspace(&state.root)
                .with_snapshot(&snapshot.snapshot_id),
        )
        .await?;
        Ok(snapshot)
    }

    pub async fn snapshot_pin(&self, snapshot_id: &str) -> GalvoFsResult<SnapshotPin> {
        self.read_snapshot(snapshot_id).await
    }

    pub async fn clone_workspace(
        &self,
        snapshot_id: &str,
        request: CreateGalvoWorkspaceRequest,
    ) -> GalvoFsResult<WorkspaceClone> {
        request.validate()?;
        self.ensure_metadata_dirs().await?;

        let snapshot = self.read_snapshot(snapshot_id).await?;
        let workspace = WorkspaceRoot {
            workspace_id: format!("ws-{}", Uuid::new_v4()),
            tenant_id: request.tenant_id,
            project_id: request.project_id,
            owner_id: request.owner_id,
            workspace_name: request.workspace_name,
            purpose: request.purpose,
            created_at: OffsetDateTime::now_utc(),
        };
        let state = WorkspaceState {
            root: workspace.clone(),
            files: snapshot
                .files
                .iter()
                .cloned()
                .map(|record| (record.path.clone(), record))
                .collect(),
            storage_inputs: Vec::new(),
            // The clone is a new workspace under a new id, so its history
            // starts here rather than continuing the source's.
            version: 0,
        };
        self.write_workspace_state(&state).await?;
        let clone = WorkspaceClone {
            clone_id: format!("clone-{}", Uuid::new_v4()),
            source_snapshot_id: snapshot.snapshot_id,
            workspace,
        };
        self.append_receipt(
            StorageReceipt::new("workspace.clone")
                .for_workspace(&clone.workspace)
                .with_snapshot(&clone.source_snapshot_id),
        )
        .await?;
        Ok(clone)
    }

    pub async fn diff_workspace(
        &self,
        workspace_id: &str,
        baseline_snapshot_id: &str,
    ) -> GalvoFsResult<WorkspaceDiff> {
        let state = self.read_workspace_state(workspace_id).await?;
        let snapshot = self.read_snapshot(baseline_snapshot_id).await?;
        let baseline: BTreeMap<String, GalvoFsFileRecord> = snapshot
            .files
            .into_iter()
            .map(|record| (record.path.clone(), record))
            .collect();
        let mut changes = Vec::new();

        for (path, after) in &state.files {
            match baseline.get(path) {
                None => changes.push(GalvoFsDiffEntry {
                    path: path.clone(),
                    kind: GalvoFsDiffKind::Added,
                    before_digest: None,
                    after_digest: Some(after.digest.clone()),
                    before_size_bytes: None,
                    after_size_bytes: Some(after.size_bytes),
                }),
                Some(before)
                    if before.digest != after.digest || before.size_bytes != after.size_bytes =>
                {
                    changes.push(GalvoFsDiffEntry {
                        path: path.clone(),
                        kind: GalvoFsDiffKind::Modified,
                        before_digest: Some(before.digest.clone()),
                        after_digest: Some(after.digest.clone()),
                        before_size_bytes: Some(before.size_bytes),
                        after_size_bytes: Some(after.size_bytes),
                    });
                }
                Some(_) => {}
            }
        }

        for (path, before) in baseline {
            if !state.files.contains_key(&path) {
                changes.push(GalvoFsDiffEntry {
                    path,
                    kind: GalvoFsDiffKind::Removed,
                    before_digest: Some(before.digest),
                    after_digest: None,
                    before_size_bytes: Some(before.size_bytes),
                    after_size_bytes: None,
                });
            }
        }

        changes.sort_by(|left, right| left.path.cmp(&right.path));
        Ok(WorkspaceDiff {
            workspace_id: state.root.workspace_id,
            baseline_snapshot_id: baseline_snapshot_id.into(),
            changes,
        })
    }

    pub async fn rollback_workspace(
        &self,
        workspace_id: &str,
        snapshot_id: &str,
    ) -> GalvoFsResult<StorageReceipt> {
        let snapshot = self.read_snapshot(snapshot_id).await?;
        let _lock = self.lock_workspace(workspace_id).await?;
        let mut state = self.read_workspace_state(workspace_id).await?;
        state.files = snapshot
            .files
            .into_iter()
            .map(|record| (record.path.clone(), record))
            .collect();
        self.commit_workspace_state(&mut state).await?;
        let receipt = StorageReceipt::new("workspace.rollback")
            .for_workspace(&state.root)
            .with_snapshot(snapshot_id);
        self.append_receipt(receipt.clone()).await?;
        Ok(receipt)
    }

    /// Push what a workspace changed back toward the input it came from.
    ///
    /// Every refusal below happens before anything outside this store is
    /// touched. That ordering is the whole contract: a caller that gets an
    /// error has a source byte-for-byte as it was, and a caller that gets a
    /// report has the exact list of what moved.
    ///
    /// Explicitly invoked. Nothing is ever pushed outward as a side effect of
    /// an ordinary workspace write.
    pub async fn writeback(
        &self,
        workspace_id: &str,
        binding_id: &str,
        lease_id: Option<&str>,
    ) -> GalvoFsResult<WritebackReport> {
        if let Some(lease_id) = lease_id {
            let lease = self
                .authorize_lease(lease_id, GalvoFsLeaseAction::Write)
                .await?;
            if lease.workspace_id != workspace_id {
                return Err(GalvoFsError::AdmissionRejected(format!(
                    "GalvoFS lease {} is not scoped to workspace {workspace_id}",
                    lease.lease_id
                )));
            }
        }

        let state = self.read_workspace_state(workspace_id).await?;
        let binding = state
            .storage_inputs
            .iter()
            .find(|candidate| candidate.binding_id == binding_id)
            .cloned()
            .ok_or_else(|| {
                GalvoFsError::ArtifactMissing(format!(
                    "GalvoFS storage input binding {binding_id} is not attached to workspace \
                     {workspace_id}"
                ))
            })?;

        // A no-op that answers "done" is how a policy field stays dead without
        // anyone noticing, so `None` refuses out loud.
        if binding.writeback_policy == GalvoFsWritebackPolicy::None {
            return Err(GalvoFsError::AdmissionRejected(format!(
                "GalvoFS storage input binding {binding_id} has writebackPolicy \"none\"; \
                 nothing was written"
            )));
        }
        if binding.access == GalvoFsAccessMode::ReadOnly {
            return Err(GalvoFsError::AdmissionRejected(format!(
                "GalvoFS storage input binding {binding_id} is read-only"
            )));
        }
        let baseline_snapshot_id = binding.baseline_snapshot_id.clone().ok_or_else(|| {
            GalvoFsError::AdmissionRejected(format!(
                "GalvoFS storage input binding {binding_id} has no materialization baseline, so \
                 drift against its source cannot be judged; writeback is refused rather than \
                 overwriting blind"
            ))
        })?;

        let diff = self
            .diff_workspace(workspace_id, &baseline_snapshot_id)
            .await?;
        let prefix = workspace_prefix_from_runtime_mount_path(&binding.mount_path);
        let changed: Vec<GalvoFsDiffEntry> = diff
            .changes
            .into_iter()
            .filter(|change| workspace_path_under_prefix(&change.path, &prefix))
            .collect();

        // A workspace file can vanish from the index — `rollback_workspace`
        // replaces the map wholesale. Deleting somebody else's file because a
        // rollback happened here is not a decision this call gets to make.
        if let Some(removed) = changed
            .iter()
            .find(|change| change.kind == GalvoFsDiffKind::Removed)
        {
            return Err(GalvoFsError::AdmissionRejected(format!(
                "GalvoFS writeback would have to delete {} at the source, which it will not do",
                removed.path
            )));
        }

        match binding.writeback_policy {
            GalvoFsWritebackPolicy::PinArtifacts => {
                self.writeback_pin_artifacts(&binding, &baseline_snapshot_id, &changed)
                    .await
            }
            GalvoFsWritebackPolicy::External => {
                self.writeback_external(&binding, &baseline_snapshot_id, &changed, &prefix)
                    .await
            }
            // Refused above, before the diff was computed.
            GalvoFsWritebackPolicy::None => unreachable!("writebackPolicy none is refused earlier"),
        }
    }

    /// `PinArtifacts`: publish the changed files as immutable artifacts.
    ///
    /// Nothing outside this store is touched, so there is no drift to check and
    /// no partial-push to describe.
    async fn writeback_pin_artifacts(
        &self,
        binding: &GalvoFsStorageInputBinding,
        baseline_snapshot_id: &str,
        changed: &[GalvoFsDiffEntry],
    ) -> GalvoFsResult<WritebackReport> {
        let mut published = Vec::with_capacity(changed.len());
        for change in changed {
            published.push(
                self.publish_artifact(PublishGalvoArtifactRequest {
                    workspace_id: binding.workspace_id.clone(),
                    path: change.path.clone(),
                    content_type: None,
                    retention_policy: binding.retention_policy.clone(),
                })
                .await?,
            );
        }
        self.append_receipt(
            StorageReceipt::new("storage-input.writeback")
                .with_optional_lease(binding.lease_id.as_deref())
                .with_storage_input(&binding.input_id)
                .with_snapshot(baseline_snapshot_id),
        )
        .await?;

        Ok(WritebackReport {
            binding_id: binding.binding_id.clone(),
            workspace_id: binding.workspace_id.clone(),
            input_id: binding.input_id.clone(),
            policy: binding.writeback_policy,
            baseline_snapshot_id: baseline_snapshot_id.to_string(),
            written_paths: Vec::new(),
            published_artifacts: published,
            completed_at: OffsetDateTime::now_utc(),
        })
    }

    /// `External`: push the changed files back to the source directory.
    ///
    /// Local-path only. Every other source kind refuses by name rather than
    /// silently doing nothing — S3-prefix and site-filesystem inputs have no
    /// working materialization to write back to, so a stub here would be dead
    /// code that reads as support.
    async fn writeback_external(
        &self,
        binding: &GalvoFsStorageInputBinding,
        baseline_snapshot_id: &str,
        changed: &[GalvoFsDiffEntry],
        prefix: &str,
    ) -> GalvoFsResult<WritebackReport> {
        if binding.source_kind != GalvoFsStorageInputSourceKind::LocalPath {
            return Err(GalvoFsError::AdmissionRejected(format!(
                "GalvoFS external writeback supports local-path sources only; binding {} is {:?}",
                binding.binding_id, binding.source_kind
            )));
        }

        let source = local_path_from_source_uri(&binding.source_uri)?;
        let canonical_source = canonicalize_existing_path(&source).await?;
        // Re-checked on the way out, not just on the way in. This call writes
        // to a filesystem this process does not own, and an allowlist consulted
        // only at attach time is an allowlist that stops applying the moment
        // configuration changes.
        self.validate_local_storage_input_allowlist(&canonical_source)
            .await?;
        let metadata = fs::symlink_metadata(&canonical_source)
            .await
            .map_err(|error| {
                GalvoFsError::RuntimeFailed(format!(
                    "failed to stat GalvoFS writeback target {}: {error}",
                    canonical_source.display()
                ))
            })?;
        if metadata.file_type().is_symlink() {
            return Err(GalvoFsError::AdmissionRejected(
                "GalvoFS writeback target must not be a symlink".into(),
            ));
        }

        let targets =
            resolve_writeback_targets(&canonical_source, metadata.is_dir(), prefix, changed)?;
        assert_no_source_drift(&targets).await?;

        let mut written_paths = Vec::with_capacity(targets.len());
        for target in &targets {
            let bytes = self.object_store.get_object(&target.after_digest).await?;
            write_external_file(&target.path, &bytes).await?;
            written_paths.push(target.workspace_path.clone());
        }

        self.append_receipt(
            StorageReceipt::new("storage-input.writeback")
                .with_optional_lease(binding.lease_id.as_deref())
                .with_storage_input(&binding.input_id)
                .with_source_uri(&binding.source_uri)
                .with_snapshot(baseline_snapshot_id),
        )
        .await?;

        Ok(WritebackReport {
            binding_id: binding.binding_id.clone(),
            workspace_id: binding.workspace_id.clone(),
            input_id: binding.input_id.clone(),
            policy: binding.writeback_policy,
            baseline_snapshot_id: baseline_snapshot_id.to_string(),
            written_paths,
            published_artifacts: Vec::new(),
            completed_at: OffsetDateTime::now_utc(),
        })
    }

    pub async fn publish_artifact(
        &self,
        request: PublishGalvoArtifactRequest,
    ) -> GalvoFsResult<ArtifactManifest> {
        request.validate()?;
        let path = normalize_galvo_path(&request.path)?;
        let state = self.read_workspace_state(&request.workspace_id).await?;
        let record = state.files.get(&path).cloned().ok_or_else(|| {
            GalvoFsError::ArtifactMissing(format!(
                "GalvoFS artifact source {} not found",
                request.path
            ))
        })?;
        let artifact = ArtifactManifest {
            artifact_id: format!("artifact-{}", Uuid::new_v4()),
            workspace_id: state.root.workspace_id.clone(),
            path: record.path.clone(),
            digest: record.digest.clone(),
            object_uri: record.object_uri.clone(),
            size_bytes: record.size_bytes,
            content_type: request.content_type,
            retention_policy: request.retention_policy,
            published_at: OffsetDateTime::now_utc(),
        };
        write_json(&self.artifact_path(&artifact.artifact_id), &artifact).await?;
        self.append_receipt(
            StorageReceipt::new("artifact.publish")
                .for_workspace(&state.root)
                .with_path(&artifact.path)
                .with_object_digest(&artifact.digest)
                .with_artifact(&artifact.artifact_id),
        )
        .await?;
        Ok(artifact)
    }

    pub async fn publish_artifact_with_lease(
        &self,
        lease_id: &str,
        mut request: PublishGalvoArtifactRequest,
    ) -> GalvoFsResult<ArtifactManifest> {
        let lease = self
            .authorize_lease(lease_id, GalvoFsLeaseAction::Write)
            .await?;
        if request.workspace_id != lease.workspace_id {
            return Err(GalvoFsError::AdmissionRejected(format!(
                "GalvoFS lease {} is not scoped to workspace {}",
                lease.lease_id, request.workspace_id
            )));
        }
        request.workspace_id = lease.workspace_id;
        request.validate()?;
        let path = normalize_galvo_path(&request.path)?;
        let state = self.read_workspace_state(&request.workspace_id).await?;
        let record = state.files.get(&path).cloned().ok_or_else(|| {
            GalvoFsError::ArtifactMissing(format!(
                "GalvoFS artifact source {} not found",
                request.path
            ))
        })?;
        let artifact = ArtifactManifest {
            artifact_id: format!("artifact-{}", Uuid::new_v4()),
            workspace_id: state.root.workspace_id.clone(),
            path: record.path.clone(),
            digest: record.digest.clone(),
            object_uri: record.object_uri.clone(),
            size_bytes: record.size_bytes,
            content_type: request.content_type,
            retention_policy: request.retention_policy,
            published_at: OffsetDateTime::now_utc(),
        };
        write_json(&self.artifact_path(&artifact.artifact_id), &artifact).await?;
        self.append_receipt(
            StorageReceipt::new("artifact.publish")
                .for_workspace(&state.root)
                .with_lease(&lease.lease_id)
                .with_path(&artifact.path)
                .with_object_digest(&artifact.digest)
                .with_artifact(&artifact.artifact_id),
        )
        .await?;
        Ok(artifact)
    }

    pub async fn artifacts_for_workspace(
        &self,
        workspace_id: &str,
    ) -> GalvoFsResult<Vec<ArtifactManifest>> {
        require_non_empty("workspaceId", workspace_id)?;
        let mut artifacts = read_json_dir::<ArtifactManifest>(&self.artifacts_root())
            .await?
            .into_iter()
            .filter(|artifact| artifact.workspace_id == workspace_id)
            .collect::<Vec<_>>();
        artifacts.sort_by(|left, right| {
            left.published_at
                .cmp(&right.published_at)
                .then_with(|| left.artifact_id.cmp(&right.artifact_id))
        });
        Ok(artifacts)
    }

    pub async fn receipts(&self) -> GalvoFsResult<Vec<StorageReceipt>> {
        let path = self.receipts_path();
        let bytes = match fs::read(&path).await {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => {
                return Err(GalvoFsError::RuntimeFailed(format!(
                    "failed to read GalvoFS receipts: {error}"
                )));
            }
        };
        let mut receipts = Vec::new();
        for line in String::from_utf8_lossy(&bytes).lines() {
            if line.trim().is_empty() {
                continue;
            }
            receipts.push(serde_json::from_str(line).map_err(|error| {
                GalvoFsError::RuntimeFailed(format!("failed to parse GalvoFS receipt: {error}"))
            })?);
        }
        Ok(receipts)
    }

    pub async fn verify_integrity(&self) -> GalvoFsResult<GalvoFsIntegrityReport> {
        let references = self.referenced_object_map().await?;
        let mut missing_objects = Vec::new();
        let mut corrupt_objects = Vec::new();
        for (digest, refs) in &references {
            match self.object_store.get_object(digest).await {
                Ok(bytes) => {
                    let actual_digest = sha256_digest(&bytes);
                    if actual_digest != *digest {
                        corrupt_objects.push(GalvoFsIntegrityObjectIssue {
                            digest: digest.clone(),
                            actual_digest: Some(actual_digest),
                            references: refs.clone(),
                        });
                    }
                }
                Err(GalvoFsError::ArtifactMissing(_)) => {
                    missing_objects.push(GalvoFsIntegrityObjectIssue {
                        digest: digest.clone(),
                        actual_digest: None,
                        references: refs.clone(),
                    });
                }
                Err(error) => return Err(error),
            }
        }

        let status = if missing_objects.is_empty() && corrupt_objects.is_empty() {
            GalvoFsIntegrityStatus::Healthy
        } else {
            GalvoFsIntegrityStatus::Degraded
        };

        Ok(GalvoFsIntegrityReport {
            status,
            checked_objects: references.len(),
            missing_objects,
            corrupt_objects,
        })
    }

    pub async fn collect_garbage(
        &self,
        request: GalvoFsGarbageCollectionRequest,
    ) -> GalvoFsResult<GalvoFsGarbageCollectionReport> {
        let references = self.referenced_object_map().await?;
        let all_objects = self.object_store.list_object_digests().await?;
        let mut candidate_objects = Vec::new();
        let mut retained_objects = Vec::new();

        for digest in all_objects {
            let record = GalvoFsObjectRecord {
                digest: digest.clone(),
                object_uri: object_uri(&digest)?,
            };
            if references.contains_key(&digest) {
                retained_objects.push(record);
            } else {
                candidate_objects.push(record);
            }
        }

        let mut removed_objects = Vec::new();
        if !request.dry_run {
            for object in &candidate_objects {
                self.object_store.delete_object(&object.digest).await?;
                removed_objects.push(object.clone());
            }
            if !removed_objects.is_empty() {
                self.append_receipt(StorageReceipt::new("object.gc"))
                    .await?;
            }
        }

        Ok(GalvoFsGarbageCollectionReport {
            dry_run: request.dry_run,
            candidate_objects,
            retained_objects,
            removed_objects,
        })
    }

    async fn materialize_storage_input(
        &self,
        state: &mut WorkspaceState,
        input: &GalvoFsStorageInput,
        binding: &mut GalvoFsStorageInputBinding,
    ) -> GalvoFsResult<GalvoFsMaterializationTask> {
        let mut task = GalvoFsMaterializationTask {
            task_id: format!("storage-input-materialization-{}", Uuid::new_v4()),
            input_id: input.input_id.clone(),
            workspace_id: state.root.workspace_id.clone(),
            lease_id: binding.lease_id.clone(),
            source_kind: input.source_kind,
            source_uri: input.source_uri.clone(),
            materialization: binding.materialization,
            status: GalvoFsMaterializationStatus::Running,
            files_materialized: 0,
            bytes_materialized: 0,
            error_message: None,
            started_at: OffsetDateTime::now_utc(),
            completed_at: None,
        };
        write_json(
            &self.storage_input_materialization_task_path(&task.task_id),
            &task,
        )
        .await?;
        self.append_receipt(
            StorageReceipt::new("storage-input.materialize.start")
                .for_workspace(&state.root)
                .with_optional_lease(binding.lease_id.as_deref())
                .with_storage_input(&input.input_id)
                .with_source_uri(&input.source_uri),
        )
        .await?;

        let outcome = match input.source_kind {
            GalvoFsStorageInputSourceKind::LocalPath => {
                self.materialize_local_path_input(state, input, binding)
                    .await
            }
            GalvoFsStorageInputSourceKind::UploadedObject => {
                self.materialize_uploaded_object_input(state, input, binding)
                    .await
            }
            GalvoFsStorageInputSourceKind::GalvofsWorkspace
            | GalvoFsStorageInputSourceKind::S3Prefix
            | GalvoFsStorageInputSourceKind::SiteFilesystemReference => {
                Err(GalvoFsError::AdmissionRejected(format!(
                    "GalvoFS {} storage input materialization is not implemented in V1",
                    storage_input_source_kind_label(input.source_kind)
                )))
            }
        };

        match outcome {
            Ok((files_materialized, bytes_materialized)) => {
                task.status = GalvoFsMaterializationStatus::Completed;
                task.files_materialized = files_materialized;
                task.bytes_materialized = bytes_materialized;
                task.completed_at = Some(OffsetDateTime::now_utc());
                binding.verification_status = GalvoFsInputVerificationStatus::Verified;
                self.update_storage_input_verification_status(
                    &input.input_id,
                    GalvoFsInputVerificationStatus::Verified,
                )
                .await?;
                write_json(
                    &self.storage_input_materialization_task_path(&task.task_id),
                    &task,
                )
                .await?;
                self.append_receipt(
                    StorageReceipt::new("storage-input.materialize.complete")
                        .for_workspace(&state.root)
                        .with_optional_lease(binding.lease_id.as_deref())
                        .with_storage_input(&input.input_id)
                        .with_source_uri(&input.source_uri),
                )
                .await?;
                Ok(task)
            }
            Err(error) => {
                task.status = GalvoFsMaterializationStatus::Failed;
                task.error_message = Some(error.to_string());
                task.completed_at = Some(OffsetDateTime::now_utc());
                write_json(
                    &self.storage_input_materialization_task_path(&task.task_id),
                    &task,
                )
                .await?;
                self.append_receipt(
                    StorageReceipt::new("storage-input.materialize.failed")
                        .for_workspace(&state.root)
                        .with_optional_lease(binding.lease_id.as_deref())
                        .with_storage_input(&input.input_id)
                        .with_source_uri(&input.source_uri),
                )
                .await?;
                Err(error)
            }
        }
    }

    async fn materialize_local_path_input(
        &self,
        state: &mut WorkspaceState,
        input: &GalvoFsStorageInput,
        binding: &GalvoFsStorageInputBinding,
    ) -> GalvoFsResult<(u64, u64)> {
        let source_path = local_path_from_source_uri(&input.source_uri)?;
        let canonical_source = canonicalize_existing_path(&source_path).await?;
        self.validate_local_storage_input_allowlist(&canonical_source)
            .await?;
        let metadata = fs::symlink_metadata(&canonical_source)
            .await
            .map_err(|error| {
                GalvoFsError::RuntimeFailed(format!(
                    "failed to stat GalvoFS local-path input {}: {error}",
                    canonical_source.display()
                ))
            })?;
        if metadata.file_type().is_symlink() {
            return Err(GalvoFsError::AdmissionRejected(
                "GalvoFS local-path input must not be a symlink".into(),
            ));
        }

        let base_prefix = workspace_prefix_from_runtime_mount_path(&binding.mount_path);
        if metadata.is_file() {
            let bytes = fs::read(&canonical_source).await.map_err(|error| {
                GalvoFsError::RuntimeFailed(format!(
                    "failed to read GalvoFS local-path input {}: {error}",
                    canonical_source.display()
                ))
            })?;
            enforce_materialization_limits(1, bytes.len() as u64)?;
            let file_name = canonical_source
                .file_name()
                .and_then(|name| name.to_str())
                .ok_or_else(|| {
                    GalvoFsError::ManifestInvalid(
                        "GalvoFS local-path file name must be UTF-8".into(),
                    )
                })?;
            let workspace_path = join_workspace_path(&base_prefix, file_name)?;
            self.insert_materialized_file(state, workspace_path, &bytes)
                .await?;
            return Ok((1, bytes.len() as u64));
        }
        if !metadata.is_dir() {
            return Err(GalvoFsError::AdmissionRejected(
                "GalvoFS local-path input must be a file or directory".into(),
            ));
        }

        let files = collect_local_input_files(&canonical_source).await?;
        let mut files_materialized = 0_u64;
        let mut bytes_materialized = 0_u64;
        for source_file in files {
            let bytes = fs::read(&source_file).await.map_err(|error| {
                GalvoFsError::RuntimeFailed(format!(
                    "failed to read GalvoFS local-path input {}: {error}",
                    source_file.display()
                ))
            })?;
            files_materialized = files_materialized.saturating_add(1);
            bytes_materialized = bytes_materialized.saturating_add(bytes.len() as u64);
            enforce_materialization_limits(files_materialized as usize, bytes_materialized)?;
            let relative = source_file
                .strip_prefix(&canonical_source)
                .map_err(|error| {
                    GalvoFsError::RuntimeFailed(format!(
                        "failed to derive GalvoFS local-path relative path: {error}"
                    ))
                })?;
            let relative = path_to_galvo_relative(relative)?;
            let workspace_path = join_workspace_path(&base_prefix, &relative)?;
            self.insert_materialized_file(state, workspace_path, &bytes)
                .await?;
        }
        Ok((files_materialized, bytes_materialized))
    }

    async fn materialize_uploaded_object_input(
        &self,
        state: &mut WorkspaceState,
        input: &GalvoFsStorageInput,
        binding: &GalvoFsStorageInputBinding,
    ) -> GalvoFsResult<(u64, u64)> {
        let digest = object_digest_from_uri(&input.source_uri)?;
        let bytes = self.object_store.get_object(&digest).await?;
        enforce_materialization_limits(1, bytes.len() as u64)?;
        let workspace_path = workspace_file_path_from_runtime_mount_path(&binding.mount_path)?;
        self.insert_materialized_file(state, workspace_path, &bytes)
            .await?;
        Ok((1, bytes.len() as u64))
    }

    async fn insert_materialized_file(
        &self,
        state: &mut WorkspaceState,
        workspace_path: String,
        bytes: &[u8],
    ) -> GalvoFsResult<()> {
        let path = normalize_galvo_path(&workspace_path)?;
        let digest = sha256_digest(bytes);
        let object_uri = self.object_store.put_object(&digest, bytes).await?;
        state.files.insert(
            path.clone(),
            GalvoFsFileRecord {
                path,
                kind: GalvoFsFileKind::File,
                size_bytes: bytes.len() as u64,
                digest,
                object_uri,
            },
        );
        Ok(())
    }

    async fn validate_local_storage_input_allowlist(&self, source: &Path) -> GalvoFsResult<()> {
        if self.local_storage_input_roots.is_empty() {
            return Err(GalvoFsError::AdmissionRejected(
                "GalvoFS local-path input requires configured allowlists".into(),
            ));
        }
        for root in self.local_storage_input_roots.iter() {
            let canonical_root = canonicalize_existing_path(root).await?;
            if source == canonical_root || source.starts_with(&canonical_root) {
                return Ok(());
            }
        }
        Err(GalvoFsError::AdmissionRejected(
            "GalvoFS local-path input is outside configured allowlists".into(),
        ))
    }

    async fn update_storage_input_verification_status(
        &self,
        input_id: &str,
        verification_status: GalvoFsInputVerificationStatus,
    ) -> GalvoFsResult<()> {
        let mut input = self.read_storage_input(input_id).await?;
        input.verification_status = verification_status;
        write_json(&self.storage_input_path(input_id), &input).await
    }

    async fn ensure_metadata_dirs(&self) -> GalvoFsResult<()> {
        for path in [
            self.metadata_root(),
            self.workspaces_root(),
            self.leases_root(),
            self.storage_inputs_root(),
            self.storage_input_materialization_tasks_root(),
            self.mount_manifests_root(),
            self.snapshots_root(),
            self.artifacts_root(),
        ] {
            fs::create_dir_all(path).await.map_err(|error| {
                GalvoFsError::RuntimeFailed(format!("failed to create GalvoFS metadata: {error}"))
            })?;
        }
        Ok(())
    }

    async fn append_receipt(&self, receipt: StorageReceipt) -> GalvoFsResult<()> {
        self.ensure_metadata_dirs().await?;
        let path = self.receipts_path();
        let mut line = serde_json::to_vec(&receipt).map_err(|error| {
            GalvoFsError::RuntimeFailed(format!("failed to encode GalvoFS receipt: {error}"))
        })?;
        line.push(b'\n');

        let mut options = fs::OpenOptions::new();
        options.create(true).append(true);
        let mut file = options.open(&path).await.map_err(|error| {
            GalvoFsError::RuntimeFailed(format!("failed to open GalvoFS receipts: {error}"))
        })?;
        use tokio::io::AsyncWriteExt;
        file.write_all(&line).await.map_err(|error| {
            GalvoFsError::RuntimeFailed(format!("failed to append GalvoFS receipt: {error}"))
        })?;
        Ok(())
    }

    async fn read_workspace_state(&self, workspace_id: &str) -> GalvoFsResult<WorkspaceState> {
        read_json(&self.workspace_path(workspace_id)).await
    }

    async fn write_workspace_state(&self, state: &WorkspaceState) -> GalvoFsResult<()> {
        write_json(&self.workspace_path(&state.root.workspace_id), state).await
    }

    /// Write a mutated workspace document, refusing if it moved underneath us.
    ///
    /// Callers hold the workspace lock across read-modify-write, so the version
    /// should always match. It is checked anyway: if the lock was broken as
    /// stale while still genuinely held, this is the only thing standing
    /// between two writers and a silently discarded record.
    async fn commit_workspace_state(&self, state: &mut WorkspaceState) -> GalvoFsResult<()> {
        let workspace_id = state.root.workspace_id.clone();
        let observed = self.read_workspace_state(&workspace_id).await?.version;
        if observed != state.version {
            return Err(GalvoFsError::Conflict(format!(
                "GalvoFS workspace {workspace_id} changed during this write \
                 (expected version {}, found {observed})",
                state.version
            )));
        }
        state.version += 1;
        self.write_workspace_state(state).await
    }

    /// Take the exclusive lock guarding a workspace's metadata document.
    ///
    /// Every mutation is read-whole-state, modify, write-whole-state. Two
    /// callers without this both read the same document, each inserts its own
    /// record, and the second write discards the first — the body object
    /// survives, unreferenced, so the index claims the file was never written
    /// while garbage collection quietly reclaims it.
    ///
    /// `create_new` is an atomic `O_EXCL` create, which is why this needs no
    /// locking dependency.
    async fn lock_workspace(&self, workspace_id: &str) -> GalvoFsResult<WorkspaceLock> {
        let path = self.workspace_lock_path(workspace_id);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await.map_err(|error| {
                GalvoFsError::RuntimeFailed(format!(
                    "failed to create GalvoFS workspace lock path: {error}"
                ))
            })?;
        }

        let deadline = std::time::Instant::now() + WORKSPACE_LOCK_ACQUIRE_TIMEOUT;
        loop {
            match fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&path)
                .await
            {
                Ok(mut file) => {
                    let holder = format!(
                        "pid={} acquiredAt={}",
                        std::process::id(),
                        OffsetDateTime::now_utc()
                    );
                    file.write_all(holder.as_bytes()).await.map_err(|error| {
                        GalvoFsError::RuntimeFailed(format!(
                            "failed to record GalvoFS workspace lock holder: {error}"
                        ))
                    })?;
                    return Ok(WorkspaceLock { path });
                }
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                    if !break_stale_workspace_lock(&path).await?
                        && std::time::Instant::now() >= deadline
                    {
                        return Err(GalvoFsError::Conflict(format!(
                            "GalvoFS workspace {workspace_id} is locked by another writer"
                        )));
                    }
                    tokio::time::sleep(WORKSPACE_LOCK_POLL_INTERVAL).await;
                }
                Err(error) => {
                    return Err(GalvoFsError::RuntimeFailed(format!(
                        "failed to lock GalvoFS workspace {workspace_id}: {error}"
                    )));
                }
            }
        }
    }

    async fn read_snapshot(&self, snapshot_id: &str) -> GalvoFsResult<SnapshotPin> {
        read_json(&self.snapshot_path(snapshot_id)).await
    }

    async fn read_storage_lease(&self, lease_id: &str) -> GalvoFsResult<StorageLease> {
        read_json(&self.lease_path(lease_id)).await
    }

    async fn read_storage_input(&self, input_id: &str) -> GalvoFsResult<GalvoFsStorageInput> {
        read_json(&self.storage_input_path(input_id)).await
    }

    async fn referenced_object_map(
        &self,
    ) -> GalvoFsResult<BTreeMap<String, Vec<GalvoFsIntegrityReference>>> {
        let mut references: BTreeMap<String, Vec<GalvoFsIntegrityReference>> = BTreeMap::new();

        for state in read_json_dir::<WorkspaceState>(&self.workspaces_root()).await? {
            for file in state.files.values() {
                references.entry(file.digest.clone()).or_default().push(
                    GalvoFsIntegrityReference {
                        source_kind: "workspace".into(),
                        source_id: state.root.workspace_id.clone(),
                        path: Some(file.path.clone()),
                    },
                );
            }
        }

        for snapshot in read_json_dir::<SnapshotPin>(&self.snapshots_root()).await? {
            for file in &snapshot.files {
                references.entry(file.digest.clone()).or_default().push(
                    GalvoFsIntegrityReference {
                        source_kind: "snapshot".into(),
                        source_id: snapshot.snapshot_id.clone(),
                        path: Some(file.path.clone()),
                    },
                );
            }
        }

        for artifact in read_json_dir::<ArtifactManifest>(&self.artifacts_root()).await? {
            references.entry(artifact.digest.clone()).or_default().push(
                GalvoFsIntegrityReference {
                    source_kind: "artifact".into(),
                    source_id: artifact.artifact_id,
                    path: Some(artifact.path),
                },
            );
        }

        Ok(references)
    }

    async fn authorize_lease(
        &self,
        lease_id: &str,
        action: GalvoFsLeaseAction,
    ) -> GalvoFsResult<StorageLease> {
        let lease = self.read_storage_lease(lease_id).await?;
        if lease.expires_at <= OffsetDateTime::now_utc() {
            return Err(GalvoFsError::Expired(format!(
                "GalvoFS storage lease {} expired",
                lease.lease_id
            )));
        }
        if !lease.access.allows(action) {
            return Err(GalvoFsError::AdmissionRejected(format!(
                "GalvoFS storage lease {} with {:?} access does not allow {}",
                lease.lease_id,
                lease.access,
                action.label()
            )));
        }
        let state = self.read_workspace_state(&lease.workspace_id).await?;
        if state.root.tenant_id != lease.tenant_id || state.root.project_id != lease.project_id {
            return Err(GalvoFsError::AdmissionRejected(format!(
                "GalvoFS storage lease {} no longer matches workspace tenant/project",
                lease.lease_id
            )));
        }
        Ok(lease)
    }

    fn metadata_root(&self) -> PathBuf {
        self.root.join("metadata")
    }

    fn workspaces_root(&self) -> PathBuf {
        self.metadata_root().join("workspaces")
    }

    fn leases_root(&self) -> PathBuf {
        self.metadata_root().join("leases")
    }

    fn storage_inputs_root(&self) -> PathBuf {
        self.metadata_root().join("storage-inputs")
    }

    fn storage_input_materialization_tasks_root(&self) -> PathBuf {
        self.metadata_root().join("storage-input-materializations")
    }

    fn mount_manifests_root(&self) -> PathBuf {
        self.metadata_root().join("mount-manifests")
    }

    fn snapshots_root(&self) -> PathBuf {
        self.metadata_root().join("snapshots")
    }

    fn artifacts_root(&self) -> PathBuf {
        self.metadata_root().join("artifacts")
    }

    fn workspace_path(&self, workspace_id: &str) -> PathBuf {
        self.workspaces_root().join(format!("{workspace_id}.json"))
    }

    /// The `.lock` extension keeps this out of every directory scan:
    /// `read_json_dir` admits only `.json`.
    fn workspace_lock_path(&self, workspace_id: &str) -> PathBuf {
        self.workspaces_root().join(format!("{workspace_id}.lock"))
    }

    fn lease_path(&self, lease_id: &str) -> PathBuf {
        self.leases_root().join(format!("{lease_id}.json"))
    }

    fn storage_input_path(&self, input_id: &str) -> PathBuf {
        self.storage_inputs_root().join(format!("{input_id}.json"))
    }

    fn storage_input_materialization_task_path(&self, task_id: &str) -> PathBuf {
        self.storage_input_materialization_tasks_root()
            .join(format!("{task_id}.json"))
    }

    fn mount_manifest_path(&self, mount_manifest_id: &str) -> PathBuf {
        self.mount_manifests_root()
            .join(format!("{mount_manifest_id}.json"))
    }

    fn snapshot_path(&self, snapshot_id: &str) -> PathBuf {
        self.snapshots_root().join(format!("{snapshot_id}.json"))
    }

    fn artifact_path(&self, artifact_id: &str) -> PathBuf {
        self.artifacts_root().join(format!("{artifact_id}.json"))
    }

    fn receipts_path(&self) -> PathBuf {
        self.metadata_root().join("receipts.jsonl")
    }
}

#[async_trait]
pub trait GalvoFsObjectStore: Send + Sync {
    async fn put_object(&self, digest: &str, bytes: &[u8]) -> GalvoFsResult<String>;
    async fn get_object(&self, digest: &str) -> GalvoFsResult<Vec<u8>>;
    async fn list_object_digests(&self) -> GalvoFsResult<Vec<String>>;
    async fn delete_object(&self, digest: &str) -> GalvoFsResult<()>;
}

#[derive(Debug, Clone)]
pub struct LocalGalvoFsObjectStore {
    root: PathBuf,
}

impl LocalGalvoFsObjectStore {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    fn object_path(&self, digest: &str) -> GalvoFsResult<PathBuf> {
        let hex = digest_hex(digest)?;
        Ok(self.root.join("sha256").join(hex))
    }
}

#[derive(Debug, Clone)]
pub struct OpenDalGalvoFsObjectStore {
    operator: Operator,
}

impl OpenDalGalvoFsObjectStore {
    pub fn new(operator: Operator) -> Self {
        Self { operator }
    }

    fn object_path(&self, digest: &str) -> GalvoFsResult<String> {
        let hex = digest_hex(digest)?;
        Ok(format!("sha256/{hex}"))
    }
}

#[async_trait]
impl GalvoFsObjectStore for OpenDalGalvoFsObjectStore {
    async fn put_object(&self, digest: &str, bytes: &[u8]) -> GalvoFsResult<String> {
        let path = self.object_path(digest)?;
        if !self
            .operator
            .exists(&path)
            .await
            .map_err(opendal_runtime_error)?
        {
            self.operator
                .write(&path, bytes.to_vec())
                .await
                .map_err(opendal_runtime_error)?;
        }
        Ok(object_uri(digest)?)
    }

    async fn get_object(&self, digest: &str) -> GalvoFsResult<Vec<u8>> {
        let path = self.object_path(digest)?;
        self.operator
            .read(&path)
            .await
            .map(|buffer| buffer.to_vec())
            .map_err(|error| {
                if error.kind() == OpenDalErrorKind::NotFound {
                    GalvoFsError::ArtifactMissing(format!("GalvoFS object {digest} not found"))
                } else {
                    GalvoFsError::RuntimeFailed(format!("failed to read GalvoFS object: {error}"))
                }
            })
    }

    async fn list_object_digests(&self) -> GalvoFsResult<Vec<String>> {
        let entries = self
            .operator
            .list("sha256/")
            .await
            .map_err(opendal_runtime_error)?;
        let mut digests = entries
            .into_iter()
            .filter_map(|entry| {
                let hex = entry.path().strip_prefix("sha256/")?;
                if hex.len() == 64 && hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                    Some(format!("sha256:{hex}"))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        digests.sort();
        Ok(digests)
    }

    async fn delete_object(&self, digest: &str) -> GalvoFsResult<()> {
        let path = self.object_path(digest)?;
        self.operator
            .delete(&path)
            .await
            .map_err(opendal_runtime_error)
    }
}

#[async_trait]
impl GalvoFsObjectStore for LocalGalvoFsObjectStore {
    async fn put_object(&self, digest: &str, bytes: &[u8]) -> GalvoFsResult<String> {
        let path = self.object_path(digest)?;
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await.map_err(|error| {
                GalvoFsError::RuntimeFailed(format!(
                    "failed to create GalvoFS object path: {error}"
                ))
            })?;
        }
        match fs::metadata(&path).await {
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                fs::write(&path, bytes).await.map_err(|error| {
                    GalvoFsError::RuntimeFailed(format!("failed to write GalvoFS object: {error}"))
                })?;
            }
            Err(error) => {
                return Err(GalvoFsError::RuntimeFailed(format!(
                    "failed to stat GalvoFS object: {error}"
                )));
            }
        }
        Ok(object_uri(digest)?)
    }

    async fn get_object(&self, digest: &str) -> GalvoFsResult<Vec<u8>> {
        let path = self.object_path(digest)?;
        fs::read(&path).await.map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                GalvoFsError::ArtifactMissing(format!("GalvoFS object {digest} not found"))
            } else {
                GalvoFsError::RuntimeFailed(format!("failed to read GalvoFS object: {error}"))
            }
        })
    }

    async fn list_object_digests(&self) -> GalvoFsResult<Vec<String>> {
        let root = self.root.join("sha256");
        let mut entries = match fs::read_dir(&root).await {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => {
                return Err(GalvoFsError::RuntimeFailed(format!(
                    "failed to read GalvoFS object directory: {error}"
                )));
            }
        };
        let mut digests = Vec::new();
        while let Some(entry) = entries.next_entry().await.map_err(|error| {
            GalvoFsError::RuntimeFailed(format!("failed to read GalvoFS object entry: {error}"))
        })? {
            let path = entry.path();
            let metadata = fs::metadata(&path).await.map_err(|error| {
                GalvoFsError::RuntimeFailed(format!("failed to stat GalvoFS object entry: {error}"))
            })?;
            let Some(hex) = path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            if metadata.is_file()
                && hex.len() == 64
                && hex.bytes().all(|byte| byte.is_ascii_hexdigit())
            {
                digests.push(format!("sha256:{hex}"));
            }
        }
        digests.sort();
        Ok(digests)
    }

    async fn delete_object(&self, digest: &str) -> GalvoFsResult<()> {
        let path = self.object_path(digest)?;
        match fs::remove_file(&path).await {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(GalvoFsError::RuntimeFailed(format!(
                "failed to delete GalvoFS object: {error}"
            ))),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CreateGalvoWorkspaceRequest {
    pub tenant_id: String,
    pub project_id: String,
    pub owner_id: String,
    pub workspace_name: String,
    pub purpose: String,
}

impl CreateGalvoWorkspaceRequest {
    fn validate(&self) -> GalvoFsResult<()> {
        require_non_empty("tenantId", &self.tenant_id)?;
        require_non_empty("projectId", &self.project_id)?;
        require_non_empty("ownerId", &self.owner_id)?;
        require_non_empty("workspaceName", &self.workspace_name)?;
        require_non_empty("purpose", &self.purpose)?;
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct WorkspaceRoot {
    pub workspace_id: String,
    pub tenant_id: String,
    pub project_id: String,
    pub owner_id: String,
    pub workspace_name: String,
    pub purpose: String,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct GalvoFsS3Config {
    pub bucket: String,
    #[serde(default)]
    pub root: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub access_key_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secret_access_key: Option<String>,
}

impl GalvoFsS3Config {
    fn validate(&self) -> GalvoFsResult<()> {
        require_non_empty("bucket", &self.bucket)?;
        if let Some(endpoint) = &self.endpoint {
            require_non_empty("endpoint", endpoint)?;
        }
        if let Some(region) = &self.region {
            require_non_empty("region", region)?;
        }
        if let Some(access_key_id) = &self.access_key_id {
            require_non_empty("accessKeyId", access_key_id)?;
        }
        if let Some(secret_access_key) = &self.secret_access_key {
            require_non_empty("secretAccessKey", secret_access_key)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct IssueGalvoStorageLeaseRequest {
    pub workspace_id: String,
    pub tenant_id: String,
    pub project_id: String,
    pub secure_entity_id: String,
    pub work_order_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub a2e_ticket_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime_id: Option<String>,
    pub access: GalvoFsAccessMode,
    #[serde(with = "time::serde::rfc3339")]
    pub expires_at: OffsetDateTime,
}

impl IssueGalvoStorageLeaseRequest {
    fn validate(&self) -> GalvoFsResult<()> {
        require_non_empty("workspaceId", &self.workspace_id)?;
        require_non_empty("tenantId", &self.tenant_id)?;
        require_non_empty("projectId", &self.project_id)?;
        require_non_empty("secureEntityId", &self.secure_entity_id)?;
        require_non_empty("workOrderId", &self.work_order_id)?;
        if let Some(ticket_id) = &self.a2e_ticket_id {
            require_non_empty("a2eTicketId", ticket_id)?;
        }
        if let Some(runtime_id) = &self.runtime_id {
            require_non_empty("runtimeId", runtime_id)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct StorageLease {
    pub lease_id: String,
    pub tenant_id: String,
    pub project_id: String,
    pub secure_entity_id: String,
    pub work_order_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub a2e_ticket_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime_id: Option<String>,
    pub workspace_id: String,
    pub access: GalvoFsAccessMode,
    #[serde(with = "time::serde::rfc3339")]
    pub issued_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub expires_at: OffsetDateTime,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum GalvoFsAccessMode {
    ReadOnly,
    ReadWrite,
    Admin,
}

impl GalvoFsAccessMode {
    fn allows(self, action: GalvoFsLeaseAction) -> bool {
        match self {
            GalvoFsAccessMode::ReadOnly => matches!(action, GalvoFsLeaseAction::Read),
            GalvoFsAccessMode::ReadWrite | GalvoFsAccessMode::Admin => true,
        }
    }

    fn requires_write(self) -> bool {
        matches!(
            self,
            GalvoFsAccessMode::ReadWrite | GalvoFsAccessMode::Admin
        )
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum GalvoFsStorageInputSourceKind {
    GalvofsWorkspace,
    UploadedObject,
    LocalPath,
    S3Prefix,
    SiteFilesystemReference,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum GalvoFsInputMaterialization {
    Import,
    Reference,
    Mirror,
    Mount,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum GalvoFsDigestPolicy {
    Required,
    BestEffort,
    Deferred,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum GalvoFsInputVerificationStatus {
    Pending,
    Verified,
    Deferred,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum GalvoFsWritebackPolicy {
    None,
    PinArtifacts,
    External,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CreateGalvoStorageInputRequest {
    pub tenant_id: String,
    pub project_id: String,
    pub name: String,
    pub source_kind: GalvoFsStorageInputSourceKind,
    pub source_uri: String,
    pub materialization: GalvoFsInputMaterialization,
    pub access: GalvoFsAccessMode,
    pub mount_path: String,
    pub digest_policy: GalvoFsDigestPolicy,
    pub retention_policy: String,
    pub writeback_policy: GalvoFsWritebackPolicy,
}

impl CreateGalvoStorageInputRequest {
    fn validate(&self) -> GalvoFsResult<()> {
        require_non_empty("tenantId", &self.tenant_id)?;
        require_non_empty("projectId", &self.project_id)?;
        require_non_empty("storageInput.name", &self.name)?;
        require_non_empty("storageInput.sourceUri", &self.source_uri)?;
        require_non_empty("storageInput.retentionPolicy", &self.retention_policy)?;
        validate_runtime_mount_path(&self.mount_path)?;
        validate_storage_input_source_uri(self.source_kind, &self.source_uri)?;
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct GalvoFsStorageInput {
    pub input_id: String,
    pub tenant_id: String,
    pub project_id: String,
    pub name: String,
    pub source_kind: GalvoFsStorageInputSourceKind,
    pub source_uri: String,
    pub materialization: GalvoFsInputMaterialization,
    pub access: GalvoFsAccessMode,
    pub mount_path: String,
    pub digest_policy: GalvoFsDigestPolicy,
    pub verification_status: GalvoFsInputVerificationStatus,
    pub retention_policy: String,
    pub writeback_policy: GalvoFsWritebackPolicy,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AttachGalvoStorageInputRequest {
    pub workspace_id: String,
    pub input_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lease_id: Option<String>,
    pub mount_path: String,
    pub materialization: GalvoFsInputMaterialization,
    pub access: GalvoFsAccessMode,
    pub writeback_policy: GalvoFsWritebackPolicy,
}

impl AttachGalvoStorageInputRequest {
    fn validate(&self) -> GalvoFsResult<()> {
        require_non_empty("workspaceId", &self.workspace_id)?;
        require_non_empty("storageInputId", &self.input_id)?;
        if let Some(lease_id) = &self.lease_id {
            require_non_empty("leaseId", lease_id)?;
        }
        validate_runtime_mount_path(&self.mount_path)?;
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct GalvoFsStorageInputBinding {
    pub binding_id: String,
    pub workspace_id: String,
    pub input_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lease_id: Option<String>,
    pub name: String,
    pub source_kind: GalvoFsStorageInputSourceKind,
    pub source_uri: String,
    pub materialization: GalvoFsInputMaterialization,
    pub access: GalvoFsAccessMode,
    pub mount_path: String,
    pub digest_policy: GalvoFsDigestPolicy,
    pub verification_status: GalvoFsInputVerificationStatus,
    pub retention_policy: String,
    pub writeback_policy: GalvoFsWritebackPolicy,
    /// The workspace as it stood the moment this input finished materializing.
    ///
    /// Writeback compares against this to decide what the workspace changed and
    /// whether the external source moved underneath it. Nothing else recorded
    /// what a source looked like when it came in — `digestPolicy` and
    /// `verificationStatus` were labels with no stored digest behind them — so
    /// without this there is no baseline to detect drift against.
    ///
    /// Absent on bindings that never materialized, and on bindings written
    /// before this field existed. Writeback refuses rather than guessing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub baseline_snapshot_id: Option<String>,
    #[serde(with = "time::serde::rfc3339")]
    pub attached_at: OffsetDateTime,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct UploadGalvoStorageObjectRequest {
    pub bytes: Vec<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_digest: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
}

impl UploadGalvoStorageObjectRequest {
    fn validate(&self) -> GalvoFsResult<()> {
        if self.bytes.is_empty() {
            return Err(GalvoFsError::ManifestInvalid(
                "GalvoFS uploaded object must not be empty".into(),
            ));
        }
        if self.bytes.len() as u64 > MAX_STORAGE_INPUT_BYTES {
            return Err(GalvoFsError::AdmissionRejected(format!(
                "GalvoFS uploaded object exceeds {MAX_STORAGE_INPUT_BYTES} byte limit"
            )));
        }
        if let Some(expected_digest) = &self.expected_digest {
            digest_hex(expected_digest)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct GalvoFsUploadedObject {
    pub digest: String,
    pub object_uri: String,
    pub size_bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
    #[serde(with = "time::serde::rfc3339")]
    pub uploaded_at: OffsetDateTime,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum GalvoFsMaterializationStatus {
    Running,
    Completed,
    Failed,
}

impl GalvoFsMaterializationStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            GalvoFsMaterializationStatus::Running => "running",
            GalvoFsMaterializationStatus::Completed => "completed",
            GalvoFsMaterializationStatus::Failed => "failed",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct GalvoFsMaterializationTask {
    pub task_id: String,
    pub input_id: String,
    pub workspace_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lease_id: Option<String>,
    pub source_kind: GalvoFsStorageInputSourceKind,
    pub source_uri: String,
    pub materialization: GalvoFsInputMaterialization,
    pub status: GalvoFsMaterializationStatus,
    pub files_materialized: u64,
    pub bytes_materialized: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_message: Option<String>,
    #[serde(with = "time::serde::rfc3339")]
    pub started_at: OffsetDateTime,
    #[serde(default, with = "time::serde::rfc3339::option")]
    pub completed_at: Option<OffsetDateTime>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GalvoFsLeaseAction {
    Read,
    Write,
}

impl GalvoFsLeaseAction {
    fn label(self) -> &'static str {
        match self {
            GalvoFsLeaseAction::Read => "read",
            GalvoFsLeaseAction::Write => "write",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct GalvoFsFileRecord {
    pub path: String,
    pub kind: GalvoFsFileKind,
    pub size_bytes: u64,
    pub digest: String,
    pub object_uri: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct GalvoFsFileData {
    pub record: GalvoFsFileRecord,
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CreateGalvoMountManifestRequest {
    pub workspace_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lease_id: Option<String>,
    pub policy_digest: String,
    pub mounts: Vec<GalvoMountBinding>,
}

impl CreateGalvoMountManifestRequest {
    fn validate(&self) -> GalvoFsResult<()> {
        require_non_empty("workspaceId", &self.workspace_id)?;
        require_non_empty("policyDigest", &self.policy_digest)?;
        digest_hex(&self.policy_digest)?;
        if let Some(lease_id) = &self.lease_id {
            require_non_empty("leaseId", lease_id)?;
        }
        if self.mounts.is_empty() {
            return Err(GalvoFsError::ManifestInvalid(
                "GalvoFS mount manifest must include at least one mount".into(),
            ));
        }
        for mount in &self.mounts {
            mount.validate()?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct GalvoMountBinding {
    pub name: String,
    pub path: String,
    pub source_uri: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_input_id: Option<String>,
    pub access: GalvoFsAccessMode,
}

impl GalvoMountBinding {
    fn validate(&self) -> GalvoFsResult<()> {
        require_non_empty("mount.name", &self.name)?;
        require_non_empty("mount.path", &self.path)?;
        require_non_empty("mount.sourceUri", &self.source_uri)?;
        if let Some(input_id) = &self.storage_input_id {
            require_non_empty("mount.storageInputId", input_id)?;
        }
        validate_runtime_mount_path(&self.path)?;
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct GalvoFsCsiNodePublish {
    pub target_path: String,
    pub read_only: bool,
    pub mount_manifest_id: String,
    pub mount_manifest_digest: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct GalvoFsKubernetesCsiProjection {
    pub volume: GalvoFsKubernetesVolume,
    pub volume_mount: GalvoFsKubernetesVolumeMount,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct GalvoFsKubernetesVolume {
    pub name: String,
    pub csi: GalvoFsKubernetesCsiVolumeSource,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct GalvoFsKubernetesCsiVolumeSource {
    pub driver: String,
    pub volume_handle: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub volume_attributes: BTreeMap<String, String>,
    pub read_only: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct GalvoFsKubernetesVolumeMount {
    pub name: String,
    pub mount_path: String,
    pub read_only: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum GalvoFsAccessEndpoint {
    Csi {
        driver: String,
        volume_handle: String,
        #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
        volume_attributes: BTreeMap<String, String>,
        node_publish: GalvoFsCsiNodePublish,
        kubernetes: GalvoFsKubernetesCsiProjection,
    },
    Nfs4 {
        server: String,
        export_path: String,
        uri: String,
        target_path: String,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        mount_options: Vec<String>,
        mount_command: String,
        #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
        environment: BTreeMap<String, String>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct GalvoFsRuntimeBinding {
    pub workspace_id: String,
    pub lease_id: String,
    pub mount_manifest_id: String,
    pub mount_manifest_digest: String,
    pub runtime_adapter: String,
    pub role: String,
    pub mount_path: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub storage_inputs: Vec<GalvoFsStorageInputBinding>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub access_endpoints: Vec<GalvoFsAccessEndpoint>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scratch: Option<GalvoFsScratchBinding>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct GalvoFsScratchBinding {
    pub policy: String,
    pub preferred_filesystems: Vec<String>,
    pub mount_path_hint: String,
    pub writeback: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct MountManifest {
    pub mount_manifest_id: String,
    pub workspace_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lease_id: Option<String>,
    pub policy_digest: String,
    pub digest: String,
    pub mounts: Vec<GalvoMountBinding>,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum GalvoFsFileKind {
    File,
    Directory,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SnapshotPin {
    pub snapshot_id: String,
    pub workspace_id: String,
    pub label: String,
    pub files: Vec<GalvoFsFileRecord>,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct WorkspaceClone {
    pub clone_id: String,
    pub source_snapshot_id: String,
    pub workspace: WorkspaceRoot,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct WorkspaceDiff {
    pub workspace_id: String,
    pub baseline_snapshot_id: String,
    pub changes: Vec<GalvoFsDiffEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct GalvoFsDiffEntry {
    pub path: String,
    pub kind: GalvoFsDiffKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub before_digest: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after_digest: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub before_size_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after_size_bytes: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum GalvoFsDiffKind {
    Added,
    Removed,
    Modified,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct PublishGalvoArtifactRequest {
    pub workspace_id: String,
    pub path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
    pub retention_policy: String,
}

impl PublishGalvoArtifactRequest {
    fn validate(&self) -> GalvoFsResult<()> {
        require_non_empty("workspaceId", &self.workspace_id)?;
        require_non_empty("path", &self.path)?;
        require_non_empty("retentionPolicy", &self.retention_policy)?;
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ArtifactManifest {
    pub artifact_id: String,
    pub workspace_id: String,
    pub path: String,
    pub digest: String,
    pub object_uri: String,
    pub size_bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
    pub retention_policy: String,
    #[serde(with = "time::serde::rfc3339")]
    pub published_at: OffsetDateTime,
}

/// What a writeback actually did.
///
/// `writtenPaths` is the point of this type. The outward push is N writes into
/// storage this system does not own, and no local filesystem offers a
/// transaction across them, so a failure part-way leaves a partial result. That
/// cannot be designed away — but naming exactly which paths landed is the
/// difference between a partial push and a silent one.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct WritebackReport {
    pub binding_id: String,
    pub workspace_id: String,
    pub input_id: String,
    pub policy: GalvoFsWritebackPolicy,
    pub baseline_snapshot_id: String,
    /// Paths written to the external source. Empty under `PinArtifacts`.
    pub written_paths: Vec<String>,
    /// Artifacts published. Empty under `External`.
    pub published_artifacts: Vec<ArtifactManifest>,
    #[serde(with = "time::serde::rfc3339")]
    pub completed_at: OffsetDateTime,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct GalvoFsIntegrityReport {
    pub status: GalvoFsIntegrityStatus,
    pub checked_objects: usize,
    pub missing_objects: Vec<GalvoFsIntegrityObjectIssue>,
    pub corrupt_objects: Vec<GalvoFsIntegrityObjectIssue>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum GalvoFsIntegrityStatus {
    Healthy,
    Degraded,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct GalvoFsIntegrityObjectIssue {
    pub digest: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actual_digest: Option<String>,
    pub references: Vec<GalvoFsIntegrityReference>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct GalvoFsIntegrityReference {
    pub source_kind: String,
    pub source_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct GalvoFsGarbageCollectionRequest {
    pub dry_run: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct GalvoFsGarbageCollectionReport {
    pub dry_run: bool,
    pub candidate_objects: Vec<GalvoFsObjectRecord>,
    pub retained_objects: Vec<GalvoFsObjectRecord>,
    pub removed_objects: Vec<GalvoFsObjectRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct GalvoFsObjectRecord {
    pub digest: String,
    pub object_uri: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct StorageReceipt {
    pub receipt_id: String,
    pub operation: String,
    #[serde(with = "time::serde::rfc3339")]
    pub issued_at: OffsetDateTime,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lease_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mount_manifest_digest: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object_digest: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_input_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_uri: Option<String>,
}

impl StorageReceipt {
    fn new(operation: impl Into<String>) -> Self {
        Self {
            receipt_id: format!("storage-receipt-{}", Uuid::new_v4()),
            operation: operation.into(),
            issued_at: OffsetDateTime::now_utc(),
            workspace_id: None,
            lease_id: None,
            tenant_id: None,
            project_id: None,
            snapshot_id: None,
            mount_manifest_digest: None,
            artifact_id: None,
            path: None,
            object_digest: None,
            storage_input_id: None,
            source_uri: None,
        }
    }

    fn for_workspace(mut self, workspace: &WorkspaceRoot) -> Self {
        self.workspace_id = Some(workspace.workspace_id.clone());
        self.tenant_id = Some(workspace.tenant_id.clone());
        self.project_id = Some(workspace.project_id.clone());
        self
    }

    fn with_tenant_project(mut self, tenant_id: &str, project_id: &str) -> Self {
        self.tenant_id = Some(tenant_id.into());
        self.project_id = Some(project_id.into());
        self
    }

    fn with_lease(mut self, lease_id: &str) -> Self {
        self.lease_id = Some(lease_id.into());
        self
    }

    fn with_optional_lease(mut self, lease_id: Option<&str>) -> Self {
        self.lease_id = lease_id.map(str::to_owned);
        self
    }

    fn with_snapshot(mut self, snapshot_id: &str) -> Self {
        self.snapshot_id = Some(snapshot_id.into());
        self
    }

    fn with_mount_manifest(mut self, digest: &str) -> Self {
        self.mount_manifest_digest = Some(digest.into());
        self
    }

    fn with_artifact(mut self, artifact_id: &str) -> Self {
        self.artifact_id = Some(artifact_id.into());
        self
    }

    fn with_path(mut self, path: &str) -> Self {
        self.path = Some(path.into());
        self
    }

    fn with_object_digest(mut self, digest: &str) -> Self {
        self.object_digest = Some(digest.into());
        self
    }

    fn with_storage_input(mut self, input_id: &str) -> Self {
        self.storage_input_id = Some(input_id.into());
        self
    }

    fn with_source_uri(mut self, source_uri: &str) -> Self {
        self.source_uri = Some(source_uri.into());
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
struct WorkspaceState {
    root: WorkspaceRoot,
    files: BTreeMap<String, GalvoFsFileRecord>,
    #[serde(default)]
    storage_inputs: Vec<GalvoFsStorageInputBinding>,
    /// Bumped on every mutation, and checked before every write.
    ///
    /// The workspace lock is what prevents concurrent mutations; this is what
    /// detects one that happened anyway, which is the case that matters
    /// because a lock is only as good as every call site remembering to take
    /// it. Documents written before this field existed deserialize as 0.
    #[serde(default)]
    version: u64,
}

/// One file this writeback intends to place, and what it expects to find there.
struct WritebackTarget {
    workspace_path: String,
    path: PathBuf,
    after_digest: String,
    /// The digest the source held at materialization. `None` means the
    /// workspace added this file, so the source is expected to have nothing.
    baseline_digest: Option<String>,
}

/// Is this workspace path inside the binding's mount prefix?
///
/// A bare `starts_with` would let `srcbackup/x` match the prefix `src`, which
/// is a different directory. The boundary has to be a path separator.
fn workspace_path_under_prefix(path: &str, prefix: &str) -> bool {
    if prefix.is_empty() {
        return true;
    }
    path == prefix || path.starts_with(&format!("{prefix}/"))
}

/// Map each changed workspace path onto the file it came from.
///
/// Refuses anything it cannot map rather than guessing, and refuses any target
/// that would land outside the source root even though workspace paths are
/// normalized on the way in — this resolves into somebody else's filesystem, so
/// the check is worth repeating where the write happens.
fn resolve_writeback_targets(
    source_root: &Path,
    source_is_dir: bool,
    prefix: &str,
    changed: &[GalvoFsDiffEntry],
) -> GalvoFsResult<Vec<WritebackTarget>> {
    let mut targets = Vec::with_capacity(changed.len());
    for change in changed {
        let relative = if prefix.is_empty() {
            change.path.as_str()
        } else {
            change
                .path
                .strip_prefix(prefix)
                .unwrap_or(&change.path)
                .trim_start_matches('/')
        };
        let after_digest = change.after_digest.clone().ok_or_else(|| {
            GalvoFsError::RuntimeFailed(format!(
                "GalvoFS writeback has no content digest for {}",
                change.path
            ))
        })?;

        // A single-file input materialized to exactly one workspace path. Any
        // other path under the prefix did not come from this source, and
        // inventing a sibling file next to somebody's file is not writeback.
        let path = if source_is_dir {
            source_root.join(relative)
        } else if source_root.file_name().and_then(|name| name.to_str()) == Some(relative) {
            source_root.to_path_buf()
        } else {
            return Err(GalvoFsError::AdmissionRejected(format!(
                "GalvoFS writeback cannot map {} onto single-file source {}",
                change.path,
                source_root.display()
            )));
        };
        if !path.starts_with(source_root) {
            return Err(GalvoFsError::AdmissionRejected(format!(
                "GalvoFS writeback path {} escapes its source root",
                change.path
            )));
        }

        targets.push(WritebackTarget {
            workspace_path: change.path.clone(),
            path,
            after_digest,
            baseline_digest: change.before_digest.clone(),
        });
    }
    Ok(targets)
}

/// Refuse the whole writeback if the source moved since materialization.
///
/// Checked for every target before any of them is written, and one drifted
/// path aborts all of them. Pushing the clean files and reporting the rest
/// would be a partial application against storage this system does not own,
/// which is the thing the whole design refuses to do.
async fn assert_no_source_drift(targets: &[WritebackTarget]) -> GalvoFsResult<()> {
    let mut drifted = Vec::new();
    for target in targets {
        let current = match fs::read(&target.path).await {
            Ok(bytes) => Some(sha256_digest(&bytes)),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => {
                return Err(GalvoFsError::RuntimeFailed(format!(
                    "failed to read GalvoFS writeback target {}: {error}",
                    target.path.display()
                )));
            }
        };
        match (&target.baseline_digest, &current) {
            // Unchanged since materialization: safe to overwrite.
            (Some(baseline), Some(current)) if baseline == current => {}
            // The workspace added it and something else created it first.
            (None, Some(_)) => drifted.push(format!(
                "{} was created at the source after materialization",
                target.workspace_path
            )),
            // Materialized from a file that has since been deleted.
            (Some(_), None) => drifted.push(format!(
                "{} was deleted at the source after materialization",
                target.workspace_path
            )),
            (Some(_), Some(_)) => drifted.push(format!(
                "{} was modified at the source after materialization",
                target.workspace_path
            )),
            // Added here, absent there. Nothing to conflict with.
            (None, None) => {}
        }
    }
    if drifted.is_empty() {
        return Ok(());
    }
    Err(GalvoFsError::AdmissionRejected(format!(
        "GalvoFS writeback refused; the source changed since materialization: {}. Nothing was \
         written.",
        drifted.join("; ")
    )))
}

/// Place one file at the source, via a temporary in the same directory.
///
/// This cannot make the writeback as a whole atomic — it is N writes into a
/// directory we do not own, and no local filesystem offers a transaction across
/// them. What it does buy is that no individual file is ever observed
/// half-written by whatever else reads that directory.
async fn write_external_file(path: &Path, bytes: &[u8]) -> GalvoFsResult<()> {
    let parent = path.parent().ok_or_else(|| {
        GalvoFsError::RuntimeFailed(format!(
            "GalvoFS writeback target {} has no parent directory",
            path.display()
        ))
    })?;
    fs::create_dir_all(parent).await.map_err(|error| {
        GalvoFsError::RuntimeFailed(format!(
            "failed to create GalvoFS writeback directory: {error}"
        ))
    })?;
    let temporary = parent.join(format!("{}.galvofs-writeback.tmp", Uuid::new_v4()));
    if let Err(error) = write_and_sync(&temporary, bytes).await {
        let _ = fs::remove_file(&temporary).await;
        return Err(error);
    }
    if let Err(error) = fs::rename(&temporary, path).await {
        let _ = fs::remove_file(&temporary).await;
        return Err(GalvoFsError::RuntimeFailed(format!(
            "failed to install GalvoFS writeback file {}: {error}",
            path.display()
        )));
    }
    Ok(())
}

/// Pin a workspace state as a snapshot without re-reading it from disk.
///
/// `snapshot_workspace` reads the document; a caller mid-mutation holds a state
/// the document does not yet have, and snapshotting through the disk would pin
/// the wrong thing. Both paths build the pin here so they cannot drift.
fn snapshot_from_state(state: &WorkspaceState, label: impl Into<String>) -> SnapshotPin {
    SnapshotPin {
        snapshot_id: format!("snap-{}", Uuid::new_v4()),
        workspace_id: state.root.workspace_id.clone(),
        label: label.into(),
        files: state.files.values().cloned().collect(),
        created_at: OffsetDateTime::now_utc(),
    }
}

/// Exclusive hold on one workspace's metadata document.
///
/// Released on drop with a blocking unlink, so an early return or a panic
/// cannot strand the lock and wedge the workspace until the stale window
/// elapses. `Drop` cannot be async and this is a single syscall.
struct WorkspaceLock {
    path: PathBuf,
}

impl Drop for WorkspaceLock {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

/// Remove a workspace lock whose holder is gone, reporting whether it went.
///
/// Staleness is judged by the lock file's own modification time rather than
/// its recorded pid: a pid is not meaningful across containers or hosts, and
/// this metadata root is reachable from more than one of them.
async fn break_stale_workspace_lock(path: &Path) -> GalvoFsResult<bool> {
    let metadata = match fs::metadata(path).await {
        Ok(metadata) => metadata,
        // The holder released it between our failed create and this stat.
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(true),
        Err(error) => {
            return Err(GalvoFsError::RuntimeFailed(format!(
                "failed to stat GalvoFS workspace lock: {error}"
            )));
        }
    };
    let modified = metadata.modified().map_err(|error| {
        GalvoFsError::RuntimeFailed(format!("failed to age GalvoFS workspace lock: {error}"))
    })?;
    if modified.elapsed().unwrap_or_default() < WORKSPACE_LOCK_STALE_AFTER {
        return Ok(false);
    }
    match fs::remove_file(path).await {
        Ok(()) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(true),
        Err(error) => Err(GalvoFsError::RuntimeFailed(format!(
            "failed to break stale GalvoFS workspace lock: {error}"
        ))),
    }
}

async fn read_json<T: for<'de> Deserialize<'de>>(path: &Path) -> GalvoFsResult<T> {
    let bytes = fs::read(path).await.map_err(|error| {
        if error.kind() == std::io::ErrorKind::NotFound {
            GalvoFsError::ArtifactMissing(format!("GalvoFS metadata {} not found", path.display()))
        } else {
            GalvoFsError::RuntimeFailed(format!("failed to read GalvoFS metadata: {error}"))
        }
    })?;
    serde_json::from_slice(&bytes).map_err(|error| {
        GalvoFsError::RuntimeFailed(format!("failed to parse GalvoFS JSON: {error}"))
    })
}

async fn read_json_dir<T: for<'de> Deserialize<'de>>(path: &Path) -> GalvoFsResult<Vec<T>> {
    let mut values = Vec::new();
    let mut entries = match fs::read_dir(path).await {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(values),
        Err(error) => {
            return Err(GalvoFsError::RuntimeFailed(format!(
                "failed to read GalvoFS metadata directory: {error}"
            )));
        }
    };

    while let Some(entry) = entries.next_entry().await.map_err(|error| {
        GalvoFsError::RuntimeFailed(format!("failed to read GalvoFS metadata entry: {error}"))
    })? {
        let path = entry.path();
        let metadata = fs::metadata(&path).await.map_err(|error| {
            GalvoFsError::RuntimeFailed(format!("failed to stat GalvoFS metadata entry: {error}"))
        })?;
        if metadata.is_file()
            && path.extension().and_then(|extension| extension.to_str()) == Some("json")
        {
            values.push(read_json(&path).await?);
        }
    }

    Ok(values)
}

async fn write_json<T: Serialize>(path: &Path, value: &T) -> GalvoFsResult<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).await.map_err(|error| {
            GalvoFsError::RuntimeFailed(format!("failed to create GalvoFS metadata path: {error}"))
        })?;
    }
    let bytes = serde_json::to_vec_pretty(value).map_err(|error| {
        GalvoFsError::RuntimeFailed(format!("failed to encode GalvoFS JSON: {error}"))
    })?;
    replace_document(path, &bytes).await
}

/// Replace a metadata document rather than rewriting it in place.
///
/// A plain write opens with truncate and then writes, so a crash between those
/// two leaves torn JSON — and for a workspace document that loses the entire
/// file index, not the one file being changed. Writing a sibling temporary and
/// renaming it over the target makes the swap atomic on POSIX: a reader sees
/// the whole previous document or the whole new one, never the middle.
///
/// The temporary carries a `.tmp` extension deliberately. `read_json_dir`
/// filters on `.json`, so a temporary orphaned by a crash is invisible to every
/// directory scan instead of being parsed as a malformed record.
///
/// Every metadata document in this crate is written through here — workspaces,
/// leases, storage inputs, bindings, snapshots, artifacts — so they all gain
/// this together.
async fn replace_document(path: &Path, bytes: &[u8]) -> GalvoFsResult<()> {
    let parent = path.parent().ok_or_else(|| {
        GalvoFsError::RuntimeFailed(format!(
            "GalvoFS metadata path {} has no parent directory",
            path.display()
        ))
    })?;
    let temporary = parent.join(format!("{}.tmp", Uuid::new_v4()));

    if let Err(error) = write_and_sync(&temporary, bytes).await {
        let _ = fs::remove_file(&temporary).await;
        return Err(error);
    }
    if let Err(error) = fs::rename(&temporary, path).await {
        let _ = fs::remove_file(&temporary).await;
        return Err(GalvoFsError::RuntimeFailed(format!(
            "failed to install GalvoFS metadata document: {error}"
        )));
    }

    sync_directory(parent).await
}

async fn write_and_sync(path: &Path, bytes: &[u8]) -> GalvoFsResult<()> {
    let mut file = fs::File::create(path).await.map_err(|error| {
        GalvoFsError::RuntimeFailed(format!(
            "failed to create GalvoFS metadata temporary: {error}"
        ))
    })?;
    file.write_all(bytes).await.map_err(|error| {
        GalvoFsError::RuntimeFailed(format!(
            "failed to write GalvoFS metadata temporary: {error}"
        ))
    })?;
    file.sync_all().await.map_err(|error| {
        GalvoFsError::RuntimeFailed(format!(
            "failed to flush GalvoFS metadata temporary: {error}"
        ))
    })
}

/// Sync the directory so the rename itself survives a crash.
///
/// Renaming is atomic but not automatically durable: after a power loss the
/// directory entry can be missing even though the file's own bytes were
/// synced. Opening the directory and syncing it closes that gap.
#[cfg(unix)]
async fn sync_directory(path: &Path) -> GalvoFsResult<()> {
    let directory = fs::File::open(path).await.map_err(|error| {
        GalvoFsError::RuntimeFailed(format!(
            "failed to open GalvoFS metadata directory: {error}"
        ))
    })?;
    directory.sync_all().await.map_err(|error| {
        GalvoFsError::RuntimeFailed(format!(
            "failed to sync GalvoFS metadata directory: {error}"
        ))
    })
}

/// Directories cannot be opened as files here, so the durability of the
/// directory entry is weaker. The rename above is still atomic, which is the
/// property the torn-document guarantee actually rests on.
#[cfg(not(unix))]
async fn sync_directory(_path: &Path) -> GalvoFsResult<()> {
    Ok(())
}

async fn canonicalize_existing_path(path: &Path) -> GalvoFsResult<PathBuf> {
    fs::canonicalize(path).await.map_err(|error| {
        if error.kind() == std::io::ErrorKind::NotFound {
            GalvoFsError::ArtifactMissing(format!(
                "GalvoFS local-path input {} not found",
                path.display()
            ))
        } else {
            GalvoFsError::RuntimeFailed(format!(
                "failed to canonicalize GalvoFS local-path input {}: {error}",
                path.display()
            ))
        }
    })
}

async fn collect_local_input_files(root: &Path) -> GalvoFsResult<Vec<PathBuf>> {
    let mut files = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(path) = stack.pop() {
        let metadata = fs::symlink_metadata(&path).await.map_err(|error| {
            GalvoFsError::RuntimeFailed(format!(
                "failed to stat GalvoFS local-path input {}: {error}",
                path.display()
            ))
        })?;
        if metadata.file_type().is_symlink() {
            return Err(GalvoFsError::AdmissionRejected(
                "GalvoFS local-path input must not contain symlinks".into(),
            ));
        }
        if metadata.is_file() {
            files.push(path);
            continue;
        }
        if !metadata.is_dir() {
            return Err(GalvoFsError::AdmissionRejected(
                "GalvoFS local-path input must contain only files and directories".into(),
            ));
        }
        let mut entries = fs::read_dir(&path).await.map_err(|error| {
            GalvoFsError::RuntimeFailed(format!(
                "failed to list GalvoFS local-path input {}: {error}",
                path.display()
            ))
        })?;
        while let Some(entry) = entries.next_entry().await.map_err(|error| {
            GalvoFsError::RuntimeFailed(format!(
                "failed to read GalvoFS local-path input entry: {error}"
            ))
        })? {
            stack.push(entry.path());
        }
    }
    files.sort();
    Ok(files)
}

fn local_path_from_source_uri(source_uri: &str) -> GalvoFsResult<PathBuf> {
    if let Some(path) = source_uri.strip_prefix("file://") {
        if path.trim().is_empty() {
            return Err(GalvoFsError::ManifestInvalid(
                "GalvoFS file:// local-path sourceUri must include a path".into(),
            ));
        }
        return Ok(PathBuf::from(path));
    }
    Ok(PathBuf::from(source_uri))
}

fn object_digest_from_uri(source_uri: &str) -> GalvoFsResult<String> {
    let Some(hex) = source_uri.strip_prefix("galvofs://objects/sha256/") else {
        return Err(GalvoFsError::ManifestInvalid(
            "GalvoFS uploaded-object import requires galvofs://objects/sha256/ sourceUri".into(),
        ));
    };
    let digest = format!("sha256:{hex}");
    digest_hex(&digest)?;
    Ok(digest)
}

fn workspace_prefix_from_runtime_mount_path(path: &str) -> String {
    path.strip_prefix("/workspace/")
        .or_else(|| path.strip_prefix("/workspace"))
        .unwrap_or_else(|| path.trim_start_matches('/'))
        .trim_matches('/')
        .to_string()
}

fn workspace_file_path_from_runtime_mount_path(path: &str) -> GalvoFsResult<String> {
    let relative = workspace_prefix_from_runtime_mount_path(path);
    normalize_galvo_path(&relative)
}

fn join_workspace_path(base_prefix: &str, relative: &str) -> GalvoFsResult<String> {
    let relative = relative.trim_matches('/');
    if base_prefix.trim().is_empty() {
        normalize_galvo_path(relative)
    } else if relative.is_empty() {
        normalize_galvo_path(base_prefix)
    } else {
        normalize_galvo_path(&format!("{}/{relative}", base_prefix.trim_matches('/')))
    }
}

fn path_to_galvo_relative(path: &Path) -> GalvoFsResult<String> {
    let mut parts = Vec::new();
    for component in path.components() {
        match component {
            Component::Normal(part) => {
                parts.push(part.to_str().ok_or_else(|| {
                    GalvoFsError::ManifestInvalid("GalvoFS local-path input must be UTF-8".into())
                })?);
            }
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(GalvoFsError::AdmissionRejected(
                    "GalvoFS local-path input must not escape its source root".into(),
                ));
            }
        }
    }
    if parts.is_empty() {
        return Err(GalvoFsError::ManifestInvalid(
            "GalvoFS local-path input file path must not be empty".into(),
        ));
    }
    Ok(parts.join("/"))
}

fn enforce_materialization_limits(files: usize, bytes: u64) -> GalvoFsResult<()> {
    if files > MAX_STORAGE_INPUT_FILES {
        return Err(GalvoFsError::AdmissionRejected(format!(
            "GalvoFS storage input exceeds {MAX_STORAGE_INPUT_FILES} file materialization limit"
        )));
    }
    if bytes > MAX_STORAGE_INPUT_BYTES {
        return Err(GalvoFsError::AdmissionRejected(format!(
            "GalvoFS storage input exceeds {MAX_STORAGE_INPUT_BYTES} byte materialization limit"
        )));
    }
    Ok(())
}

fn mount_manifest_digest(
    workspace_id: &str,
    lease_id: Option<&str>,
    policy_digest: &str,
    mounts: &[GalvoMountBinding],
) -> GalvoFsResult<String> {
    let mut payload = serde_json::json!({
        "workspaceId": workspace_id,
        "leaseId": lease_id,
        "policyDigest": policy_digest,
        "mounts": mounts,
    });
    sort_json(&mut payload);
    let bytes = serde_json::to_vec(&payload).map_err(|error| {
        GalvoFsError::RuntimeFailed(format!(
            "failed to encode GalvoFS mount manifest digest: {error}"
        ))
    })?;
    Ok(sha256_digest(&bytes))
}

fn sort_json(value: &mut Value) {
    match value {
        Value::Array(items) => {
            for item in items {
                sort_json(item);
            }
        }
        Value::Object(map) => {
            let mut sorted = BTreeMap::new();
            for (key, mut child) in std::mem::take(map) {
                sort_json(&mut child);
                sorted.insert(key, child);
            }
            for (key, child) in sorted {
                map.insert(key, child);
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
    }
}

fn normalize_galvo_path(relative_path: &str) -> GalvoFsResult<String> {
    if relative_path.trim().is_empty() {
        return Err(GalvoFsError::ManifestInvalid(
            "GalvoFS path must not be empty".into(),
        ));
    }
    let path = Path::new(relative_path);
    if path.is_absolute() {
        return Err(GalvoFsError::AdmissionRejected(
            "GalvoFS path must be relative".into(),
        ));
    }

    let mut parts = Vec::new();
    for component in path.components() {
        match component {
            Component::Normal(part) => {
                let part = part.to_str().ok_or_else(|| {
                    GalvoFsError::ManifestInvalid("GalvoFS path must be UTF-8".into())
                })?;
                parts.push(part.to_string());
            }
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(GalvoFsError::AdmissionRejected(
                    "GalvoFS path must not escape the workspace namespace".into(),
                ));
            }
        }
    }

    if parts.is_empty() {
        return Err(GalvoFsError::ManifestInvalid(
            "GalvoFS path must name a file".into(),
        ));
    }
    Ok(parts.join("/"))
}

fn validate_runtime_mount_path(path: &str) -> GalvoFsResult<()> {
    require_non_empty("mount.path", path)?;
    if !path.starts_with('/') {
        return Err(GalvoFsError::ManifestInvalid(
            "GalvoFS mount path must be absolute inside the runtime namespace".into(),
        ));
    }
    if path.contains("..") {
        return Err(GalvoFsError::AdmissionRejected(
            "GalvoFS mount path must not contain parent traversal".into(),
        ));
    }
    Ok(())
}

fn validate_storage_input_source_uri(
    source_kind: GalvoFsStorageInputSourceKind,
    source_uri: &str,
) -> GalvoFsResult<()> {
    match source_kind {
        GalvoFsStorageInputSourceKind::GalvofsWorkspace
            if !source_uri.starts_with("galvofs://workspaces/") =>
        {
            Err(GalvoFsError::ManifestInvalid(
                "GalvoFS galvofs-workspace input must use galvofs://workspaces/ sourceUri".into(),
            ))
        }
        GalvoFsStorageInputSourceKind::UploadedObject
            if !source_uri.starts_with("galvofs://objects/sha256/")
                && !source_uri.starts_with("galvofs://workspaces/") =>
        {
            Err(GalvoFsError::ManifestInvalid(
                "GalvoFS uploaded-object input must use a GalvoFS object or workspace sourceUri"
                    .into(),
            ))
        }
        GalvoFsStorageInputSourceKind::LocalPath
            if !source_uri.starts_with('/') && !source_uri.starts_with("file://") =>
        {
            Err(GalvoFsError::ManifestInvalid(
                "GalvoFS local-path input must use an absolute path or file:// sourceUri".into(),
            ))
        }
        GalvoFsStorageInputSourceKind::S3Prefix if !source_uri.starts_with("s3://") => {
            Err(GalvoFsError::ManifestInvalid(
                "GalvoFS s3-prefix input must use s3:// sourceUri".into(),
            ))
        }
        GalvoFsStorageInputSourceKind::SiteFilesystemReference
        | GalvoFsStorageInputSourceKind::GalvofsWorkspace
        | GalvoFsStorageInputSourceKind::UploadedObject
        | GalvoFsStorageInputSourceKind::LocalPath
        | GalvoFsStorageInputSourceKind::S3Prefix => Ok(()),
    }
}

fn storage_input_source_kind_label(source_kind: GalvoFsStorageInputSourceKind) -> &'static str {
    match source_kind {
        GalvoFsStorageInputSourceKind::GalvofsWorkspace => "galvofs-workspace",
        GalvoFsStorageInputSourceKind::UploadedObject => "uploaded-object",
        GalvoFsStorageInputSourceKind::LocalPath => "local-path",
        GalvoFsStorageInputSourceKind::S3Prefix => "s3-prefix",
        GalvoFsStorageInputSourceKind::SiteFilesystemReference => "site-filesystem-reference",
    }
}

fn storage_input_verification_status(
    source_kind: GalvoFsStorageInputSourceKind,
    digest_policy: GalvoFsDigestPolicy,
) -> GalvoFsInputVerificationStatus {
    match (source_kind, digest_policy) {
        (_, GalvoFsDigestPolicy::Deferred)
        | (GalvoFsStorageInputSourceKind::SiteFilesystemReference, _) => {
            GalvoFsInputVerificationStatus::Deferred
        }
        (_, GalvoFsDigestPolicy::Required | GalvoFsDigestPolicy::BestEffort) => {
            GalvoFsInputVerificationStatus::Pending
        }
    }
}

fn require_non_empty(field: &str, value: &str) -> GalvoFsResult<()> {
    if value.trim().is_empty() {
        return Err(GalvoFsError::ManifestInvalid(format!(
            "GalvoFS {field} must not be empty"
        )));
    }
    Ok(())
}

fn sha256_digest(bytes: &[u8]) -> String {
    format!("sha256:{}", hex_sha256(bytes))
}

fn hex_sha256(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut encoded = String::with_capacity(digest.len() * 2);
    for byte in digest {
        encoded.push_str(&format!("{byte:02x}"));
    }
    encoded
}

fn digest_hex(digest: &str) -> GalvoFsResult<&str> {
    let hex = digest.strip_prefix(SHA256_PREFIX).ok_or_else(|| {
        GalvoFsError::ManifestInvalid("GalvoFS object digest must use sha256: prefix".into())
    })?;
    if hex.is_empty() || hex.len() != 64 || !hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(GalvoFsError::ManifestInvalid(
            "GalvoFS object digest must be a sha256 hex digest".into(),
        ));
    }
    Ok(hex)
}

fn object_uri(digest: &str) -> GalvoFsResult<String> {
    Ok(format!("galvofs://objects/sha256/{}", digest_hex(digest)?))
}

fn opendal_config_error(error: opendal::Error) -> GalvoFsError {
    GalvoFsError::ManifestInvalid(format!(
        "failed to configure OpenDAL GalvoFS backend: {error}"
    ))
}

fn opendal_runtime_error(error: opendal::Error) -> GalvoFsError {
    if error.kind() == OpenDalErrorKind::NotFound {
        GalvoFsError::ArtifactMissing(format!("GalvoFS OpenDAL object not found: {error}"))
    } else {
        GalvoFsError::RuntimeFailed(format!("GalvoFS OpenDAL operation failed: {error}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The workspace lock is the preventer; the version field is the detector.
    ///
    /// An integration test that races two writers only ever exercises the
    /// preventer, because the lock stops the second writer from reaching the
    /// commit with a stale document. That would leave the detector asserted by
    /// nothing — present, plausible, and never fired. This drives it directly.
    ///
    /// It also pins the property the error mapping depends on: a refused commit
    /// leaves the workspace exactly as it was.
    #[tokio::test]
    async fn commit_refuses_a_workspace_that_moved_underneath_it() {
        let root = std::env::temp_dir().join(format!("galvofs-unit-conflict-{}", Uuid::new_v4()));
        let galvofs = GalvoFs::local(&root);
        let workspace = galvofs
            .create_workspace(CreateGalvoWorkspaceRequest {
                tenant_id: "tenant-alpha".into(),
                project_id: "project-research".into(),
                owner_id: "owner".into(),
                workspace_name: "conflict".into(),
                purpose: "agent-compute".into(),
            })
            .await
            .expect("workspace should be created");

        // Take a copy of the document, then let a real write advance it.
        let mut stale = galvofs
            .read_workspace_state(&workspace.workspace_id)
            .await
            .expect("state should read");
        galvofs
            .write_file(&workspace.workspace_id, "landed.txt", b"landed")
            .await
            .expect("write should land");

        stale.files.insert(
            "stale.txt".into(),
            stale
                .files
                .values()
                .next()
                .cloned()
                .unwrap_or(GalvoFsFileRecord {
                    path: "stale.txt".into(),
                    kind: GalvoFsFileKind::File,
                    size_bytes: 0,
                    digest: format!("{SHA256_PREFIX}0"),
                    object_uri: "memory://stale".into(),
                }),
        );
        let error = galvofs
            .commit_workspace_state(&mut stale)
            .await
            .expect_err("committing a stale document should be refused");
        assert!(
            matches!(error, GalvoFsError::Conflict(_)),
            "expected a Conflict, got {error:?}"
        );

        let files = galvofs
            .list_files(&workspace.workspace_id)
            .await
            .expect("workspace should still list");
        let paths: Vec<&str> = files.iter().map(|file| file.path.as_str()).collect();
        assert_eq!(
            paths,
            vec!["landed.txt"],
            "a refused commit must leave the workspace unchanged"
        );

        let _ = std::fs::remove_dir_all(root);
    }
}
