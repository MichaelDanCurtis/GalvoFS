use std::time::{SystemTime, UNIX_EPOCH};

use galvofs_core::{
    AttachGalvoStorageInputRequest, CreateGalvoMountManifestRequest,
    CreateGalvoStorageInputRequest, CreateGalvoWorkspaceRequest, GalvoFs, GalvoFsAccessMode,
    GalvoFsDiffKind, GalvoFsDigestPolicy, GalvoFsFileKind, GalvoFsGarbageCollectionRequest,
    GalvoFsInputMaterialization, GalvoFsInputVerificationStatus, GalvoFsIntegrityStatus,
    GalvoFsStorageInputSourceKind, GalvoFsWritebackPolicy, GalvoMountBinding,
    IssueGalvoStorageLeaseRequest, PublishGalvoArtifactRequest, UploadGalvoStorageObjectRequest,
};
use sha2::Digest;
use time::{Duration, OffsetDateTime};

fn test_store_root(label: &str) -> std::path::PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("galvofs-{label}-{}-{nanos}", std::process::id()))
}

fn workspace_request(name: &str) -> CreateGalvoWorkspaceRequest {
    CreateGalvoWorkspaceRequest {
        tenant_id: "tenant-alpha".into(),
        project_id: "project-research".into(),
        owner_id: "galvo-agent-researcher".into(),
        workspace_name: name.into(),
        purpose: "agent-compute".into(),
    }
}

fn lease_request(workspace_id: &str, access: GalvoFsAccessMode) -> IssueGalvoStorageLeaseRequest {
    IssueGalvoStorageLeaseRequest {
        workspace_id: workspace_id.into(),
        tenant_id: "tenant-alpha".into(),
        project_id: "project-research".into(),
        secure_entity_id: "secure-entity-agent-run".into(),
        work_order_id: "wo-agent-run".into(),
        a2e_ticket_id: Some("ticket-agent-run".into()),
        runtime_id: Some("runtime-femto-local".into()),
        access,
        expires_at: OffsetDateTime::now_utc() + Duration::minutes(5),
    }
}

fn s3_storage_input_request() -> CreateGalvoStorageInputRequest {
    CreateGalvoStorageInputRequest {
        tenant_id: "tenant-alpha".into(),
        project_id: "project-research".into(),
        name: "training-dataset".into(),
        source_kind: GalvoFsStorageInputSourceKind::S3Prefix,
        source_uri: "s3://research-data/training/".into(),
        materialization: GalvoFsInputMaterialization::Reference,
        access: GalvoFsAccessMode::ReadOnly,
        mount_path: "/workspace/data".into(),
        digest_policy: GalvoFsDigestPolicy::Deferred,
        retention_policy: "retain-run".into(),
        writeback_policy: GalvoFsWritebackPolicy::None,
    }
}

fn local_path_import_request(source_uri: String) -> CreateGalvoStorageInputRequest {
    CreateGalvoStorageInputRequest {
        tenant_id: "tenant-alpha".into(),
        project_id: "project-research".into(),
        name: "local-dataset".into(),
        source_kind: GalvoFsStorageInputSourceKind::LocalPath,
        source_uri,
        materialization: GalvoFsInputMaterialization::Import,
        access: GalvoFsAccessMode::ReadOnly,
        mount_path: "/workspace/data".into(),
        digest_policy: GalvoFsDigestPolicy::Required,
        retention_policy: "retain-run".into(),
        writeback_policy: GalvoFsWritebackPolicy::None,
    }
}

fn local_object_path(root: &std::path::Path, digest: &str) -> std::path::PathBuf {
    let hex = digest
        .strip_prefix("sha256:")
        .expect("test digest should use sha256 prefix");
    root.join("objects").join("sha256").join(hex)
}

fn policy_digest(label: &str) -> String {
    let digest = sha2::Sha256::digest(label.as_bytes());
    let value = digest
        .iter()
        .fold(String::with_capacity(digest.len() * 2), |mut acc, byte| {
            use std::fmt::Write;
            write!(&mut acc, "{byte:02x}").expect("hex digest write should not fail");
            acc
        });
    format!("sha256:{value}")
}

#[tokio::test]
async fn galvofs_writes_lists_and_reads_workspace_files_from_object_store() {
    let root = test_store_root("io");
    let galvofs = GalvoFs::local(&root);
    let workspace = galvofs
        .create_workspace(workspace_request("agent-root"))
        .await
        .expect("workspace should be created");

    let record = galvofs
        .write_file(
            &workspace.workspace_id,
            "notes/result.txt",
            b"{\"ok\":true}",
        )
        .await
        .expect("file should be written");

    assert_eq!(record.path, "notes/result.txt");
    assert_eq!(record.kind, GalvoFsFileKind::File);
    assert_eq!(record.size_bytes, 11);
    assert!(record.digest.starts_with("sha256:"));
    assert!(record.object_uri.starts_with("galvofs://objects/sha256/"));

    let files = galvofs
        .list_files(&workspace.workspace_id)
        .await
        .expect("workspace files should list");
    assert_eq!(files, vec![record.clone()]);

    let data = galvofs
        .read_file(&workspace.workspace_id, "notes/result.txt")
        .await
        .expect("workspace file should be readable");
    assert_eq!(data.record, record);
    assert_eq!(data.bytes, b"{\"ok\":true}");

    let receipts = galvofs
        .receipts()
        .await
        .expect("storage receipts should be readable");
    assert_eq!(
        receipts
            .iter()
            .map(|receipt| receipt.operation.as_str())
            .collect::<Vec<_>>(),
        vec!["workspace.create", "file.write", "file.read"]
    );

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn galvofs_opendal_fs_backend_writes_objects_outside_metadata_root() {
    let metadata_root = test_store_root("opendal-metadata");
    let object_root = test_store_root("opendal-objects");
    let galvofs = GalvoFs::opendal_fs(&metadata_root, &object_root)
        .expect("OpenDAL fs backend should configure");
    let workspace = galvofs
        .create_workspace(workspace_request("agent-opendal"))
        .await
        .expect("workspace should be created");

    let record = galvofs
        .write_file(&workspace.workspace_id, "notes/result.txt", b"opendal")
        .await
        .expect("file should be written through OpenDAL");

    let hex = record
        .digest
        .strip_prefix("sha256:")
        .expect("test digest should use sha256 prefix");
    assert!(object_root.join("sha256").join(hex).exists());
    assert!(!metadata_root
        .join("objects")
        .join("sha256")
        .join(hex)
        .exists());

    let read = galvofs
        .read_file(&workspace.workspace_id, "notes/result.txt")
        .await
        .expect("file should read through OpenDAL");
    assert_eq!(read.bytes, b"opendal");

    let integrity = galvofs
        .verify_integrity()
        .await
        .expect("integrity should inspect OpenDAL objects");
    assert_eq!(integrity.status, GalvoFsIntegrityStatus::Healthy);
    assert_eq!(integrity.checked_objects, 1);

    let _ = std::fs::remove_dir_all(metadata_root);
    let _ = std::fs::remove_dir_all(object_root);
}

#[tokio::test]
async fn galvofs_snapshots_clones_diffs_and_rolls_back_workspaces() {
    let root = test_store_root("snapshot");
    let galvofs = GalvoFs::local(&root);
    let workspace = galvofs
        .create_workspace(workspace_request("source-root"))
        .await
        .expect("workspace should be created");
    let original = galvofs
        .write_file(&workspace.workspace_id, "src/main.txt", b"one")
        .await
        .expect("original file should be written");
    let snapshot = galvofs
        .snapshot_workspace(&workspace.workspace_id, "before-agent-branch")
        .await
        .expect("snapshot should pin workspace");

    let clone = galvofs
        .clone_workspace(
            &snapshot.snapshot_id,
            CreateGalvoWorkspaceRequest {
                workspace_name: "agent-branch".into(),
                ..workspace_request("unused")
            },
        )
        .await
        .expect("snapshot should clone");

    galvofs
        .write_file(&clone.workspace.workspace_id, "src/main.txt", b"two")
        .await
        .expect("clone file should be modified");
    galvofs
        .write_file(&clone.workspace.workspace_id, "src/extra.txt", b"extra")
        .await
        .expect("clone file should be added");

    let diff = galvofs
        .diff_workspace(&clone.workspace.workspace_id, &snapshot.snapshot_id)
        .await
        .expect("clone should diff against snapshot");
    assert_eq!(diff.baseline_snapshot_id, snapshot.snapshot_id);
    assert_eq!(
        diff.changes
            .iter()
            .map(|change| (&change.path, &change.kind))
            .collect::<Vec<_>>(),
        vec![
            (&"src/extra.txt".to_string(), &GalvoFsDiffKind::Added),
            (&"src/main.txt".to_string(), &GalvoFsDiffKind::Modified),
        ]
    );

    galvofs
        .rollback_workspace(&clone.workspace.workspace_id, &snapshot.snapshot_id)
        .await
        .expect("clone should roll back to snapshot");
    let rolled_back = galvofs
        .list_files(&clone.workspace.workspace_id)
        .await
        .expect("rolled back clone should list");
    assert_eq!(rolled_back, vec![original]);

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn galvofs_publishes_artifacts_with_receipt_ready_digest_metadata() {
    let root = test_store_root("artifact");
    let galvofs = GalvoFs::local(&root);
    let workspace = galvofs
        .create_workspace(workspace_request("artifact-root"))
        .await
        .expect("workspace should be created");
    let file = galvofs
        .write_file(&workspace.workspace_id, "out/model.bin", b"weights")
        .await
        .expect("artifact source should be written");

    let artifact = galvofs
        .publish_artifact(PublishGalvoArtifactRequest {
            workspace_id: workspace.workspace_id.clone(),
            path: "out/model.bin".into(),
            content_type: Some("application/octet-stream".into()),
            retention_policy: "retain-30d".into(),
        })
        .await
        .expect("artifact should publish");

    assert_eq!(artifact.workspace_id, workspace.workspace_id);
    assert_eq!(artifact.path, "out/model.bin");
    assert_eq!(artifact.digest, file.digest);
    assert_eq!(artifact.size_bytes, 7);
    assert_eq!(
        artifact.content_type.as_deref(),
        Some("application/octet-stream")
    );
    assert_eq!(artifact.retention_policy, "retain-30d");
    assert!(artifact.artifact_id.starts_with("artifact-"));
    assert!(artifact.object_uri.starts_with("galvofs://objects/sha256/"));

    let receipts = galvofs
        .receipts()
        .await
        .expect("storage receipts should be readable");
    let publish_receipt = receipts
        .iter()
        .find(|receipt| receipt.operation == "artifact.publish")
        .expect("artifact publish should emit a receipt");
    assert_eq!(publish_receipt.workspace_id, Some(artifact.workspace_id));
    assert_eq!(publish_receipt.object_digest, Some(artifact.digest));
    assert_eq!(publish_receipt.artifact_id, Some(artifact.artifact_id));

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn galvofs_rejects_paths_that_escape_the_workspace_namespace() {
    let root = test_store_root("path-safety");
    let galvofs = GalvoFs::local(&root);
    let workspace = galvofs
        .create_workspace(workspace_request("safe-root"))
        .await
        .expect("workspace should be created");

    let error = galvofs
        .write_file(&workspace.workspace_id, "../escape.txt", b"nope")
        .await
        .expect_err("workspace paths must not escape the namespace");

    assert!(
        error.to_string().contains("GalvoFS path"),
        "unexpected path rejection: {error}"
    );

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn galvofs_storage_leases_scope_workspace_read_and_write_access() {
    let root = test_store_root("lease-rw");
    let galvofs = GalvoFs::local(&root);
    let workspace = galvofs
        .create_workspace(workspace_request("leased-root"))
        .await
        .expect("workspace should be created");
    let lease = galvofs
        .issue_storage_lease(lease_request(
            &workspace.workspace_id,
            GalvoFsAccessMode::ReadWrite,
        ))
        .await
        .expect("read-write lease should issue");

    assert_eq!(lease.workspace_id, workspace.workspace_id);
    assert_eq!(lease.access, GalvoFsAccessMode::ReadWrite);
    assert_eq!(lease.work_order_id, "wo-agent-run");
    assert_eq!(lease.a2e_ticket_id.as_deref(), Some("ticket-agent-run"));

    let record = galvofs
        .write_file_with_lease(&lease.lease_id, "leased/result.txt", b"leased")
        .await
        .expect("read-write lease should allow writes");
    assert_eq!(record.path, "leased/result.txt");

    let data = galvofs
        .read_file_with_lease(&lease.lease_id, "leased/result.txt")
        .await
        .expect("read-write lease should allow reads");
    assert_eq!(data.bytes, b"leased");

    let receipts = galvofs
        .receipts()
        .await
        .expect("storage receipts should be readable");
    assert!(receipts
        .iter()
        .any(|receipt| receipt.operation == "storage-lease.issue"
            && receipt.lease_id.as_deref() == Some(&lease.lease_id)));

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn galvofs_storage_leases_fail_closed_for_read_only_and_expired_access() {
    let root = test_store_root("lease-deny");
    let galvofs = GalvoFs::local(&root);
    let workspace = galvofs
        .create_workspace(workspace_request("leased-root"))
        .await
        .expect("workspace should be created");
    galvofs
        .write_file(&workspace.workspace_id, "input.txt", b"seed")
        .await
        .expect("seed file should be written");

    let read_only = galvofs
        .issue_storage_lease(lease_request(
            &workspace.workspace_id,
            GalvoFsAccessMode::ReadOnly,
        ))
        .await
        .expect("read-only lease should issue");
    galvofs
        .read_file_with_lease(&read_only.lease_id, "input.txt")
        .await
        .expect("read-only lease should allow reads");
    let error = galvofs
        .write_file_with_lease(&read_only.lease_id, "blocked.txt", b"blocked")
        .await
        .expect_err("read-only lease must reject writes");
    assert!(
        error.to_string().contains("does not allow write"),
        "unexpected read-only rejection: {error}"
    );

    let expired = galvofs
        .issue_storage_lease(IssueGalvoStorageLeaseRequest {
            expires_at: OffsetDateTime::now_utc() - Duration::seconds(1),
            ..lease_request(&workspace.workspace_id, GalvoFsAccessMode::ReadWrite)
        })
        .await
        .expect("expired lease records should still issue for audit replay");
    let error = galvofs
        .read_file_with_lease(&expired.lease_id, "input.txt")
        .await
        .expect_err("expired lease must reject reads");
    assert!(
        error.to_string().contains("expired"),
        "unexpected expired rejection: {error}"
    );

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn galvofs_integrity_verification_reports_healthy_missing_and_corrupt_objects() {
    let healthy_root = test_store_root("integrity-healthy");
    let galvofs = GalvoFs::local(&healthy_root);
    let workspace = galvofs
        .create_workspace(workspace_request("integrity-root"))
        .await
        .expect("workspace should be created");
    let healthy = galvofs
        .write_file(&workspace.workspace_id, "healthy.txt", b"healthy")
        .await
        .expect("healthy file should be written");
    galvofs
        .snapshot_workspace(&workspace.workspace_id, "integrity")
        .await
        .expect("snapshot should be created");
    galvofs
        .publish_artifact(PublishGalvoArtifactRequest {
            workspace_id: workspace.workspace_id.clone(),
            path: "healthy.txt".into(),
            content_type: Some("text/plain".into()),
            retention_policy: "retain-30d".into(),
        })
        .await
        .expect("artifact should publish");

    let report = galvofs
        .verify_integrity()
        .await
        .expect("healthy store should verify");
    assert_eq!(report.status, GalvoFsIntegrityStatus::Healthy);
    assert_eq!(report.checked_objects, 1);
    assert!(report.missing_objects.is_empty());
    assert!(report.corrupt_objects.is_empty());
    assert!(local_object_path(&healthy_root, &healthy.digest).is_file());

    let missing_root = test_store_root("integrity-missing");
    let galvofs = GalvoFs::local(&missing_root);
    let workspace = galvofs
        .create_workspace(workspace_request("missing-root"))
        .await
        .expect("workspace should be created");
    let missing = galvofs
        .write_file(&workspace.workspace_id, "missing.txt", b"missing")
        .await
        .expect("missing file should be written");
    std::fs::remove_file(local_object_path(&missing_root, &missing.digest))
        .expect("test should remove object body");

    let report = galvofs
        .verify_integrity()
        .await
        .expect("missing store should produce report");
    assert_eq!(report.status, GalvoFsIntegrityStatus::Degraded);
    assert_eq!(report.checked_objects, 1);
    assert_eq!(report.missing_objects.len(), 1);
    assert_eq!(report.missing_objects[0].digest, missing.digest);

    let corrupt_root = test_store_root("integrity-corrupt");
    let galvofs = GalvoFs::local(&corrupt_root);
    let workspace = galvofs
        .create_workspace(workspace_request("corrupt-root"))
        .await
        .expect("workspace should be created");
    let corrupt = galvofs
        .write_file(&workspace.workspace_id, "corrupt.txt", b"original")
        .await
        .expect("corrupt file should be written");
    std::fs::write(
        local_object_path(&corrupt_root, &corrupt.digest),
        b"tampered",
    )
    .expect("test should corrupt object body");

    let report = galvofs
        .verify_integrity()
        .await
        .expect("corrupt store should produce report");
    assert_eq!(report.status, GalvoFsIntegrityStatus::Degraded);
    assert_eq!(report.checked_objects, 1);
    assert_eq!(report.corrupt_objects.len(), 1);
    assert_eq!(report.corrupt_objects[0].digest, corrupt.digest);

    let _ = std::fs::remove_dir_all(healthy_root);
    let _ = std::fs::remove_dir_all(missing_root);
    let _ = std::fs::remove_dir_all(corrupt_root);
}

#[tokio::test]
async fn galvofs_mount_manifests_have_stable_digests_and_receipts() {
    let root = test_store_root("mount-manifest");
    let galvofs = GalvoFs::local(&root);
    let workspace = galvofs
        .create_workspace(workspace_request("mount-root"))
        .await
        .expect("workspace should be created");
    let lease = galvofs
        .issue_storage_lease(lease_request(
            &workspace.workspace_id,
            GalvoFsAccessMode::ReadWrite,
        ))
        .await
        .expect("lease should issue");
    let request = CreateGalvoMountManifestRequest {
        workspace_id: workspace.workspace_id.clone(),
        lease_id: Some(lease.lease_id.clone()),
        policy_digest: policy_digest("mount-policy"),
        mounts: vec![
            GalvoMountBinding {
                name: "workspace".into(),
                path: "/workspace".into(),
                source_uri: format!("galvofs://workspaces/{}", workspace.workspace_id),
                storage_input_id: None,
                access: GalvoFsAccessMode::ReadWrite,
            },
            GalvoMountBinding {
                name: "context".into(),
                path: "/context".into(),
                source_uri: "mirage://context/research-alpha".into(),
                storage_input_id: None,
                access: GalvoFsAccessMode::ReadOnly,
            },
        ],
    };

    let first = galvofs
        .create_mount_manifest(request.clone())
        .await
        .expect("mount manifest should be created");
    let second = galvofs
        .create_mount_manifest(request)
        .await
        .expect("same mount manifest should be recreated");

    assert!(first.mount_manifest_id.starts_with("mount-manifest-"));
    assert_ne!(first.mount_manifest_id, second.mount_manifest_id);
    assert_eq!(first.workspace_id, workspace.workspace_id);
    assert_eq!(first.lease_id.as_deref(), Some(lease.lease_id.as_str()));
    assert_eq!(first.digest, second.digest);
    assert!(first.digest.starts_with("sha256:"));
    assert_eq!(first.mounts.len(), 2);

    let receipts = galvofs
        .receipts()
        .await
        .expect("storage receipts should be readable");
    assert!(receipts.iter().any(|receipt| {
        receipt.operation == "mount-manifest.create"
            && receipt.mount_manifest_digest.as_deref() == Some(first.digest.as_str())
            && receipt.lease_id.as_deref() == Some(lease.lease_id.as_str())
    }));

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn galvofs_mount_manifests_enforce_lease_access() {
    let root = test_store_root("mount-manifest-deny");
    let galvofs = GalvoFs::local(&root);
    let workspace = galvofs
        .create_workspace(workspace_request("mount-root"))
        .await
        .expect("workspace should be created");
    let lease = galvofs
        .issue_storage_lease(lease_request(
            &workspace.workspace_id,
            GalvoFsAccessMode::ReadOnly,
        ))
        .await
        .expect("read-only lease should issue");

    let error = galvofs
        .create_mount_manifest(CreateGalvoMountManifestRequest {
            workspace_id: workspace.workspace_id.clone(),
            lease_id: Some(lease.lease_id.clone()),
            policy_digest: policy_digest("mount-policy"),
            mounts: vec![GalvoMountBinding {
                name: "workspace".into(),
                path: "/workspace".into(),
                source_uri: format!("galvofs://workspaces/{}", workspace.workspace_id),
                storage_input_id: None,
                access: GalvoFsAccessMode::ReadWrite,
            }],
        })
        .await
        .expect_err("read-only lease must reject writeable mounts");

    assert!(
        error.to_string().contains("does not allow write"),
        "unexpected mount lease rejection: {error}"
    );

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn galvofs_storage_inputs_are_admitted_attached_and_mount_manifest_referenced() {
    let root = test_store_root("storage-input");
    let galvofs = GalvoFs::local(&root);
    let workspace = galvofs
        .create_workspace(workspace_request("input-root"))
        .await
        .expect("workspace should be created");
    let lease = galvofs
        .issue_storage_lease(lease_request(
            &workspace.workspace_id,
            GalvoFsAccessMode::ReadWrite,
        ))
        .await
        .expect("lease should issue");

    let input = galvofs
        .create_storage_input(s3_storage_input_request())
        .await
        .expect("storage input should be admitted");

    assert!(input.input_id.starts_with("storage-input-"));
    assert_eq!(input.tenant_id, "tenant-alpha");
    assert_eq!(input.project_id, "project-research");
    assert_eq!(input.source_kind, GalvoFsStorageInputSourceKind::S3Prefix);
    assert_eq!(input.source_uri, "s3://research-data/training/");
    assert_eq!(
        input.materialization,
        GalvoFsInputMaterialization::Reference
    );
    assert_eq!(input.access, GalvoFsAccessMode::ReadOnly);
    assert_eq!(input.mount_path, "/workspace/data");
    assert_eq!(input.digest_policy, GalvoFsDigestPolicy::Deferred);
    assert_eq!(
        input.verification_status,
        GalvoFsInputVerificationStatus::Deferred
    );
    assert_eq!(input.writeback_policy, GalvoFsWritebackPolicy::None);

    let binding = galvofs
        .attach_storage_input(AttachGalvoStorageInputRequest {
            workspace_id: workspace.workspace_id.clone(),
            input_id: input.input_id.clone(),
            lease_id: Some(lease.lease_id.clone()),
            mount_path: "/workspace/data".into(),
            materialization: GalvoFsInputMaterialization::Reference,
            access: GalvoFsAccessMode::ReadOnly,
            writeback_policy: GalvoFsWritebackPolicy::None,
        })
        .await
        .expect("storage input should attach to workspace");

    assert!(binding.binding_id.starts_with("storage-input-binding-"));
    assert_eq!(binding.workspace_id, workspace.workspace_id);
    assert_eq!(binding.input_id, input.input_id);
    assert_eq!(binding.lease_id.as_deref(), Some(lease.lease_id.as_str()));
    assert_eq!(binding.mount_path, "/workspace/data");
    assert_eq!(binding.source_uri, "s3://research-data/training/");
    assert_eq!(binding.verification_status, input.verification_status);

    let bindings = galvofs
        .workspace_storage_inputs(&workspace.workspace_id)
        .await
        .expect("workspace inputs should list");
    assert_eq!(bindings, vec![binding.clone()]);

    let manifest = galvofs
        .create_mount_manifest(CreateGalvoMountManifestRequest {
            workspace_id: workspace.workspace_id.clone(),
            lease_id: Some(lease.lease_id.clone()),
            policy_digest: policy_digest("input-mount-policy"),
            mounts: vec![GalvoMountBinding {
                name: "data".into(),
                path: "/workspace/data".into(),
                source_uri: input.source_uri.clone(),
                storage_input_id: Some(input.input_id.clone()),
                access: GalvoFsAccessMode::ReadOnly,
            }],
        })
        .await
        .expect("mount manifest should reference storage input");

    assert_eq!(
        manifest.mounts[0].storage_input_id.as_deref(),
        Some(input.input_id.as_str())
    );

    let receipts = galvofs
        .receipts()
        .await
        .expect("storage receipts should be readable");
    assert!(receipts.iter().any(|receipt| {
        receipt.operation == "storage-input.create"
            && receipt.storage_input_id.as_deref() == Some(input.input_id.as_str())
            && receipt.source_uri.as_deref() == Some(input.source_uri.as_str())
    }));
    assert!(receipts.iter().any(|receipt| {
        receipt.operation == "storage-input.attach"
            && receipt.storage_input_id.as_deref() == Some(input.input_id.as_str())
            && receipt.workspace_id.as_deref() == Some(workspace.workspace_id.as_str())
            && receipt.lease_id.as_deref() == Some(lease.lease_id.as_str())
    }));

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn galvofs_imports_allowed_local_path_storage_input_into_workspace() {
    let root = test_store_root("storage-input-local-import");
    let source_root = root.join("allowed-inputs");
    let dataset = source_root.join("dataset");
    std::fs::create_dir_all(dataset.join("nested")).expect("source dirs should create");
    std::fs::write(dataset.join("train.txt"), b"training rows").expect("source file should write");
    std::fs::write(dataset.join("nested").join("labels.txt"), b"labels")
        .expect("nested source file should write");

    let galvofs = GalvoFs::local_with_storage_input_roots(&root, [source_root.clone()]);
    let workspace = galvofs
        .create_workspace(workspace_request("local-import-root"))
        .await
        .expect("workspace should be created");
    let lease = galvofs
        .issue_storage_lease(lease_request(
            &workspace.workspace_id,
            GalvoFsAccessMode::ReadWrite,
        ))
        .await
        .expect("lease should issue");
    let input = galvofs
        .create_storage_input(local_path_import_request(format!(
            "file://{}",
            dataset.display()
        )))
        .await
        .expect("local path input should be admitted");
    assert_eq!(
        input.verification_status,
        GalvoFsInputVerificationStatus::Pending
    );

    let binding = galvofs
        .attach_storage_input(AttachGalvoStorageInputRequest {
            workspace_id: workspace.workspace_id.clone(),
            input_id: input.input_id.clone(),
            lease_id: Some(lease.lease_id.clone()),
            mount_path: "/workspace/data".into(),
            materialization: GalvoFsInputMaterialization::Import,
            access: GalvoFsAccessMode::ReadOnly,
            writeback_policy: GalvoFsWritebackPolicy::None,
        })
        .await
        .expect("local path input should materialize into workspace");

    assert_eq!(
        binding.verification_status,
        GalvoFsInputVerificationStatus::Verified
    );

    let files = galvofs
        .list_files(&workspace.workspace_id)
        .await
        .expect("workspace files should list");
    let paths = files
        .iter()
        .map(|file| file.path.as_str())
        .collect::<Vec<_>>();
    assert_eq!(paths, vec!["data/nested/labels.txt", "data/train.txt"]);

    let data = galvofs
        .read_file(&workspace.workspace_id, "data/train.txt")
        .await
        .expect("imported file should read");
    assert_eq!(data.bytes, b"training rows");

    let tasks = galvofs
        .storage_input_materialization_tasks(&input.input_id)
        .await
        .expect("materialization tasks should list");
    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0].input_id, input.input_id);
    assert_eq!(tasks[0].workspace_id, workspace.workspace_id);
    assert_eq!(tasks[0].status.as_str(), "completed");
    assert_eq!(tasks[0].files_materialized, 2);
    assert_eq!(tasks[0].bytes_materialized, 19);

    let receipts = galvofs
        .receipts()
        .await
        .expect("storage receipts should be readable");
    assert!(receipts.iter().any(|receipt| {
        receipt.operation == "storage-input.materialize.complete"
            && receipt.storage_input_id.as_deref() == Some(input.input_id.as_str())
            && receipt.workspace_id.as_deref() == Some(workspace.workspace_id.as_str())
    }));

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn galvofs_rejects_local_path_storage_input_outside_allowlist() {
    let root = test_store_root("storage-input-local-deny");
    let allowed_root = root.join("allowed-inputs");
    let denied_root = root.join("denied-inputs");
    std::fs::create_dir_all(&allowed_root).expect("allowed root should create");
    std::fs::create_dir_all(&denied_root).expect("denied root should create");
    std::fs::write(denied_root.join("secret.txt"), b"secret")
        .expect("denied source file should write");

    let galvofs = GalvoFs::local_with_storage_input_roots(&root, [allowed_root]);
    let workspace = galvofs
        .create_workspace(workspace_request("local-deny-root"))
        .await
        .expect("workspace should be created");
    let lease = galvofs
        .issue_storage_lease(lease_request(
            &workspace.workspace_id,
            GalvoFsAccessMode::ReadWrite,
        ))
        .await
        .expect("lease should issue");
    let input = galvofs
        .create_storage_input(local_path_import_request(
            denied_root.to_string_lossy().into_owned(),
        ))
        .await
        .expect("local path input admission records source metadata");

    let error = galvofs
        .attach_storage_input(AttachGalvoStorageInputRequest {
            workspace_id: workspace.workspace_id,
            input_id: input.input_id,
            lease_id: Some(lease.lease_id),
            mount_path: "/workspace/data".into(),
            materialization: GalvoFsInputMaterialization::Import,
            access: GalvoFsAccessMode::ReadOnly,
            writeback_policy: GalvoFsWritebackPolicy::None,
        })
        .await
        .expect_err("local path outside allowlist should fail closed");

    assert!(error
        .to_string()
        .contains("GalvoFS local-path input is outside configured allowlists"));

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn galvofs_promotes_uploaded_object_storage_input_into_workspace_file() {
    let root = test_store_root("storage-input-uploaded-object");
    let galvofs = GalvoFs::local(&root);
    let workspace = galvofs
        .create_workspace(workspace_request("uploaded-object-root"))
        .await
        .expect("workspace should be created");
    let lease = galvofs
        .issue_storage_lease(lease_request(
            &workspace.workspace_id,
            GalvoFsAccessMode::ReadWrite,
        ))
        .await
        .expect("lease should issue");
    let uploaded = galvofs
        .upload_storage_object(UploadGalvoStorageObjectRequest {
            bytes: b"uploaded payload".to_vec(),
            expected_digest: None,
            content_type: Some("text/plain".into()),
        })
        .await
        .expect("object upload should persist");

    let input = galvofs
        .create_storage_input(CreateGalvoStorageInputRequest {
            tenant_id: "tenant-alpha".into(),
            project_id: "project-research".into(),
            name: "uploaded-payload".into(),
            source_kind: GalvoFsStorageInputSourceKind::UploadedObject,
            source_uri: uploaded.object_uri.clone(),
            materialization: GalvoFsInputMaterialization::Import,
            access: GalvoFsAccessMode::ReadOnly,
            mount_path: "/workspace/input/payload.txt".into(),
            digest_policy: GalvoFsDigestPolicy::Required,
            retention_policy: "retain-run".into(),
            writeback_policy: GalvoFsWritebackPolicy::None,
        })
        .await
        .expect("uploaded object input should be admitted");

    let binding = galvofs
        .attach_storage_input(AttachGalvoStorageInputRequest {
            workspace_id: workspace.workspace_id.clone(),
            input_id: input.input_id.clone(),
            lease_id: Some(lease.lease_id),
            mount_path: "/workspace/input/payload.txt".into(),
            materialization: GalvoFsInputMaterialization::Import,
            access: GalvoFsAccessMode::ReadOnly,
            writeback_policy: GalvoFsWritebackPolicy::None,
        })
        .await
        .expect("uploaded object should import into workspace");

    assert_eq!(
        binding.verification_status,
        GalvoFsInputVerificationStatus::Verified
    );
    let file = galvofs
        .read_file(&workspace.workspace_id, "input/payload.txt")
        .await
        .expect("uploaded object should read from workspace path");
    assert_eq!(file.bytes, b"uploaded payload");
    assert_eq!(file.record.digest, uploaded.digest);

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn galvofs_garbage_collection_purges_only_unreferenced_objects() {
    let root = test_store_root("gc");
    let galvofs = GalvoFs::local(&root);
    let workspace = galvofs
        .create_workspace(workspace_request("gc-root"))
        .await
        .expect("workspace should be created");
    let old = galvofs
        .write_file(&workspace.workspace_id, "current.txt", b"old")
        .await
        .expect("old object should be written");
    let current = galvofs
        .write_file(&workspace.workspace_id, "current.txt", b"current")
        .await
        .expect("current object should be written");

    let dry_run = galvofs
        .collect_garbage(GalvoFsGarbageCollectionRequest { dry_run: true })
        .await
        .expect("dry-run GC should report candidates");
    assert_eq!(dry_run.candidate_objects.len(), 1);
    assert_eq!(dry_run.candidate_objects[0].digest, old.digest);
    assert!(dry_run.removed_objects.is_empty());
    assert!(local_object_path(&root, &old.digest).is_file());
    assert!(local_object_path(&root, &current.digest).is_file());

    let purge = galvofs
        .collect_garbage(GalvoFsGarbageCollectionRequest { dry_run: false })
        .await
        .expect("purge GC should remove candidates");
    assert_eq!(purge.candidate_objects.len(), 1);
    assert_eq!(purge.removed_objects.len(), 1);
    assert_eq!(purge.removed_objects[0].digest, old.digest);
    assert!(!local_object_path(&root, &old.digest).exists());
    assert!(local_object_path(&root, &current.digest).is_file());

    let report = galvofs
        .verify_integrity()
        .await
        .expect("post-GC store should verify");
    assert_eq!(report.status, GalvoFsIntegrityStatus::Healthy);
    assert_eq!(report.checked_objects, 1);

    let _ = std::fs::remove_dir_all(root);
}

fn local_workspace_state_path(root: &std::path::Path, workspace_id: &str) -> std::path::PathBuf {
    root.join("metadata")
        .join("workspaces")
        .join(format!("{workspace_id}.json"))
}

/// A metadata document must be REPLACED, never rewritten in place.
///
/// Rewriting in place means truncate-then-write, and a crash between those two
/// leaves torn JSON — which for a workspace document loses the entire file
/// index rather than one file. Replacement by rename is what makes a reader
/// see either the whole old document or the whole new one.
///
/// Inode identity is the observable consequence: an in-place rewrite keeps the
/// same inode, a rename installs a different one. The fsync that makes the
/// replacement durable is not portably observable and is not asserted here.
#[cfg(unix)]
#[tokio::test]
async fn galvofs_metadata_documents_are_replaced_not_rewritten_in_place() {
    use std::os::unix::fs::MetadataExt;

    let root = test_store_root("atomic-replace");
    let galvofs = GalvoFs::local(&root);
    let workspace = galvofs
        .create_workspace(workspace_request("atomic-replace-root"))
        .await
        .expect("workspace should be created");
    let state_path = local_workspace_state_path(&root, &workspace.workspace_id);

    galvofs
        .write_file(&workspace.workspace_id, "first.txt", b"first")
        .await
        .expect("first write should land");
    let before = std::fs::metadata(&state_path)
        .expect("workspace document should exist")
        .ino();

    galvofs
        .write_file(&workspace.workspace_id, "second.txt", b"second")
        .await
        .expect("second write should land");
    let after = std::fs::metadata(&state_path)
        .expect("workspace document should still exist")
        .ino();

    assert_ne!(
        before, after,
        "workspace document was rewritten in place; a crash mid-write would tear it"
    );

    let leftovers: Vec<_> = std::fs::read_dir(root.join("metadata").join("workspaces"))
        .expect("workspaces directory should be readable")
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|path| path.extension().and_then(|ext| ext.to_str()) != Some("json"))
        .collect();
    assert!(
        leftovers.is_empty(),
        "temporary files survived a successful write: {leftovers:?}"
    );

    let _ = std::fs::remove_dir_all(root);
}

/// Two concurrent writes to one workspace must both survive.
///
/// Every mutation is read-whole-state, modify, write-whole-state. Without a
/// lock both callers read the same document, each inserts its own record, and
/// the second write discards the first. The body object survives because
/// bodies are content-addressed and written separately — it is simply
/// unreferenced, so the index says the file was never written while garbage
/// collection quietly reclaims it.
///
/// `join!` on the test runtime interleaves the two tasks at their await
/// points, so this reproduces deterministically rather than by scheduling luck.
#[tokio::test]
async fn galvofs_concurrent_writes_do_not_lose_records() {
    let root = test_store_root("lost-update");
    let galvofs = GalvoFs::local(&root);
    let workspace = galvofs
        .create_workspace(workspace_request("lost-update-root"))
        .await
        .expect("workspace should be created");

    let (left, right) = tokio::join!(
        galvofs.write_file(&workspace.workspace_id, "left.txt", b"left"),
        galvofs.write_file(&workspace.workspace_id, "right.txt", b"right"),
    );
    left.expect("left write should land");
    right.expect("right write should land");

    let files = galvofs
        .list_files(&workspace.workspace_id)
        .await
        .expect("workspace files should list");
    let paths: Vec<&str> = files.iter().map(|file| file.path.as_str()).collect();

    assert!(
        paths.contains(&"left.txt") && paths.contains(&"right.txt"),
        "a concurrent write was silently discarded; index holds {paths:?}"
    );

    let _ = std::fs::remove_dir_all(root);
}

/// A commit lands every file or none of them.
///
/// The refusal here is a path that escapes the workspace namespace, which is
/// reachable through the public API and so lets the all-or-nothing property be
/// asserted directly rather than inferred. `write_file` in a loop would have
/// written the two valid files before reaching the bad one.
#[tokio::test]
async fn galvofs_refused_commit_writes_none_of_its_files() {
    let root = test_store_root("commit-refused");
    let galvofs = GalvoFs::local(&root);
    let workspace = galvofs
        .create_workspace(workspace_request("commit-refused-root"))
        .await
        .expect("workspace should be created");

    let error = galvofs
        .commit_files(
            &workspace.workspace_id,
            &[
                ("keep-one.txt", b"one".as_slice()),
                ("keep-two.txt", b"two".as_slice()),
                ("../escape.txt", b"escape".as_slice()),
            ],
        )
        .await
        .expect_err("a commit naming an escaping path should be refused");
    assert!(
        format!("{error}").contains("escape") || format!("{error}").contains("path"),
        "refusal should name the offending path, got: {error}"
    );

    let files = galvofs
        .list_files(&workspace.workspace_id)
        .await
        .expect("workspace should list");
    assert!(
        files.is_empty(),
        "a refused commit left files behind: {:?}",
        files.iter().map(|file| &file.path).collect::<Vec<_>>()
    );

    let _ = std::fs::remove_dir_all(root);
}

/// Every file in an accepted commit is present afterwards.
#[tokio::test]
async fn galvofs_commit_installs_every_file_together() {
    let root = test_store_root("commit-all");
    let galvofs = GalvoFs::local(&root);
    let workspace = galvofs
        .create_workspace(workspace_request("commit-all-root"))
        .await
        .expect("workspace should be created");

    let records = galvofs
        .commit_files(
            &workspace.workspace_id,
            &[
                ("src/alpha.rs", b"alpha".as_slice()),
                ("src/beta.rs", b"beta".as_slice()),
                ("README.md", b"readme".as_slice()),
            ],
        )
        .await
        .expect("commit should land");
    assert_eq!(records.len(), 3);

    let files = galvofs
        .list_files(&workspace.workspace_id)
        .await
        .expect("workspace should list");
    let mut paths: Vec<&str> = files.iter().map(|file| file.path.as_str()).collect();
    paths.sort_unstable();
    assert_eq!(paths, vec!["README.md", "src/alpha.rs", "src/beta.rs"]);

    let beta = galvofs
        .read_file(&workspace.workspace_id, "src/beta.rs")
        .await
        .expect("committed file should read back");
    assert_eq!(beta.bytes, b"beta");

    let _ = std::fs::remove_dir_all(root);
}

/// Naming one path twice is a caller mistake, not something to resolve.
///
/// Keeping the last write silently would report three files committed when two
/// landed.
#[tokio::test]
async fn galvofs_commit_refuses_duplicate_paths() {
    let root = test_store_root("commit-duplicate");
    let galvofs = GalvoFs::local(&root);
    let workspace = galvofs
        .create_workspace(workspace_request("commit-duplicate-root"))
        .await
        .expect("workspace should be created");

    let error = galvofs
        .commit_files(
            &workspace.workspace_id,
            &[
                ("same.txt", b"first".as_slice()),
                ("other.txt", b"other".as_slice()),
                ("same.txt", b"second".as_slice()),
            ],
        )
        .await
        .expect_err("a commit naming one path twice should be refused");
    assert!(
        format!("{error}").contains("same.txt"),
        "refusal should name the duplicated path, got: {error}"
    );

    let files = galvofs
        .list_files(&workspace.workspace_id)
        .await
        .expect("workspace should list");
    assert!(files.is_empty(), "a refused commit left files behind");

    let _ = std::fs::remove_dir_all(root);
}

/// Set up a workspace with a local-path directory input already materialized.
///
/// Returns the store, the workspace id, the binding, and the source directory.
async fn writeback_fixture(
    label: &str,
    writeback_policy: GalvoFsWritebackPolicy,
    access: GalvoFsAccessMode,
) -> (
    GalvoFs,
    std::path::PathBuf,
    String,
    galvofs_core::GalvoFsStorageInputBinding,
    std::path::PathBuf,
) {
    let root = test_store_root(label);
    let source_root = root.join("allowed-inputs");
    let dataset = source_root.join("dataset");
    std::fs::create_dir_all(dataset.join("nested")).expect("source dirs should create");
    std::fs::write(dataset.join("train.txt"), b"training rows").expect("source file should write");
    std::fs::write(dataset.join("nested").join("labels.txt"), b"labels")
        .expect("nested source file should write");

    let galvofs = GalvoFs::local_with_storage_input_roots(&root, [source_root.clone()]);
    let workspace = galvofs
        .create_workspace(workspace_request("writeback-root"))
        .await
        .expect("workspace should be created");
    let input = galvofs
        .create_storage_input(local_path_import_request(format!(
            "file://{}",
            dataset.display()
        )))
        .await
        .expect("local path input should be admitted");
    let binding = galvofs
        .attach_storage_input(AttachGalvoStorageInputRequest {
            workspace_id: workspace.workspace_id.clone(),
            input_id: input.input_id.clone(),
            lease_id: None,
            mount_path: "/workspace/data".into(),
            materialization: GalvoFsInputMaterialization::Import,
            access,
            writeback_policy,
        })
        .await
        .expect("input should materialize");

    (galvofs, root, workspace.workspace_id, binding, dataset)
}

fn read_source_tree(dataset: &std::path::Path) -> Vec<(String, Vec<u8>)> {
    let mut found = Vec::new();
    for entry in walk_files(dataset) {
        let relative = entry
            .strip_prefix(dataset)
            .expect("entry should sit under dataset")
            .to_string_lossy()
            .into_owned();
        found.push((
            relative,
            std::fs::read(&entry).expect("source file readable"),
        ));
    }
    found.sort();
    found
}

fn walk_files(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut files = Vec::new();
    let Ok(entries) = std::fs::read_dir(dir) else {
        return files;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            files.extend(walk_files(&path));
        } else {
            files.push(path);
        }
    }
    files
}

/// Materialization pins a baseline, which is what makes drift judgeable at all.
#[tokio::test]
async fn galvofs_materialization_records_a_writeback_baseline() {
    let (galvofs, root, workspace_id, binding, _dataset) = writeback_fixture(
        "writeback-baseline",
        GalvoFsWritebackPolicy::External,
        GalvoFsAccessMode::ReadWrite,
    )
    .await;

    let baseline_id = binding
        .baseline_snapshot_id
        .clone()
        .expect("materialization should pin a baseline snapshot");

    // The baseline must contain the materialized files — a snapshot taken
    // before materialization would be empty and silently useless.
    let diff = galvofs
        .diff_workspace(&workspace_id, &baseline_id)
        .await
        .expect("diff against the baseline should work");
    assert!(
        diff.changes.is_empty(),
        "a freshly materialized workspace should not differ from its own baseline: {:?}",
        diff.changes
    );

    let _ = std::fs::remove_dir_all(root);
}

/// Every refusal leaves the source byte-for-byte as it was.
#[tokio::test]
async fn galvofs_writeback_refusals_never_touch_the_source() {
    // policy: none
    let (galvofs, root, workspace_id, binding, dataset) = writeback_fixture(
        "writeback-refuse-none",
        GalvoFsWritebackPolicy::None,
        GalvoFsAccessMode::ReadWrite,
    )
    .await;
    let before = read_source_tree(&dataset);
    galvofs
        .write_file(&workspace_id, "data/train.txt", b"changed here")
        .await
        .expect("workspace edit should land");
    let error = galvofs
        .writeback(&workspace_id, &binding.binding_id, None)
        .await
        .expect_err("writebackPolicy none should refuse");
    assert!(
        format!("{error}").contains("none"),
        "refusal should name the policy, got: {error}"
    );
    assert_eq!(before, read_source_tree(&dataset), "source was modified");
    let _ = std::fs::remove_dir_all(root);

    // read-only binding
    let (galvofs, root, workspace_id, binding, dataset) = writeback_fixture(
        "writeback-refuse-readonly",
        GalvoFsWritebackPolicy::External,
        GalvoFsAccessMode::ReadOnly,
    )
    .await;
    let before = read_source_tree(&dataset);
    galvofs
        .write_file(&workspace_id, "data/train.txt", b"changed here")
        .await
        .expect("workspace edit should land");
    let error = galvofs
        .writeback(&workspace_id, &binding.binding_id, None)
        .await
        .expect_err("a read-only binding should refuse");
    assert!(
        format!("{error}").contains("read-only"),
        "refusal should say read-only, got: {error}"
    );
    assert_eq!(before, read_source_tree(&dataset), "source was modified");
    let _ = std::fs::remove_dir_all(root);

    // unknown binding
    let (galvofs, root, workspace_id, _binding, dataset) = writeback_fixture(
        "writeback-refuse-unknown",
        GalvoFsWritebackPolicy::External,
        GalvoFsAccessMode::ReadWrite,
    )
    .await;
    let before = read_source_tree(&dataset);
    let error = galvofs
        .writeback(&workspace_id, "storage-input-binding-nope", None)
        .await
        .expect_err("an unknown binding should refuse");
    assert!(
        format!("{error}").contains("storage-input-binding-nope"),
        "refusal should name the binding, got: {error}"
    );
    assert_eq!(before, read_source_tree(&dataset), "source was modified");
    let _ = std::fs::remove_dir_all(root);
}

/// A source that moved since materialization aborts the WHOLE writeback.
///
/// Two files are changed in the workspace and only one drifts at the source.
/// The clean one must not be written either — pushing it and reporting the
/// other would be a partial application against storage this system does not
/// own.
#[tokio::test]
async fn galvofs_writeback_refuses_everything_when_the_source_drifted() {
    let (galvofs, root, workspace_id, binding, dataset) = writeback_fixture(
        "writeback-drift",
        GalvoFsWritebackPolicy::External,
        GalvoFsAccessMode::ReadWrite,
    )
    .await;

    galvofs
        .write_file(&workspace_id, "data/train.txt", b"workspace rows")
        .await
        .expect("workspace edit should land");
    galvofs
        .write_file(&workspace_id, "data/nested/labels.txt", b"workspace labels")
        .await
        .expect("second workspace edit should land");

    // Somebody else edits one of them at the source.
    std::fs::write(dataset.join("train.txt"), b"someone else got here first")
        .expect("source edit should write");
    let before = read_source_tree(&dataset);

    let error = galvofs
        .writeback(&workspace_id, &binding.binding_id, None)
        .await
        .expect_err("drift should refuse the writeback");
    let message = format!("{error}");
    assert!(
        message.contains("data/train.txt"),
        "refusal should name the drifted path, got: {message}"
    );
    assert!(
        message.contains("Nothing was written"),
        "refusal should say nothing was written, got: {message}"
    );
    assert_eq!(
        before,
        read_source_tree(&dataset),
        "a drifted writeback wrote to the source anyway"
    );

    let _ = std::fs::remove_dir_all(root);
}

/// The happy path: changed files reach the source, and the report names them.
#[tokio::test]
async fn galvofs_external_writeback_pushes_changed_files_and_reports_them() {
    let (galvofs, root, workspace_id, binding, dataset) = writeback_fixture(
        "writeback-external",
        GalvoFsWritebackPolicy::External,
        GalvoFsAccessMode::ReadWrite,
    )
    .await;

    galvofs
        .write_file(&workspace_id, "data/train.txt", b"workspace rows")
        .await
        .expect("workspace edit should land");
    galvofs
        .write_file(&workspace_id, "data/fresh.txt", b"brand new")
        .await
        .expect("new workspace file should land");

    let report = galvofs
        .writeback(&workspace_id, &binding.binding_id, None)
        .await
        .expect("writeback should succeed");

    let mut written = report.written_paths.clone();
    written.sort();
    assert_eq!(written, vec!["data/fresh.txt", "data/train.txt"]);
    assert!(report.published_artifacts.is_empty());

    assert_eq!(
        std::fs::read(dataset.join("train.txt")).expect("pushed file readable"),
        b"workspace rows"
    );
    assert_eq!(
        std::fs::read(dataset.join("fresh.txt")).expect("new file readable"),
        b"brand new"
    );
    // Untouched files stay untouched.
    assert_eq!(
        std::fs::read(dataset.join("nested").join("labels.txt")).expect("readable"),
        b"labels"
    );
    // No writeback temporaries survive a success. Match on the file NAME, not
    // the whole path — the test store root is itself named
    // "galvofs-writeback-external-...", so a substring check against the path
    // matches every file in the tree and asserts nothing.
    let survivors: Vec<_> = walk_files(&dataset)
        .into_iter()
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.contains("galvofs-writeback"))
        })
        .collect();
    assert!(
        survivors.is_empty(),
        "writeback temporaries survived: {survivors:?}"
    );

    let _ = std::fs::remove_dir_all(root);
}

/// PinArtifacts publishes internally and never touches the source.
#[tokio::test]
async fn galvofs_pin_artifacts_writeback_publishes_without_touching_the_source() {
    let (galvofs, root, workspace_id, binding, dataset) = writeback_fixture(
        "writeback-pin",
        GalvoFsWritebackPolicy::PinArtifacts,
        GalvoFsAccessMode::ReadWrite,
    )
    .await;

    let before = read_source_tree(&dataset);
    galvofs
        .write_file(&workspace_id, "data/train.txt", b"workspace rows")
        .await
        .expect("workspace edit should land");

    let report = galvofs
        .writeback(&workspace_id, &binding.binding_id, None)
        .await
        .expect("pin-artifacts writeback should succeed");

    assert!(
        report.written_paths.is_empty(),
        "PinArtifacts wrote outward"
    );
    assert_eq!(report.published_artifacts.len(), 1);
    assert_eq!(report.published_artifacts[0].path, "data/train.txt");
    assert_eq!(
        before,
        read_source_tree(&dataset),
        "PinArtifacts modified the external source"
    );

    let _ = std::fs::remove_dir_all(root);
}

/// Three spellings of one directory become one stored source URI.
///
/// Before normalization these were three inputs whose equality nothing
/// downstream could see, which is why duplicate detection had nothing to
/// compare and why one source could appear in the receipt trail under three
/// different strings.
#[tokio::test]
async fn galvofs_storage_input_source_uris_are_stored_canonically() {
    let root = test_store_root("source-uri-normalize");
    let source_root = root.join("allowed-inputs");
    let dataset = source_root.join("dataset");
    std::fs::create_dir_all(&dataset).expect("source dirs should create");

    let galvofs = GalvoFs::local_with_storage_input_roots(&root, [source_root.clone()]);
    let canonical = format!("file://{}", dataset.display());

    for spelling in [
        format!("file://{}", dataset.display()),
        format!("file://{}/", dataset.display()),
        format!("{}", dataset.display()),
        format!("{}/", dataset.display()),
        format!("file://{}//", dataset.display()),
    ] {
        let input = galvofs
            .create_storage_input(local_path_import_request(spelling.clone()))
            .await
            .expect("input should be admitted");
        assert_eq!(
            input.source_uri, canonical,
            "sourceUri {spelling} was not stored canonically"
        );
    }

    let _ = std::fs::remove_dir_all(root);
}

/// A trailing slash on an S3 prefix survives normalization.
///
/// `s3://bucket/training` also matches `training-old/x`; `s3://bucket/training/`
/// does not. Trimming it would silently widen what the input covers, so only
/// interior duplicate separators are collapsed. Local paths are the opposite
/// case and are trimmed, because a directory is the same with or without it.
#[tokio::test]
async fn galvofs_s3_prefix_normalization_preserves_a_meaningful_trailing_slash() {
    let root = test_store_root("s3-prefix-normalize");
    let galvofs = GalvoFs::local(&root);

    let mut request = s3_storage_input_request();
    request.source_uri = "s3://research-data//training//".into();
    let folder_prefix = galvofs
        .create_storage_input(request)
        .await
        .expect("s3 input should be admitted");
    assert_eq!(folder_prefix.source_uri, "s3://research-data/training/");

    let mut request = s3_storage_input_request();
    request.source_uri = "s3://research-data/training".into();
    let bare_prefix = galvofs
        .create_storage_input(request)
        .await
        .expect("s3 input should be admitted");
    assert_eq!(bare_prefix.source_uri, "s3://research-data/training");

    assert_ne!(
        folder_prefix.source_uri, bare_prefix.source_uri,
        "two prefixes matching different object sets were collapsed into one"
    );

    let _ = std::fs::remove_dir_all(root);
}

/// A retried create resolves to the input the first attempt made.
///
/// The create route is a plain POST with no idempotency key. A client that
/// posted, timed out, and retried used to get a second input and no way to tell
/// — nothing listed them. Identity is derived from the request content, so the
/// retry lands on the original.
#[tokio::test]
async fn galvofs_repeating_a_storage_input_create_returns_the_original() {
    let root = test_store_root("storage-input-idempotent");
    let galvofs = GalvoFs::local(&root);

    let first = galvofs
        .create_storage_input(s3_storage_input_request())
        .await
        .expect("first create should be admitted");
    let retry = galvofs
        .create_storage_input(s3_storage_input_request())
        .await
        .expect("a retry should be admitted");

    assert_eq!(
        first.input_id, retry.input_id,
        "a retried create minted a second input"
    );
    assert_eq!(
        first.created_at, retry.created_at,
        "the retry returned a rebuilt input rather than the stored one"
    );

    let listed = galvofs
        .storage_inputs("tenant-alpha", "project-research")
        .await
        .expect("inputs should list");
    assert_eq!(listed.len(), 1, "the store holds a duplicate: {listed:?}");

    let _ = std::fs::remove_dir_all(root);
}

/// Two spellings of one source are one input, because identity is computed over
/// the NORMALIZED uri rather than the string the caller happened to type.
#[tokio::test]
async fn galvofs_storage_input_identity_follows_the_normalized_uri() {
    let root = test_store_root("storage-input-identity-uri");
    let source_root = root.join("allowed-inputs");
    let dataset = source_root.join("dataset");
    std::fs::create_dir_all(&dataset).expect("source dirs should create");
    let galvofs = GalvoFs::local_with_storage_input_roots(&root, [source_root]);

    let with_scheme = galvofs
        .create_storage_input(local_path_import_request(format!(
            "file://{}",
            dataset.display()
        )))
        .await
        .expect("first create should be admitted");
    let bare_with_slash = galvofs
        .create_storage_input(local_path_import_request(format!("{}/", dataset.display())))
        .await
        .expect("second create should be admitted");

    assert_eq!(
        with_scheme.input_id, bare_with_slash.input_id,
        "two spellings of one directory produced two inputs"
    );

    let _ = std::fs::remove_dir_all(root);
}

/// Changing the name is how a caller says "another one, deliberately".
///
/// This is the escape hatch that makes returning the existing input safe: the
/// server never has to guess whether a repeat means retry or intent, because
/// the caller can express intent.
#[tokio::test]
async fn galvofs_a_differently_named_storage_input_is_a_new_one() {
    let root = test_store_root("storage-input-rename");
    let galvofs = GalvoFs::local(&root);

    let first = galvofs
        .create_storage_input(s3_storage_input_request())
        .await
        .expect("first create should be admitted");

    let mut second_request = s3_storage_input_request();
    second_request.name = "a-second-view-of-the-same-bucket".into();
    let second = galvofs
        .create_storage_input(second_request)
        .await
        .expect("a differently named create should be admitted");

    assert_ne!(
        first.input_id, second.input_id,
        "a deliberate second input was collapsed onto the first"
    );
    let listed = galvofs
        .storage_inputs("tenant-alpha", "project-research")
        .await
        .expect("inputs should list");
    assert_eq!(listed.len(), 2);

    let _ = std::fs::remove_dir_all(root);
}

/// Concurrent first creates must not both mint an input.
///
/// Both miss the index, both proceed. Without the identity lock the store keeps
/// two inputs and one pointer, so one of them is unreachable by identity
/// forever — a leak that only shows up under load. `join!` interleaves the two
/// at their await points, so this reproduces deterministically.
#[tokio::test]
async fn galvofs_concurrent_storage_input_creates_produce_one_input() {
    let root = test_store_root("storage-input-concurrent");
    let galvofs = GalvoFs::local(&root);

    let (left, right) = tokio::join!(
        galvofs.create_storage_input(s3_storage_input_request()),
        galvofs.create_storage_input(s3_storage_input_request()),
    );
    let left = left.expect("left create should be admitted");
    let right = right.expect("right create should be admitted");

    assert_eq!(
        left.input_id, right.input_id,
        "concurrent creates minted two inputs for one identity"
    );
    let listed = galvofs
        .storage_inputs("tenant-alpha", "project-research")
        .await
        .expect("inputs should list");
    assert_eq!(
        listed.len(),
        1,
        "a concurrent create leaked an input the index cannot reach: {listed:?}"
    );

    let _ = std::fs::remove_dir_all(root);
}
