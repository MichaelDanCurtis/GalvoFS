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
