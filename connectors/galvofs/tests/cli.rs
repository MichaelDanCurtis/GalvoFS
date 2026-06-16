use std::io::Write;
use std::process::{Command, Stdio};

use serde_json::{json, Value};

#[test]
fn cli_reads_access_endpoint_response_and_emits_connector_plans() {
    let mut child = Command::new(env!("CARGO_BIN_EXE_galvofs-connector"))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("connector CLI should start");

    let input = json!({
        "runId": "run-123",
        "accessEndpoints": [{
            "kind": "nfs4",
            "server": "galvofs.local",
            "exportPath": "/exports/lease-123",
            "uri": "nfs4://galvofs.local/exports/lease-123",
            "targetPath": "/femto/galvofs",
            "mountOptions": ["vers=4.2", "hard"],
            "mountCommand": "mount -t nfs4 -o vers=4.2,hard galvofs.local:/exports/lease-123 /femto/galvofs",
            "environment": {
                "FEMTO_GALVOFS_ENDPOINT_KIND": "nfs4",
                "FEMTO_GALVOFS_NFS_URI": "nfs4://galvofs.local/exports/lease-123"
            }
        }]
    });

    child
        .stdin
        .as_mut()
        .expect("stdin should be open")
        .write_all(input.to_string().as_bytes())
        .expect("input should write");

    let output = child.wait_with_output().expect("connector CLI should exit");
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let body: Value = serde_json::from_slice(&output.stdout).expect("stdout should be JSON");
    assert_eq!(body["runId"], "run-123");
    assert_eq!(body["plans"][0]["connector"], "galvofs-nfsd");
    assert_eq!(
        body["plans"][0]["actions"][0]["kind"],
        "configureKernelNfsdExport"
    );
    assert_eq!(body["plans"][0]["actions"][1]["kind"], "mountNfs4");
}

#[test]
fn cli_accepts_a_single_access_endpoint_document() {
    let mut child = Command::new(env!("CARGO_BIN_EXE_galvofs-connector"))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("connector CLI should start");

    let input = json!({
        "kind": "nfs4",
        "server": "galvofs.local",
        "exportPath": "/exports/lease-123",
        "uri": "nfs4://galvofs.local/exports/lease-123",
        "targetPath": "/femto/galvofs",
        "mountOptions": ["vers=4.2", "hard"],
        "mountCommand": "mount -t nfs4 -o vers=4.2,hard galvofs.local:/exports/lease-123 /femto/galvofs",
        "environment": {}
    });

    child
        .stdin
        .as_mut()
        .expect("stdin should be open")
        .write_all(input.to_string().as_bytes())
        .expect("input should write");

    let output = child.wait_with_output().expect("connector CLI should exit");
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let body: Value = serde_json::from_slice(&output.stdout).expect("stdout should be JSON");
    assert_eq!(body["plans"][0]["connector"], "galvofs-nfsd");
    assert_eq!(
        body["plans"][0]["actions"][0]["clientSelector"],
        "${FEMTO_GALVOFS_NFSD_CLIENTS}"
    );
}

#[test]
fn cli_apply_nfsd_requires_explicit_clients() {
    let mut child = Command::new(env!("CARGO_BIN_EXE_galvofs-connector"))
        .arg("--apply-nfsd")
        .arg("--dry-run")
        .env_remove("FEMTO_GALVOFS_NFSD_CLIENTS")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("connector CLI should start");

    let input = json!({
        "kind": "nfs4",
        "server": "galvofs.local",
        "exportPath": "/exports/lease-123",
        "uri": "nfs4://galvofs.local/exports/lease-123",
        "targetPath": "/femto/galvofs",
        "mountOptions": ["vers=4.2", "hard"],
        "mountCommand": "mount -t nfs4 -o vers=4.2,hard galvofs.local:/exports/lease-123 /femto/galvofs",
        "environment": {}
    });

    child
        .stdin
        .as_mut()
        .expect("stdin should be open")
        .write_all(input.to_string().as_bytes())
        .expect("input should write");

    let output = child.wait_with_output().expect("connector CLI should exit");
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("FEMTO_GALVOFS_NFSD_CLIENTS"),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn cli_apply_nfsd_dry_run_emits_exportfs_report() {
    let mut child = Command::new(env!("CARGO_BIN_EXE_galvofs-connector"))
        .arg("--apply-nfsd")
        .arg("--dry-run")
        .env("FEMTO_GALVOFS_NFSD_CLIENTS", "fd00:1234::/64")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("connector CLI should start");

    let input = json!({
        "runId": "run-123",
        "accessEndpoints": [{
            "kind": "nfs4",
            "server": "galvofs.local",
            "exportPath": "/exports/lease-123",
            "uri": "nfs4://galvofs.local/exports/lease-123",
            "targetPath": "/femto/galvofs",
            "mountOptions": ["vers=4.2", "hard"],
            "mountCommand": "mount -t nfs4 -o vers=4.2,hard galvofs.local:/exports/lease-123 /femto/galvofs",
            "environment": {}
        }]
    });

    child
        .stdin
        .as_mut()
        .expect("stdin should be open")
        .write_all(input.to_string().as_bytes())
        .expect("input should write");

    let output = child.wait_with_output().expect("connector CLI should exit");
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let body: Value = serde_json::from_slice(&output.stdout).expect("stdout should be JSON");
    assert_eq!(body["runId"], "run-123");
    assert_eq!(body["reports"][0]["connector"], "galvofs-nfsd");
    assert_eq!(body["reports"][0]["applied"], false);
    assert_eq!(body["reports"][0]["commands"][0]["program"], "exportfs");
    assert_eq!(
        body["reports"][0]["commands"][0]["args"][2],
        "fd00:1234::/64:/exports/lease-123"
    );
    assert_eq!(body["reports"][0]["skippedActions"][0], "mountNfs4");
}
