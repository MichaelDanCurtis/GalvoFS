use std::collections::BTreeMap;

use galvofs_connector::{ConnectorAction, ConnectorPlan};
use galvofs_core::{
    GalvoFsAccessEndpoint, GalvoFsCsiNodePublish, GalvoFsKubernetesCsiProjection,
    GalvoFsKubernetesCsiVolumeSource, GalvoFsKubernetesVolume, GalvoFsKubernetesVolumeMount,
};

#[test]
fn csi_endpoint_becomes_kubernetes_connector_plan() {
    let endpoint = GalvoFsAccessEndpoint::Csi {
        driver: "galvofs.csi.femto.local".into(),
        volume_handle: "galvofs-lease-123".into(),
        volume_attributes: BTreeMap::from([
            ("workspaceId".into(), "ws-123".into()),
            ("leaseId".into(), "lease-123".into()),
        ]),
        node_publish: GalvoFsCsiNodePublish {
            target_path: "/workspace".into(),
            read_only: false,
            mount_manifest_id: "mount-123".into(),
            mount_manifest_digest: "sha256:abc".into(),
        },
        kubernetes: GalvoFsKubernetesCsiProjection {
            volume: GalvoFsKubernetesVolume {
                name: "galvofs-workspace".into(),
                csi: GalvoFsKubernetesCsiVolumeSource {
                    driver: "galvofs.csi.femto.local".into(),
                    volume_handle: "galvofs-lease-123".into(),
                    volume_attributes: BTreeMap::from([
                        ("workspaceId".into(), "ws-123".into()),
                        ("leaseId".into(), "lease-123".into()),
                    ]),
                    read_only: false,
                },
            },
            volume_mount: GalvoFsKubernetesVolumeMount {
                name: "galvofs-workspace".into(),
                mount_path: "/workspace".into(),
                read_only: false,
            },
        },
    };

    let plan = ConnectorPlan::from_access_endpoint(endpoint).expect("CSI endpoint should plan");

    assert_eq!(plan.connector, "galvofs-csi");
    assert_eq!(
        plan.actions,
        vec![ConnectorAction::ApplyKubernetesInlineCsiVolume {
            volume_name: "galvofs-workspace".into(),
            driver: "galvofs.csi.femto.local".into(),
            volume_handle: "galvofs-lease-123".into(),
            mount_path: "/workspace".into(),
            read_only: false,
        }]
    );
}

#[test]
fn nfs_endpoint_becomes_kernel_nfsd_connector_plan() {
    let endpoint = GalvoFsAccessEndpoint::Nfs4 {
        server: "galvofs.local".into(),
        export_path: "/exports/lease-123".into(),
        uri: "nfs4://galvofs.local/exports/lease-123".into(),
        target_path: "/femto/galvofs".into(),
        mount_options: vec!["vers=4.2".into(), "hard".into()],
        mount_command:
            "mount -t nfs4 -o vers=4.2,hard galvofs.local:/exports/lease-123 /femto/galvofs".into(),
        environment: BTreeMap::from([
            ("FEMTO_GALVOFS_ENDPOINT_KIND".into(), "nfs4".into()),
            (
                "FEMTO_GALVOFS_NFS_URI".into(),
                "nfs4://galvofs.local/exports/lease-123".into(),
            ),
        ]),
    };

    let plan = ConnectorPlan::from_access_endpoint(endpoint).expect("NFS endpoint should plan");

    assert_eq!(plan.connector, "galvofs-nfsd");
    assert_eq!(
        plan.actions,
        vec![
            ConnectorAction::ConfigureKernelNfsdExport {
                export_path: "/exports/lease-123".into(),
                client_selector: "${FEMTO_GALVOFS_NFSD_CLIENTS}".into(),
                export_options: vec![
                    "rw".into(),
                    "sync".into(),
                    "no_subtree_check".into(),
                    "root_squash".into(),
                ],
                exportfs_command: "exportfs -o rw,sync,no_subtree_check,root_squash ${FEMTO_GALVOFS_NFSD_CLIENTS}:/exports/lease-123".into(),
                unexport_command: "exportfs -u ${FEMTO_GALVOFS_NFSD_CLIENTS}:/exports/lease-123".into(),
            },
            ConnectorAction::MountNfs4 {
                server: "galvofs.local".into(),
                export_path: "/exports/lease-123".into(),
                target_path: "/femto/galvofs".into(),
                mount_command:
                    "mount -t nfs4 -o vers=4.2,hard galvofs.local:/exports/lease-123 /femto/galvofs"
                        .into(),
            },
        ]
    );
}
