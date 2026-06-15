//! Connector-side helpers for consuming GalvoFS access endpoint manifests.

use galvofs_core::GalvoFsAccessEndpoint;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AccessEndpointsResponse {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    #[serde(default)]
    pub access_endpoints: Vec<GalvoFsAccessEndpoint>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConnectorPlansResponse {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    pub plans: Vec<ConnectorPlan>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConnectorPlan {
    pub connector: String,
    pub actions: Vec<ConnectorAction>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum ConnectorAction {
    ApplyKubernetesInlineCsiVolume {
        volume_name: String,
        driver: String,
        volume_handle: String,
        mount_path: String,
        read_only: bool,
    },
    ConfigureKernelNfsdExport {
        export_path: String,
        client_selector: String,
        export_options: Vec<String>,
        exportfs_command: String,
        unexport_command: String,
    },
    MountNfs4 {
        server: String,
        export_path: String,
        target_path: String,
        mount_command: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnectorError {
    EmptyConnectorPlan,
}

impl std::fmt::Display for ConnectorError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyConnectorPlan => formatter.write_str("GalvoFS endpoint produced no actions"),
        }
    }
}

impl std::error::Error for ConnectorError {}

impl ConnectorPlan {
    pub fn from_access_endpoint(endpoint: GalvoFsAccessEndpoint) -> Result<Self, ConnectorError> {
        match endpoint {
            GalvoFsAccessEndpoint::Csi {
                driver,
                volume_handle,
                kubernetes,
                ..
            } => Ok(Self {
                connector: "galvofs-csi".into(),
                actions: vec![ConnectorAction::ApplyKubernetesInlineCsiVolume {
                    volume_name: kubernetes.volume.name,
                    driver,
                    volume_handle,
                    mount_path: kubernetes.volume_mount.mount_path,
                    read_only: kubernetes.volume_mount.read_only,
                }],
            }),
            GalvoFsAccessEndpoint::Nfs4 {
                server,
                export_path,
                target_path,
                mount_command,
                ..
            } => Ok(Self {
                connector: "galvofs-nfsd".into(),
                actions: vec![
                    ConnectorAction::ConfigureKernelNfsdExport {
                        export_path: export_path.clone(),
                        client_selector: "${FEMTO_GALVOFS_NFSD_CLIENTS}".into(),
                        export_options: kernel_nfsd_export_options(),
                        exportfs_command: format!(
                            "exportfs -o {} ${{FEMTO_GALVOFS_NFSD_CLIENTS}}:{export_path}",
                            kernel_nfsd_export_options().join(",")
                        ),
                        unexport_command: format!(
                            "exportfs -u ${{FEMTO_GALVOFS_NFSD_CLIENTS}}:{export_path}"
                        ),
                    },
                    ConnectorAction::MountNfs4 {
                        server,
                        export_path,
                        target_path,
                        mount_command,
                    },
                ],
            }),
        }
        .and_then(|plan| {
            if plan.actions.is_empty() {
                Err(ConnectorError::EmptyConnectorPlan)
            } else {
                Ok(plan)
            }
        })
    }
}

fn kernel_nfsd_export_options() -> Vec<String> {
    vec![
        "rw".into(),
        "sync".into(),
        "no_subtree_check".into(),
        "root_squash".into(),
    ]
}

impl ConnectorPlansResponse {
    pub fn from_access_endpoints(
        response: AccessEndpointsResponse,
    ) -> Result<Self, ConnectorError> {
        let plans = response
            .access_endpoints
            .into_iter()
            .map(ConnectorPlan::from_access_endpoint)
            .collect::<Result<Vec<_>, _>>()?;
        if plans.is_empty() {
            return Err(ConnectorError::EmptyConnectorPlan);
        }

        Ok(Self {
            run_id: response.run_id,
            plans,
        })
    }
}
