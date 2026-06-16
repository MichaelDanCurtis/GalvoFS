//! Connector-side helpers for consuming GalvoFS access endpoint manifests.

use galvofs_core::GalvoFsAccessEndpoint;
use serde::{Deserialize, Serialize};
use std::process::Command;

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
#[serde(rename_all = "camelCase")]
pub struct NfsdApplyReportsResponse {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    pub reports: Vec<NfsdApplyReport>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NfsdApplyOptions {
    pub client_selector: String,
    pub exportfs_program: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NfsdApplyReport {
    pub connector: String,
    pub applied: bool,
    pub commands: Vec<NfsdCommandReport>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub skipped_actions: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NfsdCommandReport {
    pub program: String,
    pub args: Vec<String>,
    pub display: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<i32>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub stdout: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub stderr: String,
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
    MissingNfsdApplyPlan,
    UnsafeNfsdClientSelector(String),
    UnsafeNfsdExportPath(String),
    UnsafeNfsdProgram(String),
    NfsdApplyCommandFailed { display: String, reason: String },
}

impl std::fmt::Display for ConnectorError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyConnectorPlan => formatter.write_str("GalvoFS endpoint produced no actions"),
            Self::MissingNfsdApplyPlan => {
                formatter.write_str("GalvoFS endpoint produced no NFSD apply actions")
            }
            Self::UnsafeNfsdClientSelector(client_selector) => write!(
                formatter,
                "unsafe or missing FEMTO_GALVOFS_NFSD_CLIENTS value: {client_selector:?}"
            ),
            Self::UnsafeNfsdExportPath(export_path) => write!(
                formatter,
                "unsafe GalvoFS NFS export path for kernel nfsd apply: {export_path:?}"
            ),
            Self::UnsafeNfsdProgram(program) => write!(
                formatter,
                "unsafe exportfs program for kernel nfsd apply: {program:?}"
            ),
            Self::NfsdApplyCommandFailed { display, reason } => {
                write!(formatter, "NFSD apply command failed: {display}: {reason}")
            }
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

impl NfsdApplyOptions {
    pub fn new(client_selector: impl Into<String>) -> Result<Self, ConnectorError> {
        Self::with_exportfs_program(client_selector, "exportfs")
    }

    pub fn with_exportfs_program(
        client_selector: impl Into<String>,
        exportfs_program: impl Into<String>,
    ) -> Result<Self, ConnectorError> {
        let client_selector = client_selector.into().trim().to_string();
        if !is_safe_nfsd_client_selector(&client_selector) {
            return Err(ConnectorError::UnsafeNfsdClientSelector(client_selector));
        }

        let exportfs_program = exportfs_program.into().trim().to_string();
        if exportfs_program.is_empty() || has_control_chars(&exportfs_program) {
            return Err(ConnectorError::UnsafeNfsdProgram(exportfs_program));
        }

        Ok(Self {
            client_selector,
            exportfs_program,
        })
    }
}

impl NfsdApplyReport {
    pub fn dry_run_from_plan(
        plan: &ConnectorPlan,
        options: &NfsdApplyOptions,
    ) -> Result<Self, ConnectorError> {
        Self::from_plan(plan, options, true)
    }

    pub fn apply_from_plan(
        plan: &ConnectorPlan,
        options: &NfsdApplyOptions,
    ) -> Result<Self, ConnectorError> {
        Self::from_plan(plan, options, false)
    }

    fn from_plan(
        plan: &ConnectorPlan,
        options: &NfsdApplyOptions,
        dry_run: bool,
    ) -> Result<Self, ConnectorError> {
        if plan.connector != "galvofs-nfsd" {
            return Err(ConnectorError::MissingNfsdApplyPlan);
        }

        let mut commands = Vec::new();
        let mut skipped_actions = Vec::new();
        for action in &plan.actions {
            match action {
                ConnectorAction::ConfigureKernelNfsdExport {
                    export_path,
                    export_options,
                    ..
                } => {
                    if !is_safe_absolute_path(export_path) {
                        return Err(ConnectorError::UnsafeNfsdExportPath(export_path.clone()));
                    }
                    let mut report = NfsdCommandReport::export(
                        options.exportfs_program.clone(),
                        export_options.clone(),
                        options.client_selector.clone(),
                        export_path.clone(),
                    );
                    if !dry_run {
                        report.execute()?;
                    }
                    commands.push(report);
                }
                other => skipped_actions.push(other.kind_name().to_string()),
            }
        }

        if commands.is_empty() {
            return Err(ConnectorError::MissingNfsdApplyPlan);
        }

        Ok(Self {
            connector: plan.connector.clone(),
            applied: !dry_run,
            commands,
            skipped_actions,
        })
    }
}

impl NfsdCommandReport {
    fn export(
        program: String,
        export_options: Vec<String>,
        client_selector: String,
        export_path: String,
    ) -> Self {
        let args = vec![
            "-o".to_string(),
            export_options.join(","),
            format!("{client_selector}:{export_path}"),
        ];
        let display = command_display(&program, &args);
        Self {
            program,
            args,
            display,
            status: None,
            stdout: String::new(),
            stderr: String::new(),
        }
    }

    fn execute(&mut self) -> Result<(), ConnectorError> {
        let output = Command::new(&self.program)
            .args(&self.args)
            .output()
            .map_err(|error| ConnectorError::NfsdApplyCommandFailed {
                display: self.display.clone(),
                reason: error.to_string(),
            })?;
        self.status = output.status.code();
        self.stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
        self.stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        if output.status.success() {
            Ok(())
        } else {
            Err(ConnectorError::NfsdApplyCommandFailed {
                display: self.display.clone(),
                reason: self.stderr.clone(),
            })
        }
    }
}

impl NfsdApplyReportsResponse {
    pub fn from_connector_plans(
        run_id: Option<String>,
        plans: &[ConnectorPlan],
        options: &NfsdApplyOptions,
        dry_run: bool,
    ) -> Result<Self, ConnectorError> {
        let mut reports = Vec::new();
        for plan in plans {
            if plan.connector == "galvofs-nfsd" {
                let report = if dry_run {
                    NfsdApplyReport::dry_run_from_plan(plan, options)?
                } else {
                    NfsdApplyReport::apply_from_plan(plan, options)?
                };
                reports.push(report);
            }
        }

        if reports.is_empty() {
            return Err(ConnectorError::MissingNfsdApplyPlan);
        }

        Ok(Self { run_id, reports })
    }
}

impl ConnectorAction {
    fn kind_name(&self) -> &'static str {
        match self {
            Self::ApplyKubernetesInlineCsiVolume { .. } => "applyKubernetesInlineCsiVolume",
            Self::ConfigureKernelNfsdExport { .. } => "configureKernelNfsdExport",
            Self::MountNfs4 { .. } => "mountNfs4",
        }
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

fn is_safe_nfsd_client_selector(client_selector: &str) -> bool {
    !client_selector.is_empty()
        && client_selector != "*"
        && !client_selector.starts_with("${")
        && !client_selector.chars().any(char::is_whitespace)
        && !has_control_chars(client_selector)
}

fn is_safe_absolute_path(path: &str) -> bool {
    path.starts_with('/')
        && !path.is_empty()
        && !path.chars().any(char::is_whitespace)
        && !has_control_chars(path)
}

fn has_control_chars(value: &str) -> bool {
    value.chars().any(char::is_control)
}

fn command_display(program: &str, args: &[String]) -> String {
    std::iter::once(program.to_string())
        .chain(args.iter().cloned())
        .collect::<Vec<_>>()
        .join(" ")
}
