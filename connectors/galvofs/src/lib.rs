//! Connector-side helpers for consuming GalvoFS access endpoint manifests.

use galvofs_core::GalvoFsAccessEndpoint;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
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
    pub operation: String,
    pub applied: bool,
    pub commands: Vec<NfsdCommandReport>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub observations: Vec<NfsdExportObservation>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub skipped_actions: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NfsdExportObservation {
    pub status: String,
    pub export_path: String,
    pub client_selector: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub export_options: Vec<String>,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NfsdExportTable {
    exports: Vec<NfsdExportEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct NfsdExportEntry {
    export_path: String,
    client_selector: String,
    options: Vec<String>,
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
    InvalidNfsdExportState(String),
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
            Self::InvalidNfsdExportState(reason) => {
                write!(formatter, "invalid exportfs -v state: {reason}")
            }
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
        Self::apply_from_plan_inner(plan, options, true)
    }

    pub fn apply_from_plan(
        plan: &ConnectorPlan,
        options: &NfsdApplyOptions,
    ) -> Result<Self, ConnectorError> {
        Self::apply_from_plan_inner(plan, options, false)
    }

    pub fn reconcile_dry_run_from_plan(
        plan: &ConnectorPlan,
        options: &NfsdApplyOptions,
        existing: &NfsdExportTable,
    ) -> Result<Self, ConnectorError> {
        Self::reconcile_from_plan_inner(plan, options, existing, true)
    }

    pub fn reconcile_from_plan(
        plan: &ConnectorPlan,
        options: &NfsdApplyOptions,
        existing: &NfsdExportTable,
    ) -> Result<Self, ConnectorError> {
        Self::reconcile_from_plan_inner(plan, options, existing, false)
    }

    pub fn cleanup_dry_run_from_plan(
        plan: &ConnectorPlan,
        options: &NfsdApplyOptions,
    ) -> Result<Self, ConnectorError> {
        Self::cleanup_from_plan_inner(plan, options, true)
    }

    pub fn cleanup_from_plan(
        plan: &ConnectorPlan,
        options: &NfsdApplyOptions,
    ) -> Result<Self, ConnectorError> {
        Self::cleanup_from_plan_inner(plan, options, false)
    }

    pub fn cleanup_reconcile_dry_run_from_plan(
        plan: &ConnectorPlan,
        options: &NfsdApplyOptions,
        existing: &NfsdExportTable,
    ) -> Result<Self, ConnectorError> {
        Self::cleanup_reconcile_from_plan_inner(plan, options, existing, true)
    }

    pub fn cleanup_reconcile_from_plan(
        plan: &ConnectorPlan,
        options: &NfsdApplyOptions,
        existing: &NfsdExportTable,
    ) -> Result<Self, ConnectorError> {
        Self::cleanup_reconcile_from_plan_inner(plan, options, existing, false)
    }

    fn apply_from_plan_inner(
        plan: &ConnectorPlan,
        options: &NfsdApplyOptions,
        dry_run: bool,
    ) -> Result<Self, ConnectorError> {
        if plan.connector != "galvofs-nfsd" {
            return Err(ConnectorError::MissingNfsdApplyPlan);
        }

        let mut commands = Vec::new();
        let mut observations = Vec::new();
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
                    observations.push(NfsdExportObservation::new(
                        "planned",
                        export_path.clone(),
                        options.client_selector.clone(),
                        export_options.clone(),
                    ));
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
            operation: "apply".into(),
            applied: !dry_run,
            commands,
            observations,
            skipped_actions,
        })
    }

    fn reconcile_from_plan_inner(
        plan: &ConnectorPlan,
        options: &NfsdApplyOptions,
        existing: &NfsdExportTable,
        dry_run: bool,
    ) -> Result<Self, ConnectorError> {
        if plan.connector != "galvofs-nfsd" {
            return Err(ConnectorError::MissingNfsdApplyPlan);
        }

        let mut commands = Vec::new();
        let mut observations = Vec::new();
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
                    let status = if existing.has_export_with_options(
                        export_path,
                        &options.client_selector,
                        export_options,
                    ) {
                        "alreadyConfigured"
                    } else if existing.has_export(export_path, &options.client_selector) {
                        "drifted"
                    } else {
                        "missing"
                    };
                    observations.push(NfsdExportObservation::new(
                        status,
                        export_path.clone(),
                        options.client_selector.clone(),
                        export_options.clone(),
                    ));
                    if status != "alreadyConfigured" {
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
                }
                other => skipped_actions.push(other.kind_name().to_string()),
            }
        }

        if observations.is_empty() {
            return Err(ConnectorError::MissingNfsdApplyPlan);
        }

        Ok(Self {
            connector: plan.connector.clone(),
            operation: "reconcile".into(),
            applied: !dry_run,
            commands,
            observations,
            skipped_actions,
        })
    }

    fn cleanup_from_plan_inner(
        plan: &ConnectorPlan,
        options: &NfsdApplyOptions,
        dry_run: bool,
    ) -> Result<Self, ConnectorError> {
        if plan.connector != "galvofs-nfsd" {
            return Err(ConnectorError::MissingNfsdApplyPlan);
        }

        let mut commands = Vec::new();
        let mut observations = Vec::new();
        let mut skipped_actions = Vec::new();
        for action in &plan.actions {
            match action {
                ConnectorAction::ConfigureKernelNfsdExport { export_path, .. } => {
                    if !is_safe_absolute_path(export_path) {
                        return Err(ConnectorError::UnsafeNfsdExportPath(export_path.clone()));
                    }
                    let mut report = NfsdCommandReport::unexport(
                        options.exportfs_program.clone(),
                        options.client_selector.clone(),
                        export_path.clone(),
                    );
                    if !dry_run {
                        report.execute()?;
                    }
                    observations.push(NfsdExportObservation::new(
                        "cleanupPlanned",
                        export_path.clone(),
                        options.client_selector.clone(),
                        Vec::new(),
                    ));
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
            operation: "cleanup".into(),
            applied: !dry_run,
            commands,
            observations,
            skipped_actions,
        })
    }

    fn cleanup_reconcile_from_plan_inner(
        plan: &ConnectorPlan,
        options: &NfsdApplyOptions,
        existing: &NfsdExportTable,
        dry_run: bool,
    ) -> Result<Self, ConnectorError> {
        if plan.connector != "galvofs-nfsd" {
            return Err(ConnectorError::MissingNfsdApplyPlan);
        }

        let mut commands = Vec::new();
        let mut observations = Vec::new();
        let mut skipped_actions = Vec::new();
        for action in &plan.actions {
            match action {
                ConnectorAction::ConfigureKernelNfsdExport { export_path, .. } => {
                    if !is_safe_absolute_path(export_path) {
                        return Err(ConnectorError::UnsafeNfsdExportPath(export_path.clone()));
                    }
                    if existing.has_export(export_path, &options.client_selector) {
                        let mut report = NfsdCommandReport::unexport(
                            options.exportfs_program.clone(),
                            options.client_selector.clone(),
                            export_path.clone(),
                        );
                        if !dry_run {
                            report.execute()?;
                        }
                        observations.push(NfsdExportObservation::new(
                            "cleanupPlanned",
                            export_path.clone(),
                            options.client_selector.clone(),
                            Vec::new(),
                        ));
                        commands.push(report);
                    } else {
                        observations.push(NfsdExportObservation::new(
                            "alreadyAbsent",
                            export_path.clone(),
                            options.client_selector.clone(),
                            Vec::new(),
                        ));
                    }
                }
                other => skipped_actions.push(other.kind_name().to_string()),
            }
        }

        if observations.is_empty() {
            return Err(ConnectorError::MissingNfsdApplyPlan);
        }

        Ok(Self {
            connector: plan.connector.clone(),
            operation: "cleanup".into(),
            applied: !dry_run,
            commands,
            observations,
            skipped_actions,
        })
    }
}

impl NfsdExportObservation {
    fn new(
        status: impl Into<String>,
        export_path: String,
        client_selector: String,
        export_options: Vec<String>,
    ) -> Self {
        Self {
            status: status.into(),
            export_path,
            client_selector,
            export_options,
        }
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

    fn unexport(program: String, client_selector: String, export_path: String) -> Self {
        let args = vec!["-u".to_string(), format!("{client_selector}:{export_path}")];
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

impl NfsdExportTable {
    pub fn empty() -> Self {
        Self {
            exports: Vec::new(),
        }
    }

    pub fn parse_exportfs_verbose(input: &str) -> Result<Self, ConnectorError> {
        let mut exports = Vec::new();
        let mut current_path: Option<String> = None;

        for line in input.lines() {
            if line.trim().is_empty() {
                continue;
            }
            let starts_with_whitespace = line
                .chars()
                .next()
                .map(char::is_whitespace)
                .unwrap_or(false);
            let trimmed = line.trim();

            if !starts_with_whitespace && trimmed.starts_with('/') {
                if let Some((path, client_options)) = split_path_and_client_options(trimmed) {
                    exports.push(parse_nfsd_export_entry(path, client_options)?);
                    current_path = Some(path.to_string());
                } else {
                    current_path = Some(trimmed.to_string());
                }
                continue;
            }

            if let Some(path) = current_path.as_deref() {
                exports.push(parse_nfsd_export_entry(path, trimmed)?);
            } else {
                return Err(ConnectorError::InvalidNfsdExportState(format!(
                    "client entry appeared before export path: {trimmed}"
                )));
            }
        }

        Ok(Self { exports })
    }

    pub fn inspect_with_exportfs(exportfs_program: &str) -> Result<Self, ConnectorError> {
        let output = Command::new(exportfs_program)
            .arg("-v")
            .output()
            .map_err(|error| ConnectorError::NfsdApplyCommandFailed {
                display: format!("{exportfs_program} -v"),
                reason: error.to_string(),
            })?;
        if !output.status.success() {
            return Err(ConnectorError::NfsdApplyCommandFailed {
                display: format!("{exportfs_program} -v"),
                reason: String::from_utf8_lossy(&output.stderr).trim().to_string(),
            });
        }

        Self::parse_exportfs_verbose(&String::from_utf8_lossy(&output.stdout))
    }

    fn has_export(&self, export_path: &str, client_selector: &str) -> bool {
        self.exports.iter().any(|entry| {
            entry.export_path == export_path && entry.client_selector == client_selector
        })
    }

    fn has_export_with_options(
        &self,
        export_path: &str,
        client_selector: &str,
        required_options: &[String],
    ) -> bool {
        self.exports.iter().any(|entry| {
            if entry.export_path != export_path || entry.client_selector != client_selector {
                return false;
            }
            let present = entry.options.iter().collect::<BTreeSet<_>>();
            required_options
                .iter()
                .all(|option| present.contains(option))
        })
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

    pub fn reconcile_connector_plans(
        run_id: Option<String>,
        plans: &[ConnectorPlan],
        options: &NfsdApplyOptions,
        existing: &NfsdExportTable,
        dry_run: bool,
    ) -> Result<Self, ConnectorError> {
        let mut reports = Vec::new();
        for plan in plans {
            if plan.connector == "galvofs-nfsd" {
                let report = if dry_run {
                    NfsdApplyReport::reconcile_dry_run_from_plan(plan, options, existing)?
                } else {
                    NfsdApplyReport::reconcile_from_plan(plan, options, existing)?
                };
                reports.push(report);
            }
        }

        if reports.is_empty() {
            return Err(ConnectorError::MissingNfsdApplyPlan);
        }

        Ok(Self { run_id, reports })
    }

    pub fn cleanup_connector_plans(
        run_id: Option<String>,
        plans: &[ConnectorPlan],
        options: &NfsdApplyOptions,
        dry_run: bool,
    ) -> Result<Self, ConnectorError> {
        let mut reports = Vec::new();
        for plan in plans {
            if plan.connector == "galvofs-nfsd" {
                let report = if dry_run {
                    NfsdApplyReport::cleanup_dry_run_from_plan(plan, options)?
                } else {
                    NfsdApplyReport::cleanup_from_plan(plan, options)?
                };
                reports.push(report);
            }
        }

        if reports.is_empty() {
            return Err(ConnectorError::MissingNfsdApplyPlan);
        }

        Ok(Self { run_id, reports })
    }

    pub fn cleanup_reconcile_connector_plans(
        run_id: Option<String>,
        plans: &[ConnectorPlan],
        options: &NfsdApplyOptions,
        existing: &NfsdExportTable,
        dry_run: bool,
    ) -> Result<Self, ConnectorError> {
        let mut reports = Vec::new();
        for plan in plans {
            if plan.connector == "galvofs-nfsd" {
                let report = if dry_run {
                    NfsdApplyReport::cleanup_reconcile_dry_run_from_plan(plan, options, existing)?
                } else {
                    NfsdApplyReport::cleanup_reconcile_from_plan(plan, options, existing)?
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

fn split_path_and_client_options(line: &str) -> Option<(&str, &str)> {
    let split_at = line.find(char::is_whitespace)?;
    let path = &line[..split_at];
    let client_options = line[split_at..].trim();
    if client_options.contains('(') {
        Some((path, client_options))
    } else {
        None
    }
}

fn parse_nfsd_export_entry(
    export_path: &str,
    client_options: &str,
) -> Result<NfsdExportEntry, ConnectorError> {
    let open = client_options.find('(').ok_or_else(|| {
        ConnectorError::InvalidNfsdExportState(format!(
            "missing option list for export {export_path}: {client_options}"
        ))
    })?;
    let close = client_options.rfind(')').ok_or_else(|| {
        ConnectorError::InvalidNfsdExportState(format!(
            "unterminated option list for export {export_path}: {client_options}"
        ))
    })?;
    let client_selector = client_options[..open].trim();
    if client_selector.is_empty() {
        return Err(ConnectorError::InvalidNfsdExportState(format!(
            "missing client selector for export {export_path}"
        )));
    }
    let options = client_options[open + 1..close]
        .split(',')
        .map(str::trim)
        .filter(|option| !option.is_empty())
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();

    Ok(NfsdExportEntry {
        export_path: export_path.to_string(),
        client_selector: client_selector.to_string(),
        options,
    })
}

fn command_display(program: &str, args: &[String]) -> String {
    std::iter::once(program.to_string())
        .chain(args.iter().cloned())
        .collect::<Vec<_>>()
        .join(" ")
}
