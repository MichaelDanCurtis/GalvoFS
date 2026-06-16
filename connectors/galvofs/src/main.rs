use std::env;
use std::io::{self, Read};
use std::process::ExitCode;

use galvofs_connector::{
    AccessEndpointsResponse, ConnectorPlansResponse, NfsdApplyOptions, NfsdApplyReportsResponse,
};

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            ExitCode::from(2)
        }
    }
}

fn run() -> Result<(), String> {
    let options = CliOptions::from_args(env::args().skip(1))?;
    let mut input = String::new();
    io::stdin()
        .read_to_string(&mut input)
        .map_err(|error| format!("failed to read GalvoFS endpoint JSON from stdin: {error}"))?;

    let response = parse_access_endpoint_input(&input)?;
    let plans = ConnectorPlansResponse::from_access_endpoints(response)
        .map_err(|error| format!("failed to plan GalvoFS connector actions: {error}"))?;
    let output = match options.mode {
        CliMode::Plan => serde_json::to_string_pretty(&plans)
            .map_err(|error| format!("failed to serialize connector plan: {error}"))?,
        CliMode::ApplyNfsd {
            dry_run,
            exportfs_program,
        } => {
            let client_selector = env::var("FEMTO_GALVOFS_NFSD_CLIENTS").map_err(|_| {
                "FEMTO_GALVOFS_NFSD_CLIENTS must be set for --apply-nfsd".to_string()
            })?;
            let apply_options =
                NfsdApplyOptions::with_exportfs_program(client_selector, exportfs_program)
                    .map_err(|error| format!("failed to configure NFSD apply: {error}"))?;
            let reports = NfsdApplyReportsResponse::from_connector_plans(
                plans.run_id,
                &plans.plans,
                &apply_options,
                dry_run,
            )
            .map_err(|error| format!("failed to apply GalvoFS NFSD plan: {error}"))?;
            serde_json::to_string_pretty(&reports)
                .map_err(|error| format!("failed to serialize NFSD apply report: {error}"))?
        }
    };
    println!("{output}");
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CliOptions {
    mode: CliMode,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum CliMode {
    Plan,
    ApplyNfsd {
        dry_run: bool,
        exportfs_program: String,
    },
}

impl CliOptions {
    fn from_args(args: impl IntoIterator<Item = String>) -> Result<Self, String> {
        let mut apply_nfsd = false;
        let mut dry_run = false;
        let mut exportfs_program = "exportfs".to_string();

        let mut args = args.into_iter();
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--apply-nfsd" => apply_nfsd = true,
                "--dry-run" => dry_run = true,
                "--exportfs-command" => {
                    exportfs_program = args.next().ok_or_else(|| {
                        "--exportfs-command requires an executable path".to_string()
                    })?;
                }
                "--help" | "-h" => return Err(usage()),
                other => return Err(format!("unknown argument {other:?}\n\n{}", usage())),
            }
        }

        let mode = if apply_nfsd {
            CliMode::ApplyNfsd {
                dry_run,
                exportfs_program,
            }
        } else {
            if dry_run {
                return Err("--dry-run requires --apply-nfsd".into());
            }
            CliMode::Plan
        };

        Ok(Self { mode })
    }
}

fn usage() -> String {
    "usage: galvofs-connector [--apply-nfsd [--dry-run] [--exportfs-command PATH]]".into()
}

fn parse_access_endpoint_input(input: &str) -> Result<AccessEndpointsResponse, String> {
    if let Ok(response) = serde_json::from_str::<AccessEndpointsResponse>(input) {
        if !response.access_endpoints.is_empty() {
            return Ok(response);
        }
    }

    serde_json::from_str(input)
        .map(|endpoint| AccessEndpointsResponse {
            run_id: None,
            access_endpoints: vec![endpoint],
        })
        .or_else(|_| {
            serde_json::from_str::<Vec<_>>(input).map(|access_endpoints| AccessEndpointsResponse {
                run_id: None,
                access_endpoints,
            })
        })
        .map_err(|error| format!("invalid GalvoFS access endpoint JSON: {error}"))
}
