use std::io::{self, Read};
use std::process::ExitCode;

use galvofs_connector::{AccessEndpointsResponse, ConnectorPlansResponse};

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
    let mut input = String::new();
    io::stdin()
        .read_to_string(&mut input)
        .map_err(|error| format!("failed to read GalvoFS endpoint JSON from stdin: {error}"))?;

    let response = parse_access_endpoint_input(&input)?;
    let plans = ConnectorPlansResponse::from_access_endpoints(response)
        .map_err(|error| format!("failed to plan GalvoFS connector actions: {error}"))?;
    let output = serde_json::to_string_pretty(&plans)
        .map_err(|error| format!("failed to serialize connector plan: {error}"))?;
    println!("{output}");
    Ok(())
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
