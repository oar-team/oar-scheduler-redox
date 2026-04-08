use serde::Serialize;
use std::fs;
use std::path::Path;

#[derive(Debug, Serialize)]
pub struct PhaseBenchExport<TConfig, TResult> {
    pub mode: String,
    pub config: TConfig,
    pub results: TResult,
}

pub fn save_json<TConfig, TResult>(
    path: &str,
    mode: &str,
    config: &TConfig,
    results: &TResult,
) -> std::io::Result<()>
where
    TConfig: Serialize,
    TResult: Serialize,
{
    let export = PhaseBenchExport {
        mode: mode.to_string(),
        config,
        results,
    };

    if let Some(parent) = Path::new(path).parent() {
        fs::create_dir_all(parent)?;
    }

    let json = serde_json::to_string_pretty(&export)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

    fs::write(path, json)
}