#![allow(dead_code)]

use std::fmt::Display;
use std::path::{Path, PathBuf};

use nomai_core::CoreError;

pub(crate) mod baseline;
pub(crate) mod cases;
pub(crate) mod metrics;

fn config_error(path: &Path, message: impl Display) -> CoreError {
    CoreError::Config(format!("{}: {message}", path.display()))
}

fn read_to_string(path: &Path) -> Result<String, CoreError> {
    std::fs::read_to_string(path).map_err(|err| config_error(path, format!("read failed: {err}")))
}

fn sorted_files(dir: &Path, extension: &str) -> Result<Vec<PathBuf>, CoreError> {
    let mut paths = std::fs::read_dir(dir)
        .map_err(|err| config_error(dir, format!("read_dir failed: {err}")))?
        .map(|entry| {
            entry
                .map(|entry| entry.path())
                .map_err(|err| config_error(dir, format!("read_dir entry failed: {err}")))
        })
        .collect::<Result<Vec<_>, _>>()?;

    paths.retain(|path| path.is_file() && path.extension().is_some_and(|ext| ext == extension));
    paths.sort();
    Ok(paths)
}
