//! nomai-daemon entry point.

use std::process::ExitCode;

use nomai_core::storage;

mod config;
mod daemon;
mod handlers;
mod io;
mod rpc;
mod search_cache;
// socket.rs is a shared source file (lib re-exports it as `pub`). The bin's
// private copy is not yet wired into main — it is consumed by `serve`/`shim`
// in Tasks 2-4. Silence dead-code until then.
#[cfg(unix)]
#[allow(dead_code)]
mod serve;
#[cfg(unix)]
#[allow(dead_code)]
mod shim;
#[cfg(unix)]
#[allow(dead_code)]
mod socket;

fn install_panic_hook() {
    let default_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        eprintln!("nomai-daemon panic: {info}");
        default_hook(info);
    }));
}

/// Parse `--config <path>` / `--config=<path>` from the process args.
///
/// Returns the path to load the config from if `--config` is present, or
/// `None` to fall back to the default config path. Unknown args are rejected
/// with a usage message and a non-zero exit code.
fn parse_config_path_arg() -> Result<Option<std::path::PathBuf>, ExitCode> {
    let mut iter = std::env::args().skip(1);
    let mut config_path: Option<std::path::PathBuf> = None;
    while let Some(arg) = iter.next() {
        if arg == "--config" {
            let path = match iter.next() {
                Some(p) => p,
                None => {
                    eprintln!("error: --config requires a path argument");
                    eprintln!("usage: nomai-daemon [--config <path>]");
                    return Err(ExitCode::FAILURE);
                }
            };
            config_path = Some(std::path::PathBuf::from(path));
        } else if let Some(rest) = arg.strip_prefix("--config=") {
            config_path = Some(std::path::PathBuf::from(rest));
        } else {
            eprintln!("error: unknown argument: {arg}");
            eprintln!("usage: nomai-daemon [--config <path>]");
            return Err(ExitCode::FAILURE);
        }
    }
    Ok(config_path)
}

fn main() -> ExitCode {
    install_panic_hook();
    // Must run before any Connection::open in the process.
    storage::init_sqlite_extensions();

    let config_path = match parse_config_path_arg() {
        Ok(p) => p,
        Err(code) => return code,
    };

    let config = match config_path {
        Some(path) => match config::Config::load_from(&path) {
            Ok(c) => c,
            Err(e) => {
                eprintln!("config error: {e}");
                return ExitCode::FAILURE;
            }
        },
        None => match config::Config::load() {
            Ok(c) => c,
            Err(e) => {
                eprintln!("config error: {e}");
                return ExitCode::FAILURE;
            }
        },
    };

    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("tokio runtime error: {e}");
            return ExitCode::FAILURE;
        }
    };

    let result = runtime.block_on(async move {
        let daemon = daemon::Daemon::new(config).await?;
        daemon.run_stdio().await
    });

    if let Err(e) = result {
        eprintln!("daemon error: {e}");
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}
