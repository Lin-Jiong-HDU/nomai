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
// private copy is consumed transitively by `serve`/`shim`.
#[cfg(unix)]
mod serve;
#[cfg(unix)]
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

/// Parsed CLI args.
struct CliArgs {
    /// Path from `--config`, or None for the default config path.
    config_path: Option<std::path::PathBuf>,
    /// True if `--serve` was passed (resident daemon mode).
    serve: bool,
}

/// Parse `--serve` and `--config <path>` / `--config=<path>` from args.
fn parse_args() -> Result<CliArgs, ExitCode> {
    let mut iter = std::env::args().skip(1);
    let mut config_path: Option<std::path::PathBuf> = None;
    let mut serve = false;
    while let Some(arg) = iter.next() {
        if arg == "--serve" {
            serve = true;
        } else if arg == "--config" {
            let path = match iter.next() {
                Some(p) => p,
                None => {
                    eprintln!("error: --config requires a path argument");
                    eprintln!("usage: nomai-daemon [--serve] [--config <path>]");
                    return Err(ExitCode::FAILURE);
                }
            };
            config_path = Some(std::path::PathBuf::from(path));
        } else if let Some(rest) = arg.strip_prefix("--config=") {
            config_path = Some(std::path::PathBuf::from(rest));
        } else {
            eprintln!("error: unknown argument: {arg}");
            eprintln!("usage: nomai-daemon [--serve] [--config <path>]");
            return Err(ExitCode::FAILURE);
        }
    }
    Ok(CliArgs { config_path, serve })
}

fn main() -> ExitCode {
    install_panic_hook();
    // Must run before any Connection::open in the process.
    storage::init_sqlite_extensions();

    let args = match parse_args() {
        Ok(a) => a,
        Err(code) => return code,
    };

    let config = match args.config_path.as_deref() {
        Some(path) => match config::Config::load_from(path) {
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
    // Pass the resolved config path to spawned --serve children via env so
    // they reload the identical config (shim::spawn_serve reads this).
    if let Some(p) = &args.config_path {
        // SAFETY: main is single-threaded before the runtime starts.
        unsafe { std::env::set_var("NOMAI_CONFIG_PATH", p) };
    }

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
        if args.serve {
            serve::run(config).await
        } else {
            shim::run(config).await
        }
    });

    if let Err(e) = result {
        eprintln!("daemon error: {e}");
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}
