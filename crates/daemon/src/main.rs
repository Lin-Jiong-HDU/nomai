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
mod serve;
mod shim;
#[allow(dead_code)]
mod socket;
mod sync_cli;

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
    /// True if `--sync` was passed (dispatch `sync.run` and exit).
    sync_run: bool,
    /// URL from `--sync-init <url>` (dispatch `sync.init` and exit).
    sync_init: Option<String>,
    /// Optional `--branch <name>` for `--sync-init`.
    sync_branch: Option<String>,
}

/// Parse `--serve`, `--sync`, `--sync-init <url>`, `--branch <name>`, and
/// `--config <path>` / `--config=<path>` from args.
fn parse_args() -> Result<CliArgs, ExitCode> {
    let mut iter = std::env::args().skip(1);
    let mut config_path: Option<std::path::PathBuf> = None;
    let mut serve = false;
    let mut sync_run = false;
    let mut sync_init: Option<String> = None;
    let mut sync_branch: Option<String> = None;
    while let Some(arg) = iter.next() {
        if arg == "--serve" {
            serve = true;
        } else if arg == "--sync" {
            sync_run = true;
        } else if arg == "--sync-init" {
            sync_init = Some(match iter.next() {
                Some(u) => u,
                None => {
                    eprintln!("error: --sync-init requires a URL argument");
                    eprintln!(
                        "usage: nomai-daemon [--serve | --sync | --sync-init <url> [--branch <name>]] [--config <path>]"
                    );
                    return Err(ExitCode::FAILURE);
                }
            });
        } else if arg == "--branch" {
            sync_branch = Some(match iter.next() {
                Some(b) => b,
                None => {
                    eprintln!("error: --branch requires a value");
                    eprintln!(
                        "usage: nomai-daemon [--serve | --sync | --sync-init <url> [--branch <name>]] [--config <path>]"
                    );
                    return Err(ExitCode::FAILURE);
                }
            });
        } else if arg == "--config" {
            let path = match iter.next() {
                Some(p) => p,
                None => {
                    eprintln!("error: --config requires a path argument");
                    eprintln!(
                        "usage: nomai-daemon [--serve | --sync | --sync-init <url> [--branch <name>]] [--config <path>]"
                    );
                    return Err(ExitCode::FAILURE);
                }
            };
            config_path = Some(std::path::PathBuf::from(path));
        } else if let Some(rest) = arg.strip_prefix("--config=") {
            config_path = Some(std::path::PathBuf::from(rest));
        } else {
            eprintln!("error: unknown argument: {arg}");
            eprintln!(
                "usage: nomai-daemon [--serve | --sync | --sync-init <url> [--branch <name>]] [--config <path>]"
            );
            return Err(ExitCode::FAILURE);
        }
    }
    Ok(CliArgs {
        config_path,
        serve,
        sync_run,
        sync_init,
        sync_branch,
    })
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
        } else if args.sync_run {
            sync_cli::run(config, sync_cli::SyncCmd::Run).await
        } else if let Some(remote) = args.sync_init {
            sync_cli::run(
                config,
                sync_cli::SyncCmd::Init {
                    remote,
                    branch: args.sync_branch,
                },
            )
            .await
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
