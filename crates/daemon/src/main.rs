//! nomai-daemon entry point.

use std::process::ExitCode;

use nomai_core::storage;

mod config;
mod daemon;
// modules below are added in later tasks; only declare what exists.
// mod handlers;  // Task 5+
// mod rpc;       // Task 5+
// mod io;        // Task 5+

fn install_panic_hook() {
    let default_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        eprintln!("nomai-daemon panic: {info}");
        default_hook(info);
    }));
}

fn main() -> ExitCode {
    install_panic_hook();
    // Must run before any Connection::open in the process.
    storage::init_sqlite_extensions();

    let config = match config::Config::load() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("config error: {e}");
            return ExitCode::FAILURE;
        }
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
