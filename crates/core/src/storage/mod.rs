//! SQLite storage layer: migrations and connection setup.

mod embedded {
    use refinery::embed_migrations;
    embed_migrations!("migrations");
}

use rusqlite::Connection;

use crate::error::CoreError;

/// Run all pending migrations on the given connection. Idempotent.
pub fn run_migrations(conn: &mut Connection) -> Result<(), CoreError> {
    embedded::migrations::runner()
        .run(conn)
        .map(|_| ())
        .map_err(|e| CoreError::Migration(e.to_string()))
}
