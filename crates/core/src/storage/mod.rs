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

/// Register the sqlite-vec extension globally so any subsequently-opened
/// `Connection` supports `vec0` virtual tables. Idempotent and safe to call
/// multiple times.
///
/// # Safety
///
/// `sqlite3_auto_extension` is thread-safe and idempotent per SQLite docs.
/// The `transmute` casts the `sqlite3_vec_init` entry point to the
/// `sqlite3_ext_init` signature that `sqlite3_auto_extension` expects.
pub fn init_sqlite_extensions() {
    unsafe {
        rusqlite::ffi::sqlite3_auto_extension(Some(std::mem::transmute::<
            *const (),
            unsafe extern "C" fn(
                *mut rusqlite::ffi::sqlite3,
                *mut *mut std::os::raw::c_char,
                *const rusqlite::ffi::sqlite3_api_routines,
            ) -> std::os::raw::c_int,
        >(
            sqlite_vec::sqlite3_vec_init as *const ()
        )));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;

    #[test]
    fn init_registers_vec0_virtual_table_module() {
        init_sqlite_extensions();
        let conn = Connection::open_in_memory().unwrap();
        // After init, vec0 virtual tables should be creatable.
        conn.execute_batch(
            "CREATE VIRTUAL TABLE test_vec USING vec0(id TEXT PRIMARY KEY, embedding float[4])",
        )
        .expect("vec0 should be available after init_sqlite_extensions");
    }
}
