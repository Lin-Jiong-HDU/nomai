//! Filesystem layer for the new content-storage model (Spec 6 §3, §7).
//!
//! `ContentStore` owns the `knowledge_root` path and provides:
//! - Path resolution (`entry_dir`, `entry_file`)
//! - Atomic writes (write `.tmp` → rename)
//! - Read/delete entry directories
//!
//! Plan 2 scope: scaffolding + atomic_write. write_entry / read_entry / delete_entry
//! come in Tasks 5-7.

use std::path::{Path, PathBuf};

use ulid::Ulid;

use crate::error::CoreError;

/// Filesystem-backed content store. Root path is constructor-injected; no
/// global state. Methods are sync (FS ops are short, lock contention is OK).
#[derive(Debug, Clone)]
pub struct ContentStore {
    root: PathBuf,
}

impl ContentStore {
    /// Create a store rooted at `root`. The directory need not exist yet;
    /// callers should ensure `root` is writable when invoking write methods.
    pub fn new(root: PathBuf) -> Self {
        Self { root }
    }

    /// Path to the directory holding one entry's files: `<root>/entries/<ULID>/`.
    pub fn entry_dir(&self, entry_id: Ulid) -> PathBuf {
        self.root.join("entries").join(entry_id.to_string())
    }

    /// Path to the entry's `.nomai` file: `<root>/entries/<ULID>/entry.nomai`.
    pub fn entry_file(&self, entry_id: Ulid) -> PathBuf {
        self.entry_dir(entry_id).join("entry.nomai")
    }

    /// Root path accessor (read-only).
    pub fn root(&self) -> &Path {
        &self.root
    }
}

/// Atomically write `content` to `path`. Writes to `<path>.tmp`, fsyncs, then
/// renames to final path. POSIX rename is atomic, so readers never see a
/// partial file.
///
/// Parent directories are created if missing (equivalent to `mkdir -p`).
#[allow(dead_code)] // used by write_entry in Task 5
pub(crate) fn atomic_write(path: &Path, content: &str) -> Result<(), CoreError> {
    use std::fs::{self, File};
    use std::io::Write;

    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let tmp = path.with_extension("nomai.tmp");
    {
        let mut f = File::create(&tmp)?;
        f.write_all(content.as_bytes())?;
        f.sync_all()?;
    }

    fs::rename(&tmp, path)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entry_dir_resolves_under_root() {
        let store = ContentStore::new(PathBuf::from("/tmp/nomai-test"));
        let id: Ulid = "01ARZ3NDEKTSV4RRFFQ69G5FAV".parse().unwrap();
        assert_eq!(
            store.entry_dir(id),
            PathBuf::from("/tmp/nomai-test/entries/01ARZ3NDEKTSV4RRFFQ69G5FAV")
        );
        assert_eq!(
            store.entry_file(id),
            PathBuf::from("/tmp/nomai-test/entries/01ARZ3NDEKTSV4RRFFQ69G5FAV/entry.nomai")
        );
    }

    #[test]
    fn atomic_write_creates_parent_dirs() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("a/b/c/entry.nomai");
        atomic_write(&path, "hello").unwrap();
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "hello");
    }

    #[test]
    fn atomic_write_overwrites_existing() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("entry.nomai");
        atomic_write(&path, "v1").unwrap();
        atomic_write(&path, "v2").unwrap();
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "v2");
    }

    #[test]
    fn atomic_write_leaves_no_tmp_on_success() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("entry.nomai");
        atomic_write(&path, "content").unwrap();
        let tmp_path = path.with_extension("nomai.tmp");
        assert!(!tmp_path.exists(), "tmp file should be renamed away");
    }
}
