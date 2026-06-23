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

    /// Render `doc` and atomically write to `<root>/entries/<id>/entry.nomai`.
    /// Creates parent directories. Overwrites existing file.
    /// Does NOT touch the SQLite index — that's the service layer's job.
    pub fn write_entry(
        &self,
        entry_id: Ulid,
        doc: &crate::nomai_format::NomaiDoc,
    ) -> Result<(), CoreError> {
        let path = self.entry_file(entry_id);
        let content = crate::nomai_format::render(doc);
        atomic_write(&path, &content)
    }

    /// Read and parse `<root>/entries/<id>/entry.nomai`.
    /// Returns `CoreError::Io` if the file is missing or unreadable,
    /// `CoreError::NomaiFormat` if parsing fails.
    pub fn read_entry(&self, entry_id: Ulid) -> Result<crate::nomai_format::NomaiDoc, CoreError> {
        let path = self.entry_file(entry_id);
        let content = std::fs::read_to_string(&path)?;
        crate::nomai_format::parse(&content).map_err(CoreError::from)
    }

    /// Recursively remove the entry's directory at `<root>/entries/<id>/`.
    /// Idempotent: succeeds if the directory never existed. Returns
    /// `CoreError::Io` if the path exists but cannot be removed (e.g. it's
    /// a non-empty file instead of a directory, or permissions).
    pub fn delete_entry(&self, entry_id: Ulid) -> Result<(), CoreError> {
        let dir = self.entry_dir(entry_id);
        match std::fs::remove_dir_all(&dir) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(CoreError::Io(e)),
        }
    }
}

/// Atomically write `content` to `path`. Writes to `<path>.tmp`, fsyncs, then
/// renames to final path. POSIX rename is atomic, so readers never see a
/// partial file.
///
/// Parent directories are created if missing (equivalent to `mkdir -p`).
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

    use crate::nomai_format::{Block, BlockType, NomaiDoc};
    use serde_json::Map as JsonMap;

    fn sample_doc() -> NomaiDoc {
        NomaiDoc {
            format_version: 1,
            id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".parse().unwrap(),
            title: "Test".into(),
            tags: vec![],
            attrs: JsonMap::new(),
            source: None,
            created_at: "2026-06-23T10:00:00Z".parse().unwrap(),
            updated_at: "2026-06-23T10:00:00Z".parse().unwrap(),
            blocks: vec![Block {
                r#type: BlockType::Note,
                text: "Hello.\n".into(),
                attrs: JsonMap::new(),
            }],
        }
    }

    #[test]
    fn write_entry_creates_nomai_file() {
        let tmp = tempfile::tempdir().unwrap();
        let store = ContentStore::new(tmp.path().to_path_buf());
        let id: Ulid = "01ARZ3NDEKTSV4RRFFQ69G5FAV".parse().unwrap();
        let doc = sample_doc();

        store.write_entry(id, &doc).unwrap();

        let file = store.entry_file(id);
        assert!(file.exists(), "entry.nomai should exist");
        let content = std::fs::read_to_string(&file).unwrap();
        assert!(content.contains("#format_version 1"));
        assert!(content.contains("#title Test"));
        assert!(content.contains("@note"));
        assert!(content.contains("Hello."));
    }

    #[test]
    fn write_entry_round_trips_through_read() {
        let tmp = tempfile::tempdir().unwrap();
        let store = ContentStore::new(tmp.path().to_path_buf());
        let id: Ulid = "01ARZ3NDEKTSV4RRFFQ69G5FAV".parse().unwrap();
        let doc = sample_doc();

        store.write_entry(id, &doc).unwrap();
        let read = store.read_entry(id).unwrap();
        assert_eq!(read, doc);
    }

    #[test]
    fn read_entry_errors_on_missing_file() {
        let tmp = tempfile::tempdir().unwrap();
        let store = ContentStore::new(tmp.path().to_path_buf());
        let id: Ulid = "01ARZ3NDEKTSV4RRFFQ69G5FAV".parse().unwrap();
        let err = store.read_entry(id).unwrap_err();
        assert!(matches!(err, CoreError::Io(_)));
    }

    #[test]
    fn read_entry_errors_on_malformed_content() {
        let tmp = tempfile::tempdir().unwrap();
        let store = ContentStore::new(tmp.path().to_path_buf());
        let id: Ulid = "01ARZ3NDEKTSV4RRFFQ69G5FAV".parse().unwrap();
        // Write malformed content directly via atomic_write
        let path = store.entry_file(id);
        atomic_write(&path, "not a valid nomai file").unwrap();

        let err = store.read_entry(id).unwrap_err();
        assert!(matches!(err, CoreError::NomaiFormat(_)));
    }

    #[test]
    fn delete_entry_removes_directory_and_contents() {
        let tmp = tempfile::tempdir().unwrap();
        let store = ContentStore::new(tmp.path().to_path_buf());
        let id: Ulid = "01ARZ3NDEKTSV4RRFFQ69G5FAV".parse().unwrap();
        let doc = sample_doc();
        store.write_entry(id, &doc).unwrap();
        // Drop a sibling file to verify rm -rf semantics
        let sibling = store.entry_dir(id).join("source.pdf");
        std::fs::write(&sibling, b"fake pdf").unwrap();

        store.delete_entry(id).unwrap();

        assert!(!store.entry_dir(id).exists(), "entry dir should be gone");
    }

    #[test]
    fn delete_entry_is_idempotent() {
        let tmp = tempfile::tempdir().unwrap();
        let store = ContentStore::new(tmp.path().to_path_buf());
        let id: Ulid = "01ARZ3NDEKTSV4RRFFQ69G5FAV".parse().unwrap();
        // Never written — delete should succeed anyway.
        store.delete_entry(id).unwrap();
    }
}
