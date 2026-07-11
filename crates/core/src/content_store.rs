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
#[derive(Debug)]
pub struct ContentStore {
    root: PathBuf,
    /// Optional cleanup guard. When `Some`, the guard's `TempDir` owns the
    /// root path and deletes it on drop. Production callers use `new` (guard
    /// = `None`); test callers that would otherwise leak `nomai-test-*`
    /// directories under `std::env::temp_dir()` use `new_with_cleanup`.
    _cleanup: Option<tempfile::TempDir>,
}

/// Metadata for one sibling attachment file. Returned by `list_attachments`.
/// Not stored in SQLite — derived from FS on each call (spec §11.1: no
/// attachment manifest table).
#[derive(Debug, Clone, serde::Serialize)]
pub struct AttachmentMeta {
    pub filename: String,
    pub size: u64,
    pub modified: chrono::DateTime<chrono::Utc>,
}

impl ContentStore {
    /// Create a store rooted at `root`. The directory need not exist yet;
    /// callers should ensure `root` is writable when invoking write methods.
    ///
    /// Production path: the caller owns `root` and is responsible for its
    /// lifecycle (typically it's a user-managed knowledge base directory that
    /// must outlive the daemon). `_cleanup` is `None`.
    pub fn new(root: PathBuf) -> Self {
        Self {
            root,
            _cleanup: None,
        }
    }

    /// Test-only constructor that takes ownership of a `TempDir` cleanup
    /// guard. The returned store holds the guard; when the store (or its
    /// enclosing `Arc`) is dropped, the temp directory is deleted
    /// recursively. Callers should pass `tempfile::tempdir()?.into_path()`
    /// or equivalent — the guard expects the directory to already exist.
    #[doc(hidden)]
    pub fn new_with_cleanup(root: PathBuf, cleanup: tempfile::TempDir) -> Self {
        Self {
            root,
            _cleanup: Some(cleanup),
        }
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

    /// Walk `<root>/entries/` and return every directory whose name parses as
    /// a ULID. Invalid dir names are silently skipped (caller can decide
    /// whether to log). Returns ULIDs in arbitrary order (caller sorts if
    /// needed). Returns an empty `Vec` if `entries/` does not exist.
    pub fn scan_entry_ids(&self) -> Vec<Ulid> {
        let entries_dir = self.root.join("entries");
        let mut ids = Vec::new();
        if let Ok(read_dir) = std::fs::read_dir(&entries_dir) {
            for entry in read_dir.flatten() {
                if !entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                    continue;
                }
                let name = entry.file_name();
                let name_str = match name.to_str() {
                    Some(s) => s,
                    None => continue,
                };
                if let Ok(id) = name_str.parse::<Ulid>() {
                    ids.push(id);
                }
            }
        }
        ids
    }

    /// Return the mtime of an entry's `entry.nomai` file, or `None` if the
    /// file is missing or its mtime cannot be read.
    pub fn entry_mtime(&self, entry_id: Ulid) -> Option<chrono::DateTime<chrono::Utc>> {
        let path = self.entry_file(entry_id);
        let metadata = std::fs::metadata(&path).ok()?;
        let mtime = metadata.modified().ok()?;
        Some(chrono::DateTime::<chrono::Utc>::from(mtime))
    }

    /// Write `data` as a sibling attachment file `filename` under the entry
    /// directory. Overwrites atomically if the file exists (tmp→rename).
    /// `filename` is validated by `sanitize_attachment_filename`.
    pub fn write_attachment(
        &self,
        entry_id: Ulid,
        filename: &str,
        data: &[u8],
    ) -> Result<(), CoreError> {
        let safe = sanitize_attachment_filename(filename)?;
        let path = self.entry_dir(entry_id).join(&safe);
        atomic_write_bytes(&path, data)
    }

    /// Read a sibling attachment file. Returns `Validation("attachment not
    /// found: <name>")` (NOT `NotFound` — that variant carries an entry ULID)
    /// when the file is absent.
    pub fn read_attachment(&self, entry_id: Ulid, filename: &str) -> Result<Vec<u8>, CoreError> {
        let safe = sanitize_attachment_filename(filename)?;
        let path = self.entry_dir(entry_id).join(&safe);
        std::fs::read(&path).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                CoreError::Validation(format!("attachment not found: {filename}"))
            } else {
                CoreError::Io(e)
            }
        })
    }

    /// List sibling attachment files under the entry directory, excluding
    /// `entry.nomai` itself. Returns empty `Vec` if the entry directory does
    /// not exist yet (not an error). Sorted by filename for stable output.
    pub fn list_attachments(&self, entry_id: Ulid) -> Result<Vec<AttachmentMeta>, CoreError> {
        let dir = self.entry_dir(entry_id);
        let mut out = Vec::new();
        match std::fs::read_dir(&dir) {
            Ok(rd) => {
                for entry in rd.flatten() {
                    let name = entry.file_name();
                    let name_str = match name.to_str() {
                        Some(s) => s,
                        None => continue,
                    };
                    if name_str == "entry.nomai" {
                        continue;
                    }
                    if !entry.file_type().map(|t| t.is_file()).unwrap_or(false) {
                        continue;
                    }
                    let meta = match entry.metadata() {
                        Ok(m) => m,
                        Err(_) => continue,
                    };
                    let modified = meta
                        .modified()
                        .map(chrono::DateTime::<chrono::Utc>::from)
                        .unwrap_or_else(|_| chrono::Utc::now());
                    out.push(AttachmentMeta {
                        filename: name_str.to_string(),
                        size: meta.len(),
                        modified,
                    });
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(CoreError::Io(e)),
        }
        out.sort_by(|a, b| a.filename.cmp(&b.filename));
        Ok(out)
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

/// Atomically write `data` (raw bytes) to `path`. Same tmp→fsync→rename
/// semantics as `atomic_write`, but for binary content. The tmp name is
/// hand-computed (`<name>.tmp`) because `Path::with_extension` mangles
/// multi-dot (e.g. `sunset.png` → `sunset.tmp`) and dotless filenames.
pub(crate) fn atomic_write_bytes(path: &Path, data: &[u8]) -> Result<(), CoreError> {
    use std::fs::{self, File};
    use std::io::Write;

    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let tmp = {
        let mut name = path
            .file_name()
            .ok_or_else(|| {
                CoreError::Validation(format!("unsafe attachment path: {}", path.display()))
            })?
            .to_os_string();
        name.push(".tmp");
        path.with_file_name(name)
    };
    {
        let mut f = File::create(&tmp)?;
        f.write_all(data)?;
        f.sync_all()?;
    }
    fs::rename(&tmp, path)?;
    Ok(())
}

/// Validate an attachment filename is a single safe path segment. Rejects
/// empty, absolute paths, `~`, path separators (`/`, `\`), `.`/`..`, NUL,
/// and Windows-reserved chars (`: < > * ? | "`). This is the only line of
/// defense against path traversal — `entry_id` is a ULID (safe), but
/// `filename` comes from RPC callers.
fn sanitize_attachment_filename(filename: &str) -> Result<String, CoreError> {
    let bad = filename.is_empty()
        || filename.starts_with('/')
        || filename.starts_with('~')
        || filename.contains('/')
        || filename.contains('\\')
        || filename == "."
        || filename == ".."
        || filename.contains('\0')
        || filename
            .chars()
            .any(|c| matches!(c, ':' | '<' | '>' | '*' | '?' | '|' | '"'));
    if bad {
        return Err(CoreError::Validation(format!(
            "unsafe attachment filename: {filename}"
        )));
    }
    Ok(filename.to_string())
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

    #[test]
    fn scan_entry_ids_returns_all_written_entries() {
        let tmp = tempfile::tempdir().unwrap();
        let store = ContentStore::new(tmp.path().to_path_buf());
        let id1: Ulid = "01ARZ3NDEKTSV4RRFFQ69G5FAV".parse().unwrap();
        let id2: Ulid = "01ARZ3NDEKTSV4RRFFQ69G5FAX".parse().unwrap();
        store.write_entry(id1, &sample_doc()).unwrap();
        store.write_entry(id2, &sample_doc()).unwrap();

        let mut ids = store.scan_entry_ids();
        ids.sort();
        assert_eq!(ids, vec![id1, id2]);
    }

    #[test]
    fn scan_entry_ids_skips_invalid_dir_names() {
        let tmp = tempfile::tempdir().unwrap();
        let store = ContentStore::new(tmp.path().to_path_buf());
        let id: Ulid = "01ARZ3NDEKTSV4RRFFQ69G5FAV".parse().unwrap();
        store.write_entry(id, &sample_doc()).unwrap();

        // Drop a junk directory
        std::fs::create_dir_all(tmp.path().join("entries").join("not-a-ulid")).unwrap();

        let ids = store.scan_entry_ids();
        assert_eq!(ids, vec![id]); // junk dir skipped
    }

    #[test]
    fn entry_mtime_returns_some_for_existing() {
        let tmp = tempfile::tempdir().unwrap();
        let store = ContentStore::new(tmp.path().to_path_buf());
        let id: Ulid = "01ARZ3NDEKTSV4RRFFQ69G5FAV".parse().unwrap();
        store.write_entry(id, &sample_doc()).unwrap();

        let mtime = store.entry_mtime(id);
        assert!(mtime.is_some());
    }

    #[test]
    fn entry_mtime_returns_none_for_missing() {
        let tmp = tempfile::tempdir().unwrap();
        let store = ContentStore::new(tmp.path().to_path_buf());
        let id: Ulid = "01ARZ3NDEKTSV4RRFFQ69G5FAV".parse().unwrap();
        assert!(store.entry_mtime(id).is_none());
    }

    #[test]
    fn attachment_write_then_read_roundtrips_bytes() {
        let tmp = tempfile::tempdir().unwrap();
        let store = ContentStore::new(tmp.path().to_path_buf());
        let id: Ulid = "01ARZ3NDEKTSV4RRFFQ69G5FAV".parse().unwrap();
        // entry dir must exist for attachment writes — create via write_entry first
        let doc = sample_doc();
        store.write_entry(id, &doc).unwrap();

        let data = b"\x89PNG\r\n\x1a\nfake-png-bytes\xff\x00\x7f";
        store.write_attachment(id, "sunset.png", data).unwrap();

        let got = store.read_attachment(id, "sunset.png").unwrap();
        assert_eq!(got, data);
    }

    #[test]
    fn list_attachments_excludes_nomai_and_sorts() {
        let tmp = tempfile::tempdir().unwrap();
        let store = ContentStore::new(tmp.path().to_path_buf());
        let id: Ulid = "01ARZ3NDEKTSV4RRFFQ69G5FAV".parse().unwrap();
        store.write_entry(id, &sample_doc()).unwrap();

        store.write_attachment(id, "zebra.png", b"z").unwrap();
        store.write_attachment(id, "alpha.pdf", b"a").unwrap();

        let list = store.list_attachments(id).unwrap();
        let names: Vec<&str> = list.iter().map(|m| m.filename.as_str()).collect();
        assert_eq!(
            names,
            vec!["alpha.pdf", "zebra.png"],
            "sorted, no entry.nomai"
        );
        assert!(list.iter().all(|m| m.size >= 1));
    }

    #[test]
    fn list_attachments_missing_entry_dir_is_empty_not_error() {
        let tmp = tempfile::tempdir().unwrap();
        let store = ContentStore::new(tmp.path().to_path_buf());
        let id: Ulid = "01ARZ3NDEKTSV4RRFFQ69G5FAV".parse().unwrap();
        // no write_entry — dir absent
        let list = store.list_attachments(id).unwrap();
        assert!(list.is_empty());
    }

    #[test]
    fn attachment_filename_traversal_rejected() {
        let tmp = tempfile::tempdir().unwrap();
        let store = ContentStore::new(tmp.path().to_path_buf());
        let id: Ulid = "01ARZ3NDEKTSV4RRFFQ69G5FAV".parse().unwrap();
        store.write_entry(id, &sample_doc()).unwrap();

        for bad in [
            "../evil",
            "/etc/passwd",
            "~/secret",
            "a/b",
            "a\\b",
            ".",
            "..",
            "",
            "a:b",
        ] {
            let err = store.write_attachment(id, bad, b"x").unwrap_err();
            assert!(
                matches!(err, CoreError::Validation(ref m) if m.contains("unsafe attachment filename")),
                "expected Validation for {bad:?}, got {err:?}"
            );
        }
    }

    #[test]
    fn attachment_read_missing_file_is_validation_not_found() {
        let tmp = tempfile::tempdir().unwrap();
        let store = ContentStore::new(tmp.path().to_path_buf());
        let id: Ulid = "01ARZ3NDEKTSV4RRFFQ69G5FAV".parse().unwrap();
        store.write_entry(id, &sample_doc()).unwrap();

        let err = store.read_attachment(id, "nope.png").unwrap_err();
        assert!(
            matches!(err, CoreError::Validation(ref m) if m.contains("attachment not found")),
            "expected Validation('attachment not found'), got {err:?}"
        );
    }

    #[test]
    fn attachment_overwrite_replaces_bytes() {
        let tmp = tempfile::tempdir().unwrap();
        let store = ContentStore::new(tmp.path().to_path_buf());
        let id: Ulid = "01ARZ3NDEKTSV4RRFFQ69G5FAV".parse().unwrap();
        store.write_entry(id, &sample_doc()).unwrap();

        store.write_attachment(id, "f.bin", b"v1").unwrap();
        store
            .write_attachment(id, "f.bin", b"\x00\x01\x02\xff")
            .unwrap();
        let got = store.read_attachment(id, "f.bin").unwrap();
        assert_eq!(got, b"\x00\x01\x02\xff", "overwrite wins, binary-safe");
    }
}
