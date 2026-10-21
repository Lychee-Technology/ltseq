//! I/O operations for LTSeqTable
//!
//! This module contains helper functions for file I/O operations including:
//! - CSV writing (via Arrow's CSV writer)
//! - Parquet writing (via Arrow's Parquet writer)
//! - Arrow interop over the C Data Interface (`from_arrow`, `to_arrow`,
//!   `__arrow_c_stream__`), see `crate::arrow_ffi`
//!
//! Every entry point here runs with the GIL released (`lib.rs` wraps them in
//! `gil::detached`): they take only plain Rust values and report failures as
//! `LtseqError`.

use std::env;
use std::fs::{self, File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{DataType, Schema, SchemaRef};
use tempfile::NamedTempFile;

use crate::arrow_ffi::{
    check_requested_schema, collected_reader, BoxedBatchReader, DataFrameBatchReader,
    ImportedStream,
};
use crate::engine::RUNTIME;
use crate::error::LtseqError;
use crate::LTSeqTable;

/// The file a writer fills for `path` (#259).
///
/// A regular file, or a path that does not exist yet, is written through a
/// temporary file in the same directory, which replaces `path` only on
/// [`Output::commit`]. Dropping an uncommitted `Output`, after an error or
/// while a writer's panic unwinds, deletes the temporary file, so a failed
/// write leaves `path` as it was.
///
/// What the rename gives, and its limits:
///
/// - A reader opening `path` gets the old file or the new one, whole; one
///   that has the old file open keeps reading it. Of two writes to the
///   same path, each renames a complete file and the last one wins.
/// - The data reaches storage before the rename ([`sync`]), so after a
///   crash `path` holds the old contents or the new ones, never part of
///   them, where the filesystem keeps its fsync promise. The directory is
///   not flushed after the rename, so after a power loss a write that had
///   returned can read as the old file again. A process killed during the
///   write leaves `path` as it was, and its temporary file behind.
/// - Paths are resolved when the write starts. Renaming, replacing or
///   retargeting the destination's directory or links while it runs can
///   make the rename fail, leaving the temporary file behind, or put the
///   file where the link pointed at the start. Nothing here defends
///   against another process changing them on purpose.
/// - A regular file that is itself a mount point, such as a single-file
///   bind mount, cannot be renamed over either, but is not written in
///   place: the rename fails with EBUSY after the whole write and leaves
///   `path` as it was, where `File::create` overwrote it.
enum Output {
    Replace {
        tmp: NamedTempFile,
        dest: PathBuf,
    },
    /// What a rename cannot replace, written directly: a device or FIFO
    /// (`/dev/null`, `/dev/stdout` on a terminal or a pipe), which has no
    /// contents to keep, a file reached through a /proc descriptor link
    /// whose text does not name it, such as a deleted file's, or a macOS
    /// /dev/fd descriptor. None of it is atomic: a failed write leaves what
    /// was written before the error.
    InPlace(File),
}

/// The absolute path a write to `path` creates when nothing exists there.
///
/// It is absolute: the rename comes after the plan has run with the GIL
/// released, and by then another thread may have changed the working
/// directory. Links are followed by hand, as `File::create` followed them,
/// so that the rename creates the target of a link that points to nothing
/// yet; `canonicalize` fails on such a link.
fn new_file(path: &str) -> io::Result<PathBuf> {
    // Joined to the working directory, "" would name that directory.
    if path.is_empty() {
        return Err(io::ErrorKind::NotFound.into());
    }
    let path = Path::new(path);
    let mut dest = if path.is_absolute() {
        path.to_path_buf()
    } else {
        env::current_dir()?.join(path)
    };
    // The kernel found nothing at the end of the chain, so the chain is
    // within its limit of 40 links; the bound only matters if the links
    // change meanwhile.
    for _ in 0..40 {
        let Ok(target) = fs::read_link(&dest) else {
            break;
        };
        // A relative target is relative to the link's directory.
        dest.pop();
        dest.push(target);
    }
    Ok(dest)
}

/// Whether `path` names the file `meta` describes.
#[cfg(unix)]
fn same_file(meta: &fs::Metadata, path: &Path) -> bool {
    use std::os::unix::fs::MetadataExt;
    fs::metadata(path).is_ok_and(|other| (other.dev(), other.ino()) == (meta.dev(), meta.ino()))
}

#[cfg(not(unix))]
fn same_file(_: &fs::Metadata, _: &Path) -> bool {
    true
}

/// Whether `dest`, where `path` resolves, is a macOS /dev/fd node, which
/// stands for the file a descriptor has open. macOS's realpath gives such a
/// node `/dev/fd/` and the open file's own name, which names nothing unless
/// that name is a descriptor's number, as for standard output redirected to
/// a file named `1`. Nothing can be created beside the node, and opening it
/// duplicates the descriptor, as `File::create` did.
fn is_descriptor(dest: &Path) -> bool {
    cfg!(target_os = "macos") && dest.parent() == Some(Path::new("/dev/fd"))
}

/// Flush `file`'s data and metadata to storage.
fn sync(file: &File) -> io::Result<()> {
    // On macOS `sync_all` is F_FULLFSYNC, which also flushes the drive's
    // cache. fcntl(2) lists it for APFS, HFS, FAT and UDF only, and another
    // filesystem can refuse it as unsupported; fsync is what that one
    // offers. Any other error stands: a second flush after a failed one can
    // report success for data that was lost.
    #[cfg(target_os = "macos")]
    if let Err(e) = file.sync_all() {
        use std::os::unix::io::AsRawFd;
        if !matches!(
            e.raw_os_error(),
            Some(libc::ENOTSUP | libc::ENOTTY | libc::EINVAL)
        ) {
            return Err(e);
        }
        // SAFETY: the descriptor is open for as long as `file` is.
        if unsafe { libc::fsync(file.as_raw_fd()) } != 0 {
            return Err(io::Error::last_os_error());
        }
        return Ok(());
    }
    #[cfg(not(target_os = "macos"))]
    file.sync_all()?;
    Ok(())
}

/// An empty temporary file in `dir` that will replace an existing file
/// (`replaces`) or create a new one.
///
/// It never grants access the file at the destination will not: a
/// descriptor opened on it keeps its access after a chmod. A new file gets
/// `File::create`'s mode (0o666 less the umask) and its directory's
/// inherited ACL entries from the start. A replacement starts owner-only,
/// tempfile's default, until [`keep_access`] gives it the old file's
/// access; on macOS it also starts without inherited entries, whose allow
/// entries would apply whatever the mode.
fn temporary(dir: &Path, replaces: bool) -> io::Result<NamedTempFile> {
    // Hidden, short whatever the destination's name, and without its
    // extension, so that a reader listing the directory for `.csv` or
    // `.parquet` files skips it.
    let mut builder = tempfile::Builder::new();
    builder.prefix(".ltseq-").suffix(".tmp");
    #[cfg(target_os = "macos")]
    if replaces {
        return builder.make_in(dir, acl::create_uninherited);
    }
    #[cfg(unix)]
    if !replaces {
        use std::os::unix::fs::PermissionsExt;
        builder.permissions(fs::Permissions::from_mode(0o666));
    }
    #[cfg(not(unix))]
    let _ = replaces;
    builder.tempfile_in(dir)
}

/// Who may open the file a write replaces, read from a handle on it.
#[cfg(unix)]
struct Access {
    mode: u32,
    gid: u32,
    /// Its ACL: the POSIX access ACL on Linux, the extended ACL on macOS.
    acl: Option<acl::Acl>,
}

#[cfg(unix)]
impl Access {
    fn of(file: &File) -> io::Result<Self> {
        use std::os::unix::fs::MetadataExt;
        let meta = file.metadata()?;
        Ok(Access {
            mode: meta.mode(),
            gid: meta.gid(),
            acl: acl::get(file)?,
        })
    }
}

/// Give `file`, an owner-only temporary file, the access of the file it
/// replaces: its group, its ACL, then its mode, as truncating that file in
/// place kept them.
///
/// A writer who cannot give it that group, one they are not a member of,
/// leaves it owner-only: the group bits and the ACL's group entry would
/// otherwise grant the writer's own group what they granted the old file's.
/// The ACL is set before the mode, which would otherwise widen the mask of
/// an ACL the file inherited from its directory's default ACL.
#[cfg(unix)]
fn keep_access(file: &File, old: &Access) -> io::Result<()> {
    use std::os::unix::fs::{fchown, MetadataExt, PermissionsExt};
    let (mut mode, mut acl) = (old.mode & 0o7777, old.acl.as_ref());
    if file.metadata()?.gid() != old.gid && fchown(file, None, Some(old.gid)).is_err() {
        mode &= 0o700;
        acl = None;
    }
    acl::set(file, acl)?;
    file.set_permissions(fs::Permissions::from_mode(mode))
}

/// POSIX access ACLs, which Linux keeps in an extended attribute. A file
/// renamed into place has none of the old file's, and may have one it
/// inherited from its directory's default ACL.
#[cfg(target_os = "linux")]
mod acl {
    use std::fs::File;
    use std::io;

    use rustix::fs::{fgetxattr, fremovexattr, fsetxattr, XattrFlags};
    use rustix::io::Errno;

    const NAME: &str = "system.posix_acl_access";

    /// An ACL in the form the kernel stores it.
    pub(super) type Acl = Vec<u8>;

    /// The ACL of `file`; `None` when it has only its mode, or its
    /// filesystem has no ACLs.
    pub(super) fn get(file: &File) -> io::Result<Option<Acl>> {
        // The largest value an extended attribute can have (XATTR_SIZE_MAX).
        let mut value = vec![0; 65536];
        match fgetxattr(file, NAME, &mut value[..]) {
            Ok(len) => {
                value.truncate(len);
                Ok(Some(value))
            }
            Err(Errno::NODATA | Errno::NOTSUP) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Make `acl` the ACL of `file`; `None` leaves it only its mode.
    pub(super) fn set(file: &File, acl: Option<&Acl>) -> io::Result<()> {
        match acl {
            Some(acl) => fsetxattr(file, NAME, acl, XattrFlags::empty()).map_err(Into::into),
            None => match fremovexattr(file, NAME) {
                Ok(()) | Err(Errno::NODATA | Errno::NOTSUP) => Ok(()),
                Err(e) => Err(e.into()),
            },
        }
    }
}

/// Extended ACLs on macOS. A file renamed into place has none of the old
/// file's, and may have entries it inherited from its directory. Unlike a
/// POSIX ACL's, their allow entries apply whatever the mode, so a file
/// created owner-only is open to the users an inherited entry names.
#[cfg(target_os = "macos")]
mod acl {
    use std::ffi::{c_char, c_int, c_void, CString};
    use std::fs::File;
    use std::io;
    use std::os::unix::ffi::OsStrExt;
    use std::os::unix::io::{AsRawFd, FromRawFd};
    use std::path::Path;

    // From <sys/acl.h> and <sys/fcntl.h>; libSystem exports the functions.
    type AclT = *mut c_void;
    type FilesecT = *mut c_void;
    type AclFlagsetT = *mut c_void;
    const ACL_TYPE_EXTENDED: c_int = 0x100;
    const ACL_FLAG_NO_INHERIT: c_int = 1 << 17;
    const FILESEC_MODE: c_int = 4;
    const FILESEC_ACL: c_int = 5;
    const FILESEC_REMOVE_ACL: *const c_void = std::ptr::without_provenance(1);

    extern "C" {
        fn acl_init(count: c_int) -> AclT;
        fn acl_free(obj: *mut c_void) -> c_int;
        fn acl_get_fd_np(fd: c_int, ty: c_int) -> AclT;
        fn acl_set_fd_np(fd: c_int, acl: AclT, ty: c_int) -> c_int;
        fn acl_get_flagset_np(obj: *mut c_void, flagset: *mut AclFlagsetT) -> c_int;
        fn acl_add_flag_np(flagset: AclFlagsetT, flag: c_int) -> c_int;
        fn filesec_init() -> FilesecT;
        fn filesec_free(fsec: FilesecT);
        fn filesec_set_property(fsec: FilesecT, property: c_int, value: *const c_void) -> c_int;
        fn fchmodx_np(fd: c_int, fsec: FilesecT) -> c_int;
        fn openx_np(path: *const c_char, flags: c_int, fsec: FilesecT) -> c_int;
    }

    /// An ACL, freed on drop.
    pub(super) struct Acl(AclT);

    impl Drop for Acl {
        fn drop(&mut self) {
            // SAFETY: the pointer came from acl_init or acl_get_fd_np, and
            // only this value frees it.
            unsafe { acl_free(self.0) };
        }
    }

    /// A file's security attributes to set, freed on drop.
    struct Filesec(FilesecT);

    impl Filesec {
        fn new() -> io::Result<Self> {
            // SAFETY: filesec_init takes no arguments; null means failure.
            let fsec = unsafe { filesec_init() };
            if fsec.is_null() {
                return Err(io::Error::last_os_error());
            }
            Ok(Filesec(fsec))
        }

        /// Set `property` from `value`, which points to the property's
        /// type; the filesec copies it.
        fn set(&self, property: c_int, value: *const c_void) -> io::Result<()> {
            // SAFETY: `self.0` is a live filesec, and the callers pass a
            // pointer to the type `property` takes, or FILESEC_REMOVE_ACL.
            if unsafe { filesec_set_property(self.0, property, value) } != 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        }
    }

    impl Drop for Filesec {
        fn drop(&mut self) {
            // SAFETY: the pointer came from filesec_init, and only this
            // value frees it.
            unsafe { filesec_free(self.0) };
        }
    }

    /// The ACL of `file`; `None` when it has only its mode, or its
    /// filesystem has no ACLs.
    pub(super) fn get(file: &File) -> io::Result<Option<Acl>> {
        // SAFETY: the descriptor is open for as long as `file` is.
        let acl = unsafe { acl_get_fd_np(file.as_raw_fd(), ACL_TYPE_EXTENDED) };
        if !acl.is_null() {
            return Ok(Some(Acl(acl)));
        }
        let e = io::Error::last_os_error();
        match e.raw_os_error() {
            Some(libc::ENOENT | libc::ENOATTR | libc::ENOTSUP) => Ok(None),
            _ => Err(e),
        }
    }

    /// Make `acl` the ACL of `file`, its inherited entries included; `None`
    /// leaves it only its mode.
    pub(super) fn set(file: &File, acl: Option<&Acl>) -> io::Result<()> {
        let fd = file.as_raw_fd();
        let done = match acl {
            // SAFETY: the descriptor is open and `acl.0` is a live ACL.
            Some(acl) => unsafe { acl_set_fd_np(fd, acl.0, ACL_TYPE_EXTENDED) },
            None => {
                let fsec = Filesec::new()?;
                fsec.set(FILESEC_ACL, FILESEC_REMOVE_ACL)?;
                // SAFETY: the descriptor is open and `fsec` is live.
                unsafe { fchmodx_np(fd, fsec.0) }
            }
        };
        if done == 0 {
            return Ok(());
        }
        let e = io::Error::last_os_error();
        match (acl, e.raw_os_error()) {
            (None, Some(libc::ENOTSUP)) => Ok(()),
            _ => Err(e),
        }
    }

    /// Create `path`, which must not exist, owner-only and without the
    /// entries its directory passes to new files.
    ///
    /// An initial ACL flagged no-inherit is what turns inheritance off
    /// (`kauth_acl_inherit` in XNU); [`set`] then replaces it.
    pub(super) fn create_uninherited(path: &Path) -> io::Result<File> {
        let path = CString::new(path.as_os_str().as_bytes())?;
        // SAFETY: acl_init takes a capacity; null means failure.
        let acl = unsafe { acl_init(1) };
        if acl.is_null() {
            return Err(io::Error::last_os_error());
        }
        let acl = Acl(acl);
        let mut flags: AclFlagsetT = std::ptr::null_mut();
        // SAFETY: `acl.0` is a live ACL, and the flagset points into it.
        let flagged = unsafe {
            acl_get_flagset_np(acl.0, &mut flags) == 0
                && acl_add_flag_np(flags, ACL_FLAG_NO_INHERIT) == 0
        };
        if !flagged {
            return Err(io::Error::last_os_error());
        }
        let fsec = Filesec::new()?;
        let mode: libc::mode_t = 0o600;
        fsec.set(FILESEC_MODE, (&mode as *const libc::mode_t).cast())?;
        fsec.set(FILESEC_ACL, (&acl.0 as *const AclT).cast())?;
        let flags = libc::O_RDWR | libc::O_CREAT | libc::O_EXCL | libc::O_CLOEXEC;
        // SAFETY: `path` is a NUL-terminated string and `fsec` is live.
        let fd = unsafe { openx_np(path.as_ptr(), flags, fsec.0) };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: openx_np returned a new descriptor that nothing else owns.
        Ok(unsafe { File::from_raw_fd(fd) })
    }
}

/// Elsewhere a replacement does not carry ACLs over.
#[cfg(all(unix, not(any(target_os = "linux", target_os = "macos"))))]
mod acl {
    use std::fs::File;
    use std::io;

    pub(super) enum Acl {}

    pub(super) fn get(_: &File) -> io::Result<Option<Acl>> {
        Ok(None)
    }

    pub(super) fn set(_: &File, _: Option<&Acl>) -> io::Result<()> {
        Ok(())
    }
}

impl Output {
    fn create(path: &str) -> Result<Self, LtseqError> {
        let fail = |e: io::Error| LtseqError::io(&format!("Failed to create file '{}'", path), e);
        let in_place = || File::create(path).map(Output::InPlace).map_err(fail);

        // What `path` opens, as the kernel resolves it, before any link's
        // text is read. A /proc descriptor link (`/dev/stdout` leads to one)
        // resolves to the file it holds open, but its text can be `pipe:[…]`
        // or `/x/out (deleted)`, which name no file. So an existing file is
        // replaced at the path `canonicalize` gives only if that path is the
        // same file, and not a macOS /dev/fd node standing for it.
        let (dest, replaces) = match fs::metadata(path) {
            Err(e) if e.kind() == io::ErrorKind::NotFound => (new_file(path).map_err(fail)?, false),
            Err(e) => return Err(fail(e)),
            Ok(meta) if meta.is_dir() => return Err(fail(io::ErrorKind::IsADirectory.into())),
            Ok(meta) if !meta.is_file() => return in_place(),
            Ok(meta) => match fs::canonicalize(path) {
                Ok(dest) if same_file(&meta, &dest) && !is_descriptor(&dest) => (dest, true),
                _ => return in_place(),
            },
        };
        // The file being replaced, opened for writing although a rename needs
        // write access to the directory only: that keeps the refusal
        // `File::create` gave for a file the caller cannot write.
        let old = replaces
            .then(|| OpenOptions::new().write(true).open(&dest))
            .transpose()
            .map_err(fail)?;

        // Only a root has no parent, and it is a directory.
        let dir = dest
            .parent()
            .ok_or_else(|| fail(io::ErrorKind::IsADirectory.into()))?;
        let tmp = temporary(dir, old.is_some()).map_err(fail)?;
        #[cfg(unix)]
        if let Some(old) = &old {
            let access = Access::of(old).map_err(fail)?;
            keep_access(tmp.as_file(), &access).map_err(fail)?;
        }
        Ok(Output::Replace { tmp, dest })
    }

    fn file(&mut self) -> &mut File {
        match self {
            Output::Replace { tmp, .. } => tmp.as_file_mut(),
            Output::InPlace(file) => file,
        }
    }

    /// Make what was written the contents of `path`.
    fn commit(self, path: &str) -> Result<(), LtseqError> {
        let Output::Replace { tmp, dest } = self else {
            return Ok(());
        };
        let fail = |e: io::Error| LtseqError::io(&format!("Failed to replace '{}'", path), e);
        // On disk before the rename, so that a crash cannot leave `path`
        // naming a file whose data was never written.
        sync(tmp.as_file()).map_err(fail)?;
        tmp.persist(&dest).map_err(|e| fail(e.error))?;
        Ok(())
    }
}

/// Write table data to a CSV file using Arrow's native CSV writer.
///
/// The write is atomic (see [`Output`]).
///
/// Args:
///     table: Reference to LTSeqTable
///     path: Path to the output CSV file
pub fn write_csv_impl(table: &LTSeqTable, path: String) -> Result<(), LtseqError> {
    let df = table.require_df()?;
    let mut output = Output::create(&path)?;

    RUNTIME.block_on(async {
        use datafusion::arrow::csv::WriterBuilder;

        let df_clone = (**df).clone();
        let batches = df_clone.collect().await.map_err(LtseqError::collect)?;

        let builder = WriterBuilder::new().with_header(true);
        let mut writer = builder.build(output.file());

        for batch in &batches {
            writer.write(batch).map_err(|e| {
                LtseqError::Io(format!("Failed to write CSV batch: {}", e))
            })?;
        }

        Ok::<_, LtseqError>(())
    })?;

    output.commit(&path)
}

/// Refuse, before anything is written, a column Parquet cannot store: a
/// `struct` without fields, which the parquet crate rejects only once the
/// destination is open, and a `fixed_size_binary(0)`, on which it panics
/// (#259). Both are refused at any depth of nesting.
fn check_parquet_types(schema: &Schema) -> Result<(), LtseqError> {
    for field in schema.fields() {
        if let Some(reason) = unwritable_in_parquet(field.data_type()) {
            return Err(LtseqError::TypeMismatch(format!(
                "write_parquet cannot write column '{}' ({}): Parquet cannot store {}",
                field.name(),
                field.data_type(),
                reason
            )));
        }
    }
    Ok(())
}

/// What makes `data_type`, or a type nested in it, unwritable to Parquet.
fn unwritable_in_parquet(data_type: &DataType) -> Option<&'static str> {
    match data_type {
        DataType::FixedSizeBinary(0) => Some("a fixed_size_binary of width 0"),
        DataType::Struct(fields) if fields.is_empty() => Some("a struct without fields"),
        DataType::Struct(fields) => fields
            .iter()
            .find_map(|field| unwritable_in_parquet(field.data_type())),
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::ListView(field)
        | DataType::LargeListView(field)
        | DataType::FixedSizeList(field, _)
        | DataType::Map(field, _) => unwritable_in_parquet(field.data_type()),
        DataType::Dictionary(_, values) => unwritable_in_parquet(values),
        DataType::RunEndEncoded(_, values) => unwritable_in_parquet(values.data_type()),
        // #270 refuses a union itself; until then its members are checked.
        DataType::Union(fields, _) => fields
            .iter()
            .find_map(|(_, field)| unwritable_in_parquet(field.data_type())),
        _ => None,
    }
}

/// Write table data to a Parquet file using Arrow's native Parquet writer.
///
/// Batches are streamed from DataFusion and written incrementally, avoiding
/// full in-memory materialization before the first write. The write is
/// atomic (see [`Output`]), so a plan may read the file it replaces.
///
/// Args:
///     table: Reference to LTSeqTable
///     path: Path to the output Parquet file
///     compression: Compression algorithm name (e.g. "snappy", "zstd", "gzip", "lz4", "none")
pub fn write_parquet_impl(
    table: &LTSeqTable,
    path: String,
    compression: Option<String>,
) -> Result<(), LtseqError> {
    let df = table.require_df()?;

    RUNTIME.block_on(async {
        use datafusion::arrow::datatypes::Schema as ArrowSchema;
        use futures_util::StreamExt;
        use parquet::arrow::ArrowWriter;
        use parquet::basic::Compression;
        use parquet::file::properties::WriterProperties;

        let df_clone = (**df).clone();

        // Extract Arrow schema before consuming the DataFrame
        let df_schema = df_clone.schema();
        let arrow_fields: Vec<_> = df_schema.fields().iter().map(|f| (**f).clone()).collect();
        let schema = Arc::new(ArrowSchema::new(arrow_fields));
        check_parquet_types(&schema)?;

        let comp = match compression.as_deref() {
            Some("snappy") => Compression::SNAPPY,
            Some("zstd") | Some("zstandard") => Compression::ZSTD(Default::default()),
            Some("gzip") | Some("gz") => Compression::GZIP(Default::default()),
            Some("lz4") => Compression::LZ4,
            Some("none") | None => Compression::UNCOMPRESSED,
            Some(other) => {
                return Err(LtseqError::Validation(format!(
                    "Unknown compression '{}'. Use 'snappy', 'zstd', 'gzip', 'lz4', or 'none'.",
                    other
                )));
            }
        };

        let props = WriterProperties::builder()
            .set_compression(comp)
            .build();

        let mut output = Output::create(&path)?;

        // Stream batches from DataFusion directly into the Parquet writer
        let mut stream = df_clone
            .execute_stream()
            .await
            .map_err(|e| LtseqError::Runtime(format!("Failed to create stream: {}", e)))?;

        let mut writer = ArrowWriter::try_new(output.file(), schema, Some(props))
            .map_err(|e| LtseqError::Io(format!("Failed to create Parquet writer: {}", e)))?;

        let mut wrote_any = false;
        while let Some(batch_result) = stream.next().await {
            let batch = batch_result.map_err(|e| {
                LtseqError::Io(format!("Failed to read batch: {}", e))
            })?;
            if batch.num_rows() > 0 {
                writer.write(&batch).map_err(|e| {
                    LtseqError::Io(format!("Failed to write Parquet batch: {}", e))
                })?;
                wrote_any = true;
            }
        }

        if !wrote_any {
            return Err(LtseqError::Validation("No data to write".into()));
        }

        writer.close().map_err(|e| {
            LtseqError::Io(format!("Failed to close Parquet writer: {}", e))
        })?;

        output.commit(&path)
    })
}

/// Build a table from a stream imported over the Arrow C Data Interface.
///
/// The stream comes from any Python object implementing `__arrow_c_stream__`
/// (`pyarrow.Table`, `pyarrow.RecordBatch`, `pyarrow.RecordBatchReader`, ...).
/// Its buffers are shared, not copied. A stream with a schema but no batches
/// yields a table backed by one empty batch, so the result has a real plan
/// and the schema survives (matching a `pyarrow.Table` with zero rows).
pub fn from_arrow_impl(stream: ImportedStream) -> Result<LTSeqTable, LtseqError> {
    use crate::engine::create_session_context;

    let (schema, mut batches) = stream.read_all()?;
    if batches.is_empty() {
        batches.push(RecordBatch::new_empty(Arc::clone(&schema)));
    }
    LTSeqTable::from_batches(create_session_context(), batches, schema, Vec::new())
}

/// Schema of a table that may have no plan (an unloaded `LTSeq()` has neither).
fn schema_or_empty(table: &LTSeqTable) -> SchemaRef {
    table
        .schema
        .clone()
        .unwrap_or_else(|| Arc::new(Schema::empty()))
}

/// Execute the plan and return a reader over the collected batches.
///
/// This is the `to_arrow()` path: it materializes (ADR 0004 terminal
/// boundary) and hands the batches to pyarrow without re-encoding them. A
/// table with no plan yields an empty reader carrying its schema, if any.
pub fn collect_arrow_reader_impl(table: &LTSeqTable) -> Result<BoxedBatchReader, LtseqError> {
    let Some(df) = table.dataframe.as_ref() else {
        return Ok(collected_reader(schema_or_empty(table), Vec::new()));
    };
    let batches = RUNTIME
        .block_on((**df).clone().collect())
        .map_err(LtseqError::collect)?;
    // Batches from one execution share a schema; prefer it over the logical
    // schema so nullability/metadata match what pyarrow will see per batch.
    let schema = batches
        .first()
        .map(|b| b.schema())
        .unwrap_or_else(|| schema_or_empty(table));
    Ok(collected_reader(schema, batches))
}

/// Start executing the plan as a stream and return a lazy reader over it.
///
/// This is the `__arrow_c_stream__` path. Planning happens here (so planning
/// errors surface to the caller); batches are pulled by the consumer through
/// `DataFrameBatchReader`. Each call executes the plan anew and leaves the
/// table untouched.
///
/// `requested_schema` is the consumer's schema request, already imported
/// from its capsule. The stream always carries the table's own schema, which
/// the protocol allows for representation requests the producer does not
/// support; a request for different fields is rejected against the logical
/// schema before the plan is executed (see `arrow_ffi::check_requested_schema`).
pub fn arrow_stream_impl(
    table: &LTSeqTable,
    requested_schema: Option<&Schema>,
) -> Result<BoxedBatchReader, LtseqError> {
    let schema = schema_or_empty(table);
    if let Some(requested) = requested_schema {
        check_requested_schema(&schema, requested)?;
    }
    let Some(df) = table.dataframe.as_ref() else {
        return Ok(collected_reader(schema, Vec::new()));
    };
    let stream = RUNTIME
        .block_on((**df).clone().execute_stream())
        .map_err(|e| LtseqError::with_context("Failed to execute plan", e))?;
    Ok(Box::new(DataFrameBatchReader::new(stream)))
}

// The Python tests check what a write leaves behind. These check what they
// cannot see: the access a temporary file has until the next syscall, and a
// group the writer is not a member of, which only root can set up.
#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::os::unix::fs::MetadataExt;

    /// No other user can open a replacement's temporary file before it has
    /// the old file's access: whatever they opened would keep its access,
    /// and read what the writer writes next.
    #[test]
    fn replacement_starts_owner_only() {
        let dir = tempfile::tempdir().expect("temporary directory");
        let tmp = temporary(dir.path(), true).expect("temporary file");
        let mode = tmp.as_file().metadata().expect("metadata").mode();
        assert_eq!(mode & 0o077, 0, "created with mode {:o}", mode & 0o777);
    }

    /// The group bits of a file whose group the writer cannot set would
    /// apply to the writer's own group instead.
    #[test]
    fn group_that_cannot_be_kept_leaves_owner_only() {
        let dir = tempfile::tempdir().expect("temporary directory");
        let tmp = temporary(dir.path(), true).expect("temporary file");
        let file = tmp.as_file();
        let before = file.metadata().expect("metadata");
        if before.uid() == 0 {
            eprintln!("skipped: root can give a file any group");
            return;
        }
        let old = Access {
            mode: 0o664,
            // A group no account is a member of.
            gid: 0x7fff_fff0,
            acl: None,
        };
        keep_access(file, &old).expect("keep_access");
        let after = file.metadata().expect("metadata");
        let mode = after.mode() & 0o7777;
        assert_eq!(
            (after.gid(), mode),
            (before.gid(), 0o600),
            "mode {:o}",
            mode
        );
    }

    /// On macOS an allow entry applies whatever the mode, so an owner-only
    /// replacement that took its directory's inherited entries could be
    /// opened by the users they name before it has the old file's ACL. A
    /// new file takes them, as `File::create`'s did.
    #[cfg(target_os = "macos")]
    #[test]
    fn replacement_inherits_no_directory_acl() {
        use std::process::Command;
        let dir = tempfile::tempdir().expect("temporary directory");
        let added = Command::new("chmod")
            .args(["+a", "everyone allow read,file_inherit"])
            .arg(dir.path())
            .status()
            .expect("chmod");
        assert!(added.success());
        // `ls -le` lists one line per ACL entry after the file's own.
        let entries = |tmp: &NamedTempFile| {
            let ls = Command::new("ls")
                .arg("-led")
                .arg(tmp.path())
                .output()
                .expect("ls");
            String::from_utf8_lossy(&ls.stdout).lines().count() - 1
        };
        let new = temporary(dir.path(), false).expect("new file");
        assert_eq!(entries(&new), 1, "the directory's entry is not inherited");
        let replacement = temporary(dir.path(), true).expect("replacement");
        assert_eq!(entries(&replacement), 0);
    }
}
