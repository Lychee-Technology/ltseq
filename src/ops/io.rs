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
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::slice;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, ArrowNativeTypeOp, ArrowPrimitiveType, AsArray, BooleanBufferBuilder,
    DictionaryArray, GenericListArray, GenericListViewArray, OffsetSizeTrait, PrimitiveArray,
    RecordBatch,
};
use datafusion::arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use datafusion::arrow::datatypes::{
    ArrowDictionaryKeyType, ArrowNativeType, DataType, FieldRef, RunEndIndexType, Schema,
    SchemaRef, TimeUnit,
};
use datafusion::arrow::error::ArrowError;
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
///   same path, each renames a complete file and the last one wins. One
///   that finds `path` replaced while it checks what is there looks again
///   ([`destination`]), and fails without touching it if every look does.
/// - The data reaches storage before the rename ([`sync`]), so after a
///   crash `path` holds the old contents or the new ones, never part of
///   them, where the filesystem keeps its fsync promise. The directory is
///   not flushed after the rename, so after a power loss a write that had
///   returned can read as the old file again. A process killed during the
///   write leaves `path` as it was, and its temporary file behind.
/// - Paths are resolved when the write starts. Renaming, replacing or
///   retargeting the destination's directory or links, or taking away
///   write access to the directory, while it runs can make the rename
///   fail, leaving the temporary file behind, or put the file where the
///   link pointed at the start. Nothing here defends
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

/// Where a write to `path` goes, as found when it starts.
enum Destination {
    /// Nothing is there: a new file at this absolute path.
    New(PathBuf),
    /// A regular file, open for writing, replaced at this absolute path.
    Replace(File, PathBuf),
    /// What a rename cannot replace (see [`Output::InPlace`]).
    InPlace,
}

/// What `path` opens, as the kernel resolves it, before any link's text is
/// read; `None` if another write replaced it meanwhile.
///
/// A /proc descriptor link (`/dev/stdout` leads to one) resolves to the
/// file it holds open, but its text can be `pipe:[…]` or `/x/out (deleted)`,
/// which name no file. So a regular file is replaced at the path
/// `canonicalize` gives only if that path is the same file, and not a macOS
/// /dev/fd node standing for it. When it is not, `path` either is such a
/// link, and still opens the same file, or was replaced by another write
/// between the two looks, which only another look can tell from a link.
fn destination(path: &str) -> io::Result<Option<Destination>> {
    match fs::metadata(path) {
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            return Ok(Some(Destination::New(new_file(path)?)))
        }
        Err(e) => return Err(e),
        Ok(meta) if meta.is_dir() => return Err(io::ErrorKind::IsADirectory.into()),
        Ok(meta) if !meta.is_file() => return Ok(Some(Destination::InPlace)),
        Ok(_) => {}
    }
    // Opened for writing although a rename needs write access to the
    // directory only: that keeps the refusal `File::create` gave for a
    // file the caller cannot write. Held open, it also keeps its inode
    // number from going to another file while `path` is compared with it.
    let old = match OpenOptions::new().write(true).open(path) {
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        old => old?,
    };
    let meta = old.metadata()?;
    Ok(match fs::canonicalize(path) {
        Ok(dest) if is_descriptor(&dest) => Some(Destination::InPlace),
        Ok(dest) if same_file(&meta, &dest) => Some(Destination::Replace(old, dest)),
        _ if same_file(&meta, Path::new(path)) => Some(Destination::InPlace),
        _ => None,
    })
}

impl Output {
    fn create(path: &str) -> Result<Self, LtseqError> {
        let fail = |e: io::Error| LtseqError::io(&format!("Failed to create file '{}'", path), e);

        // Each write that replaces `path` while it is checked leaves a
        // whole file there, so looking again settles it. Writes that keep
        // landing inside those few system calls fail this one instead,
        // before it touches `path`.
        let (dest, old) = match (0..8)
            .find_map(|_| destination(path).transpose())
            .unwrap_or_else(|| Err(io::Error::other("it was replaced each time it was checked")))
            .map_err(fail)?
        {
            Destination::New(dest) => (dest, None),
            Destination::Replace(old, dest) => (dest, Some(old)),
            Destination::InPlace => return File::create(path).map(Output::InPlace).map_err(fail),
        };

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
    let df = table.ordered_df()?;
    let mut output = Output::create(&path)?;

    RUNTIME.block_on(async {
        use datafusion::arrow::csv::WriterBuilder;

        let batches = df.collect().await.map_err(LtseqError::collect)?;

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

/// The columns `write_parquet` stores in another unit than the table's
/// (#324).
///
/// Parquet has no second unit. The `parquet` crate writes a `timestamp` or
/// `time32` in seconds as a bare INT64 or INT32 with no logical type, which
/// only a reader that trusts the stored Arrow schema reads as a time, so
/// pyarrow, pandas and DuckDB read integers. The writer stores such
/// values in milliseconds instead, as pyarrow does, at any depth of nesting.
/// The conversion is `cast`'s exact one (contract §16.5, §17.6): a
/// `timestamp[s]` value whose milliseconds do not fit `int64` fails
/// ([`in_milliseconds`]).
struct ParquetUnits {
    /// The schema written, with each second unit in milliseconds.
    schema: SchemaRef,
    /// The type each column is converted to, for the columns that change.
    columns: Vec<Option<DataType>>,
}

impl ParquetUnits {
    /// `None` when `schema` holds no second unit and is written as it is.
    fn of(schema: &Schema) -> Option<Self> {
        let columns: Vec<_> = schema
            .fields()
            .iter()
            .map(|field| in_parquet_units(field.data_type()))
            .collect();
        if columns.iter().all(Option::is_none) {
            return None;
        }
        let schema = with_types(schema, &columns);
        Some(Self { schema, columns })
    }

    fn convert(&self, batch: &RecordBatch) -> Result<RecordBatch, LtseqError> {
        let schema = batch.schema();
        let columns = batch
            .columns()
            .iter()
            .zip(&self.columns)
            .zip(schema.fields())
            .map(|((column, to), field)| match to {
                Some(to) => in_milliseconds(column, to).map_err(|e| match e {
                    ArrowError::ArithmeticOverflow(_) => LtseqError::Validation(format!(
                        "write_parquet cannot write column '{}' ({}): Parquet has no \
                         second unit, so it is written in milliseconds: {}",
                        field.name(),
                        column.data_type(),
                        e
                    )),
                    e => LtseqError::runtime("Failed to convert a Parquet batch", e),
                }),
                None => Ok(Arc::clone(column)),
            })
            .collect::<Result<Vec<_>, _>>()?;
        // The batch's own schema, not `self.schema`: the writer never required
        // a batch to match the plan's schema exactly (in a nested field's
        // nullability, say), and `try_new` checks each column against it.
        RecordBatch::try_new(with_types(&schema, &self.columns), columns)
            .map_err(|e| LtseqError::runtime("Failed to convert a Parquet batch", e))
    }
}

/// `schema` with each column that `columns` gives a type converted to it.
fn with_types(schema: &Schema, columns: &[Option<DataType>]) -> SchemaRef {
    let fields: Vec<_> = schema
        .fields()
        .iter()
        .zip(columns)
        .map(|(field, to)| match to {
            Some(to) => Arc::new(field.as_ref().clone().with_data_type(to.clone())),
            None => Arc::clone(field),
        })
        .collect();
    Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()))
}

/// `data_type` with each second unit in it in milliseconds, or `None` when
/// it holds none. Nested fields keep their names, nullability and metadata.
fn in_parquet_units(data_type: &DataType) -> Option<DataType> {
    let field = |field: &FieldRef| {
        in_parquet_units(field.data_type())
            .map(|to| Arc::new(field.as_ref().clone().with_data_type(to)))
    };
    match data_type {
        DataType::Timestamp(TimeUnit::Second, tz) => {
            Some(DataType::Timestamp(TimeUnit::Millisecond, tz.clone()))
        }
        DataType::Time32(TimeUnit::Second) => Some(DataType::Time32(TimeUnit::Millisecond)),
        DataType::List(item) => field(item).map(DataType::List),
        DataType::LargeList(item) => field(item).map(DataType::LargeList),
        DataType::ListView(item) => field(item).map(DataType::ListView),
        DataType::LargeListView(item) => field(item).map(DataType::LargeListView),
        DataType::FixedSizeList(item, size) => {
            field(item).map(|item| DataType::FixedSizeList(item, *size))
        }
        DataType::Map(entries, sorted) => {
            field(entries).map(|entries| DataType::Map(entries, *sorted))
        }
        DataType::Struct(fields) => {
            let converted: Vec<_> = fields.iter().map(field).collect();
            converted.iter().any(Option::is_some).then(|| {
                DataType::Struct(
                    fields
                        .iter()
                        .zip(converted)
                        .map(|(old, new)| new.unwrap_or_else(|| Arc::clone(old)))
                        .collect(),
                )
            })
        }
        DataType::Dictionary(keys, values) => in_parquet_units(values)
            .map(|values| DataType::Dictionary(keys.clone(), Box::new(values))),
        DataType::RunEndEncoded(run_ends, values) => {
            field(values).map(|values| DataType::RunEndEncoded(Arc::clone(run_ends), values))
        }
        // A union makes the writer panic whatever its members (#270).
        _ => None,
    }
}

/// `array` in `to`, the type [`in_parquet_units`] gives for its type: all
/// its slots, as [`slots_in_milliseconds`] converts them.
fn in_milliseconds(array: &ArrayRef, to: &DataType) -> Result<ArrayRef, ArrowError> {
    slots_in_milliseconds(array, to, slice::from_ref(&(0..array.len())), None)
}

/// The `slots` of `array` in `to`, the type [`in_parquet_units`] gives for
/// its type: each second count times 1000, inside containers built with
/// `to`'s own fields. `slots` are ascending and disjoint.
///
/// Not Arrow's `cast`, for two reasons. A container holds values that no
/// row holds: under a null struct, list or map slot, outside a slice, around
/// or between list views, or in a dictionary entry no key uses. They can be
/// anything, and `cast` checks them all, so a table whose rows all fit would
/// fail to write. And `cast` gives a run-end encoded array default child
/// fields, which the batch's schema rejects when the table's differ.
///
/// `live` has a bit for each of the `slots`, in order, and marks those that a
/// row holds; all of them when `None`. Only the values in those slots are
/// checked and converted; the others become 0, and a list, map or list view
/// slot that is not live becomes empty, since no row reads them.
///
/// Each container passes its child only the slots its live slots reach. So
/// a batch costs the values its rows reach, not the child they are cut from:
/// a slice of a list, map, list view or run-end encoded array keeps its
/// parent's whole child, a slice of a dictionary array all its entries, and
/// so does each batch cut from one array; a null list or map slot can span
/// any part of the child, and list views can leave any gap between them.
fn slots_in_milliseconds(
    array: &ArrayRef,
    to: &DataType,
    slots: &[Range<usize>],
    live: Option<&NullBuffer>,
) -> Result<ArrayRef, ArrowError> {
    use datafusion::arrow::array::{FixedSizeListArray, MapArray, StructArray};
    use datafusion::arrow::datatypes::{
        Int16Type, Int32Type, Int64Type, Time32MillisecondType, Time32SecondType,
        TimestampMillisecondType, TimestampSecondType,
    };
    use datafusion::arrow::downcast_dictionary_array;

    if array.data_type() == to {
        if let [slots] = slots {
            return Ok(array.slice(slots.start, slots.len()));
        }
    }
    let len = slots.iter().map(Range::len).sum();
    let nulls = selected_nulls(array.nulls(), slots);
    let live = NullBuffer::union(nulls.as_ref(), live);
    match (array.data_type(), to) {
        (DataType::Timestamp(TimeUnit::Second, _), DataType::Timestamp(_, tz)) => {
            let converted = times_thousand::<TimestampSecondType, TimestampMillisecondType>(
                array, slots, nulls, live,
            )?;
            Ok(Arc::new(converted.with_timezone_opt(tz.clone())))
        }
        (DataType::Time32(TimeUnit::Second), _) => {
            let converted = times_thousand::<Time32SecondType, Time32MillisecondType>(
                array, slots, nulls, live,
            )?;
            Ok(Arc::new(converted))
        }
        (DataType::List(_), DataType::List(item)) => list::<i32>(array, item, slots, nulls, live),
        (DataType::LargeList(_), DataType::LargeList(item)) => {
            list::<i64>(array, item, slots, nulls, live)
        }
        (DataType::ListView(_), DataType::ListView(item)) => {
            list_view::<i32>(array, item, slots, nulls, live)
        }
        (DataType::LargeListView(_), DataType::LargeListView(item)) => {
            list_view::<i64>(array, item, slots, nulls, live)
        }
        (DataType::FixedSizeList(..), DataType::FixedSizeList(item, size)) => {
            let list = array.as_fixed_size_list();
            let per = list.value_length() as usize;
            let values: Vec<_> = slots
                .iter()
                .map(|slots| slots.start * per..slots.end * per)
                .collect();
            let live = live.map(|live| live.expand(per));
            let values =
                slots_in_milliseconds(list.values(), item.data_type(), &values, live.as_ref())?;
            let converted = FixedSizeListArray::try_new_with_length(
                Arc::clone(item),
                *size,
                values,
                nulls,
                len,
            )?;
            Ok(Arc::new(converted))
        }
        (DataType::Struct(_), DataType::Struct(fields)) => {
            let columns = array
                .as_struct()
                .columns()
                .iter()
                .zip(fields.iter())
                .map(|(column, field)| {
                    slots_in_milliseconds(column, field.data_type(), slots, live.as_ref())
                })
                .collect::<Result<Vec<_>, _>>()?;
            let converted = StructArray::try_new_with_length(fields.clone(), columns, nulls, len)?;
            Ok(Arc::new(converted))
        }
        (DataType::Map(..), DataType::Map(field, sorted)) => {
            let map = array.as_map();
            let (offsets, values) = reach(map.offsets(), slots, live.as_ref());
            let entries: ArrayRef = Arc::new(map.entries().clone());
            let entries = slots_in_milliseconds(&entries, field.data_type(), &values, None)?;
            let converted = MapArray::try_new(
                Arc::clone(field),
                offsets,
                entries.as_struct().clone(),
                nulls,
                *sorted,
            )?;
            Ok(Arc::new(converted))
        }
        (DataType::Dictionary(_, from), DataType::Dictionary(_, values)) if from != values => {
            let array = array.as_ref();
            downcast_dictionary_array!(
                array => dictionary(array, values, slots, nulls, live.as_ref()),
                other => Err(ArrowError::CastError(format!(
                    "no conversion from {other} to {to}"
                )))
            )
        }
        (DataType::RunEndEncoded(run_ends, _), DataType::RunEndEncoded(..)) => {
            match run_ends.data_type() {
                DataType::Int16 => run_end_encoded::<Int16Type>(array, to, slots, live.as_ref()),
                DataType::Int32 => run_end_encoded::<Int32Type>(array, to, slots, live.as_ref()),
                DataType::Int64 => run_end_encoded::<Int64Type>(array, to, slots, live.as_ref()),
                other => Err(ArrowError::InvalidArgumentError(format!(
                    "run ends of type {other}"
                ))),
            }
        }
        // A type that holds no second unit, cut to the slots. A dictionary
        // keeps its entries, which it shares rather than copies.
        (from, to) if from == to => selected(array, slots),
        (from, to) => Err(ArrowError::CastError(format!(
            "no conversion from {from} to {to}"
        ))),
    }
}

/// The values of `slots` of `array` times 1000, as `T`, with `nulls`. A
/// value in a `live` slot that does not fit fails with `ArithmeticOverflow`;
/// the other slots become 0.
fn times_thousand<F, T>(
    array: &ArrayRef,
    slots: &[Range<usize>],
    nulls: Option<NullBuffer>,
    live: Option<NullBuffer>,
) -> Result<PrimitiveArray<T>, ArrowError>
where
    F: ArrowPrimitiveType,
    T: ArrowPrimitiveType<Native = F::Native>,
{
    let seconds = selected_values(array.as_primitive::<F>().values(), slots);
    let thousand = F::Native::usize_as(1000);
    // `try_unary` computes only the slots its input's nulls leave valid.
    let held = PrimitiveArray::<F>::new(seconds, live);
    let converted = held.try_unary::<_, T, _>(|value| value.mul_checked(thousand))?;
    Ok(PrimitiveArray::new(converted.values().clone(), nulls))
}

fn list<O: OffsetSizeTrait>(
    array: &ArrayRef,
    item: &FieldRef,
    slots: &[Range<usize>],
    nulls: Option<NullBuffer>,
    live: Option<NullBuffer>,
) -> Result<ArrayRef, ArrowError> {
    let list = array.as_list::<O>();
    let (offsets, values) = reach(list.offsets(), slots, live.as_ref());
    let values = slots_in_milliseconds(list.values(), item.data_type(), &values, None)?;
    let converted = GenericListArray::<O>::try_new(Arc::clone(item), offsets, values, nulls)?;
    Ok(Arc::new(converted))
}

/// For a list or map with these `offsets`: the offsets of `slots`, where a
/// `live` slot keeps its values and any other is empty, and the slots of
/// the child those values are in.
fn reach<O: OffsetSizeTrait>(
    offsets: &OffsetBuffer<O>,
    slots: &[Range<usize>],
    live: Option<&NullBuffer>,
) -> (OffsetBuffer<O>, Vec<Range<usize>>) {
    let mut values = Vec::new();
    let mut ends = Vec::with_capacity(slots.iter().map(Range::len).sum::<usize>() + 1);
    ends.push(O::usize_as(0));
    let mut end = 0;
    for (at, slot) in slots.iter().cloned().flatten().enumerate() {
        if live.is_none_or(|live| live.is_valid(at)) {
            let (start, stop) = (offsets[slot].as_usize(), offsets[slot + 1].as_usize());
            push_merged(&mut values, start..stop);
            end += stop - start;
        }
        ends.push(O::usize_as(end));
    }
    (OffsetBuffer::new(ends.into()), values)
}

fn list_view<O: OffsetSizeTrait>(
    array: &ArrayRef,
    item: &FieldRef,
    slots: &[Range<usize>],
    nulls: Option<NullBuffer>,
    live: Option<NullBuffer>,
) -> Result<ArrayRef, ArrowError> {
    let list = array.as_list_view::<O>();
    let is_live = |at| live.as_ref().is_none_or(|live| live.is_valid(at));
    // Each slot's view, empty unless it is live.
    let views: Vec<(usize, usize)> = slots
        .iter()
        .cloned()
        .flatten()
        .enumerate()
        .map(|(at, slot)| match is_live(at) {
            true => (
                list.offsets()[slot].as_usize(),
                list.sizes()[slot].as_usize(),
            ),
            false => (0, 0),
        })
        .collect();
    // The values the views reach. Views may overlap, and come in any order.
    let mut reached: Vec<_> = views
        .iter()
        .filter(|&&(_, size)| size > 0)
        .map(|&(offset, size)| offset..offset + size)
        .collect();
    reached.sort_unstable_by_key(|values| values.start);
    let values = reached.into_iter().fold(Vec::new(), |mut values, view| {
        push_merged(&mut values, view);
        values
    });
    // Where each of those ranges starts among the values kept.
    let starts: Vec<usize> = values
        .iter()
        .scan(0, |start, values| {
            let at = *start;
            *start += values.len();
            Some(at)
        })
        .collect();
    let offsets: ScalarBuffer<O> = views
        .iter()
        .map(|&(offset, size)| match size {
            0 => O::usize_as(0),
            _ => {
                let range = values.partition_point(|values| values.start <= offset) - 1;
                O::usize_as(starts[range] + offset - values[range].start)
            }
        })
        .collect();
    let sizes: ScalarBuffer<O> = views.iter().map(|&(_, size)| O::usize_as(size)).collect();
    let values = slots_in_milliseconds(list.values(), item.data_type(), &values, None)?;
    let converted =
        GenericListViewArray::<O>::try_new(Arc::clone(item), offsets, sizes, values, nulls)?;
    Ok(Arc::new(converted))
}

/// The `slots` of `dictionary` with only the entries its `live` slots use,
/// in `to`. The keys of the other slots point at the first entry kept.
fn dictionary<K: ArrowDictionaryKeyType>(
    dictionary: &DictionaryArray<K>,
    to: &DataType,
    slots: &[Range<usize>],
    nulls: Option<NullBuffer>,
    live: Option<&NullBuffer>,
) -> Result<ArrayRef, ArrowError> {
    let keys = PrimitiveArray::<K>::new(selected_values(dictionary.keys().values(), slots), nulls);
    let values = dictionary.values();
    let mut used: Vec<usize> = keys
        .values()
        .iter()
        .enumerate()
        .filter(|&(at, _)| live.is_none_or(|live| live.is_valid(at)))
        .map(|(_, key)| key.as_usize())
        .collect();
    used.sort_unstable();
    used.dedup();
    if used.len() == values.len() {
        let values = in_milliseconds(values, to)?;
        return Ok(Arc::new(DictionaryArray::try_new(keys, values)?));
    }
    // With no entry used, the first is kept for the other slots' keys, and
    // becomes 0.
    let (used, held) = match used.is_empty() {
        true => (vec![0], Some(NullBuffer::new_null(1))),
        false => (used, None),
    };
    let entries = used.iter().fold(Vec::new(), |mut entries, &entry| {
        push_merged(&mut entries, entry..entry + 1);
        entries
    });
    let values = slots_in_milliseconds(values, to, &entries, held.as_ref())?;
    // A slot no row holds may have a key whose entry was not kept.
    let entry = |key: K::Native| used.binary_search(&key.as_usize()).unwrap_or(0);
    let keys = keys.unary::<_, K>(|key| K::Native::usize_as(entry(key)));
    Ok(Arc::new(DictionaryArray::try_new(keys, values)?))
}

/// The `slots` of a run-end encoded array, over the runs they fall in. The
/// value of a run that no `live` slot falls in becomes 0.
fn run_end_encoded<R: RunEndIndexType>(
    array: &ArrayRef,
    to: &DataType,
    slots: &[Range<usize>],
    live: Option<&NullBuffer>,
) -> Result<ArrayRef, ArrowError> {
    use datafusion::arrow::array::{make_array, ArrayData};

    let DataType::RunEndEncoded(_, values_field) = to else {
        return Err(ArrowError::CastError(format!(
            "no conversion from {} to {to}",
            array.data_type()
        )));
    };
    let runs = array.as_run::<R>();
    let ends = runs.run_ends();
    // For each run kept: where it ends among the slots, and whether a live
    // slot falls in it. A run that two of `slots` share is kept once.
    let (mut run_ends, mut held) = (Vec::new(), Vec::new());
    let mut values: Vec<Range<usize>> = Vec::new();
    let mut at = 0;
    for slots in slots {
        let (mut slot, mut run) = (slots.start, ends.get_physical_index(slots.start));
        while slot < slots.end {
            let end = (ends.values()[run].as_usize() - ends.offset()).min(slots.end);
            let len = end - slot;
            let is_held = live.is_none_or(|live| live.slice(at, len).null_count() < len);
            at += len;
            match values.last() {
                Some(last) if last.end == run + 1 => {
                    *run_ends.last_mut().expect("a run kept") = R::Native::usize_as(at);
                    *held.last_mut().expect("a run kept") |= is_held;
                }
                _ => {
                    run_ends.push(R::Native::usize_as(at));
                    push_merged(&mut values, run..run + 1);
                    held.push(is_held);
                }
            }
            (slot, run) = (end, run + 1);
        }
    }
    let held = NullBuffer::from(held);
    let values = slots_in_milliseconds(
        runs.values(),
        values_field.data_type(),
        &values,
        Some(&held),
    )?;
    // `to`'s fields over the runs kept.
    let data = ArrayData::builder(to.clone())
        .len(at)
        .add_child_data(PrimitiveArray::<R>::new(run_ends.into(), None).into_data())
        .add_child_data(values.to_data())
        .build()?;
    Ok(make_array(data))
}

/// Appends `values` to the ascending `ranges`, merged with the last range
/// when the two overlap or meet.
fn push_merged(ranges: &mut Vec<Range<usize>>, values: Range<usize>) {
    match ranges.last_mut() {
        _ if values.is_empty() => {}
        Some(last) if values.start <= last.end => last.end = last.end.max(values.end),
        _ => ranges.push(values),
    }
}

/// The values of `slots`, for an array with these `values`.
fn selected_values<T: ArrowNativeType>(
    values: &ScalarBuffer<T>,
    slots: &[Range<usize>],
) -> ScalarBuffer<T> {
    match slots {
        [slots] => values.slice(slots.start, slots.len()),
        _ => {
            let mut selected = Vec::with_capacity(slots.iter().map(Range::len).sum());
            for slots in slots {
                selected.extend_from_slice(&values[slots.clone()]);
            }
            selected.into()
        }
    }
}

/// The validity of `slots`, for an array with these `nulls`.
fn selected_nulls(nulls: Option<&NullBuffer>, slots: &[Range<usize>]) -> Option<NullBuffer> {
    let nulls = nulls?;
    if let [slots] = slots {
        return Some(nulls.slice(slots.start, slots.len()));
    }
    let mut selected = BooleanBufferBuilder::new(slots.iter().map(Range::len).sum());
    let offset = nulls.offset();
    for slots in slots {
        selected.append_packed_range(offset + slots.start..offset + slots.end, nulls.validity());
    }
    Some(NullBuffer::new(selected.finish()))
}

/// The `slots` of `array`, as they are.
fn selected(array: &ArrayRef, slots: &[Range<usize>]) -> Result<ArrayRef, ArrowError> {
    use datafusion::arrow::array::{make_array, MutableArrayData};

    let data = array.to_data();
    let len = slots.iter().map(Range::len).sum();
    let mut selected = MutableArrayData::new(vec![&data], false, len);
    for slots in slots {
        selected.try_extend(0, slots.start, slots.end)?;
    }
    Ok(make_array(selected.freeze()))
}

/// Write table data to a Parquet file using Arrow's native Parquet writer.
///
/// Batches are streamed from DataFusion and written incrementally, avoiding
/// full in-memory materialization before the first write. The write is
/// atomic (see [`Output`]), so a plan may read the file it replaces.
/// Columns in seconds are written in milliseconds ([`ParquetUnits`]).
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
    let df = table.ordered_df()?;

    RUNTIME.block_on(async {
        use datafusion::arrow::datatypes::Schema as ArrowSchema;
        use futures_util::StreamExt;
        use parquet::arrow::ArrowWriter;
        use parquet::basic::Compression;
        use parquet::file::properties::WriterProperties;

        // Extract Arrow schema before consuming the DataFrame
        let df_schema = df.schema();
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
        let mut stream = df
            .execute_stream()
            .await
            .map_err(|e| LtseqError::Runtime(format!("Failed to create stream: {}", e)))?;

        let units = ParquetUnits::of(&schema);
        let written = units
            .as_ref()
            .map_or(schema, |units| Arc::clone(&units.schema));
        let mut writer = ArrowWriter::try_new(output.file(), written, Some(props))
            .map_err(|e| LtseqError::Io(format!("Failed to create Parquet writer: {}", e)))?;

        let mut wrote_any = false;
        while let Some(batch_result) = stream.next().await {
            let batch = batch_result.map_err(|e| {
                LtseqError::Io(format!("Failed to read batch: {}", e))
            })?;
            let batch = match &units {
                Some(units) => units.convert(&batch)?,
                None => batch,
            };
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
    if table.dataframe.is_none() {
        return Ok(collected_reader(schema_or_empty(table), Vec::new()));
    }
    let batches = RUNTIME
        .block_on(table.ordered_df()?.collect())
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
    if table.dataframe.is_none() {
        return Ok(collected_reader(schema, Vec::new()));
    }
    let stream = RUNTIME
        .block_on(table.ordered_df()?.execute_stream())
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

    /// A run-end encoded column `x` in seconds whose child fields are not
    /// Arrow's defaults, with the field `unit` gives its values. pyarrow
    /// cannot build such a column, but another `__arrow_c_stream__` producer
    /// can.
    fn run_end_encoded_seconds() -> (RecordBatch, impl Fn(TimeUnit) -> DataType) {
        use datafusion::arrow::array::{make_array, Int32Array, RunArray, TimestampSecondArray};
        use datafusion::arrow::datatypes::{Field, Int32Type};
        use std::collections::HashMap;

        let in_unit = |unit| {
            let run_ends = Field::new("ends", DataType::Int32, false);
            let values = Field::new("v", DataType::Timestamp(unit, None), false)
                .with_metadata(HashMap::from([("k".to_string(), "v".to_string())]));
            DataType::RunEndEncoded(Arc::new(run_ends), Arc::new(values))
        };
        let runs = RunArray::<Int32Type>::try_new(
            &Int32Array::from(vec![2, 3]),
            &TimestampSecondArray::from(vec![1, -1]),
        )
        .expect("run array");
        let column = runs
            .into_data()
            .into_builder()
            .data_type(in_unit(TimeUnit::Second))
            .build()
            .expect("run array with its own fields");
        let schema = Schema::new(vec![Field::new("x", in_unit(TimeUnit::Second), false)]);
        let batch =
            RecordBatch::try_new(Arc::new(schema), vec![make_array(column)]).expect("batch");
        (batch, in_unit)
    }

    /// Converted to milliseconds, the column keeps its child fields. Arrow's
    /// `cast` gave it default ones, which the converted batch's schema,
    /// keeping the table's, rejected.
    #[test]
    fn run_end_encoded_keeps_its_fields() {
        use datafusion::arrow::datatypes::{Int32Type, TimestampMillisecondType};

        let (batch, in_unit) = run_end_encoded_seconds();
        let units = ParquetUnits::of(&batch.schema()).expect("a column in seconds");
        let converted = units.convert(&batch).expect("converted batch");
        let milliseconds = in_unit(TimeUnit::Millisecond);
        assert_eq!(converted.column(0).data_type(), &milliseconds);
        assert_eq!(units.schema.field(0).data_type(), &milliseconds);
        let runs = converted.column(0).as_run::<Int32Type>();
        assert_eq!(runs.run_ends().values(), &[2, 3]);
        let values = runs.values().as_primitive::<TimestampMillisecondType>();
        assert_eq!(values.values(), &[1000, -1000]);
    }

    /// The whole write of such a column succeeds, and the file reads back
    /// the same instants in milliseconds.
    #[test]
    fn run_end_encoded_with_its_own_fields_round_trips() {
        use crate::engine::create_session_context;
        use datafusion::arrow::datatypes::TimestampMillisecondType;
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

        let (batch, _) = run_end_encoded_seconds();
        let schema = batch.schema();
        let table =
            LTSeqTable::from_batches(create_session_context(), vec![batch], schema, Vec::new())
                .expect("table");
        let dir = tempfile::tempdir().expect("temporary directory");
        let path = dir.path().join("out.parquet");
        write_parquet_impl(&table, path.to_string_lossy().into_owned(), None)
            .expect("write_parquet");

        let file = File::open(&path).expect("written file");
        let back = ParquetRecordBatchReaderBuilder::try_new(file)
            .expect("Parquet file")
            .build()
            .expect("reader")
            .collect::<Result<Vec<_>, _>>()
            .expect("batches");
        let back = datafusion::arrow::compute::concat_batches(&back[0].schema(), &back)
            .expect("one batch");
        // The `parquet` crate reads run-end encoded values back decoded.
        let milliseconds = DataType::Timestamp(TimeUnit::Millisecond, None);
        assert_eq!(back.schema().field(0).data_type(), &milliseconds);
        let values = back.column(0).as_primitive::<TimestampMillisecondType>();
        assert_eq!(values.values(), &[1000, 1000, -1000]);
    }

    /// A slice of a list, map, list view or run-end encoded array keeps its
    /// parent's whole child, a slice of a dictionary array all its entries,
    /// and so does each batch cut from one array. A null list or map slot can
    /// span any part of the child, and list views can leave any gap between
    /// them. The conversion builds only the values the rows reach, so a few
    /// rows of a large column, or each of many batches sharing its child,
    /// cost those rows and not the column.
    #[test]
    fn a_batch_converts_only_the_values_its_rows_reach() {
        use datafusion::arrow::array::{
            FixedSizeListArray, Int32Array, LargeListArray, ListArray, ListViewArray, MapArray,
            RunArray, StructArray, TimestampMillisecondArray, TimestampSecondArray,
        };
        use datafusion::arrow::datatypes::{Field, Fields, Int32Type};

        // 100 rows of 10 values.
        const ROWS: usize = 100;
        const PER: usize = 10;
        let leaf = |unit| -> ArrayRef {
            let seconds = 0..(ROWS * PER) as i64;
            match unit {
                TimeUnit::Second => Arc::new(TimestampSecondArray::from_iter_values(seconds)),
                _ => Arc::new(TimestampMillisecondArray::from_iter_values(
                    seconds.map(|s| s * 1000),
                )),
            }
        };
        let field = |name, unit| Field::new(name, DataType::Timestamp(unit, None), true);
        let item = |unit| Arc::new(field("item", unit));
        let rows = || OffsetBuffer::<i32>::from_lengths(std::iter::repeat_n(PER, ROWS));
        let list = |item: FieldRef, values| -> ArrayRef {
            Arc::new(ListArray::new(item, rows(), values, None))
        };
        // Converts rows `offset..offset + len` of the column `build` gives,
        // which must equal the same rows built in milliseconds, and checks
        // that the leaf `built` measures holds the `reached` values.
        fn check(
            name: &str,
            build: impl Fn(TimeUnit) -> ArrayRef,
            (offset, len): (usize, usize),
            built: fn(&ArrayRef) -> usize,
            reached: usize,
        ) {
            let seconds = build(TimeUnit::Second).slice(offset, len);
            let expected = build(TimeUnit::Millisecond).slice(offset, len);
            let to = in_parquet_units(seconds.data_type()).expect("a type in seconds");
            let converted = in_milliseconds(&seconds, &to).expect(name);
            assert_eq!(converted.as_ref(), expected.as_ref(), "{name}");
            assert_eq!(built(&converted), reached, "{name}");
        }
        check(
            "list",
            |unit| list(item(unit), leaf(unit)),
            (40, 2),
            |a| a.as_list::<i32>().values().len(),
            20,
        );
        check(
            "large list",
            |unit| {
                let rows = OffsetBuffer::<i64>::from_lengths(std::iter::repeat_n(PER, ROWS));
                Arc::new(LargeListArray::new(item(unit), rows, leaf(unit), None))
            },
            (40, 2),
            |a| a.as_list::<i64>().values().len(),
            20,
        );
        check(
            // Views in reverse order, so the first row's is not the lowest.
            "list view",
            |unit| {
                let offsets: ScalarBuffer<i32> =
                    (0..ROWS).rev().map(|row| (row * PER) as i32).collect();
                let sizes = ScalarBuffer::from(vec![PER as i32; ROWS]);
                Arc::new(ListViewArray::new(
                    item(unit),
                    offsets,
                    sizes,
                    leaf(unit),
                    None,
                ))
            },
            (40, 2),
            |a| a.as_list_view::<i32>().values().len(),
            20,
        );
        check(
            "map",
            |unit| {
                let keys = Field::new("key", DataType::Int32, false);
                let entries = StructArray::new(
                    Fields::from(vec![keys, field("value", unit)]),
                    vec![
                        Arc::new(Int32Array::from_iter_values(0..(ROWS * PER) as i32)),
                        leaf(unit),
                    ],
                    None,
                );
                let entries_field = Field::new("entries", entries.data_type().clone(), false);
                Arc::new(MapArray::new(
                    Arc::new(entries_field),
                    rows(),
                    entries,
                    None,
                    false,
                ))
            },
            (40, 2),
            |a| a.as_map().entries().column(1).len(),
            20,
        );
        check(
            "list of struct",
            |unit| {
                let fields = Fields::from(vec![field("b", unit)]);
                let structs = StructArray::new(fields.clone(), vec![leaf(unit)], None);
                list(
                    Arc::new(Field::new("item", DataType::Struct(fields), true)),
                    Arc::new(structs),
                )
            },
            (40, 2),
            |a| a.as_list::<i32>().values().as_struct().column(0).len(),
            20,
        );
        check(
            // 10 rows of 10 lists of 10 values.
            "list of lists",
            |unit| {
                let inner = list(item(unit), leaf(unit));
                let item = Arc::new(Field::new("item", inner.data_type().clone(), true));
                let rows = OffsetBuffer::<i32>::from_lengths(std::iter::repeat_n(PER, ROWS / PER));
                Arc::new(ListArray::new(item, rows, inner, None))
            },
            (4, 1),
            |a| {
                let inner = a.as_list::<i32>().values();
                inner.as_list::<i32>().values().len()
            },
            100,
        );
        check(
            // A run of one row for each value.
            "run-end encoded",
            |unit| {
                let ends = Int32Array::from_iter_values(1..=(ROWS * PER) as i32);
                Arc::new(RunArray::<Int32Type>::try_new(&ends, leaf(unit).as_ref()).unwrap())
            },
            (400, 20),
            |a| a.as_run::<Int32Type>().values().len(),
            20,
        );
        check(
            // An entry for each value, in reverse order.
            "dictionary",
            |unit| {
                let keys = Int32Array::from_iter_values((0..(ROWS * PER) as i32).rev());
                Arc::new(DictionaryArray::<Int32Type>::new(keys, leaf(unit)))
            },
            (400, 20),
            |a| a.as_any_dictionary().values().len(),
            20,
        );

        // Three rows: the first 10 values, a null slot over the next 980,
        // the last 10.
        let gap = || OffsetBuffer::<i32>::new(vec![0, 10, 990, 1000].into());
        let middle_null = || Some(NullBuffer::from(vec![true, false, true]));
        check(
            "list with a null slot",
            |unit| Arc::new(ListArray::new(item(unit), gap(), leaf(unit), middle_null())),
            (0, 3),
            |a| a.as_list::<i32>().values().len(),
            20,
        );
        check(
            "map with a null slot",
            |unit| {
                let keys = Field::new("key", DataType::Int32, false);
                let entries = StructArray::new(
                    Fields::from(vec![keys, field("value", unit)]),
                    vec![
                        Arc::new(Int32Array::from_iter_values(0..(ROWS * PER) as i32)),
                        leaf(unit),
                    ],
                    None,
                );
                let entries_field = Field::new("entries", entries.data_type().clone(), false);
                Arc::new(MapArray::new(
                    Arc::new(entries_field),
                    gap(),
                    entries,
                    middle_null(),
                    false,
                ))
            },
            (0, 3),
            |a| a.as_map().entries().column(1).len(),
            20,
        );
        check(
            "list under a null struct row",
            |unit| {
                let lists: ArrayRef = Arc::new(ListArray::new(item(unit), gap(), leaf(unit), None));
                let fields = Fields::from(vec![Field::new("l", lists.data_type().clone(), true)]);
                Arc::new(StructArray::new(fields, vec![lists], middle_null()))
            },
            (0, 3),
            |a| a.as_struct().column(0).as_list::<i32>().values().len(),
            20,
        );
        check(
            "list of lists with a null inner slot",
            |unit| {
                let inner = Arc::new(ListArray::new(item(unit), gap(), leaf(unit), middle_null()));
                let item = Arc::new(Field::new("item", inner.data_type().clone(), true));
                let rows = OffsetBuffer::<i32>::new(vec![0, 3].into());
                Arc::new(ListArray::new(item, rows, inner, None))
            },
            (0, 1),
            |a| {
                let inner = a.as_list::<i32>().values();
                inner.as_list::<i32>().values().len()
            },
            20,
        );
        let views = |unit, offsets: Vec<i32>, sizes: Vec<i32>, nulls| -> ArrayRef {
            Arc::new(ListViewArray::new(
                item(unit),
                offsets.into(),
                sizes.into(),
                leaf(unit),
                nulls,
            ))
        };
        check(
            "list views with a gap between them",
            |unit| views(unit, vec![990, 0], vec![10, 10], None),
            (0, 2),
            |a| a.as_list_view::<i32>().values().len(),
            20,
        );
        check(
            "list views with a null view over the gap",
            |unit| views(unit, vec![0, 0, 990], vec![10, 1000, 10], middle_null()),
            (0, 3),
            |a| a.as_list_view::<i32>().values().len(),
            20,
        );
        check(
            "list of dictionary values with a null slot",
            |unit| {
                let keys = Int32Array::from_iter_values((0..(ROWS * PER) as i32).rev());
                let entries: ArrayRef =
                    Arc::new(DictionaryArray::<Int32Type>::new(keys, leaf(unit)));
                let item = Arc::new(Field::new("item", entries.data_type().clone(), true));
                Arc::new(ListArray::new(item, gap(), entries, middle_null()))
            },
            (0, 3),
            |a| {
                let values = a.as_list::<i32>().values();
                values.as_any_dictionary().values().len()
            },
            20,
        );
        check(
            "list of fixed-size lists with a null slot",
            |unit| {
                let rows = FixedSizeListArray::new(item(unit), PER as i32, leaf(unit), None);
                let item = Arc::new(Field::new("item", rows.data_type().clone(), true));
                let offsets = OffsetBuffer::<i32>::new(vec![0, 1, 99, 100].into());
                Arc::new(ListArray::new(item, offsets, Arc::new(rows), middle_null()))
            },
            (0, 3),
            |a| {
                let rows = a.as_list::<i32>().values();
                rows.as_fixed_size_list().values().len()
            },
            20,
        );
        check(
            // A run of 10 rows for each of 100 values. The first and third
            // slots both fall in the first run, which is built once.
            "list of run-end values with null slots",
            |unit| {
                let ends = Int32Array::from_iter_values((1..=ROWS as i32).map(|run| run * 10));
                let values = leaf(unit).slice(0, ROWS);
                let runs = RunArray::<Int32Type>::try_new(&ends, values.as_ref()).unwrap();
                let item = Arc::new(Field::new("item", runs.data_type().clone(), true));
                let offsets = OffsetBuffer::<i32>::new(vec![0, 3, 6, 9, 995, 1000].into());
                let nulls = NullBuffer::from(vec![true, false, true, false, true]);
                Arc::new(ListArray::new(item, offsets, Arc::new(runs), Some(nulls)))
            },
            (0, 5),
            |a| {
                let runs = a.as_list::<i32>().values();
                runs.as_run::<Int32Type>().values().len()
            },
            2,
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
