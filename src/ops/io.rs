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

use datafusion::arrow::array::{
    Array, ArrayRef, ArrowNativeTypeOp, ArrowPrimitiveType, AsArray, BooleanBufferBuilder,
    GenericListArray, GenericListViewArray, OffsetSizeTrait, PrimitiveArray, RecordBatch,
};
use datafusion::arrow::buffer::NullBuffer;
use datafusion::arrow::datatypes::{
    ArrowNativeType, DataType, FieldRef, RunEndIndexType, Schema, SchemaRef, TimeUnit,
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
                Some(to) => in_milliseconds(column, to, None).map_err(|e| match e {
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

/// `array` in `to`, the type [`in_parquet_units`] gives for its type: each
/// second count times 1000, inside containers built with `to`'s own fields.
///
/// Not Arrow's `cast`, for two reasons. A container holds values that no
/// row holds: under a null struct, list or map slot, outside a slice, or in
/// a dictionary entry no key uses. They can be anything, and `cast` checks
/// them all, so a table whose rows all fit would fail to write. And `cast`
/// gives a run-end encoded array default child fields, which the batch's
/// schema rejects when the table's differ.
///
/// `live` marks the slots of `array` that a row holds, all of them when
/// `None`. Only the values in those slots are checked and converted; the
/// others become 0, since no row reads them.
fn in_milliseconds(
    array: &ArrayRef,
    to: &DataType,
    live: Option<NullBuffer>,
) -> Result<ArrayRef, ArrowError> {
    use datafusion::arrow::array::{FixedSizeListArray, MapArray, StructArray};
    use datafusion::arrow::datatypes::{
        Int16Type, Int32Type, Int64Type, Time32MillisecondType, Time32SecondType,
        TimestampMillisecondType, TimestampSecondType,
    };

    if array.data_type() == to {
        return Ok(Arc::clone(array));
    }
    let live = NullBuffer::union(array.nulls(), live.as_ref());
    match (array.data_type(), to) {
        (DataType::Timestamp(TimeUnit::Second, _), DataType::Timestamp(_, tz)) => {
            let converted =
                times_thousand::<TimestampSecondType, TimestampMillisecondType>(array, live)?;
            Ok(Arc::new(converted.with_timezone_opt(tz.clone())))
        }
        (DataType::Time32(TimeUnit::Second), _) => Ok(Arc::new(times_thousand::<
            Time32SecondType,
            Time32MillisecondType,
        >(array, live)?)),
        (DataType::List(_), DataType::List(item)) => list::<i32>(array, item, live),
        (DataType::LargeList(_), DataType::LargeList(item)) => list::<i64>(array, item, live),
        (DataType::ListView(_), DataType::ListView(item)) => list_view::<i32>(array, item, live),
        (DataType::LargeListView(_), DataType::LargeListView(item)) => {
            list_view::<i64>(array, item, live)
        }
        (DataType::FixedSizeList(..), DataType::FixedSizeList(item, size)) => {
            let list = array.as_fixed_size_list();
            let live = live.map(|live| live.expand(list.value_length() as usize));
            let values = in_milliseconds(list.values(), item.data_type(), live)?;
            let converted = FixedSizeListArray::try_new_with_length(
                Arc::clone(item),
                *size,
                values,
                list.nulls().cloned(),
                list.len(),
            )?;
            Ok(Arc::new(converted))
        }
        (DataType::Struct(_), DataType::Struct(fields)) => {
            let parent = array.as_struct();
            let columns = parent
                .columns()
                .iter()
                .zip(fields.iter())
                .map(|(column, field)| in_milliseconds(column, field.data_type(), live.clone()))
                .collect::<Result<Vec<_>, _>>()?;
            let converted = StructArray::try_new_with_length(
                fields.clone(),
                columns,
                parent.nulls().cloned(),
                parent.len(),
            )?;
            Ok(Arc::new(converted))
        }
        (DataType::Map(..), DataType::Map(field, sorted)) => {
            let map = array.as_map();
            let entries: ArrayRef = Arc::new(map.entries().clone());
            let ranges = map
                .offsets()
                .windows(2)
                .map(|w| (w[0] as usize, w[1] as usize));
            let live = Some(reached(ranges, live.as_ref(), entries.len()));
            let entries = in_milliseconds(&entries, field.data_type(), live)?;
            let converted = MapArray::try_new(
                Arc::clone(field),
                map.offsets().clone(),
                entries.as_struct().clone(),
                map.nulls().cloned(),
                *sorted,
            )?;
            Ok(Arc::new(converted))
        }
        (DataType::Dictionary(..), DataType::Dictionary(_, values)) => {
            let dictionary = array.as_any_dictionary();
            let len = dictionary.values().len();
            let mut used = BooleanBufferBuilder::new(len);
            used.append_n(len, false);
            if len > 0 {
                for (slot, key) in dictionary.normalized_keys().into_iter().enumerate() {
                    if live.as_ref().is_none_or(|live| live.is_valid(slot)) {
                        used.set_bit(key, true);
                    }
                }
            }
            let used = Some(NullBuffer::new(used.finish()));
            Ok(dictionary.with_values(in_milliseconds(dictionary.values(), values, used)?))
        }
        (DataType::RunEndEncoded(run_ends, _), _) => match run_ends.data_type() {
            DataType::Int16 => run_end_encoded::<Int16Type>(array, to, live),
            DataType::Int32 => run_end_encoded::<Int32Type>(array, to, live),
            DataType::Int64 => run_end_encoded::<Int64Type>(array, to, live),
            other => Err(ArrowError::InvalidArgumentError(format!(
                "run ends of type {other}"
            ))),
        },
        (from, to) => Err(ArrowError::CastError(format!(
            "no conversion from {from} to {to}"
        ))),
    }
}

/// `array`'s values times 1000, as `T`, with `array`'s nulls. A value in a
/// `live` slot that does not fit fails with `ArithmeticOverflow`; the other
/// slots become 0.
fn times_thousand<F, T>(
    array: &ArrayRef,
    live: Option<NullBuffer>,
) -> Result<PrimitiveArray<T>, ArrowError>
where
    F: ArrowPrimitiveType,
    T: ArrowPrimitiveType<Native = F::Native>,
{
    let seconds = array.as_primitive::<F>();
    let thousand = F::Native::usize_as(1000);
    // `try_unary` computes only the slots its input's nulls leave valid.
    let held = PrimitiveArray::<F>::new(seconds.values().clone(), live);
    let converted = held.try_unary::<_, T, _>(|value| value.mul_checked(thousand))?;
    Ok(PrimitiveArray::new(
        converted.values().clone(),
        seconds.nulls().cloned(),
    ))
}

fn list<O: OffsetSizeTrait>(
    array: &ArrayRef,
    item: &FieldRef,
    live: Option<NullBuffer>,
) -> Result<ArrayRef, ArrowError> {
    let list = array.as_list::<O>();
    let ranges = list
        .offsets()
        .windows(2)
        .map(|w| (w[0].as_usize(), w[1].as_usize()));
    let live = Some(reached(ranges, live.as_ref(), list.values().len()));
    let values = in_milliseconds(list.values(), item.data_type(), live)?;
    let converted = GenericListArray::<O>::try_new(
        Arc::clone(item),
        list.offsets().clone(),
        values,
        list.nulls().cloned(),
    )?;
    Ok(Arc::new(converted))
}

fn list_view<O: OffsetSizeTrait>(
    array: &ArrayRef,
    item: &FieldRef,
    live: Option<NullBuffer>,
) -> Result<ArrayRef, ArrowError> {
    let list = array.as_list_view::<O>();
    let ranges = list
        .offsets()
        .iter()
        .zip(list.sizes().iter())
        .map(|(offset, size)| (offset.as_usize(), offset.as_usize() + size.as_usize()));
    let live = Some(reached(ranges, live.as_ref(), list.values().len()));
    let values = in_milliseconds(list.values(), item.data_type(), live)?;
    let converted = GenericListViewArray::<O>::try_new(
        Arc::clone(item),
        list.offsets().clone(),
        list.sizes().clone(),
        values,
        list.nulls().cloned(),
    )?;
    Ok(Arc::new(converted))
}

/// The values a run-end encoded array's `live` slots reach are those of the
/// runs they fall in.
fn run_end_encoded<R: RunEndIndexType>(
    array: &ArrayRef,
    to: &DataType,
    live: Option<NullBuffer>,
) -> Result<ArrayRef, ArrowError> {
    use datafusion::arrow::array::make_array;

    let DataType::RunEndEncoded(_, values) = to else {
        return Err(ArrowError::CastError(format!(
            "no conversion from {} to {to}",
            array.data_type()
        )));
    };
    let runs = array.as_run::<R>();
    let ends = runs.run_ends();
    let (offset, len) = (ends.offset(), ends.len());
    let mut used = BooleanBufferBuilder::new(runs.values().len());
    used.append_n(runs.values().len(), false);
    if len > 0 {
        for run in ends.get_start_physical_index()..=ends.get_end_physical_index() {
            let start = match run {
                0 => 0,
                _ => ends.values()[run - 1].as_usize(),
            };
            // The run's slots, within the slice.
            let start = start.max(offset) - offset;
            let end = ends.values()[run].as_usize().min(offset + len) - offset;
            let held = live
                .as_ref()
                .is_none_or(|live| live.slice(start, end - start).null_count() < end - start);
            used.set_bit(run, held);
        }
    }
    let used = Some(NullBuffer::new(used.finish()));
    let values = in_milliseconds(runs.values(), values.data_type(), used)?;
    // The array's own run ends, slice and `to`'s fields.
    let data = runs.to_data();
    let run_ends = data.child_data()[0].clone();
    let data = data
        .into_builder()
        .data_type(to.clone())
        .child_data(vec![run_ends, values.to_data()])
        .build()?;
    Ok(make_array(data))
}

/// The values of a child of `len` values that the `live` slots of a list
/// reach, given each slot's range in the child.
fn reached(
    ranges: impl Iterator<Item = (usize, usize)>,
    live: Option<&NullBuffer>,
    len: usize,
) -> NullBuffer {
    let mut reached = BooleanBufferBuilder::new(len);
    reached.append_n(len, false);
    for (slot, (start, end)) in ranges.enumerate() {
        if live.is_none_or(|live| live.is_valid(slot)) {
            (start..end).for_each(|value| reached.set_bit(value, true));
        }
    }
    NullBuffer::new(reached.finish())
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
