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
enum Output {
    Replace {
        tmp: NamedTempFile,
        dest: PathBuf,
    },
    /// An existing device or FIFO (`/dev/stdout`): it has no contents to keep
    /// and a rename cannot replace it, so it is written directly.
    InPlace(File),
}

/// The file a write to `path` replaces or creates, resolved at the call.
///
/// It is absolute: the rename comes after the plan has run with the GIL
/// released, and by then another thread may have changed the working
/// directory. Symlinks are followed as `File::create` followed them, so the
/// rename replaces a link's target rather than the link, and creates the
/// target of a link that points to nothing yet.
fn destination(path: &str) -> io::Result<PathBuf> {
    if let Ok(dest) = fs::canonicalize(path) {
        return Ok(dest);
    }
    let path = Path::new(path);
    let mut dest = if path.is_absolute() {
        path.to_path_buf()
    } else {
        env::current_dir()?.join(path)
    };
    // Nothing exists at the end of the chain, or the chain is a cycle. 40 is
    // Linux's limit; after a cycle `dest` is still a link, and reading its
    // metadata fails with ELOOP, as `File::create` did.
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

/// An empty temporary file in `dir` that will replace an existing file
/// (`replaces`) or create a new one.
///
/// It never grants access the file at the destination will not: a
/// descriptor opened on it keeps its access after a chmod. A new file gets
/// `File::create`'s mode (0o666 less the umask) from the start. A
/// replacement starts owner-only, tempfile's default, until [`keep_access`]
/// gives it the old file's access.
fn temporary(dir: &Path, replaces: bool) -> io::Result<NamedTempFile> {
    // Hidden, short whatever the destination's name, and without its
    // extension, so that a reader listing the directory for `.csv` or
    // `.parquet` files skips it.
    let mut builder = tempfile::Builder::new();
    builder.prefix(".ltseq-").suffix(".tmp");
    #[cfg(unix)]
    if !replaces {
        use std::os::unix::fs::PermissionsExt;
        builder.permissions(fs::Permissions::from_mode(0o666));
    }
    #[cfg(not(unix))]
    let _ = replaces;
    builder.tempfile_in(dir)
}

/// Give `file`, an owner-only temporary file, the `gid` and then the `mode`
/// of the file it replaces, as truncating that file in place kept them.
///
/// A writer who cannot give it that group, one they are not a member of,
/// leaves it owner-only: the group bits would otherwise grant the writer's
/// own group what they granted the old file's.
#[cfg(unix)]
fn keep_access(file: &File, mode: u32, gid: u32) -> io::Result<()> {
    use std::os::unix::fs::{fchown, MetadataExt, PermissionsExt};
    let mut mode = mode & 0o7777;
    if file.metadata()?.gid() != gid && fchown(file, None, Some(gid)).is_err() {
        mode &= 0o700;
    }
    file.set_permissions(fs::Permissions::from_mode(mode))
}

impl Output {
    fn create(path: &str) -> Result<Self, LtseqError> {
        let fail = |e: io::Error| LtseqError::io(&format!("Failed to create file '{}'", path), e);

        let dest = destination(path).map_err(fail)?;
        let existing = match fs::metadata(&dest) {
            Ok(meta) => Some(meta),
            Err(e) if e.kind() == io::ErrorKind::NotFound => None,
            Err(e) => return Err(fail(e)),
        };
        if let Some(meta) = &existing {
            if meta.is_dir() {
                return Err(fail(io::ErrorKind::IsADirectory.into()));
            }
            if !meta.is_file() {
                return File::create(&dest).map(Output::InPlace).map_err(fail);
            }
            // A rename needs write access to the directory only. Keep the
            // refusal `File::create` gave for a file the caller cannot write.
            OpenOptions::new().write(true).open(&dest).map_err(fail)?;
        }

        // Only a root has no parent, and it is a directory.
        let dir = dest
            .parent()
            .ok_or_else(|| fail(io::ErrorKind::IsADirectory.into()))?;
        let tmp = temporary(dir, existing.is_some()).map_err(fail)?;
        #[cfg(unix)]
        if let Some(meta) = &existing {
            use std::os::unix::fs::MetadataExt;
            keep_access(tmp.as_file(), meta.mode(), meta.gid()).map_err(fail)?;
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
        tmp.as_file().sync_all().map_err(fail)?;
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
        // A group no account is a member of.
        keep_access(file, 0o664, 0x7fff_fff0).expect("keep_access");
        let after = file.metadata().expect("metadata");
        let mode = after.mode() & 0o7777;
        assert_eq!(
            (after.gid(), mode),
            (before.gid(), 0o600),
            "mode {:o}",
            mode
        );
    }
}
