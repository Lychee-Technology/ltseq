//! File sources: the files a reader lists, and what each execution reads of
//! them (#262).
//!
//! A reader lists its files once, at the call ([`read_files`]), and the
//! table reads exactly those: a file that its directory or glob matches only
//! later is never read, and a path that names no file fails the call
//! (#261). Each execution looks every listed file up again, so it reads the
//! file as it is at that moment, and one that has disappeared fails the
//! execution ([`LocalFiles`]). The lookup also drops the file's
//! cached Parquet footer, so the execution reads the footer, and the
//! statistics in it, afresh; the partitions of one execution share it.
//!
//! DataFusion's `SessionContext::read_csv` and `read_parquet` build a table
//! over the path instead, which DataFusion lists again at each execution.
//! Its listing cache answers those listings with the one made at the call,
//! each file's size and modification time included, so a directory table
//! read a rewritten file by its old size and footer, and a deleted one by
//! its old statistics. A single file is looked up afresh, but a missing one
//! is listed as an empty directory, so a deleted file read as no rows.

use std::fmt;
use std::ops::Range;
use std::path::{Component, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use datafusion::catalog::TableProvider;
use datafusion::common::{exec_datafusion_err, DataFusionError, Result};
use datafusion::dataframe::DataFrame;
use datafusion::datasource::file_format::options::ReadOptions;
use datafusion::datasource::listing::{ListingTable, ListingTableConfig, ListingTableUrl};
use datafusion::datasource::source_as_provider;
use datafusion::execution::cache::cache_manager::FileMetadataCache;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{LogicalPlan, SortExpr};
use datafusion::object_store::local::LocalFileSystem;
use datafusion::object_store::path::Path;
use datafusion::object_store::{
    CopyOptions, Error as StoreError, GetOptions, GetResult, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions,
    Result as StoreResult,
};
use futures_util::stream::BoxStream;
use futures_util::TryStreamExt;
use url::{ParseError, Url};

use crate::python_glob;

/// Why a reader built no table.
#[derive(Debug)]
pub(crate) enum ReadError {
    /// The path names no file to read: it does not exist, or it is a
    /// directory or a glob without a file of the format. The message names
    /// the path. v0.5 raises `SourceNotFoundError` for it (contract §4.1,
    /// #266).
    NotFound(String),
    /// Resolving the path, listing its files or inferring their schema
    /// failed.
    Failed(DataFusionError),
}

impl From<DataFusionError> for ReadError {
    fn from(error: DataFusionError) -> Self {
        ReadError::Failed(error)
    }
}

/// The table `SessionContext::read_csv` or `read_parquet` builds for `path`,
/// over the files `path` names now.
///
/// `path` is a file, a directory whose files with the format's extension
/// are read, or a glob ([`locate`]). The table lists each file by its own
/// URL, so an execution looks up each file and no directory. The schema is
/// inferred from these files as DataFusion infers it from its listing;
/// `options` carry none, as LTSeq's readers give none.
pub(crate) async fn read_files<'a>(
    session: &SessionContext,
    path: &str,
    options: impl ReadOptions<'a>,
) -> Result<DataFrame, ReadError> {
    let options =
        options.to_listing_options(&session.copied_config(), session.copied_table_options());
    let extension = options.file_extension.as_str();
    let urls = locate(path, extension)?;
    for url in urls.iter().filter(|url| url.is_collection()) {
        let dir = url.get_url().to_file_path();
        if let Some(dir) = dir.as_ref().ok().and_then(|dir| dir.to_str()) {
            look_up_files(dir, extension)?;
        }
    }

    let state = session.state();
    let store = state.runtime_env().object_store(&urls[0])?;
    let mut files: Vec<ObjectMeta> = Vec::new();
    for url in &urls {
        let listed = url
            .list_all_files(&state, store.as_ref(), extension)
            .await?;
        files.extend(listed.try_collect::<Vec<_>>().await?);
    }
    // From here on as `ListingOptions::infer_schema`, over these files.
    if files.is_empty() {
        return Err(ReadError::NotFound(format!(
            "No {extension} file in the directory '{path}'"
        )));
    }
    // An empty file cannot add to the schema, and may fail when read for it.
    let sized: Vec<ObjectMeta> = files.iter().filter(|f| f.size > 0).cloned().collect();
    let schema = options.format.infer_schema(&state, &store, &sized).await?;

    let paths = files
        .iter()
        .map(|file| file_url(&urls[0], &file.location))
        .collect::<Result<Vec<_>>>()?;
    let config = ListingTableConfig::new_with_multi_paths(paths)
        .with_listing_options(options)
        .with_schema(schema);
    // No statistics cache: each execution reads a file's statistics from the
    // footer it reads afresh ([`LocalFiles`]), where a cached copy would
    // outlive that footer.
    let table = ListingTable::try_new(config)?;
    Ok(session.read_table(Arc::new(table))?)
}

/// The URLs of what `path` names now (contract §4.1), each a file or a
/// directory to list, or `NotFound` if it names nothing.
///
/// A local path that names an existing file or directory is read as itself,
/// even when its name holds `*`, `?` or `[`. Any other local path that holds
/// them is a glob ([`glob_files`]), which `ListingTableUrl::parse` never
/// sees: its glob crate rejects patterns Python accepts, such as `a**.csv`.
/// A URL is taken as written, as `ListingTableUrl::parse` takes it. A path
/// that cannot be looked up, such as one in a directory that cannot be
/// searched, is an error, not `NotFound` ([`python_glob::metadata`]).
fn locate(path: &str, extension: &str) -> Result<Vec<ListingTableUrl>, ReadError> {
    let local = std::path::Path::new(path);
    let exists =
        |path: &std::path::Path| -> Result<bool> { Ok(python_glob::metadata(path)?.is_some()) };
    let url = if exists(local)? {
        local_url(local)?
    } else if is_local(path) && path.contains(['*', '?', '[']) {
        return glob_files(path, extension);
    } else {
        let url = ListingTableUrl::parse(path)?;
        if let Ok(file) = url.get_url().to_file_path() {
            if !exists(&file)? {
                return Err(ReadError::NotFound(format!(
                    "No such file or directory: '{path}'"
                )));
            }
        }
        url
    };
    // `SessionContext::read_csv` and `read_parquet` refuse such a file too.
    if !url.is_collection() && !url.as_str().ends_with(extension) {
        return Err(exec_datafusion_err!(
            "File path '{}' does not match the expected extension '{extension}'",
            url.as_str()
        )
        .into());
    }
    Ok(vec![as_collection_if_directory(url)?])
}

/// Whether `ListingTableUrl::parse` reads `path` as a local path, not a
/// URL: it is absolute, or has no scheme.
fn is_local(path: &str) -> bool {
    std::path::Path::new(path).is_absolute()
        || matches!(Url::parse(path), Err(ParseError::RelativeUrlWithoutBase))
}

/// The URLs of the files with the format's `extension` that the glob
/// `pattern` matches now, in the order of their paths and each once, or
/// `NotFound` if there are none.
///
/// `pattern` matches as Python's `glob.glob(pattern, recursive=True)`
/// matches it ([`python_glob`]): `*`, `?` and `[...]` within one path
/// segment, `**` across directories, and a name that starts with `.` only
/// by a literal `.`. `ListingTableUrl`'s own glob matches the whole pattern
/// against the first segment below its directory, so it matches nothing for
/// `d/s*/x.parquet`, and the files of `d`'s subdirectories for `d/*`.
///
/// A match that cannot be looked up is an error, not a match that is not a
/// file: the table would read fewer files than the glob matches.
fn glob_files(pattern: &str, extension: &str) -> Result<Vec<ListingTableUrl>, ReadError> {
    let mut urls = Vec::new();
    for path in python_glob::glob(pattern).map_err(DataFusionError::from)? {
        if !path.ends_with(extension) {
            continue;
        }
        let meta = python_glob::metadata(&path).map_err(DataFusionError::from)?;
        if meta.is_some_and(|meta| meta.is_file()) {
            urls.push(local_url(std::path::Path::new(&path))?);
        }
    }
    // Contract §4.1's order. Matches that differ only by `.`, `..` or
    // repeated `/`, or that `**/**` gives twice, are one file.
    urls.sort_by(|a, b| a.prefix().as_ref().cmp(b.prefix().as_ref()));
    urls.dedup_by(|a, b| a.prefix() == b.prefix());
    if urls.is_empty() {
        return Err(ReadError::NotFound(format!(
            "No {extension} file matches '{pattern}'"
        )));
    }
    Ok(urls)
}

/// Fails naming the first name in the local directory `dir` with the
/// format's `extension` that cannot be looked up, or `dir` if it cannot be
/// listed.
///
/// DataFusion's listing skips such a name as if it were a link to nothing,
/// so a directory that can be listed but not searched read as holding no
/// file, and its path raised `NotFound` (#261). The cost is one more lookup
/// of each file the listing reads.
fn look_up_files(dir: &str, extension: &str) -> Result<()> {
    for entry in python_glob::listdir(dir)? {
        if entry.name.ends_with(extension) {
            python_glob::metadata(std::path::Path::new(dir).join(&entry.name))?;
        }
    }
    Ok(())
}

/// The URL of the existing local file or directory `path`, naming it as it
/// is written.
///
/// It is the URL `ListingTableUrl::parse` gives a path without glob
/// characters: absolute against the working directory, with `.` and `..`
/// resolved without following links, and a directory's ending with `/`. It
/// carries no glob, so a name with `*`, `?` or `[` is that file.
fn local_url(path: &std::path::Path) -> Result<ListingTableUrl> {
    let mut absolute = PathBuf::new();
    for component in std::path::absolute(path)?.components() {
        match component {
            Component::ParentDir => {
                absolute.pop();
            }
            component => absolute.push(component),
        }
    }
    let location =
        Path::from_absolute_path(&absolute).map_err(|e| DataFusionError::External(Box::new(e)))?;
    as_collection_if_directory(file_url(&ListingTableUrl::parse("file:///")?, &location)?)
}

/// The table `df` reads with `order` declared as the order of each file's
/// rows, for `assume_sorted`: the same files and schema, read as
/// [`read_files`] reads them. `None` if `df` is not a scan of listed files.
pub(crate) fn with_file_sort_order(
    session: &SessionContext,
    df: &DataFrame,
    order: Vec<Vec<SortExpr>>,
) -> Result<Option<DataFrame>> {
    let LogicalPlan::TableScan(scan) = df.logical_plan() else {
        return Ok(None);
    };
    let provider = source_as_provider(&scan.source)?;
    let Some(table) = provider.downcast_ref::<ListingTable>() else {
        return Ok(None);
    };
    // `read_files` declares no partition columns, so the table's schema is
    // its files' schema.
    let config = ListingTableConfig::new_with_multi_paths(table.table_paths().clone())
        .with_listing_options(table.options().clone().with_file_sort_order(order))
        .with_schema(table.schema());
    let table = ListingTable::try_new(config)?;
    session.read_table(Arc::new(table)).map(Some)
}

/// `url` with the trailing `/` of a collection if it is a `file` URL of a
/// directory.
///
/// `ListingTableUrl::parse` ends a path of a directory with `/`, but takes a
/// URL as written. DataFusion lists a URL without the `/` as a directory
/// when looking it up as a file finds none, which [`LocalFiles`] makes an
/// error, so the reader resolves the directory here, at the call.
fn as_collection_if_directory(url: ListingTableUrl) -> Result<ListingTableUrl> {
    let is_directory = !url.is_collection()
        && url.scheme() == "file"
        && url.get_url().to_file_path().is_ok_and(|path| path.is_dir());
    if !is_directory {
        return Ok(url);
    }
    let mut directory = url.get_url().clone();
    directory.set_path(&format!("{}/", directory.path()));
    ListingTableUrl::try_new(directory, url.get_glob().clone())
}

/// The URL of the listed file at `location`, in the store of `base`.
///
/// Each segment is percent-encoded, `%` included, so the URL decodes back to
/// `location` (`ListingTableUrl::try_new`) whatever the file is called. It
/// carries no glob, so a name with `*`, `?` or `[` is that file.
fn file_url(base: &ListingTableUrl, location: &Path) -> Result<ListingTableUrl> {
    let mut url = base.get_url().clone();
    url.path_segments_mut()
        .map_err(|()| DataFusionError::Internal(format!("{base} has no path")))?
        .clear()
        .extend(location.parts());
    ListingTableUrl::try_new(url, None)
}

/// The local file store of LTSeq's sessions (`engine`): DataFusion's
/// `LocalFileSystem`, except that a listing that looks up a file and finds
/// none gets an error.
///
/// DataFusion lists a file's URL by looking the file up (`head`), and on
/// `NotFound` lists the URL as a directory instead, which finds nothing, so a
/// listed file that had been deleted read as no rows. This store reports
/// that miss as a `Generic` error, which the listing passes on, so the
/// execution fails and names the file. A directory at the file's name is
/// such a miss too (`LocalFileSystem` reports it as `NotFound`), so an
/// execution never reads the files in it; a reader resolves a directory it
/// is given before any lookup ([`as_collection_if_directory`]). In
/// DataFusion 55 only that listing looks a file up this way; reading a
/// missing file's contents is `NotFound` as before.
///
/// The lookup also drops the file's footer from the session's footer cache.
/// DataFusion reuses a cached Parquet footer while the file's size and
/// modification time are unchanged, so a rewrite that kept both was read by
/// the old footer: `count()` gave its row count, and a filter lost the rows
/// that its statistics pruned (#359). An execution looks up each listed file
/// before it reads the file's footer, so it reads the footer afresh; its
/// partitions then share the copy DataFusion caches.
#[derive(Debug)]
pub(crate) struct LocalFiles {
    files: LocalFileSystem,
    footers: Arc<FileMetadataCache>,
}

impl LocalFiles {
    /// The store of a session whose footer cache is `footers`.
    pub(crate) fn new(footers: Arc<FileMetadataCache>) -> Self {
        LocalFiles {
            files: LocalFileSystem::default(),
            footers,
        }
    }
}

impl fmt::Display for LocalFiles {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.files.fmt(f)
    }
}

#[async_trait]
impl ObjectStore for LocalFiles {
    async fn get_opts(&self, location: &Path, options: GetOptions) -> StoreResult<GetResult> {
        let head = options.head;
        if head {
            self.footers.remove(location);
        }
        self.files
            .get_opts(location, options)
            .await
            .map_err(|e| match e {
                StoreError::NotFound { .. } if head => StoreError::Generic {
                    store: "LocalFileSystem",
                    source: Box::new(e),
                },
                e => e,
            })
    }

    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> StoreResult<PutResult> {
        self.files.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> StoreResult<Box<dyn MultipartUpload>> {
        self.files.put_multipart_opts(location, opts).await
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> StoreResult<Vec<Bytes>> {
        self.files.get_ranges(location, ranges).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, StoreResult<Path>>,
    ) -> BoxStream<'static, StoreResult<Path>> {
        self.files.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, StoreResult<ObjectMeta>> {
        self.files.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, StoreResult<ObjectMeta>> {
        self.files.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> StoreResult<ListResult> {
        self.files.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> StoreResult<()> {
        self.files.copy_opts(from, to, options).await
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> StoreResult<()> {
        self.files.rename_opts(from, to, options).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::RUNTIME;
    use datafusion::object_store::ObjectStoreExt;

    #[test]
    fn a_missing_file_is_an_error_when_looked_up_and_not_found_when_read() {
        let dir = tempfile::tempdir().expect("temporary directory");
        let path = dir.path().join("gone.csv");
        let location = Path::from_absolute_path(&path).expect("location");
        let footers = SessionContext::new()
            .runtime_env()
            .cache_manager
            .get_file_metadata_cache();
        let store = LocalFiles::new(footers);

        let head = RUNTIME.block_on(store.head(&location)).unwrap_err();
        assert!(matches!(head, StoreError::Generic { .. }), "{head}");
        assert!(head.to_string().contains("gone.csv"), "{head}");
        let get = RUNTIME.block_on(store.get(&location)).unwrap_err();
        assert!(matches!(get, StoreError::NotFound { .. }), "{get}");

        std::fs::write(&path, "x\n1\n").expect("write");
        let meta = RUNTIME.block_on(store.head(&location)).expect("head");
        assert_eq!(meta.size, 4);
    }

    #[test]
    fn a_file_url_decodes_to_the_listed_location() {
        // A listing's locations hold names as they are on disk.
        let base = ListingTableUrl::parse("file:///data/").expect("base");
        for name in ["plain.csv", "a b.csv", "a%20b.csv", "x[1]*?.csv", "#é.csv"] {
            let location = Path::parse(format!("data/{name}")).expect("location");
            let url = file_url(&base, &location).expect("file URL");
            assert_eq!(url.prefix(), &location, "{name}");
            assert!(url.get_glob().is_none() && !url.is_collection(), "{name}");
        }
    }

    #[test]
    fn a_file_url_of_a_directory_is_its_collection() {
        let dir = tempfile::tempdir().expect("temporary directory");
        std::fs::write(dir.path().join("file.csv"), "x\n1\n").expect("write");
        for name in ["plain.csv", "a b%20#é.csv"] {
            std::fs::create_dir(dir.path().join(name)).expect("directory");
        }
        for name in ["plain.csv", "a b%20#é.csv", "file.csv", "missing.csv"] {
            let path = dir.path().join(name);
            // A path of a directory gets its `/`; the URL as written has none.
            let parsed = ListingTableUrl::parse(path.to_str().expect("UTF-8")).expect("path");
            let url = ListingTableUrl::parse(parsed.as_str().trim_end_matches('/')).expect("url");
            assert!(!url.is_collection(), "{name}");

            let resolved = as_collection_if_directory(url.clone()).expect("resolved");
            let expected = if path.is_dir() { &parsed } else { &url };
            assert_eq!(resolved.as_str(), expected.as_str(), "{name}");
            assert_eq!(resolved.prefix(), url.prefix(), "{name}");
        }
    }
}
