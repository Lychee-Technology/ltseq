//! File sources: the files a reader lists, and what each execution reads of
//! them (#262).
//!
//! A reader lists its files once, at the call ([`read_files`]), and the
//! table reads exactly those: a file that its directory or glob matches only
//! later is never read. Each execution looks every listed file up again, so
//! it reads the file as it is at that moment, and one that has disappeared
//! fails the execution ([`LocalFiles`]).
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
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use datafusion::catalog::TableProvider;
use datafusion::common::{exec_err, plan_err, DataFusionError, Result};
use datafusion::dataframe::DataFrame;
use datafusion::datasource::file_format::options::ReadOptions;
use datafusion::datasource::listing::{ListingTable, ListingTableConfig, ListingTableUrl};
use datafusion::datasource::source_as_provider;
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

/// The table `SessionContext::read_csv` or `read_parquet` builds for `path`,
/// over the files `path` names now.
///
/// `path` resolves as DataFusion resolves it: a file, the files with the
/// format's extension in a directory, or the files a glob matches. The table
/// lists each of them by its own URL, so an execution looks up each file and
/// no directory. The schema is inferred from these files as DataFusion
/// infers it from its listing; `options` carry none, as LTSeq's readers
/// give none.
pub(crate) async fn read_files<'a>(
    session: &SessionContext,
    path: &str,
    options: impl ReadOptions<'a>,
) -> Result<DataFrame> {
    let url = ListingTableUrl::parse(path)?;
    let options =
        options.to_listing_options(&session.copied_config(), session.copied_table_options());
    let extension = options.file_extension.as_str();
    // `SessionContext::read_csv` and `read_parquet` refuse such a file too.
    if !url.is_collection() && !url.as_str().ends_with(extension) {
        return exec_err!(
            "File path '{}' does not match the expected extension '{extension}'",
            url.as_str()
        );
    }

    let state = session.state();
    let store = state.runtime_env().object_store(&url)?;
    let files: Vec<ObjectMeta> = url
        .list_all_files(&state, store.as_ref(), extension)
        .await?
        .try_collect()
        .await?;
    // From here on as `ListingOptions::infer_schema`, over these files.
    if files.is_empty() {
        return plan_err!(
            "No files found at {url}. Cannot infer schema from an empty location; \
             either add data files or declare an explicit schema for the table."
        );
    }
    // An empty file cannot add to the schema, and may fail when read for it.
    let sized: Vec<ObjectMeta> = files.iter().filter(|f| f.size > 0).cloned().collect();
    let schema = options.format.infer_schema(&state, &store, &sized).await?;

    let paths = files
        .iter()
        .map(|file| file_url(&url, &file.location))
        .collect::<Result<Vec<_>>>()?;
    let config = ListingTableConfig::new_with_multi_paths(paths)
        .with_listing_options(options)
        .with_schema(schema);
    let table = ListingTable::try_new(config)?
        .with_cache(state.runtime_env().cache_manager.get_file_statistic_cache());
    session.read_table(Arc::new(table))
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
    let table = ListingTable::try_new(config)?.with_cache(
        session
            .runtime_env()
            .cache_manager
            .get_file_statistic_cache(),
    );
    session.read_table(Arc::new(table)).map(Some)
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
/// `LocalFileSystem`, except that a missing file is an error when a listing
/// looks it up.
///
/// DataFusion lists a file's URL by looking the file up (`head`), and on
/// `NotFound` lists the URL as a directory instead, which finds nothing, so a
/// listed file that had been deleted read as no rows. This store reports
/// that miss as a `Generic` error, which the listing passes on, so the
/// execution fails and names the file. In DataFusion 55 only that listing
/// looks a file up this way; reading a missing file's contents is
/// `NotFound` as before.
#[derive(Debug, Default)]
pub(crate) struct LocalFiles(LocalFileSystem);

impl fmt::Display for LocalFiles {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[async_trait]
impl ObjectStore for LocalFiles {
    async fn get_opts(&self, location: &Path, options: GetOptions) -> StoreResult<GetResult> {
        let head = options.head;
        self.0
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
        self.0.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> StoreResult<Box<dyn MultipartUpload>> {
        self.0.put_multipart_opts(location, opts).await
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> StoreResult<Vec<Bytes>> {
        self.0.get_ranges(location, ranges).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, StoreResult<Path>>,
    ) -> BoxStream<'static, StoreResult<Path>> {
        self.0.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, StoreResult<ObjectMeta>> {
        self.0.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, StoreResult<ObjectMeta>> {
        self.0.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> StoreResult<ListResult> {
        self.0.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> StoreResult<()> {
        self.0.copy_opts(from, to, options).await
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> StoreResult<()> {
        self.0.rename_opts(from, to, options).await
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
        let store = LocalFiles::default();

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
}
