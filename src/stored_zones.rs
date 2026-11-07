//! The zone of a Parquet timestamp whose stored Arrow type has another unit
//! (#362).
//!
//! Parquet records of an aware timestamp only that it is adjusted to UTC. A
//! writer keeps the zone name in the Arrow schema it stores in the file
//! (`ARROW:schema`). The `parquet` crate's `apply_hint` takes the stored type
//! only when its unit is the column's, so a column stored in another unit
//! reads as `UTC`. pyarrow writes such columns itself: it stores a
//! `timestamp[s, tz=Z]` as `TIMESTAMP(MILLIS, isAdjustedToUTC=true)` and keeps
//! `timestamp[s, tz=Z]` in the schema, and `coerce_timestamps` does the same
//! for any unit. pyarrow reads them back as `timestamp[ms, tz=Z]`, and contract
//! §4.3 has `read_parquet` report what pyarrow reads.
//!
//! [`infer_schema`] restores the zone in the schema a table is built with, and
//! each file's column stays `UTC` as the reader decodes it. DataFusion's scan
//! casts it to the table's type, which leaves its instants as they are, and
//! its statistics take the table's type too. What reads a file without
//! DataFusion (`parallel_scan`) sees `UTC`: a group count compares instants,
//! and a pattern count that could tell the zones apart takes the general path.
//! Remove this module with the `parquet` upgrade whose `apply_hint` takes the
//! zone of a stored type in another unit, once #362's tests pass without it.

use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Fields, Schema, SchemaRef};
use datafusion::catalog::Session;
use datafusion::common::Result;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::physical_plan::parquet::metadata::DFParquetMetadata;
use datafusion::datasource::physical_plan::parquet::{
    transform_binary_to_string, transform_schema_to_view,
};
use datafusion::execution::cache::cache_manager::FileMetadataCache;
use datafusion::object_store::{ObjectMeta, ObjectStore};
use datafusion::parquet::arrow::parquet_to_arrow_schema;
use datafusion::parquet::basic::Type as PhysicalType;
use datafusion::parquet::errors::Result as ParquetResult;
use datafusion::parquet::file::metadata::ParquetMetaData;
use datafusion::parquet::schema::types::{SchemaDescriptor, Type, TypePtr};
use futures_util::{stream, StreamExt, TryStreamExt};

/// The schema of the Parquet `files`, as `format.infer_schema` gives it but
/// with each file's stored zones restored.
///
/// The zones are restored in each file before the schemas merge, so two files
/// that store different zones fail to merge, as they do when each column's
/// unit is its stored unit, and two that store one zone merge whatever their
/// units. Otherwise this follows `format.infer_schema`: files merge in the
/// order of their locations, and `format`'s transforms apply to the merged
/// schema, so a `string` and a `large_string` column do not merge as views.
pub(crate) async fn infer_schema(
    format: &ParquetFormat,
    state: &dyn Session,
    store: &Arc<dyn ObjectStore>,
    files: &[ObjectMeta],
) -> Result<SchemaRef> {
    let mut files: Vec<&ObjectMeta> = files.iter().collect();
    files.sort_by(|a, b| a.location.cmp(&b.location));
    let untransformed = ParquetFormat::new()
        .with_options(format.options().clone())
        .with_binary_as_string(false)
        .with_force_view_types(false);
    let footers = state.runtime_env().cache_manager.get_file_metadata_cache();
    let concurrency = state.config_options().execution.meta_fetch_concurrency;
    let schemas: Vec<Schema> = stream::iter(files)
        .map(|file| file_schema(&untransformed, state, store, &footers, file))
        .buffered(concurrency.get())
        .try_collect()
        .await?;
    let mut schema = Schema::try_merge(schemas)?;
    if format.binary_as_string() {
        schema = transform_binary_to_string(&schema);
    }
    if format.force_view_types() {
        schema = transform_schema_to_view(&schema);
    }
    Ok(Arc::new(schema))
}

/// The schema of one file. `format` leaves the file's footer in `footers`, so
/// reading the stored types from it fetches nothing more.
async fn file_schema(
    format: &ParquetFormat,
    state: &dyn Session,
    store: &Arc<dyn ObjectStore>,
    footers: &Arc<FileMetadataCache>,
    file: &ObjectMeta,
) -> Result<Schema> {
    let inferred = format
        .infer_schema(state, store, std::slice::from_ref(file))
        .await?;
    let footer = DFParquetMetadata::new(store.as_ref(), file)
        .with_metadata_size_hint(format.metadata_size_hint())
        .with_file_metadata_cache(Some(Arc::clone(footers)))
        .fetch_metadata()
        .await?;
    Ok(Schema::new_with_metadata(
        with_zones(inferred.fields(), stored_timestamps(&footer)?.fields()),
        inferred.metadata().clone(),
    ))
}

/// The file's schema with each timestamp in the type the file stores for it,
/// whatever its unit.
///
/// The `parquet` crate keeps its reading of the stored schema to itself, but
/// `apply_hint` gives a column with no type but INT64 the stored timestamp
/// type as it is, unit included, so that it reads what a bare integer holds.
/// The crate therefore reads the footer's stored schema here against the
/// file's Parquet schema with no type on its INT64 leaves. Without a stored
/// schema they read as `int64`, and no zone is restored.
fn stored_timestamps(footer: &ParquetMetaData) -> ParquetResult<Schema> {
    let file = footer.file_metadata();
    let bare = with_bare_int64(&file.schema_descr().root_schema_ptr())?;
    parquet_to_arrow_schema(&SchemaDescriptor::new(bare), file.key_value_metadata())
}

/// `parquet_type` with no logical or converted type on its INT64 leaves.
fn with_bare_int64(parquet_type: &TypePtr) -> ParquetResult<TypePtr> {
    let info = parquet_type.get_basic_info();
    match parquet_type.as_ref() {
        Type::GroupType { fields, .. } => {
            let fields = fields
                .iter()
                .map(with_bare_int64)
                .collect::<Result<_, _>>()?;
            let group = Type::group_type_builder(info.name())
                .with_converted_type(info.converted_type())
                .with_logical_type(info.logical_type_ref().cloned())
                .with_fields(fields);
            // The root of a schema has no repetition.
            let group = match info.has_repetition() {
                true => group.with_repetition(info.repetition()),
                false => group,
            };
            Ok(Arc::new(group.build()?))
        }
        Type::PrimitiveType {
            physical_type: PhysicalType::INT64,
            ..
        } => {
            let leaf = Type::primitive_type_builder(info.name(), PhysicalType::INT64)
                .with_repetition(info.repetition())
                .build()?;
            Ok(Arc::new(leaf))
        }
        _ => Ok(Arc::clone(parquet_type)),
    }
}

/// `inferred` with the zones of the `stored` fields at the same positions.
///
/// The `parquet` crate requires a stored schema to name a column's fields in
/// the column's order, so a position finds its field unless the file changed
/// between the two reads of its footer. A field then keeps its type.
fn with_zones(inferred: &Fields, stored: &Fields) -> Fields {
    (inferred.iter().enumerate())
        .map(|(i, field)| match stored.get(i) {
            Some(stored) if stored.name() == field.name() => field_with_zones(field, stored),
            _ => Arc::clone(field),
        })
        .collect()
}

fn field_with_zones(inferred: &Field, stored: &Field) -> FieldRef {
    Arc::new(
        inferred
            .clone()
            .with_data_type(type_with_zones(inferred.data_type(), stored.data_type())),
    )
}

/// `inferred`, the type of a file's column, with each timestamp adjusted to
/// UTC taking the zone of its `stored` type, as pyarrow reads it. When the
/// units match, the `parquet` crate has taken the stored type already.
///
/// The `parquet` crate takes a nested column's kind from the stored type, so
/// the two are the same kind wherever the zone can differ. A dictionary is
/// left alone: the crate reads a dictionary of timestamps stored in another
/// unit as plain `UTC` timestamps, and so does pyarrow.
fn type_with_zones(inferred: &DataType, stored: &DataType) -> DataType {
    use DataType::*;
    match (inferred, stored) {
        (Timestamp(unit, Some(utc)), Timestamp(_, Some(zone))) if utc.as_ref() == "UTC" => {
            Timestamp(*unit, Some(Arc::clone(zone)))
        }
        (List(i), List(s)) => List(field_with_zones(i, s)),
        (LargeList(i), LargeList(s)) => LargeList(field_with_zones(i, s)),
        (ListView(i), ListView(s)) => ListView(field_with_zones(i, s)),
        (LargeListView(i), LargeListView(s)) => LargeListView(field_with_zones(i, s)),
        (FixedSizeList(i, n), FixedSizeList(s, _)) => FixedSizeList(field_with_zones(i, s), *n),
        (Map(i, sorted), Map(s, _)) => Map(field_with_zones(i, s), *sorted),
        (Struct(i), Struct(s)) => Struct(with_zones(i, s)),
        _ => inferred.clone(),
    }
}
