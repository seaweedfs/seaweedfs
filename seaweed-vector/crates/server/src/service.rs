use anyhow::{Result, bail};
use arrow_array::{
    Array, ArrayRef, FixedSizeListArray, Float32Array, RecordBatch, StringArray, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema};
use futures::TryStreamExt;
use lance::Dataset;
use lance::dataset::ProjectionRequest;
use lance::dataset::WriteParams;
use lance::dataset::{MergeInsertBuilder, WhenMatched, WhenNotMatched};
use lance::io::WrappingObjectStore;
use lance::table::format::Fragment;
use prost_types::{ListValue, Struct, Value, value::Kind};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use tonic::{Request, Response, Status};
use weed_lance_worker::catalog::TableDescription;
use weed_lance_worker::dataset;
use weed_lance_worker::direct_read::{DirectReads, Wrappers};
use weed_lance_worker::jobs::vector_index::is_vector_table;
use weed_vector_core::index;
use weed_vector_core::query::{self, Query};
use weed_vector_core::table::VectorTable;

use crate::pb::vector::{self as pb, vector_service_server::VectorService};
use crate::read_cache::ReadCache;

fn internal(err: impl Into<anyhow::Error>) -> Status {
    Status::internal(format!("{:#}", err.into()))
}

fn invalid(err: impl Into<anyhow::Error>) -> Status {
    Status::invalid_argument(format!("{:#}", err.into()))
}

fn open_status(err: anyhow::Error) -> Status {
    let missing = err.chain().any(|cause| {
        cause.downcast_ref::<lance::Error>().is_some_and(|e| {
            matches!(
                e,
                lance::Error::DatasetNotFound { .. }
                    | lance::Error::NotFound { .. }
                    | lance::Error::VersionNotFound { .. }
            )
        })
    });
    if missing {
        Status::not_found(format!("{:#}", err))
    } else {
        Status::unavailable(format!("{:#}", err))
    }
}

const KEY_COLUMN: &str = "key";
const METADATA_COLUMN: &str = "metadata";
const VECTOR_COLUMN: &str = "vector";
const ROW_ID_COLUMN: &str = "_rowid";
fn schema_for(
    dimension: usize,
    metric: pb::DistanceMetric,
    non_filterable: &[String],
) -> Arc<Schema> {
    let vector_field = Field::new(
        VECTOR_COLUMN,
        DataType::FixedSizeList(
            Arc::new(Field::new("item", DataType::Float32, false)),
            dimension as i32,
        ),
        false,
    )
    .with_metadata(
        [(
            "distance_metric".to_string(),
            metric.as_str_name().to_string(),
        )]
        .into_iter()
        .collect(),
    );
    // The arrow.json extension marks the Utf8 column as JSON: lance stores it
    // as jsonb, which is what the json_get_* filter functions take.
    let mut metadata_meta =
        HashMap::from_iter([("ARROW:extension:name".to_string(), "arrow.json".to_string())]);
    if !non_filterable.is_empty() {
        metadata_meta.insert(
            "non_filterable_metadata_keys".to_string(),
            serde_json::to_string(non_filterable).unwrap_or_default(),
        );
    }
    let metadata_field =
        Field::new(METADATA_COLUMN, DataType::Utf8, true).with_metadata(metadata_meta);
    Arc::new(Schema::new(vec![
        Field::new(KEY_COLUMN, DataType::Utf8, false),
        vector_field,
        metadata_field,
    ]))
}

fn matches_index_schema(
    dataset: &Dataset,
    dimension: usize,
    metric: pb::DistanceMetric,
    non_filterable: &[String],
) -> bool {
    let Ok(layout) = VectorTable::resolve(dataset, Some(VECTOR_COLUMN), Some(KEY_COLUMN)) else {
        return false;
    };
    let schema = Schema::from(dataset.schema());
    let Ok(vector) = schema.field_with_name(VECTOR_COLUMN) else {
        return false;
    };
    let Ok(metadata) = schema.field_with_name(METADATA_COLUMN) else {
        return false;
    };
    let stored_keys = metadata
        .metadata()
        .get("non_filterable_metadata_keys")
        .map(|keys| serde_json::from_str::<Vec<String>>(keys))
        .transpose();
    let Ok(stored_keys) = stored_keys else {
        return false;
    };
    let mut stored_keys = stored_keys.unwrap_or_default();
    let mut requested_keys = non_filterable.to_vec();
    stored_keys.sort();
    requested_keys.sort();
    layout.dimension == dimension
        && vector.metadata().get("distance_metric").map(String::as_str)
            == Some(metric.as_str_name())
        && stored_keys == requested_keys
        && metadata
            .metadata()
            .get("ARROW:extension:name")
            .is_some_and(|name| name == "lance.json")
}

fn batch_of(vectors: &[pb::Vector], dimension: usize, schema: Arc<Schema>) -> Result<RecordBatch> {
    let mut seen = std::collections::HashSet::new();
    for vector in vectors {
        if !seen.insert(vector.key.as_str()) {
            bail!("duplicate key in request: {}", vector.key);
        }
    }
    let keys = StringArray::from(vectors.iter().map(|v| v.key.as_str()).collect::<Vec<_>>());
    // Check every vector's length before flattening: equal totals would
    // still let a short vector borrow from the next one's values.
    if vectors.iter().any(|v| v.data.len() != dimension) {
        bail!("a vector's data length does not match the index dimension");
    }
    let flat = vectors
        .iter()
        .flat_map(|v| v.data.iter().copied())
        .collect::<Vec<_>>();
    let values: ArrayRef = Arc::new(Float32Array::from(flat));
    // Lance stores the list item as nullable even when the write schema
    // declared it non-null, so the child field must come from the batch's
    // schema rather than a fixed declaration.
    let DataType::FixedSizeList(item_field, _) = schema
        .field_with_name(VECTOR_COLUMN)
        .map_err(anyhow::Error::from)?
        .data_type()
    else {
        bail!("vector column is not a fixed-size list");
    };
    let vectors_array =
        FixedSizeListArray::try_new(item_field.clone(), dimension as i32, values, None)?;
    let metas = vectors
        .iter()
        .map(|v| {
            v.metadata
                .as_ref()
                .map(|m| serde_json::to_string(&struct_to_json(m)))
                .transpose()
        })
        .collect::<Result<Vec<_>, _>>()?;
    // The stored column is lance's jsonb (LargeBinary) once the field carries
    // the arrow.json extension; encode the strings, else keep the Utf8 batch.
    let metadata_array: ArrayRef = match schema
        .field_with_name(METADATA_COLUMN)
        .map(|f| f.data_type())
    {
        Ok(DataType::LargeBinary) => {
            Arc::new(lance_arrow::json::JsonArray::try_from_iter(metas)?.into_inner())
        }
        _ => Arc::new(StringArray::from(metas)),
    };
    RecordBatch::try_new(
        schema,
        vec![Arc::new(keys), Arc::new(vectors_array), metadata_array],
    )
    .map_err(Into::into)
}

fn struct_to_json(s: &Struct) -> serde_json::Value {
    serde_json::Value::Object(
        s.fields
            .iter()
            .map(|(k, v)| (k.clone(), value_to_json(v)))
            .collect(),
    )
}

fn value_to_json(v: &Value) -> serde_json::Value {
    match &v.kind {
        None | Some(Kind::NullValue(_)) => serde_json::Value::Null,
        Some(Kind::NumberValue(n)) => serde_json::json!(n),
        Some(Kind::StringValue(s)) => serde_json::json!(s),
        Some(Kind::BoolValue(b)) => serde_json::json!(b),
        Some(Kind::StructValue(s)) => struct_to_json(s),
        Some(Kind::ListValue(l)) => {
            serde_json::Value::Array(l.values.iter().map(value_to_json).collect())
        }
    }
}

fn json_to_value(v: serde_json::Value) -> Value {
    let kind = match v {
        serde_json::Value::Null => Kind::NullValue(0),
        serde_json::Value::Bool(b) => Kind::BoolValue(b),
        serde_json::Value::Number(n) => Kind::NumberValue(n.as_f64().unwrap_or(0.0)),
        serde_json::Value::String(s) => Kind::StringValue(s),
        serde_json::Value::Array(items) => Kind::ListValue(ListValue {
            values: items.into_iter().map(json_to_value).collect(),
        }),
        serde_json::Value::Object(map) => Kind::StructValue(json_to_struct(map)),
    };
    Value { kind: Some(kind) }
}

fn json_to_struct(map: serde_json::Map<String, serde_json::Value>) -> Struct {
    Struct {
        fields: map
            .into_iter()
            .map(|(k, v)| (k, json_to_value(v)))
            .collect::<BTreeMap<_, _>>(),
    }
}

fn parse_struct(s: &str) -> Result<Struct> {
    let v: serde_json::Value = serde_json::from_str(s)?;
    match v {
        serde_json::Value::Object(map) => Ok(json_to_struct(map)),
        _ => bail!("metadata is not a JSON object"),
    }
}

fn key_list(keys: &[String]) -> String {
    format!(
        "`{KEY_COLUMN}` in ({})",
        keys.iter()
            .map(|k| format!("'{}'", k.replace('\'', "''")))
            .collect::<Vec<_>>()
            .join(", ")
    )
}

/// The columns a GetVectors or ListVectors response carries.
fn projection(return_data: bool, return_metadata: bool) -> Vec<String> {
    let mut columns = vec![KEY_COLUMN.to_string()];
    if return_data {
        columns.push(VECTOR_COLUMN.to_string());
    }
    if return_metadata {
        columns.push(METADATA_COLUMN.to_string());
    }
    columns
}

/// The fragments holding live rows `offset..offset + rows` in scan order, and
/// where `offset` falls in the first of them. A scan reads every listed
/// fragment's deletion file, so a page names only its own. None when the
/// manifest lacks a fragment's live row count.
fn page_fragments(dataset: &Dataset, offset: u64, rows: u64) -> Option<(Vec<Fragment>, u64)> {
    dataset.manifest().writer_version.as_ref()?;
    let (mut start, mut skip, mut out) = (0u64, 0u64, Vec::new());
    for fragment in dataset.fragments().iter() {
        if start >= offset.saturating_add(rows) {
            break;
        }
        let live = fragment.num_rows()? as u64;
        if start + live > offset {
            if out.is_empty() {
                skip = offset - start;
            }
            out.push(fragment.clone());
        }
        start += live;
    }
    Some((out, skip))
}

/// Live rows `offset..offset + rows` in scan order, projected to `columns`.
async fn list_page(
    dataset: &Dataset,
    columns: &[String],
    offset: u64,
    rows: u64,
) -> Result<Vec<RecordBatch>> {
    let mut scanner = dataset.scan();
    scanner.project(columns)?;
    let skip = match page_fragments(dataset, offset, rows) {
        Some((fragments, skip)) => {
            scanner.with_fragments(fragments);
            skip
        }
        None => offset,
    };
    scanner.limit(Some(rows as i64), Some(skip as i64))?;
    Ok(scanner.try_into_stream().await?.try_collect().await?)
}

/// The AWS ListVectors segmentCount limit.
const MAX_SEGMENTS: u64 = 16;

/// Up to `page` rows whose position modulo `count` is `index`, from position
/// `offset`, and the position to resume from. Keys are read across the
/// stride; the other columns only for the rows kept.
async fn list_segment(
    dataset: &Dataset,
    columns: &[String],
    offset: u64,
    page: u64,
    count: u64,
    index: u64,
) -> Result<(Vec<RecordBatch>, Option<u64>)> {
    let first = offset.saturating_add((index + count - offset % count) % count);
    let stride = vec![KEY_COLUMN.to_string(), ROW_ID_COLUMN.to_string()];
    let batches = list_page(dataset, &stride, first, page * count).await?;
    let (mut keys, mut row_ids, mut scanned) = (Vec::new(), Vec::new(), 0u64);
    for batch in &batches {
        let (Some(batch_keys), Some(ids)) = (
            batch
                .column_by_name(KEY_COLUMN)
                .and_then(|c| c.as_any().downcast_ref::<StringArray>()),
            batch
                .column_by_name(ROW_ID_COLUMN)
                .and_then(|c| c.as_any().downcast_ref::<UInt64Array>()),
        ) else {
            bail!("the scan did not return keys and row ids");
        };
        for i in 0..batch.num_rows() {
            if (scanned + i as u64) % count == 0 {
                keys.push(batch_keys.value(i).to_string());
                row_ids.push(ids.value(i));
            }
        }
        scanned += batch.num_rows() as u64;
    }
    let next = (row_ids.len() as u64 == page && scanned > (page - 1) * count + 1)
        .then(|| first.saturating_add((page - 1) * count + 1));
    if row_ids.is_empty() {
        return Ok((Vec::new(), next));
    }
    if columns.len() == 1 {
        let keys: ArrayRef = Arc::new(StringArray::from(keys));
        return Ok((
            vec![RecordBatch::try_from_iter([(KEY_COLUMN, keys)])?],
            next,
        ));
    }
    let projection = ProjectionRequest::from_columns(columns, dataset.schema());
    Ok((vec![dataset.take_rows(&row_ids, projection).await?], next))
}

fn rows_as_vectors(
    batches: &[RecordBatch],
    return_data: bool,
    return_metadata: bool,
) -> Vec<pb::Vector> {
    let mut out = Vec::new();
    for batch in batches {
        let Some(keys) = batch
            .column_by_name(KEY_COLUMN)
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
        else {
            continue;
        };
        let vectors = batch
            .column_by_name(VECTOR_COLUMN)
            .and_then(|c| c.as_any().downcast_ref::<FixedSizeListArray>());
        let metas = batch
            .column_by_name(METADATA_COLUMN)
            .and_then(|c| c.as_any().downcast_ref::<StringArray>());
        for i in 0..batch.num_rows() {
            let data = match (return_data, vectors) {
                (true, Some(list)) => list
                    .value(i)
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .map(|a| a.values().to_vec())
                    .unwrap_or_default(),
                _ => Vec::new(),
            };
            let metadata = match (return_metadata, metas) {
                (true, Some(m)) if !m.is_null(i) => parse_struct(m.value(i)).ok(),
                _ => None,
            };
            out.push(pb::Vector {
                key: keys.value(i).to_string(),
                data,
                metadata,
            });
        }
    }
    out
}

/// Refuses a table of an SSE-S3 bucket when the engine's storage has no key
/// to read or write it with, rather than failing inside Lance.
fn sse_s3_ready(sse_s3: bool, key_configured: Option<bool>) -> Result<(), Status> {
    if sse_s3 && key_configured == Some(false) {
        return Err(Status::failed_precondition(
            "the bucket encrypts with SSE-S3, which needs s3.sse.kek or s3.sse.key for the \
             engine as for the S3 gateway",
        ));
    }
    Ok(())
}

/// The description a gateway sent with the table. Only a vector index at its
/// colocated path is taken, since the engine is that path's writer already;
/// any other location would point the engine at storage the catalog does not
/// know.
fn resolved(table: &pb::Table, schemes: &[&str]) -> Option<TableDescription> {
    // Each id part is one path component; `..` would resolve the colocated
    // path elsewhere on the filer.
    if !table
        .id
        .iter()
        .all(|part| !part.is_empty() && part != "." && part != ".." && !part.contains('/'))
    {
        return None;
    }
    let colocated = dataset::table_location(&table.id)?;
    let path = colocated.strip_prefix("s3://")?;
    let location = table.location.trim_end_matches('/');
    let trusted = is_vector_table(&table.id)
        && schemes
            .iter()
            .any(|scheme| location == format!("{scheme}://{path}"));
    trusted.then(|| TableDescription {
        location: location.to_string(),
        storage_options: dataset::unsigned_options(),
        managed_versioning: false,
    })
}

pub struct VectorServer {
    storage: Option<DirectReads>,
    read_cache: Option<ReadCache>,
    nprobes: Option<usize>,
    /// URI schemes a table's location may have.
    schemes: &'static [&'static str],
}

impl Default for VectorServer {
    fn default() -> Self {
        Self::new()
    }
}

impl VectorServer {
    pub fn new() -> Self {
        Self {
            storage: None,
            read_cache: None,
            nprobes: None,
            schemes: &["s3"],
        }
    }

    /// Tables on an in-process store, for tests.
    #[cfg(test)]
    pub(crate) fn with_schemes(mut self, schemes: &'static [&'static str]) -> Self {
        self.schemes = schemes;
        self
    }

    /// Partitions every query probes, instead of the index's own default.
    pub fn with_nprobes(mut self, nprobes: Option<usize>) -> Self {
        self.nprobes = nprobes;
        self
    }

    /// Keep tables on the filers and volume servers, not through the gateway.
    /// Unset only for tests on an in-process store.
    pub fn with_storage(mut self, storage: DirectReads) -> Self {
        self.storage = Some(storage);
        self
    }

    /// Serve repeated reads of write-once table files from `cache`.
    pub fn with_read_cache(mut self, cache: ReadCache) -> Self {
        self.read_cache = Some(cache);
        self
    }

    /// The table as the gateway resolved it. A request that names no
    /// location, another location than the index's own, or storage options
    /// the engine's credentials must not be used with is refused.
    fn describe(&self, table: &pb::Table) -> Result<TableDescription, Status> {
        if table.location.is_empty() {
            return Err(Status::invalid_argument("the table names no location"));
        }
        let description = resolved(table, self.schemes).ok_or_else(|| {
            Status::permission_denied(format!(
                "{} is not the location of index {}",
                table.location,
                table.id.join("/")
            ))
        })?;
        sse_s3_ready(
            table.sse_s3,
            self.storage.as_ref().map(|s| s.sse_s3_configured()),
        )?;
        Ok(description)
    }

    /// The store Lance opens a table with: direct storage innermost, the read
    /// cache outside it so a hit never reaches storage.
    fn wrapper(&self, table: &pb::Table) -> Option<Arc<dyn WrappingObjectStore>> {
        let mut wrappers: Vec<Arc<dyn WrappingObjectStore>> = Vec::new();
        if let Some(storage) = &self.storage {
            wrappers.push(Arc::new(storage.encrypting(table.sse_s3)));
        }
        if let Some(cache) = &self.read_cache {
            wrappers.push(Arc::new(cache.clone()));
        }
        (!wrappers.is_empty()).then(|| Arc::new(Wrappers(wrappers)) as Arc<dyn WrappingObjectStore>)
    }

    async fn open(&self, table: &pb::Table) -> Result<Dataset, Status> {
        let description = self.describe(table)?;
        let wrapper = self.wrapper(table);
        let mut dataset = dataset::open_at(&description, wrapper)
            .await
            .map_err(open_status)?;
        if table.version != 0 {
            dataset = dataset
                .checkout_version(table.version)
                .await
                .map_err(|err| open_status(err.into()))?;
        }
        Ok(dataset)
    }
}

#[tonic::async_trait]
impl VectorService for VectorServer {
    async fn create_index(
        &self,
        request: Request<pb::CreateIndexRequest>,
    ) -> Result<Response<pb::CreateIndexResponse>, Status> {
        let req = request.into_inner();
        let metric = req.distance_metric();
        let table = req
            .table
            .ok_or_else(|| Status::invalid_argument("missing table"))?;
        let dimension = req.dimension as usize;
        // S3 Vectors caps at 4096; reject beyond it instead of letting an
        // oversized request build a huge Arrow schema.
        if dimension == 0 || dimension > 4096 {
            return Err(Status::invalid_argument("dimension must be 1..=4096"));
        }
        // The gateway has already created the table through the catalog.
        let description = self.describe(&table)?;
        let schema = schema_for(dimension, metric, &req.non_filterable_metadata_keys);
        let empty = RecordBatch::new_empty(schema.clone());
        let reader = arrow_array::RecordBatchIterator::new(vec![Ok(empty)].into_iter(), schema);
        let dataset = Dataset::write(
            reader,
            &description.location,
            Some(WriteParams {
                store_params: Some(dataset::store_params(self.wrapper(&table))),
                ..Default::default()
            }),
        )
        .await;
        let dataset = match dataset {
            Ok(dataset) => dataset,
            Err(error) => match self.open(&table).await {
                Ok(existing)
                    if matches_index_schema(
                        &existing,
                        dimension,
                        metric,
                        &req.non_filterable_metadata_keys,
                    ) =>
                {
                    existing
                }
                Ok(_) => {
                    return Err(Status::already_exists(
                        "index exists with a different schema",
                    ));
                }
                Err(_) => return Err(internal(error)),
            },
        };
        Ok(Response::new(pb::CreateIndexResponse {
            version: dataset.version().version,
        }))
    }

    async fn drop_index(
        &self,
        request: Request<pb::DropIndexRequest>,
    ) -> Result<Response<pb::DropIndexResponse>, Status> {
        let req = request.into_inner();
        let table = req
            .table
            .ok_or_else(|| Status::invalid_argument("missing table"))?;
        // describe() already refused any location but the index's own.
        let description = self.describe(&table)?;
        match &self.storage {
            Some(storage) => storage
                .remove_table(&description.location)
                .await
                .map_err(internal)?,
            None => {
                let (store, path) = lance::io::ObjectStore::from_uri_and_params(
                    Arc::new(lance::io::ObjectStoreRegistry::default()),
                    &description.location,
                    &dataset::store_params(self.wrapper(&table)),
                )
                .await
                .map_err(internal)?;
                store.remove_dir_all(path).await.map_err(internal)?;
            }
        }
        Ok(Response::new(pb::DropIndexResponse {}))
    }

    async fn put_vectors(
        &self,
        request: Request<pb::PutVectorsRequest>,
    ) -> Result<Response<pb::PutVectorsResponse>, Status> {
        let req = request.into_inner();
        let table = req
            .table
            .ok_or_else(|| Status::invalid_argument("missing table"))?;
        let dataset = self.open(&table).await?;
        if req.vectors.is_empty() {
            return Ok(Response::new(pb::PutVectorsResponse {
                version: dataset.version().version,
            }));
        }
        let dimension = VectorTable::resolve(&dataset, None, Some(KEY_COLUMN))
            .map_err(invalid)?
            .dimension;
        let batch = batch_of(
            &req.vectors,
            dimension,
            Arc::new(Into::<arrow_schema::Schema>::into(dataset.schema())),
        )
        .map_err(invalid)?;
        // Upsert by key in one merge commit: no window where the old rows are
        // already deleted, and a losing writer fails the request instead of
        // leaving duplicate keys.
        let mut builder =
            MergeInsertBuilder::try_new(Arc::new(dataset), vec![KEY_COLUMN.to_string()])
                .map_err(internal)?;
        builder.when_matched(WhenMatched::UpdateAll);
        builder.when_not_matched(WhenNotMatched::InsertAll);
        let (dataset, _stats) = builder
            .try_build()
            .map_err(internal)?
            .execute_batches(vec![batch])
            .await
            .map_err(internal)?;
        Ok(Response::new(pb::PutVectorsResponse {
            version: dataset.version().version,
        }))
    }

    async fn get_vectors(
        &self,
        request: Request<pb::GetVectorsRequest>,
    ) -> Result<Response<pb::GetVectorsResponse>, Status> {
        let req = request.into_inner();
        let table = req
            .table
            .ok_or_else(|| Status::invalid_argument("missing table"))?;
        let dataset = self.open(&table).await?;
        // Lease the resolved version so maintenance cannot reclaim its files
        // while this query runs against it.
        let _lease = weed_vector_core::maintenance::try_pin(&dataset)
            .await
            .map_err(internal)?;
        let resolved = VectorTable::resolve(&dataset, None, Some(KEY_COLUMN)).map_err(invalid)?;
        let columns = projection(req.return_data, req.return_metadata);
        let batches = query::get(&dataset, &resolved, &req.keys, Some(&columns))
            .await
            .map_err(internal)?;
        Ok(Response::new(pb::GetVectorsResponse {
            vectors: rows_as_vectors(&batches, req.return_data, req.return_metadata),
        }))
    }

    async fn delete_vectors(
        &self,
        request: Request<pb::DeleteVectorsRequest>,
    ) -> Result<Response<pb::DeleteVectorsResponse>, Status> {
        let req = request.into_inner();
        let table = req
            .table
            .ok_or_else(|| Status::invalid_argument("missing table"))?;
        let mut dataset = self.open(&table).await?;
        if !req.keys.is_empty() {
            dataset
                .delete(&key_list(&req.keys))
                .await
                .map_err(internal)?;
        }
        Ok(Response::new(pb::DeleteVectorsResponse {
            version: dataset.version().version,
        }))
    }

    async fn list_vectors(
        &self,
        request: Request<pb::ListVectorsRequest>,
    ) -> Result<Response<pb::ListVectorsResponse>, Status> {
        let req = request.into_inner();
        let table = req
            .table
            .ok_or_else(|| Status::invalid_argument("missing table"))?;
        let segments = req.segment_count as u64;
        if segments > MAX_SEGMENTS || (segments > 0 && req.segment_index as u64 >= segments) {
            return Err(Status::invalid_argument(format!(
                "segment_index must be below segment_count, which is at most {MAX_SEGMENTS}"
            )));
        }
        let dataset = self.open(&table).await?;
        let offset: u64 = if req.next_token.is_empty() {
            0
        } else {
            req.next_token
                .parse()
                .map_err(|_| Status::invalid_argument("invalid next_token"))?
        };
        let columns = projection(req.return_data, req.return_metadata);
        // An unset max_results arrives as 0; page it like S3 Vectors does.
        const DEFAULT_PAGE: u32 = 500;
        const MAX_PAGE: u32 = 1000;
        let page = match req.max_results {
            0 => DEFAULT_PAGE,
            n => n.min(MAX_PAGE),
        };
        if segments > 1 {
            // Data and metadata come from a second read of this version, so
            // lease it as get_vectors does.
            let _lease = if columns.len() > 1 {
                weed_vector_core::maintenance::try_pin(&dataset)
                    .await
                    .map_err(internal)?
            } else {
                None
            };
            let (vectors, next) = list_segment(
                &dataset,
                &columns,
                offset,
                page as u64,
                segments,
                req.segment_index as u64,
            )
            .await
            .map_err(internal)?;
            return Ok(Response::new(pb::ListVectorsResponse {
                vectors: rows_as_vectors(&vectors, req.return_data, req.return_metadata),
                next_token: next.map(|n| n.to_string()).unwrap_or_default(),
                segmented: true,
            }));
        }
        let batches = list_page(&dataset, &columns, offset, page as u64)
            .await
            .map_err(internal)?;
        let rows = batches.iter().map(|b| b.num_rows()).sum::<usize>();
        let next = offset + rows as u64;
        let next_token = if rows == page as usize && rows > 0 {
            next.to_string()
        } else {
            String::new()
        };
        Ok(Response::new(pb::ListVectorsResponse {
            vectors: rows_as_vectors(&batches, req.return_data, req.return_metadata),
            next_token,
            segmented: segments > 0,
        }))
    }

    async fn query_vectors(
        &self,
        request: Request<pb::QueryVectorsRequest>,
    ) -> Result<Response<pb::QueryVectorsResponse>, Status> {
        let req = request.into_inner();
        let table = req
            .table
            .ok_or_else(|| Status::invalid_argument("missing table"))?;
        let dataset = self.open(&table).await?;
        // Lease the resolved version so maintenance cannot reclaim its files
        // while this query runs against it.
        let _lease = weed_vector_core::maintenance::try_pin(&dataset)
            .await
            .map_err(internal)?;
        let resolved = VectorTable::resolve(&dataset, None, Some(KEY_COLUMN)).map_err(invalid)?;
        if req.query_vector.len() != resolved.dimension {
            return Err(Status::invalid_argument(format!(
                "query vector carries {} floats, the index dimension is {}",
                req.query_vector.len(),
                resolved.dimension
            )));
        }
        if req.top_k == 0 {
            return Err(Status::invalid_argument("top_k must be positive"));
        }
        let distance = index::distance_metric(&dataset, VECTOR_COLUMN);
        let query = Query {
            vector: req.query_vector,
            k: req.top_k as usize,
            filter: if req.filter.is_empty() {
                None
            } else {
                Some(req.filter)
            },
            postfilter: req.postfilter,
            distance,
            nprobes: self.nprobes,
            index_segments: None,
            fragments: None,
        };
        let hits = query::ann(&dataset, &resolved, &query)
            .await
            .map_err(internal)?;
        // One batched read for every hit's columns; keyed by the hit key so
        // row order does not matter.
        let mut metas: std::collections::HashMap<String, Option<Struct>> =
            std::collections::HashMap::new();
        let mut data_map: std::collections::HashMap<String, Vec<f32>> =
            std::collections::HashMap::new();
        if (req.return_metadata || req.return_data) && !hits.is_empty() {
            let row_ids: Vec<u64> = hits.iter().map(|h| h.row_id).collect();
            let mut columns = vec![KEY_COLUMN];
            if req.return_metadata {
                columns.push(METADATA_COLUMN);
            }
            if req.return_data {
                columns.push(VECTOR_COLUMN);
            }
            let batch = dataset
                .take_rows(
                    &row_ids,
                    ProjectionRequest::from_columns(columns, dataset.schema()),
                )
                .await
                .map_err(internal)?;
            let keys = batch
                .column_by_name(KEY_COLUMN)
                .and_then(|c| c.as_any().downcast_ref::<StringArray>());
            let meta_col = batch
                .column_by_name(METADATA_COLUMN)
                .and_then(|c| c.as_any().downcast_ref::<StringArray>());
            let data_col = batch
                .column_by_name(VECTOR_COLUMN)
                .and_then(|c| c.as_any().downcast_ref::<FixedSizeListArray>());
            if let Some(keys) = keys {
                for i in 0..batch.num_rows() {
                    let key = keys.value(i).to_string();
                    if let Some(meta_col) = meta_col {
                        metas.insert(
                            key.clone(),
                            if meta_col.is_null(i) {
                                None
                            } else {
                                parse_struct(meta_col.value(i)).ok()
                            },
                        );
                    }
                    if let Some(data_col) = data_col {
                        data_map.insert(
                            key,
                            data_col
                                .value(i)
                                .as_any()
                                .downcast_ref::<Float32Array>()
                                .map(|a| a.values().to_vec())
                                .unwrap_or_default(),
                        );
                    }
                }
            }
        }
        let mut vectors = Vec::with_capacity(hits.len());
        for hit in &hits {
            vectors.push(pb::QueryResult {
                key: hit.key.clone(),
                distance: if req.return_distance {
                    hit.distance
                } else {
                    0.0
                },
                metadata: metas.remove(&hit.key).flatten(),
                data: data_map.remove(&hit.key).unwrap_or_default(),
            });
        }
        Ok(Response::new(pb::QueryVectorsResponse { vectors }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn batch_of_rejects_a_ragged_vector() {
        let vectors = vec![
            pb::Vector {
                key: "a".to_string(),
                data: vec![1.0, 2.0],
                metadata: None,
            },
            pb::Vector {
                key: "b".to_string(),
                data: vec![1.0; 4],
                metadata: None,
            },
        ];
        let schema = schema_for(4, pb::DistanceMetric::L2, &[]);
        assert!(batch_of(&vectors, 4, schema).is_err());
    }

    #[test]
    fn batch_of_rejects_a_repeated_key() {
        let schema = schema_for(4, pb::DistanceMetric::L2, &[]);
        let vectors = vec![
            pb::Vector {
                key: "a".to_string(),
                data: vec![1.0; 4],
                metadata: None,
            },
            pb::Vector {
                key: "a".to_string(),
                data: vec![2.0; 4],
                metadata: None,
            },
        ];
        assert!(batch_of(&vectors, 4, schema).is_err());
    }

    fn vectors(keys: std::ops::Range<usize>) -> Vec<pb::Vector> {
        keys.map(|i| pb::Vector {
            key: format!("k{i:03}"),
            data: vec![i as f32, 1.0],
            metadata: (i % 3 != 0)
                .then(|| parse_struct(&format!(r#"{{"n":{i},"tag":"t{}"}}"#, i % 2)).unwrap()),
        })
        .collect()
    }

    /// One fragment per range, then the given keys deleted.
    async fn fragmented(
        dir: &tempfile::TempDir,
        ranges: &[std::ops::Range<usize>],
        deleted: &[String],
    ) -> Dataset {
        let uri = dir.path().to_str().unwrap();
        let schema = schema_for(2, pb::DistanceMetric::L2, &[]);
        let mut dataset: Option<Dataset> = None;
        for range in ranges {
            let batch = batch_of(&vectors(range.clone()), 2, schema.clone()).unwrap();
            let reader = arrow_array::RecordBatchIterator::new(vec![Ok(batch)], schema.clone());
            dataset = Some(match dataset {
                None => Dataset::write(reader, uri, None).await.unwrap(),
                Some(mut d) => {
                    d.append(reader, None).await.unwrap();
                    d
                }
            });
        }
        let mut dataset = dataset.unwrap();
        if !deleted.is_empty() {
            dataset.delete(&key_list(deleted)).await.unwrap();
        }
        dataset
    }

    fn keys_of(batches: &[RecordBatch]) -> Vec<String> {
        rows_as_vectors(batches, false, false)
            .into_iter()
            .map(|v| v.key)
            .collect()
    }

    /// Every page names only its fragments yet matches the same window of
    /// the full scan, across deletions and a fully deleted fragment.
    #[tokio::test]
    async fn list_page_matches_a_full_scan() {
        let dir = tempfile::tempdir().unwrap();
        let deleted: Vec<String> = ["k001", "k004", "k005", "k006", "k007", "k010"]
            .map(String::from)
            .to_vec();
        let dataset = fragmented(&dir, &[0..4, 4..8, 8..12, 12..13], &deleted).await;
        let columns = vec![KEY_COLUMN.to_string()];
        let all = keys_of(
            &dataset
                .scan()
                .project(&columns)
                .unwrap()
                .try_into_stream()
                .await
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .unwrap(),
        );
        assert_eq!(all.len(), 7);
        let total = all.len() as u64;
        assert!(page_fragments(&dataset, 0, total).is_some());
        for offset in 0..=total + 1 {
            for rows in [1, 2, 3, 5, total + 2] {
                let got = keys_of(&list_page(&dataset, &columns, offset, rows).await.unwrap());
                let end = (offset + rows).min(total) as usize;
                let want = &all[(offset.min(total) as usize)..end];
                assert_eq!(got, want, "offset {offset} rows {rows}");
            }
        }
    }

    /// Following a segment's tokens yields exactly the rows at its positions,
    /// in order, and the segments together cover every row once.
    #[tokio::test]
    async fn list_segment_strides_the_listing() {
        let dir = tempfile::tempdir().unwrap();
        let deleted: Vec<String> = ["k002", "k009"].map(String::from).to_vec();
        let dataset = fragmented(&dir, &[0..5, 5..12, 12..13, 13..20], &deleted).await;
        let all = keys_of(
            &list_page(&dataset, &[KEY_COLUMN.to_string()], 0, 100)
                .await
                .unwrap(),
        );
        let with_data = projection(true, true);
        let rows = rows_as_vectors(
            &list_page(&dataset, &with_data, 0, 100).await.unwrap(),
            true,
            true,
        );
        assert!(rows.iter().any(|v| v.metadata.is_some()));
        for count in [2u64, 3, 4, 16] {
            for page in [1u64, 2, 3, 7] {
                let mut seen = Vec::new();
                for index in 0..count {
                    let (mut got, mut offset) = (Vec::new(), Some(0));
                    while let Some(from) = offset {
                        let (batches, next) =
                            list_segment(&dataset, &with_data, from, page, count, index)
                                .await
                                .unwrap();
                        for v in rows_as_vectors(&batches, true, true) {
                            let want = rows.iter().find(|r| r.key == v.key).unwrap();
                            assert_eq!(&v, want);
                            got.push(v.key);
                        }
                        assert!(next.is_none() || next > Some(from));
                        offset = next;
                    }
                    let want: Vec<String> = all
                        .iter()
                        .skip(index as usize)
                        .step_by(count as usize)
                        .cloned()
                        .collect();
                    assert_eq!(got, want, "count {count} index {index} page {page}");
                    seen.extend(got);
                }
                seen.sort();
                assert_eq!(seen.len(), all.len());
                let mut sorted = all.clone();
                sorted.sort();
                assert_eq!(seen, sorted);
            }
        }
        // Keys alone come from the stride scan, and a page ending on the
        // last row carries no token.
        let (batches, next) = list_segment(&dataset, &[KEY_COLUMN.to_string()], 0, 9, 2, 1)
            .await
            .unwrap();
        let odd: Vec<String> = all.iter().skip(1).step_by(2).cloned().collect();
        assert_eq!(keys_of(&batches), odd);
        assert_eq!(next, None);
    }

    /// A location that is not the index's own is refused before anything is
    /// opened or removed.
    #[tokio::test]
    async fn drop_index_refuses_a_location_outside_the_table() {
        let server = VectorServer::new();
        for location in [
            "s3://bkt/vector",
            "s3://bkt/vector/prod",
            "s3://other/vector/x",
        ] {
            let request = pb::DropIndexRequest {
                table: Some(pb::Table {
                    id: vec!["bkt".into(), "vector".into(), "x".into()],
                    location: location.into(),
                    ..Default::default()
                }),
            };
            let err = server
                .drop_index(Request::new(request))
                .await
                .expect_err(location);
            assert_eq!(err.code(), tonic::Code::PermissionDenied, "{location}");
        }
    }

    /// A table of an SSE-S3 bucket is refused before Lance opens it when the
    /// storage holds no key.
    #[test]
    fn an_sse_s3_table_needs_the_key() {
        assert!(sse_s3_ready(false, Some(false)).is_ok());
        assert!(sse_s3_ready(true, Some(true)).is_ok());
        assert!(
            sse_s3_ready(true, None).is_ok(),
            "in-process stores need no key"
        );
        let err = sse_s3_ready(true, Some(false)).unwrap_err();
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
        assert!(err.message().contains("s3.sse.kek or s3.sse.key"));
    }

    /// Only a vector index at its colocated path is taken from the request.
    #[test]
    fn resolved_takes_only_a_colocated_vector_index() {
        let table = |id: &[&str], location: &str| pb::Table {
            id: id.iter().map(|s| s.to_string()).collect(),
            location: location.into(),
            ..Default::default()
        };
        let s3 = &["s3"];
        let index = ["bkt", "vector", "x"];
        for location in ["s3://bkt/vector/x", "s3://bkt/vector/x/"] {
            let description = resolved(&table(&index, location), s3).expect(location);
            assert_eq!(description.location, "s3://bkt/vector/x");
            assert_eq!(
                description
                    .storage_options
                    .get("aws_skip_signature")
                    .map(String::as_str),
                Some("true")
            );
        }
        assert!(resolved(&table(&index, ""), s3).is_none());
        assert!(resolved(&table(&index, "s3://other/vector/x"), s3).is_none());
        assert!(resolved(&table(&index, "s3://bkt/vector/y"), s3).is_none());
        assert!(resolved(&table(&index, "file://bkt/vector/x"), s3).is_none());
        assert!(resolved(&table(&index, "memory://bkt/vector/x"), &["s3", "memory"]).is_some());
        assert!(resolved(&table(&["bkt", "ns", "x"], "s3://bkt/ns/x"), s3).is_none());
        assert!(resolved(&table(&["bkt", "vector", ".."], "s3://bkt/vector/.."), s3).is_none());
        assert!(resolved(&table(&["bkt", "vector", "a/b"], "s3://bkt/vector/a/b"), s3).is_none());
        assert!(resolved(&table(&["bkt", "x"], "s3://bkt/x"), s3).is_none());
    }

    /// A request names the table's location or is refused.
    #[tokio::test]
    async fn a_table_without_its_location_is_refused() {
        let server = VectorServer::new();
        for (location, code) in [
            ("", tonic::Code::InvalidArgument),
            ("s3://other/vector/x", tonic::Code::PermissionDenied),
        ] {
            let err = server
                .get_vectors(Request::new(pb::GetVectorsRequest {
                    table: Some(pb::Table {
                        id: vec!["bkt".into(), "vector".into(), "x".into()],
                        location: location.into(),
                        ..Default::default()
                    }),
                    keys: vec!["k001".into()],
                    ..Default::default()
                }))
                .await
                .expect_err(location);
            assert_eq!(err.code(), code, "{location}");
        }
    }
}
