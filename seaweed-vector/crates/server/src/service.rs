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
use lance::io::ObjectStoreParams;
use lance::io::StorageOptionsAccessor;
use lance::table::format::Fragment;
use lance_linalg::distance::DistanceType;
use prost_types::{ListValue, Struct, Value, value::Kind};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use tonic::{Request, Response, Status};
use weed_lance_worker::catalog::NamespaceClient;
use weed_lance_worker::dataset::{self, FallbackOptions};
use weed_vector_core::query::{self, Query};
use weed_vector_core::table::VectorTable;

use crate::pb::vector::{self as pb, vector_service_server::VectorService};

fn internal(err: impl Into<anyhow::Error>) -> Status {
    Status::internal(format!("{:#}", err.into()))
}

fn invalid(err: impl Into<anyhow::Error>) -> Status {
    Status::invalid_argument(format!("{:#}", err.into()))
}

fn open_status(err: anyhow::Error) -> Status {
    let missing = err.chain().any(|cause| {
        cause
            .downcast_ref::<reqwest::Error>()
            .is_some_and(|e| e.status() == Some(reqwest::StatusCode::NOT_FOUND))
            || cause.downcast_ref::<lance::Error>().is_some_and(|e| {
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

pub struct VectorServer {
    client: NamespaceClient,
    fallback: FallbackOptions,
}

impl VectorServer {
    pub fn new(client: NamespaceClient, fallback: FallbackOptions) -> Self {
        Self { client, fallback }
    }

    async fn open(&self, table: &pb::Table) -> Result<Dataset> {
        let opened = dataset::open(&self.client, &table.id, &self.fallback).await?;
        let mut dataset = opened.dataset;
        if table.version != 0 {
            dataset = dataset.checkout_version(table.version).await?;
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
        let description = self
            .client
            .declare_table(&table.id, None)
            .await
            .map_err(internal)?;
        let mut options = self.fallback.clone();
        options.extend(description.storage_options.clone());
        let schema = schema_for(dimension, metric, &req.non_filterable_metadata_keys);
        let empty = RecordBatch::new_empty(schema.clone());
        let reader = arrow_array::RecordBatchIterator::new(vec![Ok(empty)].into_iter(), schema);
        let dataset = Dataset::write(
            reader,
            &description.location,
            Some(WriteParams {
                store_params: Some(ObjectStoreParams {
                    storage_options_accessor: Some(Arc::new(
                        StorageOptionsAccessor::with_static_options(options),
                    )),
                    ..Default::default()
                }),
                ..Default::default()
            }),
        )
        .await
        .map_err(internal)?;
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
        dataset::drop(&self.client, &table.id, &self.fallback)
            .await
            .map_err(|err| {
                if err.is::<dataset::DropRefused>() {
                    Status::failed_precondition(format!("{err:#}"))
                } else {
                    internal(err)
                }
            })?;
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
        let dataset = self.open(&table).await.map_err(open_status)?;
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
        let dataset = self.open(&table).await.map_err(open_status)?;
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
        let mut dataset = self.open(&table).await.map_err(open_status)?;
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
        let dataset = self.open(&table).await.map_err(open_status)?;
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
        let dataset = self.open(&table).await.map_err(open_status)?;
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
        // The index's configured metric lives in the vector field's metadata.
        let distance = match dataset
            .schema()
            .field(VECTOR_COLUMN)
            .and_then(|f| f.metadata.get("distance_metric").map(String::as_str))
        {
            Some("COSINE") => DistanceType::Cosine,
            Some("DOT_PRODUCT") => DistanceType::Dot,
            _ => DistanceType::L2,
        };
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
            nprobes: None,
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
            metadata: None,
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
        let with_data = vec![KEY_COLUMN.to_string(), VECTOR_COLUMN.to_string()];
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
                        for v in rows_as_vectors(&batches, true, false) {
                            assert_eq!(format!("k{:03}", v.data[0] as usize), v.key);
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

    /// A catalog that answers one describe with `location`.
    async fn catalog_describing(location: &str) -> String {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let body = serde_json::json!({ "location": location }).to_string();
        tokio::spawn(async move {
            let (mut conn, _) = listener.accept().await.unwrap();
            let mut request = [0u8; 8192];
            let _ = conn.read(&mut request).await;
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
            conn.write_all(response.as_bytes()).await.unwrap();
            let _ = conn.shutdown().await;
            let _ = tokio::io::copy(&mut conn, &mut tokio::io::sink()).await;
        });
        url
    }

    #[tokio::test]
    async fn drop_index_refuses_a_location_outside_the_table() {
        for location in ["s3://bkt/vector", "s3://bkt/vector/prod"] {
            let server = VectorServer::new(
                NamespaceClient::new(catalog_describing(location).await),
                FallbackOptions::new(),
            );
            let request = pb::DropIndexRequest {
                table: Some(pb::Table {
                    id: vec!["bkt".into(), "vector".into(), "x".into()],
                    version: 0,
                }),
            };
            let err = server
                .drop_index(Request::new(request))
                .await
                .expect_err(location);
            assert_eq!(err.code(), tonic::Code::FailedPrecondition, "{location}");
        }
    }
}
