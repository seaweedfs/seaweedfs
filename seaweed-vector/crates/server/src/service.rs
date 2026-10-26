use anyhow::{Result, bail};
use arrow_array::{Array, ArrayRef, FixedSizeListArray, Float32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use futures::TryStreamExt;
use lance::Dataset;
use lance::dataset::ProjectionRequest;
use lance::dataset::WriteParams;
use lance::io::ObjectStoreParams;
use lance::io::StorageOptionsAccessor;
use lance_linalg::distance::DistanceType;
use prost_types::{ListValue, Struct, Value, value::Kind};
use std::collections::BTreeMap;
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

const KEY_COLUMN: &str = "key";
const METADATA_COLUMN: &str = "metadata";
const VECTOR_COLUMN: &str = "vector";

fn schema_for(dimension: usize) -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new(KEY_COLUMN, DataType::Utf8, false),
        Field::new(
            VECTOR_COLUMN,
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, false)),
                dimension as i32,
            ),
            false,
        ),
        Field::new(METADATA_COLUMN, DataType::Utf8, true),
    ]))
}

fn batch_of(vectors: &[pb::Vector], dimension: usize) -> Result<RecordBatch> {
    let schema = schema_for(dimension);
    let keys = StringArray::from(vectors.iter().map(|v| v.key.as_str()).collect::<Vec<_>>());
    let flat = vectors
        .iter()
        .flat_map(|v| v.data.iter().copied())
        .collect::<Vec<_>>();
    if flat.len() != vectors.len() * dimension {
        bail!("a vector's data length does not match the index dimension");
    }
    let values: ArrayRef = Arc::new(Float32Array::from(flat));
    let vectors_array = FixedSizeListArray::try_new(
        Field::new("item", DataType::Float32, false).into(),
        dimension as i32,
        values,
        None,
    )?;
    let metas = vectors
        .iter()
        .map(|v| {
            v.metadata
                .as_ref()
                .map(|m| serde_json::to_string(&struct_to_json(m)))
                .transpose()
        })
        .collect::<Result<Vec<_>, _>>()?;
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(keys),
            Arc::new(vectors_array),
            Arc::new(StringArray::from(metas)),
        ],
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
        let table = req
            .table
            .ok_or_else(|| Status::invalid_argument("missing table"))?;
        let dimension = req.dimension as usize;
        if dimension == 0 {
            return Err(Status::invalid_argument("dimension must be positive"));
        }
        let description = self
            .client
            .declare_table(&table.id, None)
            .await
            .map_err(internal)?;
        let mut options = self.fallback.clone();
        options.extend(description.storage_options.clone());
        let schema = schema_for(dimension);
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
        _request: Request<pb::DropIndexRequest>,
    ) -> Result<Response<pb::DropIndexResponse>, Status> {
        // Dropping is a catalog decision; the gateway deletes the table record.
        Err(Status::unimplemented("drop through the catalog"))
    }

    async fn put_vectors(
        &self,
        request: Request<pb::PutVectorsRequest>,
    ) -> Result<Response<pb::PutVectorsResponse>, Status> {
        let req = request.into_inner();
        let table = req
            .table
            .ok_or_else(|| Status::invalid_argument("missing table"))?;
        let mut dataset = self.open(&table).await.map_err(invalid)?;
        let dimension = VectorTable::resolve(&dataset, None, Some(KEY_COLUMN))
            .map_err(invalid)?
            .dimension;
        let batch = batch_of(&req.vectors, dimension).map_err(invalid)?;
        // Upsert by key: delete the keys being put, then append. A single
        // merge_insert commit can replace this once it is exercised here.
        dataset
            .delete(&key_list(
                &req.vectors
                    .iter()
                    .map(|v| v.key.clone())
                    .collect::<Vec<_>>(),
            ))
            .await
            .map_err(internal)?;
        let schema = batch.schema();
        dataset
            .append(
                arrow_array::RecordBatchIterator::new(vec![Ok(batch)].into_iter(), schema),
                None,
            )
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
        let dataset = self.open(&table).await.map_err(invalid)?;
        let resolved = VectorTable::resolve(&dataset, None, Some(KEY_COLUMN)).map_err(invalid)?;
        let batches = query::get(&dataset, &resolved, &req.keys)
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
        let mut dataset = self.open(&table).await.map_err(invalid)?;
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
        let dataset = self.open(&table).await.map_err(invalid)?;
        let offset: u64 = req.next_token.parse().unwrap_or(0);
        let mut columns = vec![KEY_COLUMN.to_string()];
        if req.return_data {
            columns.push(VECTOR_COLUMN.to_string());
        }
        if req.return_metadata {
            columns.push(METADATA_COLUMN.to_string());
        }
        let mut scanner = dataset.scan();
        scanner
            .project(&columns)
            .map_err(internal)?
            .limit(Some(req.max_results as i64), Some(offset as i64))
            .map_err(internal)?;
        let batches: Vec<RecordBatch> = scanner
            .try_into_stream()
            .await
            .map_err(internal)?
            .try_collect()
            .await
            .map_err(internal)?;
        let rows = batches.iter().map(|b| b.num_rows()).sum::<usize>();
        let next = offset + rows as u64;
        let next_token = if rows == req.max_results as usize && rows > 0 {
            next.to_string()
        } else {
            String::new()
        };
        Ok(Response::new(pb::ListVectorsResponse {
            vectors: rows_as_vectors(&batches, req.return_data, req.return_metadata),
            next_token,
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
        let dataset = self.open(&table).await.map_err(invalid)?;
        let resolved = VectorTable::resolve(&dataset, None, Some(KEY_COLUMN)).map_err(invalid)?;
        let query = Query {
            vector: req.query_vector,
            k: req.top_k.max(1) as usize,
            filter: if req.filter.is_empty() {
                None
            } else {
                Some(req.filter)
            },
            postfilter: req.postfilter,
            distance: DistanceType::L2,
            nprobes: None,
        };
        let hits = query::ann(&dataset, &resolved, &query)
            .await
            .map_err(internal)?;
        let mut vectors = Vec::with_capacity(hits.len());
        for hit in &hits {
            let metadata = if req.return_metadata {
                let batch = dataset
                    .take_rows(
                        &[hit.row_id],
                        ProjectionRequest::from_columns([METADATA_COLUMN], dataset.schema()),
                    )
                    .await
                    .map_err(internal)?;
                batch
                    .column_by_name(METADATA_COLUMN)
                    .and_then(|c| c.as_any().downcast_ref::<StringArray>())
                    .filter(|m| !m.is_null(0))
                    .and_then(|m| parse_struct(m.value(0)).ok())
            } else {
                None
            };
            vectors.push(pb::QueryResult {
                key: hit.key.clone(),
                distance: if req.return_distance {
                    hit.distance
                } else {
                    0.0
                },
                metadata,
            });
        }
        Ok(Response::new(pb::QueryVectorsResponse { vectors }))
    }
}
