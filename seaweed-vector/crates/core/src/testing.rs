//! Small Lance datasets for the query and coverage tests.

use std::sync::Arc;

use arrow_array::{
    FixedSizeListArray, RecordBatch, RecordBatchIterator, StringArray, types::Float32Type,
};
use arrow_schema::{DataType, Field, Schema};
use lance::Dataset;
use lance::dataset::WriteParams;
use tempfile::TempDir;

pub const DIM: usize = 4;

fn vector_column(vectors: &[Vec<f32>], dim: usize) -> FixedSizeListArray {
    FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
        vectors
            .iter()
            .map(|v| Some(v.iter().copied().map(Some).collect::<Vec<_>>())),
        dim as i32,
    )
}

pub fn schema(dim: usize) -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("key", DataType::Utf8, false),
        Field::new(
            "vector",
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, true)),
                dim as i32,
            ),
            false,
        ),
        Field::new("tag", DataType::Utf8, true),
    ]))
}

pub fn batch(offset: usize, vectors: &[Vec<f32>], tags: &[&str], dim: usize) -> RecordBatch {
    let keys: Vec<String> = (0..vectors.len())
        .map(|i| format!("k{}", offset + i))
        .collect();
    RecordBatch::try_new(
        schema(dim),
        vec![
            Arc::new(StringArray::from(keys)),
            Arc::new(vector_column(vectors, dim)),
            Arc::new(StringArray::from(
                tags.iter().map(|t| Some(t.to_string())).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

pub async fn write_dataset(dir: &TempDir, vectors: &[Vec<f32>], tags: &[&str]) -> Dataset {
    write_dataset_at(dir.path().to_str().unwrap(), vectors, tags).await
}

pub async fn write_dataset_at(uri: &str, vectors: &[Vec<f32>], tags: &[&str]) -> Dataset {
    let dim = vectors[0].len();
    let reader = RecordBatchIterator::new(
        vec![Ok(batch(0, vectors, tags, dim))].into_iter(),
        schema(dim),
    );
    Dataset::write(reader, uri, None).await.unwrap()
}

/// Deterministic non-trivial vectors for index coverage tests.
pub fn vectors(rows: usize, dim: usize) -> Vec<Vec<f32>> {
    (0..rows)
        .map(|i| {
            (0..dim)
                .map(|j| ((i * 31 + j * 17) % 97) as f32 / 97.0)
                .collect()
        })
        .collect()
}

/// Commit a new version whose fragment is not yet covered by the index the
/// dataset had before.
pub async fn append_dataset(dataset: &mut Dataset, vectors: &[Vec<f32>], tags: &[&str]) {
    let offset = dataset.count_rows(None).await.unwrap();
    let dim = vectors[0].len();
    let reader = RecordBatchIterator::new(
        vec![Ok(batch(offset, vectors, tags, dim))].into_iter(),
        schema(dim),
    );
    dataset
        .append(reader, Some(WriteParams::default()))
        .await
        .unwrap();
}
