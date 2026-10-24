//! Sample rows of a Lance table, for a page that cannot read the format.
//!
//! Admin renders an Iceberg table by reading its Parquet files directly. There
//! is no Go Lance reader, so for Lance the worker does the reading and hands
//! back text.

use anyhow::{Context, Result};
use arrow_cast::display::{ArrayFormatter, FormatOptions};
use async_trait::async_trait;
use futures::TryStreamExt;
use seaweed_worker_core::{Preview, PreviewProvider};

use crate::catalog::NamespaceClient;
use crate::dataset::{self, FallbackOptions};
use crate::jobs::FORMAT;

pub struct LancePreview {
    namespace_url: String,
    fallback: FallbackOptions,
}

impl LancePreview {
    pub fn new(namespace_url: String, fallback: FallbackOptions) -> Self {
        Self {
            namespace_url,
            fallback,
        }
    }
}

#[async_trait]
impl PreviewProvider for LancePreview {
    fn format(&self) -> &str {
        FORMAT
    }

    async fn preview(&self, object_id: &[String], row_limit: usize) -> Result<Preview> {
        let client = NamespaceClient::new(self.namespace_url.clone());
        let table = dataset::open(&client, object_id, &self.fallback).await?;

        let total_rows = table.dataset.count_rows(None).await.unwrap_or(0) as i64;
        let mut scanner = table.dataset.scan();
        scanner.limit(Some(row_limit as i64), None)?;

        let batches: Vec<_> = scanner
            .try_into_stream()
            .await?
            .try_collect()
            .await
            .with_context(|| format!("read rows from {}", table.location))?;

        let columns = table
            .dataset
            .schema()
            .fields
            .iter()
            .map(|field| field.name.clone())
            .collect();

        let mut rows = Vec::new();
        for batch in &batches {
            // ArrayFormatter renders each Arrow type the way its own tooling
            // does, so a vector column reads as a vector rather than as bytes.
            let formatters = batch
                .columns()
                .iter()
                .map(|array| ArrayFormatter::try_new(array.as_ref(), &FormatOptions::default()))
                .collect::<Result<Vec<_>, _>>()?;
            for index in 0..batch.num_rows() {
                rows.push(
                    formatters
                        .iter()
                        .map(|formatter| formatter.value(index).to_string())
                        .collect(),
                );
            }
        }
        rows.truncate(row_limit);

        Ok(Preview {
            columns,
            rows,
            total_rows,
        })
    }
}
