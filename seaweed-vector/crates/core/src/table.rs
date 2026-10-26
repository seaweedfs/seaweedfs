//! The columns a vector query needs, resolved from a table's Arrow schema.

use anyhow::{Context, Result, bail};
use arrow_schema::DataType;
use lance::Dataset;

/// The default name for the vector column, matching what the S3 Vectors API
/// writes.
const DEFAULT_VECTOR_COLUMN: &str = "vector";

/// The default name for the key column.
const DEFAULT_KEY_COLUMN: &str = "key";

/// Layout of a Lance table used as a vector index.
#[derive(Debug, Clone)]
pub struct VectorTable {
    /// The row key column; `None` falls back to `_rowid` in results.
    pub key_column: Option<String>,
    pub vector_column: String,
    pub dimension: usize,
    pub metadata_columns: Vec<String>,
}

impl VectorTable {
    /// Resolve the vector layout of `dataset`. `vector_column` and
    /// `key_column` override detection: the vector column defaults to the one
    /// named `vector`, or the sole fixed-size float list when unambiguous.
    pub fn resolve(
        dataset: &Dataset,
        vector_column: Option<&str>,
        key_column: Option<&str>,
    ) -> Result<Self> {
        let schema: arrow_schema::Schema = dataset.schema().into();
        resolve_schema(&schema, vector_column, key_column)
    }
}

pub(crate) fn resolve_schema(
    schema: &arrow_schema::Schema,
    vector_column: Option<&str>,
    key_column: Option<&str>,
) -> Result<VectorTable> {
    let vector_column = match vector_column {
        Some(name) => name.to_string(),
        None => detect_vector_column(schema)?,
    };
    let field = schema
        .field_with_name(&vector_column)
        .with_context(|| format!("no such column {vector_column}"))?;
    let dimension = vector_dimension(field.data_type())
        .with_context(|| format!("column {vector_column} is not a fixed-size float vector"))?;

    let key_column = match key_column {
        Some(name) => {
            let field = schema
                .field_with_name(name)
                .with_context(|| format!("no such column {name}"))?;
            if field.data_type() != &DataType::Utf8 {
                bail!("key column {name} is not Utf8");
            }
            Some(name.to_string())
        }
        None => detect_key_column(schema),
    };

    let metadata_columns = schema
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .filter(|name| name != &vector_column && Some(name) != key_column.as_ref())
        .collect();

    Ok(VectorTable {
        key_column,
        vector_column,
        dimension,
        metadata_columns,
    })
}

fn detect_vector_column(schema: &arrow_schema::Schema) -> Result<String> {
    let mut named = None;
    let mut candidates = Vec::new();
    for field in schema.fields() {
        if vector_dimension(field.data_type()).is_none() {
            continue;
        }
        if field.name() == DEFAULT_VECTOR_COLUMN {
            named = Some(field.name().clone());
        }
        candidates.push(field.name().clone());
    }
    if let Some(name) = named {
        return Ok(name);
    }
    match candidates.as_slice() {
        [only] => Ok(only.clone()),
        [] => bail!("no fixed-size float vector column found"),
        many => bail!("several vector columns, pick one of: {}", many.join(", ")),
    }
}

fn detect_key_column(schema: &arrow_schema::Schema) -> Option<String> {
    schema
        .field_with_name(DEFAULT_KEY_COLUMN)
        .ok()
        .filter(|f| f.data_type() == &DataType::Utf8)
        .map(|f| f.name().clone())
}

fn vector_dimension(data_type: &DataType) -> Option<usize> {
    let DataType::FixedSizeList(child, dimension) = data_type else {
        return None;
    };
    match child.data_type() {
        DataType::Float16 | DataType::Float32 | DataType::Float64 => Some(*dimension as usize),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{Field, Schema};
    use std::sync::Arc;

    fn vector_field(name: &str, dim: i32) -> Field {
        Field::new(
            name,
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), dim),
            false,
        )
    }

    #[test]
    fn resolve_prefers_the_column_named_vector() {
        let schema = Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            vector_field("embedding", 4),
            vector_field("vector", 8),
            Field::new("tag", DataType::Utf8, true),
        ]);
        let table = resolve_schema(&schema, None, None).unwrap();
        assert_eq!(table.vector_column, "vector");
        assert_eq!(table.dimension, 8);
        assert_eq!(table.key_column.as_deref(), Some("key"));
        assert_eq!(table.metadata_columns, vec!["embedding", "tag"]);
    }

    #[test]
    fn resolve_uses_the_sole_vector_column() {
        let schema = Schema::new(vec![
            vector_field("embedding", 4),
            Field::new("tag", DataType::Utf8, true),
        ]);
        let table = resolve_schema(&schema, None, None).unwrap();
        assert_eq!(table.vector_column, "embedding");
        assert_eq!(table.key_column, None);
    }

    #[test]
    fn resolve_rejects_ambiguous_vector_columns() {
        let schema = Schema::new(vec![vector_field("a", 4), vector_field("b", 4)]);
        assert!(resolve_schema(&schema, None, None).is_err());
    }

    #[test]
    fn resolve_rejects_a_non_vector_column() {
        let schema = Schema::new(vec![
            Field::new("vector", DataType::Utf8, true),
            vector_field("embedding", 4),
        ]);
        assert!(resolve_schema(&schema, Some("vector"), None).is_err());
        let table = resolve_schema(&schema, None, Some("vector")).unwrap();
        assert_eq!(table.vector_column, "embedding");
        assert_eq!(table.key_column.as_deref(), Some("vector"));
    }
}
