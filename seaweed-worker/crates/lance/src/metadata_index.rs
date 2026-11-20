//! Metadata keys an S3 Vectors index keeps a scalar index for.
//!
//! Each indexed key is a hidden `_mk_<n>` column of `List<Utf8>`: the strings
//! an S3 `$eq` on a string matches, that is the value when it is a string or
//! the string elements of an array, and nothing for any other type or a
//! missing key. The field metadata names the key and its state. The
//! `metadata` JSON column stays the source of truth: every writer derives the
//! columns from it, and queries use one only once its state is `ready`.

use std::sync::Arc;

use anyhow::{Result, bail};
use arrow_array::builder::{ListBuilder, StringBuilder};
use arrow_array::{Array, ArrayRef, LargeBinaryArray, StringArray};
use arrow_schema::{DataType, Field, Schema};

pub const COLUMN_PREFIX: &str = "_mk_";
/// Field metadata naming the metadata key a column holds.
pub const KEY_FIELD_META: &str = "weed:metadata_key";
/// Field metadata holding the column's [`State`].
pub const STATE_FIELD_META: &str = "weed:metadata_key_state";
/// Indexed keys an index may have.
pub const MAX_KEYS: usize = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum State {
    /// The column exists; its scalar index may not.
    Building,
    /// The column and its scalar index exist; queries may use them.
    Ready,
}

impl State {
    pub fn as_str(self) -> &'static str {
        match self {
            State::Building => "building",
            State::Ready => "ready",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "building" => Some(State::Building),
            "ready" => Some(State::Ready),
            _ => None,
        }
    }
}

/// One indexed key, as the table's schema records it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexedKey {
    pub column: String,
    pub key: String,
    /// None for a state this version does not know, which no query uses.
    pub state: Option<State>,
}

/// The type of every indexed-key column.
pub fn column_type() -> DataType {
    DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)))
}

/// The field of the column holding `key`.
pub fn field(column: &str, key: &str, state: State) -> Field {
    Field::new(column, column_type(), true).with_metadata(
        [
            (KEY_FIELD_META.to_string(), key.to_string()),
            (STATE_FIELD_META.to_string(), state.as_str().to_string()),
        ]
        .into_iter()
        .collect(),
    )
}

/// The indexed-key columns of `schema`, in schema order.
pub fn indexed_keys(schema: &Schema) -> Vec<IndexedKey> {
    schema
        .fields()
        .iter()
        .filter_map(|f| indexed_key(f))
        .collect()
}

/// The key `field` holds, when it is an indexed-key column.
pub fn indexed_key(field: &Field) -> Option<IndexedKey> {
    if !field.name().starts_with(COLUMN_PREFIX) {
        return None;
    }
    let key = field.metadata().get(KEY_FIELD_META)?;
    Some(IndexedKey {
        column: field.name().clone(),
        key: key.clone(),
        state: field
            .metadata()
            .get(STATE_FIELD_META)
            .and_then(|s| State::parse(s)),
    })
}

/// The strings an S3 `$eq` on a string matches for this stored value.
pub fn json_strings(value: Option<&serde_json::Value>) -> Vec<&str> {
    match value {
        Some(serde_json::Value::String(s)) => vec![s.as_str()],
        Some(serde_json::Value::Array(items)) => items.iter().filter_map(|v| v.as_str()).collect(),
        _ => Vec::new(),
    }
}

/// [`json_strings`] for `key` of one row's metadata as Lance stores it (jsonb).
pub fn jsonb_strings(metadata: &[u8], key: &str) -> Result<Vec<String>> {
    let raw = jsonb::RawJsonb::new(metadata);
    let Some(value) = raw.get_by_name(key, false)? else {
        return Ok(Vec::new());
    };
    let value = value.as_raw();
    if let Some(s) = value.as_str()? {
        return Ok(vec![s.into_owned()]);
    }
    let Some(items) = value.array_values()? else {
        return Ok(Vec::new());
    };
    let mut out = Vec::new();
    for item in items {
        if let Some(s) = item.as_raw().as_str()? {
            out.push(s.into_owned());
        }
    }
    Ok(out)
}

/// The column for `key` over rows whose metadata is given as JSON objects.
pub fn column_from_json<'a>(
    rows: impl IntoIterator<Item = Option<&'a serde_json::Value>>,
    key: &str,
) -> ArrayRef {
    let mut builder = ListBuilder::new(StringBuilder::new());
    for row in rows {
        for s in json_strings(row.and_then(|m| m.get(key))) {
            builder.values().append_value(s);
        }
        builder.append(true);
    }
    Arc::new(builder.finish())
}

/// The column for `key` over a stored `metadata` column: jsonb (LargeBinary)
/// or JSON text (Utf8).
pub fn column_from_metadata(metadata: &dyn Array, key: &str) -> Result<ArrayRef> {
    let mut builder = ListBuilder::new(StringBuilder::new());
    if let Some(jsonb) = metadata.as_any().downcast_ref::<LargeBinaryArray>() {
        for i in 0..jsonb.len() {
            if !jsonb.is_null(i) {
                for s in jsonb_strings(jsonb.value(i), key)? {
                    builder.values().append_value(s);
                }
            }
            builder.append(true);
        }
    } else if let Some(text) = metadata.as_any().downcast_ref::<StringArray>() {
        for i in 0..text.len() {
            if !text.is_null(i) {
                let row: serde_json::Value = serde_json::from_str(text.value(i))?;
                for s in json_strings(row.get(key)) {
                    builder.values().append_value(s);
                }
            }
            builder.append(true);
        }
    } else {
        bail!("metadata column is {}", metadata.data_type());
    }
    Ok(Arc::new(builder.finish()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::ListArray;

    fn lists(array: &ArrayRef) -> Vec<Vec<String>> {
        let list = array.as_any().downcast_ref::<ListArray>().unwrap();
        (0..list.len())
            .map(|i| {
                assert!(!list.is_null(i));
                let values = list.value(i);
                let values = values.as_any().downcast_ref::<StringArray>().unwrap();
                values.iter().map(|v| v.unwrap().to_string()).collect()
            })
            .collect()
    }

    const ROWS: [Option<&str>; 12] = [
        Some(r#"{"k": "a"}"#),
        Some(r#"{"k": 7}"#),
        Some(r#"{"k": "7"}"#),
        Some(r#"{"k": ["a", "b", 7, true, null, ["c"], {"d": "e"}]}"#),
        Some(r#"{"x": "a"}"#),
        Some(r#"{"k": null}"#),
        Some(r#"{"k": true}"#),
        Some(r#"{"k": {"a": "b"}}"#),
        Some(r#"{"k": "line\nbreak\u0001"}"#),
        Some(r#"{"k": []}"#),
        Some(r#"{"K": "upper"}"#),
        None,
    ];

    fn want() -> Vec<Vec<String>> {
        let s = |v: &[&str]| v.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        vec![
            s(&["a"]),
            s(&[]),
            s(&["7"]),
            s(&["a", "b"]),
            s(&[]),
            s(&[]),
            s(&[]),
            s(&[]),
            s(&["line\nbreak\u{1}"]),
            s(&[]),
            s(&[]),
            s(&[]),
        ]
    }

    #[test]
    fn json_and_jsonb_rows_derive_the_same_strings() {
        let values: Vec<Option<serde_json::Value>> = ROWS
            .iter()
            .map(|r| r.map(|r| serde_json::from_str(r).unwrap()))
            .collect();
        assert_eq!(
            lists(&column_from_json(values.iter().map(|v| v.as_ref()), "k")),
            want()
        );

        let text = StringArray::from(ROWS.to_vec());
        assert_eq!(lists(&column_from_metadata(&text, "k").unwrap()), want());

        let jsonb = lance_arrow::json::JsonArray::try_from_iter(ROWS.iter().copied())
            .unwrap()
            .into_inner();
        assert_eq!(lists(&column_from_metadata(&jsonb, "k").unwrap()), want());
    }

    #[test]
    fn indexed_keys_come_from_the_field_metadata() {
        let mut unknown = field("_mk_2", "c", State::Ready);
        unknown.set_metadata(
            [
                (KEY_FIELD_META.to_string(), "c".to_string()),
                (STATE_FIELD_META.to_string(), "later".to_string()),
            ]
            .into_iter()
            .collect(),
        );
        let schema = Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            field("_mk_0", "a.b", State::Ready),
            Field::new("_mk_1", column_type(), true),
            field("_mk_3", "d", State::Building),
            unknown,
        ]);
        assert_eq!(
            indexed_keys(&schema),
            vec![
                IndexedKey {
                    column: "_mk_0".into(),
                    key: "a.b".into(),
                    state: Some(State::Ready),
                },
                IndexedKey {
                    column: "_mk_3".into(),
                    key: "d".into(),
                    state: Some(State::Building),
                },
                IndexedKey {
                    column: "_mk_2".into(),
                    key: "c".into(),
                    state: None,
                },
            ]
        );
    }
}
