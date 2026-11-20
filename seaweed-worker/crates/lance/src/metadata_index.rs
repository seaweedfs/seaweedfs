//! Metadata keys an S3 Vectors index keeps a scalar index for.
//!
//! Each indexed key is a hidden `_mk_<n>` column of `List<Utf8>`: the strings
//! an S3 `$eq` on a string matches, that is the value when it is a string or
//! the string elements of an array, and nothing for any other type or a
//! missing key. The field metadata names the key and its state. The
//! `metadata` JSON column stays the source of truth: every writer derives the
//! columns from it, and queries use one only once its state is `ready`.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Result, bail};
use arrow_array::builder::{ListBuilder, StringBuilder};
use arrow_array::{Array, ArrayRef, LargeBinaryArray, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use futures::TryStreamExt;
use lance::Dataset;
use lance::dataset::transaction::{Operation, TransactionBuilder};
use lance::dataset::write::CommitBuilder;
use lance::dataset::{
    BatchUDF, MergeInsertBuilder, NewColumnTransform, WhenMatched, WhenNotMatched, WriteDestination,
};
use lance::index::DatasetIndexExt;
use lance_index::IndexType;
use lance_index::optimize::OptimizeOptions;
use lance_index::scalar::{BuiltinIndexType, ScalarIndexParams};
use roaring::RoaringBitmap;

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
    /// Being removed: queries no longer use the column.
    Off,
}

impl State {
    pub fn as_str(self) -> &'static str {
        match self {
            State::Building => "building",
            State::Ready => "ready",
            State::Off => "off",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "building" => Some(State::Building),
            "ready" => Some(State::Ready),
            "off" => Some(State::Off),
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

/// The name of the LABEL_LIST index on `column`. Its physical segments each
/// cover a set of fragments; the last one listed is the head, which takes
/// new fragments until it holds [`DEFAULT_SEGMENT_ROWS`] rows or the cap the
/// caller gives.
pub fn index_name(column: &str) -> String {
    format!("{column}_idx")
}

/// Rows a segment takes before it is sealed and a new head starts.
pub const DEFAULT_SEGMENT_ROWS: u64 = 1_000_000;
/// Rows written since the last fold that are left to a direct read of the
/// column before the next fold.
pub const DEFAULT_MIN_UNINDEXED_ROWS: u64 = 50_000;
/// Rows whose column one commit of [`fill_nulls`] fills at most.
pub const FILL_ROWS_PER_COMMIT: usize = 100_000;

const KEY_COLUMN: &str = "key";
const METADATA_COLUMN: &str = "metadata";

/// The indexed keys of `dataset`.
pub fn keys_of(dataset: &Dataset) -> Vec<IndexedKey> {
    indexed_keys(&Schema::from(dataset.schema()))
}

/// Schema metadata naming a key an operator asked to index, before the
/// worker adds its column: `<prefix><key>`.
pub const REQUEST_SCHEMA_META_PREFIX: &str = "weed:metadata_key_request:";

/// Keys asked for whose column does not exist yet.
pub fn requests(dataset: &Dataset) -> Vec<String> {
    let columns = keys_of(dataset);
    let mut keys: Vec<String> = dataset
        .schema()
        .metadata
        .keys()
        .filter_map(|k| k.strip_prefix(REQUEST_SCHEMA_META_PREFIX))
        .filter(|k| !columns.iter().any(|c| c.key == *k))
        .map(str::to_string)
        .collect();
    keys.sort();
    keys
}

/// Records that `key` is to be indexed; the worker adds its column. Refused
/// for a key that is indexed or asked for already, and past [`MAX_KEYS`].
pub async fn request(dataset: &mut Dataset, key: &str) -> Result<()> {
    if key.is_empty() {
        bail!("the metadata key is empty");
    }
    let keys = keys_of(dataset);
    if let Some(existing) = keys.iter().find(|k| k.key == key) {
        bail!(
            "{key} is already indexed ({})",
            existing.state.map_or("unknown", State::as_str)
        );
    }
    let requested = requests(dataset);
    if requested.iter().any(|k| k == key) {
        bail!("{key} is already being indexed (requested)");
    }
    if keys.len() + requested.len() >= MAX_KEYS {
        bail!("an index has at most {MAX_KEYS} indexed metadata keys");
    }
    let entry = format!("{REQUEST_SCHEMA_META_PREFIX}{key}");
    dataset
        .update_schema_metadata([(entry.as_str(), "add")])
        .await?;
    Ok(())
}

/// Stops indexing `key`: a request is withdrawn, a column is set off at once,
/// so queries stop using it, and the worker removes it.
pub async fn request_removal(dataset: &mut Dataset, key: &str) -> Result<()> {
    if let Some(column) = keys_of(dataset).into_iter().find(|k| k.key == key) {
        if column.state != Some(State::Off) {
            set_state(dataset, &column.column, State::Off).await?;
        }
        return Ok(());
    }
    if requests(dataset).iter().any(|k| k == key) {
        let entry = format!("{REQUEST_SCHEMA_META_PREFIX}{key}");
        dataset
            .update_schema_metadata([(entry.as_str(), None::<&str>)])
            .await?;
        return Ok(());
    }
    bail!("{key} is not indexed")
}

/// Adds `key`'s column, `building`, computed from every row's metadata.
/// Writers fill it from the commit on; a write that raced the add leaves its
/// rows NULL, which [`finish`] fills.
pub async fn add_column(dataset: &mut Dataset, key: &str) -> Result<IndexedKey> {
    if let Some(existing) = keys_of(dataset).into_iter().find(|k| k.key == key) {
        return Ok(existing);
    }
    let column = (0..)
        .map(|n| format!("{COLUMN_PREFIX}{n}"))
        .find(|c| dataset.schema().field(c).is_none())
        .unwrap_or_default();
    let output = Arc::new(Schema::new(vec![field(&column, key, State::Building)]));
    let schema = output.clone();
    let owned = key.to_string();
    dataset
        .add_columns(
            NewColumnTransform::BatchUDF(BatchUDF {
                mapper: Box::new(move |batch: &RecordBatch| {
                    let metadata = batch.column_by_name(METADATA_COLUMN).ok_or_else(|| {
                        lance::Error::invalid_input("the table has no metadata column")
                    })?;
                    let column = column_from_metadata(metadata.as_ref(), &owned)
                        .map_err(|e| lance::Error::invalid_input(e.to_string()))?;
                    Ok(RecordBatch::try_new(schema.clone(), vec![column])?)
                }),
                output_schema: output,
                result_checkpoint: None,
            }),
            Some(vec![METADATA_COLUMN.to_string()]),
            None,
        )
        .await?;
    Ok(IndexedKey {
        column,
        key: key.to_string(),
        state: Some(State::Building),
    })
}

/// Records `state` on `column`'s field.
pub async fn set_state(dataset: &mut Dataset, column: &str, state: State) -> Result<()> {
    dataset
        .update_field_metadata()
        .update(column, [(STATE_FIELD_META, state.as_str())])?
        .await?;
    Ok(())
}

/// Fills the indexed-key columns of rows where one is NULL, from the rows'
/// stored metadata: rows written before the column was added, and rows a
/// write that raced the add left empty. Only `keys` when given. A row whose
/// column was set meanwhile is left as it is. Returns the rows filled.
pub async fn fill_nulls(
    dataset: &mut Dataset,
    columns: &[IndexedKey],
    keys: Option<&[String]>,
    rows_per_commit: usize,
) -> Result<u64> {
    if columns.is_empty() || keys.is_some_and(|k| k.is_empty()) {
        return Ok(0);
    }
    let schema = Schema::from(dataset.schema());
    let null = |prefix: &str| {
        columns
            .iter()
            .map(|k| format!("{prefix}`{}` IS NULL", k.column.replace('`', "``")))
            .collect::<Vec<_>>()
            .join(" OR ")
    };
    let mut filter = format!("({})", null(""));
    if let Some(keys) = keys {
        let list = keys
            .iter()
            .map(|k| format!("'{}'", k.replace('\'', "''")))
            .collect::<Vec<_>>()
            .join(", ");
        filter = format!("`{KEY_COLUMN}` in ({list}) AND {filter}");
    }
    let mut fields = vec![schema.field_with_name(KEY_COLUMN)?.clone()];
    for k in columns {
        fields.push(schema.field_with_name(&k.column)?.clone());
    }
    let patch_schema = Arc::new(Schema::new(fields));
    let mut scanner = dataset.scan();
    scanner
        .filter(&filter)?
        .project(&[KEY_COLUMN, METADATA_COLUMN])?;
    let mut stream = scanner.try_into_stream().await?;
    let mut filled = 0u64;
    let mut pending: Vec<RecordBatch> = Vec::new();
    let mut pending_rows = 0;
    loop {
        let next = stream.try_next().await?;
        if let Some(batch) = &next {
            if batch.num_rows() > 0 {
                let (Some(key), Some(metadata)) = (
                    batch.column_by_name(KEY_COLUMN),
                    batch.column_by_name(METADATA_COLUMN),
                ) else {
                    bail!("the scan did not return keys and metadata");
                };
                let mut out = vec![key.clone()];
                for k in columns {
                    out.push(column_from_metadata(metadata.as_ref(), &k.key)?);
                }
                pending_rows += batch.num_rows();
                pending.push(RecordBatch::try_new(patch_schema.clone(), out)?);
            }
        }
        if pending_rows >= rows_per_commit || (next.is_none() && pending_rows > 0) {
            patch(dataset, std::mem::take(&mut pending), &null("target.")).await?;
            filled += pending_rows as u64;
            pending_rows = 0;
        }
        if next.is_none() {
            break;
        }
    }
    Ok(filled)
}

/// Patches the given columns of rows matching by key where `condition` holds.
async fn patch(dataset: &mut Dataset, batches: Vec<RecordBatch>, condition: &str) -> Result<()> {
    let mut builder =
        MergeInsertBuilder::try_new(Arc::new(dataset.clone()), vec![KEY_COLUMN.to_string()])?;
    builder.when_matched(WhenMatched::update_if(dataset, condition)?);
    builder.when_not_matched(WhenNotMatched::DoNothing);
    let (patched, _) = builder.try_build()?.execute_batches(batches).await?;
    *dataset = Arc::unwrap_or_clone(patched);
    Ok(())
}

/// One physical segment of a key's index.
#[derive(Debug, Clone)]
pub struct Segment {
    pub uuid: uuid::Uuid,
    /// Live fragments it covers.
    pub fragments: RoaringBitmap,
    pub rows: u64,
    pub bytes: u64,
}

/// The segments of `column`'s index, oldest first; the last is the head.
pub async fn segments(dataset: &Dataset, column: &str) -> Result<Vec<Segment>> {
    let name = index_name(column);
    let live: HashMap<u32, u64> = dataset
        .fragments()
        .iter()
        .map(|f| (f.id as u32, f.num_rows().unwrap_or(0) as u64))
        .collect();
    Ok(dataset
        .load_indices()
        .await?
        .iter()
        .filter(|i| i.name == name)
        .map(|i| {
            let fragments: RoaringBitmap = i
                .fragment_bitmap
                .iter()
                .flatten()
                .filter(|f| live.contains_key(f))
                .collect();
            Segment {
                uuid: i.uuid,
                rows: fragments.iter().map(|f| live[&f]).sum(),
                fragments,
                bytes: i.total_size_bytes().unwrap_or(0),
            }
        })
        .collect())
}

/// Live fragments no segment of `column`'s index covers, with their rows.
pub fn unindexed(dataset: &Dataset, segments: &[Segment]) -> Vec<(u32, u64)> {
    dataset
        .fragments()
        .iter()
        .map(|f| (f.id as u32, f.num_rows().unwrap_or(0) as u64))
        .filter(|(id, _)| !segments.iter().any(|s| s.fragments.contains(*id)))
        .collect()
}

/// What [`index_fragments`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Folded {
    /// Fewer unindexed rows than the threshold, or none.
    Deferred,
    /// The first segments, built over the whole table.
    Built(usize),
    /// Unindexed fragments added to the head.
    Head,
    /// The head is full: unindexed fragments started a new segment.
    NewSegment,
}

/// Brings `column`'s index over the fragments it does not cover, once they
/// hold at least `min_unindexed_rows`: the first build splits the table into
/// segments of up to `segment_rows` in fragment order; later passes fold
/// into the head, or start a new head once it holds `segment_rows`.
pub async fn index_fragments(
    dataset: &mut Dataset,
    column: &str,
    segment_rows: u64,
    min_unindexed_rows: u64,
) -> Result<Folded> {
    let existing = segments(dataset, column).await?;
    let mut pending = unindexed(dataset, &existing);
    let rows: u64 = pending.iter().map(|(_, r)| r).sum();
    if pending.is_empty() || (!existing.is_empty() && rows < min_unindexed_rows) {
        return Ok(Folded::Deferred);
    }
    let name = index_name(column);
    let params = ScalarIndexParams::for_builtin(BuiltinIndexType::LabelList);
    if existing.is_empty() {
        pending.sort();
        let mut chunks: Vec<Vec<u32>> = Vec::new();
        let mut chunk_rows = 0;
        for (id, rows) in pending {
            if chunks.is_empty() || (chunk_rows > 0 && chunk_rows + rows > segment_rows) {
                chunks.push(Vec::new());
                chunk_rows = 0;
            }
            if let Some(chunk) = chunks.last_mut() {
                chunk.push(id);
            }
            chunk_rows += rows;
        }
        let read_version = dataset.version().version;
        let mut built = Vec::with_capacity(chunks.len());
        for chunk in chunks {
            built.push(
                dataset
                    .create_index_builder(&[column], IndexType::LabelList, &params)
                    .name(name.clone())
                    .fragments(chunk)
                    .execute_uncommitted()
                    .await?,
            );
        }
        let count = built.len();
        let transaction = TransactionBuilder::new(
            read_version,
            Operation::CreateIndex {
                new_indices: built,
                removed_indices: Vec::new(),
            },
        )
        .build();
        let committed = CommitBuilder::new(WriteDestination::Dataset(Arc::new(dataset.clone())))
            .execute(transaction)
            .await?;
        *dataset = committed;
        return Ok(Folded::Built(count));
    }
    let full = existing
        .last()
        .is_some_and(|head| head.rows >= segment_rows);
    let mut options = OptimizeOptions::default();
    options.num_indices_to_merge = Some(if full { 0 } else { 1 });
    options.index_names = Some(vec![name]);
    dataset.optimize_indices(&options).await?;
    dataset.checkout_latest().await?;
    Ok(if full {
        Folded::NewSegment
    } else {
        Folded::Head
    })
}

/// Completes adding `column`: fills its rows, builds its first segments and
/// marks it ready. Rows that are still NULL just before it is marked ready,
/// from a write that raced the add and did not fill them itself, are filled
/// then.
pub async fn finish(dataset: &mut Dataset, key: &IndexedKey, segment_rows: u64) -> Result<u64> {
    let columns = std::slice::from_ref(key);
    let mut filled = fill_nulls(dataset, columns, None, FILL_ROWS_PER_COMMIT).await?;
    index_fragments(dataset, &key.column, segment_rows, 0).await?;
    filled += fill_nulls(dataset, columns, None, FILL_ROWS_PER_COMMIT).await?;
    set_state(dataset, &key.column, State::Ready).await?;
    let entry = format!("{REQUEST_SCHEMA_META_PREFIX}{}", key.key);
    if dataset.schema().metadata.contains_key(&entry) {
        dataset
            .update_schema_metadata([(entry.as_str(), None::<&str>)])
            .await?;
    }
    Ok(filled)
}

/// One maintenance pass over a ready `key`: fill rows a write left NULL,
/// then fold unindexed fragments as [`index_fragments`] does.
pub async fn maintain(
    dataset: &mut Dataset,
    key: &IndexedKey,
    segment_rows: u64,
    min_unindexed_rows: u64,
) -> Result<(u64, Folded)> {
    let filled = fill_nulls(
        dataset,
        std::slice::from_ref(key),
        None,
        FILL_ROWS_PER_COMMIT,
    )
    .await?;
    let folded = index_fragments(dataset, &key.column, segment_rows, min_unindexed_rows).await?;
    Ok((filled, folded))
}

/// Removes `column`: off first, so queries stop using it at once, then its
/// index and the column itself.
pub async fn remove_column(dataset: &mut Dataset, column: &str) -> Result<()> {
    let Some(key) = keys_of(dataset).into_iter().find(|k| k.column == column) else {
        return Ok(());
    };
    if key.state != Some(State::Off) {
        set_state(dataset, column, State::Off).await?;
    }
    if !segments(dataset, column).await?.is_empty() {
        dataset.drop_index(&index_name(column)).await?;
    }
    dataset.drop_columns(&[column]).await?;
    Ok(())
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
    mod lifecycle {
        use super::super::*;
        use arrow_array::{FixedSizeListArray, RecordBatchIterator, types::Float32Type};
        use lance::dataset::optimize::{CompactionOptions, compact_files};

        fn author(i: usize) -> serde_json::Value {
            match i % 10 {
                0 => serde_json::json!({"author": "rare", "n": i}),
                1 => serde_json::json!({"author": ["x", "rare"]}),
                2 => serde_json::json!({"n": i}),
                _ => serde_json::json!({"author": format!("a{}", i % 7)}),
            }
        }

        /// Rows `range` in `dataset`'s current schema, the way PutVectors
        /// writes them.
        fn rows(schema: &Schema, range: std::ops::Range<usize>) -> RecordBatch {
            let metas: Vec<serde_json::Value> = range.clone().map(author).collect();
            let columns: Vec<ArrayRef> = schema
                .fields()
                .iter()
                .map(|f| -> ArrayRef {
                    match f.name().as_str() {
                        "key" => Arc::new(StringArray::from(
                            range.clone().map(|i| format!("k{i}")).collect::<Vec<_>>(),
                        )),
                        "vector" => {
                            Arc::new(
                                FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
                                    range.clone().map(|i| Some(vec![Some(i as f32), Some(1.0)])),
                                    2,
                                ),
                            )
                        }
                        "metadata" => Arc::new(StringArray::from(
                            metas
                                .iter()
                                .map(|m| Some(m.to_string()))
                                .collect::<Vec<_>>(),
                        )),
                        _ => column_from_json(metas.iter().map(Some), &indexed_key(f).unwrap().key),
                    }
                })
                .collect();
            RecordBatch::try_new(Arc::new(schema.clone()), columns).unwrap()
        }

        fn base_schema() -> Schema {
            Schema::new(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new(
                    "vector",
                    DataType::FixedSizeList(
                        Arc::new(Field::new("item", DataType::Float32, true)),
                        2,
                    ),
                    false,
                ),
                Field::new("metadata", DataType::Utf8, true).with_metadata(
                    [("ARROW:extension:name".to_string(), "arrow.json".to_string())].into(),
                ),
            ])
        }

        /// Upserts rows `range` in the schema of the version `dataset` is at.
        async fn upsert(dataset: Dataset, range: std::ops::Range<usize>) -> Dataset {
            let mut schema = base_schema();
            let fields: Vec<Field> = indexed_keys(&Schema::from(dataset.schema()))
                .iter()
                .map(|k| field(&k.column, &k.key, State::Building))
                .collect();
            let mut all: Vec<Field> = schema.fields().iter().map(|f| f.as_ref().clone()).collect();
            all.extend(fields);
            schema = Schema::new(all);
            let mut builder =
                MergeInsertBuilder::try_new(Arc::new(dataset), vec!["key".to_string()]).unwrap();
            builder
                .when_matched(WhenMatched::UpdateAll)
                .when_not_matched(WhenNotMatched::InsertAll);
            let (dataset, _) = builder
                .try_build()
                .unwrap()
                .execute_batches(vec![rows(&schema, range)])
                .await
                .unwrap();
            Arc::unwrap_or_clone(dataset)
        }

        async fn table(dir: &tempfile::TempDir, fragments: usize) -> Dataset {
            let uri = dir.path().to_str().unwrap();
            let schema = Arc::new(base_schema());
            let mut dataset = Dataset::write(
                RecordBatchIterator::new(vec![Ok(rows(&schema, 0..1000))], schema.clone()),
                uri,
                None,
            )
            .await
            .unwrap();
            for f in 1..fragments {
                dataset
                    .append(
                        RecordBatchIterator::new(
                            vec![Ok(rows(&schema, f * 1000..(f + 1) * 1000))],
                            schema.clone(),
                        ),
                        None,
                    )
                    .await
                    .unwrap();
            }
            dataset
        }

        async fn nulls(dataset: &Dataset, column: &str) -> usize {
            dataset
                .count_rows(Some(format!("{column} IS NULL")))
                .await
                .unwrap()
        }

        /// The indexed answer for "rare" equals the JSON answer.
        async fn agrees(dataset: &Dataset, column: &str) -> usize {
            let indexed = dataset
                .count_rows(Some(format!("array_has({column}, 'rare')")))
                .await
                .unwrap();
            let json = dataset
                .count_rows(Some(
                    "json_extract(metadata, '$[\"author\"]') = '\"rare\"' OR json_array_contains(metadata, '$[\"author\"]', '\"rare\"')".to_string(),
                ))
                .await
                .unwrap();
            assert_eq!(indexed, json);
            indexed
        }

        /// Add, finish, maintain and remove, with a write that raced the add
        /// and never filled its rows (a writer that died before filling them).
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn a_key_is_added_maintained_and_removed() {
            let dir = tempfile::TempDir::new().unwrap();
            let mut dataset = table(&dir, 10).await;
            request(&mut dataset, "author").await.unwrap();
            assert_eq!(requests(&dataset), ["author"]);
            assert!(request(&mut dataset, "author").await.is_err());
            let stale = dataset.clone();
            let key = add_column(&mut dataset, "author").await.unwrap();
            assert_eq!(key.column, "_mk_0");
            assert!(requests(&dataset).is_empty());
            assert_eq!(keys_of(&dataset)[0].state, Some(State::Building));
            assert!(request(&mut dataset, "author").await.is_err());
            assert_eq!(nulls(&dataset, "_mk_0").await, 0);
            // Opened before the add, committed after it, never filled: a
            // writer that died between its commit and its own fill.
            let raced = upsert(stale, 9_500..10_500).await;
            let raced_nulls = nulls(&raced, "_mk_0").await;
            assert!(raced_nulls >= 500, "{raced_nulls}");
            dataset.checkout_latest().await.unwrap();

            let filled = finish(&mut dataset, &key, 4_000).await.unwrap();
            assert_eq!(filled as usize, raced_nulls);
            assert!(
                !dataset
                    .schema()
                    .metadata
                    .keys()
                    .any(|k| k.starts_with(REQUEST_SCHEMA_META_PREFIX))
            );
            assert_eq!(nulls(&dataset, "_mk_0").await, 0);
            assert_eq!(keys_of(&dataset)[0].state, Some(State::Ready));
            let built = segments(&dataset, "_mk_0").await.unwrap();
            assert!(built.len() >= 3, "{built:?}");
            assert!(
                built
                    .iter()
                    .all(|s| s.rows <= 4_000 || s.fragments.len() == 1)
            );
            assert!(unindexed(&dataset, &built).is_empty());
            assert_eq!(agrees(&dataset, "_mk_0").await, 2_100);

            // New rows wait for the threshold, then fold into the head until
            // it is full, then start a new head.
            let mut dataset = upsert(dataset, 10_500..11_500).await;
            let ready = keys_of(&dataset).remove(0);
            assert_eq!(
                maintain(&mut dataset, &ready, 4_000, 5_000).await.unwrap(),
                (0, Folded::Deferred)
            );
            assert_eq!(agrees(&dataset, "_mk_0").await, 2_300);
            let head = segments(&dataset, "_mk_0")
                .await
                .unwrap()
                .last()
                .unwrap()
                .clone();
            let (_, folded) = maintain(&mut dataset, &ready, 4_000, 0).await.unwrap();
            let after = segments(&dataset, "_mk_0").await.unwrap();
            if head.rows >= 4_000 {
                assert_eq!(folded, Folded::NewSegment);
                assert_eq!(after.len(), built.len() + 1);
            } else {
                assert_eq!(folded, Folded::Head);
                assert_eq!(after.len(), built.len());
            }
            assert!(unindexed(&dataset, &after).is_empty());
            let mut dataset = upsert(dataset, 11_500..16_500).await;
            let (_, folded) = maintain(&mut dataset, &ready, 4_000, 0).await.unwrap();
            assert_ne!(folded, Folded::Deferred);
            let after = segments(&dataset, "_mk_0").await.unwrap();
            assert_eq!(agrees(&dataset, "_mk_0").await, 3_300);

            // Compaction keeps each segment's fragments apart and remaps them.
            dataset
                .delete("key = 'k0' OR key = 'k4000' OR key = 'k8010'")
                .await
                .unwrap();
            compact_files(
                &mut dataset,
                CompactionOptions {
                    target_rows_per_fragment: 3_000,
                    ..Default::default()
                },
                None,
            )
            .await
            .unwrap();
            dataset.checkout_latest().await.unwrap();
            let compacted = segments(&dataset, "_mk_0").await.unwrap();
            assert_eq!(compacted.len(), after.len());
            for f in dataset.fragments().iter() {
                let owners = compacted
                    .iter()
                    .filter(|s| s.fragments.contains(f.id as u32))
                    .count();
                assert!(owners <= 1, "fragment {} in {owners} segments", f.id);
            }
            assert_eq!(agrees(&dataset, "_mk_0").await, 3_297);

            request_removal(&mut dataset, "author").await.unwrap();
            assert_eq!(keys_of(&dataset)[0].state, Some(State::Off));
            remove_column(&mut dataset, "_mk_0").await.unwrap();
            assert!(keys_of(&dataset).is_empty());
            assert!(segments(&dataset, "_mk_0").await.unwrap().is_empty());
            request(&mut dataset, "author").await.unwrap();
            request_removal(&mut dataset, "author").await.unwrap();
            assert!(requests(&dataset).is_empty());
            assert!(request_removal(&mut dataset, "author").await.is_err());
            assert_eq!(
                add_column(&mut dataset, "author").await.unwrap().column,
                "_mk_0"
            );
        }
    }
}
