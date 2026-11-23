//! Filters on indexed metadata keys (see `weed_lance_worker::metadata_index`).
//!
//! The gateway renders a string `$eq` or `$in` on a key as an OR of
//! `json_extract(metadata, P) = '"v"'` (or `IN`) and
//! `json_array_contains(metadata, P, '"v"')` over the same values. In the
//! parsed filter, such a top-level conjunct on a ready key is replaced by
//! `array_has` / `array_has_any` on the key's column, which its LABEL_LIST
//! index answers. Everything else, including `$ne` and `$nin`, stays on the
//! JSON path.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use anyhow::Result;
use jsonb::OwnedJsonb;
use lance::Dataset;
use lance::deps::datafusion::common::{Column, ScalarValue};
use lance::deps::datafusion::functions_nested::expr_fn::{array_has, array_has_any};
use lance::deps::datafusion::logical_expr::utils::split_conjunction_owned;
use lance::deps::datafusion::logical_expr::{BinaryExpr, Expr, Operator};
use lance::deps::datafusion::prelude::lit;
use weed_lance_worker::metadata_index::{self, State};

const METADATA_COLUMN: &str = "metadata";

/// The column of each ready indexed key of `dataset`, by metadata key.
pub(crate) fn ready_columns(dataset: &Dataset) -> HashMap<String, String> {
    let schema: arrow_schema::Schema = dataset.schema().into();
    metadata_index::indexed_keys(&schema)
        .into_iter()
        .filter(|k| k.state == Some(State::Ready))
        .map(|k| (k.key, k.column))
        .collect()
}

/// A conjunct a key's column answers: rows whose `column` holds one of
/// `values`.
#[derive(Debug, Clone)]
pub(crate) struct Indexed {
    pub column: String,
    pub values: Vec<String>,
    pub expr: Expr,
}

/// `filter` with each top-level conjunct that is a string `$eq` or `$in` on
/// a key in `ready` answered by the key's column instead, and those
/// replacements.
pub(crate) fn rewrite(filter: Expr, ready: &HashMap<String, String>) -> (Expr, Vec<Indexed>) {
    if ready.is_empty() {
        return (filter, Vec::new());
    }
    let mut indexed: Vec<Indexed> = Vec::new();
    let mut rewritten: Option<Expr> = None;
    for conjunct in split_conjunction_owned(filter) {
        let replacement = string_membership(&conjunct).and_then(|(key, values)| {
            let name = ready.get(&key)?;
            let column = Expr::Column(Column::from_name(name));
            let expr = match values.as_slice() {
                [one] => array_has(column, lit(one.as_str())),
                _ => {
                    let values: Vec<ScalarValue> = values
                        .iter()
                        .map(|v| ScalarValue::Utf8(Some(v.clone())))
                        .collect();
                    let list = ScalarValue::new_list(&values, &arrow_schema::DataType::Utf8, true);
                    array_has_any(column, Expr::Literal(ScalarValue::List(list), None))
                }
            };
            Some(Indexed {
                column: name.clone(),
                values,
                expr,
            })
        });
        let part = match replacement {
            Some(found) => {
                let expr = found.expr.clone();
                indexed.push(found);
                expr
            }
            None => conjunct,
        };
        rewritten = Some(and(rewritten, part));
    }
    (rewritten.unwrap_or(lit(true)), indexed)
}

pub(crate) fn columns(dataset: &Dataset, filter: &str) -> Result<Vec<String>> {
    let mut scanner = dataset.scan();
    scanner.filter(filter)?;
    let expr = scanner.get_expr_filter()?.ok_or_else(|| {
        crate::query::IndexedOnlyError("indexed-only requires an indexed metadata filter".into())
    })?;
    let count = split_conjunction_owned(expr.clone()).len();
    let (_, indexed) = rewrite(expr, &ready_columns(dataset));
    if indexed.len() != count {
        return Err(crate::query::IndexedOnlyError("indexed-only supports conjunctions of string $eq/$in filters on ready metadata indexes".into()).into());
    }
    Ok(indexed.into_iter().map(|i| i.column).collect())
}

fn and(left: Option<Expr>, right: Expr) -> Expr {
    match left {
        Some(left) => Expr::BinaryExpr(BinaryExpr::new(
            Box::new(left),
            Operator::And,
            Box::new(right),
        )),
        None => right,
    }
}

/// The key and string values of a conjunct meaning "the key holds one of
/// these strings, or an array with one of them": the OR of scalar matches
/// and array-member matches over the same values and key.
fn string_membership(expr: &Expr) -> Option<(String, Vec<String>)> {
    let mut disjuncts = Vec::new();
    split_or(expr, &mut disjuncts);
    let mut path: Option<&str> = None;
    let mut scalars = BTreeSet::new();
    let mut elements = BTreeSet::new();
    for disjunct in disjuncts {
        let (p, kind) = match unwrap_false_coalesce(disjunct) {
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Operator::Eq,
                right,
            }) => (
                json_function(left, "json_extract")?,
                Member::Scalar(vec![utf8(right)?]),
            ),
            Expr::InList(list) if !list.negated => (
                json_function(&list.expr, "json_extract")?,
                Member::Scalar(list.list.iter().map(utf8).collect::<Option<_>>()?),
            ),
            Expr::ScalarFunction(f) if f.func.name() == "json_array_contains" => {
                let [data, p, value] = f.args.as_slice() else {
                    return None;
                };
                if !is_metadata(data) {
                    return None;
                }
                let element: serde_json::Value = serde_json::from_str(utf8(value)?).ok()?;
                (utf8(p)?, Member::Element(element.as_str()?.to_string()))
            }
            _ => return None,
        };
        if path.is_some_and(|q| q != p) {
            return None;
        }
        path = Some(p);
        match kind {
            Member::Scalar(values) => scalars.extend(values.into_iter().map(str::to_string)),
            Member::Element(value) => {
                elements.insert(value);
            }
        }
    }
    let key = json_path_key(path?)?;
    let rendered: BTreeSet<String> = elements
        .iter()
        .map(|s| jsonb_text(s))
        .collect::<Option<_>>()?;
    (!elements.is_empty() && rendered == scalars).then(|| (key, elements.into_iter().collect()))
}

enum Member<'a> {
    Scalar(Vec<&'a str>),
    Element(String),
}

fn split_or<'a>(expr: &'a Expr, out: &mut Vec<&'a Expr>) {
    match expr {
        Expr::BinaryExpr(BinaryExpr {
            left,
            op: Operator::Or,
            right,
        }) => {
            split_or(left, out);
            split_or(right, out);
        }
        Expr::Alias(alias) => split_or(&alias.expr, out),
        other => out.push(other),
    }
}

/// `x` for `COALESCE(x, false)` or its simplified `x IS NOT NULL AND x`;
/// either is false where `x` is null, which a top-level conjunct treats alike.
fn unwrap_false_coalesce(expr: &Expr) -> &Expr {
    match expr {
        Expr::ScalarFunction(f) if f.func.name() == "coalesce" => match f.args.as_slice() {
            [x, Expr::Literal(ScalarValue::Boolean(Some(false)), _)] => x,
            _ => expr,
        },
        Expr::BinaryExpr(BinaryExpr {
            left,
            op: Operator::And,
            right,
        }) => match left.as_ref() {
            Expr::IsNotNull(x) if x == right => right,
            _ => expr,
        },
        _ => expr,
    }
}

fn utf8(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Literal(ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)), _) => {
            Some(s.as_str())
        }
        _ => None,
    }
}

fn is_metadata(expr: &Expr) -> bool {
    matches!(expr, Expr::Column(c) if c.name == METADATA_COLUMN)
}

/// The JSONPath literal of `name(metadata, path)`.
fn json_function<'a>(expr: &'a Expr, name: &str) -> Option<&'a str> {
    match expr {
        Expr::ScalarFunction(f) if f.func.name() == name => match f.args.as_slice() {
            [data, path] if is_metadata(data) => utf8(path),
            _ => None,
        },
        _ => None,
    }
}

/// The key of a `$["key"]` path, with `\\` and `\"` unescaped.
fn json_path_key(path: &str) -> Option<String> {
    let inner = path.strip_prefix("$[\"")?.strip_suffix("\"]")?;
    let mut key = String::with_capacity(inner.len());
    let mut chars = inner.chars();
    while let Some(c) = chars.next() {
        match c {
            '\\' => match chars.next()? {
                c @ ('\\' | '"') => key.push(c),
                _ => return None,
            },
            '"' => return None,
            c => key.push(c),
        }
    }
    Some(key)
}

/// What json_extract returns for a stored string `s`.
fn jsonb_text(s: &str) -> Option<String> {
    let value: OwnedJsonb = serde_json::to_string(s).ok()?.parse().ok()?;
    Some(value.to_string())
}

/// Segments searched at once when counting.
const PARALLEL_SEGMENTS: usize = 8;

/// The rows of `scope` (every fragment when None) whose column matches
/// `indexed`, or None once there are more than `limit`. Segments are
/// searched newest first, several at once, and counting stops as soon as
/// the total passes the limit. Rows the index has not reached are counted
/// by a read of the column. Deleted rows an index segment still lists
/// count too: the total is an upper bound, which only ever sends a query
/// to the vector index instead of an exact scan.
pub(crate) async fn count_indexed(
    dataset: &Dataset,
    indexed: &Indexed,
    scope: Option<&[u64]>,
    limit: u64,
) -> Result<Option<u64>> {
    use futures::{StreamExt, TryStreamExt};
    use lance::index::DatasetIndexInternalExt;
    use lance_index::metrics::NoOpMetricsCollector;
    use lance_index::scalar::{LabelListQuery, SearchResult};
    use lance_select::RowSetOps;

    let scope: Option<roaring::RoaringBitmap> =
        scope.map(|ids| ids.iter().map(|id| *id as u32).collect());
    let segments = metadata_index::segments(dataset, &indexed.column).await?;
    let query = LabelListQuery::HasAnyLabel(
        indexed
            .values
            .iter()
            .map(|v| ScalarValue::Utf8(Some(v.clone())))
            .collect(),
    );
    let mut searches: Vec<futures::future::BoxFuture<'_, Result<u64>>> = Vec::new();
    for segment in segments.iter().rev() {
        let fragments = match &scope {
            Some(scope) => &segment.fragments & scope,
            None => segment.fragments.clone(),
        };
        if fragments.is_empty() {
            continue;
        }
        let uuid = segment.uuid;
        let query = &query;
        searches.push(Box::pin(async move {
            let index = dataset
                .open_scalar_index(&indexed.column, &uuid, &NoOpMetricsCollector)
                .await?;
            let mut found = match index.search(query, &NoOpMetricsCollector).await? {
                SearchResult::Exact(rows) | SearchResult::AtMost(rows) => rows.true_rows(),
                SearchResult::AtLeast(_) => anyhow::bail!("a LABEL_LIST index answered AtLeast"),
            };
            found.retain_fragments(fragments.iter());
            Ok(found.len().unwrap_or(u64::MAX))
        }));
    }
    let mut counts = futures::stream::iter(searches).buffered(PARALLEL_SEGMENTS);
    let mut total = 0u64;
    while let Some(count) = counts.try_next().await? {
        total = total.saturating_add(count);
        if total > limit {
            return Ok(None);
        }
    }
    let unindexed: Vec<lance_table::format::Fragment> = dataset
        .fragments()
        .iter()
        .filter(|f| !segments.iter().any(|s| s.fragments.contains(f.id as u32)))
        .filter(|f| scope.as_ref().is_none_or(|s| s.contains(f.id as u32)))
        .cloned()
        .collect();
    if !unindexed.is_empty() {
        let mut scanner = dataset.scan();
        scanner
            .filter_expr(indexed.expr.clone())
            .with_fragments(unindexed);
        total = total.saturating_add(scanner.count_rows().await?);
    }
    Ok((total <= limit).then_some(total))
}

/// How a filter compares a key.
pub mod operand {
    pub const STRING: u8 = 1;
    pub const NUMBER: u8 = 2;
    pub const BOOL: u8 = 4;
    pub const EXISTS: u8 = 8;
}

/// The metadata keys `filter` reads, each with the [`operand`] kinds it is
/// compared with.
pub fn filter_keys(dataset: &Dataset, filter: &str) -> Result<BTreeMap<String, u8>> {
    let mut scanner = dataset.scan();
    scanner.filter(filter)?;
    let mut keys = BTreeMap::new();
    if let Some(expr) = scanner.get_expr_filter()? {
        collect_keys(&expr, &mut keys);
    }
    Ok(keys)
}

fn literal_kind(text: &str) -> u8 {
    match serde_json::from_str::<serde_json::Value>(text) {
        Ok(serde_json::Value::String(_)) => operand::STRING,
        Ok(serde_json::Value::Number(_)) => operand::NUMBER,
        Ok(serde_json::Value::Bool(_)) => operand::BOOL,
        // json_extract's text of a string with control characters is not
        // JSON, but it is quoted.
        _ if text.starts_with('"') => operand::STRING,
        _ => 0,
    }
}

fn collect_keys(expr: &Expr, keys: &mut BTreeMap<String, u8>) {
    let mut note = |path: &str, kind: u8| {
        if let Some(key) = json_path_key(path) {
            *keys.entry(key).or_default() |= kind;
        }
    };
    match expr {
        Expr::TryCast(cast) if extract(&cast.expr).is_some() => {
            note(extract(&cast.expr).unwrap_or_default(), operand::NUMBER);
            return;
        }
        Expr::BinaryExpr(BinaryExpr { left, right, .. }) => {
            if let (Some(path), Some(value)) = (extract(left), utf8(right)) {
                note(path, literal_kind(value));
                return;
            }
        }
        Expr::InList(list) => {
            if let Some(path) = extract(&list.expr) {
                let kind = list
                    .list
                    .iter()
                    .filter_map(utf8)
                    .fold(0, |k, v| k | literal_kind(v));
                note(path, kind);
                return;
            }
        }
        Expr::ScalarFunction(f) if is_json_function(f.func.name()) => {
            if let [data, path, rest @ ..] = f.args.as_slice()
                && is_metadata(data)
                && let Some(path) = utf8(path)
            {
                let kind = match (f.func.name(), rest) {
                    ("json_exists", _) => operand::EXISTS,
                    (_, [value]) => utf8(value).map_or(0, literal_kind),
                    _ => 0,
                };
                note(path, kind);
                return;
            }
        }
        _ => {}
    }
    for child in children(expr) {
        collect_keys(child, keys);
    }
}

fn extract(expr: &Expr) -> Option<&str> {
    json_function(expr, "json_extract")
}

fn is_json_function(name: &str) -> bool {
    matches!(name, "json_extract" | "json_exists" | "json_array_contains")
}

fn children(expr: &Expr) -> Vec<&Expr> {
    use lance::deps::datafusion::common::tree_node::TreeNode;
    let mut out = Vec::new();
    let _ = expr.apply_children(|child| {
        out.push(child);
        Ok(lance::deps::datafusion::common::tree_node::TreeNodeRecursion::Continue)
    });
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::{self, Query};
    use crate::table::VectorTable;
    use arrow_array::{FixedSizeListArray, RecordBatch, RecordBatchIterator, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use lance::index::DatasetIndexExt;
    use lance_index::IndexType;
    use lance_index::scalar::{BuiltinIndexType, ScalarIndexParams};
    use lance_linalg::distance::DistanceType;
    use std::sync::Arc;
    use tempfile::TempDir;

    const DIM: usize = 4;

    /// The gateway's text for `{"author": "<v>"}`.
    fn eq(field: &str, json: &str) -> String {
        let json = json.replace('\'', "''");
        format!(
            "(COALESCE(json_extract(`metadata`, '$[\"{field}\"]') = '{json}', false) OR COALESCE(json_array_contains(`metadata`, '$[\"{field}\"]', '{json}'), false))"
        )
    }

    /// The gateway's text for `{"author": {"$in": [<strings>]}}`.
    fn among(values: &[&str]) -> String {
        let scalars: Vec<String> = values.iter().map(|v| format!("'\"{v}\"'")).collect();
        let mut groups = vec![format!(
            "COALESCE(json_extract(`metadata`, '$[\"author\"]') in ({}), false)",
            scalars.join(", ")
        )];
        for v in values {
            groups.push(format!(
                "COALESCE(json_array_contains(`metadata`, '$[\"author\"]', '\"{v}\"'), false)"
            ));
        }
        format!("({})", groups.join(" OR "))
    }

    /// Rows `u0..u<n>` by author, plus a few others, in fragments of 100,
    /// with the author column ready and indexed and a vector index.
    async fn dataset(dir: &TempDir, rows: usize) -> Dataset {
        let metadata = Field::new("metadata", DataType::Utf8, true)
            .with_metadata([("ARROW:extension:name".to_string(), "arrow.json".to_string())].into());
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new(
                "vector",
                DataType::FixedSizeList(
                    Arc::new(Field::new("item", DataType::Float32, true)),
                    DIM as i32,
                ),
                false,
            ),
            metadata,
            metadata_index::field("_mk_0", "author", State::Ready),
        ]));
        let metas: Vec<serde_json::Value> = (0..rows)
            .map(|i| match i % 10 {
                0 => serde_json::json!({"author": "rare"}),
                1 => serde_json::json!({"author": ["many", "rare2"]}),
                _ => serde_json::json!({"author": "many", "n": i}),
            })
            .collect();
        let batches: Vec<RecordBatch> = (0..rows)
            .step_by(100)
            .map(|start| {
                let range = start..(start + 100).min(rows);
                RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(StringArray::from(
                            range.clone().map(|i| format!("k{i}")).collect::<Vec<_>>(),
                        )),
                        Arc::new(FixedSizeListArray::from_iter_primitive::<
                            arrow_array::types::Float32Type,
                            _,
                            _,
                        >(
                            range.clone().map(|i| {
                                Some((0..DIM).map(move |j| Some(((i * 31 + j * 17) % 97) as f32)))
                            }),
                            DIM as i32,
                        )),
                        Arc::new(StringArray::from(
                            range
                                .clone()
                                .map(|i| Some(metas[i].to_string()))
                                .collect::<Vec<_>>(),
                        )),
                        metadata_index::column_from_json(range.map(|i| Some(&metas[i])), "author"),
                    ],
                )
                .unwrap()
            })
            .collect();
        let uri = dir.path().to_str().unwrap();
        let mut dataset = None::<Dataset>;
        for batch in batches {
            let reader = RecordBatchIterator::new(vec![Ok(batch)], schema.clone());
            dataset = Some(match dataset {
                None => Dataset::write(reader, uri, None).await.unwrap(),
                Some(mut d) => {
                    d.append(reader, None).await.unwrap();
                    d
                }
            });
        }
        let mut dataset = dataset.unwrap();
        dataset
            .create_index(
                &["_mk_0"],
                IndexType::LabelList,
                None,
                &ScalarIndexParams::for_builtin(BuiltinIndexType::LabelList),
                true,
            )
            .await
            .unwrap();
        crate::index::build(&mut dataset, "vector", crate::index::INDEX_NAME, 4, false)
            .await
            .unwrap();
        dataset
    }

    fn rewritten(dataset: &Dataset, filter: &str) -> (String, Option<String>) {
        let mut scanner = dataset.scan();
        let indexed = crate::filter::apply(&mut scanner, filter, &ready_columns(dataset)).unwrap();
        (
            scanner.get_expr_filter().unwrap().unwrap().to_string(),
            indexed.first().map(|i| i.expr.to_string()),
        )
    }

    #[tokio::test]
    async fn string_eq_and_in_on_a_ready_key_use_its_column() {
        let dir = TempDir::new().unwrap();
        let dataset = dataset(&dir, 200).await;
        assert_eq!(
            ready_columns(&dataset),
            HashMap::from([("author".to_string(), "_mk_0".to_string())])
        );
        for (filter, want) in [
            (eq("author", "\"rare\""), Some("array_has(_mk_0, Utf8(\"rare\"))")),
            (
                among(&["rare", "many"]),
                Some("array_has_any(_mk_0, List([many, rare]))"),
            ),
            (
                format!("{} AND TRY_CAST(json_extract(`metadata`, '$[\"n\"]') AS DOUBLE) > 3", eq("author", "\"rare\"")),
                Some("array_has(_mk_0, Utf8(\"rare\"))"),
            ),
            (eq("author", "\"o'neil \\\"q\\\"\""), Some("array_has(_mk_0, Utf8(\"o'neil \"q\"\"))")),
            (
                "(COALESCE(json_extract(`metadata`, '$[\"author\"]') = '\"c\\u0001d\\n\"', false) OR COALESCE(json_array_contains(`metadata`, '$[\"author\"]', '\"c\\u0001d\\n\"'), false))".to_string(),
                Some("array_has(_mk_0, Utf8(\"c\u{1}d\n\"))"),
            ),
            // The scalar half's text is not what json_extract returns for
            // the array half's string, so the two do not agree.
            (
                "(COALESCE(json_extract(`metadata`, '$[\"author\"]') = '\"c\\u{1}d\\n\"', false) OR COALESCE(json_array_contains(`metadata`, '$[\"author\"]', '\"c\\u0001d\\n\"'), false))".to_string(),
                None,
            ),
            // Not a string, not this key, or not every value on both sides.
            (eq("author", "true"), None),
            (eq("other", "\"rare\""), None),
            (format!("NOT {}", eq("author", "\"rare\"")), None),
            (format!("NOT {}", among(&["rare", "many"])), None),
            (
                "COALESCE(json_extract(`metadata`, '$[\"author\"]') = '\"rare\"', false)".to_string(),
                None,
            ),
            (
                among(&["rare", "many"]).replace("'\"many\"'), false))", "'\"other\"'), false))"),
                None,
            ),
            ("json_exists(`metadata`, '$[\"author\"]')".to_string(), None),
            (format!("({} OR json_exists(`metadata`, '$[\"n\"]'))", eq("author", "\"rare\"")), None),
        ] {
            let (expr, indexed) = rewritten(&dataset, &filter);
            assert_eq!(indexed.as_deref(), want, "{filter}");
            assert_eq!(expr.contains("json_"), want.is_none() || filter.contains(" AND "), "{filter}: {expr}");
        }
    }

    /// The planner scores few matches exactly and leaves many to the vector
    /// index, per scope.
    #[tokio::test]
    async fn few_indexed_matches_are_scored_exactly() {
        let dir = TempDir::new().unwrap();
        let dataset = dataset(&dir, 1000).await;
        let table = VectorTable::resolve(&dataset, None, None).unwrap();
        let query = |filter: &str, fragments: Option<Vec<u64>>| Query {
            vector: vec![1.0, 2.0, 3.0, 4.0],
            k: 10,
            filter: Some(filter.to_string()),
            postfilter: false,
            distance: DistanceType::L2,
            nprobes: None,
            index_segments: None,
            fragments,
            exact_match_limit: None,
        };
        let rare = eq("author", "\"rare\"");
        let many = eq("author", "\"many\"");
        let few = |q: Query| {
            let (dataset, table) = (&dataset, &table);
            async move {
                query::few_indexed_matches(dataset, table, &q, None)
                    .await
                    .unwrap()
            }
        };
        // 100 and 900 matches, and 90 in the first two fragments.
        assert!(few(query(&rare, None)).await);
        assert!(!few(query(&many, None)).await);
        let first = Some(vec![0, 1]);
        assert!(few(query(&many, first.clone())).await);
        assert!(
            !few(Query {
                exact_match_limit: Some(0),
                ..query(&rare, None)
            })
            .await
        );
        // A JSON-only filter is not counted.
        assert!(!few(query("json_exists(`metadata`, '$[\"n\"]')", None)).await);
        for (filter, fragments) in [(&rare, None), (&many, first)] {
            let q = query(filter, fragments);
            let exact = query::flat(&dataset, &table, &q).await.unwrap();
            let got = query::ann(&dataset, &table, &q).await.unwrap();
            assert_eq!(got.len(), 10);
            for (g, e) in got.iter().zip(&exact) {
                assert!(
                    (g.distance - e.distance).abs() < 1e-4,
                    "{got:?} vs {exact:?}"
                );
            }
        }
    }

    #[tokio::test]
    async fn indexed_only_intersects_metadata_and_vector_coverage() {
        use futures::TryStreamExt;
        let dir = TempDir::new().unwrap();
        let mut dataset = dataset(&dir, 300).await;
        let batches = dataset
            .scan()
            .try_into_stream()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let batch = &batches[0];
        let mut columns = batch.columns().to_vec();
        let key = batch.schema().index_of("key").unwrap();
        columns[key] = Arc::new(StringArray::from(
            (0..batch.num_rows())
                .map(|i| format!("new{i}"))
                .collect::<Vec<_>>(),
        ));
        let batch = RecordBatch::try_new(batch.schema(), columns).unwrap();
        let reader = RecordBatchIterator::new(vec![Ok(batch.clone())], batch.schema());
        dataset.append(reader, None).await.unwrap();
        dataset
            .optimize_indices(&lance_index::optimize::OptimizeOptions::append())
            .await
            .unwrap();
        let table = VectorTable::resolve(&dataset, None, None).unwrap();
        let query = Query {
            vector: vec![1.0, 2.0, 3.0, 4.0],
            k: 1000,
            filter: Some(eq("author", "\"rare\"")),
            postfilter: false,
            distance: DistanceType::L2,
            nprobes: Some(4),
            index_segments: None,
            fragments: None,
            exact_match_limit: None,
        };
        let reader = RecordBatchIterator::new(vec![Ok(batch.clone())], batch.schema());
        dataset.append(reader, None).await.unwrap();
        crate::index::build(&mut dataset, "vector", crate::index::INDEX_NAME, 4, true)
            .await
            .unwrap();
        let tail = Query {
            fragments: Some(vec![dataset.fragments().last().unwrap().id]),
            ..query.clone()
        };
        let (hits, excluded) = query::indexed_only(&dataset, &table, &tail).await.unwrap();
        assert!(hits.is_empty());
        assert_eq!(excluded, 1);
        for limit in [None, Some(0)] {
            let query = Query {
                exact_match_limit: limit,
                ..query.clone()
            };
            let all = query::ann(&dataset, &table, &query).await.unwrap();
            let (indexed, excluded) = query::indexed_only(&dataset, &table, &query).await.unwrap();
            assert_eq!(excluded, 1);
            assert!(all.len() > indexed.len());
            assert!(indexed.len() >= 30);
        }
        for file in &dataset.fragments().last().unwrap().files {
            std::fs::remove_file(dir.path().join("data").join(&file.path)).unwrap();
        }
        let dataset = Dataset::open(dir.path().to_str().unwrap()).await.unwrap();
        assert!(query::ann(&dataset, &table, &query).await.is_err());
        for limit in [None, Some(0)] {
            let query = Query {
                exact_match_limit: limit,
                ..query.clone()
            };
            assert!(query::indexed_only(&dataset, &table, &query).await.is_ok());
        }
        for filter in [
            eq("other", "\"rare\""),
            eq("author", "true"),
            format!("NOT {}", eq("author", "\"rare\"")),
        ] {
            let query = Query {
                filter: Some(filter),
                ..query.clone()
            };
            assert!(
                query::indexed_only(&dataset, &table, &query)
                    .await
                    .unwrap_err()
                    .is::<query::IndexedOnlyError>()
            );
        }
    }

    #[tokio::test]
    async fn the_plan_reads_the_scalar_index() {
        let dir = TempDir::new().unwrap();
        let dataset = dataset(&dir, 300).await;
        let mut scanner = dataset.scan();
        crate::filter::apply(
            &mut scanner,
            &eq("author", "\"rare\""),
            &ready_columns(&dataset),
        )
        .unwrap();
        let plan = scanner.explain_plan(true).await.unwrap();
        assert!(plan.contains("LabelList"), "{plan}");
        assert!(!plan.contains("json_"), "{plan}");
    }

    #[tokio::test]
    async fn filter_keys_note_how_each_key_is_compared() {
        let dir = TempDir::new().unwrap();
        let dataset = dataset(&dir, 100).await;
        let filter = format!(
            "{} AND TRY_CAST(json_extract(`metadata`, '$[\"n\"]') AS DOUBLE) > 3 AND NOT json_exists(`metadata`, '$[\"a.b\\\"c\"]') AND COALESCE(json_extract(`metadata`, '$[\"f\"]') = 'true', false)",
            eq("author", "\"rare\"")
        );
        let keys = filter_keys(&dataset, &filter).unwrap();
        assert_eq!(
            keys,
            BTreeMap::from([
                ("a.b\"c".to_string(), operand::EXISTS),
                ("author".to_string(), operand::STRING),
                ("f".to_string(), operand::BOOL),
                ("n".to_string(), operand::NUMBER),
            ])
        );
    }

    /// Counting walks the segments, keeps to the scope and stops once over
    /// the limit.
    #[tokio::test]
    async fn counts_follow_segments_and_scope() {
        let dir = TempDir::new().unwrap();
        let mut dataset = dataset(&dir, 1000).await;
        dataset.drop_index("_mk_0_idx").await.unwrap();
        // Ten fragments of 100 rows: four segments of up to 300 rows.
        let ids: Vec<u64> = dataset.fragments().iter().map(|f| f.id).collect();
        metadata_index::index_fragments(&mut dataset, "_mk_0", 300, 0)
            .await
            .unwrap();
        let segments = metadata_index::segments(&dataset, "_mk_0").await.unwrap();
        assert_eq!(segments.len(), 4);
        let ready = ready_columns(&dataset);
        let indexed =
            crate::filter::apply(&mut dataset.scan(), &eq("author", "\"rare\""), &ready).unwrap();
        let rare = &indexed[0];
        // "rare" in rows i % 10 == 0, "rare2" never matches it.
        assert_eq!(
            count_indexed(&dataset, rare, None, 1_000).await.unwrap(),
            Some(100)
        );
        assert_eq!(count_indexed(&dataset, rare, None, 99).await.unwrap(), None);
        assert_eq!(
            count_indexed(&dataset, rare, Some(&ids[..2]), 1_000)
                .await
                .unwrap(),
            Some(20)
        );
        assert_eq!(
            count_indexed(&dataset, rare, Some(&ids[9..]), 1_000)
                .await
                .unwrap(),
            Some(10)
        );
        assert_eq!(
            count_indexed(&dataset, rare, Some(&[]), 1_000)
                .await
                .unwrap(),
            Some(0)
        );
    }
}
