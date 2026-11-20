//! Filters on indexed metadata keys (see `weed_lance_worker::metadata_index`).
//!
//! The gateway renders a string `$eq` or `$in` on a key as an OR of
//! `json_extract(metadata, P) = '"v"'` (or `IN`) and
//! `json_array_contains(metadata, P, '"v"')` over the same values. In the
//! parsed filter, such a top-level conjunct on a ready key is replaced by
//! `array_has` / `array_has_any` on the key's column, which its LABEL_LIST
//! index answers. Everything else, including `$ne` and `$nin`, stays on the
//! JSON path.

use std::collections::{BTreeSet, HashMap};

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

/// `filter` with each top-level conjunct that is a string `$eq` or `$in` on
/// a key in `ready` answered by the key's column instead, and the
/// conjunction of those replacements.
pub(crate) fn rewrite(filter: Expr, ready: &HashMap<String, String>) -> (Expr, Option<Expr>) {
    if ready.is_empty() {
        return (filter, None);
    }
    let mut indexed: Option<Expr> = None;
    let mut rewritten: Option<Expr> = None;
    for conjunct in split_conjunction_owned(filter) {
        let replacement = string_membership(&conjunct).and_then(|(key, values)| {
            let column = Expr::Column(Column::from_name(ready.get(&key)?));
            Some(match values.as_slice() {
                [one] => array_has(column, lit(one.as_str())),
                _ => {
                    let values: Vec<ScalarValue> = values
                        .iter()
                        .map(|v| ScalarValue::Utf8(Some(v.clone())))
                        .collect();
                    let list = ScalarValue::new_list(&values, &arrow_schema::DataType::Utf8, true);
                    array_has_any(column, Expr::Literal(ScalarValue::List(list), None))
                }
            })
        });
        let part = match replacement {
            Some(expr) => {
                indexed = Some(and(indexed, expr.clone()));
                expr
            }
            None => conjunct,
        };
        rewritten = Some(and(rewritten, part));
    }
    (rewritten.unwrap_or(lit(true)), indexed)
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
            indexed.map(|e| e.to_string()),
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
                query::few_indexed_matches(dataset, table, &q)
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
}
