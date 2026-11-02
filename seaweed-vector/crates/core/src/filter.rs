//! Lance's json_extract and json_exists parse their JSONPath again for every
//! row. A filter's path is a literal, so swap in copies that parse it once per
//! batch and return the same values. json_array_contains is swapped too, and
//! compares the elements as typed JSON values: Lance's compares their text, so
//! 1 would match "1".

use std::sync::Arc;

use arrow_array::builder::{BooleanBuilder, StringBuilder};
use arrow_array::{Array, ArrayRef, LargeBinaryArray, new_null_array};
use arrow_schema::DataType;
use jsonb::jsonpath::{Selector, parse_json_path};
use jsonb::{OwnedJsonb, RawJsonb};
use lance::dataset::scanner::Scanner;
use lance::deps::datafusion::common::tree_node::{Transformed, TreeNode};
use lance::deps::datafusion::common::{DataFusionError, Result, ScalarValue};
use lance::deps::datafusion::logical_expr::expr::ScalarFunction;
use lance::deps::datafusion::logical_expr::{
    ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

/// Set `filter` on the scanner with the JSONPath functions swapped.
pub(crate) fn apply(scanner: &mut Scanner, filter: &str) -> anyhow::Result<()> {
    scanner.filter(filter)?;
    if let Some(expr) = scanner.get_expr_filter()? {
        scanner.filter_expr(swap(expr)?);
    }
    Ok(())
}

fn swap(expr: Expr) -> Result<Expr> {
    expr.transform(|e| {
        let Expr::ScalarFunction(f) = e else {
            return Ok(Transformed::no(e));
        };
        let op = match f.func.name() {
            "json_extract" => Op::Extract,
            "json_exists" => Op::Exists,
            "json_array_contains" => Op::Contains,
            _ => return Ok(Transformed::no(Expr::ScalarFunction(f))),
        };
        let literal = |i: usize| {
            matches!(
                f.args.get(i),
                Some(Expr::Literal(ScalarValue::Utf8(Some(_)), _))
            )
        };
        if !literal(1) || (op == Op::Contains && !literal(2)) {
            return Ok(Transformed::no(Expr::ScalarFunction(f)));
        }
        let udf = Arc::new(ScalarUDF::new_from_impl(JsonPath::new(op)));
        Ok(Transformed::yes(Expr::ScalarFunction(
            ScalarFunction::new_udf(udf, f.args),
        )))
    })
    .map(|t| t.data)
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
enum Op {
    Extract,
    Exists,
    Contains,
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonPath {
    op: Op,
    signature: Signature,
}

impl JsonPath {
    fn new(op: Op) -> Self {
        let mut args = vec![DataType::LargeBinary, DataType::Utf8];
        if op == Op::Contains {
            args.push(DataType::Utf8);
        }
        Self {
            op,
            signature: Signature::exact(args, Volatility::Immutable),
        }
    }
}

fn exec_err(msg: String) -> DataFusionError {
    DataFusionError::Execution(msg)
}

impl ScalarUDFImpl for JsonPath {
    fn name(&self) -> &str {
        match self.op {
            Op::Extract => "json_extract",
            Op::Exists => "json_exists",
            Op::Contains => "json_array_contains",
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(match self.op {
            Op::Extract => DataType::Utf8,
            Op::Exists | Op::Contains => DataType::Boolean,
        })
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let (data, path, value) = match args.args.as_slice() {
            [data, ColumnarValue::Scalar(ScalarValue::Utf8(Some(path)))] => (data, path, None),
            [
                data,
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(path))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(value))),
            ] => (data, path, Some(value)),
            _ => {
                return Err(exec_err(format!(
                    "{} expects literal arguments",
                    self.name()
                )));
            }
        };
        let data = data.to_array(args.number_rows)?;
        let data = data
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .ok_or_else(|| exec_err("First argument must be LargeBinary".to_string()))?;
        // The original parses per non-null row, so an all-null batch never fails.
        if data.null_count() == data.len() {
            let return_type = self.return_type(&[])?;
            return Ok(ColumnarValue::Array(new_null_array(
                &return_type,
                data.len(),
            )));
        }
        let json_path = parse_json_path(path.as_bytes())
            .map_err(|e| exec_err(format!("Invalid JSONPath '{path}': {e}")))?;
        let out: ArrayRef = match self.op {
            Op::Extract => {
                let mut builder = StringBuilder::with_capacity(data.len(), 1024);
                for i in 0..data.len() {
                    if data.is_null(i) {
                        builder.append_null();
                        continue;
                    }
                    let mut selector = Selector::new(RawJsonb::new(data.value(i)));
                    match selector.select_value(&json_path) {
                        Ok(Some(value)) => builder.append_value(value.to_string()),
                        Ok(None) => builder.append_null(),
                        Err(e) => {
                            return Err(exec_err(format!(
                                "Failed to select value from path '{path}': {e}"
                            )));
                        }
                    }
                }
                Arc::new(builder.finish())
            }
            Op::Exists => {
                let mut builder = BooleanBuilder::with_capacity(data.len());
                for i in 0..data.len() {
                    if data.is_null(i) {
                        builder.append_null();
                        continue;
                    }
                    let mut selector = Selector::new(RawJsonb::new(data.value(i)));
                    match selector.exists(&json_path) {
                        Ok(exists) => builder.append_value(exists),
                        Err(e) => {
                            return Err(exec_err(format!(
                                "Failed to check existence of path '{path}': {e}"
                            )));
                        }
                    }
                }
                Arc::new(builder.finish())
            }
            Op::Contains => {
                let value = value.map(String::as_str).unwrap_or_default();
                let value: OwnedJsonb = value
                    .parse()
                    .map_err(|e| exec_err(format!("Invalid JSON value '{value}': {e}")))?;
                let mut builder = BooleanBuilder::with_capacity(data.len());
                for i in 0..data.len() {
                    if data.is_null(i) {
                        builder.append_null();
                        continue;
                    }
                    let mut selector = Selector::new(RawJsonb::new(data.value(i)));
                    let found = match selector.select_value(&json_path) {
                        Ok(Some(array)) => array
                            .as_raw()
                            .array_values()
                            .map_err(|e| {
                                exec_err(format!("Failed to read array at '{path}': {e}"))
                            })?
                            .is_some_and(|items| items.contains(&value)),
                        Ok(None) => false,
                        Err(e) => {
                            return Err(exec_err(format!(
                                "Failed to check array contains at path '{path}': {e}"
                            )));
                        }
                    };
                    builder.append_value(found);
                }
                Arc::new(builder.finish())
            }
        };
        Ok(ColumnarValue::Array(out))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::DIM;
    use arrow_array::{
        FixedSizeListArray, RecordBatch, RecordBatchIterator, StringArray, types::Float32Type,
    };
    use arrow_schema::{Field, Schema};
    use futures::TryStreamExt;
    use lance::Dataset;
    use tempfile::TempDir;

    async fn dataset(dir: &TempDir, metas: &[Option<&str>]) -> Dataset {
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
            Field::new("metadata", DataType::Utf8, true).with_metadata(
                [("ARROW:extension:name".to_string(), "arrow.json".to_string())]
                    .into_iter()
                    .collect(),
            ),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(
                    (0..metas.len())
                        .map(|i| format!("k{i}"))
                        .collect::<Vec<_>>(),
                )),
                Arc::new(
                    FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
                        (0..metas.len()).map(|i| Some(vec![Some(i as f32); DIM])),
                        DIM as i32,
                    ),
                ),
                Arc::new(StringArray::from(metas.to_vec())),
            ],
        )
        .unwrap();
        let reader = RecordBatchIterator::new(vec![Ok(batch)].into_iter(), schema);
        Dataset::write(reader, dir.path().to_str().unwrap(), None)
            .await
            .unwrap()
    }

    async fn keys(dataset: &Dataset, filter: &str, swapped: bool) -> anyhow::Result<Vec<String>> {
        let mut scanner = dataset.scan();
        if swapped {
            apply(&mut scanner, filter)?;
        } else {
            scanner.filter(filter)?;
        }
        scanner.project(&["key"])?;
        let batches: Vec<RecordBatch> = scanner.try_into_stream().await?.try_collect().await?;
        let mut out: Vec<String> = batches
            .iter()
            .flat_map(|b| {
                let keys = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
                keys.iter()
                    .map(|k| k.unwrap().to_string())
                    .collect::<Vec<_>>()
            })
            .collect();
        out.sort();
        Ok(out)
    }

    #[tokio::test]
    async fn swapped_functions_match_lance() {
        let dir = TempDir::new().unwrap();
        let metas = [
            Some(r#"{"k": 3}"#),
            Some(r#"{"k": "3"}"#),
            Some(r#"{"k": true}"#),
            Some(r#"{"k": "v"}"#),
            Some(r#"{"k": "a"}"#),
            Some(r#"{"k": "b"}"#),
            Some(r#"{"x": 1}"#),
            Some(r#"{"k": [1, 2]}"#),
            Some(r#"{"k": null}"#),
            Some(r#"{"k": 1}"#),
            Some(r#"{"k": 2.5}"#),
            Some(r#"{"k": "line\nbreak"}"#),
            Some(r#"{"a": "1", "b": 2}"#),
            Some(r#"{"a.b\"c": 1}"#),
            Some(r#"{"a'b": "v'1"}"#),
            Some(r#"{"k": {"x": [1, true, "text"]}}"#),
            None,
        ];
        let dataset = dataset(&dir, &metas).await;
        let k = r#"`metadata`, '$["k"]'"#;
        let filters = [
            format!(r#"json_extract({k}) = '"v"'"#),
            format!("TRY_CAST(json_extract({k}) AS DOUBLE) = 3"),
            format!("json_extract({k}) = 'true'"),
            format!(r#"NOT COALESCE(json_extract({k}) = '"v"', false)"#),
            format!("json_exists({k})"),
            format!("NOT json_exists({k})"),
            r#"json_exists(`metadata`, '$["a.b\"c"]')"#.to_string(),
            format!("TRY_CAST(json_extract({k}) AS DOUBLE) > 1"),
            format!(
                r#"(COALESCE(json_extract({k}) in ('"a"'), false) OR COALESCE(json_extract({k}) in ('"b"'), false))"#
            ),
            format!("NOT (COALESCE(TRY_CAST(json_extract({k}) AS DOUBLE) in (1), false))"),
            format!(
                r#"(COALESCE(json_extract({k}) in ('"a"', 'true'), false) OR COALESCE(TRY_CAST(json_extract({k}) AS DOUBLE) in (1, 2.5), false))"#
            ),
            r#"(json_extract(`metadata`, '$["a"]') = '"1"' AND TRY_CAST(json_extract(`metadata`, '$["b"]') AS DOUBLE) = 2)"#.to_string(),
            r#"json_extract(`metadata`, '$["a''b"]') = '"v''1"'"#.to_string(),
            format!(r#"json_extract({k}) = '"line\nbreak"'"#),
            format!(r#"json_extract({k}) = '{{"x":[1,true,"text"]}}'"#),
            format!("json_extract({k}) IS NULL"),
            "json_extract(`metadata`, '$.*') IS NOT NULL".to_string(),
        ];
        for filter in &filters {
            let want = keys(&dataset, filter, false).await.unwrap();
            let got = keys(&dataset, filter, true).await.unwrap();
            assert_eq!(got, want, "{filter}");
        }
        // A bad path fails the query either way.
        let bad = "json_extract(`metadata`, '$[') = 'x'";
        assert!(keys(&dataset, bad, false).await.is_err());
        assert!(keys(&dataset, bad, true).await.is_err());
    }

    #[tokio::test]
    async fn array_contains_compares_typed_values() {
        let dir = TempDir::new().unwrap();
        let metas = [
            Some(r#"{"k": ["space", "classic"]}"#),
            Some(r#"{"k": [1, 2.5]}"#),
            Some(r#"{"k": ["1", "2.5"]}"#),
            Some(r#"{"k": [true]}"#),
            Some(r#"{"k": ["true"]}"#),
            Some(r#"{"k": "space"}"#),
            Some(r#"{"k": [["space"]]}"#),
            Some(r#"{"k": {"a": "space"}}"#),
            Some(r#"{"k": ["", "spa\"ce"]}"#),
            Some(r#"{"k": []}"#),
            Some(r#"{"x": 1}"#),
            None,
        ];
        let dataset = dataset(&dir, &metas).await;
        for (value, want) in [
            (r#""space""#, vec!["k0"]),
            ("1", vec!["k1"]),
            ("1.0", vec!["k1"]),
            ("2.5", vec!["k1"]),
            (r#""1""#, vec!["k2"]),
            ("true", vec!["k3"]),
            (r#""true""#, vec!["k4"]),
            (r#""""#, vec!["k8"]),
            (r#""spa\"ce""#, vec!["k8"]),
            (r#""classic""#, vec!["k0"]),
            ("3", vec![]),
        ] {
            let filter = format!(r#"json_array_contains(`metadata`, '$["k"]', '{value}')"#);
            assert_eq!(
                keys(&dataset, &filter, true).await.unwrap(),
                want,
                "{filter}"
            );
        }
        let bad = r#"json_array_contains(`metadata`, '$["k"]', 'not json')"#;
        assert!(keys(&dataset, bad, true).await.is_err());
    }

    #[tokio::test]
    async fn apply_swaps_literal_paths_only() {
        let dir = TempDir::new().unwrap();
        let dataset = dataset(&dir, &[Some(r#"{"k": "v"}"#)]).await;
        let mut scanner = dataset.scan();
        apply(
            &mut scanner,
            r#"json_extract(`metadata`, '$["k"]') = '"v"' AND json_exists(`metadata`, '$["k"]') AND json_get_string(`metadata`, 'k') = 'v' AND json_array_contains(`metadata`, '$["k"]', '"v"') AND NOT json_array_contains(`metadata`, '$["k"]', key)"#,
        )
        .unwrap();
        let mut swapped = Vec::new();
        scanner
            .get_expr_filter()
            .unwrap()
            .unwrap()
            .apply(|e| {
                if let Expr::ScalarFunction(f) = e {
                    let inner: &dyn std::any::Any = f.func.inner().as_ref();
                    swapped.push((f.func.name().to_string(), inner.is::<JsonPath>()));
                }
                Ok(lance::deps::datafusion::common::tree_node::TreeNodeRecursion::Continue)
            })
            .unwrap();
        swapped.sort();
        assert_eq!(
            swapped,
            [
                ("json_array_contains".to_string(), false),
                ("json_array_contains".to_string(), true),
                ("json_exists".to_string(), true),
                ("json_extract".to_string(), true),
                ("json_get_string".to_string(), false),
            ]
        );
    }
}
