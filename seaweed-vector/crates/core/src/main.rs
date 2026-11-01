use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};

use weed_lance_worker::catalog::{NamespaceClient, parse_id};
use weed_lance_worker::dataset::{self, FallbackOptions};
use weed_vector_core::coverage;
use weed_vector_core::query::{self, Query};
use weed_vector_core::table::VectorTable;

/// Mirrors `weed-worker`'s flags where the two touch the same thing: the
/// namespace is WEED_LANCE_NAMESPACE and the fallback storage credentials are
/// the same env vars.
#[derive(Parser, Debug)]
#[command(name = "weed-vector", about = "SeaweedFS vector query spike")]
struct Args {
    /// Lance namespace the tables live in.
    #[arg(
        long,
        default_value = "http://localhost:9101",
        env = "WEED_LANCE_NAMESPACE"
    )]
    namespace: String,

    /// Storage credentials to use where the namespace vends none.
    #[arg(long, env = "WEED_S3_ACCESS_KEY")]
    access_key: Option<String>,

    #[arg(long, env = "WEED_S3_SECRET_KEY")]
    secret_key: Option<String>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// A table's location, version, vector layout and index coverage.
    Describe {
        /// Delimiter-joined table id, e.g. bucket$table.
        id: String,
        #[arg(long)]
        vector_column: Option<String>,
        #[arg(long)]
        key_column: Option<String>,
    },
    /// Which fragments each vector index covers.
    Coverage {
        id: String,
        #[arg(long)]
        vector_column: Option<String>,
        #[arg(long)]
        key_column: Option<String>,
    },
    /// A top-K query over the whole dataset.
    Query {
        id: String,
        /// Comma-separated floats, e.g. --vector 0.1,0.2,0.3,0.4
        #[arg(long, allow_hyphen_values = true)]
        vector: String,
        #[arg(long, default_value = "10")]
        k: usize,
        /// Lance SQL filter over the metadata columns.
        #[arg(long)]
        filter: Option<String>,
        /// l2, cosine or dot.
        #[arg(long, default_value = "l2")]
        metric: String,
        /// Exact scan, no index.
        #[arg(long)]
        flat: bool,
        /// Filter the top-K rows after ranking (Lance's default); off keeps
        /// the filter before the ANN search.
        #[arg(long)]
        postfilter: bool,
        /// Pin the query to this committed version.
        #[arg(long)]
        version: Option<u64>,
        #[arg(long)]
        vector_column: Option<String>,
        #[arg(long)]
        key_column: Option<String>,
        #[arg(long)]
        nprobes: Option<usize>,
    },
    /// Fetch rows by key.
    Get {
        id: String,
        keys: Vec<String>,
        #[arg(long)]
        vector_column: Option<String>,
        #[arg(long)]
        key_column: Option<String>,
    },
}

/// `id` is a delimiter-joined table id; when it looks like a URI (`s3://...`,
/// `/path/...`, `file://...`) the dataset is opened directly, skipping the
/// namespace — useful for a spike run against a local dataset.
async fn open_dataset(
    client: &NamespaceClient,
    id: &str,
    fallback: &FallbackOptions,
) -> Result<lance::Dataset> {
    if id.contains("://") || id.starts_with('/') {
        let mut builder = lance::dataset::builder::DatasetBuilder::from_uri(id);
        if !fallback.is_empty() {
            builder = builder.with_storage_options(fallback.clone());
        }
        return builder.load().await.with_context(|| format!("open {id}"));
    }
    let table = dataset::open(client, &parse_id(id), fallback).await?;
    Ok(table.dataset)
}

#[tokio::main]
async fn main() -> Result<()> {
    weed_lance_worker::tls::install_default_crypto_provider();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();
    let args = Args::parse();

    let mut fallback = FallbackOptions::new();
    if let (Some(access), Some(secret)) = (args.access_key, args.secret_key) {
        fallback.insert("aws_access_key_id".to_string(), access);
        fallback.insert("aws_secret_access_key".to_string(), secret);
    }
    let client = NamespaceClient::new(args.namespace).with_fallback(&fallback);

    match args.command {
        Command::Describe {
            id,
            vector_column,
            key_column,
        } => {
            let dataset = open_dataset(&client, &id, &fallback).await?;
            let schema: arrow_schema::Schema = dataset.schema().into();
            let resolved =
                VectorTable::resolve(&dataset, vector_column.as_deref(), key_column.as_deref());
            let vector = match resolved {
                Err(e) if vector_column.is_some() || key_column.is_some() => return Err(e),
                r => r.ok(),
            };
            let coverages = match &vector {
                Some(v) => coverage::coverage(&dataset, &v.vector_column).await?,
                None => Vec::new(),
            };
            println!(
                "{}",
                serde_json::json!({
                    "id": id,
                    "uri": dataset.uri(),
                    "version": dataset.version().version,
                    "fragments": dataset.get_fragments().len(),
                    "rows": dataset.count_rows(None).await?,
                    "schema": schema.fields().iter().map(|f| serde_json::json!({
                        "name": f.name(),
                        "type": f.data_type().to_string(),
                        "nullable": f.is_nullable(),
                    })).collect::<Vec<_>>(),
                    "vector": vector.map(|v| serde_json::json!({
                        "vector_column": v.vector_column,
                        "dimension": v.dimension,
                        "key_column": v.key_column,
                        "metadata_columns": v.metadata_columns,
                    })),
                    "coverage": coverages,
                })
            );
        }
        Command::Coverage {
            id,
            vector_column,
            key_column,
        } => {
            let dataset = open_dataset(&client, &id, &fallback).await?;
            let vector =
                VectorTable::resolve(&dataset, vector_column.as_deref(), key_column.as_deref())?;
            let coverages = coverage::coverage(&dataset, &vector.vector_column).await?;
            println!("{}", serde_json::to_string_pretty(&coverages)?);
        }
        Command::Get {
            id,
            keys,
            vector_column,
            key_column,
        } => {
            let dataset = open_dataset(&client, &id, &fallback).await?;
            let vector =
                VectorTable::resolve(&dataset, vector_column.as_deref(), key_column.as_deref())?;
            for batch in query::get(&dataset, &vector, &keys, None).await? {
                println!("{}", arrow_cast::pretty::pretty_format_batches(&[batch])?);
            }
        }
        Command::Query {
            id,
            vector,
            k,
            filter,
            metric,
            flat,
            postfilter,
            version,
            vector_column,
            key_column,
            nprobes,
        } => {
            let opened = open_dataset(&client, &id, &fallback).await?;
            let dataset = match version {
                Some(v) => opened.checkout_version(v).await?,
                None => opened,
            };
            let vector_table =
                VectorTable::resolve(&dataset, vector_column.as_deref(), key_column.as_deref())?;
            let vector: Vec<f32> = vector
                .split(',')
                .map(|part| part.trim().parse::<f32>())
                .collect::<std::result::Result<_, _>>()
                .context("--vector takes comma-separated floats")?;
            let distance = match metric.as_str() {
                "l2" | "euclidean" => lance_linalg::distance::DistanceType::L2,
                "cosine" => lance_linalg::distance::DistanceType::Cosine,
                "dot" | "dotproduct" => lance_linalg::distance::DistanceType::Dot,
                other => bail!("unknown metric {other}: l2, cosine or dot"),
            };
            let query = Query {
                vector,
                k,
                filter,
                postfilter,
                distance,
                nprobes,
                index_segments: None,
                fragments: None,
            };
            let hits = if flat {
                query::flat(&dataset, &vector_table, &query).await?
            } else {
                query::ann(&dataset, &vector_table, &query).await?
            };
            println!("{}", serde_json::to_string_pretty(&hits)?);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vector_flag_takes_hyphenated_floats() {
        let args = Args::try_parse_from([
            "weed-vector",
            "query",
            "t",
            "--vector",
            "-1,-1,-1,-1",
            "--k",
            "3",
        ])
        .unwrap();
        match args.command {
            Command::Query { vector, k, .. } => {
                assert_eq!(vector, "-1,-1,-1,-1");
                assert_eq!(k, 3);
            }
            _ => panic!("expected query"),
        }
    }

    #[test]
    fn column_flags_on_get_describe_coverage() {
        for argv in [
            vec!["weed-vector", "get", "t", "k1"],
            vec!["weed-vector", "describe", "t"],
            vec!["weed-vector", "coverage", "t"],
        ] {
            let mut argv = argv;
            argv.extend(["--vector-column", "v", "--key-column", "kk"]);
            let args = Args::try_parse_from(argv).unwrap();
            let (v, k) = match args.command {
                Command::Get {
                    vector_column,
                    key_column,
                    ..
                }
                | Command::Describe {
                    vector_column,
                    key_column,
                    ..
                }
                | Command::Coverage {
                    vector_column,
                    key_column,
                    ..
                } => (vector_column, key_column),
                _ => panic!("unexpected subcommand"),
            };
            assert_eq!(v.as_deref(), Some("v"));
            assert_eq!(k.as_deref(), Some("kk"));
        }
    }
}
