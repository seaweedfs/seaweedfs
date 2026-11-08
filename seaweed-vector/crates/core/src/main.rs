use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};

use weed_lance_worker::catalog::{NamespaceClient, SigningKey, TableDescription, parse_id};
use weed_lance_worker::dataset::{self, Storage};
use weed_vector_core::coverage;
use weed_vector_core::index;
use weed_vector_core::query::{self, Query};
use weed_vector_core::table::VectorTable;

/// Mirrors `weed-worker`'s flags where the two touch the same thing: the
/// namespace that says where a table is, and the masters whose filers hold
/// it.
#[derive(Parser, Debug)]
#[command(
    name = "weed-vector",
    about = "Describe, query and index S3 Vectors tables"
)]
struct Args {
    /// Lance namespace the tables live in.
    #[arg(
        long,
        default_value = "http://localhost:9101",
        env = "WEED_LANCE_NAMESPACE"
    )]
    namespace: String,

    /// Comma-separated master addresses, whose filers the tables are kept
    /// through. A table named by a local URI needs none.
    #[arg(long, alias = "master", env = "WEED_MASTERS")]
    masters: Option<String>,

    /// The filer group whose filers hold the tables.
    #[arg(
        long,
        alias = "filer.group",
        env = "WEED_FILER_GROUP",
        default_value = ""
    )]
    filer_group: String,

    /// The `jwt.filer_signing.key` of the cluster, which the namespace
    /// authenticates cluster processes with; security.toml's by default.
    #[arg(long, env = "WEED_FILER_SIGNING_KEY")]
    filer_signing_key: Option<String>,

    /// The cluster's security.toml; by default the file weed processes find
    /// in -config_dir, ., ~/.seaweedfs and /etc/seaweedfs.
    #[arg(long, env = "WEED_VECTOR_SECURITY_TOML")]
    security_toml: Option<std::path::PathBuf>,

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
    /// Build or extend the vector index, then print its coverage.
    Index {
        #[command(subcommand)]
        action: IndexAction,
    },
}

#[derive(Subcommand, Debug)]
enum IndexAction {
    /// An IVF_FLAT index in the table's metric; partitions default to the
    /// square root of the row count.
    Build {
        id: String,
        #[arg(long)]
        partitions: Option<usize>,
        #[arg(long, default_value = index::INDEX_NAME)]
        name: String,
        /// Rebuild an index of that name instead of failing.
        #[arg(long)]
        replace: bool,
        #[arg(long)]
        vector_column: Option<String>,
    },
    /// Cover the rows written since the index was built.
    Optimize {
        id: String,
        /// Train the index again on every row instead of adding a segment.
        #[arg(long)]
        retrain: bool,
        #[arg(long)]
        vector_column: Option<String>,
    },
}

/// `id` is a delimiter-joined table id; when it looks like a URI the dataset
/// is opened directly, skipping the namespace: an `s3://` URI on the
/// cluster's storage, a local path (`/path/...`, `file://...`) as it is.
async fn open_dataset(
    client: &NamespaceClient,
    id: &str,
    storage: &Storage,
) -> Result<lance::Dataset> {
    let on_cluster = id.starts_with("s3://");
    if !on_cluster && (id.contains("://") || id.starts_with('/')) {
        return lance::dataset::builder::DatasetBuilder::from_uri(id)
            .load()
            .await
            .with_context(|| format!("open {id}"));
    }
    if storage.0.is_none() {
        bail!("{id} is kept on the cluster and needs --masters");
    }
    if on_cluster {
        let description = TableDescription {
            location: id.to_string(),
            storage_options: Default::default(),
            managed_versioning: false,
        };
        return dataset::open_at(&description, storage.wrapper(id).await?).await;
    }
    let table = dataset::open(client, &parse_id(id), storage).await?;
    Ok(table.dataset)
}

#[tokio::main]
async fn main() -> Result<()> {
    weed_lance_worker::tls::install_default_crypto_provider();
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();
    let args = Args::parse();

    let security =
        weed_lance_worker::security::Security::load(args.security_toml.as_deref(), None)?;
    let signing_key = SigningKey::new(
        args.filer_signing_key
            .or_else(|| security.get("jwt.filer_signing.key")),
    );
    let client = NamespaceClient::new(args.namespace).with_signing_key(signing_key.as_ref());
    let storage = match &args.masters {
        Some(masters) => {
            let cluster =
                weed_lance_worker::cluster::Cluster::new(masters, &args.filer_group, &security)?;
            Storage(Some(cluster.storage(&security).await?))
        }
        None => Storage::default(),
    };

    match args.command {
        Command::Describe {
            id,
            vector_column,
            key_column,
        } => {
            let dataset = open_dataset(&client, &id, &storage).await?;
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
            let dataset = open_dataset(&client, &id, &storage).await?;
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
            let dataset = open_dataset(&client, &id, &storage).await?;
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
            let opened = open_dataset(&client, &id, &storage).await?;
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
        Command::Index { action } => {
            let (id, vector_column) = match &action {
                IndexAction::Build {
                    id, vector_column, ..
                }
                | IndexAction::Optimize {
                    id, vector_column, ..
                } => (id.clone(), vector_column.clone()),
            };
            let mut dataset = open_dataset(&client, &id, &storage).await?;
            let table = VectorTable::resolve(&dataset, vector_column.as_deref(), None)?;
            let started = std::time::Instant::now();
            match action {
                IndexAction::Build {
                    partitions,
                    name,
                    replace,
                    ..
                } => {
                    let rows = dataset.count_rows(None).await?;
                    let partitions = partitions.unwrap_or_else(|| index::partitions_for(rows));
                    index::build(
                        &mut dataset,
                        &table.vector_column,
                        &name,
                        partitions,
                        replace,
                    )
                    .await?;
                }
                IndexAction::Optimize { retrain, .. } => {
                    index::optimize(&mut dataset, &table.vector_column, retrain).await?;
                }
            }
            println!(
                "{}",
                serde_json::json!({
                    "version": dataset.version().version,
                    "seconds": started.elapsed().as_secs_f64(),
                    "coverage": coverage::coverage(&dataset, &table.vector_column).await?,
                })
            );
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
    fn cluster_flags_mirror_the_engine_and_the_worker() {
        let args = Args::try_parse_from([
            "weed-vector",
            "--masters",
            "m:9333",
            "--filer-group",
            "eu",
            "describe",
            "t",
        ])
        .unwrap();
        assert_eq!(args.masters.as_deref(), Some("m:9333"));
        assert_eq!(args.filer_group, "eu");
        assert!(
            Args::try_parse_from(["weed-vector", "--encryptVolumeData", "describe", "t"]).is_err(),
            "volume encryption follows the filer"
        );
    }

    #[test]
    fn index_build_takes_partitions_and_a_name() {
        let args = Args::try_parse_from([
            "weed-vector",
            "index",
            "build",
            "b$vector$i",
            "--partitions",
            "32",
            "--replace",
        ])
        .unwrap();
        match args.command {
            Command::Index {
                action:
                    IndexAction::Build {
                        id,
                        partitions,
                        name,
                        replace,
                        ..
                    },
            } => {
                assert_eq!(id, "b$vector$i");
                assert_eq!(partitions, Some(32));
                assert_eq!(name, index::INDEX_NAME);
                assert!(replace);
            }
            _ => panic!("expected index build"),
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
