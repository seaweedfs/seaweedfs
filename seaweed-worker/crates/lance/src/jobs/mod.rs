//! One module per job type. Each declares its capability and the settings form
//! admin renders for it, then does the work.
//!
//! Detection opens each table and reads its manifest rather than its data, so a
//! sweep across a catalog stays cheap. Execution re-resolves the table instead
//! of trusting what detection saw: it may have been repointed, and the vended
//! credentials expire.

pub mod cleanup;
pub mod compact;
pub mod indices;

use std::collections::HashMap;
use std::sync::Arc;

use seaweed_worker_core::pb::{config_value::Kind, ConfigValue, ObjectObservation, StringList};
use seaweed_worker_core::JobHandler;

use crate::catalog::parse_id;

/// A table identifier travels in a proposal's parameters and comes back on the
/// job, so both sides agree on one encoding.
pub(crate) fn string_list(parts: &[String]) -> ConfigValue {
    ConfigValue {
        kind: Some(Kind::StringList(StringList {
            values: parts.to_vec(),
        })),
    }
}

pub(crate) fn table_id(parameters: &HashMap<String, ConfigValue>) -> Option<Vec<String>> {
    match parameters.get("table_id")?.kind.as_ref()? {
        Kind::StringList(list) => Some(list.values.clone()),
        Kind::StringValue(encoded) => Some(parse_id(encoded)),
        _ => None,
    }
}

/// Every handler this worker serves. A worker process may serve several job
/// types, which is why WorkerHello carries a list.
pub fn handlers(
    namespace_url: String,
    fallback: crate::dataset::FallbackOptions,
) -> Vec<Arc<dyn JobHandler>> {
    vec![
        Arc::new(
            compact::CompactHandler::new(namespace_url.clone()).with_fallback(fallback.clone()),
        ),
        Arc::new(
            indices::OptimizeIndicesHandler::new(namespace_url.clone())
                .with_fallback(fallback.clone()),
        ),
        Arc::new(cleanup::CleanupVersionsHandler::new(namespace_url).with_fallback(fallback)),
    ]
}

/// The format the catalog records for the tables this worker maintains.
pub(crate) const FORMAT: &str = "LANCE";

/// Builds the observation a detection sweep reports for one table. Detection
/// has already opened the dataset to decide whether it needs work, so saying
/// what it saw costs nothing, and for a format the cluster cannot read this is
/// the only description of the table anything can produce.
pub(crate) fn observation(
    id: &[String],
    format: &str,
    attributes: HashMap<String, ConfigValue>,
) -> ObjectObservation {
    ObjectObservation {
        object_id: id.to_vec(),
        object_kind: "table".to_string(),
        format: format.to_string(),
        attributes,
        observed_at: Some(std::time::SystemTime::now().into()),
    }
}
