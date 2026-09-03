//! The sort specification the sorting jobs share.
//!
//! A worker sorts a table by the fields the table itself declares, and by the
//! fields its own configuration names for a table that declares none. Both
//! halves of that rule, and the spelling of a field list, live here rather than
//! in each job: a Lance job and an Iceberg job that disagreed about what
//! "id desc nulls-first" means would be two features wearing one name.
//!
//! The spelling is deliberately the one `weed/worker/tasks/iceberg` already
//! writes into an Iceberg snapshot's `sort-fields` summary, so the same order
//! reads the same way whichever format holds the table and whichever language
//! sorted it.

use std::collections::{HashMap, HashSet};
use std::fmt;

use anyhow::{bail, Context, Result};
use seaweed_worker_core::config_form::{number_field, text_field};
use seaweed_worker_core::pb::ConfigField;

/// Worker configuration keys. Shared so the Iceberg and Lance forms offer an
/// operator the same names for the same settings.
pub const CONFIG_SORT_FIELDS: &str = "sort_fields";
pub const CONFIG_MIN_UNSORTED_ROWS: &str = "min_unsorted_rows";
pub const CONFIG_MEMORY_BUDGET_MB: &str = "memory_budget_mb";
pub const CONFIG_MAX_ROWS_PER_FILE: &str = "max_rows_per_file";

/// The order an operator declares on a table. The worker only ever reads this
/// one.
pub const DECLARED_FIELDS_KEY: &str = "seaweedfs.sort.fields";

/// What the worker recorded about the sort it last performed. Kept apart from
/// the declaration above so that "the operator asked for this order" and "the
/// worker achieved this order" stay distinguishable — comparing them is how a
/// changed order is noticed.
pub const SORTED_FIELDS_KEY: &str = "seaweedfs.sort.sorted_fields";
pub const SORTED_SOURCE_VERSION_KEY: &str = "seaweedfs.sort.source_version";
pub const SORTED_ROWS_KEY: &str = "seaweedfs.sort.sorted_rows";

/// One field of a sort order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SortField {
    pub path: String,
    pub descending: bool,
    pub nulls_first: bool,
}

/// A whole sort order, in the order the fields are compared.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SortSpec {
    pub fields: Vec<SortField>,
}

impl SortSpec {
    /// Reads "user_id asc nulls-last, ts desc" into a spec. An empty string is
    /// not an error: it is how an operator says nothing, and the caller decides
    /// what that means.
    ///
    /// Direction defaults to ascending and null order to Iceberg's default for
    /// the direction — nulls first ascending, nulls last descending — because
    /// this spelling is shared with tables that already have that rule.
    pub fn parse(text: &str) -> Result<Option<Self>> {
        let mut fields = Vec::new();
        let mut seen = HashSet::new();
        for entry in text.split(',') {
            let entry = entry.trim();
            if entry.is_empty() {
                continue;
            }
            let field =
                parse_field(entry).with_context(|| format!("read the sort field {entry:?}"))?;
            if !seen.insert(field.path.to_ascii_lowercase()) {
                bail!("the sort field {:?} is named twice", field.path);
            }
            fields.push(field);
        }
        if fields.is_empty() {
            return Ok(None);
        }
        Ok(Some(Self { fields }))
    }
}

/// Renders a spec back, always spelling out direction and null order so that a
/// recorded order round-trips to the same thing it was parsed from.
impl fmt::Display for SortSpec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, field) in self.fields.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(
                f,
                "{} {} {}",
                field.path,
                if field.descending { "desc" } else { "asc" },
                if field.nulls_first {
                    "nulls-first"
                } else {
                    "nulls-last"
                }
            )?;
        }
        Ok(())
    }
}

fn parse_field(entry: &str) -> Result<SortField> {
    let mut tokens = entry.split_whitespace();
    let path = tokens
        .next()
        .expect("split_whitespace yields one token for a non-empty entry")
        .to_string();

    let mut descending = None;
    let mut nulls_first = None;
    for token in tokens {
        // Underscores are accepted because that is how the same words are
        // spelled in configuration keys, and refusing them would only teach an
        // operator that the two spellings are different things.
        match token.to_ascii_lowercase().replace('_', "-").as_str() {
            "asc" | "ascending" if descending.is_none() => descending = Some(false),
            "desc" | "descending" if descending.is_none() => descending = Some(true),
            "nulls-first" if nulls_first.is_none() => nulls_first = Some(true),
            "nulls-last" if nulls_first.is_none() => nulls_first = Some(false),
            other => bail!("{other:?} is not a direction or a null order"),
        }
    }

    let descending = descending.unwrap_or(false);
    Ok(SortField {
        path,
        descending,
        nulls_first: nulls_first.unwrap_or(!descending),
    })
}

/// The order to sort a table by: what the table declares wins, and the worker's
/// configuration is the fallback for a table that declares nothing. Neither one
/// is not an error — it means this is not a table the operator wants sorted.
///
/// A declaration that cannot be read is an error rather than a reason to fall
/// back: sorting by the worker's default order instead of the one the table
/// asked for would silently rewrite the table the wrong way.
pub fn resolve(declared: Option<&str>, configured: &str) -> Result<Option<SortSpec>> {
    if let Some(declared) = declared {
        if let Some(spec) = SortSpec::parse(declared).context("read the table's declared order")? {
            return Ok(Some(spec));
        }
    }
    SortSpec::parse(configured).context("read the configured sort order")
}

/// What the worker recorded on a table the last time it sorted it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SortState {
    pub fields: Option<String>,
    /// The version the sort read. Not the version it wrote: the commit that
    /// carries this marker is the one that creates that version, so its number
    /// is not knowable while the marker is being assembled.
    pub source_version: Option<u64>,
    pub rows: Option<u64>,
}

impl SortState {
    /// Reads the marker out of a table's key/value configuration. A key that
    /// will not parse is treated as absent, which asks for a sort rather than
    /// skipping the table: the cost of a needless sort is time, and the cost of
    /// skipping is a table that silently never gets sorted again.
    pub fn from_config(config: &HashMap<String, String>) -> Self {
        Self {
            fields: config.get(SORTED_FIELDS_KEY).cloned(),
            source_version: config
                .get(SORTED_SOURCE_VERSION_KEY)
                .and_then(|v| v.parse().ok()),
            rows: config.get(SORTED_ROWS_KEY).and_then(|v| v.parse().ok()),
        }
    }

    /// The marker to write for a sort that just finished.
    pub fn record(spec: &SortSpec, source_version: u64, rows: u64) -> HashMap<String, String> {
        HashMap::from([
            (SORTED_FIELDS_KEY.to_string(), spec.to_string()),
            (
                SORTED_SOURCE_VERSION_KEY.to_string(),
                source_version.to_string(),
            ),
            (SORTED_ROWS_KEY.to_string(), rows.to_string()),
        ])
    }
}

/// Why a table does or does not need sorting.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SortVerdict {
    NeverSorted,
    OrderChanged,
    RowsAppended(u64),
    /// The table was committed to since the sort, and the row count did not
    /// grow — the data is not the data that was sorted.
    Rewritten,
    UpToDate,
}

impl SortVerdict {
    pub fn needs_sort(&self) -> bool {
        !matches!(self, Self::UpToDate)
    }

    pub fn reason(&self) -> String {
        match self {
            Self::NeverSorted => "never sorted".to_string(),
            Self::OrderChanged => "the declared order changed since the last sort".to_string(),
            Self::RowsAppended(rows) => format!("{rows} rows appended since the last sort"),
            Self::Rewritten => "the data was replaced since the last sort".to_string(),
            Self::UpToDate => "sorted".to_string(),
        }
    }
}

/// Decides whether a table is worth sorting.
///
/// The version is what makes this safe. Rows appended since the marker stand in
/// for how unsorted a table is, and that alone cannot see a rewrite that leaves
/// the row count where it was: the table would look sorted forever. Anything
/// committed after the sort's own commit is therefore treated as data the sort
/// did not produce, and only a *growing* row count gets the damping threshold —
/// which is what the threshold is for, since appends are the churn worth
/// ignoring.
///
/// The cost of that conservatism is a re-sort after rows are deleted, which
/// does not itself unsort anything. Row count and version cannot tell a delete
/// from a replacement, and of the two mistakes, sorting a table that did not
/// need it beats never sorting one that did. A rewrite reclaims the tombstones
/// either way.
///
/// The honest measure of sortedness is how far the fragments' key ranges
/// overlap, which costs a metadata read per fragment. This costs nothing.
pub fn verdict(
    spec: &SortSpec,
    state: &SortState,
    rows: u64,
    version: u64,
    min_unsorted_rows: u64,
) -> SortVerdict {
    let Some(source_version) = state.source_version else {
        return SortVerdict::NeverSorted;
    };
    if state.fields.as_deref() != Some(spec.to_string().as_str()) {
        return SortVerdict::OrderChanged;
    }
    // The sort's own commit is the one after the version it read, so a table at
    // that version has had nothing else written to it.
    if version <= source_version + 1 {
        return SortVerdict::UpToDate;
    }

    let sorted_rows = state.rows.unwrap_or(0);
    if rows > sorted_rows {
        let appended = rows - sorted_rows;
        if appended >= min_unsorted_rows.max(1) {
            return SortVerdict::RowsAppended(appended);
        }
        return SortVerdict::UpToDate;
    }
    SortVerdict::Rewritten
}

/// The settings every sorting job offers, so admin renders one form for the
/// same behaviour whichever format the job sorts.
pub fn config_fields() -> Vec<ConfigField> {
    vec![
        text_field(
            CONFIG_SORT_FIELDS,
            "Sort fields",
            "Order for tables that declare none of their own, as \"user_id asc, ts desc nulls-last\". Empty sorts only the tables that declare an order.",
            "user_id asc, ts desc",
        ),
        number_field(
            CONFIG_MIN_UNSORTED_ROWS,
            "Minimum unsorted rows",
            "Rows appended since the last sort before a table is worth sorting again",
            1,
            1_000_000_000,
        ),
        number_field(
            CONFIG_MEMORY_BUDGET_MB,
            "Memory budget (MB)",
            "Memory the sort may hold before it spills runs to disk. A table larger than this sorts more slowly rather than failing.",
            64,
            1_048_576,
        ),
        number_field(
            CONFIG_MAX_ROWS_PER_FILE,
            "Maximum rows per file",
            "Rows to write into each output file of a sorted rewrite",
            1024,
            16_777_216,
        ),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_direction_and_null_order() {
        let spec = SortSpec::parse("user_id, ts desc, name asc nulls-last")
            .unwrap()
            .unwrap();
        assert_eq!(
            spec.fields,
            vec![
                // Ascending defaults to nulls first, descending to nulls last,
                // which is the rule the Iceberg tables sharing this spelling use.
                SortField {
                    path: "user_id".into(),
                    descending: false,
                    nulls_first: true
                },
                SortField {
                    path: "ts".into(),
                    descending: true,
                    nulls_first: false
                },
                SortField {
                    path: "name".into(),
                    descending: false,
                    nulls_first: false
                },
            ]
        );
    }

    #[test]
    fn renders_back_to_something_that_parses_the_same() {
        let spec = SortSpec::parse("a, b desc nulls_first").unwrap().unwrap();
        let rendered = spec.to_string();
        assert_eq!(rendered, "a asc nulls-first, b desc nulls-first");
        assert_eq!(SortSpec::parse(&rendered).unwrap().unwrap(), spec);
    }

    #[test]
    fn empty_is_no_spec_rather_than_an_error() {
        assert!(SortSpec::parse("").unwrap().is_none());
        assert!(SortSpec::parse("  ,  ").unwrap().is_none());
    }

    #[test]
    fn rejects_nonsense_and_repeats() {
        assert!(SortSpec::parse("a sideways").is_err());
        assert!(SortSpec::parse("a asc desc").is_err());
        assert!(SortSpec::parse("a, A desc").is_err());
    }

    #[test]
    fn the_table_wins_and_configuration_fills_in() {
        let configured = "b desc";
        let from_table = resolve(Some("a asc"), configured).unwrap().unwrap();
        assert_eq!(from_table.fields[0].path, "a");

        let from_config = resolve(None, configured).unwrap().unwrap();
        assert_eq!(from_config.fields[0].path, "b");

        let from_config_again = resolve(Some("  "), configured).unwrap().unwrap();
        assert_eq!(from_config_again.fields[0].path, "b");

        assert!(resolve(None, "").unwrap().is_none());
    }

    // Falling back would sort the table by an order nobody asked for.
    #[test]
    fn a_broken_declaration_is_an_error_not_a_fallback() {
        assert!(resolve(Some("a sideways"), "b desc").is_err());
    }

    #[test]
    fn verdicts_follow_the_marker() {
        let spec = SortSpec::parse("a asc").unwrap().unwrap();
        let fresh = SortState::default();
        assert_eq!(verdict(&spec, &fresh, 10, 1, 100), SortVerdict::NeverSorted);

        // A sort that read version 4 committed version 5.
        let sorted = SortState {
            fields: Some(spec.to_string()),
            source_version: Some(4),
            rows: Some(1_000),
        };
        assert_eq!(
            verdict(&spec, &sorted, 1_000, 5, 100),
            SortVerdict::UpToDate
        );
        // Appends below the threshold are the churn the threshold exists for.
        assert_eq!(
            verdict(&spec, &sorted, 1_050, 6, 100),
            SortVerdict::UpToDate
        );
        assert_eq!(
            verdict(&spec, &sorted, 1_100, 6, 100),
            SortVerdict::RowsAppended(100)
        );

        // The case the row count alone cannot see: the data replaced by a write
        // of its own, leaving the count where it was.
        assert_eq!(
            verdict(&spec, &sorted, 1_000, 9, 100),
            SortVerdict::Rewritten
        );
        // And rows removed, which is the same shape from here.
        assert_eq!(verdict(&spec, &sorted, 900, 6, 100), SortVerdict::Rewritten);

        let other = SortSpec::parse("b desc").unwrap().unwrap();
        assert_eq!(
            verdict(&other, &sorted, 1_000, 5, 100),
            SortVerdict::OrderChanged
        );
    }

    #[test]
    fn the_marker_round_trips_through_a_config_map() {
        let spec = SortSpec::parse("a asc").unwrap().unwrap();
        let recorded = SortState::record(&spec, 7, 42);
        let state = SortState::from_config(&recorded);
        assert_eq!(state.source_version, Some(7));
        assert_eq!(state.rows, Some(42));
        assert_eq!(verdict(&spec, &state, 42, 8, 1), SortVerdict::UpToDate);
    }
}
