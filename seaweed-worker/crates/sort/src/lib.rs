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
pub const SORTED_ROWS_KEY: &str = "seaweedfs.sort.sorted_rows";
/// How many data files the sort wrote, and a digest of their names. Together
/// they identify the data the sort produced — see [`verdict`] for why a version
/// number cannot.
pub const SORTED_FILES_KEY: &str = "seaweedfs.sort.sorted_files";
pub const SORTED_DIGEST_KEY: &str = "seaweedfs.sort.sorted_digest";

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
            // Compared exactly, not case-folded: Arrow schemas are
            // case-sensitive, so `id` and `ID` can be two real columns and
            // folding them together would reject a valid order. The Iceberg
            // job, whose format resolves names case-insensitively, does its own
            // check against the table schema.
            if !seen.insert(field.path.clone()) {
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

/// A digest of data file names, in the order the fragments hold them.
///
/// FNV-1a rather than the standard library's hasher, whose output is explicitly
/// not stable across releases: a marker that hashed differently after a
/// toolchain upgrade would re-sort every table once, silently.
pub fn digest<S: AsRef<str>>(files: &[S]) -> String {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for file in files {
        for byte in file.as_ref().as_bytes().iter().chain(std::iter::once(&0u8)) {
            hash ^= *byte as u64;
            hash = hash.wrapping_mul(0x1000_0000_01b3);
        }
    }
    format!("{hash:016x}")
}

/// What the worker recorded on a table the last time it sorted it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SortState {
    pub fields: Option<String>,
    pub rows: Option<u64>,
    /// The data files the sort wrote: how many, and a digest of their names.
    /// Data file names are chosen before the commit, which is what makes them
    /// usable in a marker the same commit carries.
    pub files: Option<usize>,
    pub digest: Option<String>,
}

impl SortState {
    /// Reads the marker out of a table's key/value configuration. A key that
    /// will not parse is treated as absent, which asks for a sort rather than
    /// skipping the table: the cost of a needless sort is time, and the cost of
    /// skipping is a table that silently never gets sorted again.
    pub fn from_config(config: &HashMap<String, String>) -> Self {
        Self {
            fields: config.get(SORTED_FIELDS_KEY).cloned(),
            rows: config.get(SORTED_ROWS_KEY).and_then(|v| v.parse().ok()),
            files: config.get(SORTED_FILES_KEY).and_then(|v| v.parse().ok()),
            digest: config.get(SORTED_DIGEST_KEY).cloned(),
        }
    }

    /// The marker to write for a sort that just finished.
    pub fn record<I, S>(spec: &SortSpec, files: I, rows: u64) -> HashMap<String, String>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let files: Vec<String> = files.into_iter().map(|f| f.as_ref().to_string()).collect();
        HashMap::from([
            (SORTED_FIELDS_KEY.to_string(), spec.to_string()),
            (SORTED_ROWS_KEY.to_string(), rows.to_string()),
            (SORTED_FILES_KEY.to_string(), files.len().to_string()),
            (SORTED_DIGEST_KEY.to_string(), digest(&files)),
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
/// The question detection actually has to answer is "is this still the data the
/// sort wrote?", and neither the row count nor the dataset version can answer
/// it. A rewrite that leaves the row count where it was is invisible to the
/// first, and the second cannot be recorded at all: the commit carrying the
/// marker is the commit that creates the version, and a commit that rebases
/// past a conflict lands on a different number again.
///
/// Data file names can answer it. They are chosen before the commit, so the
/// marker the same commit carries can name them, and they are stable no matter
/// which version the commit ends up as. So:
///
/// - the same files → the table is exactly what the sort left;
/// - the sorted files still there, with more after them → rows were appended,
///   and only then is `min_unsorted_rows` consulted, which is the churn that
///   threshold exists to damp;
/// - anything else → the data was replaced, whatever the row count says.
///
/// Appends only ever add fragments after the existing ones, and lance hands out
/// fragment ids monotonically even across an overwrite, so "the sorted files
/// are still there" is a prefix test on the files in fragment order.
///
/// Deletes leave the data files alone and write a deletion file beside them, so
/// they read as unchanged here — which is right: deleting rows does not unsort
/// the ones that remain.
pub fn verdict(
    spec: &SortSpec,
    state: &SortState,
    rows: u64,
    files: &[String],
    min_unsorted_rows: u64,
) -> SortVerdict {
    let (Some(recorded_digest), Some(recorded_files)) = (state.digest.as_deref(), state.files)
    else {
        return SortVerdict::NeverSorted;
    };
    if state.fields.as_deref() != Some(spec.to_string().as_str()) {
        return SortVerdict::OrderChanged;
    }
    if digest(files) == recorded_digest {
        return SortVerdict::UpToDate;
    }

    // Not the same files. Only a prefix match means the sorted data survived
    // and the rest was appended after it.
    if recorded_files > files.len() || digest(&files[..recorded_files]) != recorded_digest {
        return SortVerdict::Rewritten;
    }
    // A marker that lost its row count cannot say how much was appended, and
    // guessing zero would report a table with new fragments as sorted.
    let Some(sorted_rows) = state.rows else {
        return SortVerdict::Rewritten;
    };
    let appended = rows.saturating_sub(sorted_rows);
    if appended >= min_unsorted_rows.max(1) {
        return SortVerdict::RowsAppended(appended);
    }
    SortVerdict::UpToDate
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
        assert!(SortSpec::parse("a, a desc").is_err());
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

    fn files(names: &[&str]) -> Vec<String> {
        names.iter().map(|n| n.to_string()).collect()
    }

    #[test]
    fn verdicts_follow_the_marker() {
        let spec = SortSpec::parse("a asc").unwrap().unwrap();
        let sorted = files(&["a.lance", "b.lance"]);

        let fresh = SortState::default();
        assert_eq!(
            verdict(&spec, &fresh, 10, &sorted, 100),
            SortVerdict::NeverSorted
        );

        let state = SortState {
            fields: Some(spec.to_string()),
            rows: Some(1_000),
            files: Some(sorted.len()),
            digest: Some(digest(&sorted)),
        };

        // The same files, whatever version the commit ended up as.
        assert_eq!(
            verdict(&spec, &state, 1_000, &sorted, 100),
            SortVerdict::UpToDate
        );

        // Appended: the sorted files are still the first ones.
        let appended = files(&["a.lance", "b.lance", "c.lance"]);
        assert_eq!(
            verdict(&spec, &state, 1_050, &appended, 100),
            SortVerdict::UpToDate
        );
        assert_eq!(
            verdict(&spec, &state, 1_100, &appended, 100),
            SortVerdict::RowsAppended(100)
        );

        // Replaced: different files, and the row count is no defence — this is
        // the case a row-count threshold lets through, whatever the delta.
        let replaced = files(&["x.lance", "y.lance", "z.lance"]);
        assert_eq!(
            verdict(&spec, &state, 1_001, &replaced, 100),
            SortVerdict::Rewritten
        );
        assert_eq!(
            verdict(&spec, &state, 1_000, &replaced, 100),
            SortVerdict::Rewritten
        );
        // Compacted into fewer files is a replacement too.
        assert_eq!(
            verdict(&spec, &state, 1_000, &files(&["merged.lance"]), 100),
            SortVerdict::Rewritten
        );

        // Deleting rows rewrites no data file, so the table still reads as
        // sorted — which it is.
        assert_eq!(
            verdict(&spec, &state, 900, &sorted, 100),
            SortVerdict::UpToDate
        );

        let other = SortSpec::parse("b desc").unwrap().unwrap();
        assert_eq!(
            verdict(&other, &state, 1_000, &sorted, 100),
            SortVerdict::OrderChanged
        );
    }

    // Guessing zero would call a table with fragments appended after the sort
    // "sorted", and it would stay that way.
    #[test]
    fn a_marker_without_a_row_count_is_stale() {
        let spec = SortSpec::parse("a asc").unwrap().unwrap();
        let sorted = files(&["a.lance"]);
        let state = SortState {
            fields: Some(spec.to_string()),
            rows: None,
            files: Some(1),
            digest: Some(digest(&sorted)),
        };
        assert_eq!(
            verdict(&spec, &state, 5, &files(&["a.lance", "b.lance"]), 100),
            SortVerdict::Rewritten
        );
        // The untouched table is still up to date: the files answer that.
        assert_eq!(
            verdict(&spec, &state, 5, &sorted, 100),
            SortVerdict::UpToDate
        );
    }

    // Distinct columns whose names differ only in case are two columns in an
    // Arrow schema, not one named twice.
    #[test]
    fn case_distinguishes_two_fields() {
        let spec = SortSpec::parse("id asc, ID desc").unwrap().unwrap();
        assert_eq!(spec.fields.len(), 2);
        assert!(SortSpec::parse("id asc, id desc").is_err());
    }

    #[test]
    fn the_marker_round_trips_through_a_config_map() {
        let spec = SortSpec::parse("a asc").unwrap().unwrap();
        let written = files(&["one.lance", "two.lance"]);
        let recorded = SortState::record(&spec, &written, 42);
        let state = SortState::from_config(&recorded);
        assert_eq!(state.rows, Some(42));
        assert_eq!(state.files, Some(2));
        assert_eq!(
            verdict(&spec, &state, 42, &written, 1),
            SortVerdict::UpToDate
        );
    }
}
