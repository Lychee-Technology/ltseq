//! The rule inventory of the rewrite table (#273).
//!
//! Row-wise demand (contract §20.2) and the defined order (§9.1) hold only
//! while every rewrite DataFusion applies keeps them, and a DataFusion
//! upgrade adds, renames and re-enables rules without telling LTSeq. The
//! trade-off record's rule inventory ("Rule inventory" in
//! `docs/proposals/v0.5/review/trade-offs.md`) rates every rule a session
//! runs for errors and for order. These tests hold it to the rules of the
//! session `create_session_context` builds: a rule that runs without a row,
//! or a row for a rule that no longer runs, fails here, so an upgrade
//! re-rates its rules instead of silently weakening the contract. Every
//! row must be rated; a rule rated Restricted must run behind the guard
//! its row names (`crate::rewrite_guard`), and the tests a row names must
//! exist. The expression rewrites inside the simplifiers have no names a
//! session lists, so their table is pinned to the DataFusion version in
//! `Cargo.lock`, and an upgrade fails until they are rated again.
//!
//! A measurement build (`m0`) adds its own prototypes to the session, so
//! these tests run in the default build only.

use std::collections::BTreeSet;

use crate::engine::{session_context_with, LayoutOverrides};
use crate::rewrite_guard::{logical_guard, physical_guarded};

/// The trade-off record that holds the inventory.
const RECORD: &str = include_str!("../docs/proposals/v0.5/review/trade-offs.md");

/// Where the tests a guard column names live.
const TESTS: &[&str] = &[
    include_str!("rewrite_guard.rs"),
    include_str!("ops/aggregation.rs"),
    include_str!("../py-ltseq/tests/test_rewrite_guards.py"),
    include_str!("../py-ltseq/tests/test_cast_comparison.py"),
    include_str!("../py-ltseq/tests/test_float_min_max.py"),
    include_str!("../py-ltseq/tests/test_offset_slice.py"),
    include_str!("../py-ltseq/tests/test_declared_order.py"),
];

/// The lockfile, for the DataFusion version the expression rewrites were
/// rated against.
const LOCK: &str = include_str!("../Cargo.lock");

/// The ratings a row may give for errors, and for order. There is no
/// rating for "not rated yet": a row rates both, or the test fails.
const ERROR_RATINGS: [&str; 5] = ["Safe", "Masked", "Restricted", "Disabled", "Unreached"];
const ORDER_RATINGS: [&str; 5] = ["Safe", "Restricted", "Disabled", "Unreached", "Reorders"];

/// One row of a table: a rule or rewrite as DataFusion names it, the stage
/// it runs in, the first word of each rating, and its guard.
#[derive(Debug)]
struct Row<'a> {
    stage: &'a str,
    name: &'a str,
    errors: &'a str,
    order: &'a str,
    guard: &'a str,
}

impl Row<'_> {
    fn guarded_by_rating(&self) -> bool {
        ["Restricted", "Disabled"].contains(&self.errors)
            || ["Restricted", "Disabled"].contains(&self.order)
    }

    /// The names in backquotes in the guard column.
    fn guard_names(&self) -> Vec<&str> {
        self.guard.split('`').skip(1).step_by(2).collect()
    }
}

/// The rows between `begin` (the start of a marker) and `end`, and the
/// rest of the marker line.
fn table(begin: &str, end: &str) -> (&'static str, Vec<Row<'static>>) {
    let at = RECORD.find(begin).expect("the record marks the table");
    let marker_end = at + RECORD[at..].find("-->").expect("the marker closes");
    let attributes = RECORD[at + begin.len()..marker_end].trim();
    let start = marker_end + 3;
    let len = RECORD[start..].find(end).expect("the table ends");
    let rows = RECORD[start..start + len]
        .lines()
        .filter(|line| line.starts_with("| `"))
        .map(|line| {
            // `\|` inside a cell is an escaped pipe.
            let cells: Vec<&str> = split_cells(line);
            let first_word = |cell: &'static str| {
                cell.split(|c: char| !c.is_alphanumeric())
                    .next()
                    .unwrap_or_default()
            };
            Row {
                name: cells[1].trim_matches('`'),
                stage: cells[2],
                errors: first_word(cells[3]),
                order: first_word(cells[4]),
                guard: cells[5],
            }
        })
        .collect();
    (attributes, rows)
}

fn split_cells(line: &'static str) -> Vec<&'static str> {
    let mut cells = Vec::new();
    let mut from = 0;
    let bytes = line.as_bytes();
    for (i, &b) in bytes.iter().enumerate() {
        if b == b'|' && (i == 0 || bytes[i - 1] != b'\\') {
            cells.push(line[from..i].trim());
            from = i + 1;
        }
    }
    cells.push(line[from..].trim());
    cells
}

fn inventory() -> Vec<Row<'static>> {
    table("<!-- rule-inventory:begin", "<!-- rule-inventory:end -->").1
}

/// `(stage, name)` of every rule a session LTSeq creates runs.
fn running() -> BTreeSet<(&'static str, String)> {
    let session = session_context_with(LayoutOverrides::default());
    let state = session.state();
    let analyzer = state.analyzer();
    let mut rules = BTreeSet::new();
    for rewrite in analyzer.function_rewrites() {
        rules.insert(("function rewrite", rewrite.name().to_string()));
    }
    for rule in &analyzer.rules {
        rules.insert(("analyzer", rule.name().to_string()));
    }
    for rule in state.optimizers() {
        rules.insert(("logical", rule.name().to_string()));
    }
    for rule in state.physical_optimizers() {
        rules.insert(("physical", rule.name().to_string()));
    }
    rules
}

#[test]
fn every_rule_a_session_runs_has_a_row_and_every_row_a_rule() {
    let rated: BTreeSet<(&str, String)> = inventory()
        .iter()
        .map(|row| (row.stage, row.name.to_string()))
        .collect();
    let running = running();
    let unrated: Vec<_> = running.difference(&rated).collect();
    let stale: Vec<_> = rated.difference(&running).collect();
    assert!(
        unrated.is_empty() && stale.is_empty(),
        "the rule inventory in docs/proposals/v0.5/review/trade-offs.md \
         is out of date: rate the rules that run without a row {unrated:?}, \
         and remove the rows of rules that no longer run {stale:?}"
    );
}

/// Every row of both tables rates errors and order, and names a guard and
/// a test that exist where it is rated Restricted or Disabled.
#[test]
fn every_row_is_rated_and_every_restricted_row_names_its_guard() {
    let (_, expressions) = table(
        "<!-- expression-rewrites:begin",
        "<!-- expression-rewrites:end -->",
    );
    let rows = inventory();
    assert!(!rows.is_empty() && !expressions.is_empty());
    let mut seen = BTreeSet::new();
    for row in rows.iter().chain(&expressions) {
        assert!(seen.insert((row.stage, row.name)), "two rows for {row:?}");
        assert!(ERROR_RATINGS.contains(&row.errors), "rate errors: {row:?}");
        assert!(ORDER_RATINGS.contains(&row.order), "rate order: {row:?}");
        let names = row.guard_names();
        for name in &names {
            let test = name.starts_with("plan_guard_") || name.starts_with("test_");
            if test && !name.contains('.') {
                let defined = [format!("fn {name}("), format!("def {name}(")];
                assert!(
                    TESTS
                        .iter()
                        .any(|source| defined.iter().any(|d| source.contains(d.as_str()))),
                    "{row:?} names a test that does not exist: {name}"
                );
            }
        }
        if row.guarded_by_rating() {
            assert!(
                names
                    .iter()
                    .any(|n| n.starts_with("plan_guard_") || n.starts_with("test_")),
                "a {} or {} row names no test: {row:?}",
                row.errors,
                row.order
            );
        }
    }
}

/// A rule rated Restricted runs behind the guard its row names, and every
/// rule the session wraps is rated Restricted.
#[test]
fn every_restricted_rule_runs_behind_its_guard() {
    let running = running();
    for row in inventory() {
        let names = row.guard_names();
        let wrapped = match row.stage {
            "logical" => logical_guard(row.name).is_some(),
            "physical" => physical_guarded(row.name),
            _ => false,
        };
        assert_eq!(
            wrapped,
            names.contains(&"Guarded") || names.contains(&"GuardedPhysical"),
            "the session wraps a rule exactly when its row says so: {row:?}"
        );
        if wrapped {
            assert_eq!(row.errors, "Restricted", "{row:?}");
        }
        if row.errors == "Restricted" || row.order == "Restricted" {
            let rule = names
                .iter()
                .any(|n| n.starts_with("ltseq_") && running.iter().any(|(_, name)| name == n));
            assert!(
                wrapped || rule,
                "a Restricted rule without a guard: {row:?}"
            );
        }
    }
    // The session's rules are wrapped, not only listed.
    let session = session_context_with(LayoutOverrides::default());
    let state = session.state();
    for rule in state.optimizers() {
        let shown = format!("{rule:?}");
        assert_eq!(
            logical_guard(rule.name()).is_some(),
            shown.starts_with("Guarded("),
            "{shown}"
        );
    }
    for rule in state.physical_optimizers() {
        let shown = format!("{rule:?}");
        assert_eq!(
            physical_guarded(rule.name()),
            shown.starts_with("GuardedPhysical("),
            "{shown}"
        );
    }
}

/// The expression rewrites were rated against one DataFusion version. An
/// upgrade can change them while every rule keeps its name, so it fails
/// here until they are rated again and the marker names the new version.
#[test]
fn the_expression_rewrites_were_rated_for_the_datafusion_in_use() {
    let (attributes, _) = table(
        "<!-- expression-rewrites:begin",
        "<!-- expression-rewrites:end -->",
    );
    let rated = attributes
        .strip_prefix("datafusion=")
        .expect("the marker names the DataFusion version");
    let mut packages = 0;
    for package in LOCK.split("[[package]]") {
        let field = |key: &str| {
            package
                .lines()
                .find_map(|line| line.strip_prefix(key))
                .map(|value| value.trim().trim_matches('"'))
        };
        let Some(name) = field("name = ") else {
            continue;
        };
        if name == "datafusion" || name.starts_with("datafusion-") {
            packages += 1;
            assert_eq!(
                field("version = "),
                Some(rated),
                "{name} is not the DataFusion {rated} whose expression rewrites \
                 docs/proposals/v0.5/review/trade-offs.md rates: re-read them, \
                 rate what changed, and update the version in its marker"
            );
        }
    }
    assert!(packages > 10, "{packages} DataFusion crates in Cargo.lock");
}
