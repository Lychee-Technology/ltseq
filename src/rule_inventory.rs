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
//! re-rates its rules instead of silently weakening the contract.
//!
//! A measurement build (`m0`) adds its own prototypes to the session, so
//! these tests run in the default build only.

use std::collections::BTreeSet;

use crate::engine::{session_context_with, LayoutOverrides};

/// The trade-off record that holds the inventory.
const RECORD: &str = include_str!("../docs/proposals/v0.5/review/trade-offs.md");

const BEGIN: &str = "<!-- rule-inventory:begin -->";
const END: &str = "<!-- rule-inventory:end -->";

/// The ratings a row may give for errors, and for order.
const ERROR_RATINGS: [&str; 6] = [
    "Safe",
    "Masked",
    "Restricted",
    "Disabled",
    "Unreached",
    "Open",
];
const ORDER_RATINGS: [&str; 5] = ["Safe", "Restricted", "Disabled", "Unreached", "Open"];

/// One row of the inventory: a rule as DataFusion names it, the stage it
/// runs in, and the first word of each rating.
#[derive(Debug)]
struct Row<'a> {
    stage: &'a str,
    name: &'a str,
    errors: &'a str,
    order: &'a str,
}

/// The rows between the inventory's markers.
fn inventory() -> Vec<Row<'static>> {
    let start = RECORD.find(BEGIN).expect("the record marks the inventory") + BEGIN.len();
    let len = RECORD[start..].find(END).expect("the inventory ends");
    RECORD[start..start + len]
        .lines()
        .filter(|line| line.starts_with("| `"))
        .map(|line| {
            let cells: Vec<&str> = line.split('|').map(str::trim).collect();
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
            }
        })
        .collect()
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

#[test]
fn every_row_rates_errors_and_order_once() {
    let rows = inventory();
    assert!(!rows.is_empty());
    let mut seen = BTreeSet::new();
    for row in &rows {
        assert!(seen.insert((row.stage, row.name)), "two rows for {row:?}");
        assert!(ERROR_RATINGS.contains(&row.errors), "{row:?}");
        assert!(ORDER_RATINGS.contains(&row.order), "{row:?}");
    }
}
