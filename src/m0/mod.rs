//! Prototypes that price the v0.5 contract's costly commitments (#251, M0).
//!
//! Compiled only with the `m0` cargo feature, so no shipped build contains
//! them. Each prototype is faithful enough to measure what its commitment
//! costs and no more; none is the implementation of the impact-map item it
//! gates, and each goes away when that item lands:
//!
//! | Measurement | Prototype | Gates |
//! |---|---|---|
//! | m1 | `checked`: checked `+ - *`, integer `**`, `sum` | M5 |
//! | m2 | `exact`: exact float `sum`, `mean`, `var`, `std`, `cov`, `corr` | M13, M4 |
//! | m3 | `ordered`: file-then-row order restored after a parallel scan | M1 |
//! | m4 | `masked`: either operand of `&`/`|` guards the other (D10) | M16 |
//! | m4b | `pushdown`: a filter kept above a sort, aggregate or distinct, or its input (D10a) | M16 |
//! | m5 | `validate`: `assume_sorted` checked on every row, no pruning (D7) | M3 |
//! | m6 | `views`: `string_view` normalized to `string` | M18 |
//!
//! Flags are process-wide and read when a session is created, like the
//! layout overrides of `engine::LayoutOverrides`: a table keeps the
//! prototypes in effect at its reader or constructor call. Python sets them
//! through `ltseq._testing.prototypes`.

use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

use datafusion::execution::config::SessionConfig;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::optimizer::Optimizer;
use datafusion::prelude::SessionContext;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

pub(crate) mod checked;
pub(crate) mod exact;
pub(crate) mod exact_acc;
pub(crate) mod masked;
pub(crate) mod ordered;
pub(crate) mod pushdown;
pub(crate) mod validate;
pub(crate) mod views;

/// m1: how integer `+ - *`, `**` and `sum` are computed.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub(crate) enum Checked {
    /// Today's wrapping DataFusion kernels.
    #[default]
    Off,
    /// arrow's checked kernels (`add`, `sub`, `mul`; `pow` through
    /// `i64::checked_pow`), which stop at the first overflow.
    Arrow,
    /// Wrapping arithmetic plus a branch-free overflow mask per batch.
    Fast,
}

/// m4: how `&` and `|` evaluate a fallible operand in a filter.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub(crate) enum Masked {
    /// Today's `BinaryExpr`: the left operand on every row, the right one
    /// pre-selected only when the left has no NULL and decides at most 20%.
    #[default]
    Off,
    /// Evaluate a fallible operand only on the rows the other leaves open
    /// (filter, evaluate, scatter).
    Select,
    /// Evaluate fallible operands on every row with errors kept as a mask,
    /// and raise only for rows whose value the result needs.
    Errors,
    /// Evaluate as today; when that raises, evaluate the batch again as
    /// `Errors` does.
    Optimistic,
}

/// m2: how float statistics are accumulated.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub(crate) enum FloatSums {
    /// DataFusion's float accumulators.
    #[default]
    Off,
    /// Exact sums rounded once (the contract).
    Exact,
    /// Double-double (compensated) sums: deterministic for a fixed order but
    /// not exact. An OD-W candidate for windows, not the contract.
    Compensated,
}

/// m3: how the rows of a multi-partition plan reach a terminal.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) enum Order {
    /// Today: partitions interleave in arrival order.
    #[default]
    Off,
    /// The scan's contiguous partitions, emitted one after another, each
    /// buffered up to `order_buffer` batches ahead.
    Concat,
    /// The scan split into one unit per row group (in file order), run
    /// `order_window` units at a time, emitted in unit order.
    Interleaved,
}

/// m5: what `assume_sorted` over Parquet costs under D7.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) enum Sorted {
    /// Today: no validation, pruning and pushdown on.
    #[default]
    Off,
    /// Pruning and pushdown off, no validation: the read D7 forces.
    PruneOff,
    /// Pruning and pushdown off, and every row pair checked.
    Validate,
}

/// m4b's `scan_filter_off` control: whether the scan may skip rows. `Off`
/// turns off the four options m5's `PruneOff` and `Validate` turn off, but
/// leaves the partitions' scheduling as today's, which those two change, so
/// the control differs from today's session in the scan's filtering alone.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) enum Pruning {
    /// Today: statistics, page-index and bloom-filter pruning, and the
    /// scan's row filter.
    #[default]
    On,
    /// None of the four.
    Off,
}

/// m6: where Parquet's `string_view` columns become `string`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) enum Views {
    /// Today: the scan yields views and so does every terminal.
    #[default]
    Off,
    /// The scan decodes plain `string`/`binary` (`schema_force_view_types`
    /// off).
    Plain,
    /// The scan yields views; a cast at the plan's root normalizes them.
    Cast,
}

/// m4b: how far a filter above a sort, aggregate or distinct may move
/// down (D10a).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) enum Restrict {
    /// Today: as far as DataFusion pushes it, into the scan if it can.
    #[default]
    Off,
    /// Not past the operator. Over a sort, the filter runs on the sort's
    /// merged stream, in one partition.
    Operator,
    /// Past the operator but not into its input, where the keys and
    /// arguments it demands on every row are computed.
    Keys,
}

/// Whether DataFusion may skip partial aggregation. It does once a
/// partition's first 100,000 rows hold more than 0.8 groups per row, which
/// for a grouping over many keys depends on how the input's batches reached
/// the partitions; m2 pins it off so every run takes the same path.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) enum PartialSkip {
    /// Today: DataFusion's probe decides.
    #[default]
    Default,
    /// Never skipped.
    Never,
}

/// The prototypes a session is created with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Prototypes {
    pub(crate) checked: Checked,
    pub(crate) masked: Masked,
    pub(crate) exact: FloatSums,
    pub(crate) order: Order,
    /// Units in flight for `Order::Interleaved`; `None` is `target_partitions`.
    pub(crate) order_window: Option<NonZeroUsize>,
    /// Batches a unit or partition may produce ahead of the consumer.
    pub(crate) order_buffer: NonZeroUsize,
    /// Delay before each batch the first `1 / target_partitions` of the
    /// scanned data produces, in milliseconds (0: none).
    pub(crate) slow_ms: u64,
    pub(crate) validate: Sorted,
    pub(crate) pruning: Pruning,
    pub(crate) views: Views,
    pub(crate) restrict_pushdown: Restrict,
    pub(crate) partial_skip: PartialSkip,
}

impl Default for Prototypes {
    fn default() -> Self {
        Prototypes {
            checked: Checked::Off,
            masked: Masked::Off,
            exact: FloatSums::Off,
            order: Order::Off,
            order_window: None,
            order_buffer: NonZeroUsize::new(2).expect("2 is nonzero"),
            slow_ms: 0,
            validate: Sorted::Off,
            pruning: Pruning::On,
            views: Views::Off,
            restrict_pushdown: Restrict::Off,
            partial_skip: PartialSkip::Default,
        }
    }
}

static CURRENT: Mutex<Option<Prototypes>> = Mutex::new(None);

impl Prototypes {
    fn current() -> Self {
        CURRENT
            .lock()
            .expect("prototype flags lock")
            .unwrap_or_default()
    }

    fn parse(flags: &HashMap<String, String>) -> Result<Self, String> {
        let mut p = Prototypes::default();
        for (key, value) in flags {
            let v = value.as_str();
            let bad = || format!("unknown value {v:?} for prototype flag {key:?}");
            match key.as_str() {
                "checked" => {
                    p.checked = match v {
                        "off" => Checked::Off,
                        "arrow" => Checked::Arrow,
                        "fast" => Checked::Fast,
                        _ => return Err(bad()),
                    }
                }
                "masked" => {
                    p.masked = match v {
                        "off" => Masked::Off,
                        "select" => Masked::Select,
                        "errors" => Masked::Errors,
                        "optimistic" => Masked::Optimistic,
                        _ => return Err(bad()),
                    }
                }
                "exact" => {
                    p.exact = match v {
                        "off" => FloatSums::Off,
                        "exact" => FloatSums::Exact,
                        "compensated" => FloatSums::Compensated,
                        _ => return Err(bad()),
                    }
                }
                "order" => {
                    p.order = match v {
                        "off" => Order::Off,
                        "concat" => Order::Concat,
                        "interleaved" => Order::Interleaved,
                        _ => return Err(bad()),
                    }
                }
                "order_window" => {
                    p.order_window = Some(v.parse().map_err(|_| bad())?);
                }
                "order_buffer" => p.order_buffer = v.parse().map_err(|_| bad())?,
                "slow_ms" => p.slow_ms = v.parse().map_err(|_| bad())?,
                "validate" => {
                    p.validate = match v {
                        "off" => Sorted::Off,
                        "prune_off" => Sorted::PruneOff,
                        "validate" => Sorted::Validate,
                        _ => return Err(bad()),
                    }
                }
                "pruning" => {
                    p.pruning = match v {
                        "on" => Pruning::On,
                        "off" => Pruning::Off,
                        _ => return Err(bad()),
                    }
                }
                "views" => {
                    p.views = match v {
                        "off" => Views::Off,
                        "plain" => Views::Plain,
                        "cast" => Views::Cast,
                        _ => return Err(bad()),
                    }
                }
                "restrict_pushdown" => {
                    p.restrict_pushdown = match v {
                        "off" => Restrict::Off,
                        "operator" => Restrict::Operator,
                        "keys" => Restrict::Keys,
                        _ => return Err(bad()),
                    }
                }
                "partial_skip" => {
                    p.partial_skip = match v {
                        "default" => PartialSkip::Default,
                        "never" => PartialSkip::Never,
                        _ => return Err(bad()),
                    }
                }
                _ => return Err(format!("unknown prototype flag {key:?}")),
            }
        }
        Ok(p)
    }

    fn to_flags(self) -> HashMap<String, String> {
        let name = |s: &str| s.to_string();
        let mut out = HashMap::new();
        out.insert(
            name("checked"),
            name(match self.checked {
                Checked::Off => "off",
                Checked::Arrow => "arrow",
                Checked::Fast => "fast",
            }),
        );
        out.insert(
            name("masked"),
            name(match self.masked {
                Masked::Off => "off",
                Masked::Select => "select",
                Masked::Errors => "errors",
                Masked::Optimistic => "optimistic",
            }),
        );
        out.insert(
            name("exact"),
            name(match self.exact {
                FloatSums::Off => "off",
                FloatSums::Exact => "exact",
                FloatSums::Compensated => "compensated",
            }),
        );
        out.insert(
            name("order"),
            name(match self.order {
                Order::Off => "off",
                Order::Concat => "concat",
                Order::Interleaved => "interleaved",
            }),
        );
        if let Some(window) = self.order_window {
            out.insert(name("order_window"), window.to_string());
        }
        out.insert(name("order_buffer"), self.order_buffer.to_string());
        out.insert(name("slow_ms"), self.slow_ms.to_string());
        out.insert(
            name("validate"),
            name(match self.validate {
                Sorted::Off => "off",
                Sorted::PruneOff => "prune_off",
                Sorted::Validate => "validate",
            }),
        );
        out.insert(
            name("pruning"),
            name(match self.pruning {
                Pruning::On => "on",
                Pruning::Off => "off",
            }),
        );
        out.insert(
            name("views"),
            name(match self.views {
                Views::Off => "off",
                Views::Plain => "plain",
                Views::Cast => "cast",
            }),
        );
        out.insert(
            name("restrict_pushdown"),
            name(match self.restrict_pushdown {
                Restrict::Off => "off",
                Restrict::Operator => "operator",
                Restrict::Keys => "keys",
            }),
        );
        out.insert(
            name("partial_skip"),
            name(match self.partial_skip {
                PartialSkip::Default => "default",
                PartialSkip::Never => "never",
            }),
        );
        out
    }
}

/// `SessionContext::new_with_config` plus the prototypes in effect now.
///
/// With every flag off this builds exactly the session the default build
/// does, so a baseline run of the `m0` build measures today's engine.
pub(crate) fn session(config: SessionConfig) -> SessionContext {
    session_with(Prototypes::current(), config)
}

/// [`session`] with the prototypes `p` instead of those in effect.
fn session_with(p: Prototypes, mut config: SessionConfig) -> SessionContext {
    let options = config.options_mut();
    if p.views == Views::Plain {
        options.execution.parquet.schema_force_view_types = false;
    }
    if p.partial_skip == PartialSkip::Never {
        // Skipping needs more groups than rows times this, which cannot be.
        options
            .execution
            .skip_partial_aggregation_probe_ratio_threshold = 1.0;
    }
    let d7 = matches!(p.validate, Sorted::PruneOff | Sorted::Validate);
    if d7 || p.pruning == Pruning::Off {
        // D7: nothing below the declaration may skip a row, so no statistics,
        // page-index or bloom-filter pruning and no row filter in the scan.
        // None of these stops the file opener from skipping a whole file by
        // its statistics, which is one reason `validate` checks one file.
        options.execution.parquet.pruning = false;
        options.execution.parquet.pushdown_filters = false;
        options.execution.parquet.enable_page_index = false;
        options.execution.parquet.bloom_filter_on_read = false;
    }
    if d7 {
        // The check pairs partition i's last row with partition i + 1's
        // first, so each partition must read only its own range, and
        // `PruneOff`, the check's baseline, reads the same way. A declared
        // order keeps partitions to their ranges, but a limit above the scan
        // clears that, and DataFusion 55 then lets an idle partition steal a
        // sibling's ranges. `Pruning::Off` alone keeps today's scheduling.
        options.execution.enable_file_stream_work_stealing = false;
    }
    if p.order != Order::Off {
        // A round-robin repartition would scatter batches across partitions
        // and lose the order the merge restores.
        options.optimizer.enable_round_robin_repartition = false;
        // DataFusion 55's file stream lets an idle partition steal another's
        // files, so a partition no longer holds a fixed range of the input.
        options.execution.enable_file_stream_work_stealing = false;
    }
    if p.restrict_pushdown == Restrict::Operator {
        // A filter kept above a sort reads the sort's merged stream.
        // DataFusion would scatter that stream round-robin for the filter's
        // sake, and the terminal would interleave the partitions again, so
        // the rows would leave unsorted (#148).
        options.optimizer.enable_round_robin_repartition = false;
    }

    let mut builder = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features();
    if p.restrict_pushdown != Restrict::Off {
        // Must run before `PushDownFilter`, so it goes first.
        let mut rules = Optimizer::new().rules;
        rules.insert(0, Arc::new(pushdown::RestrictPushdown(p.restrict_pushdown)));
        builder = builder.with_optimizer_rules(rules);
    }
    if p.checked != Checked::Off {
        builder = builder.with_optimizer_rule(Arc::new(checked::CheckedArithmetic::new(p.checked)));
    }
    if p.exact != FloatSums::Off {
        builder = builder.with_optimizer_rule(Arc::new(exact::ExactFloat::new(p.exact)));
    }
    // Physical rules run in this order: the scan is wrapped first, the root
    // last, so that the ordered merge ends up on top.
    if p.validate == Sorted::Validate {
        builder = builder.with_physical_optimizer_rule(Arc::new(validate::ValidateSorted));
    }
    if p.order == Order::Interleaved {
        builder = builder.with_physical_optimizer_rule(Arc::new(ordered::SplitScanByRowGroup));
    }
    if p.slow_ms > 0 {
        builder = builder.with_physical_optimizer_rule(Arc::new(ordered::SlowRegion {
            delay_ms: p.slow_ms,
        }));
    }
    if p.masked != Masked::Off {
        builder = builder
            .with_physical_optimizer_rule(Arc::new(masked::MaskedFilters { mode: p.masked }));
    }
    if p.views == Views::Cast {
        builder = builder.with_physical_optimizer_rule(Arc::new(views::CastViewsAtRoot));
    }
    if p.order != Order::Off {
        builder = builder.with_physical_optimizer_rule(Arc::new(ordered::OrderedOutput {
            order: p.order,
            window: p.order_window,
            buffer: p.order_buffer,
        }));
    }
    SessionContext::new_with_state(builder.build())
}

/// Replace every prototype flag with `flags` (a flag left out is off) and
/// return the previous flags. Tables built afterwards use the new flags.
#[pyfunction]
pub(crate) fn _set_m0_prototypes(
    flags: HashMap<String, String>,
) -> PyResult<HashMap<String, String>> {
    let next = Prototypes::parse(&flags).map_err(PyValueError::new_err)?;
    let mut current = CURRENT.lock().expect("prototype flags lock");
    let previous = current.unwrap_or_default();
    *current = Some(next);
    Ok(previous.to_flags())
}

/// The flags a table built now would get.
#[pyfunction]
pub(crate) fn _m0_prototypes() -> HashMap<String, String> {
    Prototypes::current().to_flags()
}

/// How this extension was compiled, so a measurement can refuse a debug
/// build: `{"debug_assertions": bool}`.
#[pyfunction]
pub(crate) fn _m0_build() -> HashMap<String, bool> {
    HashMap::from([("debug_assertions".to_string(), cfg!(debug_assertions))])
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    type Options = BTreeMap<String, Option<String>>;

    #[test]
    fn flags_round_trip_and_unknown_flags_are_refused() {
        let flags: HashMap<String, String> = [
            ("checked", "fast"),
            ("masked", "errors"),
            ("order", "interleaved"),
            ("order_window", "3"),
            ("slow_ms", "50"),
            ("restrict_pushdown", "keys"),
            ("pruning", "off"),
            ("partial_skip", "never"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
        let p = Prototypes::parse(&flags).unwrap();
        assert_eq!(p.checked, Checked::Fast);
        assert_eq!(p.order_window, NonZeroUsize::new(3));
        assert_eq!(Prototypes::parse(&p.to_flags()).unwrap(), p);
        assert!(
            Prototypes::parse(&HashMap::from([("nope".to_string(), "1".to_string())])).is_err()
        );
        assert!(Prototypes::parse(&HashMap::from([(
            "order".to_string(),
            "sorted".to_string()
        )]))
        .is_err());
    }

    /// The options a session built with `p` sets, and the names of the rules
    /// it runs, in order.
    fn session_of(p: Prototypes) -> (Options, Vec<String>) {
        // LTSeq's engine turns the scan's row filter on, which DataFusion's
        // default leaves off; with it on, all four options `pruning` turns
        // off start on, so each change shows.
        let mut config = SessionConfig::new();
        config.options_mut().execution.parquet.pushdown_filters = true;
        let state = session_with(p, config).state();
        let options = (state.config().options().entries().into_iter())
            .map(|e| (e.key, e.value))
            .collect();
        let rules = (state.analyzer().rules.iter().map(|r| r.name()))
            .chain(state.optimizers().iter().map(|r| r.name()))
            .chain(state.physical_optimizers().iter().map(|r| r.name()))
            .map(str::to_string)
            .collect();
        (options, rules)
    }

    fn changed(a: &Options, b: &Options) -> Vec<String> {
        assert!(a.keys().eq(b.keys()));
        a.keys().filter(|k| a[*k] != b[*k]).cloned().collect()
    }

    /// m4b's control differs from today's session in the four options that
    /// let the scan skip rows and in nothing else, scheduling included; m5's
    /// modes add range ownership to it.
    #[test]
    fn pruning_off_changes_only_what_the_scan_may_skip() {
        // m4b runs every mode with checked arithmetic on.
        let today = Prototypes {
            checked: Checked::Fast,
            ..Prototypes::default()
        };
        let control = Prototypes {
            pruning: Pruning::Off,
            ..today
        };
        let (today_options, today_rules) = session_of(today);
        let (control_options, control_rules) = session_of(control);
        assert_eq!(today_rules, control_rules);
        assert_eq!(
            changed(&today_options, &control_options),
            [
                "datafusion.execution.parquet.bloom_filter_on_read",
                "datafusion.execution.parquet.enable_page_index",
                "datafusion.execution.parquet.pruning",
                "datafusion.execution.parquet.pushdown_filters",
            ]
        );
        for validate in [Sorted::PruneOff, Sorted::Validate] {
            let (m5_options, _) = session_of(Prototypes { validate, ..today });
            assert_eq!(
                changed(&control_options, &m5_options),
                ["datafusion.execution.enable_file_stream_work_stealing"],
                "{validate:?}"
            );
        }
    }
}
