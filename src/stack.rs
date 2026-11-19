//! Stack for planning deep expressions (issue #335).
//!
//! Deserializing, lowering, coercing, planning and evaluating an expression
//! recurse once per level of it, in LTSeq's code and in DataFusion's.
//! Unchecked, a few hundred levels overflowed a 2 MiB thread in a debug
//! build, and fewer on a smaller one, and the overflow killed the
//! interpreter. Three measures replace that crash:
//!
//! - [`MAX_EXPR_DEPTH`] bounds the depth of an expression. The deserializers
//!   (`types::dict_to_py_expr`, the group dialect's) check it without
//!   recursing, so a deeper expression raises `ValueError` on any stack.
//! - [`guarded`] runs pure-Rust work with enough stack for the deepest
//!   expression the process has deserialized ([`red_zone`]), switching to a
//!   heap-allocated stack twice that size when the thread has less left.
//!   Each operation that plans an expression runs its planning through
//!   [`planned`], and so does each operation on a table's plan that takes
//!   none (a slice, a rename, a union, a join): a plan an earlier operation
//!   accepted holds the expression, and cloning, ordering, joining, printing
//!   or dropping the plan recurses through it. Execution runs through
//!   `gil::detached`, a table drops its plan through it, so does an Arrow
//!   stream that ends, and LTSeq's lowering calls it once per level.
//! - The Tokio runtime's threads, which evaluate a plan's physical
//!   expressions, get [`WORKER_STACK`] each.
//!
//! DataFusion grows its own stack the same way in its tree traversals (its
//! `recursive_protection` feature), but not in the derived `Clone`, `Hash`,
//! `PartialEq` and drop of `Expr`, nor when it evaluates an expression. The
//! red zone grows with the depth to leave room for them: in a debug build,
//! whose frames are several times larger, a `filter` on a predicate 999
//! levels deep overflowed a 12 MiB red zone and planned in 16 MiB; in a
//! release build it planned in 1 MiB and not in 512 KiB. At the limit the
//! red zone is about twice that: 32 MiB in a debug build, 2.2 MiB in a
//! release one. In a debug build, evaluating such a predicate overflowed a
//! 4 MiB worker and ran in 8 MiB, so a worker gets 16 MiB there; a release
//! worker gets 8 MiB, which was not measured separately but is far more
//! than the planning took. Measured on Linux x86-64 with rustc 1.99.1.
//!
//! The red zone follows the deepest expression deserialized so far rather
//! than the limit because switching stacks costs: in a release build a
//! `filter` that switched twice (to plan, and to drop its table) took about
//! 125 µs where one that did not took 70 to 100 µs. A program that never
//! builds a deep expression keeps its own stack, as before; one that has
//! built one switches on threads too small for it from then on.
//!
//! [`guarded`] must not run Python. From Python 3.14 the interpreter checks
//! its recursion against the bounds of the thread's own stack, and how it
//! reads a frame on another stack has changed between releases. So the
//! deserializers, which read Python dicts, keep their own stack instead of
//! recursing, and a closure that reads an Arrow stream a Python object
//! produces runs detached on the thread's stack
//! (`gil::detached_on_own_stack`).

use crate::error::LtseqError;
use pyo3::{PyErr, PyResult};
use std::sync::atomic::{AtomicUsize, Ordering};

/// The deepest expression LTSeq plans: the most nodes on a path from the
/// root to a leaf. A column or a literal is one level, `r.a + 1` two.
///
/// Python's default recursion limit (1000) already stops a lambda building
/// a deeper expression while LTSeq serializes it, so the limit refuses only
/// expressions a program that raises the recursion limit can pass.
pub(crate) const MAX_EXPR_DEPTH: usize = 1000;

/// The stack [`red_zone`] leaves however shallow the expressions.
const BASE: usize = if cfg!(debug_assertions) {
    1 << 20
} else {
    256 << 10
};

/// The stack [`red_zone`] leaves for each level of the deepest expression.
const PER_LEVEL: usize = if cfg!(debug_assertions) {
    32 << 10
} else {
    2 << 10
};

/// Levels LTSeq can add above an expression it is given while it plans it
/// (an alias, a cast, a window over it, the `CASE` of `update`).
const ADDED_LEVELS: usize = 8;

/// The deepest expression deserialized so far (`saw_depth`).
static DEEPEST: AtomicUsize = AtomicUsize::new(0);

/// Record that an expression `depth` levels deep was deserialized: the
/// plans built from it need [`red_zone`] to leave room for it.
pub(crate) fn saw_depth(depth: usize) {
    DEEPEST.fetch_max(depth.min(MAX_EXPR_DEPTH), Ordering::Relaxed);
}

/// The least stack [`guarded`] leaves its closure: enough for the deepest
/// expression deserialized so far, with LTSeq's own levels above it.
fn red_zone() -> usize {
    red_zone_for(DEEPEST.load(Ordering::Relaxed))
}

fn red_zone_for(depth: usize) -> usize {
    BASE + (depth + ADDED_LEVELS) * PER_LEVEL
}

/// The stack of each thread of the Tokio runtime. Reserved, not committed,
/// like the stacks [`guarded`] switches to.
pub(crate) const WORKER_STACK: usize = if cfg!(debug_assertions) {
    16 << 20
} else {
    8 << 20
};

/// Plan parsed expressions on a [`guarded`] stack. `plan` takes no Python
/// object and reports an [`LtseqError`], which becomes a `PyErr` here, back
/// on the thread's own stack: building one can import a module.
pub(crate) fn planned<T>(plan: impl FnOnce() -> Result<T, LtseqError>) -> PyResult<T> {
    guarded(plan).map_err(PyErr::from)
}

/// Run `f` with at least [`red_zone`] of stack, on a new stack twice that
/// size when the thread has less left. The new stack is reserved, not
/// committed: a page costs memory once it is touched. `f` must not call
/// into Python (see the module docs).
#[inline]
pub(crate) fn guarded<R>(f: impl FnOnce() -> R) -> R {
    let red_zone = red_zone();
    stacker::maybe_grow(red_zone, 2 * red_zone, f)
}

/// [`guarded`] for work on an expression `depth` levels deep that
/// [`saw_depth`] has not recorded: one a deserializer refused.
pub(crate) fn guarded_for<R>(depth: usize, f: impl FnOnce() -> R) -> R {
    let red_zone = red_zone().max(red_zone_for(depth));
    stacker::maybe_grow(red_zone, 2 * red_zone, f)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// Runs `test` on a thread with a 256 KiB stack.
    pub(crate) fn on_a_small_stack<T: Send>(test: impl FnOnce() -> T + Send) -> T {
        std::thread::scope(|scope| {
            std::thread::Builder::new()
                .stack_size(256 << 10)
                .spawn_scoped(scope, test)
                .expect("spawn the test thread")
                .join()
                .unwrap_or_else(|panic| std::panic::resume_unwind(panic))
        })
    }

    #[test]
    fn guarded_leaves_room_for_the_deepest_expression_on_a_small_thread() {
        saw_depth(MAX_EXPR_DEPTH);
        let left = on_a_small_stack(|| guarded(stacker::remaining_stack));
        let needed = red_zone_for(MAX_EXPR_DEPTH);
        assert!(left.is_some_and(|left| left >= needed), "{left:?}");
    }
}
