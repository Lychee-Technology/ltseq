# Stream error spike report (#256)

#256 asks, before M1 builds `to_batches()` and the PyCapsule stream, whether
the two error paths of the v0.5 contract can be built as written, and what
the first one costs. It is also the feasibility check on OD-X6, which the
owner decided on PR #250 to keep "subject to feasibility verification":
closing the reader `to_batches()` returns stops the execution (§15.1).

Three of the four answers confirm the contract. The fourth does not:
**with pyarrow 26.0.1, no `pyarrow.RecordBatchReader` both keeps the LTSeq
class on Python reads and stops the execution on `close()`.** §15.1 asks for
both, so the owner has to choose before M1 (see [Questions for the
owner](#questions-for-the-owner)).

| Question | Contract | Answer |
|---|---|---|
| Step 1: do Python reads of a reader over a Rust iterator keep the class? | §15.1 Errors | **Yes.** `read_next_batch`, iteration and `read_all` raise the LTSeq class and message unchanged, after the batches before the error |
| Step 2: does a C stream with LTSeq's own errno and message reach consumers as §16.4 says? | §16.4 | **Yes for pyarrow**: `EIO` gives `OSError`, `EINVAL` gives `ArrowInvalid`, and the message is exactly `"<class>: <message>"`. DuckDB and Polars carry the message but raise one class of their own whatever the errno |
| Step 3: what does the Rust iterator cost? | §15.1, §21.1 | Nothing measurable at 8,192-row batches with Python otherwise idle; 1.1 to 1.4 times at 1,024-row batches, and up to 2.3 times when DuckDB reads it. With another Python thread busy, every batch waits for the GIL about three times, 17 ms a batch against 5 for today's stream, and DuckDB reading `to_batches()` takes 28 to 45 times as long as DuckDB reading the table |
| OD-X6: does `close()` stop the execution? | §15.1 Resources | **No** for the reader that keeps the class: pyarrow's `close()` does not reach the iterator, and reads continue after it. Dropping the last reference does stop it. A reader imported from the C stream stops on `close()` but loses the class |

## Setup

| | |
|---|---|
| Machine | Intel Core i7-8700K (6 cores, 12 threads; 8 CPUs visible to this VM), 23 GiB, Linux 7.0.14 (Proxmox guest) |
| Build | `maturin develop --release --features stream_spike` (fat LTO), rustc 1.97.1 |
| Python side | CPython 3.13.5, pyarrow 26.0.1, DuckDB 2.0.0, Polars 2.1.0, switch interval 5 ms (the default) |
| Commit | This branch, on `b357449` |
| Commands | `python benchmarks/stream_errors/probe.py`; `python benchmarks/stream_errors/bench.py` (90 cells, about 15 minutes) |

The issue's evidence used pyarrow 25.0.1 and Python 3.14.7. Its prototype
script gives the same classes and messages with 26.0.1; one difference
from a Rust iterator is noted in step 1.

**The spike** (`src/stream_spike/`, cargo feature `stream_spike`; see
[README.md](README.md)):

- `_SpikeBatches`, a PyO3 iterator over one execution of the table's plan in
  its defined order. `to_batches(t)` wraps it in
  `pyarrow.RecordBatchReader.from_batches`.
- A C stream that fills in `FFI_ArrowArrayStream` with its own callbacks.
  arrow-rs's exporter cannot be reused: it reports `ArrowError`'s errno
  and `Display` text, so today's stream ends a division by zero with
  `ArrowInvalid: External error: Arrow error: Divide by zero error`.
- A classifier that names the §20.1 class of an execution error from the
  error at the bottom of DataFusion's chain (`Shared`, `Context`,
  `External`, `ArrowError`, `ObjectStore`, `io::Error`).
- Stand-ins for the §20.1 classes with the contract's bases, since v0.5's
  classes do not exist yet.

**Error sources.** Three are real execution errors of today's engine:
integer `//` by zero (`DivisionByZeroError`), a cast of `'a'` to `int64`
(`CastError`), and a Parquet file deleted after `read_parquet`
(`SourceNotFoundError`). They travel through DataFusion's operators at
4 partitions. `ArithmeticOverflowError` and `OrderViolationError` have no
execution error today (integer arithmetic wraps; `assume_sorted` is not
checked), so a fault injected at the top of the stream raises them, and
`SourceNotFoundError`, `LTSeqIOError` and `ExecutionError`, after three
batches. An injected fault does not cross DataFusion's operators; the real
errors do.

## Step 1: Python reads keep the class

Every cell in [results/probe.md](results/probe.md)'s three Python-read
tables raises the stand-in class itself (`type(e) is CastError`, and so on),
with the message the iterator gave it. `read_next_batch` returns the three
batches before an injected fault, then raises. This holds for all six
injected classes and the three real errors, and `polars.from_arrow` of the
reader keeps the class too.

pyarrow keeps the class because its reader over a Python iterator stores the
iterator's exception in the `Status` it returns and raises that exception
again. So the class survives exactly as far as the reader is read through
pyarrow's Python API. Through the C stream it does not
(`RecordBatchReader.from_stream(reader)`, DuckDB over the reader): pyarrow
raises `ArrowInvalid`, or `OSError` for an `OSError` subclass, with a
message it composes, for example `Unknown error: division by zero. Detail:
Python exception: ltseq._stream_spike.DivisionByZeroError: division by
zero`. That is the case §15.1 already exempts. A Python generator's
exception gets its traceback appended there; one raised in Rust has no
Python frames, so only its class and message are.

So §15.1's error sentence can be built as written. `test_stream_spike.py`
pins it for every read and class, at 1 and 4 partitions.

**Errors when the execution starts.** A Parquet footer is read at every
execution (#360), so a file deleted after `read_parquet` fails when the
execution starts, before any read: `to_batches()` itself raises
`SourceNotFoundError`, and so does `pa.table(s)`, because
`__arrow_c_stream__` is a Python call whose exception pyarrow passes on.
DuckDB wraps it (`reading the schema of the Python object 's' failed:
SourceNotFoundError: ...`). §20.2 places a missing source at the reader's
call; a file that disappears between the call and the execution is not
named there, and this is where it lands.

## Step 2: the C stream's errno and message

The spike's stream ends with `EIO` for `LTSeqIOError` and
`SourceNotFoundError` and `EINVAL` for every other class, and its
`get_last_error` returns `"<class>: <message>"`. What each consumer raises
([results/probe.md](results/probe.md)):

| Consumer | `EINVAL` | `EIO` | Message |
|---|---|---|---|
| `pa.table(s)`, `RecordBatchReader.from_stream(s)` | `pyarrow.ArrowInvalid` | `OSError` | `CastError: injected after 3 batches`, exactly |
| DuckDB 2.0.0, `duckdb.sql("... from s")` | `duckdb.InvalidInputException` | the same | `Invalid Input Error: reading the stream of the Python object 's' failed: CastError: ...` |
| Polars 2.1.0, `pl.DataFrame(s)`, `pl.from_dataframe(s)` | `polars.exceptions.ComputeError` | the same | `got external error: CastError: ...` |

pyarrow does what §16.4 says, with the message untouched (no status prefix,
no traceback). DuckDB and Polars ignore the errno and wrap the message, so
the class name is in their message but not at its start. §16.4 promises
only pyarrow's behaviour, so nothing in it is wrong; its wording that the
message "starts with the class name" describes what LTSeq's stream carries,
not what DuckDB or Polars print.

**What M1 has to write.** arrow-rs's `FFI_ArrowArrayStream::new` cannot
carry the code and message: its errno comes from the `ArrowError` variant
and its message from `ArrowError`'s `Display`, which prefixes it
(`External error: `, `Io error: `). The spike's `capsule.rs` replaces the
four callbacks (under 180 lines with comments and panic guards); M1 can
start from it. The classifier also has to see through `source::LocalFiles`, which
reports a footer read of a missing file as `Generic` around `NotFound` so
that the listing does not take the path for a directory: a classifier that
matched only `NotFound` at the top would call it `LTSeqIOError`.

## Step 3: what the iterator costs

The input and pipeline are #251's m3 baseline: the R3 harness's 8 Parquet
files (640 MiB decoded) through
`filter(id % 4 != 0).select(id, v).derive(x = v * 2)`, 12.6 million rows.
`py_*` terminals drain a reader in a Python `for` loop; `duckdb_*`
terminals run `select count(*), sum(x)` in DuckDB, whose threads pull the
stream. `*_today` reads the table's C stream as it is today. Each cell
gives the median time and the ratio to today's terminal of the same kind
([results/bench.md](results/bench.md)).

**Idle, 8,192-row batches (1,536 per run).**

| Terminal | 1 partition | 4 | 8 |
|---|---|---|---|
| `py_today` | 0.307 s | 0.109 s | 0.086 s |
| `py_to_batches` | 1.04 | 1.02 | 1.00 |
| `py_to_batches_reimported` | 1.04 | 1.05 | 1.00 |
| `duckdb_today` | 0.354 s | 0.119 s | 0.092 s |
| `duckdb_to_batches` | 1.09 | 1.01 | 0.94 |

**Idle, 1,024-row batches (12,288 per run).**

| Terminal | 1 partition | 4 | 8 |
|---|---|---|---|
| `py_today` | 0.429 s | 0.148 s | 0.120 s |
| `py_to_batches` | 1.11 | 1.17 | 1.44 |
| `py_to_batches_reimported` | 1.20 | 1.69 | 1.97 |
| `duckdb_today` | 0.482 s | 0.166 s | 0.127 s |
| `duckdb_to_batches` | 1.44 | 1.81 | 2.33 |

At 8,192 rows the iterator is within the noise band m0 used (0.1 of
today). Per batch it costs about 4 µs in a Python loop and 14 µs through
DuckDB (8 partitions), so at 1,024 rows, where the engine spends about 10 µs
on a batch, that is 1.1 to 1.4 times, 1.2 to 2.0 when a consumer
re-imports the reader through the C stream, and 1.4 to 2.3 for DuckDB.
Against m3's order-preserving merge (1.06 to 1.75 times today on the same
pipeline, on #251's machine), the iterator adds nothing at the default batch
size and is of the same order at 1,024 rows; the two costs would multiply.

**With another Python thread busy.** A thread spinning in pure Python
holds the GIL until the interpreter's switch interval (5 ms) asks it to let
go, so every GIL acquisition on the reading side waits up to 5 ms. These
cells read the first file only (192 batches of 8,192 rows):

| Terminal | 1 partition | 4 | 8 | GIL waits per batch |
|---|---|---|---|---|
| `py_today` | 5.4 ms/batch | 5.2 | 4.5 | 1 |
| `py_to_batches` | 17.0 | 17.7 | 16.9 | about 3.3 |
| `py_to_batches_poll` | 5.1 | 12.7 | 12.3 | about 2.5 at 4 and 8 |
| `py_to_batches_reimported` | 21.8 | 21.2 | 21.0 | about 4 |
| `duckdb_today` | 0.8 | 0.6 | 0.5 | 0 |
| `duckdb_to_batches` | 21.5 | 22.1 | 21.6 | about 4 |

A Python loop over today's stream waits once a batch: pyarrow releases the
GIL to read and takes it back. The reader over the iterator waits about
three times a batch: pyarrow's reader takes the GIL to call `next()` and
gives it back, about 1.5 waits more (`py_to_batches_attached`, which keeps
the GIL through the pull, takes 12.7 ms a batch at 4 partitions), and the
iterator's own release during the pull adds about one. DuckDB over the table never needs the
GIL; over `to_batches()` its threads wait four times a batch, 28 to 45
times as long as over the table. That is a property of pyarrow's reader,
not of the iterator: any `RecordBatchReader.from_batches` reader costs it.

Two ways to shrink it were measured:

- **Taking a ready batch without releasing the GIL** (`gil="poll"`) removes
  the iterator's own wait at 4 and 8 partitions (12.7 against 17.7 ms),
  where a repartition has batches waiting. At 1 partition there is no
  repartition, so "ready" means computing the batch on the calling thread
  with the GIL held: it looks faster (5.1 ms) only because the spinning
  thread is starved, which is what ADR 0016 forbids. M1 could get the gain
  without that by driving the stream on the runtime into a channel of one or
  two batches and taking from it under the GIL, at the cost of those
  batches in memory.
- **Reading the table rather than the reader.** DuckDB 2.0.0 and Polars
  scan any object with `__arrow_c_stream__`, so they can take `t` directly.
  §16.4's last bullet sends DuckDB to `t.to_batches()`, the slowest
  measured path. Only DuckDB 2.0.0 and Polars 2.1.0 were tested.

## OD-X6: closing the reader

Executions the spike holds, after one batch was read
([results/probe.md](results/probe.md)):

| Reader | Open | After `close()` | Read after `close()` | After `del` |
|---|---|---|---|---|
| `to_batches()`: `from_batches` over the Rust iterator | 1 | 1 | returns the next batch | 0 |
| `from_stream(s)`: imported from the spike's C stream | 1 | 0 | `ArrowInvalid: Attempt to read from a stream that has already been closed` | 0 |
| `from_stream(to_batches())` | 1 | 0 | the same | 0 |
| `to_batches()` in a `with` block | | 1 after the block | | 0 |

pyarrow's reader over a Python iterator does nothing on `close()`: neither
`close()` nor leaving a `with` block, which calls it, reaches the iterator
(its `close()` method is never called) or the execution, and the reader
goes on reading. A Python generator behind the reader is not closed either
(its `finally` runs only when the reader is dropped). Dropping the reader releases the iterator at once under CPython's
reference counting, and the execution with it. A reader imported from a C
stream does stop on `close()`, because pyarrow releases the stream, but it
is the reader that loses the class (step 1).

So OD-X6's MUST and §15.1's error sentence cannot both hold for a
`pyarrow.RecordBatchReader` with pyarrow 26.0.1. `pyarrow.RecordBatchReader`
cannot be subclassed to add a `close()`: its constructors are static and
return the base class, and its C++ reader is not reachable from Python.

## Questions for the owner

1. **OD-X6 against the class (§15.1).** The options this spike sees:
   - (a) Keep the reader over the iterator (the class holds on Python reads)
     and change Resources to "dropping the last reference to the reader
     stops the execution and releases its resources; `close()` does not".
     A reader left open in a reference cycle or a long-lived variable keeps
     its execution, and its memory, until it is collected.
   - (b) Return a reader imported from LTSeq's own C stream: `close()`
     stops the execution, but Python reads raise `ArrowInvalid` or
     `OSError` with `"<class>: ..."`, which revises §15.1's error sentence,
     R1, X3 and P4.
   - (c) Return an LTSeq reader class that is not a
     `pyarrow.RecordBatchReader` but offers its read methods,
     `__arrow_c_stream__` and a `close()` that stops the execution. It keeps
     both behaviours and changes the return type, which `reader.schema`,
     `isinstance` checks and libraries that require pyarrow's class see.
   - (d) Ask pyarrow to forward `close()` to the iterator's `close()`, as a
     generator's is, and take (a) until a pyarrow with it is the minimum.
     Not in 26.0.1.

   The spike recommends (a) with (d): it keeps the class, which is what
   §15.1's tests R1, X3 and P4 assert, and dropping the reader is how
   CPython code releases it anyway; the change narrows the MUST to what
   pyarrow can do.
2. **§16.4's DuckDB advice.** Should §16.4 send DuckDB and Polars to the
   table itself rather than to `t.to_batches()`? Over the reader, DuckDB
   took the GIL four times a batch: up to 2.3 times today's time idle at
   1,024-row batches, and 28 to 45 times with another Python thread busy.
3. **A budget for the iterator.** Idle at the default batch size it costs
   nothing measurable; at 1,024-row batches 1.1 to 1.4 times in a Python
   loop. Is that within M1's budget, and should M1 prefetch into a channel
   to drop the iterator's own GIL wait (question 3's `poll` result)?

## Caveats

- The v0.5 classes are stand-ins defined in `ltseq._stream_spike`; the real
  ones will be in `ltseq.exceptions`. Nothing here depends on where the
  class is defined.
- Injected faults enter at the top of the stream. Only the three real
  errors cross DataFusion's operators; those cover `DivisionByZeroError`,
  `CastError` and `SourceNotFoundError`, and the classifier has unit tests
  for an overflow, a missing file in a store error and wrapping in `Shared`
  and `Context`.
- The batch before which a real error arrives varies with the partitions'
  timing (4 to 6 batches at 4 partitions in the probe), because the
  ordered merge of #251's m3 is not built. Which read raises it is not part
  of this spike.
- The busy thread is the worst case: a thread that never blocks. A thread
  that waits on I/O releases the GIL and costs the reader less. The wait
  per acquisition follows `sys.setswitchinterval`.
- One machine, a virtual one, shared with other work; no cell was rerun for
  contention. Read idle ratios within 0.1 of 1 as no difference.
- Only CPython was tested. Under another implementation, dropping a reader
  may not release it at once, which matters for option (a).
