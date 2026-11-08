# distutils: language = c++
# cython: language_level = 3
"""A pyarrow.RecordBatchReader subclass whose close() closes its source.

OD-X6's alternative (e) in REPORT.md: `RecordBatchReader.from_batches`
keeps the class of the iterator's exception on Python reads, but its
`close()` never reaches the iterator. This subclass shares the C++ reader
`from_batches` built and keeps the iterator, so every read is the inherited
one (and raises what it raised), while `close()`, and leaving a `with`
block, which calls it, also close the iterator.

It cimports pyarrow's own declarations (`pyarrow/lib.pxd`), so it compiles
against one pyarrow's headers and its `RecordBatchReader` layout, and links
its `libarrow` and `libarrow_python`: a build is valid for that pyarrow
release only. `closing_reader.py` builds it.
"""

from pyarrow.lib cimport RecordBatchReader


cdef class ClosingReader(RecordBatchReader):
    """A reader over another reader's C++ reader that also closes `source`."""

    cdef object _source

    def close(self):
        """Close the source (stopping its execution), then the reader."""
        source, self._source = self._source, None
        if source is not None:
            source.close()
        RecordBatchReader.close(self)


def closing_reader(RecordBatchReader reader not None, source):
    """A `ClosingReader` reading what `reader` reads, closing `source`.

    `pyarrow.RecordBatchReader.__init__` refuses to run, and its factories
    return the base class, so the instance is allocated with `__new__` and
    given `reader`'s C++ reader (a shared pointer, so both read the same
    stream).
    """
    cdef ClosingReader out = ClosingReader.__new__(ClosingReader)
    out.reader = reader.reader
    out._source = source
    return out
