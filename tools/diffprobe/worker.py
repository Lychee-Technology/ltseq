"""Run one differential case file under the current interpreter.

Usage: python -I worker.py HARNESS_DIR CASE_FILE [FILTER]

Prints one JSON line naming the ltseq package it imported, then one JSON
line per case (see py-ltseq/tests/differential/harness.py). Run it with
``-I`` so the working directory and PYTHONPATH cannot change which ltseq
is imported; ``diffprobe.py`` does.
"""

import json
import sys


def main():
    harness_dir, case_file = sys.argv[1], sys.argv[2]
    only = sys.argv[3] if len(sys.argv) > 3 else None
    sys.path.insert(0, harness_dir)
    import harness  # noqa: E402  (path set above)

    import ltseq

    print(json.dumps({"_ltseq": ltseq.__file__}), flush=True)
    for record in harness.run_file(case_file, only):
        print(json.dumps(record, default=str), flush=True)


if __name__ == "__main__":
    main()
