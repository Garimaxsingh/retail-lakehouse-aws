"""One-off fix: `ingest_ts` was written at nanosecond Parquet precision
(pandas datetime64[ns] default), which Spark's Parquet reader can't read
(`Illegal Parquet type: INT64 (TIMESTAMP(NANOS,false))`). event_ts and
change_ts are unaffected (both already millisecond precision).

This rewrites files in place, casting ingest_ts to microsecond precision —
a serialization fix, not a content change, so it doesn't conflict with the
"generate once, never regenerate" rule (no random values change).
"""
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path(__file__).parent / "data" / "raw" / "order_events"


def fix_file(path: Path) -> bool:
    table = pq.read_table(path)
    field = table.schema.field("ingest_ts")
    if not (pa.types.is_timestamp(field.type) and field.type.unit == "ns"):
        return False
    idx = table.schema.get_field_index("ingest_ts")
    fixed_col = table.column("ingest_ts").cast(pa.timestamp("us"), safe=False)
    table = table.set_column(idx, "ingest_ts", fixed_col)
    pq.write_table(table, path)
    return True


def main():
    limit = int(sys.argv[1]) if len(sys.argv) > 1 else None
    files = list(ROOT.rglob("*.parquet"))
    if limit:
        files = files[:limit]
    print(f"[fix] {len(files):,} files to check", flush=True)

    fixed = 0
    with ThreadPoolExecutor(max_workers=8) as ex:
        for i, result in enumerate(ex.map(fix_file, files)):
            if result:
                fixed += 1
            if (i + 1) % 5000 == 0:
                print(f"[fix] {i+1:,}/{len(files):,} checked, {fixed:,} fixed", flush=True)

    print(f"[done] {fixed:,}/{len(files):,} files had ingest_ts rewritten to microsecond precision", flush=True)


if __name__ == "__main__":
    main()
