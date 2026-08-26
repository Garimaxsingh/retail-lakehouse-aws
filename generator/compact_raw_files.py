"""Compacts order_events from ~130 tiny files/partition (96,289 files total,
~251KB avg) down to one file per date partition (736 files total).

The Glue raw_to_staging job timed out at 60 minutes against the uncompacted
layout — almost certainly Spark's per-file S3 listing/open overhead across
96K objects, not the actual data volume (24GB is nothing for Spark). This
is, entertainingly, exactly the small-files problem the sibling
data-perf-teardown project's E1 experiment is designed to demonstrate —
it just showed up here uninvited first.
"""
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path(__file__).parent / "data" / "raw" / "order_events"


def compact_partition(pdir: Path) -> tuple[int, int]:
    files = sorted(pdir.glob("*.parquet"))
    if len(files) <= 1:
        return len(files), len(files)
    tables = [pq.read_table(f) for f in files]
    merged = pa.concat_tables(tables)
    tmp = pdir / "_compacted.parquet"
    pq.write_table(merged, tmp, compression="snappy")
    for f in files:
        f.unlink()
    tmp.rename(pdir / "part-0.parquet")
    return len(files), 1


def main():
    partitions = [d for d in ROOT.iterdir() if d.is_dir()]
    print(f"[compact] {len(partitions):,} partitions to compact", flush=True)

    total_before = total_after = 0
    with ThreadPoolExecutor(max_workers=6) as ex:
        for i, (before, after) in enumerate(ex.map(compact_partition, partitions)):
            total_before += before
            total_after += after
            if (i + 1) % 100 == 0:
                print(f"[compact] {i+1:,}/{len(partitions):,} partitions done", flush=True)

    print(f"[done] {total_before:,} files -> {total_after:,} files", flush=True)


if __name__ == "__main__":
    main()
