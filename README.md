# Wikipedia / Wiktionary Dump Processor

A pipeline that converts Wikipedia and Wiktionary `.tar.gz` dumps into [MDX](https://www.mdict.cn/) dictionary files.
Processes the English Wikipedia dump (roughly 500 GB of uncompressed HTML) in around 5 hours on a 16-core laptop with 32 GB RAM (including ~130 minutes for HTML processing and ~100 minutes for dedup).

## Features

- **Multi-project support** -- works with `wiki` (Wikipedia) and `wiktionary` (Wiktionary) dumps
- **Multi-language support** -- English (`en`) and French (`fr`) out of the box; easily extensible
- **Parallel processing** -- distributes work across CPU cores via Python `multiprocessing`
- **Efficient decompression** -- uses `indexed-gzip` + `rapidgzip` to seek directly into large `.gz` archives
- **DuckDB-backed intermediate storage** -- binned NDJSON files are deduped and merged with DuckDB
- **MDX output** -- produces `.mdx`, `.css` and `.js`

Note: Due to the large size of the resulted MDict txt, it is advised to use this multithreaded [version](https://github.com/leanhdung1994/mdict-utils) of `mdict-utils`.

## Screenshots

<p align="center">
  <img alt="Light" src="./screenshots/PDE.png" width="45%">
&nbsp; &nbsp; &nbsp; &nbsp;
  <img alt="Dark" src="./screenshots/Paris.png" width="45%">
</p>
<p align="center">
  <img alt="Light" src="./screenshots/Duoprism.png" width="45%">
&nbsp; &nbsp; &nbsp; &nbsp;
  <img alt="Dark" src="./screenshots/Python.png" width="45%">
</p>

## Usage

```bash
python main.py \
  --proj wiktionary \       # wiki | wiktionary
  --lang en \               # en | fr
  --input-dir  /path/to/dumps \
  --output-dir /path/to/output \
```

### Optional flags

  | Flag           | Default      | Description                                             |
  | -------------- | ------------ | ------------------------------------------------------- |
  | `--core N`     | 70 % of CPUs | Number of workers                                       |
  | `--chunk N`    | `0` (all)    | Number of NDJSON files per core                         |
  | `--bufsize MB` | `512`        | RAM buffer before flushing to disk                      |
  | `--debug`      | off          | Process only the first 2 NDJSON files (1000 lines each) |
  | `--mode MODE`  | `greedy`     | HTML pruning depth: `greedy` strips optional sections   |

## Pipeline overview

```
tar.gz dump
      │
      ▼
initial_setup
      │   Build gzip seek index, list NDJSON members,
      │   initialise progress log
      ▼
parallel_processor
      │   Parse HTML with selectolax across N cores,
      │   write hash-binned NDJSON files {prefix}_bin_0.ndjson … {prefix}_bin_N.ndjson
      ▼
parquet_collector
      │   Deduplicate per bin — keep latest dateModified per identifier,
      │   export {prefix}_bin_0.parquet … {prefix}_bin_N.parquet
      ▼
txt_and_modules_collector
      │   Merge all bins → .txt (MDX-formatted HTML entries),
      │   export module URLs → {prefix}_modules.parquet
      ▼
css_and_js_collector
      │   Collect unique modules from parquet, fetch CSS from
      │   live wiki in batches, bundle local JS assets
      ▼
mdx_collector
      │   Invoke mdict to produce final .mdx
      ▼
.mdx + .css + .js
```

## Project structure

```
src/
├── main.py                      # Entry point & CLI
├── config.py                    # Config dataclass & shared imports
├── initial_setup.py             # Dump indexing
├── parallel_processor.py        # Multiprocessing orchestration
├── ndjson_processor.py          # Per-file NDJSON process
├── html_processor.py            # HTML cleaning with selectolax
├── parquet_collector.py         # DuckDB dedupe
├── txt_and_modules_collector.py # Headword & modules export
├── css_and_js_collector.py      # CSS/JS bundling
├── mdx_collector.py             # MDX packaging
├── css_js/                      # Extract CSS & JS
│   ├── common.css / common.js
│   ├── wiki.css / wiki.js
│   ├── wiktionary.css / wiktionary.js
│   └── frwiki.js
└── requirements.txt
```

## Architecture

--------------------------------------------------------------------------------

### Zero-extraction parallel I/O

Workers never decompress the full `.tar.gz` archive to disk.
Instead, `initial_setup.py` builds a seek index once using `rapidgzip`, and each worker process opens the archive via `indexed-gzip`, jumping straight to its assigned NDJSON member by byte offset.

--------------------------------------------------------------------------------

### Producer / consumer paradigm

The worker pool and the disk writer run on completely separate threads.
Workers push `(bin, data, size, status)` tuples into a `Queue(maxsize=n_cores × 4)`; a single dedicated writer thread drains it and handles all disk I/O.

--------------------------------------------------------------------------------

### Hash-based binning dedup

Every parsed entry is routed to a bin by `identifier % n_bins`.
Because entries with the same `identifier` always land in the same bin, the deduplication (keep only the most recent `dateModified` per article) runs independently on each bin with zero cross-bin coordination.

--------------------------------------------------------------------------------

### In-memory write buffering

The writer maintains a separate byte buffer for each bin and only flushes to disk when the per-bin threshold (`buffer_size / n_bins`) is exceeded.
This converts millions of tiny per-entry writes into a large sequential appends per bin.

--------------------------------------------------------------------------------

### Merge and dedup

All merging, dedup, and export are delegated to DuckDB that handles:

- Memory limits (capped at 80 % of available RAM)
- Spill-to-disk via a configurable temp directory
- Multi-threaded execution across all cores

--------------------------------------------------------------------------------

### Resumable processing

After each batch, the pipeline writes a JSON log that records which NDJSON files have been processed.
On restart, already-completed files are skipped automatically.

--------------------------------------------------------------------------------

### Fault tolerance and debug mode

Individual entry failures never stall the pipeline.
Each entry is processed inside a `try/except`; any exception is printed, and the raw JSON line is collected.
The `--debug` flag processes the first 2 NDJSON files, making it fast to validate the full pipeline on a small slice of real data.

--------------------------------------------------------------------------------

## Acknowledgement

Many thanks to LE Quynh Anh for her encouragement and support.
