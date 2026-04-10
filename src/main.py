from config import *
from initial_setup import initial_setup
from parallel_processor import process_parallel
from parquet_collector import collect_parquet
from mdx_collector import collect_mdx
from css_and_js_collector import collect_css_and_js
from txt_and_modules_collector import collect_txt_and_modules

# Use ~70% of cores, but always leave at least RESERVED_CORES free for the OS.
CORE_UTILIZATION = 0.7
RESERVED_CORES = 4
cores = max(
    1, min(os.cpu_count() - RESERVED_CORES, round(CORE_UTILIZATION * os.cpu_count()))
)

parser = argparse.ArgumentParser()
parser.add_argument(
    "--proj", default="wiktionary", help="Project code, e.g., wiktionary, wiki"
)
parser.add_argument("--lang", default="en", help="Language code, e.g., en, fr")
parser.add_argument(
    "--mode", default="greedy", help="Parsing mode, e.g., greedy, keep all"
)
parser.add_argument(
    "--debug",
    action="store_true",
    default=False,
    help="Run in debug mode, processing only the first 2 ndjson files with 1 line each",
)
parser.add_argument(
    "--core", type=int, default=cores, help="Number of CPU cores, e.g., 1, 2,..."
)
parser.add_argument(
    "--chunk",
    type=int,
    default=0,
    help="Number of chunks, e.g., 0, 1, 2,... If this value is set 0, then all ndjson will be processed",
)
parser.add_argument(
    "--bufsize",
    type=int,
    default=512,
    help="Maximum size in MB of json's kept in RAM before being written to disk",
)
parser.add_argument(
    "--input-dir",
    type=Path,
    default=r"D:\tmp",
    help="Path to directory containing the .tar.gz dump",
)
parser.add_argument(
    "--output-dir",
    type=Path,
    default=r"D:\result",
    help="Path to directory for output files",
)
args = parser.parse_args()

# The number of ndjson files processed in a batch is equal to nCore*nChunk

"""
Entry point for the Wikimedia dump → MDX dictionary pipeline.

High-level flow:
  1. initial_setup   – build gzip seek-index, discover ndjson shards, set up progress log
  2. process_parallel – parse HTML from each ndjson shard in parallel, fan-out into binned ndjson files
  3. collect_parquet  – deduplicate entries per bin (keep most-recent by dateModified) → parquet
  4. collect_txt_and_modules – merge all parquet bins into a single .txt + modules.parquet
  5. collect_css_and_js – fetch Wikimedia CSS modules + combine local JS overrides
  6. collect_mdx       – invoke mdict CLI to compile .txt → final .mdx dictionary
"""


def main():
    cfg = Config(
        project_code=args.proj,
        language_code=args.lang,
        parse_mode=args.mode,
        debug=args.debug,
        n_cores=args.core,
        n_chunks=args.chunk,
        buffer_size=args.bufsize,
        input_dir=Path(args.input_dir),
        output_dir=Path(args.output_dir),
    )

    initial_setup(cfg)  # Discover shards; build gzip index if missing
    process_parallel(
        cfg
    )  # Parse HTML → binned ndjson (parallel workers + writer thread)
    collect_parquet(cfg)  # Deduplicate per bin → parquet
    collect_txt_and_modules(cfg)  # Merge parquet → .txt + modules.parquet
    collect_css_and_js(cfg)  # Fetch/combine CSS & JS
    collect_mdx(cfg)  # Compile .txt → .mdx via mdict CLI


if __name__ == "__main__":
    main()
