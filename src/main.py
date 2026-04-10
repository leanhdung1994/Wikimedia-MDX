from config import *
from initial_setup import initial_setup
from parallel_processor import process_parallel
from parquet_collector import collect_parquet
from mdx_collector import collect_mdx
from css_and_js_collector import collect_css_and_js
from txt_and_modules_collector import collect_txt_and_modules

CORE_UTILIZATION = 0.7
RESERVED_CORES = 4
cores = max(
    1, os.cpu_count() - RESERVED_CORES, round(CORE_UTILIZATION * os.cpu_count())
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

    initial_setup(cfg)
    process_parallel(cfg)
    collect_parquet(cfg)
    collect_txt_and_modules(cfg)
    collect_css_and_js(cfg)
    collect_mdx(cfg)


if __name__ == "__main__":
    main()
