from config import Config
import psutil
import time
import duckdb
import shutil


def collect_txt_and_modules(cfg: Config, delete_parquet=True) -> None:
    """
    Merges all binned parquet files into two final outputs:

    1. <lang_proj>.txt — flat text file in MDX source format.
       DuckDB reads all bin_*.parquet files via a glob and writes them as CSV
       with no header, no quoting, and newline delimiters — exactly the format
       expected by the mdict CLI in the next stage.

    2. <lang_proj>_modules.parquet — (base_url, module_url) pairs.
       Used by css_and_js_collector to reconstruct the full list of Wikimedia
       CSS modules that need to be downloaded.

    The DuckDB temp directory is cleaned up after export, and the per-bin
    parquet files are deleted to free disk space.
    """
    if cfg.dont_do_it:
        return

    print()
    print("===== Export HTMLs in all bins to TXT =====")

    cfg.temp_directory.mkdir(parents=True, exist_ok=True)
    mem = psutil.virtual_memory()
    available_mem = round(0.8 * mem.available / (1024**3))
    config = {
        "threads": cfg.n_cores,
        "memory_limit": f"{available_mem}GB",
        "max_temp_directory_size": "50GB",
        "temp_directory": str(cfg.temp_directory),
        "preserve_insertion_order": "false",
    }
    con = duckdb.connect(config=config)

    start = time.perf_counter()
    txt_path = cfg.txt_path
    input_parquet_names = f"{cfg.prefix_lang_proj}_bin_*.parquet"

    # Export all HTML entries as a flat CSV-like text file (no quoting, newline-delimited).
    txt_export_query = f"""
                            COPY (
                                SELECT html
                                FROM read_parquet('{cfg.output_dir / input_parquet_names}')
                                ) TO "{txt_path}"
                                (
                                    FORMAT 'csv',
                                    HEADER false,
                                    DELIMITER '\n',
                                    QUOTE ''
                                );
                            """
    con.sql(txt_export_query)
    print(f"HTMLs have been exported to {txt_path}")

    # Export module URL metadata for CSS/JS collection in the next stage.
    modules_export_query = f"""
                            COPY (
                                SELECT modules.base_url, modules.module_url
                                FROM read_parquet('{cfg.output_dir / input_parquet_names}')
                                ) TO "{cfg.modules_path}";
                            """
    con.sql(modules_export_query)
    print(f"Modules have been combined to {cfg.modules_path}")

    shutil.rmtree(cfg.temp_directory)  # Remove DuckDB spill directory

    # Delete per-bin parquet files now that they've been merged.
    for path in cfg.output_dir.glob(input_parquet_names):
        if delete_parquet and path.is_file():
            path.unlink(missing_ok=True)

    total = round((time.perf_counter() - start) / 60)
    print()
    print("The amount of time for exporting is", total, "minutes")
