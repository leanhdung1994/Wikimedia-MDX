from config import Config
import psutil
import time
import duckdb


def collect_parquet(cfg: Config, delete_ndjson=True) -> None:
    """
    Deduplicates entries within each binned ndjson file and exports to Parquet.

    Why deduplication is needed:
      The Wikimedia dump may contain multiple versions of the same article (identified
      by 'identifier'). We keep only the most recently modified version using a
      window function that partitions by identifier and orders by dateModified DESC.

    Why binned files:
      Entries were fanned out to n_bins files by (identifier % n_bins) during
      parallel processing. Because identical identifiers always land in the same bin,
      deduplication is correct within a single bin without cross-bin joins.

    DuckDB is used here for its ability to read ndjson directly, execute SQL window
    functions, and write compressed Parquet — all without loading everything into RAM.
    Memory and temp-dir limits are set conservatively to avoid OOM on large dumps.

    After export, the source ndjson bin is deleted to reclaim disk space.
    """
    if cfg.dont_do_it:
        return

    print()
    print("===== Export HTMLs in each bin to PARQUET =====")
    print(
        'We group by "identifier" and choose the json with most recent "dateModified"'
    )
    print()

    cfg.temp_directory.mkdir(parents=True, exist_ok=True)
    mem = psutil.virtual_memory()
    available_mem = round(
        0.8 * mem.available / (1024**3)
    )  # Use at most 80% of free RAM
    config = {
        "threads": cfg.n_cores,
        "memory_limit": f"{available_mem}GB",
        "max_temp_directory_size": "50GB",
        "temp_directory": str(cfg.temp_directory),
        "preserve_insertion_order": "false",  # Allows DuckDB to optimise sort freely
    }
    con = duckdb.connect(config=config)

    start = time.perf_counter()
    for i in range(cfg.n_bins):
        input_ndjson_path = cfg.ndjson_binned_paths[i]
        output_parquet_path = cfg.parquet_binned_paths[i]

        # Window function: rank rows per identifier by recency; keep only rank 1.
        parquet_export_query = f"""
                                COPY (
                                    SELECT html, modules
                                    FROM (
                                        SELECT
                                            html, identifier, modules,
                                            row_number() OVER (
                                                PARTITION BY identifier
                                                ORDER BY dateModified DESC
                                            ) AS rn
                                        FROM read_ndjson('{input_ndjson_path}', columns={{"identifier": 'BIGINT', "dateModified": 'TIMESTAMPTZ', "html": 'VARCHAR', "modules": 'STRUCT(base_url VARCHAR, module_url VARCHAR)'}})
                                    )
                                    WHERE rn = 1
                                    ) TO "{output_parquet_path}" (FORMAT PARQUET, COMPRESSION 'ZSTD');
                                """
        con.sql(parquet_export_query)
        print(f"HTMLs within bin {i} have been combined to {output_parquet_path}")

        if delete_ndjson and input_ndjson_path.is_file():
            input_ndjson_path.unlink(missing_ok=True)

    total = round((time.perf_counter() - start) / 60)
    print()
    print("The amount of time for deduping is", total, "minutes")
