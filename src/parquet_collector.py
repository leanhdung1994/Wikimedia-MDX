from config import *


def collect_parquet(cfg: Config, delete_ndjson=True) -> None:
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
    for i in range(cfg.n_bins):
        input_ndjson_path = cfg.ndjson_binned_paths[i]
        output_parquet_path = cfg.parquet_binned_paths[i]
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
