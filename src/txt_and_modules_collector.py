from config import *


def collect_txt_and_modules(cfg: Config, delete_parquet=True) -> None:
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
    modules_export_query = f"""
                            COPY (
                                SELECT modules.base_url, modules.module_url
                                FROM read_parquet('{cfg.output_dir / input_parquet_names}')
                                ) TO "{cfg.modules_path}";
                            """
    con.sql(modules_export_query)
    print(f"Modules have been combined to {cfg.modules_path}")

    shutil.rmtree(cfg.temp_directory)
    for path in cfg.output_dir.glob(input_parquet_names):
        if delete_parquet and path.is_file():
            path.unlink(missing_ok=True)

    total = round((time.perf_counter() - start) / 60)
    print()
    print("The amount of time for exporting is", total, "minutes")
