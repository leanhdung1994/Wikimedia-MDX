from config import *


def initial_setup(cfg: Config) -> None:
    if not cfg.index_gzip_path.exists():
        start = time.perf_counter()
        print()
        print(f"===== Generate index of {cfg.tar_name} =====")

        command = [
            cfg.rapidgzip_exe_path,
            "--export-index",
            cfg.index_gzip_path,
            "--chunk-size",
            str(128 * 1024),
            "--decoder-parallelism",
            "4",
            cfg.tar_path,
        ]

        subprocess.run(command, check=True)

        print("Finish generating the index")
        total = round(time.perf_counter() - start)
        print("The amount of time for indexing is", total, "seconds")
        print()

    cfg.output_dir.mkdir(parents=True, exist_ok=True)
    for d in cfg.output_dir.iterdir():
        if d.is_file() and ("debug" in d.name):
            d.unlink()

    if cfg.progress_log_path.exists():
        with open(cfg.progress_log_path, mode="rb") as g:
            progress_log = orjson.loads(g.read())
            ndjson_names = [k for k in list(progress_log.keys()) if ".ndjson" in k]
    else:
        with igzip.IndexedGzipFile(
            cfg.tar_path, index_file=cfg.index_gzip_path
        ) as myGzip, tarfile.open(fileobj=myGzip, mode="r:*") as tarFile:
            ndjson_names = tarFile.getnames()
            if cfg.debug:
                ndjson_names = ndjson_names[: cfg.n_cores]
        progress_log = {}
        progress_log["acc_run_time"] = 0

        for name in ndjson_names:
            progress_log[name] = {"done": 0, "run_time": 0}

        with open(cfg.progress_log_path, mode="wb") as g:
            data = orjson.dumps(progress_log)
            g.write(data)

    cfg.n_bins = int(3 * len(ndjson_names))
    cfg.progress_log = progress_log
    cfg.ndjson_names_left = [
        k
        for k in list(progress_log.keys())
        if (".ndjson" in k) and (progress_log[k]["done"] == 0)
    ]
    if cfg.n_chunks > 0:
        ndjson_names_size = cfg.n_cores * cfg.n_chunks
    else:
        ndjson_names_size = len(cfg.ndjson_names_left)
    cfg.ndjson_names_batch = cfg.ndjson_names_left[:ndjson_names_size]
    cfg.parquet_binned_paths = [
        cfg.output_dir / f"{cfg.prefix_lang_proj}_bin_{i}.parquet"
        for i in range(cfg.n_bins)
    ]
    cfg.ndjson_binned_paths = [
        cfg.output_dir / f"{cfg.prefix_lang_proj}_bin_{i}.ndjson"
        for i in range(cfg.n_bins)
    ]
