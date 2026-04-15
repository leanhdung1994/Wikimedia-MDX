from config import Config
from pathlib import Path
import indexed_gzip as igzip
import subprocess
import sys
import orjson
import tarfile
import time


def initial_setup(cfg: Config) -> None:
    """
    Prepares the pipeline before any parallel work begins:
      1. Verifies required executables (mdict, rapidgzip) exist on PATH.
      2. Generates a rapidgzip seek-index for the .tar.gz dump if one doesn't exist.
         This index allows multiple workers to seek to arbitrary positions in the
         compressed archive without decompressing from the start each time.
      3. Cleans up stale debug files from previous runs.
      4. Loads or initialises the progress log (a JSON dict keyed by ndjson shard name).
         The progress log is the resume mechanism: shards with "done": 1 are skipped.
      5. Slices the list of unprocessed shards into cfg.ndjson_names_batch according
         to --chunk (0 = all remaining shards).
      6. Derives the binned output paths written by parallel_processor.
    """

    # Locate platform-appropriate executables inside the current Python environment.
    scripts = "Scripts" if sys.platform == "win32" else "bin"
    suffix = ".exe" if sys.platform == "win32" else ""

    exe = Path(sys.executable).resolve()
    # If already inside Scripts/bin, use parent directly, otherwise append it
    parent = (
        exe.parent if exe.parent.name in ("Scripts", "bin") else exe.parent / scripts
    )

    cfg.mdict_exe_path = parent / f"mdict{suffix}"
    cfg.rapidgzip_exe_path = parent / f"rapidgzip{suffix}"

    # Abort the entire pipeline if either required tool is missing.
    for path in [cfg.mdict_exe_path, cfg.rapidgzip_exe_path]:
        if not path.exists():
            tmp = str(path)
            print(f"{tmp} is not found!")
            cfg.dont_do_it = True

    if cfg.dont_do_it:
        return

    # Build the rapidgzip seek-index once; subsequent runs reuse it.
    # The 128 MB chunk size balances index file size against seek granularity.
    if not cfg.index_gzip_path.exists():
        start = time.perf_counter()
        print()
        print(f"===== Generate index of {cfg.tar_path} =====")

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

    # Remove any leftover debug output files so reruns are clean.
    for d in cfg.output_dir.iterdir():
        if d.is_file() and ("debug" in d.name):
            d.unlink()

    # Load existing progress log or create a fresh one.
    if cfg.progress_log_path.exists():
        with open(cfg.progress_log_path, mode="rb") as g:
            progress_log = orjson.loads(g.read())
            ndjson_names = [k for k in list(progress_log.keys()) if ".ndjson" in k]
    else:
        # Open the .tar.gz via the seek-index to list all ndjson member names.
        with (
            igzip.IndexedGzipFile(
                cfg.tar_path, index_file=cfg.index_gzip_path
            ) as myGzip,
            tarfile.open(fileobj=myGzip, mode="r:*") as tarFile,
        ):
            ndjson_names = tarFile.getnames()
            if cfg.debug:
                ndjson_names = ndjson_names[: cfg.n_cores]  # 2 shards in debug mode
        progress_log = {}
        progress_log["acc_run_time"] = 0  # Accumulates total minutes across batches

        for name in ndjson_names:
            progress_log[name] = {"done": 0, "run_time": 0}

        with open(cfg.progress_log_path, mode="wb") as g:
            data = orjson.dumps(progress_log)
            g.write(data)

    # n_bins: number of output buckets for fan-out. 3× shard count gives fine-enough
    # granularity to spread entries evenly while keeping file count manageable.
    cfg.n_bins = int(3 * len(ndjson_names))
    cfg.progress_log = progress_log

    # Only enqueue shards that haven't been successfully processed yet.
    cfg.ndjson_names_left = [
        k
        for k in list(progress_log.keys())
        if (".ndjson" in k) and (progress_log[k]["done"] == 0)
    ]

    # Respect the --chunk limit if set; 0 means process everything remaining.
    if cfg.n_chunks > 0:
        ndjson_names_size = cfg.n_cores * cfg.n_chunks
    else:
        ndjson_names_size = len(cfg.ndjson_names_left)
    cfg.ndjson_names_batch = cfg.ndjson_names_left[:ndjson_names_size]

    # Pre-compute all binned file paths so workers and the writer share the same names.
    cfg.parquet_binned_paths = [
        cfg.output_dir / f"{cfg.prefix_lang_proj}_bin_{i}.parquet"
        for i in range(cfg.n_bins)
    ]
    cfg.ndjson_binned_paths = [
        cfg.output_dir / f"{cfg.prefix_lang_proj}_bin_{i}.ndjson"
        for i in range(cfg.n_bins)
    ]
