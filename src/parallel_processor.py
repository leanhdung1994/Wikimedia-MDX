from config import *
from ndjson_processor import *


def writer_loop(cfg: Config, queue: Queue, n_tasks: int):
    """
    Dedicated writer thread that drains the shared queue produced by worker processes.

    A single writer thread serialises all I/O while workers run CPU-bound HTML parsing
    in separate processes.

    Buffering strategy:
      - Maintains one in-memory list and a byte-size counter per bin.
      - Flushes a bin to disk only when its buffer exceeds (buffer_size / n_bins).
      - This amortises syscall overhead while bounding peak RAM usage to ~buffer_size MB.

    Termination: each worker sends a "stop" sentinel when its ndjson shard is done.
    The loop exits once it has received n_tasks sentinels (one per shard).
    """
    done = 0
    write_buffer_list = [[] for _ in range(cfg.n_bins)]
    write_buffer_size = [0 for _ in range(cfg.n_bins)]
    # Per-bin flush threshold in bytes
    buffer_size = int(cfg.buffer_size * 1024 * 1024 / cfg.n_bins)

    with ExitStack() as stack:
        # Open all bin files once in append-binary mode; ExitStack closes them on exit.
        g = [
            stack.enter_context(open(path, mode="ab"))
            for path in cfg.ndjson_binned_paths
        ]
        while done < n_tasks:
            (idx, data, data_size, status) = queue.get()
            if status == "stop":
                done += 1
                continue

            # Accumulate bytes for this bin
            write_buffer_list[idx].append(data + b"\n")
            write_buffer_size[idx] += data_size + 1

            # Flush this bin if its buffer is full
            if write_buffer_size[idx] >= buffer_size:
                tmp = b"".join(write_buffer_list[idx])
                g[idx].write(tmp)
                write_buffer_list[idx].clear()
                write_buffer_size[idx] = 0

        # Final flush: drain any remaining buffered data for every bin
        for idx in range(cfg.n_bins):
            tmp = b"".join(write_buffer_list[idx])
            if tmp:
                g[idx].write(tmp)


def process_parallel(cfg: Config, delete_log=False) -> None:
    """
    Orchestrates parallel parsing of all ndjson shards in cfg.ndjson_names_batch.

    Architecture:
      - One writer thread (writer_loop) owns all file I/O.
      - A multiprocessing Pool of cfg.n_cores workers parses HTML.
      - Workers communicate results to the writer via a bounded Queue
        (maxsize = n_cores * 4 provides backpressure so fast workers don't OOM).
      - pool.imap_unordered processes shards one at a time per worker (chunksize=1)
        because each shard is large enough that per-task overhead is negligible.

    Progress persistence:
      After the pool finishes, the progress log is updated and saved. If all shards
      are done, the pipeline continues; otherwise cfg.dont_do_it is set so later
      stages are skipped until the script is re-run.
    """
    # Skip if the final MDX already exists (full pipeline was completed previously).
    if cfg.mdx_path.is_file():
        print(f"The file {cfg.mdx_path.name} already exists!")
        cfg.dont_do_it = True

    if cfg.dont_do_it:
        return

    if cfg.debug:
        print("===== This batch is running in debug mode =====")

    start = time.perf_counter()
    print()
    print("===== Process this batch in parallel =====")

    queue = Queue(maxsize=cfg.n_cores * 4)
    n_tasks = len(cfg.ndjson_names_batch)

    # Start the writer thread before the pool so it's ready to drain immediately.
    writer = threading.Thread(target=writer_loop, args=(cfg, queue, n_tasks))
    writer.start()

    with Pool(
        processes=cfg.n_cores, initializer=initializer, initargs=(cfg, queue)
    ) as pool:
        # initializer runs once per worker process: opens the gzip/tar and caches
        # HtmlFactory so they're reused across multiple shard assignments.
        results = pool.imap_unordered(
            process_ndjson_worker, cfg.ndjson_names_batch, chunksize=1
        )
        for [name, run_time] in results:
            cfg.progress_log[name]["done"] = 1
            cfg.progress_log[name]["run_time"] = run_time

    writer.join()  # Wait for all queued data to be flushed before continuing
    total = round((time.perf_counter() - start) / 60)
    cfg.progress_log["acc_run_time"] += total

    # Persist updated progress log so the run can be resumed later.
    with open(cfg.progress_log_path, mode="wb") as g:
        data = orjson.dumps(cfg.progress_log)
        g.write(data)

    print()
    print("The processing of this batch is completed")
    print("The running time of this batch is", total, "minutes")

    # Recompute which shards are still pending after this batch.
    cfg.ndjson_names_left = [
        k
        for k in list(cfg.progress_log.keys())
        if (".ndjson" in k) and (cfg.progress_log[k]["done"] == 0)
    ]

    if not cfg.ndjson_names_left:
        print()
        print(f"The entire {cfg.tar_path} has been processed")
        print(
            "The total running time of all batches is",
            cfg.progress_log["acc_run_time"],
            "minutes",
        )
        if delete_log:
            cfg.progress_log_path.unlink(missing_ok=True)
    else:
        # More shards remain; signal downstream stages to skip until re-run.
        print(
            "There are ndjson files remained to process, so you need to re-run the script"
        )
        cfg.dont_do_it = True
