from config import *
from ndjson_processor import *


def writer_loop(cfg: Config, queue: Queue, n_tasks: int):
    done = 0
    write_buffer_list = [[] for _ in range(cfg.n_bins)]
    write_buffer_size = [0 for _ in range(cfg.n_bins)]
    buffer_size = int(cfg.buffer_size * 1024 * 1024 / cfg.n_bins)

    with ExitStack() as stack:
        g = [
            stack.enter_context(open(path, mode="ab"))
            for path in cfg.ndjson_binned_paths
        ]
        while done < n_tasks:
            (idx, data, data_size, status) = queue.get()
            if status == "stop":
                done += 1
                continue

            write_buffer_list[idx].append(data + b"\n")
            write_buffer_size[idx] += data_size + 1

            if write_buffer_size[idx] >= buffer_size:
                tmp = b"".join(write_buffer_list[idx])
                g[idx].write(tmp)
                write_buffer_list[idx].clear()
                write_buffer_size[idx] = 0

        for idx in range(cfg.n_bins):
            tmp = b"".join(write_buffer_list[idx])
            if tmp:
                g[idx].write(tmp)


def process_parallel(cfg: Config, delete_log=False) -> None:
    if cfg.mdx_path.is_file():
        print(f"The file {cfg.mdx_path.name} already exists!")
        return

    if cfg.debug:
        print("===== This batch is running in debug mode =====")

    start = time.perf_counter()
    print()
    print("===== Process this batch in parallel =====")

    queue = Queue(maxsize=cfg.n_cores * 4)
    n_tasks = len(cfg.ndjson_names_batch)
    writer = threading.Thread(target=writer_loop, args=(cfg, queue, n_tasks))
    writer.start()

    with Pool(
        processes=cfg.n_cores, initializer=initializer, initargs=(cfg, queue)
    ) as pool:
        results = pool.imap_unordered(
            process_ndjson_worker, cfg.ndjson_names_batch, chunksize=1
        )
        for [name, run_time] in results:
            cfg.progress_log[name]["done"] = 1
            cfg.progress_log[name]["run_time"] = run_time

    writer.join()
    total = round((time.perf_counter() - start) / 60)
    cfg.progress_log["acc_run_time"] += total

    with open(cfg.progress_log_path, mode="wb") as g:
        data = orjson.dumps(cfg.progress_log)
        g.write(data)

    print()
    print("The processing of this batch is completed")
    print("The running time of this batch is", total, "minutes")

    cfg.ndjson_names_left = [
        k
        for k in list(cfg.progress_log.keys())
        if (".ndjson" in k) and (cfg.progress_log[k]["done"] == 0)
    ]

    if not cfg.ndjson_names_left:
        print()
        print(f"The entire {cfg.tar_name} has been processed")
        print(
            "The total running time of all batches is",
            cfg.progress_log["acc_run_time"],
            "minutes",
        )
        cfg.dont_do_it = False
        if delete_log:
            cfg.progress_log_path.unlink(missing_ok=True)
    else:
        print(
            "There are ndjson files remained to process, so you need to re-run the script"
        )
