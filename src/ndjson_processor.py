from config import Config
from html_processor import HtmlFactory
from multiprocessing import Queue
import indexed_gzip as igzip
import tarfile
import atexit
import time
import orjson
import traceback


class NdjsonFactory:
    """
    Per-worker object that holds open handles to the compressed archive and
    processes one ndjson shard at a time.

    Lifecycle (managed by the multiprocessing Pool initializer):
      1. init_worker()  — opens the gzip+tar handles once per process; registers
                          close_worker() with atexit so handles are cleaned up even
                          if a worker is killed abruptly.
      2. process_ndjson() — called on each shard assigned to this worker.
      3. close_worker()  — closes handles; called by atexit.
    """

    def __init__(self, cfg: Config, html_factory: HtmlFactory, queue: Queue):
        self.cfg = cfg
        self.html_factory = html_factory
        self.queue = queue

    def close_worker(self):
        """Safely close tar and gzip handles; tolerates partial initialisation."""
        try:
            if hasattr(self, "tar"):
                self.tar.close()
            if hasattr(self, "gzip"):
                self.gzip.close()
        except Exception:
            pass

    def init_worker(self):
        """
        Opens the indexed gzip + tar once per worker process.
        readbuf_size=16MB reduces system-call overhead.
        """
        self.gzip = igzip.IndexedGzipFile(
            self.cfg.tar_path,
            index_file=self.cfg.index_gzip_path,
            readbuf_size=16 * 1024 * 1024,
        )
        self.tar = tarfile.open(fileobj=self.gzip, mode="r:*")
        atexit.register(self.close_worker)

    def process_ndjson(self, ndjson_name: str) -> list[str, int]:
        """
        Reads every JSON line from one ndjson shard, parses the HTML, and sends
        results to the writer thread via the queue.

        Each successfully processed entry is serialised with orjson and sent as:
            (bin_index, data_bytes, byte_len, "continue")
        A "stop" sentinel is sent once the shard is exhausted (even on failure)
        so the writer thread can track completion correctly.

        Failed lines are collected and written to a separate 'failed_*' file for
        later inspection.

        Returns [ndjson_name, elapsed_minutes] for progress-log updates in the parent.
        """
        start_time = time.perf_counter()
        failed_jsons = []
        member = self.tar.getmember(ndjson_name)
        data_file = self.tar.extractfile(member)

        for i, one_line in enumerate(data_file):
            if self.cfg.debug and (i > self.cfg.n_lines):
                break  # Stop early in debug mode

            json = orjson.loads(one_line)

            try:
                tmp = self.html_factory.process_json(json)
                if not tmp:
                    continue  # Entry filtered out (wrong language section, bad chars, etc.)
                else:
                    [bin, data] = tmp
                data = orjson.dumps(data)
                self.queue.put((bin, data, len(data), "continue"))

            except Exception:
                traceback.print_exc()
                failed_jsons.append(one_line)
                continue

        # Always send stop sentinel so the writer knows this shard is done.
        self.queue.put((None, None, None, "stop"))

        # Persist failed lines for debugging without blocking the pipeline.
        if failed_jsons:
            failed_ndjson_name = f"failed_{self.cfg.mode_prefix}_{ndjson_name}"
            failed_ndjson_path = self.cfg.output_dir / failed_ndjson_name
            with open(failed_ndjson_path, mode="wb") as g:
                for one_line in failed_jsons:
                    g.write(one_line + b"\n")

        total = round((time.perf_counter() - start_time) / 60)
        print(ndjson_name, "has been processed in", total, "minutes")
        return [ndjson_name, total]


def initializer(cfg: Config, queue: Queue):
    """
    Pool initializer: runs once per worker process before any tasks are dispatched.
    Stores the NdjsonFactory in a module-level global so process_ndjson_worker
    can call it without re-opening the archive for every shard.
    """
    global worker_processor
    html_factory = HtmlFactory(cfg)
    p = NdjsonFactory(cfg, html_factory, queue)
    p.init_worker()
    worker_processor = p


def process_ndjson_worker(ndjson_name: str):
    """Top-level function called by pool.imap_unordered (must be picklable)."""
    return worker_processor.process_ndjson(ndjson_name)
