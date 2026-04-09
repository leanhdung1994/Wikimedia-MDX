from config import *
from html_processor import HtmlFactory


class NdjsonFactory:
    def __init__(self, cfg: Config, html_factory: HtmlFactory, queue: Queue):
        self.cfg = cfg
        self.html_factory = html_factory
        self.queue = queue

    def close_worker(self):
        try:
            if hasattr(self, "tar"):
                self.tar.close()
            if hasattr(self, "gzip"):
                self.gzip.close()
        except Exception:
            pass

    def init_worker(self):
        self.gzip = igzip.IndexedGzipFile(
            self.cfg.tar_path,
            index_file=self.cfg.index_gzip_path,
            readbuf_size=16 * 1024 * 1024,
        )
        self.tar = tarfile.open(fileobj=self.gzip, mode="r:*")
        atexit.register(self.close_worker)

    def process_ndjson(self, ndjson_name: str) -> list[str, int]:
        start_time = time.perf_counter()
        failed_jsons = []
        member = self.tar.getmember(ndjson_name)
        data_file = self.tar.extractfile(member)

        for i, one_line in enumerate(data_file):
            if self.cfg.debug and (i > self.cfg.n_lines):
                break
            json = orjson.loads(one_line)

            try:
                tmp = self.html_factory.process_json(json)
                if not tmp:
                    continue
                else:
                    [bin, data] = tmp
                data = orjson.dumps(data)
                self.queue.put((bin, data, len(data), "continue"))

            except Exception:
                traceback.print_exc()
                failed_jsons.append(one_line)
                continue

        self.queue.put((None, None, None, "stop"))

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
    global worker_processor
    html_factory = HtmlFactory(cfg)
    p = NdjsonFactory(cfg, html_factory, queue)
    p.init_worker()
    worker_processor = p


def process_ndjson_worker(ndjson_name: str):
    return worker_processor.process_ndjson(ndjson_name)
