from multiprocessing import Pool
from pathlib import Path
from selectolax.lexbor import LexborHTMLParser
import indexed_gzip as igzip
from contextlib import ExitStack
from itertools import chain
from dataclasses import dataclass
from urllib.parse import urljoin
from multiprocessing import Queue
import json, tarfile, time, string, orjson, traceback, requests, rapidgzip, os, subprocess, argparse, shutil, duckdb, atexit, psutil, threading


@dataclass()
class Config:
    project_code: str  # "wiki" or "wiktionary"
    language_code: str  # "en", "fr", ...
    parse_mode: str  # "greedy", etc.
    debug: bool
    n_cores: int
    n_chunks: int
    buffer_size: int
    input_dir: Path
    output_dir: Path
    rapidgzip_exe_path: Path
    mdict_exe_path: Path

    @property
    def mode_prefix(self):
        if self.debug:
            return "debug_"
        else:
            return ""

    @property
    def language(self):
        if self.language_code == "en":
            return "English"
        elif self.language_code == "fr":
            return "Français"

    def __post_init__(self):
        self.dont_do_it = True
        if self.debug:
            self.n_cores = 2

    @property
    def lang_proj(self) -> str:
        return f"{self.language_code}{self.project_code}"

    @property
    def prefix_lang_proj(self) -> str:
        return f"{self.mode_prefix}{self.lang_proj}"

    @property
    def tar_name(self) -> str:
        return f"{self.lang_proj}_namespace_0.tar.gz"

    @property
    def tar_path(self) -> Path:
        return self.input_dir / self.tar_name

    @property
    def index_gzip_name(self) -> str:
        return f"{self.lang_proj}_namespace_0.gz.gzindex"

    @property
    def index_gzip_path(self) -> Path:
        return self.input_dir / self.index_gzip_name

    @property
    def progress_log_name(self) -> str:
        return f"{self.prefix_lang_proj}_log.json"

    @property
    def progress_log_path(self) -> Path:
        return self.output_dir / self.progress_log_name

    @property
    def txt_path(self):
        tmp = self.output_dir / f"{self.prefix_lang_proj}.txt"
        return tmp

    @property
    def modules_path(self):
        tmp = self.output_dir / f"{self.prefix_lang_proj}_modules.parquet"
        return tmp

    @property
    def csslink_path(self):
        path = self.output_dir / f"{self.prefix_lang_proj}_csslink.txt"
        return path

    @property
    def css_path(self):
        path = self.output_dir / f"{self.prefix_lang_proj}.css"
        return path

    @property
    def js_path(self):
        path = self.output_dir / f"{self.prefix_lang_proj}.js"
        return path

    @property
    def temp_directory(self):
        return self.output_dir / "duckdb_tmp"

    @property
    def mdx_path(self):
        return self.output_dir / f"{self.prefix_lang_proj}.mdx"

    @property
    def n_lines(self):
        return 1_000
