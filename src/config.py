from pathlib import Path
from dataclasses import dataclass


@dataclass()
class Config:
    """
    Central configuration object passed to every pipeline stage.
    Constructed once in main(), then mutated by
    initial_setup() to attach runtime state (file lists, paths, etc.).
    """

    project_code: str  # "wiki" or "wiktionary"
    language_code: str  # "en", "fr", ...
    parse_mode: str  # "greedy", etc.
    debug: bool  # Limits run to 2 ndjson files × 1000 lines for fast validation
    n_cores: int  # Number of workers for parallel HTML processing
    n_chunks: int  # Batching: 0 = process all remaining ndjson shards at once
    buffer_size: int  # Max MB buffered in RAM before flushing ndjson bins to disk
    input_dir: Path  # Directory containing the .tar.gz dump
    output_dir: Path  # Directory for all intermediate and final output files

    @property
    def mode_prefix(self):
        """Prepends 'debug_' to output filenames when running in debug mode."""
        if self.debug:
            return "debug_"
        else:
            return ""

    @property
    def language(self):
        """Full language name used as the section-heading selector in wiktionary HTML."""
        if self.language_code == "en":
            return "English"
        elif self.language_code == "fr":
            return "Français"

    def __post_init__(self):
        self.dont_do_it = False  # Set True by any stage to abort subsequent stages
        if self.debug:
            self.n_cores = 2

    @property
    def lang_proj(self) -> str:
        """Short identifier, e.g. 'enwiki' or 'frwiktionary'."""
        return f"{self.language_code}{self.project_code}"

    @property
    def prefix_lang_proj(self) -> str:
        """lang_proj with optional debug prefix; used in most output filenames."""
        return f"{self.mode_prefix}{self.lang_proj}"

    # --- Input paths ---

    @property
    def tar_path(self) -> Path:
        """Expected dump filename, e.g. 'enwiki_namespace_0.tar.gz'."""
        return self.input_dir / f"{self.lang_proj}_namespace_0.tar.gz"

    @property
    def index_gzip_path(self) -> Path:
        """rapidgzip seek-index file; generated once by initial_setup and reused."""
        return self.input_dir / f"{self.lang_proj}_namespace_0.gz.gzindex"

    # --- Output paths ---

    @property
    def progress_log_path(self) -> Path:
        """JSON file tracking which ndjson shards are done — enables resumability."""
        return self.output_dir / f"{self.prefix_lang_proj}_log.json"

    @property
    def txt_path(self):
        """Flat text file consumed by the mdict CLI to build the final MDX."""
        tmp = self.output_dir / f"{self.prefix_lang_proj}.txt"
        return tmp

    @property
    def modules_path(self):
        """Parquet of (base_url, module_url) pairs used to reconstruct CSS URLs."""
        tmp = self.output_dir / f"{self.prefix_lang_proj}_modules.parquet"
        return tmp

    @property
    def csslink_path(self):
        """Optional debug output listing every CSS URL fetched from Wikimedia."""
        path = self.output_dir / f"{self.prefix_lang_proj}_csslink.txt"
        return path

    @property
    def css_path(self):
        """Combined CSS (Wikimedia modules + local overrides)."""
        path = self.output_dir / f"{self.prefix_lang_proj}.css"
        return path

    @property
    def js_path(self):
        """Combined JS (section-hiding logic + local scripts)."""
        path = self.output_dir / f"{self.prefix_lang_proj}.js"
        return path

    @property
    def temp_directory(self):
        """DuckDB spill-to-disk scratch directory used during dedup/export queries."""
        return self.output_dir / "duckdb_tmp"

    @property
    def mdx_path(self):
        """Final MDX dictionary file."""
        return self.output_dir / f"{self.prefix_lang_proj}.mdx"

    @property
    def n_lines(self):
        """Max lines read per ndjson shard in debug mode."""
        return 1_000
