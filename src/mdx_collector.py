from config import *


def collect_mdx(cfg: Config, delete_txt=True) -> None:
    """
    Final pipeline stage: compiles the flat .txt source file into an MDX dictionary
    using the mdict CLI.

    MDX is the binary dictionary format used by MDict-compatible readers (e.g. GoldenDict,
    MDict, Eudic). The mdict CLI handles encoding, compression, and index building.

    Key mdict parameters:
      --record-size 10240  Max size in KB of a single record
      --key-size 1024      Max length in KB of a headword/key
      -a <input.txt>       Input file in MDX source format
      <output.mdx>         Output MDX file path

    After successful compilation, the intermediate .txt file is deleted to save space.
    The stage is skipped entirely if cfg.dont_do_it is True (set by an earlier failure).
    """
    if cfg.dont_do_it:
        return

    mdx_path = str(cfg.mdx_path)
    mdict_exe_path = str(cfg.mdict_exe_path)

    print()
    print("===== Generate MDX from TXTs =====")
    subprocess.run(
        [
            mdict_exe_path,
            "--record-size",
            str(10 * 1024),
            "--key-size",
            str(1024),
            "-a",
            cfg.txt_path,
            mdx_path,
        ],
        check=True,
    )

    if delete_txt and cfg.txt_path.is_file():
        cfg.txt_path.unlink(missing_ok=True)
