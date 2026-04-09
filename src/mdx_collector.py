from config import *


def collect_mdx(cfg: Config, delete_txt=True) -> None:
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
