# ENTRY POINT: CHOOSE RUN STATE, REGISTER WITH CACHE, START WORKERS
# A --run-dir run uses the same source code but its own frontier, statistics,
# stop words, configuration and logs. Without that option paths belong to the
# current directory. A resume uses existing state; --restart means a fresh run.
import multiprocessing
import os
from pathlib import Path
from run_setup import project_owner
from configparser import ConfigParser
from argparse import ArgumentParser

from utils.config import Config
from crawler import Crawler


def main(config_file, restart):
    cparser = ConfigParser()
    if not cparser.read(config_file):
        raise ValueError("Configuration file not found: " + str(config_file))
    config = Config(cparser)
    # Validate settings before registering; accidental source/config mistakes
    # should not create a fresh cache registration or erase existing results.
    if restart and any(Path(name).exists() for name in (
        "crawl_stats.sqlite3", config.save_file, config.save_file + ".sqlite3",
        config.save_file + ".dumb.dat",
    )):
        raise ValueError(
            "Refusing to reset an existing run or mix old statistics. "
            "Prepare a separate directory with prepare_fresh.py."
        )
    from utils.server_registration import get_cache_server
    # Import registration only when really launching, not for --help or offline
    # preparation. The returned endpoint is consumed by utils/download.py.
    config.cache_server = get_cache_server(config, restart)
    crawler = Crawler(config, restart)
    crawler.start()


if __name__ == "__main__":
    if "fork" in multiprocessing.get_all_start_methods():
        # The supplied spacetime package creates a local class that caused a
        # spawn pickling error on this Mac setup. Use fork only where supported.
        # This is registration-process compatibility, not crawler multithreading.
        multiprocessing.set_start_method("fork")
    parser = ArgumentParser()
    parser.add_argument("--restart", action="store_true", default=False)
    parser.add_argument("--config_file", type=str, default="config.ini")
    parser.add_argument("--run-dir", help="Prepared run directory; old root state is not used")
    args = parser.parse_args()
    project = Path(__file__).resolve().parent
    with project_owner(project):
        # Hold one project-wide owner lock for the whole crawl. Separate run
        # directories must not accidentally allow two crawlers from this project.
        if args.run_dir:
            run = Path(args.run_dir).resolve()
            if not (run / "config.ini").is_file() or not (run / "run_metadata.json").is_file():
                raise ValueError("Prepare this new run with prepare_fresh.py first")
            # All relative files (frontier, analytics, logs) belong to this run.
            os.chdir(run)
        main(args.config_file, args.restart)
