"""Prepare separate crawl state; this command makes no network requests."""
# This helper creates config/stopwords/metadata only. It does not seed a saved
# frontier, launch workers, or count as evidence that a deployment crawl ran.
import argparse
from pathlib import Path
from run_setup import prepare_run, project_owner


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("run_dir", help="New directory, e.g. runs/corrected-01")
    parser.add_argument("--config_file", default="config.ini")
    args = parser.parse_args()
    project = Path(__file__).resolve().parent
    with project_owner(project):
        destination = prepare_run(project, args.run_dir, args.config_file)
    print("Prepared:", destination)
    print("Old frontier, statistics, reports, logs and Git history were left in place.")
    print("This command did not launch or register a crawl.")


if __name__ == "__main__":
    main()
