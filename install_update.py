"""Install this source update into an existing project, preserving crawl data."""
# Reversible source installation is separate from starting/restarting a crawl.
# Back up overwritten files, preserve actual group/cache identification and
# leave the existing run state and real Git history with the target project.
import argparse
import io
import shutil
from configparser import ConfigParser
from datetime import datetime
from pathlib import Path
from run_setup import project_owner

EXCLUDED = {"__pycache__", "Logs", ".git", ".venv", "runs", "source-backups", ".crawler-process.lock"}
IGNORE_BLOCK = "\n# Local crawl state and source backups\n.venv/\nLogs/\nruns/\nsource-backups/\n*.sqlite3*\nfrontier.shelve*\n.crawler-process.lock\n__pycache__/\n"


def install(source, target):
    source, target = Path(source).resolve(), Path(target).resolve()
    if source == target:
        raise ValueError("Extract the update separately, then specify the existing project")
    if not (target / "launch.py").is_file():
        raise ValueError("Target must be your existing spacetime-crawler4py directory")
    # Preserve group identification and the assigned registration endpoint.
    new = ConfigParser()
    new.read(source / "config.ini")
    old = ConfigParser()
    old.read(target / "config.ini")
    for section, keys in (("IDENTIFICATION", ("USERAGENT",)), ("CONNECTION", ("HOST", "PORT"))):
        for key in keys:
            if old.has_option(section, key):
                new[section][key] = old[section][key]
    from utils.config import Config
    Config(new)  # Validate before overwriting any source.
    buffer = io.StringIO()
    new.write(buffer)
    files = [p for p in source.rglob("*") if p.is_file()
             and not any(piece in EXCLUDED for piece in p.relative_to(source).parts)
             and not p.name.endswith((".sqlite3", ".log", ".zip", ".pyc"))]
    with project_owner(target):
        # Refuse updates while a crawler owns this project. Keep earlier source
        # files for recovery while preserving the target's existing Git history.
        backup = target / "source-backups" / datetime.now().strftime("before-update-%Y%m%d-%H%M%S-%f")
        backup.mkdir(parents=True, exist_ok=False)
        for path in files:
            relative = path.relative_to(source)
            destination = target / relative
            destination.resolve().relative_to(target)  # Reject escaping symlinks.
            if destination.is_file():
                saved = backup / relative
                saved.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(destination, saved)
            destination.parent.mkdir(parents=True, exist_ok=True)
            if relative == Path("config.ini"):
                destination.write_text(buffer.getvalue())
            elif relative == Path(".gitignore") and destination.exists():
                existing = destination.read_text()
                if "# Local crawl state and source backups" not in existing:
                    destination.write_text(existing + IGNORE_BLOCK)
            else:
                shutil.copy2(path, destination)
    print("Updated source:", target)
    print("Previous source backup:", backup)
    print("Existing frontier, statistics, reports, logs, run directories and Git history preserved.")
    return backup


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("project", help="Existing spacetime-crawler4py directory")
    args = parser.parse_args()
    install(Path(__file__).resolve().parent, args.project)
