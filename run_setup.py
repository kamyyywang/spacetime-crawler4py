"""Create an isolated fresh run without deleting or launching anything."""
# Separate data directories prevent a new report from mixing older statistics
# with a fresh frontier. Preparing files alone sends no cache registration.
import json
import shutil
from configparser import ConfigParser
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
import fcntl


def prepare_run(project, destination, config_file="config.ini"):
    project = Path(project).resolve()
    destination = Path(destination).resolve()
    if destination.exists():
        # Require an explicit new destination rather than deleting previous work.
        raise ValueError("Run directory already exists; resume it instead of overwriting it")
    parser = ConfigParser()
    if not parser.read(project / config_file):
        raise ValueError("Configuration file not found")
    # Check the actual settings before creating a run, without registering it.
    from utils.config import Config
    Config(parser)
    stopwords = project / "stopwords.txt"
    if not stopwords.is_file():
        raise ValueError("stopwords.txt is missing")
    # Relative paths in the prepared configuration now belong to this run.
    parser["LOCAL PROPERTIES"]["SAVE"] = "frontier.shelve"
    destination.mkdir(parents=True, exist_ok=False)
    try:
        with (destination / "config.ini").open("w") as stream:
            parser.write(stream)
        shutil.copy2(stopwords, destination / "stopwords.txt")
        (destination / "run_metadata.json").write_text(json.dumps({
            # This records PREPARATION time/state, not live crawl completion.
            # Final status is obtained from the frontier, not this static marker.
            "prepared_at_utc": datetime.now(timezone.utc).isoformat(),
            "state": "prepared; no requests made",
            "url_identity": "requested URL with fragment removed",
            "parent_project": str(project),
        }, indent=2) + "\n")
    except BaseException:
        # Only remove the new, task-created directory if preparation fails.
        shutil.rmtree(destination)
        raise
    return destination


@contextmanager
def project_owner(project):
    """Reject simultaneous fresh/resumed crawls, including a legacy root run."""
    project = Path(project).resolve()
    handle = (project / ".crawler-process.lock").open("a+b")
    # flock belongs to an open file descriptor, not to the mere existence of a
    # lock filename. Closing/exiting releases ownership after a stopped process.
    try:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError("Another crawler is running; do not launch a second copy")
        # Older code held only a frontier-specific lock. Check known run state
        # too, without opening, modifying or deleting any progress database.
        candidates = list(project.glob("*.sqlite3.lock"))
        if (project / "runs").is_dir():
            candidates.extend((project / "runs").rglob("*.sqlite3.lock"))
        for path in candidates:
            with path.open("a+b") as old:
                try:
                    fcntl.flock(old, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    raise RuntimeError("An existing crawler owns " + str(path))
                fcntl.flock(old, fcntl.LOCK_UN)
        yield
    finally:
        handle.close()
