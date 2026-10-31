"""Package current source, Git history and one run's report without old data."""
# The main project directory may still contain an older report. Always generate
# and package the explicitly selected run's report/config/status instead.
# Copy the existing .git history along with the current source snapshot.
# The final status distinguishes unavailable jobs from accepted HTML pages.
import argparse
import io
import zipfile
from contextlib import redirect_stdout
from pathlib import Path
from run_setup import project_owner
import crawl_status
import generate_report
import os


def package(project, run, output):
    project, run, output = Path(project).resolve(), Path(run).resolve(), Path(output).resolve()
    if output.exists():
        raise ValueError("Output already exists; choose a new ZIP filename")
    if not (run / "run_metadata.json").is_file():
        raise ValueError("Select a prepared fresh run directory")
    with project_owner(project):
        # Packaging a stopped run avoids copying a changing status snapshot.
        previous = Path.cwd()
        try:
            os.chdir(run)
            generate_report.main()
            # Report generation is offline; it never fetches pages or edits the
            # accepted corpus to make the statistics look better.
        finally:
            os.chdir(previous)
        if not (run / "report.txt").is_file():
            raise ValueError("No accepted HTML report exists for this run")
        status = io.StringIO()
        with redirect_stdout(status):
            # Include the full unavailable-budget list for an auditable final
            # snapshot, not just the shortened list shown interactively.
            crawl_status.main(run / "frontier.shelve.sqlite3", run / "crawl_stats.sqlite3", True)
        root = "spacetime-crawler4py"
        excluded = {".venv", "__pycache__", "Logs", "runs", "source-backups", ".pytest_cache"}
        output.parent.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(output, "x", zipfile.ZIP_DEFLATED) as archive:
            for path in sorted(project.rglob("*")):
                relative = path.relative_to(project)
                if not path.is_file() or path == output:
                    continue
                if any(piece in excluded for piece in relative.parts):
                    continue
                if relative.parts[0].startswith(("test-backup-", "crawl-backup-", "backup-")):
                    # Old shelf/config/report backups can otherwise leak stale
                    # answers into the submission even when SQLite is excluded.
                    continue
                if path.name.startswith(("frontier.shelve", ".crawler-process", ".active-run")):
                    continue
                if path.suffix in {".zip", ".sqlite3", ".pyc", ".log"} or ".sqlite3-" in path.name:
                    continue
                # Always use the selected run's actual report/configuration.
                if relative.as_posix() in {"report.txt", "config.ini", "final_crawl_status.txt"}:
                    continue
                archive.write(path, root + "/" + relative.as_posix())
            archive.write(run / "report.txt", root + "/report.txt")
            # Promote the selected report/config to the archive's main level;
            # the local internal run directory name is not a submitted folder.
            archive.write(run / "config.ini", root + "/config.ini")
            archive.write(run / "run_metadata.json", root + "/run_metadata.json")
            archive.writestr(root + "/final_crawl_status.txt", status.getvalue())
    print("Submission ZIP:", output)
    if not (project / ".git").exists():
        print("NOTE: No .git directory was present. Preserve your real development history if you have one.")
    return output


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--output", default="assignment2-final.zip")
    args = parser.parse_args()
    package(Path(__file__).resolve().parent, args.run_dir, args.output)
