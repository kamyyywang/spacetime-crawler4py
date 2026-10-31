"""Read saved crawl progress without stopping the crawler or making requests."""
# This operational status is NOT the assignment's accepted-page report.
# "completed" includes filtered, denied, unavailable and other terminal jobs.
# "eligible" also includes requests a running worker may currently own; it
# does not prove a worker process exists or that a download is in progress.
import argparse
import sqlite3
import time
from contextlib import closing
from datetime import datetime
from pathlib import Path


def main(frontier="frontier.shelve.sqlite3", stats="crawl_stats.sqlite3", all_failures=False):
    print("Snapshot:", datetime.now().astimezone().isoformat(timespec="seconds"))
    path = Path(frontier)
    if not path.is_file():
        print("Frontier not found:", path)
        return

    # mode=ro prevents accidentally creating a fresh, empty database.
    with closing(sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)) as db:
        now = time.time()
        total, pending = db.execute(
            "SELECT COUNT(*),COALESCE(SUM(completed=0),0) FROM urls"
        ).fetchone()
        ready, delayed = db.execute("""
            SELECT COALESCE(SUM(MAX(u.ready_at,COALESCE(h.ready_at,0))<=?),0),
                   COALESCE(SUM(MAX(u.ready_at,COALESCE(h.ready_at,0))>?),0)
            FROM urls u LEFT JOIN deferred_hosts h USING(hostname)
            WHERE u.completed=0
        """, (now, now)).fetchone()
        # Count a URL as delayed if either its own or its host's deadline is
        # still in the future. Zero eligible URLs can mean sleeping, not done.
        print("Discovered URLs:", total)
        print("Completed frontier entries:", total - pending)
        print("Unavailable frontier entries:", db.execute(
            "SELECT COUNT(*) FROM urls WHERE completed=1 AND reason LIKE 'unavailable: %'"
        ).fetchone()[0])
        if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='failure_budget'").fetchone():
            print("Exhausted failure budgets (scope, target, attempts, last error):")
            exhausted = db.execute(
                "SELECT scope,target,attempts,reason FROM failure_budget WHERE exhausted=1 ORDER BY scope,target"
            ).fetchall()
            for row in (exhausted if all_failures else exhausted[:10]):
                print(*row, sep=" | ")
            if len(exhausted) > 10 and not all_failures:
                print(f"... {len(exhausted) - 10} more; use --all-failures for the complete list")
        print("Pending URLs:", pending)
        print("Eligible pending URLs (includes active requests):", ready)
        print("Pending URLs waiting on URL/host delays:", delayed)
        print("Largest pending hosts (count, earliest eligible in seconds, hostname):")
        for host, count, next_time in db.execute("""
            SELECT u.hostname,COUNT(*),MIN(MAX(u.ready_at,COALESCE(h.ready_at,0)))
            FROM urls u LEFT JOIN deferred_hosts h USING(hostname)
            WHERE u.completed=0 GROUP BY u.hostname
            ORDER BY COUNT(*) DESC,u.hostname LIMIT 10
        """):
            print(count, max(0, int(next_time - now)), host)

    path = Path(stats)
    # This separate count is the accepted HTML corpus size used by the report.
    # Do not infer it by subtracting failed jobs from frontier completion.
    if path.is_file():
        with closing(sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)) as db:
            if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='pages'").fetchone():
                print("Recorded HTML URLs:", db.execute("SELECT COUNT(*) FROM pages").fetchone()[0])
            else:
                print("Recorded HTML URLs: 0 (pages table not created yet)")
    else:
        print("Statistics database not found:", path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--frontier", default="frontier.shelve.sqlite3")
    parser.add_argument("--stats", default="crawl_stats.sqlite3")
    parser.add_argument("--run-dir", help="Read state from an isolated run directory")
    parser.add_argument("--all-failures", action="store_true")
    args = parser.parse_args()
    base = Path(args.run_dir) if args.run_dir else Path(".")
    main(base / args.frontier, base / args.stats, args.all_failures)
