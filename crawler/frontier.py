# PERSISTENT FRONTIER: URL DISCOVERY, WORK CLAIMS, AND RETRY STATE
# urls records discovered URLs and terminal/pending state. Accepted HTML and
# word statistics live separately in crawl_stats.sqlite3 (analytics.py).
#
# Concurrency has two levels: flock rejects another crawler PROCESS using this
# database; the Condition/RLock serializes THREADS inside this one crawler.
# active claims are deliberately in memory. A process crash therefore leaves
# unfinished rows pending on disk, available again when the crawler resumes.
import dbm.dumb
import fcntl
import os
import shelve
import sqlite3
import threading
import time
from pathlib import Path
from urllib.parse import urldefrag, urlsplit

from requests.utils import requote_uri
from scraper import is_valid, prepare_wiki_link
from utils import get_logger
from crawler.runtime import SharedRuntime, domain_key


class Frontier:
    def __init__(self, config, restart):
        self.logger = get_logger("FRONTIER")
        self.config = config
        self.runtime = SharedRuntime(config)
        # Every Worker receives this same frontier/runtime, not a private lock
        # or scheduler. Separate locks per worker would not enforce politeness.
        self.condition = threading.Condition(threading.RLock())
        self.active = set()
        self.active_domains = set()
        # active_domains uses the assignment's four domain families. Holding
        # one job per family is conservative; the request scheduler independently
        # enforces the actual timing of pages, robots, sitemaps and retries.
        self.closed = False
        self.path = str(config.save_file) + ".sqlite3"
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self._lock_file = open(self.path + ".lock", "a+b")
        try:
            fcntl.flock(self._lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            self._lock_file.close()
            raise RuntimeError("Another crawler owns this frontier; stop it first")
        # All connection operations after initialization hold the condition lock.
        self.db = sqlite3.connect(self.path, check_same_thread=False, timeout=30)
        # check_same_thread=False only permits access from workers; the lock is
        # what makes that access safe. WAL supports readers alongside writes;
        # FULL asks SQLite for durable commits, subject to filesystem guarantees.
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS urls (
                url TEXT PRIMARY KEY,
                domain TEXT NOT NULL,
                hostname TEXT NOT NULL,
                completed INTEGER NOT NULL DEFAULT 0,
                ready_at REAL NOT NULL DEFAULT 0,
                reason TEXT NOT NULL DEFAULT ''
            );
            CREATE INDEX IF NOT EXISTS urls_pending ON urls(completed, ready_at);
            CREATE TABLE IF NOT EXISTS deferred_hosts (
                hostname TEXT PRIMARY KEY, ready_at REAL NOT NULL, reason TEXT
            );
            CREATE TABLE IF NOT EXISTS failure_budget (
                scope TEXT NOT NULL, target TEXT NOT NULL,
                attempts INTEGER NOT NULL, first_at REAL NOT NULL,
                last_at REAL NOT NULL, reason TEXT NOT NULL,
                exhausted INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY(scope,target)
            );
            CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT);
        """)
        # ready_at values are Unix timestamps, so delays survive a process exit.
        # In-memory request intervals use monotonic time instead (runtime.py).
        # failure_budget persists attempts; relaunching cannot give a dead URL
        # an unlimited supply of "first attempts".
        try:
            with self.db:
                if restart:
                    self.db.execute("DELETE FROM urls")
                    self.db.execute("DELETE FROM deferred_hosts")
                    self.db.execute("DELETE FROM failure_budget")
                initialized = self.db.execute(
                    "SELECT 1 FROM metadata WHERE key='initialized'"
                ).fetchone()
                if not initialized and not restart:
                    # Import an older shelf once. A fresh run deliberately does
                    # not inherit URLs, completion flags or retries from it.
                    self._import_legacy()
                if not initialized or restart:
                    for url in config.seed_urls:
                        self._insert(url)
                self.db.execute(
                    "INSERT OR REPLACE INTO metadata VALUES ('initialized', '1')"
                )
        except BaseException:
            self.close()
            raise
        total, pending = self.db.execute(
            "SELECT COUNT(*), COALESCE(SUM(completed=0),0) FROM urls"
        ).fetchone()
        self.logger.info("Loaded %s pending URLs from %s discovered URLs", pending, total)

    def _import_legacy(self):
        """Copy the old shelf once; never modify or delete its files."""
        base = str(self.config.save_file)
        if os.path.exists(base + ".dumb.dat"):
            old = shelve.Shelf(dbm.dumb.open(base + ".dumb", "r"))
        elif os.path.exists(base):
            old = shelve.open(base, flag="r")
        else:
            return
        count = 0
        with old:
            for url, completed in old.values():
                url = urldefrag(url)[0]
                parts = urlsplit(url)
                self.db.execute(
                    "INSERT OR IGNORE INTO urls(url,domain,hostname,completed) "
                    "VALUES (?,?,?,?)",
                    (url, domain_key(url), parts.hostname or "", int(completed)),
                )
                count += 1
        self.logger.info("Copied %s legacy records; original shelf preserved", count)

    def _insert(self, url):
        # Preserve schemes and trailing slashes: only fragments are discarded.
        try:
            url = requote_uri(urldefrag(url.strip())[0])
            if not is_valid(url):
                return False
            parts = urlsplit(url)
        except (ValueError, TypeError, AttributeError):
            return False
        result = self.db.execute(
            "INSERT OR IGNORE INTO urls(url,domain,hostname) VALUES (?,?,?)",
            (url, domain_key(url), parts.hostname),
        )
        added = result.rowcount == 1
        # SQL's primary key is the final arbiter of duplicate insertion. A URL
        # linked by many pages still occupies a single frontier row.
        if added:
            failed = self.db.execute(
                "SELECT reason FROM failure_budget WHERE scope='host' AND target=? AND exhausted=1",
                (parts.hostname,),
            ).fetchone()
            if failed:
                # A new link must not revive a host whose bounded retry budget
                # is already exhausted. Preserve the URL with an honest reason.
                self.db.execute(
                    "UPDATE urls SET completed=1,reason=? WHERE url=?",
                    ("unavailable: " + failed[0], url),
                )
        return added

    def add_url(self, url):
        # Commit before notifying idle workers, so a woken worker sees the row.
        with self.condition:
            with self.db:
                added = self._insert(url)
            if added:
                self.condition.notify_all()
            return added

    def get_tbd_url(self):
        # Claim a ready job under the same lock that protects active claims.
        # This method waits when work is active/deferred; None means there is
        # no remaining discoverable frontier work, or shutdown was requested.
        with self.condition:
            while not self.runtime.stop_event.is_set():
                rows = self.db.execute("""
                    SELECT u.url,u.domain,MAX(u.ready_at,COALESCE(h.ready_at,0))
                    FROM urls u LEFT JOIN deferred_hosts h USING(hostname)
                    WHERE u.completed=0
                    ORDER BY CASE WHEN u.reason='' THEN 0 ELSE 1 END,
                             u.ready_at,u.rowid
                """).fetchall()
                # A job is ready only after BOTH its own and its host's delay.
                # Prefer never-attempted jobs so failing URLs do not repeatedly
                # consume the opportunity to discover healthy pages.
                waiting_until = None
                filtered = []
                replacements = set()
                now = time.time()
                for url, domain, ready_at in rows:
                    if url in self.active:
                        continue
                    if not is_valid(url):
                        # Filtering on resume matters: a rule added after URL
                        # discovery must also apply to the already-saved queue.
                        # Old saved action links are filtered without fetching.
                        # Preserve namespace-index discovery at one shared URL.
                        replacement = prepare_wiki_link(url)
                        if replacement != url and is_valid(replacement):
                            replacements.add(replacement)
                        filtered.append((url,))
                        continue
                    if domain in self.active_domains:
                        continue
                    if ready_at > now:
                        waiting_until = min(waiting_until or ready_at, ready_at)
                        continue
                    # Claim before unlocking: no other worker can take this URL.
                    self.active.add(url)
                    self.active_domains.add(domain)
                    if filtered:
                        self._filter_pending(filtered, replacements)
                    return url
                if filtered:
                    self._filter_pending(filtered, replacements)
                    continue
                # Other active workers may discover new links: wait for them.
                if not self.active and waiting_until is None:
                    return None
                # Notifications wake workers immediately for new/completed
                # work. Avoid repeatedly scanning a quiet deferred queue.
                timeout = 60.0
                if waiting_until is not None:
                    timeout = min(timeout, max(0.01, waiting_until - now))
                self.condition.wait(timeout)
                # Condition.wait releases the lock while asleep; another worker
                # can insert links or finish a job and wake this worker promptly.
        return None

    def _filter_pending(self, filtered, replacements):
        """Called under the condition lock; preserve old rows and crawl counts."""
        # Terminal "filtered" rows remain audit evidence; they are not inserted
        # into the accepted-page statistics used to answer the report questions.
        with self.db:
            self.db.executemany(
                "UPDATE urls SET completed=1,reason='filtered' WHERE url=?",
                filtered,
            )
            for url in sorted(replacements):
                self._insert(url)
        self.condition.notify_all()

    def mark_url_complete(self, url):
        # Worker calls this only after processing and saving discovered links.
        # completed means the frontier job is resolved, not that it was HTML.
        with self.condition:
            with self.db:
                self.db.execute(
                    "UPDATE urls SET completed=1,reason='' WHERE url=?", (url,)
                )
            self._release(url)

    def release_url(self, url, delay=0, reason=""):
        """Leave failures pending, including across process exits."""
        with self.condition:
            with self.db:
                self.db.execute(
                    "UPDATE urls SET ready_at=?,reason=? WHERE url=? AND completed=0",
                    (time.time() + delay, reason, url),
                )
            self._release(url)

    def _release(self, url):
        # Release both the URL claim and family job slot, including on failure.
        # Otherwise one abandoned claim could leave other workers waiting forever.
        self.active.discard(url)
        self.active_domains.discard(domain_key(url))
        self.condition.notify_all()

    def defer_host(self, hostname, seconds, reason):
        # MAX preserves an existing longer deadline. A later, shorter failure
        # must not accidentally cancel a previously scheduled host cooldown.
        with self.condition:
            with self.db:
                self.db.execute("""
                    INSERT INTO deferred_hosts VALUES (?,?,?)
                    ON CONFLICT(hostname) DO UPDATE SET
                      ready_at=MAX(ready_at,excluded.ready_at),reason=excluded.reason
                """, (hostname, time.time() + seconds, reason))
            self.condition.notify_all()

    def note_failure(self, url, error):
        """Persist one failed scheduling attempt, with a four-attempt budget.

        Three configured local deferrals precede exhaustion. The fresh-run
        configuration uses 5 minutes, 30 minutes and 2 hours. Only known origin/robots failures reach here; inability
        to contact the course cache stays inside the worker's outage loop.
        Return None after exhaustion, otherwise the next delay in seconds.
        """
        scope = "host" if error.host_wide else "url"
        # Scope determines what becomes unavailable at exhaustion: one URL or
        # all pending URLs on a host. Worker classifies page DNS failures as URL
        # failures so a successful robots check cannot erase their history.
        target = error.host if error.host_wide else url
        now = time.time()
        with self.condition:
            with self.db:
                previous = self.db.execute(
                    "SELECT attempts,first_at,exhausted FROM failure_budget WHERE scope=? AND target=?",
                    (scope, target),
                ).fetchone()
                attempts = previous[0] + 1 if previous else 1
                first_at = previous[1] if previous else now
                exhausted = attempts >= 4 or bool(previous and previous[2])
                self.db.execute(
                    "INSERT OR REPLACE INTO failure_budget VALUES (?,?,?,?,?,?,?)",
                    (scope, target, attempts, first_at, now, str(error), int(exhausted)),
                )
                if exhausted:
                    # completed means frontier work resolved, NOT indexed HTML.
                    column = "hostname" if error.host_wide else "url"
                    self.db.execute(
                        f"UPDATE urls SET completed=1,reason=? WHERE {column}=? AND completed=0",
                        ("unavailable: " + str(error), target),
                    )
                else:
                    retry_delays = getattr(self.config, "retry_delays", (900, 3600, 21600))
                    # The local schedule is a minimum, not a cap on a server's
                    # requested wait. error.seconds may require a longer delay.
                    delay = max(error.seconds, retry_delays[attempts - 1])
                    if error.host_wide:
                        self.db.execute("""
                            INSERT INTO deferred_hosts VALUES (?,?,?)
                            ON CONFLICT(hostname) DO UPDATE SET
                              ready_at=MAX(ready_at,excluded.ready_at),reason=excluded.reason
                        """, (error.host, now + delay, str(error)))
                    self.db.execute(
                        "UPDATE urls SET ready_at=?,reason=? WHERE url=? AND completed=0",
                        (now + delay, str(error), url),
                    )
            self.condition.notify_all()
        return None if exhausted else delay

    def clear_failure(self, url, host_wide=False):
        """Reset a recovered consecutive-failure budget; retain terminal records."""
        # A success clears only a live budget of the requested scope; it does
        # not resurrect terminal unavailable rows or rewrite report statistics.
        scope, target = ("host", urlsplit(url).hostname) if host_wide else ("url", url)
        with self.condition:
            with self.db:
                self.db.execute(
                    "DELETE FROM failure_budget WHERE scope=? AND target=? AND exhausted=0",
                    (scope, target),
                )

    def is_host_deferred(self, hostname):
        with self.condition:
            row = self.db.execute(
                "SELECT ready_at FROM deferred_hosts WHERE hostname=?", (hostname,)
            ).fetchone()
            return bool(row and row[0] > time.time())

    def stop(self):
        # Wake idle workers as well as signaling busy ones. Active network calls
        # still need to return or time out before their worker can finish.
        self.runtime.stop_event.set()
        with self.condition:
            self.condition.notify_all()

    def close(self):
        with self.condition:
            if not self.closed:
                self.db.close()
                fcntl.flock(self._lock_file, fcntl.LOCK_UN)
                self._lock_file.close()
                self.closed = True
