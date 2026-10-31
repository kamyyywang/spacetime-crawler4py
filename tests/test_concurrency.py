# OFFLINE CONCURRENCY AND BEHAVIOR REGRESSIONS
# Network calls are mocked/blocked. These tests demonstrate locking, atomic
# claims, timing and report invariants; they do not establish what happened in
# the course cache logs. The submitted deployment configuration used one worker.
# AI assistance disclosure:
# Some automated tests in this project were generated or revised with
# assistance from OpenAI Codex during development and debugging.
# These tests use synthetic inputs and mocked responses for offline validation.
import dbm.dumb
import gzip
import io
import logging
import shelve
import sqlite3
import tempfile
import threading
import time
import traceback
import unittest
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing, redirect_stdout, nullcontext
from functools import partial
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from requests.exceptions import ConnectionError
from crawler import Crawler
from crawler.frontier import Frontier
from crawler.runtime import RequestScheduler, SharedRuntime, CrawlStopped
from crawler.worker import Worker
from crawler.errors import RetryLater, CacheError
import crawler.worker as worker_module
import scraper
from analytics import record_page
from storage import write_database
from similarity import record_similarity
from robots_support import parse_robots
from sitemap_support import discover_sitemaps
import generate_report
import crawl_status

logging.disable(logging.CRITICAL)


def config(folder, seeds=(), threads=2, delay=0.5):
    return SimpleNamespace(
        user_agent="IR UF26 11215456", time_delay=delay,
        cache_server=("fake-cache.invalid", 9000),
        save_file=str(Path(folder) / "frontier.shelve"),
        seed_urls=list(seeds), threads_count=threads,
    )


def response(url, content=b"<p>Hello students</p>", status=200, headers=None, error=None):
    return SimpleNamespace(
        url=url, status=status, error=error,
        raw_response=SimpleNamespace(
            url=url, content=content,
            headers=headers or {"Content-Type": "text/html"},
        ) if status < 600 else None,
    )


class OfflineTest(unittest.TestCase):
    # Every test gets disposable storage and rejects accidental real HTTP calls.
    # Thread tests therefore exercise scheduler/storage behavior without sending
    # concurrent traffic to the course server or to any target website.
    def setUp(self):
        # Any accidentally unmocked request is a test failure, never a live crawl.
        self.guard = patch(
            "requests.sessions.Session.request",
            side_effect=AssertionError("Network forbidden in offline tests"),
        )
        self.guard.start()
        self.addCleanup(self.guard.stop)
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.folder = self.directory.name
        # Check connection lifetimes on every supported Python version, even
        # versions that do not emit ResourceWarning for leaked SQLite handles.
        connections = []
        class TrackedConnection(sqlite3.Connection):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.explicitly_closed = False
                self.owner_thread = threading.get_ident()
                self.owner_name = threading.current_thread().name
                project = Path(__file__).resolve().parent.parent
                frames = [
                    frame for frame in traceback.extract_stack()
                    if Path(frame.filename).parent == project
                    or Path(frame.filename).parent == project / "crawler"
                ]
                self.opened_at = (
                    f"{Path(frames[-1].filename).name}:{frames[-1].lineno}"
                    if frames else "test helper"
                )
                connections.append(self)

            def close(self):
                super().close()
                self.explicitly_closed = True

        real_connect = sqlite3.connect
        def tracked_connect(*args, **kwargs):
            kwargs["factory"] = TrackedConnection
            return real_connect(*args, **kwargs)

        def check_connections():
            leaked = [db for db in connections if not db.explicitly_closed]
            for db in leaked:
                # A failing test may expose an old writer that leaks handles.
                # Never mask that failure by closing a worker's connection
                # from the main test thread (SQLite forbids this by default).
                if db.owner_thread == threading.get_ident():
                    db.close()
            details = ", ".join(
                f"{db.opened_at} on {db.owner_name}" for db in leaked[:3]
            )
            self.assertEqual(
                len(leaked), 0,
                f"{len(leaked)} SQLite connections were not explicitly closed; "
                f"opened at {details}. Check analytics.py, similarity.py and storage.py.",
            )

        # LIFO cleanup: frontier cleanup runs first, then remove the patch,
        # inspect handles, and finally remove the temporary files.
        self.addCleanup(check_connections)
        tracker = patch("sqlite3.connect", side_effect=tracked_connect)
        tracker.start()
        self.addCleanup(tracker.stop)

    def frontier(self, seeds=()):
        f = Frontier(config(self.folder, seeds), False)
        self.addCleanup(f.close)
        return f


class FrontierTests(OfflineTest):
    def test_saved_wiki_action_variants_are_filtered_but_index_discovery_survives(self):
        f = self.frontier()
        article = "https://wiki.ics.uci.edu/doku.php/accounts:account_activation"
        variants = [article + "?do=edit", article + "?do=login&sectok=", article + "?idx=accounts", article + "?do=index"]
        # Simulate pending records saved before the trap filter was installed.
        with f.db:
            for url in variants:
                f.db.execute("INSERT INTO urls(url,domain,hostname) VALUES (?,?,?)", (url, "ics.uci.edu", "wiki.ics.uci.edu"))
        f.add_url(article)
        seen = []
        while True:
            url = f.get_tbd_url()
            if url is None:
                break
            seen.append(url)
            f.mark_url_complete(url)
        self.assertEqual(set(seen), {
            article,
            "https://wiki.ics.uci.edu/doku.php?do=index&idx=accounts",
            "https://wiki.ics.uci.edu/doku.php?do=index",
        })
        self.assertEqual(f.db.execute("SELECT COUNT(*) FROM urls WHERE reason='filtered'").fetchone()[0], len(variants))

    def test_parallel_insert_and_url_identity(self):
        f = self.frontier()
        urls = ["http://www.ics.uci.edu/a", "https://www.ics.uci.edu/a",
                "http://www.ics.uci.edu/a/", "https://www.ics.uci.edu/a/"]
        with ThreadPoolExecutor(max_workers=8) as pool:
            list(pool.map(f.add_url, [u + "#fragment" for u in urls] * 20))
        self.assertEqual(f.db.execute("SELECT COUNT(*) FROM urls").fetchone()[0], 4)

    def test_no_duplicate_claims_and_correct_completion(self):
        urls = [f"https://www.{domain}/page{i}" for domain in
                ("ics.uci.edu", "cs.uci.edu", "stat.uci.edu", "informatics.uci.edu")
                for i in range(6)]
        f = self.frontier(urls)
        claimed, lock = [], threading.Lock()
        def consume():
            while True:
                url = f.get_tbd_url()
                if url is None:
                    return
                with lock:
                    claimed.append(url)
                time.sleep(0.002)
                f.mark_url_complete(url)
        with ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(lambda _: consume(), range(4)))
        self.assertEqual(Counter(claimed), Counter(urls))
        self.assertFalse(f.active)

    def test_idle_worker_waits_for_active_worker_to_discover_links(self):
        f = self.frontier(["https://www.ics.uci.edu/first"])
        first = f.get_tbd_url()
        result, started = [], threading.Event()
        def waiter():
            started.set()
            result.append(f.get_tbd_url())
        thread = threading.Thread(target=waiter)
        thread.start()
        started.wait(1)
        time.sleep(0.05)
        self.assertTrue(thread.is_alive(), "Worker stopped while discovery was active")
        child = "https://www.cs.uci.edu/child"
        f.add_url(child)
        thread.join(1)
        self.assertEqual(result, [child])
        f.mark_url_complete(first)
        f.mark_url_complete(child)
        self.assertIsNone(f.get_tbd_url())

    def test_resume_reclaims_inflight_url(self):
        cfg = config(self.folder, ["https://www.ics.uci.edu/pending"])
        f = Frontier(cfg, False)
        claimed = f.get_tbd_url()
        # Completion is never persisted merely because a worker claimed it.
        f.close()
        resumed = Frontier(cfg, False)
        try:
            self.assertEqual(resumed.get_tbd_url(), claimed)
            resumed.mark_url_complete(claimed)
        finally:
            resumed.close()

    def test_deferred_host_does_not_block_other_hosts_and_persists(self):
        f = self.frontier(["https://tutors.ics.uci.edu/a", "https://www.ics.uci.edu/b"])
        f.defer_host("tutors.ics.uci.edu", 900, "DNS failure")
        self.assertEqual(f.get_tbd_url(), "https://www.ics.uci.edu/b")
        self.assertTrue(f.is_host_deferred("tutors.ics.uci.edu"))
        f.close()
        resumed = Frontier(config(self.folder), False)
        try:
            self.assertTrue(resumed.is_host_deferred("tutors.ics.uci.edu"))
        finally:
            resumed.close()

    def test_stop_wakes_idle_worker(self):
        f = self.frontier(["https://www.ics.uci.edu/a"])
        f.get_tbd_url()
        result = []
        thread = threading.Thread(target=lambda: result.append(f.get_tbd_url()))
        thread.start()
        time.sleep(0.03)
        f.stop()
        thread.join(1)
        self.assertEqual(result, [None])

    def test_ready_failed_url_does_not_starve_untried_page(self):
        bad = "https://www.ics.uci.edu/old-failure"
        good = "https://www.ics.uci.edu/new-page"
        f = self.frontier([bad, good])
        self.assertEqual(f.get_tbd_url(), bad)
        f.release_url(bad, 0, "Repeated temporary failure")
        self.assertEqual(f.get_tbd_url(), good)
        f.mark_url_complete(good)
        self.assertEqual(f.get_tbd_url(), bad)
        f.mark_url_complete(bad)

    def test_failed_url_priority_and_progress_survive_resume(self):
        bad = "https://www.ics.uci.edu/old-failure"
        good = "https://www.ics.uci.edu/new-page"
        done = "https://www.cs.uci.edu/done"
        f = self.frontier([bad, good, done])
        f.release_url(f.get_tbd_url(), 0, "Repeated temporary failure")
        f.mark_url_complete(done)
        f.close()
        resumed = Frontier(config(self.folder), False)
        try:
            self.assertEqual(resumed.get_tbd_url(), good)
            self.assertEqual(resumed.db.execute("SELECT completed FROM urls WHERE url=?", (done,)).fetchone()[0], 1)
        finally:
            resumed.close()

    def test_legacy_migration_is_once_and_preserves_completed_records(self):
        cfg = config(self.folder)
        with shelve.Shelf(dbm.dumb.open(cfg.save_file + ".dumb", "c")) as old:
            old["one"] = ("https://www.ics.uci.edu/done", True)
            old["two"] = ("http://www.ics.uci.edu/pending", False)
        f = Frontier(cfg, False)
        url = f.get_tbd_url()
        self.assertEqual(url, "http://www.ics.uci.edu/pending")
        f.mark_url_complete(url)
        f.close()
        f = Frontier(cfg, False)
        self.assertIsNone(f.get_tbd_url())
        f.close()
        self.assertTrue(Path(cfg.save_file + ".dumb.dat").exists())

    def test_second_process_owner_is_rejected(self):
        f = self.frontier()
        with self.assertRaises(RuntimeError):
            Frontier(config(self.folder), False)

    def test_restart_uses_new_seeds_without_importing_legacy(self):
        cfg = config(self.folder, ["https://www.ics.uci.edu/a"])
        f = Frontier(cfg, False)
        f.mark_url_complete(f.get_tbd_url())
        f.close()
        f = Frontier(cfg, True)
        self.assertEqual(f.get_tbd_url(), cfg.seed_urls[0])
        f.close()


class SchedulerTests(OfflineTest):
    def test_same_family_serial_and_spaced_across_schemes_and_hosts(self):
        scheduler = RequestScheduler(0)  # Still enforces the 500ms minimum.
        barrier, lock = threading.Barrier(3), threading.Lock()
        times, active, maximum = [], [0], [0]
        def request(url):
            barrier.wait()
            with scheduler.request(url):
                with lock:
                    active[0] += 1
                    maximum[0] = max(maximum[0], active[0])
                    times.append(time.monotonic())
                time.sleep(0.01)
                with lock:
                    active[0] -= 1
        urls = ["http://www.ics.uci.edu/a", "https://isg.ics.uci.edu/b",
                "https://icde2023.ics.uci.edu/c"]
        with ThreadPoolExecutor(max_workers=3) as pool:
            list(pool.map(request, urls))
        times.sort()
        self.assertEqual(maximum[0], 1)
        self.assertTrue(all(b - a >= .5 for a, b in zip(times, times[1:])))

    def test_different_families_can_overlap(self):
        scheduler = RequestScheduler()
        barrier = threading.Barrier(2)
        def request(url):
            with scheduler.request(url):
                barrier.wait(timeout=2)
        with ThreadPoolExecutor(max_workers=2) as pool:
            list(pool.map(request, ["https://www.ics.uci.edu/a", "https://www.cs.uci.edu/b"]))

    def test_stricter_robots_delay_is_origin_scoped(self):
        scheduler = RequestScheduler()
        url = "https://www.ics.uci.edu/a"
        scheduler.set_delay(url, .65)
        with scheduler.request(url):
            first = time.monotonic()
        # A much longer rule on one host must not delay its sibling.
        scheduler.set_delay(url, 1200)
        with scheduler.request("http://other.ics.uci.edu/b"):
            elapsed = time.monotonic() - first
            self.assertGreaterEqual(elapsed, .5)
            self.assertLess(elapsed, 2)
        with self.assertRaises(__import__("crawler.runtime", fromlist=["ScheduledWait"]).ScheduledWait):
            with scheduler.request(url, yield_long_wait=True):
                self.fail("The origin's robots cooldown must remain enforced")

    def test_stop_interrupts_request_wait(self):
        runtime = SharedRuntime(config(self.folder))
        url = "https://www.ics.uci.edu/a"
        runtime.scheduler.postpone(url, 100)
        caught = []
        def wait():
            try:
                with runtime.scheduler.request(url):
                    self.fail("Should not obtain a stopped gate")
            except CrawlStopped:
                caught.append(True)
        thread = threading.Thread(target=wait)
        thread.start()
        runtime.stop_event.set()
        thread.join(1)
        self.assertEqual(caught, [True])


class StorageTests(OfflineTest):
    def test_status_counts_host_delays_without_changing_progress(self):
        f = self.frontier(["https://www.ics.uci.edu/a", "https://www.cs.uci.edu/b"])
        f.defer_host("www.ics.uci.edu", 900, "Unavailable robots rules")
        stats = Path(self.folder) / "stats.sqlite3"
        record_page("https://www.cs.uci.edu/recorded", ["hello"], stats)
        output = io.StringIO()
        with redirect_stdout(output):
            crawl_status.main(f.path, stats)
        self.assertIn("Pending URLs: 2", output.getvalue())
        self.assertIn("Eligible pending URLs (includes active requests): 1", output.getvalue())
        self.assertIn("Pending URLs waiting on URL/host delays: 1", output.getvalue())
        self.assertIn("Recorded HTML URLs: 1", output.getvalue())
        self.assertEqual(f.db.execute("SELECT SUM(completed) FROM urls").fetchone()[0], 0)

    def test_failed_write_rolls_back_and_closes_connection(self):
        database = Path(self.folder) / "rollback.sqlite3"
        with write_database(database) as db:
            db.execute("CREATE TABLE entries (value INTEGER)")
            db.execute("INSERT INTO entries VALUES (1)")
        with self.assertRaisesRegex(RuntimeError, "interrupted write"):
            with write_database(database) as db:
                db.execute("INSERT INTO entries VALUES (2)")
                raise RuntimeError("interrupted write")
        with closing(sqlite3.connect(database)) as db:
            self.assertEqual(db.execute("SELECT value FROM entries").fetchall(), [(1,)])

    def test_report_closes_its_read_connection(self):
        folder = Path(self.folder)
        record_page("https://www.ics.uci.edu/a", ["hello", "students"], folder / "crawl_stats.sqlite3")
        (folder / "stopwords.txt").write_text("the and", encoding="utf-8")
        with patch.object(generate_report, "Path", side_effect=lambda name: folder / name), redirect_stdout(io.StringIO()):
            generate_report.main()
        self.assertIn("1. Unique successfully processed HTML URLs\n1", (folder / "report.txt").read_text())

    def test_parallel_statistics_and_exact_detection_are_atomic(self):
        database = str(Path(self.folder) / "stats.sqlite3")
        words = "university students study software engineering research".split() * 10
        text = " ".join(words)
        def save(i):
            url = f"https://www.ics.uci.edu/{i}"
            record_page(url, words, database)
            return record_similarity(url, text, words, database)
        with ThreadPoolExecutor(max_workers=8) as pool:
            matches = list(pool.map(save, range(20)))
        self.assertEqual(sum(match is None for match in matches), 1)
        self.assertTrue(all(m is None or m["kind"] == "exact" for m in matches))
        with closing(sqlite3.connect(database)) as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM pages").fetchone()[0], 20)
            self.assertEqual(db.execute("SELECT COUNT(*) FROM page_fingerprints_v1").fetchone()[0], 20)
        with ThreadPoolExecutor(max_workers=8) as pool:
            list(pool.map(save, range(20)))
        with closing(sqlite3.connect(database)) as db:
            self.assertEqual(db.execute("SELECT SUM(word_count) FROM pages").fetchone()[0], 20 * len(words))


class WorkerTests(OfflineTest):
    def test_wiki_action_and_revision_urls_rejected_but_articles_allowed(self):
        base = "https://wiki.ics.uci.edu/doku.php/accounts:account_activation"
        for suffix in ("", "?do=", "?do=show"):
            with self.subTest(suffix=suffix):
                self.assertTrue(scraper.is_valid(base + suffix))
        self.assertTrue(scraper.is_valid("https://wiki.ics.uci.edu/doku.php?id=accounts:accounts"))
        for query in (
            "do=backlink", "do=edit", "do=export_pdf", "do=index",
            "do=login&sectok=", "do=media&ns=accounts", "do=recent",
            "do=revisions", "idx=accounts", "idx=accounts%3Aemail",
            "rev=123", "at=2026-01-01", "sectok=token", "do=show&do=edit",
        ):
            with self.subTest(query=query):
                self.assertFalse(scraper.is_valid(base + "?" + query))

    def test_wiki_rules_do_not_block_other_sites_query_pages(self):
        self.assertTrue(scraper.is_valid("https://www.cs.uci.edu/search?do=research&idx=2"))
        self.assertTrue(scraper.is_valid("https://wiki.ics.uci.edu/other-app?do=edit"))

    def test_wiki_index_variants_collapse_without_merging_article_urls(self):
        canonical = "https://wiki.ics.uci.edu/doku.php?do=index&idx=accounts%3Aemail"
        for article in ("accounts:accounts", "accounts:account_activation"):
            url = f"https://wiki.ics.uci.edu/doku.php/{article}?idx=accounts%3Aemail"
            self.assertEqual(scraper.prepare_wiki_link(url), canonical)
            self.assertFalse(scraper.is_valid(url))
        self.assertTrue(scraper.is_valid(canonical))
        self.assertEqual(scraper.prepare_wiki_link(canonical), canonical)
        article = "https://wiki.ics.uci.edu/doku.php?id=accounts:email&do=show"
        self.assertEqual(scraper.prepare_wiki_link(article), article)
        self.assertFalse(scraper.is_valid("https://wiki.ics.uci.edu/doku.php?do=index&idx=accounts&rev=1"))

    def test_wiki_link_extraction_keeps_articles_and_shared_namespace_indexes(self):
        page = "https://wiki.ics.uci.edu/doku.php/accounts:accounts"
        html = b'''<p>Accounts documentation</p>
            <a href="?do=edit">Edit</a><a href="?do=login&amp;sectok=">Login</a>
            <a href="?do=export_pdf">PDF</a><a href="?do=revisions">Revisions</a>
            <a href="?idx=accounts">Index</a>
            <a href="/doku.php/accounts:other?idx=accounts">Same index</a>
            <a href="/doku.php/accounts:email">Email instructions</a>
            <a href="/doku.php?id=accounts:activation">Activation instructions</a>'''
        with patch.object(scraper, "record_page"), patch.object(scraper, "record_similarity", return_value=None):
            links = scraper.scraper(page, response(page, html))
        self.assertEqual(set(links), {
            "https://wiki.ics.uci.edu/doku.php?do=index&idx=accounts",
            "https://wiki.ics.uci.edu/doku.php/accounts:email",
            "https://wiki.ics.uci.edu/doku.php?id=accounts:activation",
        })

    def test_real_workers_robots_sitemap_pages_and_spacing(self):
        hosts = ["https://www.ics.uci.edu", "https://www.cs.uci.edu"]
        cfg = config(self.folder, [h + "/seed" for h in hosts])
        crawler = Crawler(cfg, False)
        requests, lock = [], threading.Lock()
        html_calls = []
        def fetch(url, config, logger):
            with lock:
                requests.append((url, time.monotonic()))
            host = next(h for h in hosts if url.startswith(h + "/"))
            if url.endswith("/robots.txt"):
                data = f"User-agent: *\nDisallow: /private/\nSitemap: {host}/map.xml\n".encode()
                return response(url, data, headers={"Content-Type": "text/plain"})
            if url.endswith("/map.xml"):
                data = f"<urlset><url><loc>{host}/found</loc></url><url><loc>{host}/private/a</loc></url><url><loc>https://example.com/out</loc></url></urlset>".encode()
                return response(url, data, headers={"Content-Type": "application/xml"})
            self.assertNotIn("/private/", url)
            return response(url)
        def process(url, resp):
            with lock:
                html_calls.append(url)
            return []
        with patch.object(worker_module, "download", side_effect=fetch), patch.object(scraper, "scraper", side_effect=process):
            crawler.start()
        urls = Counter(url for url, _ in requests)
        self.assertTrue(all(count == 1 for count in urls.values()))
        self.assertEqual(len(requests), 8)
        self.assertEqual(set(html_calls), {h + p for h in hosts for p in ("/seed", "/found")})
        for host in hosts:
            timeline = [t for u, t in requests if u.startswith(host + "/")]
            self.assertTrue(all(b - a >= .5 for a, b in zip(timeline, timeline[1:])))
        # Both domain families should start before either completes its second fetch.
        self.assertLess(abs(requests[0][1] - requests[1][1]), .5)

    def test_robots_single_flight_for_direct_concurrent_callers(self):
        f = self.frontier()
        a, b = Worker(0, f.config, f), Worker(1, f.config, f)
        calls = []
        def fetch(url, *args):
            calls.append(url)
            time.sleep(.03)
            return response(url, b"User-agent: *\nDisallow: /private/", headers={"Content-Type": "text/plain"})
        with patch.object(worker_module, "download", side_effect=fetch):
            with ThreadPoolExecutor(max_workers=2) as pool:
                policies = list(pool.map(lambda w: w.get_robots_policy("https://www.ics.uci.edu/a"), [a, b]))
        self.assertEqual(len(calls), 1)
        self.assertIs(policies[0], policies[1])

    def test_dns_failure_defers_host_without_allowing_unknown_robots(self):
        bad, good = "https://tutors.ics.uci.edu/a", "https://www.cs.uci.edu/a"
        cfg = config(self.folder, [bad, good])
        f = Frontier(cfg, False)
        self.addCleanup(f.close)
        w = Worker(0, cfg, f)
        def fetch(url, *args):
            if "tutors." in url:
                return response(url, status=601, error="Failed to resolve tutors.ics.uci.edu")
            if url.endswith("robots.txt"):
                return response(url, status=404)
            # Stop once the good page is reached; deferred work must stay pending.
            f.stop()
            return response(url)
        with patch.object(worker_module, "download", side_effect=fetch), patch.object(scraper, "scraper", return_value=[]):
            w.run()
        self.assertTrue(f.is_host_deferred("tutors.ics.uci.edu"))
        self.assertEqual(f.db.execute("SELECT completed FROM urls WHERE url=?", (bad,)).fetchone()[0], 0)

    def test_scraper_exception_stops_all_and_keeps_current_url_pending(self):
        cfg = config(self.folder, ["https://www.ics.uci.edu/a"])
        f = Frontier(cfg, False)
        self.addCleanup(f.close)
        w = Worker(0, cfg, f)
        def fetch(url, *args):
            return response(url, status=404) if url.endswith("robots.txt") else response(url)
        with patch.object(worker_module, "download", side_effect=fetch), patch.object(scraper, "scraper", side_effect=ValueError("test failure")):
            w.run()
        self.assertTrue(f.runtime.stop_event.is_set())
        self.assertTrue(f.runtime.errors)
        self.assertEqual(f.db.execute("SELECT completed FROM urls").fetchone()[0], 0)
        self.assertFalse(f.active)

    def test_blocked_robots_are_not_permission(self):
        f = self.frontier()
        w = Worker(0, f.config, f)
        with patch.object(worker_module, "download", return_value=response("https://www.ics.uci.edu/robots.txt", b"<html>Access Restricted</html>")):
            with self.assertRaises(RetryLater):
                w.get_robots_policy("https://www.ics.uci.edu/a")
        self.assertFalse(f.runtime.robots_cache)

    def test_sitemap_gzip_cycle_and_single_flight(self):
        f = self.frontier()
        w1, w2 = Worker(0, f.config, f), Worker(1, f.config, f)
        host = "https://www.ics.uci.edu"
        policy = parse_robots(host + "/robots.txt", f"User-agent: *\nSitemap: {host}/map.xml")
        calls = []
        def fetch(url, *args, **kwargs):
            calls.append(url)
            if url.endswith("map.xml"):
                return response(url, f"<sitemapindex><sitemap><loc>{host}/pages.xml.gz</loc></sitemap><sitemap><loc>{host}/map.xml</loc></sitemap></sitemapindex>".encode())
            return response(url, gzip.compress(f"<urlset><url><loc>{host}/found</loc></url></urlset>".encode()))
        with patch.object(Worker, "get_robots_policy", return_value=policy), patch.object(Worker, "fetch_with_retry", side_effect=fetch):
            with ThreadPoolExecutor(max_workers=2) as pool:
                list(pool.map(lambda w: discover_sitemaps(w, host + "/page", policy), [w1, w2]))
        self.assertEqual(len(calls), 2)
        self.assertEqual(f.db.execute("SELECT COUNT(*) FROM urls").fetchone()[0], 1)

    def test_connection_retry_and_optional_timeout_limit(self):
        f = self.frontier()
        w = Worker(0, f.config, f)
        # Isolate retry classification; the real gate has separate timing tests.
        w.runtime.scheduler = Mock()
        w.runtime.scheduler.request.side_effect = lambda *a, **kw: nullcontext()
        w.runtime.wait = Mock()
        good = response("https://www.ics.uci.edu/a")
        with patch.object(worker_module, "download", side_effect=[ConnectionError("offline"), good]):
            self.assertIs(w.fetch_with_retry(good.url), good)
        w.runtime.wait.assert_called_with(30)
        timeout = response(good.url, status=601, error="Read timed out. (read timeout=10)")
        with patch.object(worker_module, "download", return_value=timeout) as fetch:
            self.assertIs(w.fetch_with_retry(good.url, origin_timeout_limit=3), timeout)
            self.assertEqual(fetch.call_count, 3)

    def test_origin_connect_timeout_recovers_or_skips_optional_sitemap(self):
        f = self.frontier()
        w = Worker(0, f.config, f)
        w.runtime.scheduler = Mock()
        w.runtime.scheduler.request.side_effect = lambda *a, **kw: nullcontext()
        w.runtime.wait = Mock()
        url = "https://tippersweb.ics.uci.edu/robots.txt"
        errors = (
            "Download Exception HTTPSConnectionPool: Max retries exceeded "
            "(Caused by ConnectTimeoutError(connection, "
            "'Connection to tippersweb.ics.uci.edu timed out. (connect timeout=10)'))",
            "Connection to tippersweb.ics.uci.edu timed out. (connect timeout=10)",
        )
        for error in errors:
            with self.subTest(error=error):
                timeout = response(url, status=601, error=error)
                good = response(url, status=404)
                w.runtime.wait.reset_mock()
                with patch.object(worker_module, "download", side_effect=[timeout, good]) as fetch:
                    with self.assertRaises(RetryLater) as deferred:
                        w.fetch_with_retry(url)
                    self.assertTrue(deferred.exception.host_wide)
                    self.assertEqual(fetch.call_count, 1)
                    # A later scheduling turn can recover; the worker did not
                    # hold a family job slot while sleeping through retries.
                    self.assertIs(w.fetch_with_retry(url), good)
                    self.assertEqual(fetch.call_count, 2)
                w.runtime.wait.assert_not_called()
                with patch.object(worker_module, "download", return_value=timeout) as fetch:
                    self.assertIs(w.fetch_with_retry(url, origin_timeout_limit=3), timeout)
                    self.assertEqual(fetch.call_count, 3)

    def test_repeated_connect_timeout_defers_host_and_other_work_continues(self):
        bad = "https://tippersweb.ics.uci.edu"
        good = "https://www.cs.uci.edu/a"
        f = self.frontier([bad, good])
        w = Worker(0, f.config, f)
        w.runtime.scheduler = Mock()
        w.runtime.scheduler.request.side_effect = lambda *a, **kw: nullcontext()
        w.runtime.wait = Mock()
        calls = []
        def fetch(url, *args):
            calls.append(url)
            if "tippersweb." in url:
                self.assertTrue(url.endswith("/robots.txt"))
                return response(url, status=601, error="ConnectTimeoutError: connect timeout=10")
            if url.endswith("/robots.txt"):
                return response(url, status=404)
            f.stop()
            return response(url)
        with patch.object(worker_module, "download", side_effect=fetch), patch.object(scraper, "scraper", return_value=[]):
            w.run()
        self.assertEqual(calls.count(bad + "/robots.txt"), 1)
        self.assertIn(good, calls)
        self.assertTrue(f.is_host_deferred("tippersweb.ics.uci.edu"))
        self.assertFalse(f.runtime.errors)
        self.assertFalse(f.runtime.robots_cache.get(("https", "tippersweb.ics.uci.edu")))
        self.assertEqual(f.db.execute("SELECT completed FROM urls WHERE url=?", (bad,)).fetchone()[0], 0)
        self.assertFalse(f.active)

    def test_unrecognized_601_still_stops_instead_of_blind_retry(self):
        f = self.frontier()
        w = Worker(0, f.config, f)
        for status in (601, 607):
            with self.subTest(status=status), patch.object(worker_module, "download", return_value=response("https://www.ics.uci.edu/a", status=status, error="unrecognized cache failure")) as fetch:
                with self.assertRaises(CacheError):
                    w.fetch_with_retry("https://www.ics.uci.edu/a")
                fetch.assert_called_once()

    def test_oversized_page_is_completed_without_indexing_and_crawl_continues(self):
        bad = "https://emj.ics.uci.edu/wp-content/uploads/MjolsnessCunhaPMAV24Oct2012"
        good = "https://emj.ics.uci.edu/about"
        f = self.frontier([bad, good])
        w = Worker(0, f.config, f)
        w.runtime.scheduler = Mock()
        w.runtime.scheduler.request.side_effect = lambda *a, **kw: nullcontext()
        calls = []
        def fetch(url, *args):
            calls.append(url)
            if url.endswith("/robots.txt"):
                return response(url, status=404)
            if url == bad:
                return response(url, status=607, error="Content too big. 1748835")
            return response(url)
        with patch.object(worker_module, "download", side_effect=fetch), patch.object(scraper, "record_page") as recorded, patch.object(scraper, "record_similarity", return_value=None), redirect_stdout(io.StringIO()):
            w.run()
        self.assertFalse(f.runtime.errors)
        self.assertEqual(calls.count(bad), 1)
        self.assertIn(good, calls)
        self.assertEqual(f.db.execute("SELECT completed FROM urls WHERE url=?", (bad,)).fetchone()[0], 1)
        self.assertEqual(recorded.call_count, 1)
        self.assertEqual(recorded.call_args.args[0], good)

    def test_cache_oversized_robots_remains_unavailable(self):
        f = self.frontier()
        w = Worker(0, f.config, f)
        url = "https://emj.ics.uci.edu/robots.txt"
        with patch.object(worker_module, "download", return_value=response(url, status=607, error="Content too big. 1748835")) as fetch:
            with self.assertRaises(RetryLater):
                w.get_robots_policy("https://emj.ics.uci.edu/about")
        fetch.assert_called_once()
        self.assertFalse(w.robots_cache)

    def test_oversized_optional_sitemap_is_skipped_without_retry(self):
        f = self.frontier()
        w = Worker(0, f.config, f)
        host = "https://emj.ics.uci.edu"
        sitemap = host + "/map.xml"
        policy = parse_robots(host + "/robots.txt", f"User-agent: *\nSitemap: {sitemap}\n")
        with patch.object(w, "get_robots_policy", return_value=policy), patch.object(worker_module, "download", return_value=response(sitemap, status=607, error="Content too big. 1748835")) as fetch:
            self.assertTrue(discover_sitemaps(w, host + "/about", policy))
        fetch.assert_called_once()
        self.assertIn(sitemap, w.sitemaps_seen)
        self.assertFalse(w.runtime.errors)

    def test_redirect_limit_page_completes_without_indexing_and_crawl_continues(self):
        bad = "https://password.ics.uci.edu"
        good = "https://www.cs.uci.edu/about"
        f = self.frontier([bad, good])
        w = Worker(0, f.config, f)
        w.runtime.scheduler = Mock()
        w.runtime.scheduler.request.side_effect = lambda *a, **kw: nullcontext()
        calls = []
        def fetch(url, *args):
            calls.append(url)
            if url.endswith("/robots.txt"):
                return response(url, status=404)
            if url == bad:
                return response(url, status=601, error="Download Exception Exceeded 30 redirects..")
            return response(url)
        with patch.object(worker_module, "download", side_effect=fetch), patch.object(scraper, "record_page") as recorded, patch.object(scraper, "record_similarity", return_value=None), redirect_stdout(io.StringIO()):
            w.run()
        self.assertFalse(f.runtime.errors)
        self.assertEqual(calls.count(bad), 1)
        self.assertIn(good, calls)
        self.assertEqual(f.db.execute("SELECT completed FROM urls WHERE url=?", (bad,)).fetchone()[0], 1)
        self.assertEqual(recorded.call_count, 1)
        self.assertEqual(recorded.call_args.args[0], good)

    def test_redirect_limit_robots_does_not_grant_permission(self):
        f = self.frontier()
        w = Worker(0, f.config, f)
        url = "https://password.ics.uci.edu/robots.txt"
        with patch.object(worker_module, "download", return_value=response(url, status=601, error="Download Exception Exceeded 30 redirects..")) as fetch:
            with self.assertRaises(RetryLater):
                w.get_robots_policy("https://password.ics.uci.edu")
        fetch.assert_called_once()
        self.assertFalse(w.robots_cache)

    def test_redirect_limit_optional_sitemap_is_skipped(self):
        f = self.frontier()
        w = Worker(0, f.config, f)
        host = "https://password.ics.uci.edu"
        sitemap = host + "/map.xml"
        policy = parse_robots(host + "/robots.txt", f"User-agent: *\nSitemap: {sitemap}\n")
        with patch.object(w, "get_robots_policy", return_value=policy), patch.object(worker_module, "download", return_value=response(sitemap, status=601, error="Download Exception Exceeded 30 redirects..")) as fetch:
            self.assertTrue(discover_sitemaps(w, host, policy))
        fetch.assert_called_once()
        self.assertIn(sitemap, w.sitemaps_seen)
        self.assertFalse(w.runtime.errors)

    def test_cache_robots_denial_of_robots_blocks_origin_and_other_hosts_continue(self):
        host = "http://kdd.ics.uci.edu"
        blocked = [host + "/databases/msnbc/msnbc.html", host + "/other"]
        good = "https://www.cs.uci.edu/about"
        f = self.frontier(blocked + [good])
        w = Worker(0, f.config, f)
        w.runtime.scheduler = Mock()
        w.runtime.scheduler.request.side_effect = lambda *a, **kw: nullcontext()
        calls = []
        def fetch(url, *args):
            calls.append(url)
            if url == host + "/robots.txt":
                return response(url, status=608, error="Denied by domain robot rules.")
            self.assertNotIn(url, blocked)
            return response(url, status=404) if url.endswith("/robots.txt") else response(url)
        with patch.object(worker_module, "download", side_effect=fetch), patch.object(scraper, "record_page") as recorded, patch.object(scraper, "record_similarity", return_value=None), redirect_stdout(io.StringIO()):
            w.run()
        self.assertFalse(f.runtime.errors)
        self.assertEqual(calls.count(host + "/robots.txt"), 1)
        self.assertIn(good, calls)
        self.assertFalse(w.robots_cache[("http", "kdd.ics.uci.edu")][1].can_fetch(f.config.user_agent, blocked[0]))
        self.assertEqual(f.db.execute("SELECT COUNT(*) FROM urls WHERE completed=0").fetchone()[0], 0)
        self.assertEqual(recorded.call_count, 1)
        self.assertEqual(recorded.call_args.args[0], good)

    def test_cache_robots_denial_of_page_skips_without_indexing(self):
        bad = "http://kdd.ics.uci.edu/databases/msnbc/msnbc.html"
        good = "http://kdd.ics.uci.edu/about"
        f = self.frontier([bad, good])
        w = Worker(0, f.config, f)
        w.runtime.scheduler = Mock()
        w.runtime.scheduler.request.side_effect = lambda *a, **kw: nullcontext()
        calls = []
        def fetch(url, *args):
            calls.append(url)
            if url.endswith("/robots.txt"):
                return response(url, status=404)
            return response(url, status=608, error="Denied by domain robot rules.") if url == bad else response(url)
        with patch.object(worker_module, "download", side_effect=fetch), patch.object(scraper, "record_page") as recorded, patch.object(scraper, "record_similarity", return_value=None), redirect_stdout(io.StringIO()):
            w.run()
        self.assertFalse(f.runtime.errors)
        self.assertEqual(calls.count(bad), 1)
        self.assertIn(good, calls)
        self.assertEqual(f.db.execute("SELECT completed FROM urls WHERE url=?", (bad,)).fetchone()[0], 1)
        self.assertEqual(recorded.call_count, 1)
        self.assertEqual(recorded.call_args.args[0], good)

    def test_cache_robots_denial_of_sitemap_is_skipped(self):
        f = self.frontier()
        w = Worker(0, f.config, f)
        host = "http://kdd.ics.uci.edu"
        sitemap = host + "/map.xml"
        policy = parse_robots(host + "/robots.txt", f"User-agent: *\nSitemap: {sitemap}\n")
        with patch.object(w, "get_robots_policy", return_value=policy), patch.object(worker_module, "download", return_value=response(sitemap, status=608, error="Denied by domain robot rules.")) as fetch:
            self.assertTrue(discover_sitemaps(w, host, policy))
        fetch.assert_called_once()
        self.assertIn(sitemap, w.sitemaps_seen)
        self.assertFalse(w.runtime.errors)

    def test_one_bad_page_does_not_defer_healthy_pages_on_same_host(self):
        bad = "https://www.ics.uci.edu/broken"
        good = "https://www.ics.uci.edu/healthy"
        f = self.frontier([bad, good])
        w = Worker(0, f.config, f)
        w.runtime.scheduler = Mock()
        w.runtime.scheduler.request.side_effect = lambda *a, **kw: nullcontext()
        w.runtime.wait = Mock()
        calls = []
        def fetch(url, *args):
            calls.append(url)
            if url.endswith("/robots.txt"):
                return response(url, status=404)
            if url == bad:
                return response(url, status=500)
            f.stop()
            return response(url)
        with patch.object(worker_module, "download", side_effect=fetch), patch.object(scraper, "scraper", return_value=[]):
            w.run()
        self.assertEqual(calls.count(bad), 1)
        self.assertIn(good, calls)
        self.assertFalse(f.is_host_deferred("www.ics.uci.edu"))
        self.assertEqual(f.db.execute("SELECT completed FROM urls WHERE url=?", (bad,)).fetchone()[0], 0)
        self.assertEqual(f.db.execute("SELECT completed FROM urls WHERE url=?", (good,)).fetchone()[0], 1)
        self.assertFalse(f.runtime.errors)
        w.runtime.wait.assert_not_called()
        w.runtime.scheduler.postpone.assert_not_called()

    def test_retry_after_is_shared_before_permit_release(self):
        f = self.frontier()
        w = Worker(0, f.config, f)
        busy = response("https://www.ics.uci.edu/a", status=429, headers={"Retry-After": "900"})
        good = response(busy.url)
        w.runtime.scheduler = Mock()
        w.runtime.scheduler.request.side_effect = lambda *a, **kw: nullcontext()
        w.runtime.wait = Mock()
        with patch.object(worker_module, "download", side_effect=[busy, good]):
            with self.assertRaises(RetryLater) as deferred:
                w.fetch_with_retry(busy.url)
        self.assertTrue(deferred.exception.host_wide)
        self.assertGreaterEqual(deferred.exception.seconds, 900)
        self.assertIn(unittest.mock.call(busy.url, 900), w.runtime.scheduler.postpone.call_args_list)
        w.runtime.wait.assert_not_called()

    def test_page_origin_timeout_is_url_scoped_and_pending(self):
        f = self.frontier()
        w = Worker(0, f.config, f)
        url = "https://www.ics.uci.edu/broken"
        with patch.object(worker_module, "download", return_value=response(url, status=601, error="Read timed out. (read timeout=10)")) as fetch:
            with self.assertRaises(RetryLater) as deferred:
                w.fetch_with_retry(url)
        self.assertFalse(deferred.exception.host_wide)
        self.assertGreaterEqual(deferred.exception.seconds, 300)
        fetch.assert_called_once()

    def test_two_workers_run_actual_scraper_and_statistics(self):
        hosts = ["https://www.ics.uci.edu", "https://www.cs.uci.edu"]
        database = str(Path(self.folder) / "actual-stats.sqlite3")
        crawler = Crawler(config(self.folder, [h + "/page" for h in hosts]), False)
        def fetch(url, *args):
            if url.endswith("robots.txt"):
                return response(url, status=404)
            return response(url, b"<h1>Hello students</h1><p>Hello university</p>")
        with patch.object(worker_module, "download", side_effect=fetch), patch.object(scraper, "record_page", side_effect=partial(record_page, database=database)), patch.object(scraper, "record_similarity", side_effect=partial(record_similarity, database=database)), redirect_stdout(io.StringIO()):
            crawler.start()
        with closing(sqlite3.connect(database)) as db:
            self.assertEqual(db.execute("SELECT COUNT(*),SUM(word_count) FROM pages").fetchone(), (2, 8))
            self.assertEqual(db.execute("SELECT COUNT(*) FROM page_fingerprints_v1").fetchone()[0], 2)

    def test_redirect_aliases_are_counted_separately_and_links_use_destination(self):
        database = str(Path(self.folder) / "redirect-stats.sqlite3")
        target = "https://www.ics.uci.edu/new/location/"
        resp = response(target, b"<p>Hello students</p><a href='child'>Child</a>")
        with patch.object(scraper, "record_page", side_effect=partial(record_page, database=database)), patch.object(scraper, "record_similarity", side_effect=partial(record_similarity, database=database)), redirect_stdout(io.StringIO()):
            a = scraper.scraper("http://www.ics.uci.edu/old#one", resp)
            b = scraper.scraper("https://www.ics.uci.edu/old#two", resp)
        self.assertEqual(a, [target + "child"])
        self.assertEqual(b, a)
        with closing(sqlite3.connect(database)) as db:
            urls = {row[0] for row in db.execute("SELECT url FROM pages")}
        self.assertEqual(urls, {"http://www.ics.uci.edu/old", "https://www.ics.uci.edu/old"})

    def test_access_block_is_not_indexed(self):
        url = "https://www.ics.uci.edu/blocked"
        content = b"<html><title>Access Restricted - IP Location Block</title><p>The website owner has configured geographic access restrictions.</p></html>"
        with patch.object(scraper, "record_page") as page, patch.object(scraper, "record_similarity") as similarity, patch.object(scraper, "record_rejected_page") as rejected, redirect_stdout(io.StringIO()):
            self.assertEqual(scraper.scraper(url, response(url, content)), [])
        page.assert_not_called()
        similarity.assert_not_called()
        rejected.assert_called_once()

    def test_stop_releases_active_work_for_resume(self):
        cfg = config(self.folder, ["https://www.ics.uci.edu/a"])
        crawler = Crawler(cfg, False)
        entered = threading.Event()
        def fetch(url, *args):
            entered.set()
            crawler.frontier.runtime.wait(60)
        with patch.object(worker_module, "download", side_effect=fetch):
            crawler.start_async()
            self.assertTrue(entered.wait(1))
            crawler.frontier.stop()
            crawler.join()
        self.assertFalse(crawler.frontier.active)
        crawler.frontier.close()
        resumed = Frontier(cfg, False)
        try:
            self.assertEqual(resumed.get_tbd_url(), cfg.seed_urls[0])
        finally:
            resumed.close()

    def test_unknown_cache_error_is_visible_and_pending(self):
        f = self.frontier(["https://www.ics.uci.edu/a"])
        w = Worker(0, f.config, f)
        with patch.object(worker_module, "download", return_value=response("https://www.ics.uci.edu/robots.txt", status=608, error="policy error")):
            w.run()
        self.assertTrue(f.runtime.errors)
        self.assertEqual(f.db.execute("SELECT completed FROM urls").fetchone()[0], 0)


if __name__ == "__main__":
    unittest.main()
