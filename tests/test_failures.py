"""Regression tests use mocked cache replies; no network is permitted."""
# Simulated scheduling turns advance retry attempts without waiting for actual
# minutes/hours. Assertions check pending/unavailable state and preserved page
# statistics, not a fabricated successful download after a failure.
# AI assistance disclosure:
# Some automated tests in this project were generated or revised with
# assistance from OpenAI Codex during development and debugging.
# These tests use synthetic inputs and mocked responses for offline validation.
import io
import sqlite3
from contextlib import nullcontext, redirect_stdout
from unittest.mock import Mock, patch

from test_concurrency import OfflineTest, response
from crawler.frontier import Frontier
from crawler.worker import Worker
from crawler.errors import RetryLater, CacheError
import crawler.worker as worker_module
import scraper
import crawl_status


class FailureTests(OfflineTest):
    def test_dns_failure_is_host_scoped_for_robots_but_url_scoped_for_pages(self):
        f = self.frontier()
        w = self.ungated(f)
        for url, host_wide in (("https://asterix.ics.uci.edu/robots.txt", True), ("https://asterix.ics.uci.edu/fuzzyjoin-mapreduce/", False)):
            with self.subTest(url=url), patch.object(worker_module, "download", return_value=response(url, status=601, error="Download Exception Failed to resolve origin")):
                with self.assertRaises(RetryLater) as result:
                    w.fetch_with_retry(url)
                self.assertEqual(result.exception.host_wide, host_wide)

    def test_page_dns_budget_exhausts_across_resumes_with_successful_robots(self):
        url = "https://asterix.ics.uci.edu/fuzzyjoin-mapreduce/"
        sibling = "https://asterix.ics.uci.edu/research"
        f = self.frontier([url, sibling])
        cfg = f.config
        cfg.use_sitemaps = False
        def fetch(target, *args):
            if target.endswith("/robots.txt"):
                return response(target, b"User-agent: *\nDisallow:\n", headers={"Content-Type": "text/plain"})
            self.assertEqual(target, url)
            return response(target, status=601, error="Download Exception Failed to resolve redirect destination")
        for attempt in range(1, 5):
            if attempt > 1:
                f.close()
                f = Frontier(cfg, False)
                self.addCleanup(f.close)
            w = self.ungated(f)
            with patch.object(worker_module, "download", side_effect=fetch), patch.object(f, "get_tbd_url", side_effect=[url, None]), patch.object(scraper, "scraper") as process:
                w.run()
            process.assert_not_called()
            self.assertEqual(f.db.execute("SELECT attempts,exhausted FROM failure_budget WHERE scope='url' AND target=?", (url,)).fetchone(), (attempt, int(attempt == 4)))
            self.assertEqual(f.db.execute("SELECT completed FROM urls WHERE url=?", (sibling,)).fetchone()[0], 0)
            self.assertFalse(f.runtime.errors)
        self.assertEqual(f.db.execute("SELECT completed,reason FROM urls WHERE url=?", (url,)).fetchone(), (1, "unavailable: Origin DNS failure for " + url))

    def ungated(self, f):
        w = Worker(0, f.config, f)
        w.runtime.scheduler = Mock()
        w.runtime.scheduler.request.side_effect = lambda *a, **kw: nullcontext()
        w.runtime.wait = Mock()
        return w

    def test_refused_unreachable_and_tls_are_deferred_not_fatal(self):
        f = self.frontier()
        w = self.ungated(f)
        url = "https://curly.lom.ics.uci.edu/robots.txt"
        for message in ("[Errno 101] Network is unreachable", "[Errno 111] Connection refused", "SSLError: certificate verify failed"):
            with self.subTest(message=message), patch.object(
                worker_module, "download", return_value=response(url, status=601, error="Download Exception HTTPSConnectionPool: " + message)
            ) as fetch:
                with self.assertRaises(RetryLater) as result:
                    w.fetch_with_retry(url)
                self.assertTrue(result.exception.host_wide)
                fetch.assert_called_once()
        w.runtime.wait.assert_not_called()

    def test_unreachable_host_does_not_stop_other_pages_or_allow_unknown_rules(self):
        bad, good = "https://curly.lom.ics.uci.edu/a", "https://healthy.ics.uci.edu/a"
        f = self.frontier([bad, good])
        w = self.ungated(f)
        seen = []
        def fetch(url, *args):
            seen.append(url)
            if "curly." in url:
                return response(url, status=601, error="Download Exception [Errno 101] Network is unreachable")
            if url.endswith("/robots.txt"):
                return response(url, status=404)
            f.stop()
            return response(url)
        with patch.object(worker_module, "download", side_effect=fetch), patch.object(scraper, "scraper", return_value=[]) as process:
            w.run()
        self.assertIn(good, seen)
        self.assertNotIn(bad, seen)
        self.assertFalse(f.runtime.errors)
        self.assertFalse(f.active)
        self.assertEqual(process.call_args.args[0], good)
        self.assertEqual(f.db.execute("SELECT attempts FROM failure_budget").fetchone()[0], 1)

    def test_failure_budget_and_backoff_survive_resume_and_exhaust_host(self):
        bad, sibling = "https://dead.ics.uci.edu/a", "http://dead.ics.uci.edu/b"
        good = "https://healthy.ics.uci.edu/a"
        f = self.frontier([bad, sibling, good])
        error = RetryLater("unreachable robots", "dead.ics.uci.edu", 300)
        for attempt, expected in enumerate((900, 3600, 21600), 1):
            self.assertEqual(f.note_failure(bad, error), expected)
            self.assertEqual(f.db.execute("SELECT attempts FROM failure_budget").fetchone()[0], attempt)
        cfg = f.config
        f.close()
        resumed = Frontier(cfg, False)
        self.addCleanup(resumed.close)
        self.assertIsNone(resumed.note_failure(bad, error))
        self.assertEqual(resumed.db.execute("SELECT attempts,exhausted FROM failure_budget").fetchone(), (4, 1))
        self.assertEqual(resumed.get_tbd_url(), good)
        resumed.mark_url_complete(good)
        self.assertIsNone(resumed.get_tbd_url())
        self.assertEqual(resumed.db.execute("SELECT COUNT(*) FROM urls WHERE reason LIKE 'unavailable: %'").fetchone()[0], 2)
        # A later link to this exhausted host cannot revive an infinite loop.
        resumed.add_url("https://dead.ics.uci.edu/later")
        self.assertIsNone(resumed.get_tbd_url())
        self.assertEqual(resumed.db.execute("SELECT completed FROM urls WHERE url LIKE '%/later'").fetchone()[0], 1)

    def test_page_budget_does_not_abandon_sibling_pages_or_change_statistics(self):
        bad, good = "https://www.ics.uci.edu/bad", "https://www.ics.uci.edu/good"
        f = self.frontier([bad, good])
        f.db.execute("CREATE TABLE existing_statistics (value INTEGER)")
        f.db.execute("INSERT INTO existing_statistics VALUES (42)")
        f.db.commit()
        error = RetryLater("origin status 500", "www.ics.uci.edu", host_wide=False)
        for _ in range(4):
            f.note_failure(bad, error)
        self.assertEqual(f.get_tbd_url(), good)
        f.mark_url_complete(good)
        self.assertIsNone(f.get_tbd_url())
        self.assertEqual(f.db.execute("SELECT value FROM existing_statistics").fetchone()[0], 42)

    def test_recovery_resets_consecutive_budget_and_retry_after_is_not_shortened(self):
        f = self.frontier(["https://www.ics.uci.edu/a"])
        url = "https://www.ics.uci.edu/a"
        error = RetryLater("rate limited", "www.ics.uci.edu", 86400)
        self.assertEqual(f.note_failure(url, error), 86400)
        f.clear_failure(url, host_wide=True)
        self.assertEqual(f.db.execute("SELECT COUNT(*) FROM failure_budget").fetchone()[0], 0)
        self.assertEqual(f.note_failure(url, RetryLater("new failure", error.host)), 900)

    def test_cache_outage_does_not_consume_origin_budget_and_unknown_codes_stop(self):
        from requests.exceptions import ConnectionError
        f = self.frontier()
        w = self.ungated(f)
        url = "https://www.ics.uci.edu/a"
        good = response(url)
        with patch.object(worker_module, "download", side_effect=[ConnectionError("cache offline"), good]):
            self.assertIs(w.fetch_with_retry(url), good)
        self.assertEqual(f.db.execute("SELECT COUNT(*) FROM failure_budget").fetchone()[0], 0)
        for message in ("unknown policy problem", "quota exceeded"):
            with patch.object(worker_module, "download", return_value=response(url, status=601, error=message)):
                with self.assertRaises(CacheError):
                    w.fetch_with_retry(url)

    def test_worker_exhausts_robots_without_fetching_page_or_indexing(self):
        url = "https://dead.ics.uci.edu/a"
        f = self.frontier([url])
        w = self.ungated(f)
        with patch.object(w, "get_robots_policy", side_effect=RetryLater("HTML robots", "dead.ics.uci.edu")), patch.object(scraper, "scraper") as process:
            for _ in range(4):
                # One scheduling turn per simulated deadline; never sleep.
                with patch.object(f, "get_tbd_url", side_effect=[url, None]):
                    w.run()
        process.assert_not_called()
        self.assertFalse(f.runtime.errors)
        self.assertFalse(f.active)
        self.assertEqual(f.db.execute("SELECT completed,reason FROM urls").fetchone(), (1, "unavailable: HTML robots"))
        output = io.StringIO()
        with redirect_stdout(output):
            crawl_status.main(f.path, self.folder + "/absent-stats.sqlite3")
        self.assertIn("Unavailable frontier entries: 1", output.getvalue())
        self.assertIn("host | dead.ics.uci.edu | 4 | HTML robots", output.getvalue())

    def test_migration_adds_budget_without_resetting_frontier_and_restart_clears_it(self):
        url = "https://www.ics.uci.edu/a"
        f = self.frontier([url])
        cfg = f.config
        f.mark_url_complete(url)
        f.db.execute("DROP TABLE failure_budget")
        f.db.commit()
        f.close()
        resumed = Frontier(cfg, False)
        self.assertEqual(resumed.db.execute("SELECT completed FROM urls").fetchone()[0], 1)
        resumed.note_failure(url, RetryLater("failure", "www.ics.uci.edu"))
        resumed.close()
        restarted = Frontier(cfg, True)
        self.addCleanup(restarted.close)
        self.assertEqual(restarted.db.execute("SELECT COUNT(*) FROM failure_budget").fetchone()[0], 0)
        self.assertEqual(restarted.db.execute("SELECT completed FROM urls").fetchone()[0], 0)
