"""Offline regression fixtures for the corrected fresh crawl."""
# Synthetic HTML/URLs exercise observed failure classes: calendars, wiki tools,
# binary payloads, malformed markup, VM images and DNS budget resets. Keeping
# these fixtures does not mean all real low-information families were detected.
# AI assistance disclosure:
# Some automated tests in this project were generated or revised with
# assistance from OpenAI Codex during development and debugging.
# These tests use synthetic inputs and mocked responses for offline validation.
import io
import os
import pickle
import sqlite3
import sys
import tempfile
import time
from configparser import ConfigParser
from contextlib import closing, contextmanager, nullcontext, redirect_stdout
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import cbor
import requests
from bs4 import BeautifulSoup

from test_concurrency import OfflineTest, response, config
import scraper
import crawler.worker as worker_module
import utils.download as downloader
from analytics import record_page
from content_validation import html_rejection
from crawler.frontier import Frontier
from crawler.runtime import RequestScheduler, ScheduledWait
from crawler.worker import Worker
from crawler.errors import RetryLater
from run_setup import prepare_run, project_owner
from install_update import install
from utils.config import Config
import generate_report

PROJECT = Path(__file__).resolve().parent.parent


@contextmanager
def cwd(path):
    previous = Path.cwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(previous)


class FreshCrawlTests(OfflineTest):
    def parse(self, html, headers=None, url="https://www.ics.uci.edu/page"):
        with patch.object(scraper, "record_page") as pages, patch.object(
            scraper, "record_similarity", return_value=None
        ), patch.object(scraper, "record_rejected_page") as rejected:
            links = scraper.scraper(url, response(url, html, headers=headers))
        return links, pages, rejected

    def test_calendar_paths_and_query_only_variants_are_rejected(self):
        variants = [
            "/events/category/social/day/1973-04-17",
            "/events/category/internal-affairs/1971-03/",
            "/events/list/?tribe-bar-date=1973-04-17",
            "/page/6/?post_type=tribe_events&eventDisplay=day&eventDate=2024-04-16",
            "/?post_type=tribe_events&eventDisplay=day&paged=2",
            "/?post_type=tribe_events&paged=2",
            "/page/12/?post_type=tribe_events",
            "/?eventDisplay=day&eventDate=2024-04-16",
        ]
        for path in variants:
            for scheme in ("http", "https"):
                with self.subTest(path=path, scheme=scheme):
                    self.assertFalse(scraper.is_valid(scheme + "://wics.ics.uci.edu" + path))

    def test_event_details_and_ordinary_articles_still_allowed(self):
        for path in ("/event/company-night/", "/events/company-night/", "/events/",
                     "/events/category/social/", "/page/6/", "/2025-04-12/research-news/",
                     "/?post_type=tribe_events&p=123", "/?post_type=tribe_events&eventDisplay=single&p=123", "/?post_type=tribe_events&p=123&eventDate=2024-04-16"):
            self.assertTrue(scraper.is_valid("https://wics.ics.uci.edu" + path), path)

    def test_trac_history_selects_current_article_and_timeline_is_rejected(self):
        base = "https://grape.ics.uci.edu/wiki/public/wiki/cs122b-project1"
        for query in ("?action=diff&version=63", "?format=txt&version=2", "?version=0", "?%76ersion=5"):
            self.assertFalse(scraper.is_valid(base + query))
            self.assertEqual(scraper.prepare_wiki_link(base + query), base)
        self.assertTrue(scraper.is_valid(base))
        self.assertEqual(scraper.prepare_wiki_link(base + "?q=a%20b&version=5"), base + "?q=a%20b")
        self.assertFalse(scraper.is_valid("https://grape.ics.uci.edu/wiki/public/timeline?from=1971-03-01"))

    def test_dokuwiki_highlight_controls_work_on_both_alias_hosts(self):
        for host in ("wiki.ics.uci.edu", "swiki.ics.uci.edu"):
            base = "https://" + host + "/doku.php/accounts:ics_google_apps"
            variant = base + "?s[]=google&s[]=setup"
            self.assertFalse(scraper.is_valid(variant))
            self.assertEqual(scraper.prepare_wiki_link(variant), base)
            self.assertTrue(scraper.is_valid(base))

    def test_raw_source_extensions_filtered_across_all_four_families(self):
        for host in ("ics.uci.edu", "www.cs.uci.edu", "informatics.uci.edu", "stat.uci.edu"):
            for path in ("/CodeExamples/Song.java", "/src/cluster.cc", "/src/cluster.h", "/Makefile", "/program.py", "/SSLPoke.class"):
                self.assertFalse(scraper.is_valid("https://" + host + path))
            self.assertTrue(scraper.is_valid("https://" + host + "/CodeExamples/index.html"))

    def test_dokuwiki_media_downloads_rejected_but_articles_allowed(self):
        for host in ("wiki.ics.uci.edu", "swiki.ics.uci.edu"):
            for scheme in ("http", "https"):
                base = scheme + "://" + host
                self.assertFalse(scraper.is_valid(base + "/lib/exe/fetch.php?tok=d77876&media=http%3A%2F%2Fde3.php.net%2Fimages%2Fphp.gif"))
                self.assertTrue(scraper.is_valid(base + "/doku.php/accounts:connecting_to_openlab"))
        self.assertTrue(scraper.is_valid("https://www.cs.uci.edu/lib/exe/fetch.php?article=research"))

    def test_vm_downloads_rejected_but_vm_documentation_allowed(self):
        for host in ("ics.uci.edu", "www.cs.uci.edu", "informatics.uci.edu", "stat.uci.edu"):
            for extension in ("ova", "ovf", "vmdk", "vdi", "vhd", "vhdx", "qcow", "qcow2", "vbox"):
                for suffix in ("." + extension, "." + extension.upper() + "?download=1"):
                    with self.subTest(host=host, suffix=suffix):
                        self.assertFalse(scraper.is_valid("https://" + host + "/~thornton/ics46/vm/ics46-2022spring-x64" + suffix))
            self.assertTrue(scraper.is_valid("https://" + host + "/~thornton/ics46/vm/index.html"))

    def test_resume_filters_vm_image_already_in_saved_frontier(self):
        vm = "http://www.ics.uci.edu/~thornton/ics46/vm/ics46-2022spring-x64.ova"
        page = "http://www.ics.uci.edu/~thornton/ics46/vm/index.html"
        cfg = config(self.folder, seeds=(vm, page))
        # Simulate a prior version accepting the appliance URL into its queue.
        with patch("crawler.frontier.is_valid", return_value=True):
            old = Frontier(cfg, True)
            old.close()
        resumed = Frontier(cfg, False)
        try:
            self.assertEqual(resumed.get_tbd_url(), page)
            resumed.mark_url_complete(page)
            self.assertIsNone(resumed.get_tbd_url())
        finally:
            resumed.close()

    def test_binary_bytes_rejected_even_if_extensionless_or_mislabeled_html(self):
        for payload in (b"\x7fELF" + b"random letters " * 100, b"image\x00binary" * 50):
            for headers in ({}, {"Content-Type": "text/html"}):
                self.assertIsNotNone(html_rejection(payload, headers))
                with patch.object(scraper, "BeautifulSoup") as parse:
                    links, pages, rejected = self.parse(payload, headers=headers, url="http://dynamo.ics.uci.edu/files/zImage_paapi")
                parse.assert_not_called()
                pages.assert_not_called()
                rejected.assert_called_once()
                self.assertEqual(links, [])

    def test_missing_type_requires_positive_html_evidence(self):
        self.assertIsNone(html_rejection(b"<p>Real university text</p>", {}))
        self.assertIsNotNone(html_rejection(b"random letters from a non HTML file", {}))
        self.assertIsNotNone(html_rejection(b"<html>text</html>", {"Content-Type": "application/octet-stream"}))

    def test_utf16_html_and_small_html_fragments_remain_supported(self):
        self.assertIsNone(html_rejection("<html><p>Students learn</p></html>".encode("utf-16"), {}))
        links, pages, rejected = self.parse(b"<p>Students learn</p><a href='child'>More</a>")
        pages.assert_called_once()
        rejected.assert_not_called()
        self.assertEqual(links, ["https://www.ics.uci.edu/child"])

    def test_empty_soft404_and_login_are_not_recorded_as_pages(self):
        for body in (b"", b"<html><body></body></html>",
                     b"<title>404 Not Found</title><p>No page here</p>",
                     b"<title>Log In</title><form><input type='password'></form>"):
            links, pages, rejected = self.parse(body)
            pages.assert_not_called()
            rejected.assert_called_once()

    def test_textless_hub_can_still_discover_informative_pages(self):
        links, pages, rejected = self.parse(b"<a href='/article'><img src='icon.png'></a>")
        self.assertEqual(links, ["https://www.ics.uci.edu/article"])
        pages.assert_not_called()
        rejected.assert_called_once()

    def test_malformed_numeric_entity_is_parsed_and_link_retained(self):
        body = b"<p>Wroc&#322aw, Poland, 2014, PC Chair</p><a href='/research'>Research</a>"
        links, pages, rejected = self.parse(body)
        self.assertEqual(links, ["https://www.ics.uci.edu/research"])
        pages.assert_called_once()

    def test_parser_fallback_and_double_failure(self):
        body = b"<p>Students research</p><a href='/more'>More</a>"
        real = BeautifulSoup
        def fallback(content, parser):
            if parser == "lxml":
                raise ValueError("bad parser input")
            return real(content, parser)
        with patch.object(scraper, "BeautifulSoup", side_effect=fallback):
            links, pages, rejected = self.parse(body)
        self.assertEqual(links, ["https://www.ics.uci.edu/more"])
        pages.assert_called_once()
        with patch.object(scraper, "BeautifulSoup", side_effect=ValueError("bad HTML")):
            links, pages, rejected = self.parse(body)
        self.assertEqual(links, [])
        pages.assert_not_called()
        rejected.assert_called_once()

    def test_word_counts_ignore_markup_scripts_styles_and_alphanumeric_codes(self):
        body = "<style>fake words</style><script>more fake words</script><p>A I data data 262P CS171 women’s</p>".encode()
        links, pages, rejected = self.parse(body)
        self.assertEqual(pages.call_args.args[1], ["a", "i", "data", "data", "women's"])

    def test_event_calendar_links_never_reach_frontier(self):
        body = b"<p>Students research</p><a href='/events/day/1971-01-01'>Day</a><a href='/page/6/?post_type=tribe_events&amp;eventDisplay=day&amp;eventDate=2024-04-16'>View</a><a href='/event/research-talk'>Talk</a>"
        links, _, _ = self.parse(body, url="https://wics.ics.uci.edu/")
        self.assertEqual(links, ["https://wics.ics.uci.edu/event/research-talk"])

    def test_saved_calendar_pending_is_filtered_without_a_request(self):
        f = self.frontier()
        bad = "https://wics.ics.uci.edu/page/6/?post_type=tribe_events&eventDisplay=day&eventDate=1971-04-16"
        with f.db:
            f.db.execute("INSERT INTO urls(url,domain,hostname) VALUES (?,?,?)", (bad,"ics.uci.edu","wics.ics.uci.edu"))
        self.assertIsNone(f.get_tbd_url())
        self.assertEqual(f.db.execute("SELECT completed,reason FROM urls").fetchone(), (1,"filtered"))

    def test_long_origin_delay_yields_without_spending_budget_and_sibling_runs(self):
        bad, good = "https://slow.ics.uci.edu/a", "https://healthy.ics.uci.edu/a"
        f = self.frontier([bad, good])
        w = Worker(0, f.config, f)
        seen = []
        def fetch(url, *args):
            seen.append(url)
            if url == "https://slow.ics.uci.edu/robots.txt":
                return response(url, b"User-agent: *\nCrawl-delay: 1200", headers={"Content-Type":"text/plain"})
            if url.endswith("/robots.txt"):
                return response(url, status=404)
            f.stop()
            return response(url)
        with patch.object(worker_module, "download", side_effect=fetch), patch.object(scraper, "scraper", return_value=[]):
            w.run()
        self.assertNotIn(bad, seen)
        self.assertIn(good, seen)
        self.assertEqual(f.db.execute("SELECT COUNT(*) FROM failure_budget").fetchone()[0], 0)
        self.assertTrue(f.is_host_deferred("slow.ics.uci.edu"))
        self.assertFalse(f.runtime.errors)

    def test_origin_retry_after_does_not_spread_to_sibling_but_family_minimum_remains(self):
        gate = RequestScheduler(.5)
        a, b = "https://a.ics.uci.edu/a", "https://b.ics.uci.edu/b"
        with gate.request(a):
            gate.postpone(a, 3600)
            finish = time.monotonic()
        with gate.request(b, yield_long_wait=True):
            elapsed = time.monotonic() - finish
            self.assertGreaterEqual(elapsed, .5)
            self.assertLess(elapsed, 2)
        with self.assertRaises(ScheduledWait):
            with gate.request(a, yield_long_wait=True):
                self.fail("Cooldown was bypassed")

    def test_generic_page_connection_failure_is_url_scoped(self):
        f = self.frontier()
        w = Worker(0, f.config, f)
        with patch.object(worker_module, "download", return_value=response("https://www.ics.uci.edu/a", status=601, error="Download Exception Connection refused")):
            with self.assertRaises(RetryLater) as error:
                w.fetch_with_retry("https://www.ics.uci.edu/a")
        self.assertFalse(error.exception.host_wide)

    def test_downloader_refuses_outside_unicode_credentials_and_malformed_urls(self):
        cfg = config(self.folder)
        for url in ("https://google.com/a", "https://fakeics.uci.edu/a", "https://ics.uci.edu.evil.test/a",
                    "https://user@ics.uci.edu/a", "https://ics.uci.edu/é", "<html>garbage</html>",
                    "https://ics.uci.edu:bad/a", "https://ics.uci.edu/a#fragment", "https://ics.uci.edu/a\nb"):
            with patch.object(downloader.requests, "get") as get:
                with self.assertRaises(requests.exceptions.InvalidURL):
                    downloader.download(url, cfg)
                get.assert_not_called()

    def test_downloader_contacts_only_cache_with_ascii_q(self):
        cfg = config(self.folder)
        url = "https://www.ics.uci.edu/a"
        raw = requests.Response()
        raw.status_code, raw.url, raw._content = 200, url, b"<p>Research</p>"
        outer = SimpleNamespace(status_code=200, content=cbor.dumps({"url":url,"status":200,"response":pickle.dumps(raw)}))
        with patch.object(downloader.requests, "get", return_value=outer) as get:
            result = downloader.download(url, cfg)
        self.assertEqual(result.status, 200)
        self.assertEqual(get.call_args.args[0], "http://fake-cache.invalid:9000/")
        self.assertEqual(get.call_args.kwargs["params"][0], ("q",url))
        self.assertEqual(get.call_args.kwargs["timeout"], (10,60))

    def test_cache_outer_error_and_corrupt_envelope_are_infrastructure_failures(self):
        cfg = config(self.folder)
        for status, body in ((500,b"cache unavailable"),(200,b"not CBOR")):
            outer = SimpleNamespace(status_code=status, content=body, headers={})
            with patch.object(downloader.requests, "get", return_value=outer):
                result = downloader.download("https://www.ics.uci.edu/a", cfg)
            self.assertTrue(result.cache_transport_error)

    def test_cache_infrastructure_failure_never_consumes_origin_budget(self):
        f = self.frontier()
        w = Worker(0, f.config, f)
        w.runtime.scheduler = Mock()
        w.runtime.scheduler.request.side_effect = lambda *a, **kw: nullcontext()
        w.runtime.wait = Mock()
        bad = response("https://www.ics.uci.edu/a", status=503, headers={"Retry-After":"120"})
        bad.cache_transport_error = True
        good = response(bad.url)
        with patch.object(worker_module,"download",side_effect=[bad,good]):
            self.assertIs(w.fetch_with_retry(bad.url),good)
        w.runtime.wait.assert_called_once_with(120)
        self.assertEqual(f.db.execute("SELECT COUNT(*) FROM failure_budget").fetchone()[0],0)
        w.runtime.scheduler.postpone_cache.assert_any_call(120)

    def test_prepare_run_does_not_copy_old_statistics_or_legacy_frontier(self):
        project = Path(self.folder)/"project"
        project.mkdir()
        for name in ("config.ini","stopwords.txt"):
            (project/name).write_bytes((PROJECT/name).read_bytes())
        for name in ("crawl_stats.sqlite3","frontier.shelve.sqlite3","report.txt"):
            (project/name).write_bytes(b"original evidence")
        run = prepare_run(project,project/"runs"/"corrected")
        self.assertFalse((run/"crawl_stats.sqlite3").exists())
        self.assertFalse((run/"frontier.shelve.sqlite3").exists())
        parser=ConfigParser();parser.read(run/"config.ini")
        cfg=Config(parser)
        with cwd(run):
            f=Frontier(cfg,True)
            try:
                self.assertEqual(f.db.execute("SELECT COUNT(*) FROM urls").fetchone()[0],4)
                self.assertEqual(f.db.execute("SELECT SUM(completed) FROM urls").fetchone()[0],0)
            finally:
                f.close()
        self.assertEqual((project/"crawl_stats.sqlite3").read_bytes(),b"original evidence")
        with self.assertRaises(ValueError):
            prepare_run(project,run)

    def test_project_owner_rejects_live_legacy_frontier(self):
        f=self.frontier()
        with self.assertRaises(RuntimeError):
            with project_owner(self.folder):
                self.fail("A second crawler must be rejected")

    def test_report_uses_only_selected_runs_words_and_preserves_single_letter_counts(self):
        run=Path(self.folder)/"run";run.mkdir()
        (run/"stopwords.txt").write_text("a i will")
        record_page("https://www.cs.uci.edu/a",["a","i","will","data"],run/"crawl_stats.sqlite3")
        record_page("https://www.ics.uci.edu/a",["old"]*100,Path(self.folder)/"crawl_stats.sqlite3")
        with cwd(run),redirect_stdout(io.StringIO()):
            generate_report.main()
        report=(run/"report.txt").read_text()
        self.assertIn("1. Unique successfully processed HTML URLs\n1",report)
        self.assertIn("Word count: 4",report)
        self.assertIn("data, 1",report)
        self.assertNotIn("old, 100",report)
        self.assertNotIn("will, 1",report)

    def test_installer_preserves_data_git_and_group_identification(self):
        project=Path(self.folder)/"project";project.mkdir()
        (project/"launch.py").write_text("old source")
        (project/".git").mkdir();(project/".git"/"history").write_text("keep")
        (project/"crawl_stats.sqlite3").write_bytes(b"keep data")
        parser=ConfigParser();parser.read(PROJECT/"config.ini")
        parser["IDENTIFICATION"]["USERAGENT"]="IR UF26 11215456,12345678"
        with (project/"config.ini").open("w") as out:parser.write(out)
        backup=install(PROJECT,project)
        self.assertEqual((project/"crawl_stats.sqlite3").read_bytes(),b"keep data")
        self.assertEqual((project/".git"/"history").read_text(),"keep")
        self.assertEqual((backup/"launch.py").read_text(),"old source")
        parser.read(project/"config.ini")
        self.assertEqual(parser["IDENTIFICATION"]["USERAGENT"],"IR UF26 11215456,12345678")

    def test_config_requires_numeric_ids_and_allowed_ascii_seeds(self):
        parser=ConfigParser();parser.read(PROJECT/"config.ini")
        parser["IDENTIFICATION"]["USERAGENT"]="IR UF26 student"
        with self.assertRaises(ValueError):Config(parser)
        parser["IDENTIFICATION"]["USERAGENT"]="IR UF26 11215456"
        parser["CRAWLER"]["SEEDURL"]="https://evil.test/"
        with self.assertRaises(ValueError):Config(parser)

    def test_exact_duplicate_pagination_stops_growth_but_preserves_article_links(self):
        url="https://www.ics.uci.edu/news/page/2/"
        html=b"<p>Repeated index text</p><a href='/news/page/3/'>Next</a><a href='/article/new-research'>Article</a>"
        with patch.object(scraper,"record_page") as pages, patch.object(scraper,"record_similarity",return_value={"kind":"exact","url":"https://www.ics.uci.edu/news/","distance":0}), patch.object(scraper,"record_rejected_page"), redirect_stdout(io.StringIO()):
            links=scraper.scraper(url,response(url,html))
        self.assertEqual(links,["https://www.ics.uci.edu/article/new-research"])
        pages.assert_called_once()  # Distinct fetched URLs remain counted.

    def test_robots_binary_or_unrecognized_text_never_grants_permission(self):
        f=self.frontier()
        w=Worker(0,f.config,f)
        w.runtime.scheduler=Mock()
        w.runtime.scheduler.request.side_effect=lambda *a,**kw:nullcontext()
        for content in (b"binary\x00rules",b"Access is unavailable right now"):
            with patch.object(worker_module,"download",return_value=response("https://www.ics.uci.edu/robots.txt",content,headers={"Content-Type":"text/plain"})):
                with self.assertRaises(RetryLater):w.get_robots_policy("https://www.ics.uci.edu/a")
        self.assertFalse(w.robots_cache)

    def test_restart_guard_refuses_existing_statistics_before_registration(self):
        import launch
        project=Path(self.folder)
        for name in ("config.ini","stopwords.txt"):
            (project/name).write_bytes((PROJECT/name).read_bytes())
        (project/"crawl_stats.sqlite3").write_bytes(b"old evidence")
        with cwd(project):
            with self.assertRaisesRegex(ValueError,"Refusing to reset"):
                launch.main("config.ini",True)
        self.assertEqual((project/"crawl_stats.sqlite3").read_bytes(),b"old evidence")

    def test_submission_zip_uses_selected_report_and_keeps_real_git_history(self):
        import zipfile
        from package_submission import package
        project=Path(self.folder)/"project";project.mkdir()
        for name in ("config.ini","stopwords.txt"):
            (project/name).write_bytes((PROJECT/name).read_bytes())
        (project/"launch.py").write_text("source file")
        (project/"report.txt").write_text("OLD REPORT")
        (project/"crawl_stats.sqlite3").write_bytes(b"old state")
        (project/".git").mkdir();(project/".git"/"HEAD").write_text("real existing history")
        (project/"test-backup-20261026").mkdir()
        (project/"test-backup-20261026"/"report.txt").write_text("STALE TEST REPORT")
        (project/"test-backup-20261026"/"frontier.shelve.dumb.dat").write_bytes(b"stale shelf")
        run=prepare_run(project,project/"runs"/"corrected")
        record_page("https://www.cs.uci.edu/a",["research","students"],run/"crawl_stats.sqlite3")
        archive=package(project,run,project/"submission.zip")
        with zipfile.ZipFile(archive) as z:
            names=z.namelist()
            self.assertIn("spacetime-crawler4py/.git/HEAD",names)
            self.assertIn("spacetime-crawler4py/launch.py",names)
            self.assertIn("spacetime-crawler4py/final_crawl_status.txt",names)
            report=z.read("spacetime-crawler4py/report.txt").decode()
            self.assertNotIn("OLD REPORT",report)
            self.assertIn("Word count: 2",report)
            self.assertFalse(any("crawl_stats.sqlite3" in name for name in names))
            self.assertFalse(any("/runs/" in name for name in names))
            self.assertFalse(any("/test-backup-" in name for name in names))
        self.assertEqual((project/"report.txt").read_text(),"OLD REPORT")
        with self.assertRaises(ValueError):package(project,run,archive)

    def test_family_gate_covers_cache_outage_and_cooldowns_without_network(self):
        gate=RequestScheduler(.5)
        gate.postpone_cache(120)
        for url in ("https://www.ics.uci.edu/a","https://www.cs.uci.edu/a"):
            with self.assertRaises(ScheduledWait):
                with gate.request(url,yield_long_wait=True):
                    self.fail("Global cache backoff was bypassed")

    def test_exposed_redirect_is_resolved_and_filtered_before_scheduling(self):
        url="https://www.ics.uci.edu/old/path"
        with patch.object(scraper,"record_page") as recorded:
            self.assertEqual(scraper.scraper(url,response(url,status=302,headers={"Location":"../article#section"})),["https://www.ics.uci.edu/article"])
            self.assertEqual(scraper.scraper(url,response(url,status=301,headers={"Location":"https://evil.test/"})),[])
        recorded.assert_not_called()

    def test_control_characters_and_whitespace_never_enter_frontier(self):
        for url in ("https://www.ics.uci.edu/a\nb","https://www.ics.uci.edu/a b","https://www.ics.uci.edu/a\x7f"):
            self.assertFalse(scraper.is_valid(url))

