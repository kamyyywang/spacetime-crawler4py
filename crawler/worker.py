# WORKER LIFECYCLE
# Claim URL -> obtain robots policy -> optional sitemap discovery -> polite
# cache fetch -> scrape/index -> enqueue links -> resolve the frontier job.
#
# Expected origin failures release the job for a bounded later retry. Cache
# connectivity outages stay pending with shared backoff. Unknown cache/policy
# failures stop visibly. No branch falls back to downloading an origin directly.
import math
import re
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from inspect import getsource
from threading import Thread
from urllib.parse import urlsplit

from requests.exceptions import ConnectionError, Timeout, RequestException
from utils.download import download
from utils import get_logger
from robots_support import robots_url_for, parse_robots
from response_filters import is_access_block
from sitemap_support import discover_sitemaps
from crawler.runtime import CrawlStopped, ScheduledWait
from crawler.errors import RetryLater, CacheError
import scraper


class Worker(Thread):
    def __init__(self, worker_id, config, frontier):
        super().__init__(name=f"Worker-{worker_id}", daemon=False)
        self.logger = get_logger(f"Worker-{worker_id}", "Worker")
        self.config = config
        self.frontier = frontier
        # All workers use the same runtime. This shares request gates, robots
        # policies and sitemap bookkeeping rather than duplicating them per thread.
        self.runtime = frontier.runtime
        # These are shared objects, not a separate cache/set for each worker.
        self.robots_cache = self.runtime.robots_cache
        self.sitemaps_seen = self.runtime.sitemaps_seen
        self.sitemap_origins = self.runtime.sitemap_origins
        source = getsource(scraper)
        assert all(s not in source for s in ("from requests import", "import requests")), "Do not download in scraper.py"
        assert all(s not in source for s in ("from urllib.request import", "import urllib.request")), "Do not download in scraper.py"

    def retry_after_seconds(self, resp):
        # Retry-After may be a number of seconds or an HTTP date. Invalid or
        # missing values contribute zero; the scheduler's normal minimum remains.
        raw = getattr(resp, "raw_response", None)
        value = getattr(raw, "headers", {}).get("Retry-After")
        if not value:
            return 0
        try:
            result = float(value)
            return max(0, result) if math.isfinite(result) else 0
        except (TypeError, ValueError):
            try:
                date = parsedate_to_datetime(value)
                if date.tzinfo is None:
                    date = date.replace(tzinfo=timezone.utc)
                return max(0, (date - datetime.now(timezone.utc)).total_seconds())
            except (TypeError, ValueError, OverflowError):
                return 0

    def fetch_with_retry(self, url, delay=None, origin_timeout_limit=None):
        """Every actual cache request, including retries, obtains a shared gate."""
        # Two kinds of failure need different treatment:
        # (1) We cannot contact/decode the course cache: preserve the work and
        #     back off globally, without spending an origin's failure budget.
        # (2) The cache reports an origin failure: yield the job so healthy work
        #     can continue, retaining a persistent URL/host retry budget.
        interval = max(0.5, self.config.time_delay, delay or 0)
        retry_delay = 30
        origin_failures = 0
        while True:
            resp = None
            cache_retry_after = 0
            try:
                with self.runtime.scheduler.request(url, interval, yield_long_wait=True):
                    # The permit spans the whole download, not just this log line.
                    self.logger.info("Fetching %s", url)
                    resp = download(url, self.config, self.logger)
                    # Publish a server cooldown before another thread can enter.
                    retry_after_header = self.retry_after_seconds(resp)
                    if retry_after_header:
                        if getattr(resp, "cache_transport_error", False):
                            self.runtime.scheduler.postpone_cache(retry_after_header)
                        else:
                            self.runtime.scheduler.postpone(url, retry_after_header)
                    if getattr(resp, "cache_transport_error", False):
                        cache_retry_after = retry_after_header
                        self.logger.warning("Cache infrastructure failure; preserving URL: %s", resp.error)
                        resp = None
            except (ConnectionError, Timeout) as error:
                # Keep the work pending and automatically recover from outages.
                self.logger.warning("Cache connection failed: %s", error)
            except RequestException as error:
                raise CacheError(f"Invalid cache request for {url}: {error}") from error

            if resp is not None:
                self.logger.info(
                    "Downloaded %s, status <%s>, using cache %s.",
                    url, resp.status, self.config.cache_server,
                )
                message = str(getattr(resp, "error", "")).lower()
                if resp.status == 608 and "denied by domain robot rules" in message:
                    # A robots denial is terminal for this request, not an error
                    # that justifies another route around the cache or the rule.
                    # Respect the cache's robots denial; do not retry or bypass
                    # it. Page/sitemap callers skip this non-200 response, and
                    # robots callers cache a conservative disallow-all policy.
                    self.logger.warning("ROBOTS: cache denied %s: %s", url, resp.error)
                    return resp
                if resp.status == 601 and re.search(r"exceeded \d+ redirects", message):
                    # The origin's redirect loop cannot be repaired by repeatedly
                    # requesting the same page. Return the failure for normal skipping.
                    # The cache exhausted its redirect limit. Treat this URL
                    # as a terminal failure, not an outage worth retrying.
                    # Robots callers still defer because rules are unknown.
                    self.logger.warning("Skipping redirect-limit response for %s: %s", url, resp.error)
                    return resp
                # Skip responses rejected by the cache for being too large.
                if resp.status == 607 and message.strip().startswith("content too big"):
                    self.logger.warning(
                        "Skipping oversized response for %s: %s",
                        url,
                        resp.error,
                    )
                    return resp
                if resp.status == 601 and (
                    "failed to resolve" in message or "name or service not known" in message
                ):
                    raise RetryLater(
                        f"Origin DNS failure for {url}",
                        urlsplit(url).hostname,
                        900,
                        # Count page failures separately from robots failures.
                        host_wide=(
                            urlsplit(url).path.lower().rstrip("/") == "/robots.txt"
                        ),
                    )
                # The cache wraps origin timeouts in status 601. A connect
                # timeout and a read timeout are both temporary origin failures;
                # other cache/policy errors must still stop visibly.
                cache_timeout = resp.status == 601 and any(
                    marker in message for marker in (
                        "read timed out", "connecttimeouterror", "connect timeout="
                    )
                )
                # A 601 explicitly labelled Download Exception describes a
                # cache-to-origin failure (refused/unreachable connection, TLS,
                # etc.), not a denial of this crawler's cache access.
                if (resp.status == 601 and not cache_timeout
                        and message.strip().startswith("download exception")):
                    if origin_timeout_limit is not None:
                        self.logger.warning("Skipping unavailable optional sitemap %s: %s", url, resp.error)
                        return resp
                    raise RetryLater(
                        f"Origin download failure for {url}: {resp.error}",
                        urlsplit(url).hostname, 900,
                        host_wide=(urlsplit(url).path.lower().rstrip("/") == "/robots.txt"),
                    )
                if resp.status >= 600 and not cache_timeout:
                    # Do not guess the meaning of an unrecognized course-cache
                    # status. Preserve pending progress and surface the problem.
                    raise CacheError(f"Cache error {resp.status}: {getattr(resp, 'error', None)}")
                temporary = cache_timeout or resp.status in {408, 429} or 500 <= resp.status < 600
                # Ordinary HTTP 404/403 responses are not retried here. They pass
                # through to the caller, which can skip pages or interpret robots.
                missing = resp.status == 200 and resp.raw_response is None
                if not temporary and not missing:
                    return resp
                origin_failures += 1
                retry_after = self.retry_after_seconds(resp)
                if retry_after:
                    self.runtime.scheduler.postpone(url, retry_after)
                # Yield ordinary failures immediately. Keeping the job while
                # sleeping/retrying blocks its whole domain-family work slot.
                # Optional sitemaps retain their explicit bounded retry limit.
                limit = origin_timeout_limit or 1
                # An ordinary page failure yields immediately. The persistent
                # frontier, not an unbounded inner loop, controls later attempts.
                # Optional sitemap callers may request several local attempts.
                if origin_failures >= limit:
                    if origin_timeout_limit is not None:
                        self.logger.warning("Skipping optional sitemap %s after %s failures", url, limit)
                        return resp
                    raise RetryLater(
                        f"Temporary failure (status {resp.status}) for {url}",
                        urlsplit(url).hostname, max(300, retry_after),
                        host_wide=(
                            urlsplit(url).path.lower().rstrip('/') == '/robots.txt'
                            or resp.status == 429
                        ),
                    )
            else:
                retry_after = cache_retry_after

            seconds = max(retry_delay, interval, retry_after)
            # Retry-After was already published inside the request gate.
            # Only a cache connection failure needs this shared outage backoff;
            # an origin page failure must not freeze sibling hosts' requests.
            if resp is None:
                self.runtime.scheduler.postpone_cache(seconds)
            self.logger.warning("Keeping %s pending. Retrying in %.0f seconds.", url, seconds)
            self.runtime.wait(seconds)
            retry_delay = min(retry_delay * 2, 300)
            # Cap only the local exponential backoff. A larger Retry-After value
            # is still honored by the max() and published scheduler deadline.

    def policy_interval(self, policy):
        # Enforce the largest of the assignment minimum, configured delay,
        # robots Crawl-delay and the average interval from Request-rate.
        interval = max(0.5, self.config.time_delay, policy.crawl_delay(self.config.user_agent) or 0)
        rate = policy.request_rate(self.config.user_agent)
        if rate and rate.requests > 0:
            interval = max(interval, rate.seconds / rate.requests)
        return interval

    def get_robots_policy(self, page_url):
        # Policies belong to origins, not individual pages. The per-origin lock
        # lets one worker load rules while peers reuse the result. The shared
        # metadata lock is not held over unrelated-origin network operations.
        parts = urlsplit(page_url)
        origin = (parts.scheme, parts.netloc)
        # Only one worker loads a given origin's rules. Other origins proceed.
        with self.runtime.lock_for("robots", origin):
            cached = self.robots_cache.get(origin)
            if cached and time.monotonic() - cached[0] < 3600:
                # Revalidate after one hour; this cache is not persisted to disk.
                return cached[1]
            url = robots_url_for(page_url)
            previous_delay = self.policy_interval(cached[1]) if cached else None
            resp = self.fetch_with_retry(url, previous_delay)
            if resp.status in {404, 410}:
                # An absent robots file differs from unreadable/blocked rules:
                # only these explicit absence statuses create an empty policy.
                policy = parse_robots(url, "")
            elif resp.status in {401, 403} or (
                resp.status == 608
                and "denied by domain robot rules"
                    in str(getattr(resp, "error", "")).lower()
            ):
                policy = parse_robots(url, "")
                policy.disallow_all = True
            elif resp.status == 200:
                # A 200 response can be an HTML error/login/geographic block.
                # Such a body does not establish permission to crawl the host.
                content = resp.raw_response.content
                if len(content) > 512 * 1024:
                    raise RetryLater(f"Oversized robots file at {url}", parts.hostname, 900)
                if b"\x00" in content or any(byte < 32 and byte not in (9, 10, 13) for byte in content):
                    raise RetryLater(f"Binary/unreadable robots file at {url}", parts.hostname, 900)
                text = content.decode("utf-8-sig", errors="replace")
                kind = resp.raw_response.headers.get("Content-Type", "").lower()
                if (
                    is_access_block(text) or "text/html" in kind
                    or content.lstrip().lower().startswith((b"<!doctype html", b"<html"))
                ):
                    # Unavailable rules are not the same thing as no restrictions.
                    raise RetryLater(f"Blocked/HTML robots response at {url}", parts.hostname, 900)
                if len(content) > 512 * 1024:
                    raise RetryLater(f"Oversized robots file at {url}", parts.hostname, 900)
                meaningful = [line.strip() for line in text.splitlines()
                              if line.strip() and not line.lstrip().startswith("#")]
                if meaningful and not any(re.match(
                    r"(?i)^(?:user-agent|allow|disallow|sitemap|crawl-delay|request-rate)\s*:", line
                ) for line in meaningful):
                    raise RetryLater(f"Unrecognized robots text at {url}", parts.hostname, 900)
                policy = parse_robots(url, text)
            else:
                raise RetryLater(f"Robots status {resp.status} at {url}", parts.hostname, 900)
            self.frontier.clear_failure(page_url, host_wide=True)
            # This clears a recovered HOST budget. Page DNS failures have their
            # own URL budget, so reading robots successfully cannot reset them.
            self.robots_cache[origin] = (time.monotonic(), policy)
            self.runtime.scheduler.set_delay(page_url, self.policy_interval(policy))
            self.logger.info("ROBOTS: loaded %s; origin delay %.3fs; declared sitemaps=%s", url, self.policy_interval(policy), policy.site_maps() or [])
            return policy

    def run(self):
        # A claimed URL stays pending on disk until processing succeeds or its
        # retry budget resolves it as unavailable. In-memory ownership is released
        # in finally, including when parsing or metadata handling raises.
        while not self.runtime.stop_event.is_set():
            url = self.frontier.get_tbd_url()
            if url is None:
                return
            completed = False
            release_delay = 0
            reason = ""
            try:
                policy = self.get_robots_policy(url)
                if not policy.can_fetch(self.config.user_agent, url):
                    self.logger.info("ROBOTS: skipping disallowed URL %s", url)
                else:
                    if getattr(self.config, "use_sitemaps", True):
                        # Real Config defaults SITEMAPS to false. It was disabled
                        # in the submitted deployment; this is an optional path.
                        discover_sitemaps(self, url, policy)
                    resp = self.fetch_with_retry(url, self.policy_interval(policy))
                    if resp.status == 200:
                        self.frontier.clear_failure(url)
                        self.frontier.clear_failure(url, host_wide=True)
                    for link in scraper.scraper(url, resp):
                        # Persist discovered links BEFORE marking the parent done.
                        # If interrupted, reprocessing the parent is safe because
                        # the frontier and analytics use unique URL primary keys.
                        self.frontier.add_url(link)
                self.frontier.mark_url_complete(url)
                completed = True
            except ScheduledWait as wait:
                # This is ordinary scheduling, not a failed download. Persist
                # the wait for resumes without spending a retry attempt.
                release_delay = wait.seconds
                reason = "Robots/politeness/Retry-After wait for " + wait.url
                self.frontier.defer_host(urlsplit(wait.url).hostname, wait.seconds, reason)
                self.logger.info("Scheduled %s in %.0fs; other work remains eligible", url, wait.seconds)
            except RetryLater as error:
                # note_failure persists both the attempt count and next deadline.
                # Reaching the budget marks unavailable, never a successful page.
                next_delay = self.frontier.note_failure(url, error)
                reason = str(error)
                if next_delay is None:
                    reason = "unavailable: " + reason
                    self.logger.warning(
                        "Retry budget exhausted; %s %s unavailable during this crawl: %s",
                        "host" if error.host_wide else "URL",
                        error.host if error.host_wide else url, error,
                    )
                else:
                    release_delay = next_delay
                    self.logger.warning(
                        "Deferred %s %s for %.0fs; other work remains eligible: %s",
                        "host" if error.host_wide else "URL",
                        error.host if error.host_wide else url, release_delay, error,
                    )
            except CrawlStopped:
                # Controlled shutdown is distinct from a crawl/programming error.
                return
            except Exception as error:
                # A programming/policy error must be visible, not silently lose a URL.
                reason = str(error)
                self.logger.exception("Stopping crawl; %s remains pending", url)
                self.runtime.fail(error)
                self.frontier.stop()
                return
            finally:
                if not completed:
                    # Do not strand a family's active job slot after an error.
                    self.frontier.release_url(url, release_delay, reason)
