# OPTIONAL SITEMAP DISCOVERY
# Implemented here, but SITEMAPS=false in the supplied final crawl config.
# This path can discover pages that ordinary hyperlinks miss. Sitemap URLs,
# redirects and page entries still obey scope, robots and shared request timing.
# XML/gzip metadata is not recorded as an HTML page in the report.
import gzip
import io
from urllib.parse import urlsplit, urljoin, urldefrag
from xml.etree.ElementTree import ParseError
from requests.utils import requote_uri

from robots_support import parse_sitemap
from crawler.errors import RetryLater
from crawler.runtime import ScheduledWait
import scraper


def allowed_sitemap_url(url):
    # Apply origin scope/ASCII checks without the ordinary page-extension filter:
    # an XML or .gz sitemap is metadata even though compressed page downloads
    # are normally excluded. The transport validates the complete request too.
    try:
        parts = urlsplit(url)
        if (
            not url.isascii() or parts.username is not None
            or parts.password is not None or parts.port not in (None, 80, 443)
        ):
            return False
        return scraper.is_valid(f"{parts.scheme}://{parts.netloc}/")
    except (ValueError, TypeError):
        return False


def discover_sitemaps(worker, page_url, policy):
    parts = urlsplit(page_url)
    origin = (parts.scheme, parts.netloc)
    runtime = worker.runtime
    # Do not hold one global metadata lock over I/O to different origins.
    with runtime.lock_for("sitemap-origin", origin):
        with runtime.metadata_lock:
            if origin in worker.sitemap_origins:
                return True
        pending = [(url, 0) for url in (policy.site_maps() or [])]
        # Bound the metadata traversal independently of the page frontier.
        # Index depth <=5 and at most 50 examined candidates limit recursive maps.
        examined = 0
        while pending and examined < 50:
            if runtime.stop_event.is_set():
                from crawler.runtime import CrawlStopped
                raise CrawlStopped()
            candidate, depth = pending.pop()
            examined += 1
            try:
                current = requote_uri(urldefrag(urljoin(page_url, candidate))[0])
            except (ValueError, TypeError):
                continue
            if not allowed_sitemap_url(current):
                continue
            # Different sitemap indexes may refer to the very same child.
            with runtime.lock_for("sitemap-url", current):
                with runtime.metadata_lock:
                    if current in worker.sitemaps_seen:
                        continue
                sitemap_host = urlsplit(current).hostname
                # Reuse host deferrals rather than repeatedly contacting metadata
                # on a host already waiting after an origin/robots failure.
                if worker.frontier.is_host_deferred(sitemap_host):
                    continue
                try:
                    # No origin lock is held by get_robots_policy after it returns.
                    rules = worker.get_robots_policy(current)
                    if not rules.can_fetch(worker.config.user_agent, current):
                        _seen(worker, current)
                        continue
                    response = worker.fetch_with_retry(
                        current, worker.policy_interval(rules), origin_timeout_limit=3
                    )
                except ScheduledWait as wait:
                    worker.frontier.defer_host(urlsplit(wait.url).hostname, wait.seconds, str(wait))
                    worker.logger.info("SITEMAP: deferring optional metadata: %s", wait)
                    continue
                except RetryLater as error:
                    worker.frontier.defer_host(error.host, error.seconds, str(error))
                    if error.host == parts.hostname:
                        raise
                    worker.logger.warning("SITEMAP: deferred metadata host: %s", error)
                    continue
                if response.status != 200:
                    worker.logger.warning("SITEMAP: skipping %s, status %s", current, response.status)
                    _seen(worker, current)
                    continue
                final_url = (getattr(response.raw_response, "url", None)
                             or getattr(response, "url", None) or current)
                if not allowed_sitemap_url(final_url):
                    # A metadata redirect must not expand the crawl's scope.
                    _seen(worker, current)
                    continue
                try:
                    content = response.raw_response.content
                    if len(content) > 5 * 1024 * 1024:
                        raise ValueError("Input sitemap exceeds 5 MiB")
                    if content.startswith(b"\x1f\x8b"):
                        # Limit decompressed output too; a small compressed file
                        # can expand far beyond its original transfer size.
                        with gzip.GzipFile(fileobj=io.BytesIO(content)) as stream:
                            content = stream.read(5 * 1024 * 1024 + 1)
                    kind, links = parse_sitemap(final_url, content)
                    if len(links) > 50000:
                        raise ValueError("Too many sitemap entries")
                except (ParseError, ValueError, OSError, EOFError) as error:
                    worker.logger.warning("SITEMAP: skipping %s: %s", current, error)
                    _seen(worker, current)
                    continue
                if kind == "sitemaps":
                    if depth < 5:
                        pending.extend((url, depth + 1) for url in links)
                    else:
                        worker.logger.warning("SITEMAP: depth limit at %s", current)
                else:
                    for link in links:
                        # Only page entries enter Frontier. It performs its own
                        # validation and URL duplicate check before persisting.
                        try:
                            link = requote_uri(link)
                        except (ValueError, TypeError):
                            continue
                        if link.isascii() and scraper.is_valid(link):
                            worker.frontier.add_url(link)
                _seen(worker, current)
                worker.logger.info("SITEMAP: processed %s, %s entries", current, len(links))
        if pending:
            worker.logger.warning("SITEMAP: reached 50-entry discovery budget for %s", origin)
        with runtime.metadata_lock:
            worker.sitemap_origins.add(origin)
        return True


def _seen(worker, url):
    # Shared bookkeeping avoids repeated child-map downloads across workers
    # during this process. It is not a persistent accepted-page count.
    with worker.runtime.metadata_lock:
        worker.sitemaps_seen.add(url)
