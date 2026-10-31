# PAGE PROCESSING AND LINK DISCOVERY
# Worker calls scraper(requested_url, cached_response) after a polite download.
# This module reads that response only; it never opens a network connection.
# Its two outputs are persisted page statistics and a list of allowed links.
# Frontier, rather than this module, decides when those links are downloaded.
#
# Two identities matter: the requested URL is the report's page identity, while
# the final response URL is the base for resolving links after a redirect.
# Similar content at two requested URLs still counts as two accepted pages.
import re
from urllib.parse import urlparse, urljoin, urldefrag, parse_qs, parse_qsl, urlencode, unquote_plus
from requests.utils import requote_uri
from analytics import record_page, record_rejected_page
from response_filters import is_access_block
from similarity import record_similarity
from bs4 import BeautifulSoup, ParserRejectedMarkup
from content_validation import html_rejection, soft_error_reason

def _wiki_endpoint(parsed):
    """Limit this policy to the observed DokuWiki site and PHP endpoint."""
    if parsed.hostname not in {"wiki.ics.uci.edu", "swiki.ics.uci.edu"}:
        return None
    match = re.search(r"(?:^|/)doku\.php(?=/|$)", parsed.path, re.IGNORECASE)
    return parsed.path[:match.end()] if match else None


def prepare_wiki_link(url):
    """Share one namespace-index request across all article toolbar links.

    Ordinary article URLs are unchanged. DokuWiki's idx selects the index
    action regardless of article ID; retaining that ID creates a cross product.
    This selects which links to request, not how already-crawled URLs are counted.
    """
    try:
        parsed = urlparse(url)
        # This is a discovery policy for known wiki controls, not a general
        # URL canonicalizer. Keep meaningful article parameters elsewhere.
        # History/export URLs select the current Trac article for discovery.
        # Already fetched URLs are never merged in the statistics database.
        if (parsed.hostname == "grape.ics.uci.edu"
                and re.match(r"^/wiki/[^/]+/wiki(?:/|$)", parsed.path)):
            controls = {"action", "version", "old_version", "format"}
            pieces = parsed.query.split("&")
            remaining = [piece for piece in pieces
                         if unquote_plus(piece.partition("=")[0]).lower() not in controls]
            if remaining != pieces:
                return parsed._replace(query="&".join(remaining)).geturl()
        endpoint = _wiki_endpoint(parsed)
        if endpoint is None:
            return url
        query = parse_qs(parsed.query, keep_blank_values=True)
        if set(query) & {"s", "s[]"}:
            # Search highlighting changes presentation rather than the article.
            # Removing only these keys avoids a separate crawl per search term.
            pieces = [piece for piece in parsed.query.split("&")
                      if unquote_plus(piece.partition("=")[0]) not in {"s", "s[]"}]
            return prepare_wiki_link(parsed._replace(query="&".join(pieces)).geturl())
        actions = [value.lower() for value in query.get("do", [])]
        if any(value not in {"", "show", "index"} for value in actions):
            return url
        if set(query) & {"rev", "at", "sectok"}:
            return url
        if "idx" not in query and "index" not in actions:
            return url
        parameters = [("do", "index")]
        # Namespace indexes remain useful for discovering article links; remove
        # the unrelated article ID that otherwise multiplies index requests.
        namespace = query.get("idx", [""])[0]
        if namespace:
            parameters.append(("idx", namespace))
        return parsed._replace(
            path=endpoint, params="", query=urlencode(parameters), fragment=""
        ).geturl()
    except (TypeError, ValueError, AttributeError):
        return url


def scraper(url, resp):
    # Recheck scope at the public scraper boundary as well as during extraction.
    # Returning a URL does not fetch it: Worker adds it to the saved frontier.
    links = extract_next_links(url, resp)
    return [link for link in links if is_valid(link)]

def is_calendar_navigation(url):
    """Identify archive navigation, including query-only Tribe Events views."""
    parsed = urlparse(url)
    query = {key.lower(): [value.lower() for value in values]
             for key, values in parse_qs(parsed.query, keep_blank_values=True).items()}
    event_path = bool(re.search(r"/events(?:/|$)", parsed.path, re.I))
    # Both pretty paths and query-only WordPress views generated calendar traps.
    # Restrict this rule to event navigation so dates in real articles survive.
    event_query = ("tribe_events" in query.get("post_type", [])
                   or "eventdisplay" in query)
    if not (event_path or event_query):
        return False
    # WordPress p identifies an individual post; do not discard an event's
    # explicitly identified detail view merely because it includes a date.
    if any(value.isdigit() and int(value) > 0 for value in query.get("p", [])):
        return False
    if {"tribe-bar-date", "eventdate"} & query.keys():
        return True
    if set(query.get("eventdisplay", [])) & {
        "day", "week", "month", "list", "past", "upcoming", "photo", "map"
    }:
        return True
    if event_query and ("paged" in query or re.search(r"/page/\d+(?:/|$)", parsed.path)):
        return True
    return bool(re.search(r"(?:^|/)\d{4}-\d{2}(?:-\d{2})?(?:/|$)", parsed.path))


def extract_next_links(url, resp):
    """Validate cached content, record text analytics, and return allowed links."""
    if resp.status in {301, 302, 303, 307, 308} and resp.raw_response is not None:
        # Some cache replies expose the redirect rather than following it.
        # Schedule its destination through the same cache/frontier policy.
        location = resp.raw_response.headers.get("Location")
        if location:
            try:
                target = requote_uri(urldefrag(urljoin(resp.url or url, location))[0])
                return [target] if is_valid(target) else []
            except (TypeError, ValueError):
                return []
        return []
    if resp.status != 200 or resp.raw_response is None:
        # A cache error, HTTP error, or missing body is not an indexable page.
        # Retry classification belongs to Worker, before this parsing stage.
        return []
    raw = resp.raw_response
    rejection = html_rejection(raw.content, raw.headers)
    # Check bytes and declared type before building an expensive parse tree.
    # Rejection evidence is kept separately from accepted-page report rows.
    if rejection:
        record_rejected_page(urldefrag(url)[0], rejection)
        return []

    # Resolve links against the destination, but keep requested URLs distinct
    # for the assignment's URL-based analytics (including redirect aliases).
    record_url = urldefrag(url)[0]
    page_url = getattr(raw, "url", None) or resp.url or url
    if not is_valid(page_url):
        return []
    
    # lxml recovers malformed numeric entities that crashed html.parser.
    # A second parser is attempted only if the primary rejects the document.
    try:
        soup = BeautifulSoup(raw.content, "lxml")
    except (ValueError, ParserRejectedMarkup):
        try:
            soup = BeautifulSoup(raw.content, "html.parser")
        except (ValueError, ParserRejectedMarkup) as error:
            record_rejected_page(record_url, f"Unparseable HTML: {error}")
            return []
    # Remove scripts, styles, and templates before counting words.
    for element in soup(["script", "style", "template"]):
        element.decompose()

    # Get page text with spaces between HTML elements.
    # A separator prevents adjacent tags such as </p><p> from joining two words.
    # This is parsed document text, not a browser-rendered visibility analysis.
    text = soup.get_text(separator=" ", strip=True)

    if is_access_block(text):
        record_rejected_page(record_url, "Geographic access-block response")
        print(f"BLOCKED RESPONSE: {page_url}")
        return []

    # Convert text into lowercase English words.
    # Whole alphabetic tokens: do not turn course code 262P into word 'p'.
    # Single-letter English words (a, I) still count before stop-word filtering.
    words = re.findall(
        r"(?<![a-z0-9_])[a-z]+(?:'[a-z]+)*(?![a-z0-9_])",
        text.lower().replace("’", "'").replace("‘", "'"),
    )
    rejection = soft_error_reason(soup, text, len(words))
    # A status of 200 alone is insufficient: empty/error/login responses can
    # still be returned successfully by a server. The checks are conservative.
    if not words:
        rejection = "No alphabetic words after removing HTML markup"
    if len(raw.content) >= 512 * 1024 and len(words) < 100:
        rejection = "Large HTML response with fewer than 100 words"
    if rejection:
        record_rejected_page(record_url, rejection)
        if rejection.startswith(("No alphabetic words", "Large HTML response")):
            # A textless navigation page may lead to useful text elsewhere.
            # Discover its links even though it contributes no report row.
            return links_from_soup(soup, page_url)
        return []

    # Save the URL, hostname, word count, and word frequencies.
    # Count before similarity checking: exact or near similarity must not merge
    # distinct URLs in the assignment's unique-page count or word frequencies.
    # Stop words are retained here; only the report's top-50 calculation filters
    # them. Longest-page word counts therefore include ordinary function words.
    record_page(record_url, words, evidence={
        "final_url": page_url, "byte_count": len(raw.content),
        "content_type": raw.headers.get("Content-Type", ""),
    })
    match = record_similarity(record_url, text, words)
    # A near match is diagnostic, not proof that the pages are interchangeable.
    # In particular, a SimHash distance of zero is not an exact-text guarantee.
    if match:
        print(
            f"SIMILARITY: {match['kind']} | "
            f"{page_url} matches {match['url']} | "
            f"distance={match['distance']}"
        )
    links = set(links_from_soup(soup, page_url))

    if match and match["kind"] == "exact":
        # A repeated index can expose fresh article links, so retain those.
        # Only stop growth of the same pagination family, not all outlinks.
        links = {link for link in links
                 if not same_pagination_family(page_url, link)}

    # For matching calendar views on the same host, stop following
    # further dated navigation. Keep other links, including event details.
    if (
        match
        and is_calendar_navigation(page_url)
        and is_calendar_navigation(match["url"])
        and urlparse(page_url).hostname
            == urlparse(match["url"]).hostname
    ):
        links = {
            link for link in links
            if not is_calendar_navigation(link)
        }

        print(
            f"TRAP FILTER: suppressed dated calendar navigation "
            f"from {page_url}"
        )

    return sorted(links)

def same_pagination_family(current, candidate):
    """Recognize further pagination of one exact-duplicate index."""
    # Compare the non-paging path/parameters too. Otherwise a repeated index
    # could accidentally suppress a different category or a new article.
    a, b = urlparse(current), urlparse(candidate)
    if (a.scheme, a.netloc) != (b.scheme, b.netloc):
        return False
    paging = {"page", "paged", "offset", "start"}
    if not (paging & {key.lower() for key, value in parse_qsl(b.query)}
            or re.search(r"/page/\d+(?:/|$)", b.path)):
        return False
    def identity(parts):
        path = re.sub(r"/page/\d+(?=/|$)", "", parts.path).rstrip("/")
        other = sorted((key, value) for key, value in parse_qsl(parts.query, keep_blank_values=True)
                       if key.lower() not in paging)
        return path, other
    return identity(a) == identity(b)


def links_from_soup(soup, page_url):
    """Extract allowed absolute ASCII links from already downloaded HTML."""
    # Resolve /absolute-path, ../relative, and protocol-relative hrefs against
    # the final page URL, unless the document supplies its own <base href>.
    base_url = page_url
    
    base_tag = soup.find("base", href=True)
    if base_tag:
        base_url = urljoin(page_url, base_tag["href"])
        
    links = set()
    for tag in soup.find_all("a", href=True):
        try:
            # Order matters: resolve first, remove #fragment, encode for an
            # ASCII request, then apply wiki and scope/trap policies.
            absolute_url = urljoin(base_url, tag["href"].strip())
            absolute_url = urldefrag(absolute_url)[0]
            # encode unicode characters for an ascii request url
            absolute_url = requote_uri(absolute_url)
            absolute_url = prepare_wiki_link(absolute_url)
            if absolute_url.isascii() and is_valid(absolute_url):
                links.add(absolute_url)
        except(TypeError, ValueError):
            continue
    
    # Sorting makes traversal reproducible; the set removes repeated links
    # within this one document. Frontier handles cross-document URL duplicates.
    return sorted(links)


def is_valid(url):
    # This is a pre-download policy, not a claim that a URL will return HTML.
    # Apply it both to new links and saved pending links on resume. Body/type
    # validation is still necessary after a cache response is received.
    try:
        parsed = urlparse(url)
        if is_calendar_navigation(url):
            return False
        # Simple structural trap checks, documented in the run guide.
        if len(url) > 2048 or len(parsed.path.split("/")) > 30:
            # Structural bounds limit runaway generated URLs. They are explicit
            # heuristics, not a measurement of distance from a seed page.
            return False
        segments = [piece for piece in parsed.path.split("/") if piece]
        if any(segments.count(piece) >= 4 for piece in set(segments)):
            return False
        if (
            not isinstance(url, str) or not url.isascii()
            or any(ord(c) < 33 or ord(c) == 127 for c in url)
            or parsed.username is not None or parsed.password is not None
            or parsed.port not in (None, 80, 443)
        ):
            return False
        if parsed.path.lower().rstrip("/").endswith("/wp-login.php"):
            return False
        if parsed.path.lower() == "/wp-admin" or parsed.path.lower().startswith("/wp-admin/"):
            return False
        query = parse_qs(parsed.query, keep_blank_values=True)
        if len(query) > 12:
            return False
        if parsed.hostname == "grape.ics.uci.edu":
            if re.match(r"^/wiki/[^/]+/timeline(?:/|$)", parsed.path):
                return False
            if url != prepare_wiki_link(url):
                return False
        # Share/reply views repeat articles; article URLs remain discoverable.
        if {key.lower() for key in query} & {"share", "replytocom"}:
            return False
        if parsed.path.lower().rstrip("/").rsplit("/", 1)[-1] in {"makefile", "cmakelists.txt"}:
            return False
        if _wiki_endpoint(parsed) is not None:
            # Request current article views, not edit/login/export/history tools.
            actions = [value.lower() for value in query.get("do", [])]
            if any(value not in {"", "show", "index"} for value in actions):
                return False
            if set(query) & {"rev", "at", "sectok", "s", "s[]"}:
                return False
            # Keep index-based article discovery, but only one URL per namespace.
            if "idx" in query or "index" in actions:
                if url != prepare_wiki_link(url):
                    return False
        if any(v.lower() in {"true", "1"} for v in query.get("preview", [])):
            return False
        # Skip calendar export URLs.
        query_keys = {
            key.lower()
            for key in parse_qs(parsed.query, keep_blank_values=True)
        }

        if query_keys & {"outlook-ical", "ical"}:
            return False
        # Skip directory-listing links that only change sorting.
        if re.fullmatch(
            r"(?:C=[NMSD][;&]O=[AD]|O=[AD][;&]C=[NMSD])",
            parsed.query,
            flags=re.IGNORECASE,
        ):
            return False
        if parsed.scheme not in set(["http", "https"]):
            return False
        allowed_domains = (
            "ics.uci.edu",
            "cs.uci.edu",
            "informatics.uci.edu",
            "stat.uci.edu",
        )
        hostname = parsed.hostname
        # Skip DokuWiki attachment downloads, including images.
        if (
            hostname in {"wiki.ics.uci.edu", "swiki.ics.uci.edu"}
            and parsed.path.lower() == "/lib/exe/fetch.php"
        ):
            return False
        if not hostname:
            return False
        if not any(
            hostname == domain or hostname.endswith("." + domain)
            for domain in allowed_domains
        ):
            # The leading dot is essential: fakeics.uci.edu must not match
            # ics.uci.edu, nor may ics.uci.edu.example.org match the scope.
            return False

        # Inspect the path so an image.pdf?download=1 cannot evade filtering.
        # This extension list is a heuristic; missing extensions and misleading
        # headers still require content_validation.py. It does not prevent all
        # low-text HTML galleries or source-like HTML templates (see review).
        return not re.match(
            r".*\.(css|js|bmp|gif|jpe?g|ico|c|cc|cpp|cxx|h|hh|hpp|hxx|java|py|class|o|a|so|wasm|img|imgz|bin"
            + r"|png|tiff?|mid|mp2|mp3|mp4"
            + r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
            + r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names"
            + r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
            + r"|epub|dll|cnf|tgz|sha1"
            # Skip virtual-machine appliances and disk images.
            + r"|ova|ovf|vmdk|vdi|vhdx?|qcow2?|vbox"
            + r"|thmx|mso|arff|rtf|jar|csv"
            + r"|rm|smil|wmv|swf|wma|zip|rar|gz)$", parsed.path.lower())

    except (TypeError, ValueError, AttributeError):
        return False
