# ROBOTS/SITEMAP PARSING WITHOUT NETWORK I/O
# Worker downloads metadata through the same cache and politeness scheduler as
# ordinary pages. Here RobotFileParser.parse consumes text already downloaded;
# calling its network-reading helper would bypass that architecture.
import xml.etree.ElementTree as ET
from urllib.parse import urljoin, urldefrag
from urllib.parse import urlsplit, urlunsplit
from urllib.robotparser import RobotFileParser


def robots_url_for(page_url):
    # Reset path/query/fragment but retain scheme and authority, so rules for
    # one origin are not silently reused for a different scheme or hostname.
    #Find robots.txt at the root of this URL's origin.
    parsed = urlsplit(page_url)

    return urlunsplit((
        parsed.scheme,
        parsed.netloc,
        "/robots.txt",
        "",
        "",
    ))


def parse_robots(robots_url, text):
    #Parse already-downloaded robots text without making requests.
    policy = RobotFileParser(robots_url)
    policy.parse(text.splitlines())
    return policy

def parse_sitemap(sitemap_url, content):
    """Return ('pages' or 'sitemaps', discovered URLs)."""
    # Set a conservative limit for this crawler.
    if len(content) > 5 * 1024 * 1024:
        raise ValueError("Sitemap exceeds our 5 MiB processing limit.")

    # Sitemaps do not need document type or entity declarations.
    if b"<!DOCTYPE" in content.upper() or b"<!ENTITY" in content.upper():
        raise ValueError("Unsupported XML declarations.")

    root = ET.fromstring(content)
    # urlset lists pages; sitemapindex lists child maps. Keep that distinction
    # so child XML documents are parsed as metadata rather than counted as HTML.

    # Preserve the document's namespace when locating elements.
    namespace = ""
    if root.tag.startswith("{"):
        namespace = root.tag.split("}", 1)[0] + "}"

    root_name = root.tag.rsplit("}", 1)[-1]
    # Sitemaps commonly declare a default XML namespace. Local tag names alone
    # are insufficient for findall() unless we keep the namespace prefix too.

    if root_name == "urlset":
        kind = "pages"
        entry_name = "url"
    elif root_name == "sitemapindex":
        kind = "sitemaps"
        entry_name = "sitemap"
    else:
        raise ValueError("Unrecognized sitemap root element.")

    links = set()

    # Read direct loc children, avoiding image/video metadata links.
    for entry in root.findall(namespace + entry_name):
        location = entry.find(namespace + "loc")

        if location is not None and location.text:
            absolute_url = urljoin(
                sitemap_url, location.text.strip()
            )
            links.add(urldefrag(absolute_url)[0])

    return kind, sorted(links)
