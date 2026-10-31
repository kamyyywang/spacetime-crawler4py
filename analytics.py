# ACCEPTED-PAGE ANALYTICS
# The report reads this database, not frontier completion counts. A completed
# frontier row may instead represent a denial, an unavailable URL, or a filter.
# Each accepted fragmentless requested URL gets one immutable first-seen row.
import json
from storage import write_database
from collections import Counter
from urllib.parse import urldefrag, urlparse


def record_page(url, words, database="crawl_stats.sqlite3", evidence=None):
    # One page's statistics are stored together in a transaction. Retrying a
    # URL after a crash cannot add its words twice because url is PRIMARY KEY.
    # Treat URLs with different fragments as the same URL.
    url = urldefrag(url)[0]
    hostname = urlparse(url).hostname

    # Count how often each word appears on this page.
    frequencies = json.dumps(dict(Counter(words)))
    # A compact per-page histogram lets the report recompute corpus totals
    # without downloading again or confusing document frequency with token
    # frequency: ten occurrences on one page contribute ten, not one.

    # Open the database; create it automatically if necessary.
    with write_database(database) as connection:
        connection.execute("""
            CREATE TABLE IF NOT EXISTS pages (
                url TEXT PRIMARY KEY,
                hostname TEXT NOT NULL,
                word_count INTEGER NOT NULL,
                frequencies TEXT NOT NULL
            )
        """)

        # Save each URL only once.
        connection.execute("""
            INSERT OR IGNORE INTO pages
                (url, hostname, word_count, frequencies)
            VALUES (?, ?, ?, ?)
        """, (url, hostname, len(words), frequencies))
        if evidence is not None:
            # Keep provenance (final URL, MIME label, size) alongside analytics
            # so suspicious report results can be investigated after the crawl.
            # This does not archive the original raw HTML bytes.
            connection.execute("""
                CREATE TABLE IF NOT EXISTS page_evidence (
                    url TEXT PRIMARY KEY, final_url TEXT,
                    content_type TEXT, byte_count INTEGER
                )
            """)
            connection.execute(
                "INSERT OR IGNORE INTO page_evidence VALUES (?,?,?,?)",
                (url, evidence.get("final_url", url),
                 evidence.get("content_type", ""), evidence.get("byte_count", 0)),
            )

def record_rejected_page(url, reason, database="crawl_stats.sqlite3"):
    """Retain rejected URLs separately from valid HTML content statistics."""
    # Rejections explain why content was excluded. They never increase the
    # accepted-page count, longest-page count, or common-word frequencies.
    url = urldefrag(url)[0]
    with write_database(database) as connection:
        connection.execute("""
            CREATE TABLE IF NOT EXISTS rejected_responses (
                url TEXT PRIMARY KEY, reason TEXT NOT NULL
            )
        """)
        connection.execute(
            "INSERT OR REPLACE INTO rejected_responses VALUES (?,?)", (url, reason)
        )
