# REPRODUCIBLE OFFLINE REPORT FROM ACCEPTED-PAGE STATISTICS
# 1. Count rows keyed by fragmentless requested URL (not distinct fingerprints).
# 2. Compare stored word_count, which includes stop words but excludes markup.
# 3. Sum per-page TOKEN frequencies, remove the chosen stop-word list, sort top 50.
# 4. Group accepted URLs by ICS subdomain hostname and sort alphabetically.
#
# This command reads saved results; generating a report never launches a crawl.
# Acceptance heuristics can still admit low-text/source-like pages. The report
# records those observations; REVIEW_NOTES.txt discusses this snapshot's limits.
import argparse
import os
import json
import sqlite3
from collections import Counter
from contextlib import closing
from pathlib import Path


def main():
    database = Path("crawl_stats.sqlite3")

    if not database.exists():
        print("No crawl statistics found yet.")
        return

    stop_words = set(
        Path("stopwords.txt").read_text(encoding="utf-8").lower().split()
    )
    # Apply stop words only to the top-50 answer. Applying them during crawling
    # would also change the assignment's longest-page word count unnecessarily.

    word_totals = Counter()
    subdomain_totals = Counter()
    domain_totals = Counter()
    page_count = 0
    longest_url = None
    longest_count = -1

    # This is read-only work. closing() releases the connection on every exit;
    # a Connection's own context manager only commits/rolls back transactions.
    with closing(sqlite3.connect(database.resolve().as_uri() + "?mode=ro", uri=True)) as connection:
        connection.execute("BEGIN")
        # One read transaction gives the four answers a consistent page-table
        # snapshot even if another terminal is still recording new pages.
        if not connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='pages'"
        ).fetchone():
            print("No accepted HTML pages recorded yet.")
            return
        rows = connection.execute("""
            SELECT url, hostname, word_count, frequencies
            FROM pages
            ORDER BY url
        """)

        for url, hostname, word_count, frequencies in rows:
            # A UNIQUE/PRIMARY KEY URL in analytics.py already prevents counting
            # the same accepted request twice after retries or process resumes.
            page_count += 1
            for domain in ("ics.uci.edu", "cs.uci.edu", "informatics.uci.edu", "stat.uci.edu"):
                if hostname == domain or hostname.endswith("." + domain):
                    domain_totals[domain] += 1
                    break

            if word_count > longest_count:
                longest_url = url
                longest_count = word_count

            word_totals.update(json.loads(frequencies))
            # A word repeated on many similar pages still contributes on each
            # distinct URL. Similarity does not deduplicate the report corpus.

            # Count subdomains; exclude the parent hostname itself.
            if hostname.endswith(".ics.uci.edu"):
                # Combine HTTP/HTTPS by hostname for this grouping only. Their
                # URL rows remain separate, and ics.uci.edu itself is not one
                # of its own subdomains. Deeper hostnames retain their full name.
                subdomain_totals[hostname] += 1

    # Sort by decreasing frequency, then alphabetically for ties.
    top_words = sorted(
        # Alphabetical tie-breaking makes equal-frequency output deterministic.
        (
            (word, count)
            for word, count in word_totals.items()
            if word not in stop_words
        ),
        key=lambda item: (-item[1], item[0]),
    )[:50]

    lines = [
        "CRAWL REPORT",
        "",
        "1. Unique successfully processed HTML URLs",
        str(page_count),
        "",
        "2. Longest page",
    ]

    if longest_url is not None:
        lines.extend([
            longest_url,
            f"Word count: {longest_count}",
        ])
    else:
        lines.append("No pages recorded.")

    lines.extend([
        "",
        "3. Most common words, excluding stop words",
    ])

    for word, count in top_words:
        lines.append(f"{word}, {count}")

    lines.extend([
        "",
        "4. Subdomains under ics.uci.edu",
        f"Number of subdomains: {len(subdomain_totals)}",
        "HTTP and HTTPS page counts are combined by hostname.",
        "HTTPS is used below as a display prefix.",
    ])

    for hostname, count in sorted(subdomain_totals.items()):
        lines.append(f"https://{hostname}, {count}")

    lines.extend([
        "", "5. Counting method and coverage",
        "Pages are accepted, nonempty HTML responses with readable alphabetic text.",
        "URL identity is the requested URL with the fragment removed.",
        "HTTP/HTTPS, trailing-slash variants and distinct redirect aliases remain separate.",
        "Content similarity does not merge or remove distinct accepted page URLs.",
        "Words are whole ASCII alphabetic tokens, allowing internal apostrophes;",
        "curly apostrophes are normalized. Mixed codes such as CS171 and 262P are excluded.",
        "Longest-page counts include stop words; top-50 frequencies exclude stopwords.txt.",
        "Calendar navigation, wiki UI/history variants and raw source files are filtered before fetching.",
        "Empty, binary, non-HTML, recognized error/login and large low-text responses are excluded.",
        "See FRESH_CRAWL_GUIDE.md for limits and rejected_responses for recorded rejection reasons.",
        "Accepted HTML URLs by domain family:",
    ])
    for domain in ("ics.uci.edu", "cs.uci.edu", "informatics.uci.edu", "stat.uci.edu"):
        lines.append(f"{domain}, {domain_totals[domain]}")
    lines.extend([
        "", "This report describes this run only. Frontier completion is not proof",
        "that every reachable page was discovered. Include final_crawl_status.txt",
        "and discuss traps, failures and any early stopping in your submission.",
    ])
    report = "\n".join(lines) + "\n"
    Path("report.txt").write_text(report, encoding="utf-8")
    # Only the generated text file is written; frontier/statistics stay intact.
    print(report)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate a report from saved statistics only")
    parser.add_argument("--run-dir", help="Use one isolated crawl's statistics and stop words")
    args = parser.parse_args()
    if args.run_dir:
        run = Path(args.run_dir).resolve()
        if not run.is_dir():
            raise ValueError("Run directory does not exist")
        os.chdir(run)
    main()
