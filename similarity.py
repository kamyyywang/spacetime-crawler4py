# CONTENT SIMILARITY: HANDWRITTEN HASHING AND BIT OPERATIONS
# Exact comparison and near similarity answer different questions. The exact
# path compares whitespace-normalized text; the near path compares weighted
# 64-bit SimHash fingerprints. Neither changes the URL-based report identity.
# A near match is a heuristic and may reflect repeated site navigation.
from storage import write_database
from urllib.parse import urldefrag
def record_similarity(
    url, text, words, database="crawl_stats.sqlite3"
):
    """Save a fingerprint and return an exact/near match, if found."""
    if not words:
        return None

    url = urldefrag(url)[0]

    # Exact comparison preserves case and punctuation, but normalizes spaces.
    normalized = " ".join(text.split())
    exact_hash = f"{stable_hash(normalized):016x}"

    fingerprint = simhash(words)

    # Four 16-bit sections help find candidates without comparing every page.
    bands = [
        (fingerprint >> (16 * index)) & 65535
        for index in range(4)
    ]

    match = None

    with write_database(database) as connection:
        # The lookup and insertion share one transaction, making the "already
        # seen" decision atomic when more than one worker processes a page.
        connection.execute("""
            CREATE TABLE IF NOT EXISTS page_fingerprints_v1 (
                url TEXT PRIMARY KEY,
                normalized_text TEXT NOT NULL,
                exact_hash TEXT NOT NULL,
                fingerprint TEXT NOT NULL,
                b0 INTEGER,
                b1 INTEGER,
                b2 INTEGER,
                b3 INTEGER,
                word_count INTEGER NOT NULL
            )
        """)

        connection.execute("""
            CREATE INDEX IF NOT EXISTS fp_v1_exact
            ON page_fingerprints_v1(exact_hash)
        """)

        for index in range(4):
            connection.execute(
                f"CREATE INDEX IF NOT EXISTS fp_v1_b{index} "
                f"ON page_fingerprints_v1(b{index})"
            )

        # Matching hashes alone are insufficient: compare the text too.
        # Exclude this same URL when processing it again.
        exact = connection.execute("""
            SELECT url
            FROM page_fingerprints_v1
            WHERE exact_hash = ?
              AND normalized_text = ?
              AND url <> ?
            ORDER BY url
            LIMIT 1
        """, (exact_hash, normalized, url)).fetchone()

        if exact:
            match = {
                "kind": "exact",
                "url": exact[0],
                "distance": 0,
            }

        # Short pages are less reliable for near-duplicate comparisons.
        elif len(words) >= 50:
            # With at most three differing bits, at least one of four disjoint
            # 16-bit bands is unchanged. Indexed bands narrow the candidates;
            # Hamming distance and the length test make the final decision.
            # The length ratio is an additional heuristic and can miss matches.
            candidates = connection.execute("""
                SELECT url, fingerprint, word_count
                FROM page_fingerprints_v1
                WHERE url <> ?
                  AND word_count >= 50
                  AND (b0 = ? OR b1 = ? OR b2 = ? OR b3 = ?)
            """, (url, *bands))

            for old_url, old_fingerprint, old_count in candidates:
                # Require reasonably similar document lengths.
                length_ratio = (
                    min(len(words), old_count)
                    / max(len(words), old_count)
                )

                if length_ratio < 0.9:
                    continue

                distance = hamming_distance(
                    fingerprint, int(old_fingerprint, 16)
                )

                # Initial threshold; inspect real matches before tuning.
                if distance <= 3:
                    if match is None or distance < match["distance"]:
                        match = {
                            "kind": "near",
                            "url": old_url,
                            "distance": distance,
                        }

        # Hex strings avoid SQLite's signed 64-bit integer limit.
        connection.execute("""
            INSERT OR IGNORE INTO page_fingerprints_v1
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            url,
            normalized,
            exact_hash,
            f"{fingerprint:016x}",
            *bands,
            len(words),
        ))

    return match

def stable_hash(text):
    # calculate a repeatable 64-bit FNV-1a hash.
    # Python's built-in string hash can change between processes. FNV's fixed
    # offset basis and multiplier make saved fingerprints stable on resume.
    # The mask implements unsigned 64-bit overflow in Python's large integers.
    value = 14695981039346656037
    mask = (1 << 64) - 1

    for byte in text.encode("utf-8"):
        value ^= byte
        value = (value * 1099511628211) & mask

    return value

def simhash(words):
    """Create a 64-bit fingerprint weighted by word frequency."""
    # This is a bag-of-words fingerprint: word order is not represented, and
    # frequent terms have more votes. Exact-text checking above remains needed.
    if not words:
        return None

    # Count word occurrences without importing a counting library.
    frequencies = {}
    for word in words:
        frequencies[word] = frequencies.get(word, 0) + 1

    scores = [0] * 64
    # Each position accumulates a signed vote over all distinct words. A word
    # occurring five times contributes +5 or -5 at each of the 64 positions.

    for word, frequency in frequencies.items():
        fingerprint = stable_hash(word)

        # Each word votes positively or negatively for each bit.
        for bit in range(64):
            if fingerprint & (1 << bit):
                scores[bit] += frequency
            else:
                scores[bit] -= frequency

    # Positive scores become 1; zero and negative scores become 0.
    result = 0
    for bit in range(64):
        if scores[bit] > 0:
            result |= 1 << bit

    return result


def hamming_distance(first, second):
    """Count differing bits between two nonempty-page fingerprints."""
    if first is None or second is None:
        raise ValueError("Cannot compare an empty-page fingerprint.")

    difference = first ^ second
    # XOR is 1 exactly where the two fingerprints differ. Counting those set
    # bits gives a distance from 0 (same fingerprint) to 64 (all bits differ).
    count = 0

    # Remove one set bit per iteration.
    while difference:
        difference &= difference - 1
        count += 1

    return count
