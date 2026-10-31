"""Offline checks on cache-provided bytes; this module never downloads."""
# URL filtering alone cannot identify extensionless binary files, mislabeled
# payloads, empty pages, or HTTP-200 error bodies. These checks inspect actual
# response evidence before indexing. Their thresholds/signatures are heuristics,
# not a guarantee that every accepted page is useful or correctly classified.
import re

MAX_HTML_BYTES = 5 * 1024 * 1024
HTML_MARKER = re.compile(
    br"<\s*(?:!doctype\s+html|html\b|head\b|body\b|title\b|p\b|div\b|a\b|ul\b|li\b|h[1-6]\b|table\b|pre\b)",
    re.IGNORECASE,
)


def html_rejection(content, headers):
    """Return a reason for rejecting a body, or None for plausible HTML.

    Missing Content-Type is allowed only with positive HTML evidence. A
    text/html label never overrides binary signatures or control bytes.
    """
    if not content or not content.strip():
        return "Empty response body"
    if len(content) > MAX_HTML_BYTES:
        return "HTML exceeds 5 MiB processing limit"
    headers = {str(k).lower(): str(v) for k, v in headers.items()}
    kind = headers.get("content-type", "").split(";", 1)[0].strip().lower()
    if kind and kind not in {"text/html", "application/xhtml+xml"}:
        return "Non-HTML Content-Type: " + kind
    sample = content[:65536]
    # Examine a bounded prefix rather than scanning arbitrarily large binary
    # content. The complete body's processing-size limit was checked above.
    if sample.startswith((b"\x7fELF", b"MZ", b"PK\x03\x04", b"\x1f\x8b", b"%PDF-", b"\x89PNG", b"\xff\xd8\xff")):
        return "Binary file signature"
    # UTF-16/32 HTML legitimately contains NUL bytes; require a BOM and
    # decode it before testing control characters and HTML evidence.
    if sample.startswith((b"\xff\xfe", b"\xfe\xff", b"\x00\x00\xfe\xff")):
        encoding = "utf-32" if sample.startswith((b"\xff\xfe\x00\x00", b"\x00\x00\xfe\xff")) else "utf-16"
        try:
            sample = content.decode(encoding).encode("utf-8")[:65536]
        except UnicodeError:
            return "Invalid Unicode byte-order-mark encoding"
    if b"\x00" in sample:
        return "Binary NUL bytes in response"
    controls = sum(byte < 32 and byte not in (9, 10, 12, 13) for byte in sample)
    if controls > max(2, len(sample) * 0.01):
        return "Excess binary control characters"
    if not kind and not HTML_MARKER.search(sample):
        # A missing MIME header is not an automatic rejection, but it requires
        # positive markup evidence. A source template containing HTML can still
        # pass this heuristic; the report review documents the pubs.ff example.
        return "Missing Content-Type and no HTML evidence"
    return None


def soft_error_reason(soup, text, word_count):
    """Conservative signatures; do not confuse long articles with errors."""
    title = soup.title.get_text(" ", strip=True).casefold() if soup.title else ""
    # Combine recognizable titles with short content. Rejecting every document
    # containing "404" or "login" would discard tutorials discussing those terms.
    if word_count < 250 and re.fullmatch(
        r"(?:404(?:\s+error)?(?:\s*[:-]?\s*page)?\s+not\s+found|"
        r"404|page\s+not\s+found|403(?:\s+forbidden)?|access\s+denied|"
        r"internal\s+server\s+error|service\s+unavailable)(?:\s*[-|:].*)?",
        title,
    ):
        return "Soft HTTP error page: " + title
    if word_count < 500 and re.search(r"\b(?:log\s*in|sign\s*in)\b", title):
        if soup.find("input", attrs={"type": re.compile("^password$", re.I)}):
            return "Login page instead of public content"
    return None
