# COURSE-CACHE TRANSPORT ONLY
# The HTTP destination here is the cache's assigned host/port. The target page
# URL is carried in q and the student/group user agent in u. Do not replace this
# with a direct request to the target site: the cache is part of the assignment.
#
# There are two responses: the outer cache HTTP reply and the serialized origin
# Response inside it. An outer 503 is a cache transport problem; an inner 503
# describes the requested website. Worker treats these failures differently.
import requests
import cbor
import pickle
from urllib.parse import urlsplit

from utils.response import Response

def download(url, config, logger=None):
    # Validate every metadata/page/retry URL before sending it to the cache.
    # This is a scope check, not a file-extension policy: sitemap.gz is valid
    # metadata even though ordinary compressed downloads are not page links.
    from scraper import is_valid
    if not isinstance(url, str):
        raise requests.exceptions.InvalidURL("Request URL must be a string")
    try:
        parts = urlsplit(url)
        port = parts.port
    except (TypeError, ValueError) as error:
        raise requests.exceptions.InvalidURL("Malformed request URL") from error
    if (not url.isascii()
            or any(ord(c) < 33 or ord(c) == 127 for c in url)
            or parts.fragment or parts.username is not None
            or parts.password is not None or port not in (None, 80, 443)
            or not is_valid(f"{parts.scheme}://{parts.netloc}/")):
        raise requests.exceptions.InvalidURL("URL is outside the crawl request policy")
    host, port = config.cache_server
    # Registration may assign a port different from config.ini's registration
    # port. Always use the returned cache endpoint for actual downloads.
    # The timeout tuple is connect/read INACTIVITY timeout, not a strict total
    # wall-clock deadline or a maximum size for a slowly arriving response.
    resp = requests.get(
        f"http://{host}:{port}/",
        params=[("q", url), ("u", config.user_agent)],
        timeout=(10, 60),
    )
    if resp.status_code != 200 and resp.status_code not in {408, 429} and resp.status_code < 500:
        raise requests.exceptions.RequestException(
            f"Cache endpoint returned HTTP {resp.status_code}; not an origin response"
        )
    if resp.status_code == 200:
        try:
            # Decode only the course cache's wrapper, then adapt it to the
            # starter framework's Response interface used by the scraper.
            result = Response(cbor.loads(resp.content))
            if not (result.status == 200 and result.raw_response is None):
                return result
        except (EOFError, ValueError, TypeError, KeyError, pickle.UnpicklingError):
            pass
    # An outer HTTP error or invalid cache envelope must not consume an
    # origin URL's failure budget, or be counted as a downloaded page.
    result = Response({"url": url, "status": resp.status_code,
                       "error": f"Cache transport/envelope failure: HTTP {resp.status_code}"})
    result.cache_transport_error = True
    # This explicit marker prevents malformed envelopes from consuming a
    # website's bounded failure budget or contributing accepted-page statistics.
    result.raw_response = resp  # Preserve cache Retry-After for the worker.
    return result
