# SHARED THREAD RUNTIME AND ACTUAL-REQUEST POLITENESS
# Frontier controls which job a worker owns; this scheduler controls when a
# network request may start. Every cache request goes through the same shared
# scheduler, including robots, optional sitemaps, and retry requests.
#
# The final supplied crawl used THREADCOUNT=1. These synchronization mechanisms
# also support multiple workers; their presence does not mean that deployment
# ran with several threads. Separate domain families may overlap when enabled.
import math
import threading
import time
from contextlib import contextmanager
from urllib.parse import urlsplit

DOMAINS = ("ics.uci.edu", "cs.uci.edu", "informatics.uci.edu", "stat.uci.edu")


class CrawlStopped(Exception):
    """Wake sleeping workers when the user or another worker stops the crawl."""


def domain_key(url):
    """HTTP/HTTPS and sibling subdomains conservatively share one gate."""
    host = (urlsplit(url).hostname or "").lower()
    # Deliberately group www.ics.uci.edu and wiki.ics.uci.edu with ics.uci.edu,
    # across HTTP/HTTPS. This is stricter than timing only identical hostnames.
    for domain in DOMAINS:
        if host == domain or host.endswith("." + domain):
            return domain
    return host


class ScheduledWait(Exception):
    """A normal politeness/cooldown wait; never an origin failure attempt."""
    def __init__(self, url, seconds):
        super().__init__(f"Scheduled wait for {url}: {seconds:.1f}s")
        self.url, self.seconds = url, seconds


def origin_key(url):
    # Origin-specific robots delays/cooldowns should not freeze every sibling
    # host. These keys retain scheme and port while domain_key groups families.
    parts = urlsplit(url)
    return (parts.scheme.lower(), parts.hostname, parts.port or (443 if parts.scheme == "https" else 80))


class RequestScheduler:
    """Share the 500ms family gate, but keep longer rules per origin.

    The family gate covers the actual complete cache call. A robots delay on
    one host must never become the permanent minimum for every ICS subdomain.
    Long normal waits yield to the frontier instead of monopolizing a worker.
    """
    def __init__(self, delay=0.5, stop_event=None):
        if not math.isfinite(delay) or delay < 0:
            raise ValueError("POLITENESS must be finite and nonnegative")
        self.minimum = max(0.5, delay)
        self.stop_event = stop_event or threading.Event()
        self.condition = threading.Condition()
        # Conditions guard check-and-reserve as one atomic action. Merely giving
        # every worker time.sleep(0.5) would allow simultaneous request starts.
        self.states, self.origins = {}, {}
        self.cache_ready = 0.0
        # All in-memory deadlines use a monotonic clock, which is unaffected by
        # wall-clock adjustments. Persistent frontier deadlines use epoch time.

    def _state(self, key):
        return self.states.setdefault(key, {"busy": False, "ready": 0.0})

    def _origin(self, url):
        return self.origins.setdefault(origin_key(url), {
            "ready": 0.0, "interval": self.minimum, "last_finish": 0.0,
        })

    def set_delay(self, url, delay):
        if not math.isfinite(delay) or delay < 0:
            raise ValueError("Invalid request interval")
        with self.condition:
            state = self._origin(url)
            state["interval"] = max(state["interval"], delay)
            # A robots delay can tighten the requirement, never weaken it.
            if state["last_finish"]:
                state["ready"] = max(state["ready"], state["last_finish"] + state["interval"])
            self.condition.notify_all()

    def postpone(self, url, seconds):
        """Publish an origin's Retry-After while its request permit is held."""
        with self.condition:
            state = self._origin(url)
            state["ready"] = max(state["ready"], time.monotonic() + seconds)
            self.condition.notify_all()

    def postpone_cache(self, seconds):
        """Failure to reach the cache applies to every domain, not one URL."""
        with self.condition:
            self.cache_ready = max(self.cache_ready, time.monotonic() + seconds)
            self.condition.notify_all()

    @contextmanager
    def request(self, url, delay=None, yield_long_wait=False):
        # Reserve while holding the condition lock, then release that lock
        # during network I/O. family['busy'] keeps same-family requests serial;
        # other families can still enter their own permits concurrently.
        with self.condition:
            family, origin = self._state(domain_key(url)), self._origin(url)
            if delay is not None:
                self.set_delay(url, delay)
            while True:
                if self.stop_event.is_set():
                    raise CrawlStopped()
                remaining = max(family["ready"], origin["ready"], self.cache_ready) - time.monotonic()
                # This occurs before taking the permit, so other work can run.
                if yield_long_wait and remaining > 1.0:
                    # Worker persists this ordinary wait and releases its job.
                    # It is not counted as a failed download or retry attempt.
                    raise ScheduledWait(url, remaining)
                if not family["busy"] and remaining <= 0:
                    family["busy"] = True
                    break
                self.condition.wait(0.2 if family["busy"] else min(max(remaining, 0.01), 0.2))
        try:
            yield
        finally:
            # Even exceptions release the permit. Spacing from COMPLETION is
            # conservative: the next same-family call starts at least 500 ms
            # after this call ends, not merely 500 ms after its start.
            with self.condition:
                now = time.monotonic()
                family["busy"] = False
                family["ready"] = max(family["ready"], now + self.minimum)
                origin["last_finish"] = now
                origin["ready"] = max(origin["ready"], now + origin["interval"])
                self.condition.notify_all()


class SharedRuntime:
    # Construct exactly one runtime per Frontier and share it with all workers.
    # Locks/caches here protect metadata discovery, not the persistent URL table.
    def __init__(self, config):
        self.stop_event = threading.Event()
        self.scheduler = RequestScheduler(config.time_delay, self.stop_event)
        self.metadata_lock = threading.RLock()
        self.robots_cache = {}
        self.sitemaps_seen = set()
        self.sitemap_origins = set()
        self._locks = {}
        self.errors = []

    def lock_for(self, kind, key):
        """Single-flight metadata loading; no global lock is held over I/O."""
        with self.metadata_lock:
            return self._locks.setdefault((kind, key), threading.RLock())

    def wait(self, seconds):
        # Event.wait makes a long retry sleep interruptible by Ctrl+C/failure.
        # Plain time.sleep would keep shutdown waiting through the entire delay.
        if self.stop_event.wait(seconds):
            raise CrawlStopped()

    def fail(self, error):
        # Unexpected code/policy errors stop visibly and wake peers. Expected
        # origin failures instead stay in the frontier's bounded retry system.
        with self.metadata_lock:
            self.errors.append(str(error))
        self.stop_event.set()
