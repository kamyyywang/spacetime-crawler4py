# These exceptions communicate scheduling decisions, not successful downloads.
# RetryLater is expected and resumable; CacheError is a visible stop condition.
class RetryLater(Exception):
    def __init__(self, message, host, seconds=300, host_wide=True):
        super().__init__(message)
        self.host = host
        self.seconds = seconds
        # seconds is a minimum requested delay. Frontier may impose a longer
        # configured backoff while preserving the four-attempt failure budget.
        # DNS, robots and rate-limit failures affect the host. An ordinary
        # page timeout/500 should leave healthy pages on that host eligible.
        self.host_wide = host_wide


class CacheError(RuntimeError):
    pass
