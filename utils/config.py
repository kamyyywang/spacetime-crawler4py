# PARSE AND VALIDATE THE RUN'S SETTINGS BEFORE CACHE REGISTRATION
# Student/group IDs are sent to the cache for attribution. Thread count and
# politeness are independent: enabling workers never removes the shared delay.
# Real configuration defaults optional sitemap discovery to false.
import re


class Config(object):
    def __init__(self, config):
        self.user_agent = config["IDENTIFICATION"]["USERAGENT"].strip()
        print (self.user_agent)
        assert self.user_agent != "DEFAULT AGENT", "Set useragent in config.ini"
        assert re.match(r"^[a-zA-Z0-9_ ,]+$", self.user_agent), "User agent should not have any special characters outside '_', ',' and 'space'"
        if not re.fullmatch(r"IR [A-Z]{1,3}[0-9]{2} [0-9]+(?:,[0-9]+)*", self.user_agent):
            raise ValueError("USERAGENT must contain the quarter and all numeric student IDs")
        self.threads_count = int(config["LOCAL PROPERTIES"]["THREADCOUNT"])
        self.save_file = config["LOCAL PROPERTIES"]["SAVE"]

        self.host = config["CONNECTION"]["HOST"]
        # These identify registration; get_cache_server supplies the actual
        # load-balanced download endpoint later via self.cache_server.
        self.port = int(config["CONNECTION"]["PORT"])

        self.seed_urls = config["CRAWLER"]["SEEDURL"].split(",")
        self.time_delay = float(config["CRAWLER"]["POLITENESS"])

        self.use_sitemaps = config.getboolean("CRAWLER", "SITEMAPS", fallback=False)
        self.retry_delays = tuple(float(value) for value in config.get(
            "CRAWLER", "RETRY_DELAYS", fallback="300,1800,7200"
        ).split(","))
        # Three delays cover attempts 1, 2 and 3; attempt 4 exhausts the budget.
        # Worker-requested waits can exceed these values (for example DNS uses
        # at least 900 seconds). Reject NaN/infinity, not just negative values.
        import math
        if (len(self.retry_delays) != 3
                or any(not math.isfinite(x) or x < 0 for x in self.retry_delays)):
            raise ValueError("RETRY_DELAYS needs three finite nonnegative seconds")
        if not math.isfinite(self.time_delay) or self.time_delay < 0.5:
            raise ValueError("POLITENESS must be at least 0.5 seconds")
        if not 1 <= self.threads_count <= 4:
            raise ValueError("THREADCOUNT must be between 1 and 4")
        from scraper import is_valid
        # Seed URLs need the same ASCII/scope/trap validation as discovered
        # links; a bad seed would otherwise bypass link extraction's filters.
        self.seed_urls = [value.strip() for value in self.seed_urls if value.strip()]
        if not self.seed_urls or any(not is_valid(url) for url in self.seed_urls):
            raise ValueError("Every seed must be an ASCII URL within the allowed domains")
        self.cache_server = None
