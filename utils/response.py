# STARTER RESPONSE ADAPTER
# The course cache supplies URL/status/error plus an optional serialized HTTP
# response. raw_response is the page response, including its headers/content;
# it may be absent for cache-generated failures. Scraper must check it first.
import pickle

class Response(object):
    def __init__(self, resp_dict):
        self.url = resp_dict["url"]
        self.status = resp_dict["status"]
        self.error = resp_dict["error"] if "error" in resp_dict else None
        try:
            # The pickle payload is part of the supplied course-cache protocol,
            # not a general-purpose format for arbitrary external website data.
            self.raw_response = (
                pickle.loads(resp_dict["response"])
                if "response" in resp_dict else
                None)
        except TypeError:
            self.raw_response = None
