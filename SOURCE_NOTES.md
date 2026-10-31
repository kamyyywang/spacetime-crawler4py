# Source and assistance notes

- Starter framework: https://github.com/Mondego/spacetime-crawler4py, supplied by
  the assignment and in the user's uploaded source. Its README and packaged
  wheel are retained. No pull request was sent to that repository.
- The starting analytics/similarity/frontier/worker files were the versions
  provided in this conversation, including the two October 29 pasted files.
- This consolidated update and regression fixtures were written with OpenAI
  Codex assistance in this conversation. Preserve/disclose that assistance as
  required by the course. No other classmate's crawler code was used here.
- HTML libraries: Beautiful Soup and lxml. HTTP/CBOR dependencies remain the
  starter's Requests/CBOR interfaces. These libraries are not used to implement
  page-content hashing, SimHash or Hamming distance.
- Exact/near similarity uses the existing handwritten FNV-1a hash, weighted
  64-bit SimHash and bit-count loop. URL hashing in the framework is separate
  from page-content similarity detection.
- `stopwords.txt` retains the provided list, adding `will`, `us` and `may`.
  Stop-word lists vary. The assignment's suggested reference is
  https://www.ranks.nl/stopwords; it could not be retrieved during this review,
  so this update does not claim that the list was newly downloaded from it.
- Documentation consulted for behavior, not copied crawler implementations:
  https://www.crummy.com/software/BeautifulSoup/bs4/doc/
  https://docs.python.org/3/library/urllib.robotparser.html
  https://requests.readthedocs.io/en/latest/user/quickstart/#timeouts
- The synthetic regression HTML, URLs and responses are offline fixtures.
  No actual course cache or destination website was crawled by these tests.

The fresh run's own logs, statistics, rejection evidence and report must be
reviewed and described by the students. Test results are not crawl results.
