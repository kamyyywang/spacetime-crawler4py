# Assignment requirement review

| Requirement | Implementation/evidence |
| --- | --- |
| All four allowed domain families only | Seed/domain-boundary validation, ASCII request guard, tests for misleading hostnames and credentials |
| Relative links and fragments | Final response/base URL resolution; fragment removal; URL identity tests |
| Unique pages by URL, not text similarity | `pages.url` primary key; count before similarity; separate HTTP/HTTPS/trailing-slash/redirect aliases |
| Longest page excludes HTML markup | Validated HTML, parser recovery, script/style/template removal, whole-word tokens |
| Top 50 excludes English stop words | Supplied stop-word list; frequency ordering with alphabetical ties |
| Alphabetical ICS subdomains and unique counts | `generate_report.py`; hostname totals preserve distinct URL rows |
| Politeness at least 500ms | Shared actual-request family gate, including retries and metadata; timing tests |
| Traps and repetitive low-value families | Both calendar forms, wiki UI/history, sort-only and structural checks; exact-index pagination suppression |
| Redirected content | Parse final URL/body; keep requested aliases distinct in analytics |
| Dead/soft-200 and large low-value responses | Empty, binary, known soft errors/login/geoblock, size/text checks; rejection evidence retained |
| No cache bypass | Only `utils.download` downloads, through assigned cache; no extra HEAD/live-test requests |
| Correct identification | Config validates quarter plus numeric group IDs; installer preserves actual group string |
| Restart without mixing data | Offline prepared run directories; old state and Git preserved; existing-run reset guard |
| Monitor the real crawl | Status tool, fetching logs, persisted reason/budgets, per-page evidence and run metadata |
| Complete code, report, Git | Submission packaging tool; actual report generated only from selected run |
| Robots/sitemaps extra credit | Robots retained; sitemap implementation retained but off by default |
| From-scratch exact/near detection extra credit | Existing handwritten FNV/SimHash/Hamming implementation retained; no similarity library |
| Multithreading extra credit | Thread-safe implementation retained/tested, but fresh default is one worker; do not claim a multithreaded run unless used |

This is an implementation review, not certification of complete compliance.
The real run, its report, server logs and the interview remain decisive.
