# Corrected fresh crawl

This is source code, not a completed assignment submission. No real crawl was
launched while preparing or testing it. A fresh crawl uses one of the course's
permitted restarts; it does not erase previous cache-server logs. Use the same
student identification and the assigned cache. Never bypass the cache.

## Install once, while the old crawler is stopped

Extract `crawler-ready.zip` separately, for example into
`/Users/kamish/Downloads/crawler-ready`. Keep the existing project and its Git
history. In the existing project terminal:

```bash
cd /Users/kamish/spacetime-crawler4py
source .venv/bin/activate
python -m pip install -r /Users/kamish/Downloads/crawler-ready/packages/requirements.txt
python /Users/kamish/Downloads/crawler-ready/install_update.py /Users/kamish/spacetime-crawler4py
python -m unittest discover -s tests -v
```

The installer refuses concurrent crawls, backs up replaced source under
`source-backups`, and preserves existing statistics, frontier files, reports,
logs, run directories, Git history, group IDs, and registration host/port.
The installer changes source; it does not contact the cache. Check `config.ini`
contains the actual quarter and every group member's numeric student ID.
The supplied identification is `IR UF26 11215456`; the installer preserves your
existing identification instead of silently replacing a group's IDs.

If this is a new Python environment, install the supplied starter wheel too:
`python -m pip install packages/spacetime-2.1.1-py3-none-any.whl`.

## Prepare and start exactly one fresh run

Only after the offline tests pass:

```bash
python prepare_fresh.py runs/corrected-01
python launch.py --run-dir runs/corrected-01 --restart
```

Preparation creates an empty, isolated directory containing a configuration,
stop words and run metadata. It refuses an existing directory. All frontier,
analytics, fingerprint, rejected-response and log files are then created inside
that directory. Nothing is imported from the old root crawl. New-run defaults:
four domain seeds, one worker, 500ms minimum, robots checks on, optional sitemap
discovery off. The optional implementations remain in the code and are tested.
Do not claim multithreading or sitemap extra credit for a run that did not use
those features. To use sitemaps, enable SITEMAPS before preparing the run.

You must be on the campus network or UCI VPN. Keep the laptop awake while the
program runs. Watch the actual URLs and recorded content; successful status
codes and increasing counts alone do not establish a useful or correct crawl.

## Resume the same run after interruption

```bash
python launch.py --run-dir runs/corrected-01
```

Do not add `--restart` again. The updated launcher refuses a reset when progress
or statistics already exist, preventing mixed reports or accidental data loss.
Only one crawler may run from this project, including an old root crawl.

## Monitor and report from a second terminal

Activate `.venv` and use the project directory. Always select the new run:

```bash
python crawl_status.py --run-dir runs/corrected-01
python generate_report.py --run-dir runs/corrected-01
```

The report is `runs/corrected-01/report.txt`. Running the legacy commands without
`--run-dir` reads the old root data; it does not report on this new crawl.
Status summarizes exhausted budgets; use `--all-failures` for the complete list.

Stop with Ctrl+C once and wait for the prompt. Pending work remains resumable.
A queue with no eligible URLs can be waiting for retries; it is not necessarily
finished. A completed discovered frontier is not proof of complete Web coverage.

## Filtering and counting policies to explain in the report/interview

- Scope is the four given roots and their dot-separated subdomains. HTTP/HTTPS
  only; no credentials or nonstandard ports. Every cache request is ASCII.
- Resolve relative links against the final URL and any HTML base element.
  Remove fragments. Requested HTTP/HTTPS URLs, trailing-slash variants and
  redirect aliases remain distinct; text similarity never merges page counts.
- Reject dated calendar paths and Tribe Events query views, including the
  `/page/N/?post_type=tribe_events&eventDisplay=day&eventDate=...` trap. Retain
  undated listings and ordinary individual-event links.
- Reject wiki history/diff/export/edit/login/search-highlight controls. Select
  current wiki articles for discovery and shared DokuWiki namespace indexes.
- Reject known non-HTML/source/build-file extensions, sorting-only directory
  links, WordPress administration/login/preview/share/reply views, URLs longer
  than 2048 characters, more than 12 distinct query keys, paths over 30 split
  components, or the same nonempty component repeated four times.
- Reject binary signatures/NUL/control bytes even when a server labels them
  HTML. Missing Content-Type requires positive HTML evidence. Maximum HTML body
  is 5 MiB; the cache may enforce a smaller limit. No HEAD/direct-origin bypass.
- Use lxml first, with a second parser on rejection. Reject empty text, known
  geographic blocks, short soft-error/login pages, and HTML at least 512 KiB
  with fewer than 100 words. Textless/low-text discovery hubs may still expose
  allowed article links but are not included in text statistics.
- Remove script/style/template elements before word extraction. Words are whole
  ASCII alphabetic tokens with internal apostrophes; curly apostrophes normalize
  to straight apostrophes. Mixed codes such as 262P are excluded. Single-letter
  words count. Stop words are removed only for the top-50 list, not page lengths.
- Record accepted URL statistics before detecting similarity. Exact repeated
  indexes stop expanding the same pagination family but retain article links.
  Near matches are evidence for monitoring, not proof that all words match.
- Report the longest accepted text page, top 50 non-stop words in descending
  frequency, and alphabetically sorted ICS subdomains with counts of distinct
  URL rows. Scheme counts combine only in the hostname summary. Include a
  coverage count for each of the four required domain families.

## Politeness, errors and retries

Every actual page/robots/sitemap/retry request uses the shared gate. Requests in
one assignment domain family are serialized and separated by at least 500ms
after the prior request finishes. Longer robots rules and Retry-After apply to
their origin; they no longer permanently throttle every sibling ICS host.
Long ordinary waits return to the frontier without spending failure attempts.

Ordinary page timeouts/HTTP 500/generic connection failures defer that URL.
Robots failures, DNS failures and rate limits can defer a host. Unknown robots
rules never grant permission. Known 608 denials, 607 oversized bodies and redirect
limit failures are skipped explicitly; unrecognized cache/policy errors remain
visible rather than being blindly bypassed or retried.

Four consecutive failed scheduling attempts exhaust the budget. Local delays
after attempts 1, 2 and 3 are 5 minutes, 30 minutes and 2 hours (the configured
request-specific minimum or a larger Retry-After may lengthen them). Budgets
survive a resume. Unavailable URLs are resolved frontier work, not successful
HTML pages. Failure to reach the cache, a cache HTTP error, or malformed cache
envelope retains pending work with interruptible exponential backoff, without
consuming the origin's budget.

## Finish and package

Review the new report's longest page, common words, domain coverage and failures.
Document actual traps/failures and any early stopping. Save code development
with Git; this update does not fabricate earlier commits. Do not PR the course
repository. After the crawler has stopped:

```bash
python package_submission.py --run-dir runs/corrected-01 --output assignment2-final.zip
```

This builds a complete current-code directory including your existing `.git`
history, dependencies, selected run's report/configuration and final status.
It excludes environments, old run data, source backups and huge runtime files.
It refuses to overwrite an existing ZIP. Review the contents before Canvas
submission. Generated report numbers are never supplied or invented here.

## Practical limits

The policies are heuristics, not a guarantee against every possible trap,
incorrect server header, or unusual soft-error page. A fresh run still needs
monitoring. Tests are offline; they cannot establish campus connectivity,
actual cache log presence, grading-policy interpretation or final coverage.
Discuss actual behavior honestly at the grader meeting. See SOURCE_NOTES.md.
