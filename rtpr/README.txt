RTPR: one consolidated news dataset

This folder contains RTPR news only. Historical article downloads, recorded
WebSocket news and REST updates use the same articles/ and events/ partitions.
There are no separate history/download/observations datasets to combine.
The delta updater writes new data into these same files.

Market prices/order books, Jev requests/results and the original mixed collector
archive live separately in ~/data/news_market_capture/. They are not required
to read or update this RTPR dataset. The server collector on trade-columbus3 was
stopped and disabled on 2026-10-03; its data was removed after a complete local
SHA-256-verified copy. No live service is installed by update.py.

Layout

articles/YYYY-MM-DD.jsonl.gz
    Full article text and metadata, partitioned by UTC publication date.
    One row per unique content/format version. article_version_id is its exact
    lookup key. article_id + article_published_at identifies a release.
    Original Markdown and plain-text captures can be separate versions of the
    same release. Text is complete; it is never truncated to a model limit.

events/YYYY-MM-DD.jsonl.gz
    News events partitioned by UTC local receipt date. Includes WebSocket
    frames/alerts, connections/heartbeats, REST reconciliation responses,
    fetch errors and article_fetched completions. Full bodies are not stored
    here: article_version_id points to articles/. Article-related records have
    titles when available; control records naturally have no article/title.
    Historical download receipts were incorporated into this same table from
    their saved fetch metadata, using their original download timestamps.

metadata/feed_pages/YYYY-MM-DD.jsonl.gz
    Complete historical/API listing responses, all ticker references, cursors,
    unknown provider fields, request metadata and receipt timestamps.

metadata/migration/ and metadata/snapshots/
    Provenance manifests and import ledgers from the earlier layout. Old paths
    in these audit documents are historical references, not dependencies or
    separate datasets you need to join. They contain no market message stream.

reports/YYYY-MM-DD/
    Coverage checks, integrity validation, import proofs and latency reports.

manifest.json
    Current file hashes/counts and article totals. "articles" counts distinct
    article ID/publication pairs; "article_versions" includes format/body
    variants. These counts deliberately need not equal the event count.

.state/
    Portable JSON/gzip checkpoints, article index, source import ledger,
    pending API responses and a single-writer lock. Copy this hidden directory.

update.py, update-config.json, updater/
    Self-contained Python 3.10+ updater and bundled standard-library helpers.
    Linux/macOS/WSL. No repository checkout, SSH connection, rsync, SQLite,
    external Python packages or running server are required.

Data and clocks

Article records share article_id, article_published_at, article_version_id,
text, body_sha256, body_characters, body_format, title and receipt fields.
Additional original metadata is preserved. Source record kinds are retained
where applicable; article storage and lookup keys are the same for all sources.
Event message/page objects retain all provider fields, including future/unknown
fields; repeated ticker listings remain in the full saved pages and alerts.

RTPR currently supplies ticker, publication timestamp and article URL in its
feed/alerts. The title comes from the fetched document header. document_metadata
keeps the parsed header and original header lines, while text retains the whole
unaltered response. If a provider reference supplies title/headline, it is kept.
Authentication material such as API keys/signatures/cookies is redacted.

article_published_at is the source publication clock. receipt_utc,
receipt_unix_ns and receipt_monotonic_ns preserve local receipt precision.
Historical REST receipt times mean when we downloaded a body/page, not when the
news first arrived in a live feed. capture_transport distinguishes websocket,
rest_reconciliation and historical_rest where that field was recorded. Legacy
WebSocket alerts are also identifiable by kind=rtpr_frame and message.type=alert.
Monotonic timestamps are comparable only within the same originating run/host.

article_fetched events derived from old article download metadata carry
record_origin=saved_article_fetch_metadata. They are real saved fetch receipts,
not newly measured HTTP calls or reconstructed historical WebSocket arrivals.
An alert's title_source=article_lookup and title_available_at describe a later
body lookup; do not treat that title/text as information available at alert
arrival. Keep source publication and receipt clocks separate in latency work.

Coverage

The original fully downloaded API history begins on 2026-05-01. A full index
rescan and body reconciliation were run again on 2026-10-03. Saved live news
receipts cover 2026-09-29 through shutdown at about 22:45 UTC on 2026-10-03.
All recorded RTPR news events in the stopped collector have been merged here.
All fetched content versions are retained. Exact counts, latest API scan status
and pending bodies are available with --status; dated validation reports record
the checked coverage at consolidation time.

Verified consolidation snapshot (2026-10-03 23:13 UTC): 184,800 releases,
186,061 content/format versions and 213,059 news events. Latest stored publication:
2026-10-03 23:08:16.141 UTC. Full validation found zero missing bodies or duplicate
article versions. reports/2026-10-03/consolidation-coverage.json and
consolidation-validation.json hold the detailed evidence; future updates will
advance the live counts in manifest.json without rewriting this snapshot note.

"Complete" means all captured news plus full text for the provider references
we downloaded and audited. REST cannot reconstruct WebSocket arrival times from
before/after the actual capture period. It also cannot guarantee that text
retrieved later equals the version available at its original publication.
This dataset makes those distinctions explicit instead of inventing timestamps.

Update from RTPR

From this folder:
    python3 update.py --api-key-file ../api_keys/rtpr.json

Or set RTPR_API_KEY in the updater process environment and run:
    python3 update.py

On this machine the existing private account profile is:
    python3 ~/data/rtpr/update.py --api-key-file ~/arbitrage/api_keys/rtpr.json

The key-file profile accepts {"api_key": "..."}. No credential is bundled in
the shared dataset. Supply a private profile/environment on the next machine.
Only the key is external; no code is imported from its directory or the repo.

Each run scans from the latest feed backwards through a 24-hour overlap before
the newest stored publication, follows pagination, fetches every unseen
article ID/publication pair and appends full bodies and article_fetched events
into the same dated files above. A long gap between runs is handled by scanning
all the intervening pages. All listing response metadata is also saved.
Known bodies are not downloaded again. Retries and interrupted runs resume from
checkpoints, without duplicating bodies or replaying committed capture events.

Default request spacing is 1.1 seconds. HTTP 429/Retry-After and learned cooldowns
persist across restarts. Unavailable article bodies remain queued; a completed
run with unresolved bodies reports status=incomplete and exits with code 2.
A paused bounded run is resumable. Always inspect its reported status and pending
count when scheduling updates.

For an explicit complete index rescan back to the archive start:
    python3 update.py --since 2026-05-01 --api-key-file ../api_keys/rtpr.json

An empty dataset requires --since YYYY-MM-DD. The normal overlap catches recent
late arrivals, not arbitrarily old inserted references or silent body edits to
an already known article ID/publication pair. --since extends the index scan;
it does not refetch every known body merely to detect silent revisions.

Useful options:
    --max-requests N       Pause after N HTTP attempts; next call resumes.
    --overlap-hours 24     Recent publication window to rescan.
    --request-interval 1.1 Seconds between requests; minimum 1.
    --min-free-gib 1       Free disk reserve; updater pauses below this.
    --status              Local counts/checkpoint status; no networking.
    --verify              Check every file/record, body hash and article link.

Relocation and a mounted network drive

Copy the whole rtpr/ folder while its writer is idle, including .state/.
Default output is the folder containing update.py, wherever it is copied.
CLI --destination, --config, --import-capture and --api-key-file paths are
relative to the current working directory. api_key_file inside a config file
is relative to that config file. ~ and paths containing spaces are supported.

From a different working directory, for example:
    python3 ../rtpr/update.py --destination ../rtpr --api-key-file ../api_keys/rtpr.json

Run one writer at a time. Atomic lock-directory creation and file replacement
are used on the mounted drive; this requires the share to support those
filesystem operations correctly. Checkpoints use files, not a shared database.
Readers can use committed data. If a writer crashed, confirm that it stopped
before using --recover-lock. Never clear a lock while another host is writing.
The updater has no scheduler dependency; your other machine can call it on its
own schedule. No recurring process is installed by this bundle.

Reading

Gzip JSONL: one JSON object per line. Files can have multiple gzip members.
Sort by the relevant timestamp for time-series analysis; append/import order
is not guaranteed chronological. Historical and live rows share the same files.

Run Python from this folder:
    from updater.read import Reader
    data = Reader('.')
    for event in data.events('2026-10-03'):
        if event.get('article_version_id'):
            article = data.article(event['article_version_id'])
            print(event['receipt_utc'], article['title'])

Optional joined rows are produced in memory:
    for row in data.events('2026-10-03', with_articles=True):
        pass  # row['event'], row['article']

There is no duplicated joined table. Instantiate a fresh Reader after an update
to see new data; a Reader uses the committed manifest from when it was created.

Offline capture import (normally already done)

    python3 update.py --import-capture /path/to/sealed/collector/archive

This adds only RTPR news to the same article/event tables. It preserves clocks,
all news fields and complete bodies; markets and model records stay outside this
dataset. Imports are checkpointed by original immutable journal/checksum and
refuse changed previously imported files. Repeating an import adds no duplicate
news events or article versions. The original mixed journals are retained as a
verified archival backup in the separate news_market_capture folder; normal
RTPR analysis and delta updates do not depend on that backup.

Validation tests:
    python3 -m unittest discover -s updater -p 'test_*.py'
