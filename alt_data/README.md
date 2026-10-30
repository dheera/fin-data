# Alternative data

**1,183 more series added; the library now contains 119,420 datasets.** Batch 11 adds **57,528,287 observations** covering European energy, Japanese financing and investment flows, Turkish card spending, river and power conditions, and measured soil moisture. Korean filings, outage details and current component inventory have separate event/observation tables. See [completion report](logs/expansion_batch11_complete.json) for counts and checks.

The earlier atmospheric-data acquisition has its own [batch 10 status](logs/expansion_batch10_job.json) and [Copernicus status](logs/cams10_backfill_job.json); only published histories appear in the catalog. [Access status](logs/access_setup_20261021.json) records credential paths without their values. AISStream remains research-only: [assessment](logs/aisstream_usefulness_20261021.json).

Start with **`catalog.parquet`** (or the text-searchable **`catalog.csv`**). Each row describes one populated dataset and points to one **`data/<category>/<id>.parquet`** file. Every data file has just two columns:

| Column | Type | Meaning |
|---|---|---|
| `date` | UTC timestamp | Observation/period label; see `timestamp_semantics` for forecast issue versus target time |
| `value` | float64 | Source measurement, index or forecast in the catalog's units |

The catalog records the title, provider, geography, source URL, units, native/output frequency, actual start/end, observation count, gaps, freshness, market hypothesis, limitations, and download checksums. Each Parquet footer preserves typed metadata. The flat catalog stores nested metadata as JSON text and mixed-type provider-specific fields as searchable text. A dataset can be a particular metric at a particular location; dataset counts are **not counts of independent providers**.

## Read and search

```bash
cd /data/fin/alt_data
python3 library.py list --search shipping
python3 library.py list --search temperature --new
python3 library.py list --batch 11 --search electricity
python3 library.py validate
```

```python
from pathlib import Path
import pandas as pd

root = Path('/data/fin/alt_data')
catalog = pd.read_parquet(root / 'catalog.parquet')
new = catalog.query('batch == 11')  # this expansion
current = new.query('not stale_over_45_days')
foreign = current.query("geography_scope == 'non_us'")
selected = foreign[foreign.title.str.contains('load', case=False)]
data = pd.read_parquet(root / selected.iloc[0]['path'])
# data.columns == ['date', 'value']; no filling or resampling needed to read it.
```

From this directory, the optional convenience functions are:

```python
from library import load, panel, load_collected_asof
cash = load('treasury_tga_closing_balance')
joined = panel(['nyfed_sofr_volume', 'nyfed_effr_volume'])
# Outer join on observation dates; gaps remain NaN.
```

Use `batch == 11` for this expansion; batch 10 covers the separately tracked atmospheric acquisition. Use `batch == 9` for the 638 intraday additions, `batch == 8` for the previous 4,622, `batch == 7` for the preceding 50,000, `batch == 6` for the preceding 50,000, `batch == 5` for the earlier 10,000, and `batch == 4` for the earlier 2,000. Batches 1, 2 and 3 contain 100, 200 and 400. Batch 0 contains the original 132 datasets. Batches 6–11 also admit weekly histories, as requested for weekly trading research; monthly legacy feeds remain separate.

`geography_scope` is `non_us`, `us`, `mixed` or `global`; it describes the activity measured. Worldwide crypto users and foreign-language Wikipedia readers are classified as global. They are not assumed to live in the provider's country or the language's home country. `country_codes` contains a JSON list of ISO alpha-2 country/territory codes, with the documented XK extension. Country arrays can be empty for worldwide or regional aggregates. Some country tags describe connected gas networks or economic exposures rather than physical station coordinates; source-specific geography fields explain their basis. `expected_publication_lag` distinguishes observation frequency from release timing. These standardized geography and lag fields are complete for batches 6–11; earlier batches may have missing fields.

## Directory layout

```text
alt_data/
  README.md                   this guide
  catalog.parquet             one searchable row per available dataset
  catalog.csv                 the same catalog in readable text
  data/<category>/<id>.parquet the only directory needed to consume values
  supplementary/              event tables, current observations, quality and release details
  archive/                    raw downloads, old versions, acquisition work files
  logs/                       acquisition results, batch receipts, validation
  library.py                  search, load, publish and validate
  refresh.py                  refresh all selected datasets
  sources_*.py                new source collectors, grouped by subject
  collect.py                  original FRED/memory/shipping collectors
  run_daily.sh                convenience refresh command
  tests/                      parser and storage checks
```

The old nested `series/`, `snapshots/`, and `vintages/` directories have been removed from the public data layout. A *series* simply means one indicator over time. A *snapshot* meant a feed with little accumulated history. A *vintage* meant an earlier version of a value before a source revised it. A *run* meant one execution of a collector. Consumers no longer need those storage distinctions.

Original records are preserved byte-for-byte in `archive/legacy_observations/`, `archive/legacy_short_history/`, and `archive/revisions/`. Original source responses are in `archive/raw/`. Large multi-file feeds use explicit `raw_manifest_sha256` manifests that list the original response checksums; validation expands and verifies every member. Those manifest JSON objects are local provenance indexes, not HTTP payloads. Their old `.json` sidecars and the new immutable receipts retain URLs and retrieval times. `logs/layout_migration.json` records original file checksums and moves. Original methodology notes are retained in `archive/original_readme.md`. Subsequent changes to a public Parquet preserve the previous file under `archive/revisions/<id>/`.

## Refresh

```bash
python3 -m pip install -r requirements.txt
./run_daily.sh                         # refresh all groups, rebuild catalog, validate
./run_daily.sh --group environment     # refresh one subject group
./run_daily.sh --group macro
./run_daily.sh --group activity2       # new power, transport and pipeline sources
./run_daily.sh --group digital2        # new software, internet and safety sources
./run_daily.sh --group physical2       # observed weather, water and air quality
./run_daily.sh --group macro2          # financing, money markets and SEC filings
./run_daily.sh --group physical3      # more water gauges and pollutant monitors
./run_daily.sh --group activity3      # ports, aviation, taxi trips and regional power
./run_daily.sh --group digital3       # software, internet, attention and safety reports
./run_daily.sh --group research3      # SEC, fiscal, currencies, GitHub and ocean conditions
./run_daily.sh --group energy4        # foreign grids, gas systems and reservoirs
./run_daily.sh --group economy4       # foreign auctions, trade, prices, payments and security
./run_daily.sh --group digital4       # international internet use, crypto activity and attention
./run_daily.sh --group research4      # global news, weekly positioning and NZ traffic
./run_daily.sh --group knowledge5    # software use, virtual goods and cybersecurity
./run_daily.sh --group commerce5     # fuel, business notices, property and procurement
./run_daily.sh --group realworld5    # fires, European air and aquaculture conditions
./run_daily.sh --group health5       # disease surveillance and research registrations
./run_daily.sh --group asia6         # China inventories, Korean financing/power
./run_daily.sh --group emerging6     # Indonesia prices, India flows, Mexico wholesale
./run_daily.sh --group supply6       # coal, petroleum, gas/LNG stocks and flows
./run_daily.sh --group intent6       # job-posting indices and search-attention archive
./run_daily.sh --group expectations6 # original GFS forecasts for five priority countries
./run_daily.sh --group foreign7      # Korea 5min, India 15min, Mexico hourly power
./run_daily.sh --group grid7         # UK 5min/30min operations and outage publications
./run_daily.sh --group attention7    # 15min world news and hourly Wikimedia traffic
./run_daily.sh --group forecast7     # four GFS cycles and same-target forecast changes
./run_daily.sh --group energy11      # European power, gas storage and LNG
./run_daily.sh --group corporate11   # Japan flows/financing and Korea filing content
./run_daily.sh --group commerce11    # Turkey weekly card spending and transactions
./run_daily.sh --group physical11    # Rhine gauges and South African power
./run_daily.sh --group smap11        # measured soil moisture; Earthdata token required
python3 sources_mouser11.py          # one current component inventory/pricing collection
python3 sources_outages11.py         # UK outage revision/capacity companion tables
./run_daily.sh --refresh               # recheck source caches (immutable archives may be retained)
python3 refresh.py --publish-only      # recover/rebuild from downloaded candidates
python3 collect.py --source memradar   # original collector still works
python3 collect.py --include-short-history
python3 -m unittest discover -s tests -v
```

No recurring schedule has been installed. The script can be called by an existing scheduler. Provider requests are cached and paced; a full refresh can take hours at this scale. All collector groups are included by default. A failed provider preserves previously collected values, successes are kept, and the command returns a nonzero status. Inspect `logs/latest_refresh.json` and provider logs. An old cached candidate is not evidence of a successful new upstream refresh. Catalog `retrieved_at` describes local dataset processing; Immutable receipts in `archive/receipts/` record actual HTTP retrieval times; `archive/requests/` is the current URL cache.

Downloads stage under `archive/staging/`; only the selected datasets are promoted into `data/`. Refreshing cannot silently add hundreds of extra candidates to the public catalog. Older dates remain when a provider returns a rolling window. Newly observed revisions replace the matching dates, with prior public files preserved. An ordinary missing observation is not automatically treated as a retraction. Explicit source withdrawals (`retracted_dates`) remove earlier values; for example, an OFR final disclosure edit supersedes an earlier preliminary value. Collectors preserve the published daily/weekly cadence. A change in frequency, units, category or underlying identity fails preflight before any refresh publication. Writes are atomic per file; a whole refresh is not a transaction across every dataset.

## Quality and interpretation

Every daily-or-finer addition has at least 90 actual observation days spanning at least 90 days, at least 50% calendar-day coverage, and a median interval no longer than one day. Business-day sources qualify without invented weekends. Weekly additions require at least 26 observed weeks spanning at least 180 days, median gaps of 5–8 days, and at least 50% expected-week coverage. Weekly data are never stretched into daily rows. Native weekly reports and weekly aggregations of finer records are distinguished by `native_frequency` and `aggregation`. Aggregations include event counts or sums, transaction/quote medians, and last observed quotes. Colombian security histories sum reported daily quantities; weekly Mexican wholesale prices use medians of the original observed quotes, without filling missing days or weeks. `finer_frequency_available` also marks those daily inputs. `daily_eligible` and `research_eligible` expose the admission distinctions. Admission rejects duplicate dates, nonfinite values, identical numeric histories and constant histories. Missing measurements stay missing; event-count zeros are admitted only where a complete source reporting interval supports zero. Measurements, source estimates, sampled intraday readings, text indices and event aggregates are explicitly distinguished.

Intraday samples are retained where collected, including electricity, buoy, space-weather and mobility observations. `frequency` describes the output; `native_frequency`, `finer_frequency_available`, and methodology fields record finer source availability. Some city feeds are aggregated to daily source-calendar counts because their floating local timestamps lack an unambiguous UTC offset. No fabricated intraday timestamps are added. `median_interval_seconds`, `max_gap_days`, `observed_days`, and `day_coverage` describe actual coverage. `age_days` and `stale_over_45_days` make lagged or discontinued histories visible.

The market hypotheses are research leads, not demonstrated predictive returns. Some indicators are correlated, nested, or derived from the same upstream dataset. The catalog identifies these relationships and the specific metric/location. Dataset counts include related metrics and overlapping geographic aggregates; they are not counts of independent trading signals.

These are **latest-history research data**, not a historically point-in-time feed. `date` is not the date a trader could first have known a value. Daily calendar labels can refer to different source timezones, and modeled observations or revised statistics may appear well after their observation date. The original downloaded bytes and retrieval receipts support reproducibility, not reconstruction of unavailable historical release timestamps. Use source release lags and an appropriate as-of model before backtesting.

Batches 8 and later record `locally_available_at` for each newly published numeric version. `load_collected_asof(series_id, "2026-10-21T00:00:00Z")` returns the numeric version present in this library by that timezone-aware cutoff, or raises if no tracked version existed. It never assigns a newly downloaded backfill to its old observation date. Future revisions remain in `archive/revisions/`; unchanged refreshes keep the original local availability clock. This is prospective local version history, not historical source-release reconstruction. Earlier batches remain untracked by this helper. Using the latest-batch `selected` rows from the example above:

```python
from library import load_collected_asof
known = load_collected_asof(
    selected.iloc[0]["series_id"], pd.Timestamp.now(tz="UTC"), root=root
)
# Returns a tracked numeric version; it does not filter by observation date.
```

GFS canonical dates are **model initialization times**, preceding computation and publication. `supplementary/gfs_forecast_issue_target.parquet` retains each initialization, valid time, lead/window, value and source object Last-Modified clock. Its daily cycles permit comparisons only when target times coincide. The source object clock is not asserted to be first public release. Native forecasts are available every six hours; batch 8 samples the 00 UTC cycle daily at 24-, 72- and 168-hour leads. The 1-degree cells are unweighted regional proxies. Forecasts and search-topic counts are related features, not independent signals.

`logs/batch_<number>.json` records public membership for each published batch; pending batches have no publication receipt. For the preceding expansion, batch 7 was selected from 110,780 populated, audited candidates in four new collector groups. Earlier unselected candidates did not count toward batch 7. That historical selection balanced providers, categories and geography, targeting 75% non-US activity while retaining global and US research. Its policy, IDs and numeric checksums remain in `logs/expansion_batch7_selection.json`; batch 8 follows its separately documented gap-focused selection policy. Unselected candidates remain in `archive/staging/` and do not count as public additions. `logs/expansion_batch7_baseline.json` records the preserved 62,832 IDs and checksums. Earlier completion reports, including `logs/expansion_50000_complete.json` for batch 6, remain intact. `logs/validation.json` records full-library schema, coverage, numeric checksum, duplicate-history and catalog checks. The batch 11 selection and source audits separately verify its original downloaded bytes; this final library validation did not rehash earlier raw archives. `logs/expansion_batch7_source_checks.json` links independent source reconstruction checks.

## Batch 11: energy, spending, financing and physical constraints

| Provider | Histories | Observations | Stored cadence |
|---|---:|---:|---|
| Central Bank of the Republic of Türkiye — EVDS | 58 | 37,956 | weekly |
| ENTSO-E Transparency Platform | 595 | 37,660,380 | 15min, 30min, hourly |
| Eskom Data Portal | 8 | 1,416 | weekly |
| GIE AGSI | 384 | 387,042 | daily |
| GIE ALSI | 65 | 66,495 | daily |
| German Waterways and Shipping Administration (WSV) PEGELONLINE | 21 | 19,313,722 | 15min |
| Japan Exchange Group | 29 | 35,574 | weekly |
| Japan Ministry of Finance | 22 | 24,992 | weekly |
| NASA NSIDC SMAP | 1 | 710 | daily |

The [selection audit](logs/expansion_batch11_selection.json) pins each source-checked history and records rejected duplicate histories. The [refresh check](logs/expansion_batch11_refresh_smoke.json) exercises each new provider/cadence combination in an isolated library. Prior public series retain their paths, observation counts and numeric checksums.

**European energy.** ENTSO-E covers actual load, fuel-level generation, directional cross-border flows, load/renewable forecasts and energy-price products from 2024. Original hourly, half-hour and quarter-hour periods remain at their source intervals; a series may change resolution over time. Auction type, contract type and classification sequence identify separate price products. Forecast dates are delivery targets in the latest retrieved history: original forecast issues and lead times are unavailable, so these are unsuitable for an as-issued revision backtest. Document clocks and source resolutions remain in `supplementary/energy11/`.

GIE AGSI provides daily country and selected facility/group storage, injection, withdrawal and capacity data; ALSI provides daily country LNG inventories, send-out and related metrics. Units and source status/estimate flags are preserved. Country totals and selected facilities overlap and must not be added together. The selected facilities are not a complete facility census; LNG facility discovery was unavailable to this key. AGSI unavailability events are in `supplementary/energy11/gie_agsi_unavailability_events.parquet`. Typical gas-day reporting lag is recorded as approximately 1–2 days, not verified historical first publication.

**Financing and spending.** Japan MOF weekly securities flows start in 2005; the current file uses its documented net-acquisition-positive convention, including backcast older values. JPX weekly financing histories start in 2002 and stop at the September 2026 schema change; the new-format observations remain separate. Share counts, market-valued balances and customer-contract-valued balances are distinct. Turkey CBRT histories preserve 29 weekly spending amounts and 29 transaction-count categories, mostly from 2014. Amounts are nominal thousand TRY; online/mail-phone and other informational breakdowns overlap headline categories. Weekly labels are period dates: approximate release lags are five days for MOF and six days for CBRT, with holiday exceptions.

**Physical conditions.** Rhine gauge heights and discharge retain 15-minute observations, mostly from 2000. Heights are relative to each gauge datum, not navigable depth; measurements can be unvalidated and datum changes matter. Eskom's eligible weekly capacity ratios, peak demand and energy demand are canonical; these weekly histories also retain archive gaps. The 33 sparse hourly histories remain in `supplementary/physical11/` with actual gaps. Ambiguous malformed source files were excluded and listed in the inventory. A more complete historical hourly Eskom panel requires its [free manual data-request form](https://www.eskom.co.za/dataportal/data-request-form/).

**Measured soil moisture.** NASA SMAP L3 Version 9 covers 2024–October 2026. One history, Madhya Pradesh PM, meets the main catalog's daily coverage rule. Thirteen other observed AM/PM histories remain as date/value Parquets in `supplementary/smap11/`, with their real irregular revisits and missing days retained. These supply useful measurements for seven regional boxes; they are not 14 continuously observed daily feeds. Neither Sumatra pass met the minimum regional quality/coverage rule, so no usable Sumatra or palm-oil-region history is claimed. Rectangular boxes can cross national borders; country intersections and fixed-grid cell coverage are documented in metadata and the linked quality/geography companions. `regional_observation_quality.parquet` records valid-pixel fractions, excluded malformed timestamps and actual acquisition-time ranges. The provider's acquisition-to-generation latency is separate from the unknown historical publication delay measured from a product-date label. See the [source inventory](logs/smap11_inventory.json) for exact coverage and exclusions. Regional satellite estimates are not crop yields, irrigation measurements or crop-weighted averages.

**Event and current-observation tables.** These retain richer schemas and are not counted as date/value histories.

| Table | Rows | Location |
|---|---:|---|
| Korea disclosure index | 68,179 | [dart_disclosures.parquet](supplementary/corporate11/dart_disclosures.parquet) |
| Korea structured major events | 837 | [dart_structured_events.parquet](supplementary/corporate11/dart_structured_events.parquet) |
| Korea event-content fields | 23,942 | [dart_event_fields.parquet](supplementary/corporate11/dart_event_fields.parquet) |
| Korea ownership filings | 7,875 | [dart_ownership_events.parquet](supplementary/corporate11/dart_ownership_events.parquet) |
| Japan current premium charges | 953 | [jpx_current_premiums.parquet](supplementary/corporate11/jpx_current_premiums.parquet) |
| Japan current designated/restricted issues | 347 | [jpx_current_daily_designated.parquet](supplementary/corporate11/jpx_current_daily_designated.parquet) |
| UK outage revisions and capacity fields | 57,761 | [capacity_revisions.parquet](supplementary/outages11/capacity_revisions.parquet) |
| UK outage profile segments | 261,932 | [outage_profile_segments.parquet](supplementary/outages11/outage_profile_segments.parquet) |
| Mouser current listings | 24 | [parts.parquet](supplementary/mouser11/parts.parquet) |
| Mouser eligible numeric observations | 173 | [measurements.parquet](supplementary/mouser11/measurements.parquet) |

The OpenDART panel contains 48 currently selected companies across 14 sectors, with disclosures from 2015 and ownership records from late 2024. It is subject to selection/survivorship bias. Filing dates are Korean calendar dates, not fabricated precise release timestamps. Amendment and withdrawal flags, original fields, source dictionaries and 378 original documents are retained; six requested documents were unavailable from the provider. Ownership filings are not transaction records.

UK outage tables separate publication times, effective intervals, revision identifiers and local retrieval times. Today’s exact-ID responses may contain later changes. They do not reconstruct historically known unavailable capacity; profile capacities retain neutral source semantics and invalid intervals are flagged.

Mouser is a first-day pilot across fixed component identifiers. It records stock, explicit lead times and quantity-tier prices with currencies and packaging retained. Restricted listings remain flagged in the raw part table and contribute no numeric observations. Unmatched parts are not assumed to have zero stock. `collected_at` is actual retrieval time; no past inventory history is invented. The command caches each UTC day's requests and can accumulate observations on future manually requested runs.

All supplied credentials are stored in `/data/fin/api_keys/` as owner-readable files excluded from Git; no values are copied into data or reports. The Earthdata bearer token authenticated successfully. Google Trends alpha approval remains pending. No paid feed, recurring schedule or AIS collector was added.

## Batch 9: public intraday histories

| Provider / feed family | Added histories |
|---|---:|
| Elexon Insights operational electricity | 47 |
| Elexon REMIT publication archive | 15 |
| GDELT 2.0 worldwide news events | 163 |
| Indian Energy Exchange | 10 |
| Korea Power Exchange operational telemetry | 13 |
| Mexico CENACE initial settlement demand balance | 32 |
| NOAA GFS archived operational forecasts | 354 |
| Wikimedia hourly project pageviews | 4 |

Korean power data retain native five-minute readings; estimated total demand and modeled behind-the-meter solar are labeled separately from market measurements. Indian IEX histories preserve 15-minute market bids, prices and scheduled volumes from 2022; these are not national electricity consumption. Mexican CENACE histories preserve hourly settlement quantities and original-liquidation publication labels, not live demand. Canonical Mexico timestamps are normalized operating-hour labels, not verified UTC instants; see their metadata before intraday alignment.

UK operational values preserve five-minute/half-hour measurements. Forecast series use source release timestamps and retain targets. REMIT files count announcements by closed publication hour; exact message/revision identifiers and clocks are in `supplementary/grid7/`. They do not reconstruct historically known unavailable capacity from today's outage profiles.

GDELT counts newly discovered coded events by ActionGeo country in complete 15-minute export batches. Protest, material-conflict and business/labor-actor counts are related subsets; tone is a mean over event-associated texts, not unique articles or measured investor sentiment. Wikimedia hourly page views describe global readers of each language edition. The API rate-limited expansion after English channels and Spanish desktop 2024; missing hours stay missing, and the stale partial Spanish archive is labeled.

The new `supplementary/gfs_intraday_issue_target.parquet` keeps all four daily GFS cycles, target times, lead windows and archive Last-Modified clocks. Targets are 00 UTC one, three or seven calendar days after the initialization day, so lead shortens through the day. Revision series subtract adjacent six-hour cycles only for identical targets/windows; they omit the midnight target rollover and missing predecessor cycles. They are derived features of the forecast histories. Model initialization and archive Last-Modified do not prove first public availability.

`logs/expansion_batch9_selection.json` pins every published addition and `logs/expansion_batch9_baseline.json` pins the preceding 117,454 histories. Independent source reconstruction is recorded in `logs/expansion_batch9_source_checks.json`. Free credential options and remaining access limits are in `logs/expansion_batch9_access_options.json`; no paid data was acquired. Direct spending, bookings/cancellations, securities-lending rates and full historical consensus revisions remain substantial gaps.

ENTSO-E access is configured and batch 11 includes acquired power histories. Alpha Vantage free access supplies the initial estimates observations described below; no paid subscription was purchased.

### Credentialed estimates collection

The supplied Alpha Vantage key is configured in `/data/fin/api_keys/alphavantage.json`, excluded from Git and readable only by its owner. `ALPHAVANTAGE_API_KEY` can override that file. Authentication values are omitted from source receipts and logs. ENTSO-E, GIE, OpenDART, Mouser and NASA Earthdata access are configured; batch 11 records the acquisitions. Google Trends alpha access has also been requested. Current access status and other useful registrations are recorded in `logs/access_setup_20261021.json`.

```bash
cd /data/fin/alt_data
python3 sources_alphavantage.py
```

This standalone command collects earnings estimates for 20 companies, primarily international companies' US-listed shares/ADRs. It uses at most one request per symbol per UTC day, reuses that day's archived responses, spaces requests, and keeps a persistent 25-call daily budget. Calls made by other programs are outside that local budget; provider quota errors stop collection. It is not part of the default all-source refresh and no recurring schedule is installed.

Read `supplementary/alphavantage/earnings_estimates.parquet` for the accumulating observations and `logs/alphavantage_latest.json` for outcomes. Columns are `collected_at`, `symbol`, `fiscal_period_end`, `horizon`, `metric`, `value`, and `raw_sha256`. `collected_at` is the actual local retrieval clock; `fiscal_period_end` is the source target/period label, even for past fiscal periods. Sparse 7/30/60/90-day EPS lookbacks remain named source fields. They do not create observations on earlier dates. Repeating the same cached collection does not duplicate rows, and future collection days append observations.

These first-day snapshots are **not counted as new historical series in the main catalog**. Missing fields stay missing. The provider does not identify currency or ordinary-share/ADR basis; these can differ even between fiscal periods for one company, so values are not normalized or assumed comparable. Original response bytes and retrieval receipts are retained for checking those semantics before constructing forecast-revision signals.

Copernicus ADS and OpenAQ access are configured, with batch 10 progress tracked separately. Google Trends alpha approval remains pending. The AISStream key is stored and its usefulness assessed, with no collector started.

## Batch 8: domestic activity and the identified gaps

| Priority country | New histories | Main additions |
|---|---:|---|
| China | 798 | Commodity warehouse warrants/stocks; weather expectations |
| India | 698 | Foreign portfolio flows, derivatives activity, plant coal operations, search attention and weather expectations |
| South Korea | 226 | Financing/fund balances, power operations, search attention and weather expectations |
| Indonesia | 669 | Provincial food prices, search attention and weather expectations |
| Mexico | 702 | Wholesale food prices, gas-network flows, search attention and weather expectations |

| New provider / feed family | Populated histories |
|---|---:|
| Bank Indonesia PIHPS retail food survey | 592 |
| Central Depository Services India FPI activity | 43 |
| GoogleTrendArchive (Urman et al.) | 352 |
| Indeed Hiring Lab | 748 |
| Korea Financial Investment Association FreeSIS | 164 |
| Korea Power Exchange EPSIS | 4 |
| Mexico Economy SNIIM wholesale market quotes | 447 |
| NOAA GFS archived operational forecasts | 177 |
| Shanghai Futures Exchange warehouse reports | 762 |
| enagas_gts | 32 |
| india_central_electricity_authority | 562 |
| mexico_cenagas | 182 |
| us_energy_information_administration | 557 |

Indeed indices are daily seven-day trailing averages released weekly; their historical values can be revised. They are not vacancy counts or a provider revenue measure. Search counts refer to displayed Google Trending Now clusters in author-collected exports, use explicit multilingual topic rules, and are not total search volumes, sentiment or purchases. Missing captures remain gaps; overlapping regions are not summed. The raw search archive is finite and stale.

`logs/expansion_batch8_selection.json` pins this expansion, `logs/expansion_batch8_baseline.json` pins the preceding 112,832 histories, and `logs/expansion_batch8_source_checks.json` records complete sampled-history reconstruction from original downloads. [The semantic review](logs/expansion_batch8_semantic_review.json) excludes 11 alternate gas-flow reports already covered by ENTSOG. Provider-specific definitions, units and release limits remain in each footer. Direct spending, bookings/cancellations, securities borrowing costs and full analyst-consensus revision histories remain substantial gaps.

## Source families through batch 7

Batch 7 covered desktop-app downloads by IP country; virtual-goods executed trading; internet scanning and vulnerability exploitation probabilities; retail fuel quotes and temporary stockout starts; commercial-court notices, insolvencies and new businesses; property transactions; government contract signatures and face values; satellite fire detections and measured radiative power; European air pollutants; Scottish fish-farm sea lice and harmful algal organisms; international respiratory-virus surveillance; Brazilian arbovirus outbreaks; UK primary-care activity; and first-published clinical-study registrations.

Software downloads and updates are not unique users; IP location is not nationality. Country-level app observations are retained only when their complete country breakdown reconciles exactly to the app's global download total; incomplete breakdowns remain gaps. Game-market users and security telemetry are global. EPSS values are source model probabilities, not observed attacks. The batch-7 EPSS histories cover only the verified 2024 archive window, ending no later than 2024-11-30; later archive access was denied by the provider. They are historical research inputs, and the collector records the blocked extension. Fire detections from different satellites overlap and are not unique fires; radiative power is measured in MW, not energy. Clinical-study country tags describe currently listed study locations, and first publication is not trial commencement. Registry classifications may change retrospectively. Property and procurement dates can precede public reporting by weeks or months. Every footer describes its own units, aggregation, coverage and limitations.

Counts below refer to populated series, not independent organizations or independent trading signals.

| Provider / feed family | Earlier 700 | Earlier 2,000 | Earlier 10,000 | Batch 6: 50,000 | Batch 7: 50,000 |
|---|---:|---:|---:|---:|---:|
| AMS-IX international Internet traffic | 0 | 0 | 0 | 9 | 0 |
| Alternative.me | 0 | 0 | 0 | 1 | 0 |
| BIS effective exchange rates | 0 | 0 | 64 | 0 | 0 |
| Banco Central do Brasil foreign-exchange interventions | 0 | 0 | 0 | 1 | 0 |
| Banco Central do Brasil payments and currency | 0 | 0 | 0 | 44 | 0 |
| Bank of Canada | 5 | 0 | 0 | 0 | 0 |
| Binance global spot trading | 0 | 0 | 0 | 3081 | 0 |
| Blockchain.com network telemetry | 0 | 0 | 0 | 4 | 0 |
| Brazil ONS | 0 | 0 | 0 | 5571 | 0 |
| CFTC Commitments of Traders | 0 | 0 | 0 | 3081 | 0 |
| CRAN R-hub | 0 | 111 | 550 | 0 | 0 |
| Cefas and Food Standards Scotland shellfish monitoring | 0 | 0 | 0 | 0 | 433 |
| Chicago 311 | 20 | 0 | 0 | 0 | 0 |
| Colombia Compra Eficiente SECOP II contracts | 0 | 0 | 0 | 0 | 4209 |
| Colombia Ministry of Defence security observatory | 0 | 0 | 0 | 1144 | 0 |
| DeFiLlama protocol activity | 0 | 0 | 0 | 3080 | 0 |
| ECB sovereign yield curves | 0 | 0 | 198 | 0 | 0 |
| ENTSOG Transparency | 0 | 0 | 0 | 3435 | 0 |
| ERA5 Ocean via Open-Meteo | 0 | 0 | 609 | 0 | 0 |
| Economic Policy Uncertainty researchers | 2 | 0 | 0 | 0 | 0 |
| Elexon Insights | 0 | 0 | 0 | 1000 | 0 |
| European Central Bank ESTR | 0 | 7 | 0 | 0 | 0 |
| European Commission TAXUD customs surveillance | 0 | 0 | 0 | 6827 | 0 |
| European Commission agricultural market prices | 0 | 0 | 0 | 2111 | 0 |
| European Environment Agency observed air chemistry | 0 | 0 | 0 | 0 | 2333 |
| FIRST EPSS vulnerability risk forecasts | 0 | 0 | 0 | 0 | 3197 |
| Federal Reserve Bank of New York | 10 | 0 | 0 | 0 | 0 |
| Federal Reserve commercial paper | 0 | 76 | 0 | 0 | 0 |
| Flathub desktop application telemetry | 0 | 0 | 0 | 0 | 9276 |
| France DILA BODACC commercial notices | 0 | 0 | 0 | 0 | 709 |
| France Ministry of Economy fuel-price reporting | 0 | 0 | 0 | 0 | 5836 |
| GDELT 1.0 worldwide news events | 0 | 0 | 0 | 7043 | 0 |
| GH Archive via ClickHouse | 0 | 0 | 575 | 0 | 0 |
| Google Transparency Report via IODA | 0 | 0 | 0 | 1004 | 0 |
| HM Land Registry property transactions | 0 | 0 | 0 | 0 | 5836 |
| Hokkaido Electric Power Network | 0 | 0 | 0 | 17 | 0 |
| IMF PortWatch | 56 | 42 | 1184 | 0 | 0 |
| INPE South America satellite fire observations | 0 | 0 | 0 | 0 | 5835 |
| IODA global Internet connectivity | 0 | 0 | 0 | 710 | 0 |
| NYC 311 | 30 | 0 | 0 | 0 | 0 |
| New Zealand Electricity Authority | 0 | 0 | 0 | 534 | 0 |
| OFR repo markets | 0 | 66 | 0 | 0 | 0 |
| OFR sponsored repo | 0 | 2 | 0 | 0 | 0 |
| OSRS Wiki RuneLite virtual-goods market | 0 | 0 | 0 | 0 | 3439 |
| Office of Financial Research | 9 | 0 | 0 | 0 | 0 |
| Python Software Foundation PyPI Stats | 0 | 42 | 0 | 0 | 0 |
| RIPE NCC RIS | 0 | 150 | 521 | 0 | 0 |
| SANS Internet Storm Center DShield | 0 | 0 | 0 | 0 | 366 |
| SEC EDGAR filing indexes | 0 | 43 | 1124 | 0 | 0 |
| Scottish Government fish-farm health reporting | 0 | 0 | 0 | 0 | 165 |
| TEPCO Power Grid | 0 | 0 | 0 | 16 | 0 |
| Taiwan Fisheries Agency wholesale markets | 0 | 0 | 0 | 1400 | 0 |
| Taiwan Ministry of Agriculture wholesale markets | 0 | 0 | 0 | 5089 | 0 |
| Tor Project | 27 | 50 | 390 | 0 | 0 |
| US FDA openFDA | 0 | 80 | 166 | 0 | 0 |
| US Treasury Fiscal Data | 31 | 0 | 155 | 0 | 0 |
| Waka Kotahi NZ Transport Agency traffic monitoring | 0 | 0 | 0 | 3288 | 0 |
| Wikimedia | 66 | 120 | 100 | 0 | 0 |
| Wikimedia multilingual attention | 0 | 0 | 0 | 491 | 0 |
| australian_energy_market_operator | 0 | 10 | 0 | 0 | 0 |
| chicago_department_of_buildings | 5 | 0 | 0 | 0 | 0 |
| chicago_transit_authority | 2 | 33 | 178 | 0 | 0 |
| ClinicalTrials.gov study registrations | 0 | 0 | 0 | 0 | 5273 |
| coin_metrics | 34 | 0 | 177 | 0 | 0 |
| defillama | 25 | 0 | 0 | 0 | 0 |
| dwd_climate_observations | 0 | 162 | 0 | 0 | 0 |
| energinet | 0 | 99 | 0 | 1019 | 0 |
| environment_canada_hydat | 0 | 66 | 354 | 0 | 0 |
| eurocontrol | 0 | 120 | 1001 | 0 | 0 |
| fraunhofer_energy_charts | 66 | 0 | 0 | 0 | 0 |
| gfz_space_weather | 2 | 0 | 0 | 0 | 0 |
| InfoDengue Brazilian arbovirus surveillance | 0 | 0 | 0 | 0 | 158 |
| mta | 9 | 0 | 0 | 0 | 0 |
| nasa_power | 98 | 0 | 0 | 0 | 0 |
| new_york_city_taxi_and_limousine_commission | 0 | 0 | 354 | 0 | 0 |
| new_york_state_department_of_health | 10 | 0 | 0 | 0 | 0 |
| noaa_coops | 0 | 41 | 0 | 0 | 0 |
| noaa_ghcn_daily | 0 | 239 | 0 | 0 | 0 |
| noaa_gml | 1 | 0 | 0 | 0 | 0 |
| noaa_ndbc | 65 | 0 | 0 | 0 | 0 |
| npm | 50 | 118 | 501 | 0 | 0 |
| nsidc_sea_ice | 2 | 0 | 0 | 0 | 0 |
| nyc_department_of_buildings | 20 | 0 | 0 | 0 | 0 |
| queensland_long_paddock | 3 | 0 | 0 | 0 | 0 |
| rte_france_odre | 0 | 0 | 102 | 0 | 0 |
| seattle_department_of_transportation | 12 | 0 | 0 | 0 | 0 |
| UK Health Security Agency surveillance | 0 | 0 | 0 | 0 | 65 |
| us_energy_information_administration | 0 | 291 | 117 | 0 | 0 |
| us_epa_airdata | 0 | 32 | 177 | 0 | 0 |
| usgs_water | 40 | 0 | 1403 | 0 | 0 |
| World Health Organization FluNet | 0 | 0 | 0 | 0 | 2870 |
