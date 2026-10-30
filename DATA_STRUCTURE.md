# /data/fin — data inventory

Working map of this data repo for research and for agents. Everything below was verified by
inspection on **2026-09-01**; schemas and sample rows are copied from real files.

## Rules of the road

- **Read parquet, not staging.** `*_v1` directories are download staging (see
  [Staging dirs](#staging-dirs-_v1-and-the-zero-byte-convention)). Use the non-`_v1` parquet trees.
- **You do not need to run the pipeline.** The `download_*.py` / `csv2parquet_*.py` / `gen_*.py`
  scripts are run periodically by the owner. Read the parquet files.
- **This is an NFS mount** (10 GbE, ~400 MB/s read, ~280 MB/s write, 61 T total). Big reads are
  bandwidth-bound — use column projection and row-group filters instead of reading whole files.
- **Every bar timestamp is timezone-aware** (normalized 2026-09-01). US equities, options,
  futures and indices are `America/New_York`; forex and crypto are `UTC`. No naive timestamps
  remain in the bar datasets.
- Storage used: **23 T of 61 T**. Largest single tree is `us_options_opra/quotes` (~8.2 T).

## Quick index

| Dataset | Path | Grain | Coverage |
|---|---|---|---|
| Stock day bars | `us_stocks_sip/day_aggs/<date>.parquet` | ticker × day | 2003-09-10 → 2026-08-31 |
| Stock minute bars | `us_stocks_sip/minute_aggs/<date>.parquet` | ticker × minute | 2003-09-10 → 2026-08-31 |
| Stock bars by ticker | `us_stocks_sip/day_aggs_by_ticker/<TICKER>.parquet` | ticker | 36,347 tickers |
| Split/div-adjusted bars | `us_stocks_sip/adjusted_day_aggs_by_ticker/<TICKER>.parquet` | ticker | 36,347 tickers |
| Stock minute by ticker | `us_stocks_sip/minute_aggs_by_ticker/<year>/<TICKER>.parquet` | ticker × year | 2002 → 2026 |
| Stock matrix + indicators | `us_stocks_sip/minute_aggs_matrix/<date>.parquet` | minute × (ticker, field) | 2003-09-10 → 2026-06-05 |
| Stock quotes (NBBO ticks) | `us_stocks_sip/quotes/<date>/<date>-<TICKER>.parquet` | tick | 2024-03-01 → 2026-08-11 |
| Stock trades (ticks) | `us_stocks_sip/trades/<date>/<date>-<TICKER>.parquet` | tick | 2024-03-01 → 2026-08-11 |
| Trade+quote 10 s bars | `us_stocks_sip/tq_aggs/<date>/<date>-<TICKER>.parquet` | ticker × 10 s | 2020-01-02 → 2026-07-10 |
| Options day bars | `us_options_opra/day_aggs/<date>.parquet` | contract × day | 2014-06-02 → 2026-08-31 |
| Options minute bars | `us_options_opra/minute_aggs/<date>.parquet` | contract × minute | 2014-06-02 → 2026-08-31 |
| Options quotes (ticks) | `us_options_opra/quotes/<date>/<date>-<UNDERLYING>.parquet` | tick | 160 days, 2025-01-02 → 2026-08-11 |
| Options trades (ticks) | `us_options_opra/trades/<date>/<date>-<UNDERLYING>.parquet` | tick | 402 days, 2025-01-02 → 2026-08-11 |
| Index bars | `us_indices/{day,minute}_aggs/<date>.parquet` | index × bar | 2023-03-09 → 2026-08-11 |
| Futures quotes/trades/bars | `us_futures_{cme,cbot,comex,nymex}/…` | tick / minute | 2021-08-10 → 2026-08-11 |
| FX bars | `global_forex/{day,minute}_aggs/<date>.parquet` | pair × bar | 2010-01-01 → 2026-08-11 |
| Crypto bars | `global_crypto/{day,minute}_aggs/<date>.parquet` | pair × bar | 2013-11-04 → 2026-08-11 |
| FX quotes (ticks) | `global_forex/quotes/<date>.parquet` | tick | 2026-01-01 → 2026-10-09 |
| Crypto trades (ticks) | `global_crypto/trades/<date>.parquet` | tick | 2026-01-01 → 2026-10-10 |
| Crypto live top-20 books/trades | Daily books: `global_crypto/books/{coinbase,binance}/<date>/<date>-<ASSET>.parquet` and `perpetuals/kalshi/books/<date>/<date>-<ASSET>.parquet`; S3 retains hourly native-symbol files | event; UTC receipt-time windows | Recorder dataset introduced 2026-10-17; Kalshi crypto perps, matching Coinbase USD pairs, and Binance spot USDT pairs added 2026-10-20. Daily books are rebuilt from committed S3 hours by `download_crypto_books.py`; embedded coverage metadata describes partial days and gaps. Collector trades are not fetched by this script. See [dataset notes](global_crypto/books/README.md). |
| xStocks (tokenized stocks, 24/7) | `global_xstocks/{kraken,bybit}/{trades,minute_aggs,day_aggs}/<date>.parquet` | tick / bar | 2025-06-30 → 2026-10-12 (Kraken), → 2026-10-11 (Bybit) |
| Kalshi perps | `perpetuals/kalshi/{trades,minute_aggs,day_aggs}/<date>.parquet` | tick / bar | 2026-06-03 → 2026-10-12 |
| Hyperliquid perps (core + HIP-3) | `perpetuals/hyperliquid/{trades,minute_aggs,day_aggs,fills,candles_1s,books_1m,positions}/` | fill / tick / 1-min book / daily positions | 2025-07-28 → 2026-10-11 |
| Splits / dividends / tickers | `us_stocks_sip/{splits,dividends,tickers}.parquet` | table | — |
| Split / total-return factors | `us_stocks_sip/adjustment_factors.parquet` | security × event | rebuilt daily |
| Ticker changes / inactive tickers | `us_stocks_sip/{ticker_changes,tickers_inactive}.parquet` | event / ticker | 11.8 k events, 23.6 k inactive |
| Security ↔ symbol history | `us_stocks_sip/security_history.parquet` | segment | ~37 k securities |
| Corporate events | `us_stocks_sip/corporate_events.parquet` | event | 1.01 M rows |
| Earnings calendar | `benzinga/earnings.parquet` | ticker × event | 2010-04-30 → 2027 |
| ETF flows / taxonomy / holdings | `etf_global/…` | ETF × day | 2017-04-03 → 2026-08 |
| FINRA short volume | `finra_short_volume/…` | ticker × day | 2024 → 2026 |
| Press releases | `us_stocks_sip/press/<TICKER>.parquet` | article | 73 tickers, → 2026-03 |
| Carbon Arc alternative-data panels | `carbonarc/CA0056/day_aggs_by_ticker/`, `carbonarc/CA0056/constant_shopper/day_aggs_by_ticker/`, `carbonarc/CA0054/day_aggs_by_app/` | ticker/app × day × channel/platform | Updated 2026-09-27: 11 Core restaurant tickers (spend/transactions; users only for CMG/CAVA/SG); separate CAVA Constant Shopper spend/transactions and BROS/SBUX Constant Shopper transactions only; Duolingo users. Card histories start 2023-01-01 and end 2026-09-12/13. Eight new Core names have 41 missing Online keys; Constant Shopper BROS has 50 missing Online keys in 2023. Duolingo 2023-12-01 → 2026-09-23. Immutable retrieval snapshots, not historical release vintages; see [archive notes](carbonarc/README.md). |
| Apify alternative-data pilot | `apify/carvana/vehicles/`, `apify/doordash/menu_prices/` | VIN or store/item × capture | Added 2026-09-25 Pacific: 25 current-timestamp Carvana listings; CMG/CAVA/SG menus at 9 locations (461 distinct store/name/price rows). Separate 21-row stale cache retained and flagged. Prospective snapshots only; see [archive notes](apify/README.md). |

---

## Conventions

### File naming

- `<date>.parquet` — one file per session, all symbols inside (aggregate datasets).
- `<date>/<date>-<SYMBOL>.parquet` — tick datasets: a directory per session, one file per symbol.
  The date is repeated in the filename on purpose so files remain identifiable when copied.
- `<TICKER>.parquet` — "by_ticker" pivots: whole history of one symbol in one file.
- For the converted Massive daily datasets, a day directory only exists once that day
  finished converting, so **presence == complete**. A directory ending in `.tmp` is a
  conversion in flight — ignore it. **Exception: live recorder files under
  `global_crypto/{books,trades}/<venue>/` can contain a partial day. Daily book
  files are atomically replaced as more committed S3 hours arrive; their embedded
  coverage metadata, not directory presence, describes actual coverage. S3 and
  older local hourly files retain their manifest-defined coverage.**

### Staging dirs (`*_v1`) and the zero-byte convention

`*_v1` holds the raw Polygon flat files (`<date>.csv.gz`) as downloaded. After a day converts,
the converter **truncates the file to 0 bytes but keeps it** — `download_flatfiles.py` skips any
object whose local file exists, so the empty file is what stops it re-downloading ~100 GB. So:

- `0 bytes` in `*_v1` = "downloaded and already converted".
- non-zero = downloaded but not yet converted (or a dataset whose sources were never freed).
- **Never delete these placeholders**, and never treat a 0-byte `.csv.gz` as corrupt.

### Index columns

Most files were written by pandas and carry an index in the parquet metadata, so
`pd.read_parquet` restores it. Typical indices:

| Dataset | Index |
|---|---|
| stock `day_aggs` | `window_start` (ticker is a column) |
| stock `minute_aggs`, futures/forex/crypto/indices aggs | `(ticker, window_start)` |
| `us_indices/day_aggs_by_ticker` | `window_start` (regenerated 2026-09-01) |
| `*_by_ticker` | `window_start` |
| options `day_aggs` / `minute_aggs` | `(underlying, expiry, type, strike, window_start)` |
| options `trades` | `(expiry, type, strike)` |
| options `quotes` | none — flat columns |
| `splits` | `(ticker, execution_date)`; `dividends` | `(ticker, ex_dividend_date)` |

### Option contract encoding

OPRA tickers `O:<UNDERLYING><YYMMDD><C|P><strike×1000, 8 digits>` are parsed into columns:

- `underlying` — folded into the filename for tick data, a column/index level for aggregates
- `expiry` — **integer `YYMMDD`** (e.g. `260417` = 2026-04-17), not a date
- `type` — `"C"` / `"P"` string
- `strike` — float dollars (the raw 8-digit integer ÷ 1000)

### Timezones

| Dataset family | Timezone |
|---|---|
| `us_stocks_sip`, `us_options_opra`, `us_futures_*`, `us_indices` | `America/New_York` |
| `global_forex`, `global_crypto` | `UTC` |

The split is deliberate: US markets stay on exchange local time so session boundaries
(09:30/16:00) are stable across daylight-saving changes, while 24/7 markets stay on UTC so they
have no DST discontinuity at all.

Normalized on 2026-09-01 by `fix/fix_timezones.py`:

- `us_indices/day_aggs` — 503 files (2023-03-09 … 2025-02-10) had a **naive** `window_start`
  holding UTC clock times; they were localized to UTC and converted to New York, so
  naive `2024-11-25 06:00:00` is now `2024-11-25 01:00:00-05:00` — the same instant, and
  identical to how the newer files already stored it.
- `us_indices/day_aggs_by_ticker` — was built by `tz_localize`-ing those naive values straight
  to New York, i.e. **every pre-2025-02-10 bar was 4–5 hours off**. Regenerated from the
  corrected `day_aggs`; it now matches bar for bar.
- `global_crypto/minute_aggs` — 4,477 files were `America/New_York` and 181 `UTC`; all are
  now `UTC`. Converting a tz-aware column relabels it without moving the instant, so no
  values changed.

If a naive *instant* ever enters these trees again, the six generators that consume them
(`gen_aggs_by_ticker.py`, `gen_aggs_by_ticker2.py`, `gen_last_n_day_aggs.py`,
`gen_aggs_adjusted.py`, `gen_last_n_adjusted_day_aggs.py`, `predict_spx_from_spy.py`) assume it
is New York wall clock and print a `WARNING` naming the file — normalize the source first with
`fix/fix_timezones.py`. Naive **calendar dates** (`splits.execution_date`,
`dividends.ex_dividend_date`) are legitimate and do not warn.

### Compression and row order

- Options quotes: **zstd**, delta-encoded timestamps, 1 M-row row groups, rows grouped by
  contract (see below). ~6.2 bytes/row.
- Forex quotes, crypto trades: **zstd**, delta-encoded timestamps, 1 M-row row groups, rows
  ordered by `(ticker, participant_timestamp)`, one file per day.
- Everything else: snappy, pandas defaults.

---

## `us_stocks_sip/`

### `day_aggs/` — 5,780 date files (+8 rollups), `<date>.parquet`, ~0.25 MiB/day

```
schema: ticker:string, volume:int32, open:float, close:float, high:float, low:float,
        transactions:int32, window_start:timestamp[ns, tz=America/New_York]   index: window_start

                          ticker    volume       open      close       high        low  transactions
window_start
2015-03-06 05:00:00-05:00      A   1528564  41.950001  41.529999  42.049999  41.490002         11455
2015-03-06 05:00:00-05:00     AA  19998204  14.310000  14.480000  14.560000  14.230000         62926
```

This directory **also** contains rolled-up convenience files, not just dates:
`last365.parquet`, `last730.parquet`, `last1460.parquet`, `last2920.parquet` and
`adjusted_last*.parquet` (same schema, N calendar days of history in one file). Glob
`20??-??-??.parquet` if you want dates only.

### `minute_aggs/` — 5,780 files, ~18 MiB/day

```
schema: volume:int32, open:float, close:float, high:float, low:float, transactions:int32,
        ticker:string, window_start:timestamp[ns, tz=America/New_York]   index: (ticker, window_start)

                                  volume     open      close       high      low  transactions
ticker window_start
A      2015-03-02 09:30:00-05:00   46107  42.3400  42.369701  42.459999  42.3400            84
       2015-03-02 09:31:00-05:00    5146  42.3797  42.400002  42.410000  42.3703            46
```

Bars are wall-clock minutes including pre/post market (04:00–20:00 ET).

### `day_aggs_by_ticker/` and `minute_aggs_by_ticker/<year>/` — per-symbol pivots

36,347 ticker files for days; minute data is split by year (`2002/` … `2026/`, plus `last730/`),
e.g. `2015/` holds 9,170 tickers. Same OHLCV columns, indexed by `window_start`.

```
us_stocks_sip/day_aggs_by_ticker/AAPL.parquet
                            volume   open      close       high        low  transactions
window_start
2003-09-10 04:00:00-04:00  3957751  22.25  22.180000  22.610001  22.110001         10128
```

### `adjusted_day_aggs_by_ticker/`, `adjusted_minute_aggs_by_ticker/last730/`

Split/dividend-adjusted versions. Raw OHLCV is retained and adjusted columns added:

```
schema: window_start, volume, open, close, high, low, transactions,
        cum_factor:double, adj_open:double, adj_high:double, adj_low:double, adj_close:double

               window_start   volume   open      close  transactions  cum_factor  adj_open  adj_close
0 2003-09-10 04:00:00-04:00  3957751  22.25  22.180000         10128    0.014943  0.332486   0.331440
```

`cum_factor` is the cumulative adjustment applied; `adj_* = raw × cum_factor`. Note these files
have a plain RangeIndex — `window_start` is a column, not the index.

### `minute_aggs_matrix/` — wide matrix with indicators, 5,721 files

> ⚠️ **SURVIVORSHIP / LOOK-AHEAD BIAS — DO NOT USE THIS FOR UNIVERSE SELECTION.**
> The columns of each daily file are the **top ~1024 names of that day**, chosen using that
> day's own data. Trading "the names in today's matrix" therefore uses information that was not
> available before the session, and it silently excludes anything that was liquid earlier but
> died. This has repeatedly produced **false strategies** in backtests here.
> Use the matrix only for reading bars of a universe you selected elsewhere — build the universe
> from data strictly prior to the session (e.g. `day_aggs` up to T-1, or a point-in-time
> membership source such as `etf_global/constituents`), then pull those columns.

One file per session, 391 rows (minutes), columns are a **MultiIndex `(ticker, field)`** over the
top ~1024 names, `field ∈ {open, high, low, close, volume, vwap, atv, ema12, ema26, macd, rsi}`.
Index `window_start`. ~32 MiB/day. Read a slice, not the whole thing:

```python
pd.read_parquet(path, columns=[("AAPL","close"), ("MSFT","close")])
```

### `quotes/` and `trades/` — tick data, 613 sessions (2024-03-01 → 2026-08-11)

`<date>/<date>-<TICKER>.parquet`, ~11.4 k symbols per session.

```
quotes  schema: sip_timestamp:timestamp[ns, tz=NY], ask_exchange:int16, ask_price:float,
                ask_size:int32, bid_exchange:int16, bid_price:float, bid_size:int32

trades  schema: sip_timestamp, participant_timestamp, price:float, size:int32, exchange:int16,
                sequence_number:int64, conditions:string, correction:int8, id:int64, tape:int8,
                trf_id:int64, trf_timestamp:int64

                        sip_timestamp   price  size  exchange  conditions  tape
0 2025-05-21 09:30:00.158838496-04:00  24.535   100        19       16,41     1
1 2025-05-21 09:38:43.436777865-04:00  24.455   721         4                 1
```

`conditions` is a comma-joined string of Polygon condition codes (empty = no flags).

### `tq_aggs/` — merged trade+quote 10-second bars, 1,638 sessions (2020-01-02 → 2026-07-10)

```
schema: last:float, last_size:double, volume:double, bid:float, bid_size:double,
        ask:float, ask_size:double, window_start   index: window_start

                                last  last_size  volume   bid  bid_size        ask  ask_size
2023-04-04 09:30:00-04:00  79.790001        7.0    14.0  79.0       1.0  81.199997       1.0
2023-04-04 09:30:10-04:00  79.790001        7.0     0.0  79.0       1.0  81.199997       1.0
```

Last-observation-carried-forward within the bar; `volume` is the bar's traded volume (0 = no trade).

### Reference tables

```
splits.parquet      28,136 rows   index (ticker, execution_date); split_from, split_to
dividends.parquet    2.00 M rows  index (ticker, ex_dividend_date); cash_amount, currency,
                                  dividend_type, frequency, pay_date, record_date, declaration_date
tickers.parquet     13,136 rows   ticker, name, market, locale, primary_exchange, type, active,
                                  currency_name, cik, composite_figi, share_class_figi, last_updated_utc
corporate_events.parquet 1.01 M   isin, ticker, type, tmx_record_id, tmx_company_id, status, url,
                                  name, company_name, trading_venue, date:date32
```

```
tickers.parquet
  ticker                       name  market locale primary_exchange type  active  cik
0      A  Agilent Technologies Inc.  stocks     us             XNYS   CS    True  0001090872

corporate_events.parquet   type ∈ {stock_split, earnings_conference_call, …}, status ∈ {approved, confirmed, …}
0  CA8062153074  CA:SVP.H  stock_split  approved  …  Reverse Stock Split
```

**Using splits** (for P&L held across a split date): 5:1 reverse split has `split_from=5`,
`split_to=1`. On the split date (1) adjust share count `new_shares = old_shares × (split_to/split_from)`;
(2) use a split-adjusted prior price so the move is not double counted:
`adj_prev = price_prev × (split_from/split_to)` in post-split terms, then
`pnl = shares × (price_now − adj_prev)`. Note `splits.parquet` contains some non-ticker
identifiers (CUSIP-like keys) alongside real tickers.

### `ticker_changes.parquet`, `tickers_inactive.parquet` — symbol history

Downloaded daily by `download_ticker_changes.py` (after `download_tickers.py`). Bars, splits and
dividends are all keyed by the symbol **as it was on that day**, so one security's history can be
split across symbols, and one symbol can hold several securities:

- `QQQ.parquet` has a gap 2004-11-30 → 2011-03-23; those days are in `QQQQ.parquet`.
- `META.parquet` starts with Roundhill's Metaverse ETF (~$15, 2021-06-30 → 2022-01-31) before
  Meta Platforms (~$164 from 2022-06-09); Meta's earlier history is in `FB.parquet`.

```
ticker_changes.parquet     11.8 k rows  composite_figi, cik, name, ticker, date:date32, fetched_utc
                                        one row per Polygon ticker_change event; `ticker` is in
                                        effect from `date` (until the FIGI's next event)
       composite_figi         cik                                       name ticker        date
       BBG000MM2P62    0001326801  Meta Platforms, Inc. Class A Common Stock     FB  2012-05-18
       BBG000MM2P62    0001326801  Meta Platforms, Inc. Class A Common Stock   META  2022-06-09

tickers_inactive.parquet   23.6 k rows  ticker, name, type, primary_exchange, cik, composite_figi,
                                        share_class_figi, delisted_utc, last_updated_utc
       QQQQ  POWERSHARES QQQ TR UNIT SER 1  XNAS  0001067839  None  None  2011-03-23 04:00:00+00:00
```

Caveats: events exist only for FIGIs active today (10.8 k), and Polygon's event history has
gaps (QQQ's only event is `QQQ 2018-06-04`; QQQQ isn't mentioned). Many 2003 rows are just a
ticker's first date in Polygon's history, not real changes. Old inactive tickers mostly lack a
FIGI (6.0 k of 23.6 k have one) but carry the issuer CIK and a delisting date, so QQQQ links to
QQQ via CIK `0001067839` plus QQQQ's delisting on the day QQQ's bars resume. CIK is per issuer,
not per security (share classes, preferreds and warrants share it), so CIK links need that
date/price continuity check — `gen_security_history.py` does this. `ticker_changes_state.parquet` is the downloader's incremental
state (`composite_figi`, `fetched_utc`, `found`, `ticker_at_fetch`), not data.

### `security_history.parquet` — which symbol each security traded under, when

Rebuilt daily by `gen_security_history.py` (after `day_aggs_by_ticker` and the ticker-change
download), ~40 k rows for ~37 k securities. Each row is a **segment**: the bars of `ticker` dated
`start`..`end` (NY dates, inclusive) belong to `security_id`. To read a security's full history,
take its rows sorted by `start` and read each `ticker` clipped to its range.

```
schema: security_id, current_ticker, ticker, start:date32, end:date32, n_bars, first_close,
        last_close, source, link, composite_figi, cik, name

 security_id   current_ticker ticker      start        end  source      link        cik
 BBG000BSWKH7  QQQ            QQQ    2003-09-10 2004-11-30  default                 None
 BBG000BSWKH7  QQQ            QQQQ   2004-12-01 2011-03-22  default     continuity  0001067839
 BBG000BSWKH7  QQQ            QQQ    2011-03-23 2026-10-28  figi_event  handoff     0001067839
 BBG000D4LWF6  MDLZ           KFT    2003-09-10 2012-10-01  default                 0001103982
 BBG000D4LWF6  MDLZ           MDLZ   2012-10-02 2026-10-28  figi_event  handoff     0001103982
 BBG011J1MN62  METV           META   2021-06-30 2022-01-28  figi_event              0001776878
```

- `security_id` is the composite FIGI when there is one, else `<ticker>@<first date>`;
  `current_ticker` is the ticker of its latest segment. To resolve a symbol typed today, take the
  `ticker == T` row with the latest `end`.
- `source`: `figi_event` (a Polygon ticker_change event assigns the bars), `figi_current` (the
  FIGI's current ticker with no event for it), `default` (no FIGI claims them; one security per
  unclaimed run, split at gaps of a year or more — `TT` 2003–05, 2007–08 and Trane from 2020
  are three securities).
- `link` says how a segment joined the previous one: `figi`, `handoff` (same CIK, predecessor
  delisted the day the successor starts — accepted up to a 3× price jump, e.g. KFT → MDLZ fell
  34% on the Kraft Foods Group spinoff), `continuity` (same CIK, next bar within 10 days within
  15% in price or a split ratio). Links need the same security type (no units → shares, warrants
  → shares) and are skipped for CIKs with more than 4 securities trading at the boundary (fund
  trusts: iShares' hundreds of $25 ETFs share one CIK), never merge two FIGIs, and ambiguous
  matches are left unlinked.
- A FIGI claim extends back over earlier unclaimed bars of its ticker until a gap of a year
  (Polygon event dates are often late: QQQ's is 2018). Overlapping segments within a security (a
  when-issued `MDLZV` alongside `KFT`) are trimmed so the earlier one wins; `n_bars`/`first_close`
  describe the untrimmed run.
- Known limits: spinoffs aren't price-adjusted (MDLZ's chart drops at the KFT handoff), and
  securities whose symbol changed without a FIGI event or a same-CIK delisting stay separate.

### `adjustment_factors.parquet` — cumulative split / total-return factors

Rebuilt from scratch daily by `gen_adjustment_factors.py` (after `gen_security_history.py`), one
row per (security, event date), sorted by `security_id` then date. ~460 k rows for ~15 k
securities. Factors follow the security, not the symbol: QQQ's include its QQQQ years, META's
exclude the Roundhill ETF, MDLZ's include Kraft's dividends back to 2003.

```
schema: security_id:string, ticker:string (current), date:date32, split_ratio, div_factor, dividend,
        ref_close, split_adj, total_adj (float64)

       security_id ticker        date  split_ratio  div_factor  dividend  ref_close  split_adj  total_adj
       BBG000BBJQV0  NVDA  2006-04-07     0.500000    1.000000     0.000        NaN   0.008333   0.007623
       BBG000BBJQV0  NVDA  2012-11-20     1.000000    0.993589     0.075    11.6983   0.025000   0.022868
```

**Usage:** a raw price on New York date `d` × the factor of the security's **first row with
`date > d`** (1.0 if none). `split_adj` = splits only; `total_adj` = splits + USD cash dividends
(total return). Factors are anchored so the latest bar is 1.0, so they change whenever a new
event lands — always read the current file rather than caching values.

```python
sid = "BBG000BSWKH7"     # from security_history.parquet
f = pd.read_parquet("us_stocks_sip/adjustment_factors.parquet", filters=[("security_id", "=", sid)])
i = np.searchsorted(f["date"].values, bar_dates, side="right")   # bar_dates: datetime64[D], NY
adj = raw * np.r_[f["split_adj"].values, 1.0][i]
```

Per-event columns: `split_ratio = split_from/split_to`; `div_factor = 1 − dividend/ref_close`
with `ref_close` the last close strictly before the ex-date (in post-split terms on a same-day
split). Each ticker's events go to the security holding that ticker on the event date; an event
up to 30 days before a security takes the ticker goes to it too (Polygon re-labels: QQQ's
2011-03-18 dividend was paid while it traded as QQQQ). The same event reported under two
symbols counts once. Events apply to all bars before their date even with no bar on it;
non-USD dividends and events after the security's last bar (declared future dividends) are left
out; dividends ≥ the prior close are dropped as bad data. Metadata key `adjustment_factors`
holds `built`, `asof` and the input mtimes. Polygon has no dividends under `QQQQ`, so QQQ's
total return misses 2005–2010 dividends.

### `press/` and `press_json/`

73 tickers of company press releases. Parquet: `ticker, timestamp (tz NY), title, text`.
`press_json/<TICKER>/<slug>.json` holds the same records one file per article (same four keys).
Last refreshed 2026-03.

```
  ticker                 timestamp                                                             title
0   HOOD 2023-01-12 09:00:00-05:00  The Wait(list) is Over – Robinhood Retirement is Now Available…
```

### `etf_holdings/` — 53 ETFs, scraped composition

```
schema: url:string, name:string, weight:double, asOf:string, symbol:string
0  None                        U.S. Dollar  0.1194  2026-08-06T00:00:00.000Z   None
1  https://www.etf.com/…/NVDA  NVIDIA Corp  0.0705  2026-08-06T00:00:00.000Z   NVDA
```

Cash rows have `symbol=None`. A point-in-time history is in `etf_global/constituents/` instead.

---

## `us_options_opra/`

### `day_aggs/`, `minute_aggs/` — 3,084 / 3,081 files, 2014-06-02 → 2026-08-31

```
schema: ticker:string, index:int64, volume:int32, open:float, close:float, high:float, low:float,
        transactions:int32, underlying:string, expiry:int64, type:string, strike:double,
        window_start:timestamp[ns, tz=America/New_York]
index: (underlying, expiry, type, strike, window_start)

                                                        ticker  volume  open  close  high   low  transactions
underlying expiry type strike window_start
A          200717 C    85.0   2020-07-13 00:00:00-04:00  O:A200717C00085000  22  5.60   5.37  6.12  5.37    4
                       87.5   2020-07-13 00:00:00-04:00  O:A200717C00087500  30  3.43   3.16  3.80  3.16    8
```

**No implied vol is precomputed.** Derive IV from the option close plus the underlying spot
(`us_stocks_sip/day_aggs`) via Black-Scholes. The stray `index:int64` column is a leftover
row counter — ignore it.

### `quotes/` — the big one: 160 sessions, ~8.2 T, ~6,100 underlyings/session

`<date>/<date>-<UNDERLYING>.parquet`. Every option on one underlying for one session in one file:
from 30 kB for an illiquid name to **6 GB for SPXW** (~1.3 B rows).

```
schema: expiry:int32, type:string, strike:double, sip_timestamp:timestamp[ns, tz=America/New_York],
        ask_exchange:int16, ask_price:float, ask_size:int32,
        bid_exchange:int16, bid_price:float, bid_size:int32          (no index)

   expiry type  strike                       sip_timestamp  ask_exchange  ask_price  ask_size  bid_price  bid_size
0  260417    C    12.0 2026-04-17 09:30:00.143145054-04:00           307  18.600000         1       16.5         1
1  260417    C    12.0 2026-04-17 09:30:00.196973165-04:00           307  19.700001         1       15.4         1
```

**Row order is by contract, not by time**: all rows of one `(expiry, type, strike)` are contiguous
and time-ordered within that contract; contracts are sorted by `(expiry, type, strike)`. A file
contains 2–8 such sorted runs (the source flat file is a few concatenated partitions), so a
contract appears exactly once but the file is not globally sorted by either key.

This layout is what makes big files usable — parquet row-group statistics let you skip almost
everything:

```python
# ~0.06 s instead of ~4 s: reads only the row groups holding that expiry/strike band
pq.read_table(f, filters=[("expiry","=",260417), ("strike",">=",690), ("strike","<=",710)])
```

Coverage is **not** every session: 160 days spread over 2025-01-02 → 2026-08-11.
List `us_options_opra/quotes/` before assuming a date exists.

### `trades/` — 402 sessions, `<date>/<date>-<UNDERLYING>.parquet`

```
schema: conditions:double, correction:int64, exchange:int64, price:double,
        sip_timestamp:timestamp[ns, tz=NY], size:int64, underlying:string,
        expiry:int64, type:string, strike:double        index: (expiry, type, strike)

                    conditions  correction  exchange  price                    sip_timestamp  size underlying
expiry type strike
260220 P    2.5          227.0           0       323   0.12 2025-10-22 09:56:37.223000-04:00     5       KTCC
```

`conditions` is a float here (single code), unlike the string form in stock trades.

---

## `us_indices/`

| Path | Files | Coverage | Notes |
|---|---|---|---|
| `day_aggs/<date>.parquet` | 894 | 2023-03-09 → 2026-08-11 | index `(ticker, window_start)`; **no volume/transactions** — only OHLC |
| `day_aggs_by_ticker/<TICKER>.parquet` | 14,177 | — | index `window_start`; regenerated 2026-09-01 |
| `minute_aggs/<date>.parquet` | 890 | 2023-03-09 → 2026-08-11 | ~107 MiB/day, 6.2 M rows |
| `minute_aggs_by_ticker/` | 1 | — | only `I:SPX.parquet` |

```
                                            open       close        high         low
ticker     window_start
I:AAVE100  2024-11-25 01:00:00-05:00  172.758804  181.025894  188.357101  171.841095
```

Index tickers carry the `I:` prefix (`I:SPX`, `I:VIX`, …). All timestamps are
`America/New_York`; index day bars start at 01:00 ET, not midnight.

---

## `us_futures_{cme,cbot,comex,nymex}/`

Same layout in all four exchange roots.

| Path | Files | Coverage |
|---|---|---|
| `minute_aggs/<date>.parquet` | ~930 | 2023-01-02 → 2026-08-11 |
| `quotes/<date>/<date>-<ROOT>.parquet` | ~1,550 sessions | 2021-08-10 → 2026-08-11 |
| `trades/<date>/<date>-<ROOT>.parquet` | ~1,300 sessions | 2021-08-10 → 2026-08-11 |

Files are per **product root** (`ES`, `MBT`, `6A`, …), holding every contract month and listed
spread on that root.

```
quotes schema: ticker:string, type:string, expiry:int32, timestamp:timestamp[ns, tz=NY],
               sequence_number:int64, report_sequence:int64,
               ask_timestamp, ask_price:double, ask_size:int32,
               bid_timestamp, bid_price:double, bid_size:int32, exchange:int16

        ticker type  expiry                           timestamp  ask_price  ask_size
0  MBTG4-MBTJ4    C       0 2024-02-08 17:55:00.008746347-05:00      820.0         1
1  MBTG4-MBTH4    C       0 2024-02-08 17:55:00.418455835-05:00        NaN         0

trades schema: ticker, timestamp, sequence_number, report_sequence, price:double, size:int32,
               correction:int16, exchange:int16, root:string, type:string, expiry:int32
0  METH4 2024-02-07 18:00:05.672606071-05:00  2455.5  5  MET  O  202403
```

**`type` / `expiry` semantics** (both quotes and trades):

- `type='O'` — outright (e.g. `ESU6`); `expiry` is an integer **`YYYYMM`** (`202403`).
- `type='C'` — listed spread/combo (`6AU6-6AQ6`, `KE:BF H7-K7-N7`, `SR3:DF …`); `expiry = 0`
  because a spread has no single expiry. It is filed under the root of its first leg.

Filter `type == 'O'` for a clean outright series. Note the futures session starts the **previous
evening** (17:55 ET rows sit in the next session's file), and a quote row can have a NaN side.

---

## `global_forex/` and `global_crypto/`

```
day_aggs/<date>.parquet, minute_aggs/<date>.parquet     index: (ticker, window_start)
schema: volume:int32, open:float, close:float, high:float, low:float, transactions:int32,
        ticker:string, window_start:timestamp

                                     volume     open    close     high      low  transactions
ticker    window_start
C:AED-AUD 2018-07-17 00:00:00+00:00     113  0.36711  0.36850  0.36871  0.36621           113
```

Both are **UTC** throughout (day and minute). FX pairs use the `C:` prefix (`C:EUR-USD`),
crypto uses `X:` (`X:BTC-USD`).
Forex 2010-01-01 → 2026-08-11 (5,355 sessions); crypto 2013-11-04 → 2026-08-11 (4,658).
Crypto files exist for weekends; forex files exist for Sundays (partial sessions).

### `global_forex/quotes/` — 242 files, `<date>.parquet`, ~260 MiB/day

```
schema: ticker:string, participant_timestamp:timestamp[ns, tz=UTC], bid_price:double,
        ask_price:double, bid_exchange:int16, ask_exchange:int16          no index

      ticker     participant_timestamp  bid_price  ask_price  bid_exchange  ask_exchange
0  C:EUR-USD 2026-10-09 00:00:03+00:00    1.12123    1.12130            48            48
```

~1,200 pairs and ~45 M quotes per weekday; 10.5 B rows total (2026-01-01 → 2026-10-09).
Exchange is always 48 in current data. Built by `csv2parquet_forex_quotes.py`.

### `global_crypto/trades/` — 283 files, `<date>.parquet`, ~35 MiB/day

```
schema: ticker:string, participant_timestamp:timestamp[ns, tz=UTC], price:double, size:double,
        exchange:int16, conditions:int8, id:int64                         no index

      ticker            participant_timestamp     price      size  exchange  conditions          id
0  X:BTC-USD 2026-10-10 00:00:00.048394+00:00  82544.71  3.10e-07         1           2  1104413732
```

~370 pairs and ~3 M trades per day; 1.03 B rows total (2026-01-01 → 2026-10-10).
`conditions` is 1 = sell-side or 2 = buy-side aggressor. `exchange` is Polygon's crypto
exchange id (1 Coinbase, 2 Bitfinex, 6 Bitstamp, 23 Kraken). `id` is the exchange's trade id.
Built by `csv2parquet_crypto_trades.py`.

Both tick sets are ticker-ordered, so filter on ticker to read one pair in tens of
milliseconds instead of loading the whole day:
`pd.read_parquet(path, filters=[("ticker", "==", "C:EUR-USD")])`.

### Coinbase and Binance spot books and Kalshi perpetual books — live recorder datasets

Introduced 2026-10-17 by `/ml/crypto-data-recorder`; archive source is
`s3://fin.dheera.net/global_crypto/`. Scope is active Kalshi crypto perpetuals
and only the matching Coinbase USD products (19 coins at initial discovery),
plus 18 matching Binance spot USDT pairs collected since 2026-10-20.
This is separate from the Massive daily crypto trades above and excludes Kalshi
binary/15-minute markets.

Daily local views: `global_crypto/books/coinbase/<date>/<date>-<ASSET>.parquet`,
`global_crypto/books/binance/<date>/<date>-<ASSET>.parquet`
and `perpetuals/kalshi/books/<date>/<date>-<ASSET>.parquet`,
created by `./download_crypto_books.py`. Sources are discovered from S3 books/
automatically; use `--sources binance` for Binance alone (`binance_spot` remains
an accepted alias). Local `global_crypto/` holds spot data, so its Binance
directory is simply `binance`; S3 keys, row source values and provenance retain
the archive source name `binance_spot`. The default rebuilds each asset's
latest local day plus newer days, and fills missing daily files, with no progress
ledger. Use `--date` or `--rebuild-all` to repair older existing files. Every
selected file is reconstructed from checksum-verified, committed S3 hourly
inputs and replaced atomically. It preserves all original rows, order,
timestamps and checkpoints; it does not resample or deduplicate. The filename
uses `base_asset` (including Kalshi KSHIB → SHIB), while the row's native
`symbol` and normalized values are unchanged.

The S3 source remains hourly: `books/<venue>/<date>/<SYMBOL>-<HHMM>.parquet`
with commit manifests beside it; reconnect fragments are assembled before
publication. Historical Kalshi collector trades live at
`perpetuals/kalshi/trades/<date>/<SYMBOL>-<HHMM>.parquet`; the REST updater
writes `perpetuals/kalshi/trades/<date>.parquet`. Coinbase and Binance collector trades
are not downloaded by the book sync; Massive already supplies Coinbase trades. Previous local
Coinbase collector copies were archived under
`_archive_backups/crypto-data-recorder/2026-10-19-layout/coinbase-trades/`.
Earlier local hourly copies coexist and are not inputs to the daily builder.
Massive daily trades remain at `global_crypto/trades/<date>.parquet`.

**A daily file or date directory does not promise complete coverage.** A
current-day daily file includes only hours published at fetch time. The Parquet
footer's `btc_book_recorder` JSON retains hourly source references/checksums,
coverage intervals, gaps and collector identities, plus day-wide coverage and
missing-hour summaries. These are descriptive data provenance, never download
state. Fresh production collection began **2026-10-17 07:20:11 UTC**, so that
first day begins partway through the day. Full book state is internal to the
recorder; only top-20 views and trades are archived.

Version-2 Parquets use `received_timestamp`, `participant_timestamp`, and
`source_send_timestamp` as `timestamp[ns, tz=UTC]`; books additionally have
`state_asof_timestamp`. Source time can mean latest event in a batch or gateway
send, not necessarily matching-engine time: use `source_ts_kind` and
`source_ts_resolution_ns`. Null source times stay null. `monotonic_ns` is a local
boot-relative clock, not UTC. Top-20 bids/asks are nested price/size arrays;
normalized prices are USD per underlying coin for Coinbase/Kalshi, and USDT per
coin for Binance. Sizes are underlying coin units. Binance's 100 ms partial-depth
snapshots have null exchange timestamps; local nanosecond receipt times remain
unchanged. See [dataset notes and schema links](global_crypto/books/README.md).

## `global_xstocks/` — Backed xStocks tokenized equities (Kraken, Bybit spot)

Spot tokens that track US stocks/ETFs 1:1 and trade 24/7. Two venues, **identical schemas**,
one file per **UTC** day, built by `download_xstocks.py` (delta-updates to the latest complete
day; rerun any time). Every day from 2025-06-30 has a file, even if empty.

```
trades/<date>.parquet   schema: ticker:string, participant_timestamp:timestamp[ns, tz=UTC],
                                price:double, size:double, conditions:int8, id:int64   no index
        ticker            participant_timestamp   price       size  conditions  id
0  X:AAOIx-USD 2026-10-12 10:20:46.439154+00:00  107.40   0.401989           1  10

minute_aggs/<date>.parquet, day_aggs/<date>.parquet          index: (ticker, window_start)
schema: volume:double, open/close/high/low:double, transactions:int32, window_start UTC
                                        volume    open   close    high     low  transactions
ticker       window_start
X:AAPLx-USDT 2026-10-09 00:06:00+00:00   0.163  341.48  341.49  341.49  341.48             2
```

- Tickers are `X:<token>-<quote>`: `X:AAPLx-USD` on Kraken, `X:AAPLx-USDT` on Bybit. Strip the
  `X:` and the trailing `x` to get the underlying.
- Trades are zstd, ticker-ordered, like `global_crypto/trades`. `conditions` 1 = sell, 2 = buy
  aggressor. `id`: the Kraken trade id (per pair); on Bybit, the row's sequence number within
  Bybit's daily dump (not globally unique).
- Bars are built from the trades. Minutes with no trades have no row. Unlike `global_crypto`
  aggs, volume and prices are double (fractional shares).
- **Kraken**: 177 pairs, 1.32 M trades (as of 2026-10-12), ~46 MB. Pulled from the public REST
  `Trades` endpoint back to each pair's first trade. Prices and trade counts match Kraken's own
  OHLC endpoint bar for bar; volume differs by ≤1e-5 because Kraken's OHLC rounds it. Pairs
  delisted before a run are not in `symbols.parquet` and their history is not fetched.
  Liquidity is thin: even AAPLx has trades in only about 1 minute in 10.
- **Bybit**: 11 symbols, 9.33 M trades, ~99 MB. From `public.bybit.com/spot` daily dumps,
  published the next day, so Bybit lags Kraken by about one day. Symbols are matched to Kraken's
  xStock list. `VRTXUSDT` is excluded: it's an older crypto token, not Vertiv. The dumps' `rpi`
  flag is not kept.
- `<venue>/symbols.parquet` maps venue symbol → ticker. `kraken/trades_v1/` is a resume spool
  that is empty between runs. Logs are in `global_xstocks/logs/`.

## `perpetuals/kalshi/` — Kalshi perpetual futures (CFTC-regulated, 24/7)

Built by `download_perpetuals_kalshi.py` from Kalshi's public perps REST API, without a key,
so it never uses the account's API limits. It's paced to 10 requests/s and delta-updates to the
latest complete UTC day. Uses the same writer, schemas and layout as `global_xstocks/`:
`trades/`, `minute_aggs/`, `day_aggs/` (one file per UTC day) plus `markets.parquet`.

```
      ticker            participant_timestamp   price    size  conditions                                    id
0  KXAAVEPERP 2026-10-12 00:06:16.676514+00:00  1.7176   389.0           1  072598d7-e929-953c-bffd-c8ecc640c363
```

- 29 markets (20 crypto, 6 metals, 2 indices `KXUS500PERP`/`KXUSTECH100PERP`, 1 energy
  `KXWTIFUTURE`). Includes the 3 inactive ones. 41.7 M trades, 2026-06-03 → 2026-10-12, ~1 GB.
- **`price` is USD per contract**, not per unit of the underlying. Multiply by
  `markets.parquet` `contract_size` to get the underlying price. For example, KXBTCPERP is
  0.0001 BTC, so 8.37 means BTC ≈ $83.7 k. `size` is contracts.
- `id` is Kalshi's `trade_id` (a UUID string). `conditions` is 1 = sell / 2 = buy aggressor.
- Kalshi gives one timestamp to every fill of a sweep, and the API lists them out of order.
  Rows with the same timestamp are stored best price first: ascending for buys, descending for
  sells. With that order, bar open/close match Kalshi's own 1-minute candles exactly. High,
  low and volume match regardless. Checked on BTC, ETH and GOLD for 2026-10-12, 1,439 bars each.
- Not stored: order-book history (Kalshi only serves the live book), and funding.
  `/margin/funding_rates/historical` has 8-hourly funding back to launch if needed.

## `perpetuals/hyperliquid/` — Hyperliquid perps: core crypto + every HIP-3 dex

Built by `download_perpetuals_hyperliquid.py` from Hydromancer's free archive
(`s3://hydromancer-reservoir`). The bucket is requester-pays, so the script needs `aws.json`
and AWS bills the downloads (~$0.11/GB from Tokyo). The full backfill was 245 GB in Oct 2026.
Hydromancer uploads about weekly. The script delta-updates to the newest day it has for core
Hyperliquid. 154 GB on disk.

Tickers are Hyperliquid's own. Core perps (crypto) have no prefix: `BTC`, `HYPE`. HIP-3 dexes,
deployed by third parties on Hyperliquid's engine, are `<dex>:<coin>`. **xyz** = US stocks and
indices (`xyz:NVDA`, `xyz:SP500`). **io** = pre-IPO (`io:ANTH`). **cash/km/mkts** = index perps.
**vntl** = pre-IPO and sector baskets. **flx/hyna/para** = mixed. All times are UTC.

| Dir | Content | Coverage |
|---|---|---|
| `trades/<date>.parquet` | One row per trade (the taker's fill). Same schema and writer as `global_xstocks/` and `perpetuals/kalshi/`. 1.65 B trades | 2025-07-28 → 2026-10-11, every day |
| `minute_aggs/`, `day_aggs/` | Bars from trades, index `(ticker, window_start)` | same |
| `fills/<date>/<date>-<dex>.parquet` | Every fill, both sides (2 per trade), 3.3 B rows, 120 GB | per dex, below |
| `books_1m/<date>.parquet` | 20-level L2 snapshots about once a minute per market | 2025-12-15 → 2026-10-11 |
| `positions/<date>.parquet` | Daily snapshot of every open perp position (all accounts) | 2025-07-29 → 2026-10-11; the source has none for 2025-08-01 or 2025-10-25 → 12-14 |

```
trades:      ticker  participant_timestamp            price   size  conditions               id
          xyz:NVDA  2026-10-11 00:00:21.544000+00:00  230.31  4.342           2   44383359661201
books_1m:  ticker, block_time:timestamp[ns,UTC], block_number:int64,
          bids/asks: list<struct<price:double, size:double, n:int32>>   (n = orders at that level)
positions: snapshot_time, block_number, user, ticker, size (signed), notional, entry_price,
          liquidation_price, leverage_type, leverage:int32, funding_pnl, account_value, account_mode
fills:    ticker, participant_timestamp, price, size, side, direction, realized_pnl, tx_hash,
          order_id, trade_id, fee, fee_token, address, crossed (= taker), start_position,
          client_order_id, builder, builder_fee, deployer_fee, priority_gas, twap_id,
          is_liquidation, liquidation_mark_px, liquidation_method
```

Book naming convention: `books/` contains event-driven book views; `books_1m/`
contains approximately one-minute snapshots, not OHLC aggregates. Book levels
use `price` and `size`, matching the local Massive trade field names. Hyperliquid
also retains `n` (order count at each level); source-specific timestamps and units
are preserved.


- **Fills by dex** (`hyperliquid` = core): hyperliquid 2025-07-28 →, xyz 2025-10-13 →,
  flx/vntl 2025-11-13 → 2026-06-20, hyna 2025-12-03 → 2026-09-02, km 2026-01-12 → 06-17,
  cash 2026-01-20 → 07-02, para 2026-04-01 →, mkts 2026-07-01 →, io 2026-08-19 →.
  Several dexes stop in mid-2026. Hyperliquid still lists them, so they're probably dormant,
  or Hydromancer stopped covering them; this hasn't been checked.
- Core fills are a median of 176 MB per day, up to 589 MB on 2025-10-10 (the crash day). They
  are ticker-sorted with 1 M-row row groups. A one-coin filter on the biggest day reads in 0.3 s,
  so they're kept one file per dex instead of one per coin.
- `trades` leaves out "Net Child Vaults" fills: internal vault netting, not market trades,
  39 on 2026-10-10. Those fills stay in `fills/`. With that rule, minute volume and trade
  counts match Hydromancer's own 1-second candles on all 273,571 bars of 2026-10-10.
  Open/close differ on about 25 bars (0.01%) because a few trade timestamps differ slightly
  between their two datasets.
- **`trade_id` / trades `id` is unique per ticker only**: different coins occasionally share one.
- Hydromancer's 1 s candles, spot and outcome markets, and account-value snapshots are not
  downloaded.

---

## Vendor datasets (one-time pulls, no ongoing API access)

Pulled 2026-08-10 via prorated 1-day Massive (ex-Polygon) add-ons. Scripts:
`download_benzinga_earnings.py`, `download_etfglobal_{fundflows,taxonomies,constituents}.py`.

### `benzinga/` — earnings calendar

`earnings.parquet` (295,754 rows, 2010-04-30 → 2027) plus a per-year split in `earnings/<year>.parquet`.

```
  ticker       date      time date_status  actual_eps  estimated_eps  eps_surprise_percent
0    FGP 2010-04-30  00:00:00   projected        0.41            NaN                   NaN
```

Columns: `actual_eps, estimated_eps, previous_eps, eps_surprise, eps_surprise_percent,
actual_revenue, estimated_revenue, previous_revenue, revenue_surprise, revenue_surprise_percent,
eps_method, revenue_method, currency, date_status, importance, company_name, fiscal_period,
fiscal_year, ticker, last_updated, date, time, notes, benzinga_id` — `time` is the release clock
time (BMO/AMC), `date_status` is `projected`/`confirmed`.
Rows before ~2011 have actuals only (estimate fields sparse).

### `etf_global/`

| File | Rows | Contents |
|---|---|---|
| `fund_flows.parquet` (+ `fund_flows/<YYYY-MM>.parquet`) | 6.84 M | daily per-ETF `fund_flow`, `nav`, `shares_outstanding`, 2017-04-03 → 2026-08 |
| `taxonomies.parquet`, `taxonomies_latest.parquet`, `taxonomies/<YYYY-MM>.parquet` | 4.44 M / 7,168 | ETF classification, 39 columns: `issuer, description, inception_date, tax_classification, product_type, asset_class, category, focus, development_class, region, country, strategic_focus, targeted_focus, objective, secondary_objective, maturity, duration, selection_universe, weighting_methodology, rebalance_frequency, selection_methodology, reconstitution_frequency, management_style, holdings_disclosure_frequency, credit_quality_rating, exposure_mechanism, leverage_style, management_classification, factor, primary_benchmark, esg, leverage_reset, levered_amount, hedge_reset, us_code, isin` |
| `constituents/by_ticker/<ETF>/<year>.parquet` | 79 priority ETFs | point-in-time daily holdings |
| `constituents/by_date/<date>.parquet` | full universe (~3.2 k ETFs) | point-in-time daily holdings |
| `tail_smallmid_screen_panel.parquet` | 2.37 M | derived research panel (see below) |

```
fund_flows:  processed_date effective_date composite_ticker  shares_outstanding      nav  fund_flow
0                2017-04-03     2017-04-03             AADR            825000.0  46.5811        0.0

constituents: processed_date effective_date composite_ticker constituent_ticker    weight  shares_held
0                 2022-01-03     2022-01-03             SOXL                ADI  0.026762     920682.0
```

**Constituents restatements — do not blindly dedupe.** ~2 % of `(ETF, effective_date)` pairs appear
under two `processed_date`s: the vendor re-published that day's holdings the next day, and in about
half of those the weights/membership actually changed. Both versions are stored on purpose and the
API is gone, so dedup is irreversible. Loader rule:

- causal / point-in-time backtests → keep the **lowest** `processed_date` per
  `(composite_ticker, effective_date, constituent)`
- corrected "ground truth" → keep the **highest**

Other quirks: `fund_flows.composite_ticker` can have leading whitespace (strip before joining);
early-2017 constituents cover only ~330–1,700 ETFs (full ~3.2 k by 2018); a few thousand rows have
corrupt `effective_date` (year `0012`/`0014`) — excluded from `by_date` but present in some
`by_ticker` files, so filter `effective_date >= 2017-01-01`. Data is survivorship-free (delisted
ETFs included). `taxonomies_latest` has null metadata rows for a few dead tickers.

`tail_smallmid_screen_panel.parquet` is a derived research panel, not vendor data:
`date, stock, demand, bucket, processed, open, close, volume, dollar_volume, adv20,
next_date, next_open, next_close, next_volume, next_return, signal, current_active, current_etb`.

### `finra_short_volume/`

- `daily/<year>/CNMSshvol<YYYYMMDD>.txt` — raw FINRA daily short-volume files, pipe-delimited,
  2024 → 2026 (~250 files/year):

  ```
  Date|Symbol|ShortVolume|ShortExemptVolume|TotalVolume|Market
  20250102|A|102002|124|308407|B,Q,N
  ```

- `screen_panel_2024_2026.parquet` — 1.32 M rows, FINRA joined to price/volume with derived fields:
  `ticker, date, open, close, volume, dollar_volume, next_date, next_open, next_close,
  next_volume, next_return, adv60, liquidity_rank, signal_day_return, short_volume,
  short_exempt_volume, finra_total_volume, short_ratio, offexchange_share, signed_offshare,
  short_ratio_abnormal, signed_offshare_abnormal, current_active, current_etb`

  ```
    ticker        date       close   volume  short_volume  short_ratio  offexchange_share
  0      A  2024-01-02  138.750000  1408745      214302.0     0.503361           0.302214
  ```

  `short_ratio = short_volume / finra_total_volume`; FINRA volume is off-exchange (ATS + OTC) only,
  so it is a *fraction of* consolidated volume, not short interest.

### `sec_edgar/issuer_tenders/`

- `submissions/<accession>.txt` — 260 raw SEC SGML submissions (`SC TO-I` issuer tender offers).
- `company_tickers_exchange.json` — SEC's ticker↔CIK↔exchange map.

Raw text, no parquet layer; parse the SGML headers (`CONFORMED SUBMISSION TYPE`, `ACCESSION NUMBER`, …).

### `lists/`

`dow30.json`, `sp500.json` — static membership lists, `[{"symbol": "MMM", "name": "3M Company"}, …]`.
Point-in-time membership is **not** available here; use `etf_global/constituents` for that.

### `alpaca_assets.parquet` (root)

13 k+ Alpaca assets indexed by `symbol`: `id, asset_class, exchange, name, status, tradable,
marginable, shortable, easy_to_borrow, fractionable, min_order_size, min_trade_increment,
price_increment, maintenance_margin_requirement, attributes` — useful as a shortability /
tradability / fractionability filter. Contains contra/when-issued symbols with mangled names.

---

## Reading recipes

```python
import pandas as pd, pyarrow.parquet as pq, glob

# one session of stock day bars
df = pd.read_parquet("/data/fin/us_stocks_sip/day_aggs/2026-08-24.parquet")

# whole history of one ticker (already pivoted — do not glob 5,783 day files)
aapl = pd.read_parquet("/data/fin/us_stocks_sip/day_aggs_by_ticker/AAPL.parquet")
adj  = pd.read_parquet("/data/fin/us_stocks_sip/adjusted_day_aggs_by_ticker/AAPL.parquet")

# one expiry out of a multi-GB options quotes file (seconds, not minutes)
t = pq.read_table("/data/fin/us_options_opra/quotes/2026-04-17/2026-04-17-SPXW.parquet",
                  filters=[("expiry", "=", 260417), ("type", "=", "C")],
                  columns=["expiry","type","strike","sip_timestamp","bid_price","ask_price"])

# outright futures only
q = pd.read_parquet("/data/fin/us_futures_cme/quotes/2024-02-09/2024-02-09-ES.parquet")
q = q[q["type"] == "O"]
```

**Performance rules for the big trees** (options/stock quotes and trades):

1. Always pass `columns=` — these files are column-heavy and you rarely need all ten.
2. Use `filters=` on `expiry`/`strike`; row-group stats make it 50–100× faster than a full read.
3. `type` as a pandas object column costs ~90 bytes/row; `to_pandas(strings_to_categorical=True)`
   or `dtype_backend="pyarrow"` drops it to ~41. A full SPXW day is ~60 GB in naive pandas.
4. Prefer `pq.ParquetFile(...).iter_batches()` over `read_table` when scanning a whole big file.
5. Parallelism is limited by NFS, not CPU: ~8 concurrent readers saturate the link.

---

## Known quirks and gotchas

- **Timezones**: US markets `America/New_York`, forex/crypto `UTC`, no naive bar timestamps
  (normalized 2026-09-01 — see [Timezones](#timezones)). Vendor tables (`benzinga`, `etf_global`)
  still carry naive `date`/`processed_date`/`effective_date` values — those are calendar dates,
  not instants, and were left alone.
- **`us_stocks_sip/minute_aggs_matrix` encodes a look-ahead universe** — see the warning in its
  section before using it for anything that selects what to trade.
- **`us_indices/day_aggs/2023-04-07.parquet`** (Good Friday) has 8,492 rows with a bogus
  `window_start` of `1754-08-30`, a Polygon source artifact. It propagates into
  `day_aggs_by_ticker` (7 rows in `I:SPX`). Filter `window_start.year >= 2000`.
- **`expiry` is an integer, not a date**: `YYMMDD` for options, `YYYYMM` for futures outrights,
  `0` for futures spreads.
- **Options quotes are contract-ordered, not time-ordered** (see above). If you need chronological
  order, sort explicitly — or use `us_options_opra/trades`, which is time-ordered within a contract.
- **Options quotes cover only 160 sessions**, not the full trades range.
- **Stock quotes/trades start 2024-03-01**, though staging shows downloads from 2023-01-03
  (see [Stale](#stale-and-incomplete-things)).
- `us_options_opra/quotes/2026-02-20/2026-02-20-MT.parquet.corrupt` — a 1.6 kB truncated file from
  a March 2026 conversion bug, renamed out of the `*.parquet` namespace. That underlying-day is
  missing and its source CSV is gone; re-downloading 2026-02-20 from Polygon is the only recovery.
- `us_stocks_sip/day_aggs/` mixes `<date>.parquet` with `last*.parquet` rollups — glob carefully.
- `splits.parquet` and `dividends.parquet` contain non-ticker identifiers (CUSIP-like) as well as
  tickers.
- The `index:int64` column in options aggregates is a meaningless leftover row counter.

## Stale and incomplete things

State as of 2026-09-01 — worth knowing before you trust a dataset's recency.

| Item | Status |
|---|---|
| `us_stocks_sip/{quotes,trades}` 2023-01-03 → 2024-02-29 | 291 sessions were downloaded, then zeroed, but **never converted** — that raw data is gone; only 2024-03-01 onward exists. The quotes/trades subscription is currently inactive and is being resumed shortly, so coverage stops at 2026-08-11 until then. |
| `us_stocks_sip/minute_aggs_matrix` | last file 2026-06-05 (~3 months behind `minute_aggs`); repopulating is planned — read the look-ahead warning in its section first |
| `us_stocks_sip/tq_aggs` | last session 2026-07-10 (~7 weeks behind quotes/trades) |
| `us_stocks_sip/press`, `press_json` | last refreshed 2026-03-09 |
| `us_indices/*` | staging stops at 2026-08-11, so index bars end there while stocks/options run to 2026-08-31 |
| `prediction/{kalshi,polymarket,polymarket_us}` | empty, reserved for future data |
| `global_forex/quotes_v1`, `global_crypto/trades_v1` | empty staging dirs |

Cleaned up on 2026-09-01: the pending agg sessions were converted (stocks and options day+minute
now run through **2026-08-31**); **4 TB** of never-freed futures `quotes_v1`/`trades_v1` CSV was
converted and released (`update_daily.sh` now passes `--delete-original` on those lines, and its
options-quotes line was fixed and re-enabled); the abandoned `us_indices/minute_aggs_matrix`
experiment, the empty `us_stocks_sip/etf_holdings/old-2025-10-11/`, and both `__pycache__`
directories were removed.

## Pipeline (for reference)

| Script | Produces |
|---|---|
| `download_flatfiles.py` | `*_v1/<date>.csv.gz` staging from Polygon S3 |
| `csv2parquet_aggs.py` | `{day,minute}_aggs` for stocks / options / indices / futures / fx / crypto |
| `csv2parquet_stocks_{quotes,trades}.py` | `us_stocks_sip/{quotes,trades}` |
| `csv2parquet_options_{quotes,trades}.py` | `us_options_opra/{quotes,trades}` |
| `csv2parquet_futures_{quotes,trades}.py` | `us_futures_*/{quotes,trades}` |
| `reencode_options_quotes.py` | one-off: re-encodes existing options quotes into the zstd format (complete as of 2026-09-01) |
| `fix/fix_timezones.py` | one-off: normalizes bar timestamps to a target timezone (`--to`, `--naive-is`) |
| `gen_aggs_by_ticker.py`, `gen_aggs_adjusted.py`, `gen_last_n_*.py` | `*_by_ticker`, `adjusted_*`, `last*.parquet` |
| `gen_stocks_matrix.py`, `gen_options_matrix.py` | `minute_aggs_matrix` |
| `gen_stocks_tq_aggs.py`, `gen_options_tq_aggs.py` | `tq_aggs` |
| `download_{splits,dividends,tickers,corporate_events}.py` | the reference tables |
| `gen_security_history.py` | `us_stocks_sip/security_history.parquet` |
| `gen_adjustment_factors.py` | `us_stocks_sip/adjustment_factors.parquet` |
| `download_ticker_changes.py` | `us_stocks_sip/{ticker_changes,tickers_inactive}.parquet` (incremental; `--full` refetches) |
| `download_xstocks.py` | `global_xstocks/{kraken,bybit}/*` (Kraken REST + Bybit public dumps) |
| `download_perpetuals_kalshi.py` | `perpetuals/kalshi/*` (public perps REST, no key) |
| `download_perpetuals_hyperliquid.py` | `perpetuals/hyperliquid/*` (Hydromancer requester-pays S3, needs `aws.json`) |
| `update_daily.sh` | the nightly driver that chains the above |

`vis/` holds option-chain plotting helpers; `fix/` holds one-off repair scripts; `p` is a
throwaway loop script. Credentials live in `polygon.json`, `kalshi.{json,key}`, `finnhub.json`,
`alpaca.json` — never read or echo these.


## `hyperliquid/private_company_perps/`

Added 2026-10-09 UTC. Immutable `snapshots/<UTC>/` directories contain
`candles/<interval>/<coin>.parquet`, `funding/<coin>.parquet`,
`orderbooks.parquet`, `asset_contexts.parquet`, `source_snapshots.parquet`, and
a small SHA-256 `manifest.json`. Initial symbols: `io:OAI`, `io:ANTH`,
`xyz:OURA`; intervals 5m/15m/1h/1d. Source API retention limits apply.

Candle index `window_start` is UTC bar-open; `bar_end` is exclusive UTC end
(`T+1ms`); filter `bar_end <= captured_at` before using completed bars. OHLC,
volume, transactions and ticker are explicit columns. Funding retains actual
UTC `time`, rate, premium, ticker and capture time. Book rows retain side,
level, price and size. Source envelopes (including initial public GitHub
provenance) are stored as Parquet rows, not loose JSON files. Each snapshot
preserves retrieval time; it is not a historical publication-vintage archive.

`update.py` defaults to a no-write/no-network dry run; `--apply` appends a new
Parquet snapshot from the public Hyperliquid API. It does not update GitHub
events. Existing snapshots are never changed. No scheduled job is installed.
Research code, notes, tests, charts and backtest outputs live in the quant
repository, not this dataset directory.


## `tardis/hyperliquid/samples/`

Added 2026-10-09 UTC. Verified free 2026-10-01 samples for `io:OAI`,
`io:ANTH`, and `xyz:OURA`: nine Parquet tables under
`samples/2026-10-01/{trades,quotes,derivative_ticker}/<DATASET-SYMBOL>.parquet`,
291,393 rows, plus `manifest.parquet` with source URLs, retrieval clocks,
source/output hashes and row counts. Source gzip CSVs are not retained.

Original vendor columns remain; `time` and `received_at` are UTC conversions
of exchange and local-receipt microseconds, respectively. `ticker` preserves
the deployer-prefixed Hyperliquid ID. Files partition by receipt date.
`derivative_ticker` funding fields are rate observations, not settlements.
These samples are only one day and do not establish continuous full-history
coverage. `update_samples.py` is data-only, defaults to dry-run, and downloads
only the fixed free sample allowlist with `--apply`; it refuses overwrite.
All liquidity analysis and research outputs remain in the quant repository.


## Archived Columbus3 oracle snapshot

Moved out of the active Kalshi store on 2026-10-19 to
`_archive_backups/2026-10-19-kalshi-cleanup/btc_oracle_columbus3/20261010_offline_snapshot/`.
The local scheduled Columbus3 pull was removed. Kalshi updates use the public API
for trades/metadata and Predexon for books.

Added 2026-10-10 UTC. Immutable preservation of **30 completed Columbus3
btc-oracle sessions, 132.99 recorded hours**, selected from existing local
recordings; no collector or download pipeline was run. Session span:
**2026-10-04 07:06:55.303245 → 2026-10-09 23:39:15.971796 UTC**. This is a
selection of sessions, not uninterrupted coverage. Total uncompressed tar size
is 14,070,927,360 bytes (13,953,361,794 original source bytes).

Each `<session_id>.tar` preserves original `tape/quotes_*.parquet`,
`tape/trades_*.parquet`, `tape/liqs_*.parquet`, `summary.json`, and `summary.md`,
with member paths relative to the session root. `manifest.json` records the
selection, actual session bounds derived from summary end time and elapsed
duration, tar byte counts and SHA-256 hashes. `<session_id>.manifest.json`
additionally lists every source member's size and modification time. Existing
archives are never overwritten; the research archiver verifies size and hash
on reuse. Research code and derived outputs remain in the quant repository.

Quotes retain `recv_ns`, `exch_ms`, `venue`, bid/ask prices and sizes, scalar
`obi`, and `seq`; trades/liquidations retain receipt/exchange clocks, venue,
price, size and side. `recv_ns` is application receipt time, **not hardware
arrival time**. Original records include auxiliary assets/ETFs: select explicit
venue identities before constructing a BTC panel. Kalshi perp prices are
dollars per 0.0001-BTC contract (multiply by 10,000 for USD/BTC); recorded sizes
are already BTC. Other venue conventions require their source adapters.

Limitations: BBO plus scalar depth imbalance cannot recover full L2 queues;
unchanged Kalshi tops are suppressed, so quote silence or skipped recorded
sequence values alone do not prove a feed outage. Original Coinbase trade
sides are inverted in sessions **before `20261009_092407Z`**; apply the correction
at research read time, preserving these originals. Some OKX liquidation
messages arrive seconds to hours after their exchange timestamps; use receipt
time and explicit stale-event checks. Recorded touch liquidity does not prove
executable fills, queue priority, or delivery/order latency.
