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
| Splits / dividends / tickers | `us_stocks_sip/{splits,dividends,tickers}.parquet` | table | — |
| Corporate events | `us_stocks_sip/corporate_events.parquet` | event | 1.01 M rows |
| Earnings calendar | `benzinga/earnings.parquet` | ticker × event | 2010-04-30 → 2027 |
| ETF flows / taxonomy / holdings | `etf_global/…` | ETF × day | 2017-04-03 → 2026-08 |
| FINRA short volume | `finra_short_volume/…` | ticker × day | 2024 → 2026 |
| Press releases | `us_stocks_sip/press/<TICKER>.parquet` | article | 73 tickers, → 2026-03 |

---

## Conventions

### File naming

- `<date>.parquet` — one file per session, all symbols inside (aggregate datasets).
- `<date>/<date>-<SYMBOL>.parquet` — tick datasets: a directory per session, one file per symbol.
  The date is repeated in the filename on purpose so files remain identifiable when copied.
- `<TICKER>.parquet` — "by_ticker" pivots: whole history of one symbol in one file.
- A day directory only exists once that day finished converting, so **presence == complete**.
  A directory ending in `.tmp` is a conversion in flight — ignore it.

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
| `update_daily.sh` | the nightly driver that chains the above |

`vis/` holds option-chain plotting helpers; `fix/` holds one-off repair scripts; `p` is a
throwaway loop script. Credentials live in `polygon.json`, `kalshi.{json,key}`, `finnhub.json`,
`alpaca.json` — never read or echo these.
