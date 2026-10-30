"""Download observed / source-modelled daily and intraday physical-world series.

Run ``python3 sources_environment.py --provider nasa`` or omit --provider for all.
No interpolation, synthetic zeros, or cadence upsampling is performed. Native
NDBC timestamps and GFZ three-hour blocks are retained. --refresh bypasses the
raw-response cache. All outputs pass expansion_common's daily-history gate.
"""
from __future__ import annotations

import argparse
import calendar
from concurrent.futures import ThreadPoolExecutor, as_completed
import gzip
import html
import io
import json
from urllib.parse import urlencode
import xml.etree.ElementTree as ET

import numpy as np
import pandas as pd
import requests

from expansion_common import Fetcher, ROOT, atomic_bytes, save_candidate


START = "2024-01-01"
NASA_URL = "https://power.larc.nasa.gov/api/temporal/daily/point"
NASA_DOC = "https://power.larc.nasa.gov/docs/services/api/temporal/daily/"
NDBC_DOC = "https://www.ndbc.noaa.gov/faq/measdes.shtml"
USGS_URL = "https://waterservices.usgs.gov/nwis/dv/"
GFZ_URL = "https://kp.gfz.de/app/files/Kp_ap_Ap_SN_F107_since_1932.txt"
QLD_URL = ("https://data.longpaddock.qld.gov.au/SeasonalClimateOutlook/"
           "SouthernOscillationIndex/SOIDataFiles/DailySOI1933-1992Base.txt")

# Each point is an economically motivated local proxy, not a regional aggregate.
# Different parameter sets target different mechanisms (crop moisture, heat,
# frost, renewable output, or mine/port storm disruption).
NASA_POINTS = [
    ("iowa_corn", 42.0, -93.5, "Iowa, US", "corn and ethanol feedstock", "T2M_MAX,PRECTOTCORR,GWETROOT,RH2M"),
    ("kansas_wheat", 38.5, -98.0, "Kansas, US", "winter wheat yield and drought", "T2M_MAX,T2M_MIN,PRECTOTCORR,GWETROOT"),
    ("saskatchewan_canola", 52.1, -106.7, "Saskatchewan, Canada", "canola frost and soil moisture", "T2M_MIN,T2M_MAX,PRECTOTCORR,GWETROOT"),
    ("mato_grosso_soy", -12.5, -56.1, "Mato Grosso, Brazil", "soybean sowing and harvest conditions", "T2M_MAX,PRECTOTCORR,GWETROOT,RH2M"),
    ("cordoba_soy", -32.0, -63.5, "Cordoba, Argentina", "soybean and maize export supply", "T2M_MAX,PRECTOTCORR,GWETROOT,RH2M"),
    ("punjab_rice", 30.9, 75.9, "Punjab, India", "rice irrigation and heat stress", "T2M_MAX,PRECTOTCORR,GWETROOT,RH2M"),
    ("hubei_rice", 30.6, 114.3, "Hubei, China", "rice crop heat and flood exposure", "T2M_MAX,PRECTOTCORR,GWETROOT,RH2M"),
    ("wa_wheat", -31.5, 117.0, "Western Australia, Australia", "export wheat drought and frost exposure", "T2M_MIN,T2M_MAX,PRECTOTCORR,GWETROOT"),
    ("thai_rubber", 7.0, 100.5, "Songkhla, Thailand", "rubber tapping interruption and supply", "PRECTOTCORR,RH2M,T2M_MAX,ALLSKY_SFC_SW_DWN"),
    ("dak_lak_coffee", 12.7, 108.1, "Dak Lak, Vietnam", "robusta flowering, harvest and irrigation", "PRECTOTCORR,GWETROOT,T2M_MAX,RH2M"),
    ("minas_coffee", -21.4, -45.9, "Minas Gerais, Brazil", "arabica frost and flowering", "T2M_MIN,T2M_MAX,PRECTOTCORR,GWETROOT"),
    ("ivory_coast_cocoa", 6.4, -5.3, "Cote d'Ivoire", "cocoa disease, drought and harvest drying", "PRECTOTCORR,RH2M,GWETROOT,ALLSKY_SFC_SW_DWN"),
    ("turkey_hazelnut", 40.9, 38.4, "Giresun, Turkey", "hazelnut frost, disease and yield", "T2M_MIN,PRECTOTCORR,RH2M,GWETROOT"),
    ("lombardy_rice", 45.2, 9.1, "Lombardy, Italy", "rice irrigation demand and drought", "PRECTOTCORR,GWETROOT,T2M_MAX,ALLSKY_SFC_SW_DWN"),
    ("xinjiang_cotton", 41.7, 86.2, "Xinjiang, China", "cotton heat and irrigation stress", "T2M_MAX,T2M_MIN,PRECTOTCORR,GWETROOT"),
    ("andalusia_olives", 37.8, -3.8, "Andalusia, Spain", "olive oil drought and flowering", "T2M_MAX,PRECTOTCORR,GWETROOT,RH2M"),
    ("california_almonds", 36.7, -119.8, "Central Valley, California, US", "almond frost and irrigation demand", "T2M_MIN,T2M_MAX,PRECTOTCORR,GWETROOT"),
    ("atacama_copper", -22.3, -68.9, "Atacama, Chile", "copper mine weather and solar power supply", "PRECTOTCORR,WS10M,T2M_MAX,ALLSKY_SFC_SW_DWN"),
    ("pilbara_iron_ore", -22.3, 117.7, "Pilbara, Australia", "iron ore mine and rail storm disruption", "PRECTOTCORR,WS10M_MAX,T2M_MAX,RH2M"),
    ("zambia_copper", -12.8, 28.2, "Copperbelt, Zambia", "copper mining rain and hydropower exposure", "PRECTOTCORR,GWETROOT,T2M_MAX,ALLSKY_SFC_SW_DWN"),
    ("sulawesi_nickel", -2.0, 121.5, "Sulawesi, Indonesia", "nickel mining rain and logistics disruption", "PRECTOTCORR,WS10M_MAX,T2M_MAX,RH2M"),
    ("texas_wind", 32.5, -100.0, "West Texas, US", "wind supply and electricity heat demand", "WS50M,WS10M_MAX,T2M_MAX,ALLSKY_SFC_SW_DWN"),
    ("rajasthan_solar", 27.0, 71.5, "Rajasthan, India", "solar output, panel heat losses and dust transport", "ALLSKY_SFC_SW_DWN,T2M_MAX,WS10M,RH2M"),
    ("north_sea_wind", 56.0, 4.0, "North Sea, Europe", "offshore wind output and storm shutdown risk", "WS50M,WS10M_MAX,T2M,PS"),
    ("rhine_industry", 50.0, 8.0, "Rhine corridor, Germany", "industrial cooling demand and basin drought", "PRECTOTCORR,T2M_MAX,GWETROOT,RH2M"),
]

NDBC_STATIONS = [
    ("41004", "Charleston approach, US", "container shipping and coastal storm losses"),
    ("41002", "South Hatteras offshore, US", "Atlantic shipping weather and hurricane approach"),
    ("41008", "Savannah approach, US", "container port access and coastal logistics"),
    ("41009", "Cape Canaveral, US", "space launch weather and Atlantic port access"),
    ("41043", "Northeast Puerto Rico", "Caribbean shipping, tourism and hurricane exposure"),
    ("42001", "Central Gulf of Mexico", "offshore oil production and hurricane disruption"),
    ("42002", "Western Gulf of Mexico", "oil production and tanker routing"),
    ("42012", "Orange Beach, Gulf of Mexico", "coastal insurance and fishing disruption"),
    ("44009", "Delaware Bay approach, US", "crude imports, refining and Mid-Atlantic port logistics"),
    ("44007", "Portland, Maine approach, US", "New England energy deliveries and fishing logistics"),
    ("42035", "Galveston, Texas, US", "Houston ship-channel and refining disruption"),
    ("46001", "Gulf of Alaska", "fisheries and North Pacific shipping"),
    ("46050", "Newport offshore, Oregon, US", "Pacific Northwest shipping and fisheries"),
    ("46011", "Santa Maria offshore, US", "offshore operations and California storm exposure"),
    ("46025", "Santa Monica Basin, US", "Los Angeles/Long Beach container access"),
    ("44013", "Boston approach, US", "New England ports, LNG logistics and coastal storm exposure"),
    ("46042", "Monterey offshore, US", "California fisheries and marine heat conditions"),
    ("51001", "Northwest Hawaii", "Pacific aviation, tourism and transoceanic weather"),
]
NDBC_MEASURES = {
    "WSPD": ("wind_speed", "wind speed", "m/s", 99.0),
    "WVHT": ("significant_wave_height", "significant wave height", "m", 99.0),
    "WTMP": ("water_temperature", "sea water temperature", "degC", 999.0),
    "PRES": ("air_pressure", "sea level air pressure", "hPa", 9999.0),
    "ATMP": ("air_temperature", "air temperature", "degC", 999.0),
    "DEWP": ("dew_point", "dew point temperature", "degC", 999.0),
    "GST": ("wind_gust", "peak wind gust", "m/s", 99.0),
}
NDBC_SELECTED_MEASURES = {
    # Water-temperature sensors at these stations stopped reporting; retain
    # genuinely observed atmospheric temperature rather than stale values.
    "41008": ("WSPD", "WVHT", "ATMP", "PRES"),
    "41009": ("WSPD", "WVHT", "ATMP", "PRES"),
    "46050": ("WSPD", "WVHT", "GST", "PRES"),
    "44009": ("WSPD", "DEWP", "WTMP", "PRES"),
}

# Flow is requested at every listed river; available water temperature,
# conductance and oxygen are separate physical measurements, not transforms.
USGS_SITES = {
    "07289000": "Mississippi barge depth and grain export logistics",
    "07010000": "upper Mississippi navigation and industrial water supply",
    "07374000": "lower Mississippi petrochemical and export logistics",
    "06934500": "Missouri grain-belt moisture and barge conditions",
    "05587450": "Mississippi-Illinois confluence agricultural barge network",
    "03303280": "Ohio River industrial and power-plant water supply",
    "03086000": "Ohio River steel and industrial cooling corridor",
    "01646500": "Mid-Atlantic drought, water utilities and cooling",
    "01463500": "Delaware industrial water and downstream navigation",
    "01578310": "Susquehanna hydropower and Chesapeake runoff",
    "02089500": "North Carolina agriculture and flood exposure",
    "02177000": "upper Savannah basin runoff and southeastern power/water supply",
    "02231000": "Florida freshwater and flood exposure",
    "02469761": "Mobile basin industrial and forest-products logistics",
    "08067000": "Texas refinery, chemical and freshwater exposure",
    "08158000": "Texas water supply and drought exposure",
    "08313000": "Rio Grande irrigation water availability",
    "09380000": "Colorado River hydroelectric and irrigation supply",
    "15515500": "Alaska interior barge access, fuel deliveries and placer mining water",
    "09498500": "Arizona water supply and semiconductor water exposure",
    "10346000": "Truckee water supply and western drought",
    "11377100": "Sacramento irrigation supply and California drought",
    "14105700": "Columbia hydroelectric power and navigation",
    "14211720": "Willamette industrial water, timber and flood risk",
    "14246900": "Columbia hydropower, shipping and fishery conditions",
    "12353000": "Montana hydroelectric inflows and snowmelt",
}
USGS_PARAMETERS = {
    "00060": ("discharge", "hydrology"),
    "00010": ("water_temperature", "water_quality"),
    "00095": ("specific_conductance", "water_quality"),
    "00300": ("dissolved_oxygen", "water_quality"),
}
# Fixed dataset identities: 26 river-flow series and 14 measured water-quality
# series. Missing history is an error, never an opportunity to silently change
# an indicator's identity on refresh.
USGS_QUALITY_SELECTION = {
    "00010": {"08313000", "07374000", "14105700", "01646500"},
    "00095": {"01463500", "06934500", "07374000", "09380000", "14211720"},
    "00300": {"01463500", "01646500", "06934500", "07374000", "14211720"},
}


def frame_from_values(dates, values, *, min_value=None, sentinel=None):
    frame = pd.DataFrame({"date": pd.to_datetime(dates, utc=True), "value": pd.to_numeric(values)})
    valid = np.isfinite(frame.value)
    if sentinel is not None:
        valid &= frame.value != sentinel
    if min_value is not None:
        valid &= frame.value >= min_value
    return frame.loc[valid].copy()


def run_jobs(jobs, worker, workers=3):
    results, errors = [], []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        tasks = {pool.submit(worker, job): job for job in jobs}
        for task in as_completed(tasks):
            try:
                result = task.result()
                results.extend(result)
                print(f"OK {tasks[task][0]}: {len(result)} series", flush=True)
            except Exception as exc:
                error = {"item": str(tasks[task]), "error": str(exc)}
                errors.append(error)
                print(f"FAILED {error}", flush=True)
    return results, errors


def nasa(fetcher, start=START):
    end = (pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=7)).strftime("%Y%m%d")

    def point(item):
        slug, lat, lon, location, mechanism, parameters = item
        params = dict(parameters=parameters, community="AG", latitude=lat, longitude=lon,
                      start=start.replace("-", ""), end=end, format="JSON", **{"time-standard": "UTC"})
        content, digest = fetcher.get(NASA_URL, params, timeout=180)
        payload = json.loads(content)
        if "properties" not in payload:
            raise ValueError(payload)
        metadata = payload["parameters"]
        missing = payload.get("header", {}).get("fill_value", -999)
        result = []
        for parameter in parameters.split(","):
            values = payload["properties"]["parameter"][parameter]
            frame = frame_from_values(pd.to_datetime(list(values), format="%Y%m%d", utc=True),
                                      list(values.values()), sentinel=missing)
            descriptor = metadata[parameter]
            result.append(save_candidate(
                "environment", f"nasa_power_{slug}_{parameter.lower()}", frame,
                title=f"{location}: {descriptor['longname']}", provider="nasa_power",
                category="energy_weather" if slug in {"texas_wind", "rajasthan_solar", "north_sea_wind"}
                    else "industrial_weather" if slug in {"atacama_copper", "pilbara_iron_ore",
                    "zambia_copper", "sulawesi_nickel", "rhine_industry"} else "agriculture_weather",
                unit=descriptor["units"], frequency="daily",
                source_url=NASA_URL + "?" + urlencode(params), geography=location,
                hypothesis=f"Research hypothesis: local weather may affect {mechanism}; test seasonality, nonlinear thresholds and economic release lags.",
                limitations="NASA POWER source-modelled/reanalysis or satellite-derived grid-cell estimate, not a farm/mine observation or region-weighted production index. Revised history is not point-in-time. Solar radiation publication can lag meteorological fields by months; inspect each end date. Missing source values omitted; no interpolation.",
                raw_sha256=digest, dataset_identity=f"NASA POWER daily point {lat},{lon} {parameter}",
                latitude=lat, longitude=lon, documentation_url=NASA_DOC,
                measurement_type="source_modelled_grid_cell", native_frequency="daily",
                timestamp_semantics="UTC calendar day label, not publication time",
                source_parameters=descriptor, license="NASA open data; acknowledge NASA POWER",
            ))
        return result
    return run_jobs(NASA_POINTS, point, workers=3)


def parse_ndbc(content):
    """Parse native NDBC records; sentinels handled by measurement, not globally."""
    if content[:2] == b"\x1f\x8b":
        content = gzip.decompress(content)
    lines = content.decode("utf-8").splitlines()
    if not lines or not lines[0].startswith("#YY"):
        raise ValueError("NDBC schema changed")
    columns = lines[0].lstrip("#").split()
    frame = pd.read_csv(io.StringIO("\n".join(lines[2:])), sep=r"\s+", names=columns,
                        na_values=["MM"], dtype=str)
    time_columns = {"YY": "year", "MM": "month", "DD": "day", "hh": "hour", "mm": "minute"}
    dates = pd.to_datetime(frame[list(time_columns)].rename(columns=time_columns), utc=True)
    out = pd.DataFrame({"date": dates})
    for column in NDBC_MEASURES:
        out[column] = pd.to_numeric(frame[column], errors="raise")
    if out.date.duplicated().any():
        # Exact duplicates in one source are harmless; conflicting ones are not.
        if out.groupby("date")[list(NDBC_MEASURES)].nunique().gt(1).any().any():
            raise ValueError("Conflicting duplicate timestamps within NDBC file")
        out = out.drop_duplicates("date")
    return out


def ndbc(fetcher, start=START):
    now = pd.Timestamp.now(tz="UTC")
    station_content, station_digest = fetcher.get("https://www.ndbc.noaa.gov/activestations.xml")
    station_metadata = {node.attrib["id"]: node.attrib
                        for node in ET.fromstring(station_content).findall("station")}

    def station(item):
        station_id, location, mechanism = item
        frames, hashes, missing_urls = [], [], []
        source_station = station_metadata.get(station_id, {})
        # Archives are post-processed; current monthly and realtime data are provisional.
        urls = [f"https://www.ndbc.noaa.gov/data/historical/stdmet/{station_id}h{year}.txt.gz"
                for year in range(pd.Timestamp(start).year, now.year)]
        for month in range(1, now.month):
            code = str(month) if month < 10 else {10: "a", 11: "b", 12: "c"}[month]
            urls.append(f"https://www.ndbc.noaa.gov/data/stdmet/{calendar.month_abbr[month]}/{station_id}{code}{now.year}.txt.gz")
        urls.append(f"https://www.ndbc.noaa.gov/data/realtime2/{station_id}.txt")
        for url in urls:
            try:
                content, digest = fetcher.get(url)
            except requests.HTTPError as exc:
                if exc.response.status_code == 404:
                    missing_urls.append(url)
                    continue
                raise
            frame = parse_ndbc(content)
            # Do not mistake rotating monthly files from another year for current data.
            if "/data/stdmet/" in url:
                frame = frame.loc[frame.date.dt.year == now.year]
            if not frame.empty:
                hashes.append(digest)
                frames.append(frame)
        if not frames:
            raise ValueError("No NDBC history")
        # Earlier files have higher priority: annual archived > monthly > realtime.
        joined = pd.concat(frames, ignore_index=True)
        result = []
        selected_measures = NDBC_SELECTED_MEASURES.get(station_id, ("WSPD", "WVHT", "WTMP", "PRES"))
        for column in selected_measures:
            slug, title, unit, sentinel = NDBC_MEASURES[column]
            frame = joined[["date", column]].rename(columns={column: "value"})
            frame = frame.loc[frame.value.notna() & (frame.value != sentinel)]
            frame = frame.loc[frame.date >= pd.Timestamp(start, tz="UTC")]
            frame = frame.drop_duplicates("date", keep="first").sort_values("date")
            cadence = frame.date.diff().dt.total_seconds().median()
            frequency = "10min" if cadence <= 600 else "30min" if cadence <= 1800 else "hourly"
            result.append(save_candidate(
                "environment", f"ndbc_{station_id}_{slug}", frame, title=f"{location}: {title}",
                provider="noaa_ndbc", category="marine_conditions", unit=unit, frequency=frequency,
                source_url=f"https://www.ndbc.noaa.gov/station_history.php?station={station_id}",
                geography=location, hypothesis=f"Research hypothesis: {title} may affect {mechanism}; examine disruption thresholds rather than assuming a linear return relationship.",
                limitations="Point buoy observation; coverage and sensor availability vary. Historical records are post-processed; current months and realtime values can be revised. Higher-quality annual archives take precedence at overlapping timestamps. Measurement-specific missing sentinels omitted. No gap filling.",
                raw_sha256=hashes + [station_digest], dataset_identity=f"NOAA NDBC station {station_id} standard meteorological {column}",
                native_frequency="native intraday observations retained; varies by sensor and deployment",
                timestamp_semantics="Native UTC observation timestamp; not publication time",
                documentation_url=NDBC_DOC, unavailable_source_files=missing_urls,
                source_urls=urls, source_station_metadata=source_station,
                license="US government data; acknowledge NOAA NDBC",
            ))
        return result
    return run_jobs(NDBC_STATIONS, station, workers=3)


def parse_usgs(payload):
    records = []
    for series in payload["value"]["timeSeries"]:
        site = series["sourceInfo"]["siteCode"][0]["value"]
        variable = series["variable"]
        code = variable["variableCode"][0]["value"]
        rows, qualifiers = [], set()
        for block in series["values"]:
            for observation in block["value"]:
                value = float(observation["value"])
                if value == float(variable["noDataValue"]):
                    continue
                # DV source is a local calendar day, not an instantaneous UTC reading.
                rows.append({"date": observation["dateTime"][:10], "value": value})
                qualifiers.update(observation.get("qualifiers", []))
        if rows:
            records.append((site, code, pd.DataFrame(rows), series, sorted(qualifiers)))
    return records


def usgs(fetcher, start=START):
    params = dict(format="json", sites=",".join(USGS_SITES), startDT=start,
                  endDT=(pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=1)).strftime("%Y-%m-%d"),
                  parameterCd=",".join(USGS_PARAMETERS), statCd="00003", siteStatus="all")
    content, digest = fetcher.get(USGS_URL, params, timeout=180)
    candidates = parse_usgs(json.loads(content))
    result, errors = [], []
    for site, code, frame, series, qualifiers in candidates:
        if code != "00060" and site not in USGS_QUALITY_SELECTION[code]:
            continue
        slug, category = USGS_PARAMETERS[code]
        variable = series["variable"]
        location = series["sourceInfo"]["siteName"]
        try:
            result.append(save_candidate(
                "environment", f"usgs_{site}_{slug}", frame,
                title=f"{location}: daily mean {html.unescape(variable['variableName'])}",
                provider="usgs_water", category=category, unit=variable["unit"]["unitCode"],
                frequency="daily", source_url=USGS_URL + "?" + urlencode(params),
                geography=location, hypothesis=f"Research hypothesis: {slug.replace('_', ' ')} may affect {USGS_SITES[site]}; calibrate relevance to actual assets and operating thresholds.",
                limitations="Daily mean, not an instantaneous measurement. Recent data are provisional and historical data can be revised. Source quality qualifiers are retained in metadata/raw responses. Local calendar-day label stored at UTC midnight; no interpolation.",
                raw_sha256=digest, dataset_identity=f"USGS {site} parameter {code} statistic 00003",
                timestamp_semantics="Source local calendar day represented at UTC midnight; not availability time",
                native_frequency="daily mean; finer observations may exist in USGS instantaneous-values service",
                source_qualifiers=qualifiers,
                documentation_url="https://waterservices.usgs.gov/docs/dv-service/daily-values-service-details/",
                license="USGS public-domain data; acknowledge USGS",
            ))
        except ValueError as exc:
            errors.append({"item": f"{site}/{code}", "error": str(exc)})
    expected = {(site, "00060") for site in USGS_SITES}
    expected |= {(site, code) for code, sites in USGS_QUALITY_SELECTION.items() for site in sites}
    found = {(site, code) for site, code, *_ in candidates}
    for site, code in sorted(expected - found):
        errors.append({"item": f"{site}/{code}", "error": "Expected source series missing"})
    print(f"OK USGS: {len(result)} series", flush=True)
    return result, errors


def gfz(fetcher, start=START):
    content, digest = fetcher.get(GFZ_URL)
    table = pd.read_csv(io.BytesIO(content), sep=r"\s+", comment="#", header=None)
    if len(table.columns) != 28:
        raise ValueError("GFZ column schema changed")
    dates = pd.to_datetime(table[[0, 1, 2]].rename(columns={0: "year", 1: "month", 2: "day"}), utc=True)
    keep = dates >= pd.Timestamp(start, tz="UTC")
    dates, table = dates.loc[keep], table.loc[keep]
    kp = pd.concat([
        frame_from_values(dates + pd.Timedelta(hours=3 * block), table[7 + block], min_value=0)
        for block in range(8)
    ], ignore_index=True)
    radio = frame_from_values(dates, table[25], min_value=0)
    results = []
    for sid, title, frame, unit, frequency, explanation in [
        ("gfz_planetary_kp", "Planetary geomagnetic Kp index", kp, "Kp index", "3-hourly",
         "Grid geomagnetic disturbance, GNSS degradation, polar aviation and satellite operating risk"),
        ("gfz_f107_observed", "Observed 10.7 cm solar radio flux", radio, "solar flux units (10^-22 W m^-2 Hz^-1)", "daily",
         "Ionospheric/thermospheric state, satellite drag and radio-communication operating risk"),
    ]:
        results.append(save_candidate(
            "environment", sid, frame, title=title, provider="gfz_space_weather",
            category="space_weather", unit=unit, frequency=frequency, source_url=GFZ_URL,
            geography="Global / Sun-Earth system", hypothesis=f"Research hypothesis: {explanation}; economic effects are event-dependent and not a demonstrated alpha signal.",
            limitations="Latest Kp values are nowcasts later replaced by definitive values. F10.7 is the observed local-noon radio flux provided by DRAO/Natural Resources Canada, distributed by GFZ. No forecasts used. Sunspot-number column is deliberately not redistributed.",
            raw_sha256=digest, dataset_identity=sid, native_frequency=frequency,
            timestamp_semantics="Kp: UTC start of 3-hour interval; F10.7: source UT day label, not release time",
            documentation_url="https://doi.org/10.5880/Kp.0001", license="CC BY 4.0 per GFZ file header, excluding unused sunspot column",
            citation="Matzka et al. (2021), https://doi.org/10.1029/2020SW002641",
        ))
    return results, []


def nsidc(fetcher, start=START):
    results = []
    for hemisphere, letter in [("north", "N"), ("south", "S")]:
        url = f"https://noaadata.apps.nsidc.org/NOAA/G02135/{hemisphere}/daily/data/{letter}_seaice_extent_daily_v4.0.csv"
        content, digest = fetcher.get(url)
        table = pd.read_csv(io.BytesIO(content), skiprows=[1])
        table.columns = table.columns.str.strip()
        dates = pd.to_datetime(table[["Year", "Month", "Day"]].rename(
            columns={"Year": "year", "Month": "month", "Day": "day"}), utc=True)
        frame = frame_from_values(dates, table.Extent, min_value=0)
        frame = frame.loc[frame.date >= pd.Timestamp(start, tz="UTC")]
        results.append(save_candidate(
            "environment", f"nsidc_{hemisphere}_sea_ice_extent", frame,
            title=f"{hemisphere.title()}ern hemisphere daily sea ice extent",
            provider="nsidc_sea_ice", category="cryosphere", unit="million square km",
            frequency="daily", source_url=url, geography=f"{hemisphere.title()}ern hemisphere",
            hypothesis="Research hypothesis: seasonal sea-ice access can affect polar shipping, offshore access, fisheries and climate-sensitive energy demand; daily noise alone is unlikely to be actionable.",
            limitations="Satellite-derived area above an ice-concentration threshold, not ice volume or navigability. Near-real-time inputs are revised. Weather and algorithm effects matter. No interpolation or rolling-average substitution.",
            raw_sha256=digest, dataset_identity=f"NSIDC G02135 V4 {hemisphere} daily extent",
            native_frequency="daily", timestamp_semantics="Source calendar day, not publication time",
            documentation_url="https://nsidc.org/data/g02135", license="NSIDC open data; cite Sea Ice Index Version 4",
        ))
    return results, []


def co2(fetcher, start=START):
    url = "https://gml.noaa.gov/webdata/ccgg/trends/co2/co2_daily_mlo.txt"
    content, digest = fetcher.get(url)
    table = pd.read_csv(io.BytesIO(content), sep=r"\s+", comment="#", header=None)
    if len(table.columns) != 5:
        raise ValueError("NOAA CO2 schema changed")
    dates = pd.to_datetime(table[[0, 1, 2]].rename(columns={0: "year", 1: "month", 2: "day"}), utc=True)
    frame = frame_from_values(dates, table[4], min_value=0)
    frame = frame.loc[frame.date >= pd.Timestamp(start, tz="UTC")]
    meta = save_candidate(
        "environment", "noaa_mauna_loa_daily_co2", frame, title="Mauna Loa atmospheric CO2 daily mean",
        provider="noaa_gml", category="atmospheric_composition", unit="ppm", frequency="daily",
        source_url=url, geography="Mauna Loa, Hawaii; global background atmosphere proxy",
        hypothesis="Research hypothesis: atmospheric carbon conditions may contextualize carbon-market and climate-policy event studies; this slow physical background is not a direct emissions or daily carbon-price signal.",
        limitations="Background atmospheric concentration, not industrial emissions. Source omits days failing background-air selection. Latest months are preliminary. Daily seasonal and meteorological variation dominates short horizons. 2022-2023 temporary site substitution is documented in raw data.",
        raw_sha256=digest, dataset_identity="NOAA GML trends CO2 daily Mauna Loa",
        native_frequency="daily selected-air mean", timestamp_semantics="Source day label; not release time",
        documentation_url="https://gml.noaa.gov/ccgg/trends/", license="NOAA public data; acknowledge NOAA GML and source investigators",
    )
    return [meta], []


def qld(fetcher, start=START):
    content, digest = fetcher.get(QLD_URL)
    table = pd.read_csv(io.BytesIO(content), sep=r"\s+")
    if list(table.columns) != ["Year", "Day", "Tahiti", "Darwin", "SOI"]:
        raise ValueError("Queensland SOI schema changed")
    dates = pd.to_datetime(table.Year.astype(str), format="%Y", utc=True) + pd.to_timedelta(table.Day - 1, unit="D")
    results = []
    for column, unit, location in [("SOI", "Troup SOI index", "Tahiti-Darwin pressure contrast"),
                                   ("Tahiti", "hPa", "Tahiti"), ("Darwin", "hPa", "Darwin, Australia")]:
        frame = frame_from_values(dates, table[column], sentinel=-999.0)
        frame = frame.loc[frame.date >= pd.Timestamp(start, tz="UTC")]
        results.append(save_candidate(
            "environment", f"qld_daily_{column.lower()}", frame,
            title=f"Queensland daily {column}" if column == "SOI" else f"{location} daily mean sea level pressure",
            provider="queensland_long_paddock", category="climate_drivers", unit=unit,
            frequency="daily", source_url=QLD_URL, geography=location,
            hypothesis="Research hypothesis: sustained tropical pressure patterns can inform Australian agricultural, hydroelectric and ENSO-sensitive commodity supply expectations.",
            limitations="Daily pressure noise is not a reliable standalone ENSO classification. SOI uses the 1933-1992 climate base. Third-party pressure inputs can be revised and source updates can be interrupted. No monthly data expanded into daily observations.",
            raw_sha256=digest, dataset_identity=f"Long Paddock daily SOI 1933-1992 base field {column}",
            native_frequency="daily", timestamp_semantics="Source year/day-of-year label; not availability time",
            documentation_url="https://www.longpaddock.qld.gov.au/soi/about/",
            license="Queensland Government copyright; source website attribution and reuse terms apply",
        ))
    return results, []


PROVIDERS = {"nasa": nasa, "ndbc": ndbc, "usgs": usgs, "gfz": gfz,
             "nsidc": nsidc, "co2": co2, "qld": qld}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--provider", choices=PROVIDERS)
    parser.add_argument("--refresh", action="store_true")
    parser.add_argument("--start", default=START)
    args = parser.parse_args()
    fetcher = Fetcher(refresh=args.refresh)
    results, errors = [], []
    for name, collect in PROVIDERS.items():
        if args.provider and name != args.provider:
            continue
        try:
            collected, failed = collect(fetcher, start=args.start)
            results.extend(collected)
            errors.extend([dict(provider=name, **e) for e in failed])
        except Exception as exc:
            errors.append({"provider": name, "error": str(exc)})
            print(f"FAILED {name}: {exc}", flush=True)
    report = {"series_count": len(results), "provider_count": len({x["provider"] for x in results}),
              "series_ids": [x["series_id"] for x in results], "errors": errors}
    report_path = ROOT / "archive/staging/environment" / f"_report_{args.provider or 'all'}.json"
    atomic_bytes(report_path, json.dumps(report, indent=2).encode())
    print(json.dumps({"series_count": len(results), "provider_count": report["provider_count"], "errors": errors}, indent=2))
    if errors or not results:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
