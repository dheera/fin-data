"""Observed physical-economy expansion; explicit raw provenance, no imputation.

CLI: python3 sources_physical2.py --provider ghcn|dwd|hydat|coops|epa [--refresh]
The source selection is pinned in logs/physical2_<provider>_selection.json.
Refresh preserves those station identities and updates their genuine observations.
"""
from __future__ import annotations

import argparse
import calendar
from concurrent.futures import ThreadPoolExecutor, as_completed
import io
import json
import re
from urllib.parse import urlencode
import zipfile

import numpy as np
import pandas as pd

from expansion_common import (
    Fetcher, ROOT, atomic_bytes, coverage, normalize_frame, qualifies_daily, save_candidate,
)

GROUP = "physical2"
START = "2020-01-01"
GHCN_BASE = "https://www.ncei.noaa.gov/pub/data/ghcn/daily/"
DWD_BASE = "https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/daily/kl/"
HYDAT_BASE = "https://api.weather.gc.ca/collections/"
COOPS_API = "https://api.tidesandcurrents.noaa.gov/api/prod/datagetter"

# Site selection concerns physical exposures, not historical asset returns.
# Coordinates are targets; the catalog records the actual observed station and
# its distance, which can be substantial in sparsely observed regions.
TARGETS = [
    ("Punjab wheat", 30.9, 75.9, "India", "wheat heat stress and irrigation", "agriculture_weather"),
    ("Haryana rice", 29.7, 76.9, "India", "rice heat and irrigation", "agriculture_weather"),
    ("Gujarat cotton", 22.3, 70.8, "India", "cotton supply and textile input costs", "agriculture_weather"),
    ("Assam tea", 27.5, 95.0, "India", "tea harvest and flood disruption", "agriculture_weather"),
    ("Kerala rubber", 9.6, 76.5, "India", "rubber tapping interruptions", "agriculture_weather"),
    ("Maharashtra sugar", 16.7, 74.2, "India", "sugar cane yields and irrigation", "agriculture_weather"),
    ("Bangladesh apparel", 23.8, 90.4, "Bangladesh", "apparel labor heat and flooding", "industrial_weather"),
    ("Pakistan cotton", 30.2, 71.5, "Pakistan", "cotton crop stress and flooding", "agriculture_weather"),
    ("Sri Lanka tea", 6.9, 80.8, "Sri Lanka", "tea production and drying", "agriculture_weather"),
    ("Nepal hydropower", 27.7, 85.3, "Nepal", "hydropower inflows and flood exposure", "energy_weather"),
    ("Vietnam robusta", 12.7, 108.0, "Vietnam", "coffee flowering and harvest", "agriculture_weather"),
    ("Vietnam rice", 10.0, 105.8, "Vietnam", "rice production and flooding", "agriculture_weather"),
    ("Thailand rubber", 7.2, 100.6, "Thailand", "rubber tapping interruption", "agriculture_weather"),
    ("Thailand rice", 14.9, 102.1, "Thailand", "rice drought and heat", "agriculture_weather"),
    ("Malaysia palm", 5.8, 118.1, "Malaysia", "palm oil harvesting and flooding", "agriculture_weather"),
    ("Sumatra palm", 3.6, 98.7, "Indonesia", "palm oil production and logistics", "agriculture_weather"),
    ("Java rice", -7.6, 110.8, "Indonesia", "rice yield and flood exposure", "agriculture_weather"),
    ("Sulawesi nickel", -3.9, 122.5, "Indonesia", "nickel ore mining and transport", "industrial_weather"),
    ("Philippines coconut", 7.1, 125.6, "Philippines", "coconut crop and port weather", "agriculture_weather"),
    ("Taiwan semiconductors", 24.8, 121.0, "Taiwan", "semiconductor cooling and water supply", "industrial_weather"),
    ("Guangdong manufacturing", 23.1, 113.3, "China", "factory heat and export logistics", "industrial_weather"),
    ("Henan wheat", 34.7, 113.7, "China", "winter wheat harvest rain and heat", "agriculture_weather"),
    ("Heilongjiang soy", 45.8, 126.6, "China", "soybean frost and crop conditions", "agriculture_weather"),
    ("Xinjiang cotton", 43.8, 87.6, "China", "cotton heat and irrigation demand", "agriculture_weather"),
    ("Yunnan hydropower", 25.0, 102.7, "China", "hydropower and aluminum energy supply", "energy_weather"),
    ("Shanghai port", 31.2, 121.5, "China", "container shipping and storm delays", "port_weather"),
    ("Qingdao port", 36.1, 120.4, "China", "iron ore and grain import logistics", "port_weather"),
    ("Busan port", 35.2, 129.1, "South Korea", "container shipping and shipbuilding", "port_weather"),
    ("Ulsan industry", 35.5, 129.3, "South Korea", "petrochemical and auto production weather", "industrial_weather"),
    ("Tokyo energy", 35.7, 139.8, "Japan", "power and LNG demand", "energy_weather"),
    ("Osaka industry", 34.7, 135.5, "Japan", "industrial power and logistics", "industrial_weather"),
    ("Hokkaido dairy", 43.1, 141.3, "Japan", "dairy heat and winter transport", "agriculture_weather"),
    ("Perth wheat", -31.9, 116.0, "Australia", "wheat harvest and export supply", "agriculture_weather"),
    ("Pilbara iron ore", -20.3, 118.6, "Australia", "iron ore port loading and cyclone exposure", "industrial_weather"),
    ("Queensland coal", -21.2, 149.2, "Australia", "coal mining rail and port disruption", "industrial_weather"),
    ("Darwin LNG", -12.4, 130.9, "Australia", "LNG loading and tropical storms", "port_weather"),
    ("Riverina irrigation", -35.1, 147.4, "Australia", "rice and cotton irrigation demand", "agriculture_weather"),
    ("Adelaide wine", -34.9, 138.6, "Australia", "wine grapes and crop heat stress", "agriculture_weather"),
    ("Auckland shipping", -36.9, 174.8, "New Zealand", "container logistics and dairy exports", "port_weather"),
    ("Canterbury dairy", -43.5, 172.6, "New Zealand", "dairy pasture and irrigation", "agriculture_weather"),
    ("Mato Grosso soy", -15.6, -56.1, "Brazil", "soybean sowing and harvest", "agriculture_weather"),
    ("Parana grain", -25.4, -49.3, "Brazil", "soy and maize frost and exports", "agriculture_weather"),
    ("Minas coffee", -19.9, -43.9, "Brazil", "arabica weather and frost", "agriculture_weather"),
    ("Sao Paulo sugar", -21.2, -47.8, "Brazil", "sugar cane harvest and ethanol feedstock", "agriculture_weather"),
    ("Santos port", -23.9, -46.3, "Brazil", "agricultural export loading", "port_weather"),
    ("Amazon river", -3.1, -60.0, "Brazil", "river logistics and drought", "port_weather"),
    ("Bahia cocoa", -14.8, -39.0, "Brazil", "cocoa yields and disease conditions", "agriculture_weather"),
    ("Cordoba soy", -31.4, -64.2, "Argentina", "soy and maize drought", "agriculture_weather"),
    ("Rosario grain", -32.9, -60.7, "Argentina", "grain export and crushing logistics", "port_weather"),
    ("Mendoza wine", -32.9, -68.8, "Argentina", "wine and irrigation demand", "agriculture_weather"),
    ("Uruguay livestock", -34.8, -56.2, "Uruguay", "pasture and livestock heat stress", "agriculture_weather"),
    ("Paraguay soy", -25.3, -57.6, "Paraguay", "soy yields and river exports", "agriculture_weather"),
    ("Atacama copper", -23.7, -70.4, "Chile", "copper export and mine weather", "industrial_weather"),
    ("Chilean fruit", -33.5, -70.7, "Chile", "fruit frost and irrigation demand", "agriculture_weather"),
    ("Peru fisheries", -12.0, -77.1, "Peru", "coastal fishery and port operations", "port_weather"),
    ("Ecuador bananas", -2.2, -79.9, "Ecuador", "banana disease and export logistics", "agriculture_weather"),
    ("Colombia coffee", 4.8, -75.7, "Colombia", "coffee flowering and harvest", "agriculture_weather"),
    ("Panama canal", 9.0, -79.5, "Panama", "canal water supply and transit capacity", "port_weather"),
    ("Costa Rica pineapple", 10.0, -84.2, "Costa Rica", "pineapple and coffee crop conditions", "agriculture_weather"),
    ("Guatemala coffee", 14.6, -90.5, "Guatemala", "coffee yields and volcanic-belt rain", "agriculture_weather"),
    ("Mexico avocado", 19.7, -101.2, "Mexico", "avocado heat and irrigation", "agriculture_weather"),
    ("Monterrey industry", 25.7, -100.3, "Mexico", "nearshoring industry water and heat", "industrial_weather"),
    ("Veracruz port", 19.2, -96.1, "Mexico", "Gulf trade and energy logistics", "port_weather"),
    ("Iowa corn", 41.6, -93.6, "United States", "corn yields and ethanol feedstock", "agriculture_weather"),
    ("Kansas wheat", 37.7, -97.3, "United States", "wheat drought and harvest", "agriculture_weather"),
    ("Dakota spring wheat", 46.9, -96.8, "United States", "spring wheat planting and frost", "agriculture_weather"),
    ("Fresno almonds", 36.8, -119.8, "United States", "almond frost and irrigation", "agriculture_weather"),
    ("Bakersfield oil", 35.4, -119.0, "United States", "oilfield heat and power demand", "industrial_weather"),
    ("Houston refining", 29.8, -95.4, "United States", "refinery flooding and power demand", "industrial_weather"),
    ("New Orleans exports", 30.0, -90.1, "United States", "grain and energy port disruption", "port_weather"),
    ("Seattle aviation", 47.6, -122.3, "United States", "aviation and port operations", "port_weather"),
    ("Saskatchewan canola", 52.1, -106.7, "Canada", "canola frost and soil moisture", "agriculture_weather"),
    ("Alberta energy", 53.6, -113.5, "Canada", "oil sands operations and heating demand", "energy_weather"),
    ("Winnipeg grain", 49.9, -97.1, "Canada", "wheat logistics and winter disruption", "agriculture_weather"),
    ("Vancouver timber", 49.3, -123.1, "Canada", "timber and Pacific port operations", "port_weather"),
    ("Abidjan cocoa", 5.3, -4.0, "Cote d'Ivoire", "cocoa shipping and regional harvest weather", "agriculture_weather"),
    ("Ghana cocoa", 6.7, -1.6, "Ghana", "cocoa disease and drying", "agriculture_weather"),
    ("Cameroon cocoa", 4.1, 9.7, "Cameroon", "cocoa production and export", "agriculture_weather"),
    ("Lagos oil", 6.6, 3.3, "Nigeria", "oil and consumer port disruption", "port_weather"),
    ("Senegal groundnut", 14.7, -17.5, "Senegal", "groundnut and food import logistics", "agriculture_weather"),
    ("Mali cotton", 12.6, -8.0, "Mali", "cotton rainfall and heat exposure", "agriculture_weather"),
    ("Casablanca phosphate", 33.6, -7.6, "Morocco", "phosphate exports and crop drought", "industrial_weather"),
    ("Tunis olives", 36.8, 10.2, "Tunisia", "olive oil crop water stress", "agriculture_weather"),
    ("Egypt wheat", 30.1, 31.2, "Egypt", "irrigation demand and wheat heat", "agriculture_weather"),
    ("Kenya tea", -0.3, 36.1, "Kenya", "tea yield and frost", "agriculture_weather"),
    ("Ethiopia coffee", 9.0, 38.8, "Ethiopia", "coffee growing conditions", "agriculture_weather"),
    ("Tanzania gold", -2.5, 32.9, "Tanzania", "gold mining runoff and logistics", "industrial_weather"),
    ("Zambia copper", -13.0, 28.6, "Zambia", "copper mining rain and hydropower", "industrial_weather"),
    ("Congo copper", -11.7, 27.5, "Democratic Republic of Congo", "copper and cobalt mine access", "industrial_weather"),
    ("Mozambique coal", -16.2, 33.6, "Mozambique", "coal rail flood exposure", "industrial_weather"),
    ("Durban port", -29.9, 31.0, "South Africa", "bulk and container exports", "port_weather"),
    ("Johannesburg platinum", -26.2, 28.0, "South Africa", "mining and power demand", "industrial_weather"),
    ("Cape Town fruit", -33.9, 18.4, "South Africa", "fruit and wine drought", "agriculture_weather"),
    ("Madagascar vanilla", -18.9, 47.5, "Madagascar", "vanilla supply and cyclone exposure", "agriculture_weather"),
    ("Mauritius sugar", -20.3, 57.5, "Mauritius", "sugar cane and tourism storms", "agriculture_weather"),
    ("Dubai aviation", 25.3, 55.4, "United Arab Emirates", "aviation heat and power demand", "industrial_weather"),
    ("Qatar LNG", 25.3, 51.6, "Qatar", "LNG operations and heat demand", "energy_weather"),
    ("Saudi oil", 26.4, 50.1, "Saudi Arabia", "oil and petrochemical heat stress", "energy_weather"),
    ("Oman port", 23.6, 58.5, "Oman", "energy shipping and cyclone risk", "port_weather"),
    ("Turkey hazelnut", 41.0, 39.7, "Turkey", "hazelnut frost and rainfall", "agriculture_weather"),
    ("Ukraine grain", 46.5, 30.7, "Ukraine", "grain harvest and Black Sea logistics", "agriculture_weather"),
    ("Romania grain", 44.2, 28.7, "Romania", "grain exports and regional drought", "agriculture_weather"),
    ("Poland industry", 52.2, 21.0, "Poland", "power and industrial heating demand", "energy_weather"),
    ("Hungary maize", 47.5, 19.0, "Hungary", "maize heat and drought", "agriculture_weather"),
    ("Italian rice", 45.5, 9.2, "Italy", "rice irrigation and heat stress", "agriculture_weather"),
    ("Spain olives", 37.4, -5.9, "Spain", "olive oil supply and heat", "agriculture_weather"),
    ("Portugal cork", 38.7, -9.1, "Portugal", "cork drought and wildfire exposure", "agriculture_weather"),
    ("French wheat", 48.0, 1.5, "France", "wheat harvest rain and heat", "agriculture_weather"),
    ("Bordeaux wine", 44.8, -0.6, "France", "wine frost and summer drought", "agriculture_weather"),
    ("Rotterdam shipping", 51.9, 4.5, "Netherlands", "port and refinery weather", "port_weather"),
    ("Antwerp chemicals", 51.2, 4.4, "Belgium", "chemical and container logistics", "industrial_weather"),
    ("UK gas demand", 51.5, -0.1, "United Kingdom", "gas and heating demand", "energy_weather"),
    ("Norway hydro", 60.4, 5.3, "Norway", "hydropower inflows and Nordic power", "energy_weather"),
    ("Sweden timber", 63.8, 20.3, "Sweden", "forestry access and power demand", "industrial_weather"),
    ("Finland pulp", 61.1, 28.2, "Finland", "pulp mill logistics and heat demand", "industrial_weather"),
    ("Iceland aluminum", 64.1, -21.9, "Iceland", "aluminum power and port weather", "industrial_weather"),
    ("Kazakhstan wheat", 53.2, 63.6, "Kazakhstan", "wheat drought and frost", "agriculture_weather"),
    ("Uzbekistan cotton", 41.3, 69.3, "Uzbekistan", "cotton irrigation and heat", "agriculture_weather"),
    ("Mongolia copper", 47.9, 106.9, "Mongolia", "copper logistics and winter weather", "industrial_weather"),
]

GHCN_METRICS = {
    "PRCP": ("precipitation", "daily precipitation", "mm", .1),
    "TMAX": ("maximum_temperature", "daily maximum temperature", "degC", .1),
    "TMIN": ("minimum_temperature", "daily minimum temperature", "degC", .1),
    "TAVG": ("mean_temperature", "daily mean temperature", "degC", .1),
    "SNWD": ("snow_depth", "snow depth", "mm", 1),
    "AWND": ("wind_speed", "daily mean wind speed", "m/s", .1),
}


def parallel(jobs, worker, workers=4):
    result, errors = [], []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        tasks = {pool.submit(worker, item): item for item in jobs}
        for task in as_completed(tasks):
            try:
                output = task.result()
                result.extend(output)
                print(f"OK {str(tasks[task])[:85]}: {len(output)} series", flush=True)
            except Exception as exc:
                errors.append({"item": str(tasks[task]), "error": str(exc)})
                print(f"ERROR {str(tasks[task])[:85]}: {exc}", flush=True)
    return result, errors


def pinned_selection(name, build):
    path = ROOT / "logs" / f"physical2_{name}_selection.json"
    if path.exists():
        return json.loads(path.read_text())
    selection = build()
    atomic_bytes(path, json.dumps(selection, indent=2).encode())
    return selection


class EligibilityOmission(ValueError):
    """Valid source response without enough usable daily history."""


class SourceUnavailable(ValueError):
    """Provider explicitly says this optional station/product has no data."""


def omission(item, exc):
    return {"item": str(item), "error": str(exc), "kind": "expected_omission"}


def save(series_id, frame, **kwargs):
    if frame.empty:
        raise EligibilityOmission(f"{series_id}: no numeric observations")
    frame = normalize_frame(frame)
    if frame["value"].nunique() < 2:
        raise EligibilityOmission(f"{series_id}: constant observations")
    if not qualifies_daily(dict(coverage(frame), frequency=kwargs["frequency"])):
        raise EligibilityOmission(f"{series_id}: insufficient genuine daily history: {coverage(frame)}")
    return save_candidate(GROUP, series_id, frame, **kwargs)


def clean_frame(dates, values):
    out = pd.DataFrame({"date": pd.to_datetime(dates, utc=True), "value": pd.to_numeric(values, errors="coerce")})
    out = out[np.isfinite(out.value)].sort_values("date")
    out = out[out.date >= START]
    # Duplicate source records are allowed only when the values agree.
    conflicts = out.groupby("date")["value"].nunique()
    if (conflicts > 1).any():
        raise ValueError("Conflicting values at the same source observation timestamp")
    return out.drop_duplicates("date").reset_index(drop=True)


def parse_ghcn(content, start=START):
    """Read NOAA fixed-width monthly daily records; discard QC and presumed zeros."""
    result = {element: [] for element in GHCN_METRICS}
    flags = {element: set() for element in GHCN_METRICS}
    for line in content.decode("ascii").splitlines():
        if len(line) < 269 or line[17:21] not in result or int(line[11:15]) < int(start[:4]):
            continue
        year, month, element = int(line[11:15]), int(line[15:17]), line[17:21]
        for day in range(1, calendar.monthrange(year, month)[1] + 1):
            offset = 21 + (day - 1) * 8
            value = int(line[offset:offset + 5])
            measurement, quality, source = line[offset + 5:offset + 8]
            if value == -9999 or quality.strip() or measurement in {"I", "P"}:
                continue
            date = f"{year:04d}-{month:02d}-{day:02d}"
            if date < start:
                continue
            result[element].append({"date": date, "value": value * GHCN_METRICS[element][3]})
            flags[element].add(source)
    return result, {key: sorted(value) for key, value in flags.items()}


def ghcn(fetcher):
    omissions = []
    station_bytes, station_digest = fetcher.get(GHCN_BASE + "ghcnd-stations.txt", max_age_hours=720)
    inventory_bytes, inventory_digest = fetcher.get(GHCN_BASE + "ghcnd-inventory.txt", max_age_hours=720)
    country_bytes, country_digest = fetcher.get(GHCN_BASE + "ghcnd-countries.txt", max_age_hours=720)
    countries = {line[:2]: line[3:].strip() for line in country_bytes.decode().splitlines()}
    stations = {}
    for line in station_bytes.decode().splitlines():
        stations[line[:11]] = dict(station=line[:11], latitude=float(line[12:20]),
                                  longitude=float(line[21:30]), station_name=line[41:71].strip(),
                                  country=countries.get(line[:2], line[:2]))
    active = {}
    for line in inventory_bytes.decode().splitlines():
        station, element = line[:11], line[31:35]
        if element in GHCN_METRICS and int(line[36:40]) <= 2022 and int(line[41:45]) >= 2025:
            active.setdefault(station, []).append(element)
    available = [stations[s] | {"elements": elements} for s, elements in active.items()
                 if len(elements) >= 3 and s in stations and s[:2] != "GM"]

    def select():
        chosen, seen = [], set()
        for target, lat, lon, country, mechanism, category in TARGETS:
            options = sorted(available, key=lambda s:
                             (s["latitude"] - lat) ** 2 +
                             ((s["longitude"] - lon) * np.cos(np.radians(lat))) ** 2)
            for station in options[:12]:
                dist = 111.2 * ((station["latitude"] - lat) ** 2 +
                               ((station["longitude"] - lon) * np.cos(np.radians(lat))) ** 2) ** .5
                if dist > 240 or station["station"] in seen:
                    continue
                chosen.append(station | dict(target=target, mechanism=mechanism, category=category,
                                              target_distance_km=round(dist, 1)))
                seen.add(station["station"])
                break
        return chosen

    def worker(item):
        url = GHCN_BASE + f"all/{item['station']}.dly"
        data, digest = fetcher.get(url)
        parsed, flags = parse_ghcn(data)
        result = []
        # Four distinct measured quantities per site, with an extra wind/snow
        # measurement when available to diversify the meteorological mechanisms.
        for element in item["elements"]:
            if not parsed[element]:
                continue
            slug, label, unit, _ = GHCN_METRICS[element]
            frame = pd.DataFrame(parsed[element])
            try:
                result.append(save(
                    f"ghcn_{item['station'].lower()}_{slug}", frame,
                    title=f"{item['station_name']}, {item['country']}: {label}",
                    provider="noaa_ghcn_daily", category=item["category"], unit=unit,
                    frequency="daily", source_url=url, geography=item["country"],
                    hypothesis=f"Research hypothesis: observed station weather near {item['target']} may affect {item['mechanism']}; test seasonal and threshold effects.",
                    limitations="Point station, not production-weighted region. Historical observations can be revised; not point-in-time. Nonblank NOAA quality flags and missing/presumed-zero measurement flags I/P excluded; trace precipitation retains source zero. Daily periods vary by national service; source S derives daily reports from synoptic observations and precipitation can be less reliable. Missing days remain missing.",
                    raw_sha256=[digest, station_digest, inventory_digest, country_digest],
                    dataset_identity=f"NOAA GHCN-Daily {item['station']} {element}",
                    native_frequency="daily", documentation_url=GHCN_BASE + "readme.txt",
                    timestamp_semantics="Source observation-day label represented at UTC midnight, not release time",
                    measurement_type="observed_station", source_qualifiers=flags[element],
                    source_station_metadata=item, latitude=item["latitude"], longitude=item["longitude"],
                    license="NOAA public data; cite GHCN-Daily doi:10.7289/V5D21VHZ",
                ))
            except EligibilityOmission as exc:
                omissions.append(omission(f"{item['station']} {element}", exc))
                print(f"SKIP {item['station']} {element}: {exc}", flush=True)
        return result
    rows, errors = parallel(pinned_selection("ghcn", select), worker, workers=5)
    return rows, errors + omissions


DWD_TARGETS = [
    ("Hamburg", 53.6, 10.0, "container port and wind power"),
    ("Bremen", 53.0, 8.8, "port, aerospace and wind power"),
    ("Wilhelmshaven", 53.5, 8.1, "LNG terminal and crude imports"),
    ("Rostock", 54.1, 12.1, "Baltic port, crops and wind power"),
    ("Kiel", 54.3, 10.1, "Baltic shipping and offshore wind"),
    ("Ruhr", 51.5, 7.0, "steel, chemicals and power demand"),
    ("Frankfurt", 50.1, 8.7, "aviation, data centers and power"),
    ("Mannheim", 49.5, 8.5, "Rhine chemical industry and cooling"),
    ("Karlsruhe", 49.0, 8.4, "refining and Rhine transport"),
    ("Saarbrucken", 49.2, 7.0, "steel and industrial power"),
    ("Stuttgart", 48.8, 9.2, "automotive manufacturing"),
    ("Munich", 48.1, 11.6, "automotive and industrial heat"),
    ("Leipzig", 51.3, 12.4, "auto production and air freight"),
    ("Dresden", 51.1, 13.8, "semiconductor water and cooling"),
    ("Potsdam", 52.4, 13.1, "regional agriculture and power"),
    ("Magdeburg", 52.1, 11.6, "sugar beet and cereal agriculture"),
    ("Erfurt", 51.0, 11.0, "cereal yields and transport"),
    ("Freiburg", 48.0, 7.9, "solar generation and vineyards"),
    ("Wurzburg", 49.8, 9.9, "vineyards and agricultural drought"),
    ("Kempten", 47.7, 10.3, "dairy pasture and Alpine runoff"),
    ("Cottbus", 51.8, 14.3, "lignite generation and water demand"),
    ("Emden", 53.4, 7.2, "automotive shipping and wind generation"),
]
DWD_METRICS = {
    "FX": ("wind_gust", "maximum wind gust", "m/s"),
    "FM": ("wind_speed", "mean wind speed", "m/s"),
    "SDK": ("sunshine_duration", "sunshine duration", "hours"),
    "SHK_TAG": ("snow_depth", "snow depth", "cm"),
    "NM": ("cloud_cover", "mean cloud cover", "oktas"),
    "VPM": ("vapor_pressure", "mean vapor pressure", "hPa"),
    "UPM": ("relative_humidity", "mean relative humidity", "percent"),
    "TGK": ("ground_minimum_temperature", "minimum temperature 5cm above ground", "degC"),
}


def parse_dwd(content):
    with zipfile.ZipFile(io.BytesIO(content)) as archive:
        names = [name for name in archive.namelist() if name.startswith("produkt_klima_tag_")]
        if len(names) != 1:
            raise ValueError(f"Expected one DWD data member, got {names}")
        frame = pd.read_csv(io.BytesIO(archive.read(names[0])), sep=";", skipinitialspace=True)
    frame.columns = frame.columns.str.strip()
    frame["date"] = pd.to_datetime(frame["MESS_DATUM"].astype(str), format="%Y%m%d", utc=True)
    return frame


def dwd(fetcher):
    omissions = []
    metadata, metadata_digest = fetcher.get(DWD_BASE + "recent/KL_Tageswerte_Beschreibung_Stationen.txt", max_age_hours=720)
    listing, listing_digest = fetcher.get(DWD_BASE + "historical/", max_age_hours=720)
    files = re.findall(r'href="(tageswerte_KL_\d+_\d+_\d+_hist.zip)"', listing.decode())
    historical = {}
    for name in files:
        station = name.split("_")[2]
        historical.setdefault(station, []).append(name)
    stations = []
    for line in metadata.decode("latin1").splitlines()[2:]:
        fields = line.split()
        if len(fields) < 8:
            continue
        try:
            stations.append(dict(station=f"{int(fields[0]):05d}", latitude=float(fields[4]),
                                 longitude=float(fields[5]), station_name=fields[6],
                                 from_date=fields[1], to_date=fields[2]))
        except ValueError:
            continue

    def select():
        chosen, seen = [], set()
        for target, lat, lon, mechanism in DWD_TARGETS:
            options = sorted(stations, key=lambda s: (s["latitude"] - lat) ** 2 + ((s["longitude"] - lon) * .65) ** 2)
            for item in options[:12]:
                if item["station"] not in seen and item["station"] in historical and item["to_date"] >= "20260101":
                    chosen.append(item | dict(target=target, mechanism=mechanism))
                    seen.add(item["station"])
                    break
        return chosen

    def worker(item):
        station = item["station"]
        names = sorted(historical[station])
        parts, hashes, urls = [], [metadata_digest, listing_digest], []
        for name in names + [f"tageswerte_KL_{station}_akt.zip"]:
            url = DWD_BASE + ("recent/" if name.endswith("_akt.zip") else "historical/") + name
            content, digest = fetcher.get(url, max_age_hours=24 if name.endswith("_akt.zip") else 720)
            parts.append(parse_dwd(content))
            hashes.append(digest)
            urls.append(url)
        # The quality-controlled historical file wins over the provisional recent overlap.
        frame = pd.concat(parts).drop_duplicates("date", keep="first")
        result = []
        for column, (slug, label, unit) in DWD_METRICS.items():
            value = pd.to_numeric(frame[column], errors="coerce")
            candidate = clean_frame(frame.date, value.mask(value == -999))
            try:
                result.append(save(
                    f"dwd_{station}_{slug}", candidate, title=f"{item['station_name']}, Germany: {label}",
                    provider="dwd_climate_observations", category="european_industrial_weather", unit=unit,
                    frequency="daily", source_url=urls[-1], geography="Germany",
                    hypothesis=f"Research hypothesis: {label} near {item['target']} may affect {item['mechanism']}; test nonlinear thresholds and seasonality.",
                    limitations="Single DWD station; not a regional production index. Historical observations are quality controlled, recent observations provisional and revisable. -999 missing values omitted; no interpolation. Historical file takes priority on overlap. Observation-day conventions vary by measurement and historical era.",
                    raw_sha256=hashes, dataset_identity=f"DWD CDC daily KL {station} {column}",
                    native_frequency="daily", source_urls=urls, source_station_metadata=item,
                    latitude=item["latitude"], longitude=item["longitude"],
                    documentation_url=DWD_BASE + "DESCRIPTION_obsgermany_climate_daily_kl_en.pdf",
                    timestamp_semantics="DWD observation-day label represented at UTC midnight",
                    license="DWD open data, attribution to Deutscher Wetterdienst",
                ))
            except EligibilityOmission as exc:
                omissions.append(omission(f"DWD {station} {column}", exc))
                print(f"SKIP DWD {station} {column}: {exc}", flush=True)
        return result
    rows, errors = parallel(pinned_selection("dwd", select), worker)
    return rows, errors + omissions


HYDAT_RIVERS = [
    ("FRASER", "Pacific export corridor, salmon and flood exposure"),
    ("COLUMBIA", "hydropower supply and downstream industrial water"),
    ("PEACE", "hydropower generation and northern resource logistics"),
    ("ATHABASCA", "oil sands water availability and river navigation"),
    ("MACKENZIE", "Arctic barge fuel deliveries and northern mining"),
    ("SLAVE", "hydropower and northern resource logistics"),
    ("SKEENA", "Pacific grain/coal port corridor and salmon"),
    ("THOMPSON", "irrigation, rail corridor and salmon"),
    ("NECHAKO", "aluminum smelter hydropower and fisheries"),
    ("BOW RIVER", "Alberta irrigation and urban water supply"),
    ("OLDMAN", "irrigated crop yields and agricultural water"),
    ("SOUTH SASKATCHEWAN", "prairie irrigation and crop supply"),
    ("NORTH SASKATCHEWAN", "oil and industrial water availability"),
    ("RED RIVER", "prairie agriculture and flood disruption"),
    ("ASSINIBOINE", "prairie grain irrigation and flooding"),
    ("SASKATCHEWAN RIVER", "grain-belt water and hydropower"),
    ("WINNIPEG RIVER", "hydropower supply and pulp/industrial water"),
    ("NELSON", "Manitoba hydropower exports"),
    ("CHURCHILL", "hydropower generation and subarctic logistics"),
    ("OTTAWA", "hydropower, urban floods and pulp production"),
    ("ST. LAWRENCE", "Great Lakes shipping and hydroelectric supply"),
    ("SAINT-LAURENT", "Great Lakes shipping and hydroelectric supply"),
    ("SAINT-MAURICE", "Quebec hydropower and pulp production"),
    ("SAGUENAY", "aluminum hydropower and export logistics"),
    ("SAINT JOHN", "New Brunswick hydropower and flood exposure"),
    ("MIRAMICHI", "forestry, fisheries and flood disruption"),
    ("EXPLOITS", "Newfoundland hydropower and pulp production"),
    ("YUKON", "northern gold mining and barge access"),
    ("LIARD", "northern transport and hydropower inflows"),
    ("HAY RIVER", "northern barge logistics and flood exposure"),
    ("COPPERMINE", "Arctic mining water and transport"),
    ("BACK RIVER", "northern mining and freshwater conditions"),
    ("KOOTENAY", "hydroelectric supply and cross-border water"),
    ("PEND D'OREILLE", "hydroelectric supply and regional inflows"),
    ("COWICHAN", "pulp mill water and salmon exposure"),
]


def parse_hydat(payload):
    if payload.get("numberMatched", len(payload["features"])) > len(payload["features"]):
        raise ValueError("HYDAT response truncated; request pagination required")
    return pd.DataFrame([item["properties"] for item in payload["features"]])


def hydat(fetcher):
    omissions = []
    content, metadata_digest = fetcher.get(HYDAT_BASE + "hydrometric-stations/items",
                                          params={"limit": 10000, "f": "json"}, max_age_hours=720)
    metadata_hashes = [metadata_digest]
    payload = json.loads(content)
    if payload.get("numberMatched", 0) > len(payload["features"]):
        # Metadata pagination is explicit; station catalog currently exceeds 10k.
        features = payload["features"]
        next_urls = [x["href"] for x in payload.get("links", []) if x.get("rel") == "next"]
        while next_urls:
            page_content, page_digest = fetcher.get(next_urls[0], max_age_hours=720)
            metadata_hashes.append(page_digest)
            page = json.loads(page_content)
            features += page["features"]
            next_urls = [x["href"] for x in page.get("links", []) if x.get("rel") == "next"]
    else:
        features = payload["features"]

    def select():
        chosen, seen = [], set()
        for river, mechanism in HYDAT_RIVERS:
            options = [x for x in features if river in x["properties"]["STATION_NAME"].upper()
                       and x["properties"].get("STATUS_EN") == "Active"]
            options.sort(key=lambda x: x["properties"].get("DRAINAGE_AREA_GROSS") or 0, reverse=True)
            for feature in options[:2]:
                properties = feature["properties"]
                station = properties["STATION_NUMBER"]
                if station in seen:
                    continue
                coordinates = feature["geometry"]["coordinates"]
                chosen.append(dict(station=station, station_name=properties["STATION_NAME"],
                                   province=properties["PROV_TERR_STATE_LOC"], mechanism=mechanism,
                                   longitude=coordinates[0], latitude=coordinates[1]))
                seen.add(station)
        return chosen

    def worker(item):
        params = dict(STATION_NUMBER=item["station"], datetime=f"{START}/..", limit=10000, f="json")
        content, digest = fetcher.get(HYDAT_BASE + "hydrometric-daily-mean/items", params=params, timeout=120)
        frame = parse_hydat(json.loads(content))
        result = []
        for column, unit, category, label in [
                ("DISCHARGE", "m3/s", "canadian_river_flow", "daily mean discharge"),
                ("LEVEL", "m", "canadian_water_levels", "daily mean water level")]:
            if column not in frame:
                continue
            candidate = clean_frame(frame["DATE"], frame[column])
            try:
                result.append(save(
                    f"hydat_{item['station'].lower()}_{column.lower()}", candidate,
                    title=f"{item['station_name']}: {label}", provider="environment_canada_hydat",
                    category=category, unit=unit, frequency="daily",
                    source_url=HYDAT_BASE + "hydrometric-daily-mean/items?" + urlencode(params),
                    geography=f"{item['province']}, Canada",
                    hypothesis=f"Research hypothesis: observed {label} may affect {item['mechanism']}; test seasonal deviations and nonlinear low/high water effects.",
                    limitations="Historical HYDAT archive has publication lag and revisions; not a live gauge feed or point-in-time archive. Levels are relative to local gauge datum and are not comparable across stations. Discharge uses rating curves and can include estimated or ice-affected source values. Missing records omitted with no filling.",
                    raw_sha256=[digest] + metadata_hashes,
                    dataset_identity=f"Environment Canada HYDAT {item['station']} daily {column}",
                    native_frequency="daily", source_station_metadata=item,
                    source_qualifiers=sorted(str(x) for x in frame.get(column + "_SYMBOL_EN", pd.Series(dtype=str)).dropna().unique()),
                    latitude=item["latitude"], longitude=item["longitude"],
                    documentation_url="https://eccc-msc.github.io/open-data/msc-data/obs_hydrometric/readme_hydrometric_en/",
                    timestamp_semantics="Station's source calendar-day label represented at UTC midnight, not release time",
                    expected_publication_lag="Historical HYDAT archive can lag recent observations by months or years; inspect end date",
                    license="Open Government Licence - Canada",
                ))
            except EligibilityOmission as exc:
                omissions.append(omission(f"HYDAT {item['station']} {column}", exc))
                print(f"SKIP HYDAT {item['station']} {column}: {exc}", flush=True)
        return result
    rows, errors = parallel(pinned_selection("hydat", select), worker, workers=4)
    return rows, errors + omissions


COOPS_STATIONS = [
    ("9414290", "San Francisco", "Pacific refining, shipping and marine ecosystems"),
    ("9410660", "Los Angeles", "container port access and coastal logistics"),
    ("9410170", "San Diego", "naval, tourism and cross-border logistics"),
    ("9414750", "Alameda", "Oakland container and Bay shipping"),
    ("9447130", "Seattle", "Pacific grain, container and fishing operations"),
    ("9446484", "Tacoma", "container and automobile port operations"),
    ("9439040", "Astoria", "Columbia grain exports and salmon"),
    ("9432780", "Charleston Oregon", "fisheries and forest-product exports"),
    ("9455920", "Anchorage", "Alaska cargo, fuel deliveries and ice exposure"),
    ("9459450", "Sand Point", "Alaska fishing operations"),
    ("1612340", "Honolulu", "Pacific trade, tourism and coastal exposure"),
    ("1770000", "Pago Pago", "tuna fisheries and Pacific shipping"),
    ("1630000", "Apra Harbor", "western Pacific cargo and tourism"),
    ("8728690", "Apalachicola", "seafood, estuary health and coastal losses"),
    ("8761724", "Grand Isle", "offshore oil, fisheries and hurricane exposure"),
    ("8764227", "Amerada Pass", "Gulf energy logistics and estuary conditions"),
    ("8771450", "Galveston", "Houston refining and petrochemical shipping"),
    ("8770613", "Morgans Point", "Houston ship channel and petrochemical port"),
    ("8775241", "Aransas Pass", "Corpus Christi crude and LNG exports"),
    ("8779770", "Port Isabel", "Gulf energy and coastal storm exposure"),
    ("8735180", "Dauphin Island", "Mobile port, forest products and seafood"),
    ("8726520", "St Petersburg", "Florida fuel imports and coastal tourism"),
    ("8724580", "Key West", "Florida tourism and coastal storm exposure"),
    ("8723214", "Virginia Key", "Miami trade and insurance storm exposure"),
    ("8720218", "Mayport", "Jacksonville container and vehicle logistics"),
    ("8670870", "Fort Pulaski", "Savannah container shipping"),
    ("8665530", "Charleston", "container and vehicle exports"),
    ("8658120", "Wilmington", "North Carolina container and chemicals"),
    ("8638610", "Sewells Point", "Hampton Roads coal exports and shipping"),
    ("8632200", "Kiptopeke", "Chesapeake seafood and coastal navigation"),
    ("8575512", "Annapolis", "Chesapeake navigation and coastal flooding"),
    ("8574680", "Baltimore", "automobile and bulk imports"),
    ("8545240", "Philadelphia", "refining, food imports and Delaware shipping"),
    ("8534720", "Atlantic City", "tourism and coastal insurance exposure"),
    ("8518750", "The Battery", "New York shipping and coastal flood risk"),
    ("8516945", "Kings Point", "Long Island shipping and coastal real estate"),
    ("8461490", "New London", "New England ferry and port operations"),
    ("8452660", "Newport", "coastal tourism and marine conditions"),
    ("8443970", "Boston", "LNG, fuel delivery and container shipping"),
    ("8418150", "Portland Maine", "fuel imports and lobster fisheries"),
    ("9755371", "San Juan", "Caribbean cargo and tourism storms"),
    ("9751401", "Lime Tree Bay", "Caribbean energy and fuel logistics"),
]
COOPS_PRODUCTS = {
    "hourly_height": ("water_level", "verified water level", "m above station datum"),
    "water_temperature": ("water_temperature", "water temperature", "degC"),
    "air_pressure": ("air_pressure", "barometric pressure", "hPa"),
    "wind": ("wind_speed", "wind speed", "m/s"),
}
COOPS_REGIONS = {
    "941": "California", "944": "Washington", "943": "Oregon", "945": "Alaska",
    "161": "Hawaii", "177": "American Samoa", "163": "Guam", "872": "Florida",
    "876": "Louisiana", "877": "Texas", "873": "Alabama", "867": "Georgia",
    "866": "South Carolina", "865": "North Carolina", "863": "Virginia",
    "857": "Maryland", "854": "Pennsylvania", "853": "New Jersey", "851": "New York",
    "846": "Connecticut", "845": "Rhode Island", "844": "Massachusetts",
    "841": "Maine",
}


def coops_region(station):
    return {"9755371": "Puerto Rico", "9751401": "US Virgin Islands"}.get(
        station, COOPS_REGIONS.get(station[:3], "United States"))


def parse_coops(payload, product):
    if "error" in payload:
        if "No data was found" in str(payload["error"]):
            raise SourceUnavailable(payload["error"])
        raise ValueError(payload["error"])
    if "data" not in payload:
        raise ValueError("Unexpected NOAA CO-OPS response: missing data field")
    rows = []
    for row in payload.get("data", []):
        # API flags are product-specific; 1 signals a failed automated check.
        flags = row.get("f", "").split(",")
        if "1" in flags:
            continue
        value = row.get("s" if product == "wind" else "v", "")
        if value not in ("", None):
            rows.append(dict(date=row["t"], value=value))
    if not rows:
        return pd.DataFrame(columns=["date", "value"])
    return clean_frame([x["date"] for x in rows], [x["value"] for x in rows])


def coops(fetcher):
    omissions = []
    end = (pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=3)).strftime("%Y%m%d")

    def worker(item):
        station, name, mechanism = item
        result = []
        for product, (slug, label, unit) in COOPS_PRODUCTS.items():
            frames, hashes, urls = [], [], []
            station_metadata = {"id": station, "name": name}
            for year in range(2024, int(end[:4]) + 1):
                params = dict(product=product, station=station, begin_date=f"{year}0101",
                              end_date=min(f"{year}1231", end), time_zone="gmt",
                              units="metric", format="json", application="FinAltDataResearch")
                if product == "hourly_height":
                    params["datum"] = "STND"
                else:
                    params["interval"] = "h"
                try:
                    content, digest = fetcher.get(COOPS_API, params=params, timeout=120)
                    payload = json.loads(content)
                    frame = parse_coops(payload, product)
                    station_metadata.update(payload.get("metadata", {}))
                    frames.append(frame)
                    hashes.append(digest)
                    urls.append(COOPS_API + "?" + urlencode(params))
                except SourceUnavailable as exc:
                    omissions.append(omission(f"COOPS {station} {product} {year}", exc))
                    print(f"MISSING COOPS {station} {product} {year}: {exc}", flush=True)
            if not frames:
                continue
            candidate = pd.concat(frames).drop_duplicates("date").sort_values("date")
            try:
                result.append(save(
                    f"coops_{station}_{slug}", candidate, title=f"{name}: {label}",
                    provider="noaa_coops", category="port_water_levels" if product == "hourly_height"
                    else "coastal_water_temperature" if product == "water_temperature" else "coastal_operations_weather",
                    unit=unit, frequency="hourly", source_url=urls[-1],
                    geography=f"{name}, {coops_region(station)}, United States",
                    hypothesis=f"Research hypothesis: observed {label} may affect {mechanism}; separate predictable tides and seasons from unusual operational stress.",
                    limitations="Point coastal sensor; not port throughput. Water levels use station-specific STND datum and are not comparable in absolute magnitude across stations. Verified heights lag monthly quality control; meteorological observations may be provisional/revised. Automated flagged records and missing values omitted. Hourly meteorological records are actual six-minute observations sampled on the hour; the native finer cadence is available from NOAA.",
                    raw_sha256=hashes, dataset_identity=f"NOAA CO-OPS {station} {product} GMT metric" + (" STND" if product == "hourly_height" else ""),
                    native_frequency="hourly" if product == "hourly_height" else "6min",
                    finer_frequency_available=product != "hourly_height",
                    source_urls=urls, source_station_metadata=station_metadata,
                    latitude=float(station_metadata["lat"]) if "lat" in station_metadata else None,
                    longitude=float(station_metadata["lon"]) if "lon" in station_metadata else None,
                    documentation_url="https://api.tidesandcurrents.noaa.gov/api/prod/",
                    timestamp_semantics="Actual observation timestamp, GMT/UTC",
                    license="NOAA public data",
                ))
            except EligibilityOmission as exc:
                omissions.append(omission(f"COOPS {station} {product}", exc))
                print(f"SKIP COOPS {station} {product}: {exc}", flush=True)
        return result
    rows, errors = parallel(COOPS_STATIONS, worker, workers=5)
    return rows, errors + omissions


EPA_COUNTIES = [
    ("California", "Los Angeles", "port drayage, industrial activity and outdoor labor"),
    ("California", "Alameda", "Oakland shipping, logistics and wildfire disruptions"),
    ("California", "Contra Costa", "refining, chemical production and wildfire smoke"),
    ("California", "Kern", "oil production, agriculture and outdoor labor"),
    ("California", "Fresno", "agriculture, irrigation work and wildfire exposure"),
    ("California", "Tulare", "dairy production and agricultural labor"),
    ("California", "San Joaquin", "distribution centers and agricultural labor"),
    ("California", "Riverside", "warehouse logistics and construction"),
    ("California", "San Bernardino", "rail, warehouse logistics and construction"),
    ("California", "Santa Clara", "technology facilities and wildfire disruption"),
    ("California", "San Diego", "cross-border manufacturing and tourism"),
    ("California", "Sacramento", "regional construction and wildfire disruptions"),
    ("Oregon", "Multnomah", "Pacific export logistics and wildfire exposure"),
    ("Oregon", "Lane", "forestry, outdoor labor and wildfire smoke"),
    ("Washington", "King", "aviation production, logistics and wildfire smoke"),
    ("Washington", "Pierce", "Tacoma port operations and outdoor labor"),
    ("Washington", "Spokane", "grain transport and wildfire exposure"),
    ("Washington", "Yakima", "fruit production and agricultural labor"),
    ("Idaho", "Ada", "semiconductor production and wildfire smoke"),
    ("Montana", "Yellowstone", "refining and wildfire smoke"),
    ("Nevada", "Clark", "tourism, construction and outdoor labor"),
    ("Utah", "Salt Lake", "refining, mining logistics and inversion pollution"),
    ("Arizona", "Maricopa", "semiconductor construction and desert dust exposure"),
    ("Arizona", "Pima", "mining, aerospace and desert dust"),
    ("Colorado", "Denver", "transportation activity and construction"),
    ("Colorado", "Weld", "oil and gas production and agricultural labor"),
    ("New Mexico", "Bernalillo", "semiconductor facilities and wildfire smoke"),
    ("Texas", "Harris", "refining, petrochemicals and ship-channel operations"),
    ("Texas", "Jefferson", "refinery and petrochemical production"),
    ("Texas", "Nueces", "crude and LNG export operations"),
    ("Texas", "Dallas", "freight, construction and consumer activity"),
    ("Texas", "Tarrant", "aerospace production and freight activity"),
    ("Texas", "El Paso", "cross-border manufacturing and freight"),
    ("Texas", "Travis", "semiconductor construction and local activity"),
    ("Louisiana", "East Baton Rouge", "refinery and chemical production"),
    ("Louisiana", "Calcasieu", "LNG, refinery and chemical operations"),
    ("Louisiana", "Orleans", "port, tourism and outdoor labor"),
    ("Oklahoma", "Tulsa", "oil refining and aviation manufacturing"),
    ("Oklahoma", "Oklahoma", "energy service and construction activity"),
    ("Kansas", "Sedgwick", "aerospace manufacturing and industrial activity"),
    ("Nebraska", "Douglas", "food processing and freight activity"),
    ("Iowa", "Polk", "agriculture logistics and construction"),
    ("Minnesota", "Hennepin", "industrial activity and wildfire smoke"),
    ("Missouri", "St. Louis", "industrial manufacturing and logistics"),
    ("Illinois", "Cook", "rail, manufacturing and freight activity"),
    ("Illinois", "Madison", "steel, refining and Mississippi logistics"),
    ("Indiana", "Lake", "steel, refining and heavy industry"),
    ("Indiana", "Marion", "auto supply chains and freight"),
    ("Ohio", "Cuyahoga", "steel and manufacturing output"),
    ("Ohio", "Hamilton", "manufacturing and Ohio River logistics"),
    ("Ohio", "Franklin", "logistics and construction"),
    ("Michigan", "Wayne", "automotive and steel production"),
    ("Wisconsin", "Milwaukee", "industrial production and manufacturing"),
    ("Pennsylvania", "Allegheny", "steel and coke production"),
    ("Pennsylvania", "Philadelphia", "refining, port and transportation activity"),
    ("New Jersey", "Essex", "port trucking and industrial activity"),
    ("New Jersey", "Middlesex", "chemical industry and distribution logistics"),
    ("New York", "Queens", "aviation and freight activity"),
    ("New York", "Erie", "industrial production and cross-border logistics"),
    ("Massachusetts", "Suffolk", "aviation, construction and consumer activity"),
    ("Connecticut", "Hartford", "aerospace and precision manufacturing"),
    ("Maryland", "Baltimore", "port and industrial activity"),
    ("Virginia", "Fairfax", "data centers, construction and transport"),
    ("West Virginia", "Kanawha", "chemical production and energy industry"),
    ("Kentucky", "Jefferson", "air cargo, auto manufacturing and chemicals"),
    ("Tennessee", "Shelby", "air cargo and Mississippi logistics"),
    ("Alabama", "Jefferson", "steel and automotive production"),
    ("Alabama", "Mobile", "port, shipbuilding and aerospace production"),
    ("Georgia", "Fulton", "aviation, construction and consumer activity"),
    ("Georgia", "Chatham", "container shipping and port trucking"),
    ("South Carolina", "Charleston", "auto exports and aerospace production"),
    ("North Carolina", "Mecklenburg", "distribution logistics and construction"),
    ("Florida", "Miami-Dade", "tourism, aviation and container imports"),
    ("Florida", "Hillsborough", "fuel imports and regional construction"),
    ("Florida", "Duval", "automobile imports and port logistics"),
    ("Hawaii", "Honolulu", "tourism, aviation and shipping"),
    ("Alaska", "Anchorage", "air cargo, fuel logistics and wildfire smoke"),
    ("Puerto Rico", "San Juan", "Caribbean logistics and manufacturing"),
]


def parse_epa_county_zip(content):
    with zipfile.ZipFile(io.BytesIO(content)) as archive:
        names = [x for x in archive.namelist() if x.endswith(".csv")]
        if len(names) != 1:
            raise ValueError("Expected a single EPA AQI CSV")
        frame = pd.read_csv(io.BytesIO(archive.read(names[0])),
                            dtype={"State Code": str, "County Code": str})
    required = {"State Name", "county Name", "State Code", "County Code", "Date", "AQI"}
    if not required <= set(frame):
        raise ValueError("Unexpected EPA county AQI columns")
    return frame


def epa(fetcher):
    parts, hashes, urls = [], [], []
    # Archived AQS data, not AirNow's real-time product. Include each available
    # current-year record and expose the resulting lag explicitly in metadata.
    for year in range(2020, pd.Timestamp.now(tz="UTC").year + 1):
        url = f"https://aqs.epa.gov/aqsweb/airdata/daily_aqi_by_county_{year}.zip"
        content, digest = fetcher.get(url, timeout=120)
        parts.append(parse_epa_county_zip(content))
        hashes.append(digest)
        urls.append(url)
    all_data = pd.concat(parts, ignore_index=True)
    result, errors = [], []
    for state, county, mechanism in EPA_COUNTIES:
        subset = all_data[(all_data["State Name"] == state) & (all_data["county Name"] == county)]
        if subset.empty:
            errors.append(omission(f"{county}, {state}", "No matching source county"))
            continue
        codes = subset[["State Code", "County Code"]].drop_duplicates()
        if len(codes) != 1:
            raise ValueError(f"Ambiguous EPA county {state} {county}")
        code = "".join(codes.iloc[0].tolist())
        frame = clean_frame(subset["Date"], subset["AQI"])
        try:
            result.append(save(
                f"epa_county_{code}_aqi", frame, title=f"{county} County, {state}: daily air quality index",
                provider="us_epa_airdata", category="industrial_air_quality", unit="AQI index points",
                frequency="daily", source_url=urls[-1], geography=f"{county}, {state}, United States",
                hypothesis=f"Research hypothesis: measured air pollution can proxy {mechanism}, or disrupt activity through smoke and air-quality restrictions. Control for wind, temperature, transported pollution and changing pollutant mix.",
                limitations="EPA county AQI is the highest defining monitor/pollutant AQI, not an emissions, production or population-weighted measure. Monitor coverage and defining pollutant change. EPA AQI breakpoints/methods may change and historical data can be revised. AQS reporting lags months; these are historical observations, not a real-time trading feed or point-in-time dataset. No absent day is filled.",
                raw_sha256=hashes, dataset_identity=f"EPA AirData county {code} daily AQI",
                native_frequency="daily", source_urls=urls,
                documentation_url="https://aqs.epa.gov/aqsweb/airdata/FileFormats.html",
                timestamp_semantics="Local source day label represented at UTC midnight, not publication time",
                expected_publication_lag="Months; inspect per-series end and age_days",
                source_parameters={"state_code": codes.iloc[0]["State Code"], "county_code": codes.iloc[0]["County Code"],
                                   "defining_pollutants": sorted(subset["Defining Parameter"].dropna().unique().tolist())},
                license="US EPA public data; acknowledge EPA AirData/AQS",
            ))
        except EligibilityOmission as exc:
            errors.append(omission(f"{county}, {state}", exc))
    return result, errors


PROVIDERS = {"ghcn": ghcn, "dwd": dwd, "hydat": hydat, "coops": coops, "epa": epa}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--provider", choices=PROVIDERS)
    parser.add_argument("--refresh", action="store_true")
    args = parser.parse_args(argv)
    fetcher = Fetcher(refresh=args.refresh)
    failed = False
    for name in ([args.provider] if args.provider else PROVIDERS):
        try:
            rows, problems = PROVIDERS[name](fetcher)
        except Exception as exc:
            rows = []
            problems = [{"item": name, "error": f"{type(exc).__name__}: {exc}"}]
        errors = [item for item in problems if item.get("kind") != "expected_omission"]
        omissions = [item for item in problems if item.get("kind") == "expected_omission"]
        failed |= bool(errors)
        report = dict(provider=name, generated_at=pd.Timestamp.now(tz="UTC").isoformat(),
                      count=len(rows), rows=sum(x["rows"] for x in rows), errors=errors, omissions=omissions,
                      series_ids=[x["series_id"] for x in rows])
        atomic_bytes(ROOT / "logs" / f"physical2_{name}.json", json.dumps(report, indent=2).encode())
        print(json.dumps({k: v for k, v in report.items() if k != "series_ids"}), flush=True)
    return int(failed)


if __name__ == "__main__":
    raise SystemExit(main())
