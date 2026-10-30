#!/usr/bin/env python3
"""Additional real daily digital-adoption, internet, attention and safety series.

Each collector stages date/value Parquet plus documented source metadata. Network
responses are hashed and retained by Fetcher. No interpolation or missing-day fill.
"""
from __future__ import annotations
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib
import io
import json
import threading
import time
from urllib.parse import quote, urlparse

import pandas as pd
import requests
from expansion_common import Fetcher, ROOT, atomic_bytes, save_candidate
from sources_digital import parse_wikimedia, parse_npm, parse_csv, slug, year_windows

GROUP = 'digital2'
LOCKS = {}
LAST = {}
LOCKS_GUARD = threading.Lock()


def get(fetcher, url, params=None, *, interval=1.1, **kw):
    """Pace each host; a verified cached response needs no network delay."""
    prepared = requests.Request('GET', url, params=params).prepare().url
    key = hashlib.sha256(prepared.encode()).hexdigest()
    path = fetcher.root / 'archive/requests' / (key + '.json')
    if not fetcher.refresh and path.exists():
        meta = json.loads(path.read_text())
        age = (pd.Timestamp.now(tz='UTC') - pd.Timestamp(meta['retrieved_at'])).total_seconds()
        raw = fetcher.root / 'archive/raw' / (meta['sha256'] + '.bin')
        if 0 <= age < 86400 and raw.exists() and hashlib.sha256(raw.read_bytes()).hexdigest() == meta['sha256']:
            return fetcher.get(url, params, max_age_hours=24, **kw)
    host = urlparse(url).netloc
    with LOCKS_GUARD:
        lock = LOCKS.setdefault(host, threading.Lock())
    with lock:
        delay = interval - (time.monotonic() - LAST.get(host, 0))
        if delay > 0:
            time.sleep(delay)
        LAST[host] = time.monotonic()
    return fetcher.get(url, params, **kw)


def save(series_id, frame, **meta):
    if pd.to_numeric(frame['value']).nunique() < 2:
        raise ValueError('Constant source values do not provide a usable activity signal')
    return save_candidate(GROUP, series_id, frame, **meta)


def jobs(provider, specs, one, workers=3):
    good, bad = [], []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(one, spec): spec for spec in specs}
        for future in as_completed(futures):
            try:
                result = future.result()
                result = result if isinstance(result, list) else [result]
                good.extend(result)
                for meta in result:
                    print(f"OK {meta['series_id']} {meta['rows']} {meta['start'][:10]}..{meta['end'][:10]}", flush=True)
            except Exception as exc:
                error = {'provider': provider, 'candidate': str(futures[future]), 'error': str(exc)}
                bad.append(error)
                print('FAIL ' + json.dumps(error), flush=True)
    return good, bad


def spec_list(groups):
    return [(package, theme) for theme, packages in groups.items() for package in packages.split()]


CRAN = spec_list({
    'data science and analytics adoption': 'dplyr ggplot2 tidyr readr data.table arrow duckdb DBI odbc RPostgres RMySQL dbplyr tidyverse readxl openxlsx haven jsonlite httr2 curl rvest xml2 lubridate stringr purrr tibble',
    'machine learning and compute demand': 'xgboost ranger randomForest caret tidymodels parsnip recipes workflows tune mlr3 torch keras tensorflow reticulate h2o e1071 nnet glmnet lightgbm kernlab',
    'spatial analysis and infrastructure planning': 'sf terra raster sp stars leaflet tmap mapview geosphere gstat spdep exactextractr osmdata',
    'financial and economic modeling': 'quantmod TTR PerformanceAnalytics xts zoo rugarch rmgarch RQuantLib tidyquant forecast fable tsibble vars urca plm fixest AER sandwich lmtest',
    'life science clinical and statistical research': 'survival survminer lme4 nlme mgcv brms rstan rstanarm bayesplot posterior MCMCpack deSolve vegan ape phytools meta metafor rms pROC',
    'enterprise reporting and reproducible applications': 'shiny bslib shinydashboard flexdashboard plotly DT gt flextable officer rmarkdown knitr bookdown quarto targets renv',
})

NPM = spec_list({
    'front-end application development': 'react react-dom vue svelte angular next nuxt vite webpack rollup esbuild typescript babel-core preact lit solid-js astro electron react-native expo',
    'server and automation workload': 'express fastify koa hapi restify nest bun pm2 nodemon ts-node tsx puppeteer playwright cheerio axios got undici ws socket.io bullmq node-cron',
    'databases analytics and infrastructure': 'prisma sequelize knex typeorm mongoose mariadb sqlite3 better-sqlite3 duckdb apache-arrow kafkajs amqplib cassandra-driver oracledb tedious neo4j-driver rethinkdb couchbase nano influx statsd-client',
    'identity security and observability': 'jsonwebtoken jose bcrypt argon2 helmet passport keycloak-js casbin oauth4webapi prom-client winston pino bunyan log4js promjs node-vault otplib speakeasy',
    'communications commerce and customer platforms': 'resend nodemailer mailgun.js postmark intercom-client hubspot airtable monday-sdk-js notion-client sanity meilisearch typesense directus commerce.js razorpay paytmchecksum mercadopago adyen-node-api-library paystack',
    'robotics geospatial and media workloads': 'three babylonjs pixi.js cesium leaflet ol geolib satellite.js turf sharp jimp fluent-ffmpeg opencv4nodejs johnny-five i2c-bus onoff rpio node-red zigbee-herdsman noble',
})
# Fixed, deliberately varied selections; not an automated scrape of the registry.
NPM = [(package, theme) for package, theme in NPM[:120] if package != 'adyen-node-api-library']

PYPI = spec_list({
    'AI training inference and vector search': 'torch transformers diffusers accelerate vllm ollama sentence-transformers llama-index langchain-core chromadb qdrant-client faiss-cpu onnxruntime triton',
    'data platform and cloud workloads': 'pandas polars pyarrow duckdb pyspark dask ray boto3 google-cloud-bigquery azure-storage-blob snowflake-connector-python databricks-sql-connector dbt-core apache-airflow prefect dagster',
    'scientific industrial and geospatial computing': 'scipy scikit-learn xgboost lightgbm catboost xarray rasterio geopandas astropy biopython opencv-python rdkit',
})

COUNTRIES = dict(x.split(':', 1) for x in '''US:United States|CA:Canada|MX:Mexico|BR:Brazil|AR:Argentina|CL:Chile|CO:Colombia|PE:Peru|GB:United Kingdom|DE:Germany|FR:France|ES:Spain|IT:Italy|NL:Netherlands|PL:Poland|UA:Ukraine|TR:Turkey|RU:Russia|CN:China|JP:Japan|KR:South Korea|TW:Taiwan|IN:India|ID:Indonesia|MY:Malaysia|SG:Singapore|TH:Thailand|VN:Vietnam|PH:Philippines|AU:Australia|NZ:New Zealand|PK:Pakistan|BD:Bangladesh|LK:Sri Lanka|NP:Nepal|IR:Iran|IQ:Iraq|SA:Saudi Arabia|AE:United Arab Emirates|IL:Israel|EG:Egypt|MA:Morocco|DZ:Algeria|NG:Nigeria|ZA:South Africa|KE:Kenya|ET:Ethiopia|GH:Ghana|TZ:Tanzania|CD:Democratic Republic of Congo'''.split('|'))

# Five topics per edition span non-English household and industrial attention.
WIKIPEDIA = []
for lang, topics in {
    'de': ['Inflation','Kurzarbeit','Wärmepumpe','Elektroauto','Erdgas'],
    'fr': ['Inflation','Chômage','Énergie_nucléaire','Voiture_électrique','Assurance'],
    'es': ['Inflación','Desempleo','Litio','Vehículo_eléctrico','Turismo'],
    'pt': ['Inflação','Desemprego','Soja','Café','Minério_de_ferro'],
    'it': ['Inflazione','Disoccupazione','Turismo','Energia_solare','Gas_naturale'],
    'ja': ['インフレーション','半導体','電気自動車','原子力発電','円安'],
    'zh': ['通货膨胀','半导体','房地产','电动汽车','稀土元素'],
    'ko': ['인플레이션','반도체','전기자동차','리튬','관광'],
    'ru': ['Инфляция','Безработица','Природный_газ','Нефть','Золото'],
    'ar': ['تضخم','بطالة','نفط','تحلية_مياه','طاقة_شمسية'],
    'hi': ['मुद्रास्फीति','बेरोज़गारी','सोना','चावल','कपास'],
    'id': ['Inflasi','Pengangguran','Minyak_sawit','Nikel','Pariwisata'],
    'vi': ['Lạm_phát','Thất_nghiệp','Cà_phê','Gạo','Du_lịch'],
    'tr': ['Enflasyon','İşsizlik','Altın','Doğalgaz','Turizm'],
    'pl': ['Inflacja','Bezrobocie','Węgiel_kamienny','Pompa_ciepła','Turystyka'],
    'nl': ['Inflatie','Werkloosheid','Aardgas','Warmtepomp','Halfgeleider'],
}.items():
    WIKIPEDIA.extend((lang, topic, f'{lang} language readership economic and industrial attention') for topic in topics)
WIKIPEDIA.extend(('en', topic, theme) for theme, topics in {
    'industrial bottlenecks and commodities': 'Helium|Sulfur|Potash|Phosphate|Nickel|Tin|Bauxite|Gallium|Germanium|Antimony',
    'consumer adoption and spending': 'Lab-grown_diamond|Plant-based_meat|Air_fryer|Pickleball|Weight_loss|Streaming_media|Cloud_gaming|Contactless_payment|Induction_cooking|Vertical_farming',
    'labor supply and industrial automation': 'Industrial_robot|Warehouse_automation|Autonomous_truck|Precision_agriculture|Reshoring|Nearshoring|Remote_work|Four-day_workweek|Digital_nomad|Child_care',
    'health and infrastructure risk': 'Dengue_fever|Cholera|Pertussis|Malaria|Antimicrobial_resistance|Water_scarcity|Grid_energy_storage|Power_outage|Undersea_cable|Pumped-storage_hydroelectricity',
}.items() for topic in topics.split('|'))


def collect_cran(fetcher, start, end, workers=3):
    specs = CRAN
    results, errors = [], []
    for offset in range(0, len(specs), 20):
        batch = specs[offset:offset+20]
        url = f"https://cranlogs.r-pkg.org/downloads/daily/{start}:{end}/" + ','.join(p for p, _ in batch)
        try:
            raw, digest = get(fetcher, url, interval=1.2)
            obj = {item['package']: item for item in json.loads(raw)}
            for package, theme in batch:
                try:
                    frame = pd.DataFrame(obj[package]['downloads']).rename(columns={'day':'date','downloads':'value'})[['date','value']]
                    meta = save('cran_' + slug(package) + '_downloads', frame, title=f'CRAN RStudio mirror downloads: {package}',
                        provider='CRAN R-hub', category='scientific_software_adoption', unit='package downloads', frequency='daily',
                        source_url=url, geography='Global RStudio/cloud CRAN mirror users', raw_sha256=digest,
                        hypothesis=f'Changes in {package} downloads may monitor {theme}, with potential exposure to cloud compute, analytics vendors and domain-specific investment.',
                        limitations='One popular CRAN mirror only; all-mirror totals are unknown. Downloads include dependency installations, automated builds, repeated installs and education; not unique users or revenue. Present-day package selection creates selection bias. Source may revise logs. No missing-day interpolation.',
                        documentation_url='https://github.com/r-hub/cranlogs.app', native_frequency='daily', dataset_identity=f'cranlogs/daily/{package}', package=package, market_theme=theme)
                    results.append(meta); print(f"OK {meta['series_id']} {meta['rows']}",flush=True)
                except Exception as exc:
                    errors.append({'provider':'cran','candidate':package,'error':str(exc)})
        except Exception as exc:
            errors.append({'provider':'cran','candidate':str(batch),'error':str(exc)})
    return results, errors


def collect_npm(fetcher, start, end, workers=3):
    frames = {p:[] for p,_ in NPM}; hashes=[];urls=[]
    for offset in range(0,len(NPM),60):
        packages = [p for p,_ in NPM[offset:offset+60]]
        for begin,stop in year_windows(start,end):
            url=f'https://api.npmjs.org/downloads/range/{begin}:{stop}/'+','.join(packages)
            raw,digest=get(fetcher,url,interval=2.1)
            obj=json.loads(raw);hashes.append(digest);urls.append(url)
            for package in packages:
                if obj.get(package):frames[package].append(parse_npm(json.dumps(obj[package])))
    def one(spec):
        package,theme=spec
        frame=pd.concat(frames[package],ignore_index=True)
        return save('npm_'+slug(package)+'_downloads',frame,title=f'npm daily downloads: {package}',provider='npm',category='software_adoption',unit='package downloads',frequency='daily',source_url=urls[0],geography='Global',raw_sha256=hashes,
            hypothesis=f'Package activity may monitor {theme}, with potential relevance to developer platforms, infrastructure and enterprise software demand.',
            limitations='Counts include CI builds, dependency installs, bots, retries and version churn. Not users, customers or revenue. Strong calendar seasonality and current package-selection bias. Missing source dates remain absent.',
            documentation_url='https://github.com/npm/registry/blob/main/docs/download-counts.md',native_frequency='daily',dataset_identity=f'npm/downloads/{package}',request_urls=urls,package=package,market_theme=theme)
    return jobs('npm',NPM,one,workers)


def parse_pypi(raw):
    obj=json.loads(raw);frame=pd.DataFrame(obj['data']);frame=frame[frame.category=='without_mirrors']
    return frame[['date','downloads']].rename(columns={'downloads':'value'})


def collect_pypi(fetcher,start,end,workers=2):
    def one(spec):
        package,theme=spec;url=f'https://pypistats.org/api/packages/{quote(package)}/overall'
        raw,digest=get(fetcher,url,{'mirrors':'false'},interval=15.1)
        return save('pypi_'+slug(package)+'_downloads',parse_pypi(raw),title=f'PyPI downloads excluding known mirrors: {package}',provider='Python Software Foundation PyPI Stats',category='python_software_adoption',unit='package downloads',frequency='daily',source_url=url,geography='Global',raw_sha256=digest,
            hypothesis=f'Download activity may monitor {theme}; changes can proxy developer deployment intensity and relevant cloud, AI and enterprise tooling demand.',
            limitations='The API documents a roughly 180-day rolling history; actual returned coverage is recorded separately. Downloads include automated builds, dependencies, retries and bots even after known mirrors are excluded; not unique users or revenue. Current package-selection bias. Refresh must merge retained local history to extend the research record.',
            documentation_url='https://pypistats.org/api/',native_frequency='daily',dataset_identity=f'pypistats/{package}/without_mirrors',package=package,source_parameters={'mirrors':'false'},documented_retention_days=180,market_theme=theme)
    return jobs('pypi',PYPI,one,workers)


def parse_ripe(raw, metric):
    obj=json.loads(raw)
    if obj.get('status')!='ok':raise ValueError(str(obj.get('messages')))
    data=obj['data']
    if data['resolution']!='1d':raise ValueError('RIPE returned non-daily resolution: '+str(data['resolution']))
    rows=[]
    for item in data['stats']:
        value=item.get(metric)
        if value is None or value<0:continue
        for interval in item['timeline']:
            # Keep only timestamps explicitly present in source validity intervals.
            # No intermediate dates are reconstructed or forward-filled.
            for timestamp in dict.fromkeys([interval['starttime'], interval['endtime']]):
                rows.append({'date':timestamp,'value':value})
    frame = pd.DataFrame(rows)
    if not frame.empty and frame['date'].duplicated().any():
        if (frame.groupby('date')['value'].nunique() > 1).any():
            raise ValueError('Conflicting RIPE interval endpoint values')
        frame = frame.drop_duplicates('date')
    return frame


def collect_ripe(fetcher,start,end,workers=3):
    def one(spec):
        code,country=spec;url='https://stat.ripe.net/data/country-routing-stats/data.json'
        params={'resource':code,'starttime':start,'endtime':end,'resolution':'1d'}
        raw,digest=get(fetcher,url,params,interval=1.2,timeout=120)
        out=[]
        for metric,label,unit in [('v4_prefixes_ris','announced IPv4 prefixes','prefixes'),('v6_prefixes_ris','announced IPv6 prefixes','prefixes'),('asns_ris','visible autonomous systems','autonomous systems')]:
            frame=parse_ripe(raw,metric)
            out.append(save('ripe_'+code.lower()+'_'+metric,frame,title=f'RIPE RIS {label}: {country}',provider='RIPE NCC RIS',category='internet_routing',unit=unit,frequency='daily',source_url=url,geography=country,raw_sha256=digest,
                hypothesis='Country internet routing contractions can flag outages, geopolitical interference and connectivity risk; sustained growth can proxy network investment and digitization relevant to telecom, cloud and country exposures.',
                limitations='Routing visibility is from RIS collectors and registration-based geography, not traffic, bandwidth or user counts. Source daily statistics may be fractional due to aggregation. Collector coverage, address transfers and methodological changes affect levels. Some outages occur below BGP visibility. Daily aggregates can hide intraday disruption; the source retains finer recent observations.',
                documentation_url='https://stat.ripe.net/docs/data-api/',native_frequency='BGP collector observations; API daily aggregate',finer_frequency_available=True,dataset_identity=f'RIPE/country-routing-stats/{code}/{metric}/1d',source_parameters=params,source_resolution='1d',timestamp_semantics='UTC source daily observation or explicit validity-interval endpoint label; intermediate dates in compressed intervals are omitted. Not an archive of historical availability.',aggregation='Source resolution=1d retained. Only explicitly returned start/end timestamps retained; no expansion of intervals.'))
        return out
    return jobs('ripe',list(COUNTRIES.items()),one,workers)


def collect_tor(fetcher,start,end,workers=3):
    # Bridge users provide a separate censorship-circumvention measure from the
    # earlier relay-user collection. No previously published country bridge series.
    def one(spec):
        code,country=spec;url='https://metrics.torproject.org/userstats-bridge-country.csv'
        params={'start':start,'end':end,'country':code.lower()}
        raw,digest=get(fetcher,url,params,interval=2.1)
        frame=parse_csv(raw,'users',{'country':code.lower()})
        return save('tor_bridge_users_'+code.lower(),frame,title=f'Estimated Tor bridge users: {country}',provider='Tor Project',category='internet_resilience',unit='estimated users',frequency='daily',source_url=url,geography=country,raw_sha256=digest,
            hypothesis='Country-level privacy-network bridge use can help detect censorship, disruptions and changes in demand for circumvention tools relevant to country and connectivity risk.',
            limitations='Estimated users, not a census. Geolocation, reporting fractions, protocol adoption and statistical methodology produce level shifts. Privacy use has many benign motivations and is not itself evidence of political unrest or cybercrime. No interpolation of unreported dates.',
            documentation_url='https://metrics.torproject.org/stats.html',native_frequency='daily',dataset_identity=f'Tor/userstats-bridge-country/{code.lower()}/users',source_parameters=params)
    return jobs('tor',list(COUNTRIES.items()),one,workers)


def collect_wikipedia(fetcher,start,end,workers=3):
    def one(spec):
        lang,article,theme=spec
        url=('https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/'
             f'{lang}.wikipedia.org/all-access/user/{quote(article,safe="")}/daily/{start.replace("-","")}00/{end.replace("-","")}00')
        raw,digest=get(fetcher,url,interval=5.1)
        # Unicode-only page titles need an ASCII stable identifier.
        article_slug=slug(article) or hashlib.sha256(article.encode()).hexdigest()[:12]
        if len(article_slug)<3:article_slug=hashlib.sha256(article.encode()).hexdigest()[:12]
        sid=f'wikipedia_{lang}_{article_slug}_views'
        return save(sid,parse_wikimedia(raw),title=f'Wikipedia human page views ({lang}): {article.replace("_"," ")}',provider='Wikimedia',category='multilingual_attention' if lang!='en' else 'attention',unit='page views',frequency='daily',source_url=url,geography=f'Global readers of {lang} Wikipedia',raw_sha256=digest,
            hypothesis=f'Reader research and news attention may monitor {theme}; compare attention changes with commodity, consumption, industrial or country exposures.',
            limitations='Attention is not sentiment, orders or revenue. Language is not country residency. News, education, redirects and bots can dominate. User-classified traffic excludes identified automated requests only. Current topic and language selection create selection bias. Redirect titles are not merged; absent dates remain absent.',
            documentation_url='https://doc.wikimedia.org/generated-data-platform/aqs/analytics-api/reference/page-views.html',native_frequency='daily',dataset_identity=f'pageviews/{lang}.wikipedia.org/all-access/user/{article}',article=article,language=lang,market_theme=theme)
    return jobs('wikipedia',WIKIPEDIA,one,workers)


FDA_GROUPS = [
    ('drug_classes','drug/event','receivedate','patient.drug.openfda.pharm_class_epc.exact',45,'drug_safety','pharmacologic class'),
    ('drug_reactions','drug/event','receivedate','patient.reaction.reactionmeddrapt.exact',35,'drug_safety','reported reaction'),
    ('device_harmonized','device/event','date_received','device.openfda.device_name.exact',45,'device_safety','FDA harmonized device type'),
    ('food_industries','food/event','date_created','products.industry_name.exact',20,'food_safety','food industry'),
]


def parse_fda(raw):
    obj=json.loads(raw)
    return pd.DataFrame([{'date':pd.to_datetime(row['time'],format='%Y%m%d',utc=True),'value':row['count']} for row in obj['results']])


def collect_fda(fetcher,start,end,workers=2):
    specs=[];errors=[]
    for label,endpoint,datefield,field,count,category,dimension in FDA_GROUPS:
        url=f'https://api.fda.gov/{endpoint}.json'
        selected=fetcher.root/'archive/digital2_selection'/f'fda_{label}.json'
        # Keep research universe stable across refreshes; archive discovery evidence.
        if selected.exists():
            selection=json.loads(selected.read_text())
        else:
            params={'count':field,'limit':count,'search':f'{datefield}:[{start.replace("-","")} TO {end.replace("-","")}]'}
            try:
                raw,digest=get(fetcher,url,params,interval=1.2)
                selection={'terms':[row['term'] for row in json.loads(raw)['results']], 'raw_sha256':digest,'source_parameters':params}
                atomic_bytes(selected,json.dumps(selection,indent=2).encode())
            except Exception as exc:
                errors.append({'provider':'fda','candidate':label,'error':str(exc)});continue
        seen_terms = set()
        for term in selection.get('eligible_terms', selection['terms']):
            normalized = slug(term)
            if normalized in seen_terms or normalized in {'no_match', 'unknown', 'other'}:
                continue
            seen_terms.add(normalized)
            specs.append((label,url,datefield,field,category,dimension,term,selection))
    def one(spec):
        label,url,datefield,field,category,dimension,term,selection=spec
        escaped=term.replace('\\','\\\\').replace('"','\\"')
        params={'count':datefield,'search':f'{field}:"{escaped}" AND {datefield}:[{start.replace("-","")} TO {end.replace("-","")}]'}
        raw,digest=get(fetcher,url,params,interval=1.2,timeout=120);obj=json.loads(raw)
        sid='fda_'+label+'_'+slug(term)
        return save(sid,parse_fda(raw),title=f'FDA adverse-event reports by {dimension}: {term}',provider='US FDA openFDA',category=category,unit='adverse-event reports',frequency='daily',source_url=url,geography='Reports received by US FDA; may include foreign events',raw_sha256=[digest,selection['raw_sha256']],
            hypothesis='Reported safety-event volume and unexpected changes may provide research leads for product liability, recall exposure, medical-device reliability, drug scrutiny and food-category risk affecting relevant manufacturers and insurers.',
            limitations='Reports do not establish causality, incidence or medical risk; exposure denominators and under-reporting are unknown. Multiple products/reactions can appear in one report, so category series overlap and must not be summed. Media effects and litigation can drive reporting. The current database includes revisions and only latest report versions; received date is not publication availability. FDA updates are lagged and the data is not suitable for medical decisions. Harmonized device nomenclature avoids inconsistent manufacturer-entered type spellings; historical classifications can still be revised. Only returned source date bins are retained; no missing-day fill.',
            documentation_url='https://open.fda.gov/apis/timeseries/',native_frequency='report-level dates aggregated daily by the source',finer_frequency_available=False,dataset_identity=f'openFDA/{url.split("api.fda.gov/")[1]}/{field}={term}/count={datefield}',source_parameters=params,source_last_updated=obj['meta'].get('last_updated'),source_release_frequency='weekly' if category=='device_safety' else 'quarterly',availability_documentation_url=url.replace('https://api.fda.gov/','https://open.fda.gov/apis/').replace('.json','/'),selection_parameters=selection['source_parameters'],date_field=datefield,term=term,
            timestamp_semantics=f'Source {datefield} calendar date; not time of incident or first public availability. Retrospectively revised histories can create look-ahead bias.')
    good,bad=jobs('fda',specs,one,workers)
    return good,errors+bad


PROVIDERS=['cran','npm','pypi','ripe','tor','wikipedia','fda']
def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--provider',choices=PROVIDERS+['all'],default='all')
    parser.add_argument('--start',default='2024-01-01')
    parser.add_argument('--end',default=(pd.Timestamp.now(tz='UTC').normalize()-pd.Timedelta(days=2)).date().isoformat())
    parser.add_argument('--refresh',action='store_true')
    parser.add_argument('--workers',type=int,default=3)
    args=parser.parse_args();fetcher=Fetcher(refresh=args.refresh);good=[];bad=[]
    for provider in PROVIDERS:
        if args.provider not in ['all',provider]:continue
        try:
            result,errors=globals()['collect_'+provider](fetcher,args.start,args.end,args.workers);good.extend(result);bad.extend(errors)
        except Exception as exc:
            bad.append({'provider':provider,'error':str(exc)});print('FAIL provider '+provider+': '+str(exc),flush=True)
    report={'created_at':pd.Timestamp.now(tz='UTC').isoformat(),'provider':args.provider,'successful_series':[m['series_id'] for m in good],'count':len(good),'errors':bad}
    atomic_bytes(ROOT/'archive/digital2_reports'/f'{args.provider}.json',json.dumps(report,indent=2).encode())
    print(json.dumps({'count':len(good),'errors':len(bad)}),flush=True)
    return int(bool(bad))
if __name__=='__main__':raise SystemExit(main())
