"""Original FRED collection definitions; the populated library catalog is catalog.parquet."""
GROUPS = {
    'travel': ['CUUR0000SETG01', 'PCU481111481111', 'LOADFACTOR', 'RPM', 'ASM'],
    'energy': ['DCOILWTICO','DCOILBRENTEU','DHHNGSP','DHOILNYH','DJFUELUSGULF','DGASUSGULF','DPROPANEMBTX','GASREGW','GASDESW'],
    'commodities': ['PCOPPUSDM','PALUMUSDM','PNICKUSDM','PZINCUSDM','PLEADUSDM','PTINUSDM','PIORECRUSDM','PCOALAUUSDM','PMAIZMTUSDM','PWHEAMTUSDM','PSOYBUSDM','PRICENPQUSDM','PCOFFOTMUSDM','PCOCOUSDM','PSUGAISAUSDM','PCOTTINDUSDM','PRUBBUSDM'],
    'food_prices': ['APU0000708111','APU0000709112','APU0000702111','APU0000703112','APU0000711211','APU0000712311'],
    'freight': ['TSIFRGHT','RAILFRTCARLOADSD11','RAILFRTINTERMODALD11','PCU484121484121','PCU492110492110','PCU483111483111'],
    'housing': ['HOUST','PERMIT','HSN1F','EXHOSLUSM495S','MSACSR','CSUSHPINSA','MORTGAGE30US','MORTGAGE15US'],
    'labor': ['ICSA','CCSA','UNRATE','PAYEMS','JTSJOL','JTSQUR','AWHAETP','CES0500000003'],
    'consumer': ['RSAFS','RRSFS','TOTALSA','UMCSENT','PSAVERT','REVOLSL','PCE','DSPIC96'],
    'industry': ['INDPRO','TCU','DGORDER','NEWORDER','AMTMNO','BUSINV','ISRATIO','IPG3344S'],
    'financial_conditions': ['STLFSI4','NFCI','BAMLH0A0HYM2','BAMLC0A0CM','T10YIE','T5YIFR','DTWEXBGS','VIXCLS'],
}
CATALOG = [dict(series_id=f'fred_{symbol.lower()}', source='fred', symbol=symbol,
                category=category, url=f'https://fred.stlouisfed.org/series/{symbol}')
           for category, symbols in GROUPS.items() for symbol in symbols]
