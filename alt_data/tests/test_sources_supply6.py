import io
import json
import unittest
from unittest.mock import patch
import numpy as np
import pandas as pd
from sources_supply6 import parse_eia_petroleum, parse_eia_gas, parse_india_coal, parse_enagas, ENAGAS_FIELDS, Access, parse_cenagas, parse_cenagas_excel


def workbook(sheets):
    out=io.BytesIO()
    with pd.ExcelWriter(out,engine='openpyxl') as w:
        for name,rows in sheets.items(): pd.DataFrame(rows).to_excel(w,sheet_name=name,header=False,index=False)
    return out.getvalue()

class SupplyParsers(unittest.TestCase):
    def test_eia_weekly_not_four_week_or_stock_change(self):
        data=[['Back to Contents','Data 1'],['Sourcekey','WK_STOCK','WK_CHANGE'],['Date','Weekly stock (Thousand Barrels)','Weekly stock change (Thousand Barrels)'],[pd.Timestamp('2025-01-03'),0,2],[pd.Timestamp('2025-01-10'),'W',3],[pd.Timestamp('2025-01-17'),7,4]]
        b=workbook({'Contents':[['Data 1','Weekly'],['Data 2','4-Week']], 'Data 1':data,'Data 2':data})
        r=parse_eia_petroleum(b)
        self.assertEqual(len(r),1)
        self.assertEqual(r[0][2],'Thousand Barrels')
        self.assertEqual(r[0][4].value.tolist(),[0,7])
        self.assertEqual(r[0][4].date.dt.strftime('%Y-%m-%d').tolist(),['2025-01-03','2025-01-17'])
    def test_gas_excludes_backcast_not_survey_observations(self):
        rows=[[None]*3 for _ in range(6)] + [['Week ending','Source','East Region'],[pd.Timestamp('2015-06-12'),'Derived EIA Weekly Estimates',90],[pd.Timestamp('2015-06-19'),'EIA-912',0],[pd.Timestamp('2015-06-26'),'EIA-912',91]]
        r=parse_eia_gas(workbook({'html_report_history':rows}))
        self.assertEqual(r[0][1].value.tolist(),[0,91])
    def coal_book(self, missing_capacity=False):
        rows=[['CENTRAL ELECTRICITY AUTHORITY'],[],['(AS ON 01-01-2025)'],['Sl. No./State','Name of Thermal Power Station','Capacity (MW)','Actual Stock','Receipt of the day','Consumption of the day'],[None,None,None,'Total'],[]]
        rows += [[i,'PLANT '+str(i),None if missing_capacity and i==1 else 500,0 if i==1 else 20,None if i==1 else 2,3] for i in range(1,102)]
        return workbook({'dailyCoalReport':rows})
    def test_coal_zero_not_blank_and_unit_column_selection(self):
        d=parse_india_coal(self.coal_book(),'2025-01-01')
        self.assertEqual(len(d),302)
        self.assertEqual(d[(d.plant=='PLANT 1') & (d.metric=='stock_total')].value.item(),0)
        self.assertFalse(((d.plant=='PLANT 1') & (d.metric=='receipts')).any())
    def test_coal_missing_capacity_does_not_drop_reported_quantities(self):
        d=parse_india_coal(self.coal_book(missing_capacity=True),'2025-01-01')
        self.assertEqual(len(d),302)
        self.assertEqual(d[(d.plant=='PLANT 1') & (d.metric=='consumption')].value.item(),3)
    def test_coal_date_mismatch_fails(self):
        with self.assertRaisesRegex(ValueError,'date mismatch'):parse_india_coal(self.coal_book(),'2025-01-02')
    def test_enagas_no_invented_zeros_or_alternate_unit_aliases(self):
        gas={key:{'Terminal A':'0','Terminal B':None} for key in ENAGAS_FIELDS}
        gas.update(tankFinalStocksGwh={'Terminal A':'99'},tankInitialStocksM3={'Terminal A':'100'},aass={'Exist. Comerciales Finales':'900'},gasPipelines={'Final':'123'})
        d,last=parse_enagas(json.dumps({'gasSystemPPDDataReportPojo':gas,'lastDate':'02/01/2025 04:47'}).encode(),'2025-01-01')
        self.assertEqual(len(d),6)
        self.assertEqual(last,'02/01/2025 04:47')
        self.assertNotIn('tankInitialStocksM3',set(d.group))
        self.assertNotIn('tankFinalStocksGwh',set(d.group))
        self.assertEqual(d[d.group=='tankFinalStocksM3'].value.item(),0)
    def test_enagas_excludes_existing_entsog_physical_flow_measurements(self):
        gas={key:{'Terminal A':'1'} for key in ENAGAS_FIELDS}
        gas.update(regasificationGWh={'Terminal A':'100','Total Plantas':'200'},
                   aass={'Exist. Comerciales Finales':'900','Entradas desde RT':'70','Salidas a RT':'80'},
                   transferNetInflows={'Yac. Nacion.':'60','BiometanoDistribucion':'4','BiometanoTransporte':'5'})
        d,_=parse_enagas(json.dumps({'gasSystemPPDDataReportPojo':gas}).encode(),'2025-01-01')
        self.assertNotIn('regasificationGWh',set(d.group))
        self.assertEqual(set(d[d.group=='aass'].entity),{'Exist. Comerciales Finales'})
        self.assertEqual(set(d[d.group=='transferNetInflows'].entity),{'BiometanoDistribucion','BiometanoTransporte'})
    def test_cenagas_preserves_positive_native_days_and_omits_ambiguous_zeros(self):
        obj={'nodoComercial':'V001 TEST','fechas':['2025-01-01','2025-01-02','2025-01-03'],'valores':[0,None,17.25]}
        d,_,zeros=parse_cenagas(json.dumps(obj).encode(),'V001 TEST')
        self.assertEqual(zeros,1)
        self.assertEqual(d.value.tolist(),[17.25])
        self.assertEqual(d.date.dt.strftime('%Y-%m-%d').tolist(),['2025-01-03'])
    def test_cenagas_node_mismatch_and_duplicate_dates_fail(self):
        obj={'nodoComercial':'V001','fechas':['2025-01-01','2025-01-01'],'valores':[1,2]}
        with self.assertRaisesRegex(ValueError,'node response mismatch'):parse_cenagas(json.dumps(obj).encode(),'V002')
        with self.assertRaisesRegex(ValueError,'duplicate source dates'):parse_cenagas(json.dumps(obj).encode(),'V001')
    def test_cenagas_excel_checks_native_schema_and_duplicate_identity(self):
        raw=workbook({'Extracciones':[['Fecha','Nodo Comercial','Zona','Cantidad'],[pd.Timestamp('2025-01-01'),'E001 TEST','Zona 1',17.25],[pd.Timestamp('2025-01-02'),'E001 TEST','Zona 1',0]]})
        d=parse_cenagas_excel(raw,'Extracciones')
        self.assertEqual(d.value.tolist(),[17.25,0])
        bad=workbook({'Extracciones':[['Fecha','Nodo Comercial','Zona','Cantidad'],[pd.Timestamp('2025-01-01'),'E001 TEST','Zona 1',17.25],[pd.Timestamp('2025-01-01'),'E001 TEST','Zona 1',20]]})
        with self.assertRaisesRegex(ValueError,'duplicate node/date'):parse_cenagas_excel(bad,'Extracciones')
    def test_provider_denial_stops_subsequent_calls(self):
        import requests
        response=requests.Response();response.status_code=403
        access=Access(interval=0)
        with patch.object(access.fetcher,'get',side_effect=requests.HTTPError('denied',response=response)) as get:
            with self.assertRaises(requests.HTTPError):access.get('https://example.com')
            with self.assertRaisesRegex(RuntimeError,'stopped'):access.get('https://example.com/next')
            self.assertEqual(get.call_count,1)

if __name__=='__main__':unittest.main()
