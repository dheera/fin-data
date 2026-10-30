import io,json,unittest,zipfile
import pandas as pd
import sources_foreign7 as s

class KPXTests(unittest.TestCase):
 def fixture(self,total_short=False):
  vals={'y_time':[20250101000000,20250101000500],'t_time':[20250102000000,20250102000500],'x':[50,51],'y':[52,53],'v':[54] if total_short else [54,55],'w':[56,57]}
  return '\n'.join('var '+k+' = '+json.dumps(v)+';' for k,v in vals.items()).encode()
 def test_timezone_and_day_direction(self):
  d=s.parse_kpx(self.fixture(),['2025-01-01','2025-01-02']);f=d['market_demand'];self.assertEqual(f.iloc[0].date,pd.Timestamp('2024-12-31 15:00Z'));self.assertEqual(f.value.tolist(),[52,53,50,51])
 def test_unaligned_total_omitted_but_market_kept(self):
  d=s.parse_kpx(self.fixture(True),['2025-01-01','2025-01-02']);self.assertEqual(len(d['market_demand']),4);self.assertEqual(len(d['estimated_total_demand']),2)
 def test_wrong_query_rejected(self):
  with self.assertRaisesRegex(ValueError,'ignored'):s.parse_kpx(self.fixture(),['2024-01-01','2024-01-02'])
 def test_generation_native_time_and_negative_pumping(self):
  raw=('var ictArr = '+json.dumps([{'once':'20250101000000','raisingWater':'-5.2','sunlight':'0'}])+';').encode();d=s.parse_kpx_generation(raw,'2025-01-01');self.assertEqual(d['raisingWater'].value.tolist(),[-5.2]);self.assertEqual(d['sunlight'].value.tolist(),[0])

 def test_generation_rejects_non_native_clock(self):
  raw=('var ictArr = '+json.dumps([{'once':'20250101000100','gas':'5'}])+';').encode()
  with self.assertRaisesRegex(ValueError,'clock'):s.parse_kpx_generation(raw,'2025-01-01')

class IEXTests(unittest.TestCase):
 def fixture(self,period='00:00-00:15',duplicate=False):
  row={'date':'01-01-2025','period':period,'purchase_bid':'10.25','mcp':'5000'};payload='0:'+json.dumps({'reportData':{'records':[row,row] if duplicate else [row]}})
  return ('<script>self.__next_f.push('+json.dumps([1,payload])+')</script>').encode()
 def test_interval_start_ist(self):
  df=s.parse_iex(self.fixture(),'2025-01-01','2025-01-31');self.assertEqual(df.date.iloc[0],pd.Timestamp('2024-12-31 18:30Z'))
 def test_whitespace_in_source_time_block(self):
  df=s.parse_iex(self.fixture('00:00 - 00:15'),'2025-01-01','2025-01-31');self.assertEqual(df.date.iloc[0],pd.Timestamp('2024-12-31 18:30Z'))
 def test_invalid_window_and_duplicate(self):
  for b in [self.fixture('00:00-00:30'),self.fixture('25:00-25:15'),self.fixture('00:61-01:16'),self.fixture(duplicate=True)]:
   with self.assertRaises(ValueError):s.parse_iex(b,'2025-01-01','2025-01-31')
 def test_wrong_period_rejected(self):
  with self.assertRaisesRegex(ValueError,'ignored'):s.parse_iex(self.fixture(),'2024-01-01','2024-01-31')

class CenaceTests(unittest.TestCase):
 def fixture(self,ambiguous=False):
  buf=io.BytesIO()
  with zipfile.ZipFile(buf,'w') as z:
   for liquidation,val in [(0,101.5),(1,201.5)]:
    rows='"LIQUIDACION '+str(liquidation)+' (Dia de Operacion: 01/01/2025)"\nSistema, Area, Hora, Generacion (MWh), Importacion Total (MWh)\nSIN,CEN,1,'+str(val)+',---\nSIN,CEN,'+('25' if ambiguous else '24')+',5,0\n'
    z.writestr(f'Demanda Real Balance_{liquidation}_v3 Dia Operacion 2025-01-01 v2025 01 15_12 25 01.csv',rows)
  return buf.getvalue()
 def test_original_settlement_and_hour_labels(self):
  df,refs=s.parse_cenace(self.fixture(),'2025-01-01','2025-01-31');g=df[df.metric.eq('generation')];self.assertEqual(g.value.tolist(),[101.5,5]);self.assertEqual(g.date.iloc[1],pd.Timestamp('2025-01-01 23:00Z'));self.assertEqual(len(refs),1);self.assertEqual(df[df.metric.eq('imports')].value.tolist(),[0])
 def test_out_of_range_clock_not_guessed(self):
  with self.assertRaisesRegex(ValueError,'No CENACE'):s.parse_cenace(self.fixture(True),'2025-01-01','2025-01-31')
 def test_wrong_archive_dates_rejected(self):
  with self.assertRaisesRegex(ValueError,'ignored'):s.parse_cenace(self.fixture(),'2024-01-01','2024-01-31')

if __name__=='__main__':unittest.main()
