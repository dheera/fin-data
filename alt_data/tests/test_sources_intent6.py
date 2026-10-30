import io,sys,unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from sources_intent6 import parse_indeed,parse_trends,clean,Omission

class IntentParsers(unittest.TestCase):
    def test_indices_remain_indices_and_dimensions_remain_distinct(self):
        raw=b'date,jobcountry,indeed_job_postings_index_SA,indeed_job_postings_index_NSA,variable\n2020-02-01,AU,100,100,total postings\n2020-02-02,AU,98,99,total postings\n2020-02-01,AU,100,100,new postings\n2020-02-02,AU,92,93,new postings\n'
        results=list(parse_indeed(raw));self.assertEqual(len(results),4)
        total_sa=next(f for d,c,f in results if d['variable']=='total postings' and c.endswith('_SA'))
        self.assertEqual(total_sa.value.tolist(),[100.,98.])
        self.assertEqual(str(total_sa.date.dtype),'datetime64[ns, UTC]')
    def test_conflicting_dates_fail(self):
        with self.assertRaisesRegex(ValueError,'Conflicting'):clean(['2020-01-01']*2,[1,2])
    def test_missing_is_not_zero(self):
        f=clean(['2020-01-01','2020-01-02','2020-01-03'],[1,None,3])
        self.assertEqual(f.value.tolist(),[1,3]);self.assertEqual(len(f),2)
    def test_empty_trends_is_not_zero_attention(self):
        with self.assertRaises(Omission):parse_trends(b'Trends,Search volume,Started,Trend breakdown\n')
    def test_trends_count_clusters_not_bucketed_search_volume(self):
        raw='Trends,Search volume,Started,Trend breakdown\nbitcoin,50K+,first,bitcoin price\nbitcoin,50K+,first,bitcoin price\nbitcoin,2M+,second,bitcoin price\nbanjir,10K+,first,banjir jakarta\n'.encode()
        r=parse_trends(raw);self.assertEqual(r['listed_clusters'],3);self.assertEqual(r['markets'],2);self.assertEqual(r['weather_disruption'],1)
        self.assertEqual(r['shopping'],0)
    def test_multilingual_topic_rule_does_not_imply_sentiment(self):
        raw='Trends,Search volume,Started,Trend breakdown\n환율,1K+,first,환율\ndespidos,200+,second,despidos\n'.encode()
        r=parse_trends(raw);self.assertEqual(r['markets'],1);self.assertEqual(r['labor'],1)

if __name__=='__main__':unittest.main()
