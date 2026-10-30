import tempfile,unittest,sys
from pathlib import Path
from unittest.mock import patch
import pandas as pd
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import test_library
from library import publish,publish_staged,build_catalog,load_collected_asof,read_metadata
import pyarrow.parquet as pq
from refresh import promote_existing

class LocalAvailabilityTests(unittest.TestCase):
    def test_backfilled_history_is_not_available_before_collection(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);test_library.LibraryTests().stage(root)
            with patch('library._collection_clock',return_value='2026-01-10T12:00:00+00:00'):publish_staged(1,8,root)
            with self.assertRaises(LookupError):load_collected_asof('sample','2026-01-09T23:59Z',root)
            with self.assertRaises(ValueError):load_collected_asof('sample','2026-01-11',root)
            self.assertEqual(load_collected_asof('sample','2026-01-10T12:00Z',root).value.iloc[0],0)
    def test_revision_and_unchanged_refresh_preserve_prior_numeric_version(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);helper=test_library.LibraryTests();helper.stage(root)
            with patch('library._collection_clock',return_value='2026-01-10T12:00:00+00:00'):publish_staged(1,8,root)
            helper.stage(root,offset=1000)
            with patch('library._collection_clock',return_value='2026-01-12T12:00:00+00:00'):promote_existing(root)
            build_catalog(root)
            self.assertEqual(load_collected_asof('sample','2026-01-11T00:00Z',root).value.iloc[0],0)
            self.assertEqual(load_collected_asof('sample','2026-01-13T00:00Z',root).value.iloc[0],1000)
            with patch('library._collection_clock',return_value='2026-01-14T12:00:00+00:00'):promote_existing(root)
            self.assertEqual(read_metadata(root/'data/test/sample.parquet')['locally_available_at'],'2026-01-12T12:00:00+00:00')
    def test_untracked_legacy_history_fails_closed(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);test_library.LibraryTests().stage(root);publish_staged(1,7,root)
            with self.assertRaises(LookupError):load_collected_asof('sample','2026-01-10T12:00Z',root)
    def test_recurring_numeric_state_keeps_each_availability_event(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);helper=test_library.LibraryTests();helper.stage(root)
            with patch('library._collection_clock',return_value='2026-01-10T12:00:00+00:00'):publish_staged(1,8,root)
            a_meta=read_metadata(root/'data/test/sample.parquet');a=pd.read_parquet(root/a_meta['path'])
            b_meta=helper.stage(root,offset=1000);b_meta.update(batch=8,status='ready');b=pd.read_parquet(root/'archive/staging/test/sample.parquet')
            c_meta=helper.stage(root,offset=2000);c_meta.update(batch=8,status='ready');c=pd.read_parquet(root/'archive/staging/test/sample.parquet')
            for stamp,frame,meta in [('2026-01-12T12:00:00Z',b,b_meta),('2026-01-14T12:00:00Z',a,a_meta),('2026-01-16T12:00:00Z',c,c_meta)]:
                with patch('library._collection_clock',return_value=stamp):publish(frame,meta,root)
            build_catalog(root)
            self.assertEqual(load_collected_asof('sample','2026-01-11T00:00Z',root).value.iloc[0],0)
            self.assertEqual(load_collected_asof('sample','2026-01-13T00:00Z',root).value.iloc[0],1000)
            self.assertEqual(load_collected_asof('sample','2026-01-15T00:00Z',root).value.iloc[0],0)
    def test_atomic_replacement_between_metadata_and_values_cannot_leak(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);helper=test_library.LibraryTests();helper.stage(root)
            with patch('library._collection_clock',return_value='2026-01-10T12:00:00Z'):publish_staged(1,8,root)
            b_meta=helper.stage(root,offset=1000);b_meta.update(batch=8,status='ready');b=pd.read_parquet(root/'archive/staging/test/sample.parquet')
            original=pq.ParquetFile.read;swapped=[]
            def swap_and_read(parquet,*args,**kwargs):
                if not swapped:
                    swapped.append(True)
                    with patch('library._collection_clock',return_value='2026-01-12T12:00:00Z'):publish(b,b_meta,root)
                return original(parquet,*args,**kwargs)
            with patch.object(pq.ParquetFile,'read',swap_and_read):
                result=load_collected_asof('sample','2026-01-11T00:00Z',root)
            self.assertEqual(result.value.iloc[0],0)
    def test_current_is_opened_before_archives_are_listed(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);helper=test_library.LibraryTests();helper.stage(root)
            with patch('library._collection_clock',return_value='2026-01-08T12:00:00Z'):publish_staged(1,8,root)
            b_meta=helper.stage(root,offset=1000);b_meta.update(batch=8,status='ready');b=pd.read_parquet(root/'archive/staging/test/sample.parquet')
            with patch('library._collection_clock',return_value='2026-01-10T12:00:00Z'):publish(b,b_meta,root)
            c_meta=helper.stage(root,offset=2000);c_meta.update(batch=8,status='ready');c=pd.read_parquet(root/'archive/staging/test/sample.parquet')
            original=Path.open;swapped=[];current=root/'data/test/sample.parquet'
            def swap_then_open(path,*args,**kwargs):
                if path==current and not swapped:
                    swapped.append(True)
                    with patch('library._collection_clock',return_value='2026-01-12T12:00:00Z'):publish(c,c_meta,root)
                return original(path,*args,**kwargs)
            with patch.object(Path,'open',swap_then_open):
                result=load_collected_asof('sample','2026-01-11T00:00Z',root)
            self.assertEqual(result.value.iloc[0],1000)

if __name__=='__main__':unittest.main()
