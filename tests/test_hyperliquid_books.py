"""Book conversion preserves values and nulls while standardizing field names."""
import importlib.util
from pathlib import Path
import sys

import pyarrow as pa

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
spec = importlib.util.spec_from_file_location('hyperliquid_books', ROOT / 'download_perpetuals_hyperliquid.py')
app = importlib.util.module_from_spec(spec)
spec.loader.exec_module(app)


def test_levels_preserve_values_nulls_and_slices():
    native = pa.list_(pa.struct([('px', pa.string()), ('sz', pa.string()), ('n', pa.uint32())]))
    values = pa.array([[{'px': '12.25', 'sz': '0.125', 'n': 3}], None, [],
                       [None, {'px': None, 'sz': '2', 'n': None}]], type=native)
    expected = [[{'price': 12.25, 'size': 0.125, 'n': 3}], None, [],
                [None, {'price': None, 'size': 2.0, 'n': None}]]
    assert app.convert_levels(values).to_pylist() == expected
    assert app.convert_levels(values.slice(1)).to_pylist() == expected[1:]
    assert app.convert_levels(pa.chunked_array([values.slice(0, 2), values.slice(2)])).to_pylist() == expected


def test_snapshot_timestamps_and_tickers_are_preserved():
    levels = [[{'px': '100', 'sz': '0.5', 'n': 2}]]
    source = pa.table({'block_time_ms': [1234], 'block_number': [42],
                       'bids': levels, 'asks': levels})
    result = app.convert_book([('BTC', source)])
    assert result['ticker'].to_pylist() == ['BTC']
    assert result['block_time'].cast(pa.int64()).to_pylist() == [1234000000]
    assert result['bids'].to_pylist() == [[{'price': 100.0, 'size': 0.5, 'n': 2}]]
