import io
import json
from pathlib import Path
import sys
import zipfile

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from sources_physical11 import (fiscal_week_start, parse_eskom, parse_pegel,
                                persist, select_eskom_attachments, SourceFormatOmission)


def test_pegel_preserves_dst_instants_and_omits_null_measurements():
    raw = json.dumps([
        {"timestamp": "2025-10-26T02:00:00+02:00", "value": 100},
        {"timestamp": "2025-10-26T02:00:00+01:00", "value": 101},
        {"timestamp": "2025-10-26T02:15:00+01:00", "value": None},
    ]).encode()
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("gauge.json", raw)
    frame = parse_pegel(buf.getvalue())
    assert list(frame.value) == [100, 101]
    assert list(frame.date) == [pd.Timestamp("2025-10-26T00:00Z"), pd.Timestamp("2025-10-26T01:00Z")]


def test_pegel_rejects_ambiguous_time_conflicts_and_forecast_schema():
    with pytest.raises(ValueError, match="explicit"):
        parse_pegel(b'[{"timestamp":"2025-01-01T01:00:00","value":10}]')
    with pytest.raises(ValueError, match="Conflicting"):
        parse_pegel(b'[{"timestamp":"2025-01-01T00:00:00Z","value":10},{"timestamp":"2025-01-01T00:00:00Z","value":11}]')
    with pytest.raises(ValueError, match="schema changed"):
        parse_pegel(b'[{"timestamp":"2025-01-01T00:00:00Z","value":10,"initialized":"2024-12-31"}]')


def test_eskom_excludes_shifted_prior_year_comparisons_and_forecasts():
    data = b"""WeekStartDate,Forecast_Or_Actual_Value,Forecast_Or_Actual_Value_Legend
2026-03-30,10,2026-27-Residual Peak Demand
2026-03-30,99,2025-26-Residual Peak Demand
2026-04-06,11,2026-27-Residual Peak Demand
2026-04-06,12,2026-27-Residual Peak Forecast
"""
    parsed = parse_eskom(data, "weekly_peak")
    assert list(parsed) == ["Residual Peak Demand"]
    assert list(parsed["Residual Peak Demand"].value) == [10, 11]
    assert parsed["Residual Peak Demand"].date.iloc[0] == pd.Timestamp("2026-03-29T22:00Z")
    assert fiscal_week_start(2026) == pd.Timestamp("2026-03-30")
    assert fiscal_week_start(2022) == pd.Timestamp("2022-04-04")


def test_eskom_capacity_factors_must_reconcile():
    good = b"Week Start Date,Weekly EAF,Weekly PCLF,Weekly UCLF,Weekly OCLF,Year,Fin_Year\n2025-04-07,60,15,24,1,2025,2025-26\n"
    parsed = parse_eskom(good, "weekly_capacity")
    assert set(parsed) == {"Weekly EAF", "Weekly PCLF", "Weekly UCLF", "Weekly OCLF"}
    with pytest.raises(ValueError, match="100-percent"):
        parse_eskom(good.replace(b",60,", b",70,"), "weekly_capacity")


def test_legacy_semicolon_decimal_comma_is_unambiguous():
    data = b"Date_Time_Hour_Beginning;Thermal_Generation;Pumped_Water_Generation\n2025-01-01 00:00:00;20000,25;-30,5\n"
    parsed = parse_eskom(data, "generation")
    assert parsed["Thermal_Generation"].value.iloc[0] == 20000.25
    assert parsed["Pumped_Water_Generation"].value.iloc[0] == -30.5


def test_legacy_single_value_can_be_recovered_but_multicolumn_cannot():
    data = b"WeekStartDate,Forecast_Or_Actual_Value,Forecast_Or_Actual_Value_Legend\n07-Apr-2025,4000,125,2025-26-Residual Energy Demand\n"
    assert parse_eskom(data, "weekly_energy")["Residual Energy Demand"].value.iloc[0] == 4000.125
    ambiguous = b"Date_Time_Hour_Beginning,Thermal_Generation,Pumped_Water_Generation\n2025-01-01 00:00:00,20000,25,30\n"
    with pytest.raises(SourceFormatOmission, match="Ambiguous"):
        parse_eskom(ambiguous, "generation")


def test_eskom_hourly_preserves_negative_pumping_and_timezone():
    data = b"Date_Time_Hour_Beginning,Thermal_Generation,Pumped_Water_Generation\n2025-01-01 00:00:00,20000,-30\n2025-01-01 01:00:00,21000,100\n"
    parsed = parse_eskom(data, "generation")
    assert parsed["Pumped_Water_Generation"].value.iloc[0] == -30
    assert parsed["Thermal_Generation"].date.iloc[0] == pd.Timestamp("2024-12-31T22:00Z")


def test_attachment_selection_does_not_combine_intra_day_duplicates():
    items = [dict(id=i, date_gmt="2025-01-01T01:00:00", modified_gmt=f"2025-01-01T0{i}:00:00") for i in [1, 2]]
    assert [r["id"] for r in select_eskom_attachments(items, "hourly")] == [2]
    assert len(select_eskom_attachments(items, "weekly")) == 2


def test_short_history_is_supplementary_and_has_no_padded_dates(tmp_path):
    raw_hash = "a" * 64
    raw_path = tmp_path / "archive/raw" / (raw_hash + ".bin")
    raw_path.parent.mkdir(parents=True)
    raw_path.write_bytes(b"original source")
    frame = pd.DataFrame({"date": pd.date_range("2025-01-01", periods=48, freq="h", tz="UTC"), "value": range(48)})
    result = persist("short_test", frame, root=tmp_path, frequency="hourly", title="Test", provider="Test", category="test",
                     unit="MW", source_url="https://example.org", geography="ZA", hypothesis="test", limitations="test", raw_sha256=[raw_hash])
    assert result["storage_class"] == "supplementary"
    assert not result["research_eligible"]
    path = tmp_path / "supplementary/physical11/short_test.parquet"
    assert len(pd.read_parquet(path)) == 48
    assert not (tmp_path / "archive/staging/physical11").exists()
