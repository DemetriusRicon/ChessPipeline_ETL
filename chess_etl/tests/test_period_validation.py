import pytest
import pendulum
from datetime import datetime
from src.utils.validation import resolve_period

def test_resolve_period_with_params():
    params = {"start_date": "2024-01-01", "end_date": "2024-01-31"}
    logical_date = datetime(2024, 2, 1)
    start_dt, end_dt = resolve_period(params, logical_date)
    
    assert start_dt == pendulum.datetime(2024, 1, 1)
    assert end_dt == pendulum.datetime(2024, 1, 31)

def test_resolve_period_default_to_last_week():
    params = {}
    logical_date = pendulum.datetime(2024, 1, 10)
    start_dt, end_dt = resolve_period(params, logical_date)
    
    assert end_dt == pendulum.datetime(2024, 1, 10)
    assert start_dt == pendulum.datetime(2024, 1, 3)

def test_resolve_period_invalid_dates():
    params = {"start_date": "invalid-date", "end_date": "2024-01-31"}
    logical_date = datetime(2024, 2, 1)
    with pytest.raises(ValueError, match="Invalid period parameters"):
        resolve_period(params, logical_date)

def test_resolve_period_start_after_end():
    params = {"start_date": "2024-01-31", "end_date": "2024-01-01"}
    logical_date = datetime(2024, 2, 1)
    with pytest.raises(ValueError, match="start_date must be before or equal to end_date"):
        resolve_period(params, logical_date)
