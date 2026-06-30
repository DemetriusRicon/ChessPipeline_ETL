import pendulum
from typing import Tuple, Dict


def resolve_period(
    params: Dict,
    logical_date: pendulum.DateTime,
) -> Tuple[pendulum.DateTime, pendulum.DateTime]:

    start_str = params.get("start_date")
    end_str = params.get("end_date")

    if start_str and end_str:
        try:
            start_dt = pendulum.parse(start_str)
            end_dt = pendulum.parse(end_str)
        except Exception as e:
            raise ValueError(f"Invalid period parameters: {e}")
    else:
        # Default to previous week for scheduled runs
        end_dt = logical_date
        start_dt = end_dt.subtract(days=7)

    if start_dt > end_dt:
        raise ValueError("start_date must be before or equal to end_date")

    return start_dt, end_dt