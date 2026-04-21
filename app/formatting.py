import math
from typing import Optional


def fmt_usd(x: Optional[float], decimals: int = 0) -> str:
    if x is None:
        return "—"
    try:
        value = float(x)
    except (TypeError, ValueError):
        return "—"
    if math.isnan(value) or math.isinf(value):
        return "—"

    sign = "-" if value < 0 else ""
    value = abs(value)
    if decimals:
        return f"{sign}${value:,.{decimals}f}"
    return f"{sign}${value:,.0f}"
