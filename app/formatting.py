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
    if 0 < value < 0.01:
        return f"{sign}$0.01" if sign else "<$0.01"
    if value < 10:
        return f"{sign}${value:,.2f}"
    return f"{sign}${value:,.0f}"
