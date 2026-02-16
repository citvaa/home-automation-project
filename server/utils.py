import re
from datetime import datetime, timezone
from typing import Any, Optional, Tuple


def normalize_value(value: Any, unit: Optional[str] = None) -> Tuple[Any, Optional[str]]:
    """Normalize a sensor value.

    - If value is a boolean-like string ("true","false","on","off","1","0")
      return a Python bool.
    - If value is a numeric string optionally with unit suffix ("12.3C" or "45 %"),
      return a float and the detected unit (if any).
    - Otherwise return the value unchanged and the passed unit.
    """
    if isinstance(value, str):
        s = value.strip()
        sl = s.lower()
        # boolean-like
        if sl in ("true", "false", "on", "off", "1", "0"):
            return (sl in ("true", "on", "1")), unit
        m = re.match(r'^([+-]?\d+(?:\.\d+)?)(?:\s*([a-zA-Z%]+))?$', s)
        if m:
            num = float(m.group(1))
            suff = m.group(2)
            if suff:
                unit = unit or suff
            return num, unit
    return value, unit


def parse_timestamp(timestamp: Any) -> Optional[datetime]:
    """Convert a timestamp (ISO string or epoch number) to a timezone-aware datetime in UTC.

    Returns `None` on parse failure.
    """
    try:
        if isinstance(timestamp, str):
            ts = timestamp.replace("Z", "+00:00")
            return datetime.fromisoformat(ts)
        if isinstance(timestamp, (int, float)):
            return datetime.fromtimestamp(float(timestamp), timezone.utc)
    except Exception:
        return None
    return None
