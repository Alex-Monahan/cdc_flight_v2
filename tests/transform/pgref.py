"""A small printer of Postgres text, written for the tests (it shares no code with cdc_v2.transform).

It prints values the way Postgres prints them under the read sessions' settings (DateStyle ISO, IntervalStyle
iso_8601, TimeZone UTC, extra_float_digits 3, bytea_output hex), and the engine's transport forms of the same values.
"""
from __future__ import annotations

import struct

DAY_US = 86400000000
PG_EPOCH_DAYS = 10957
PG_EPOCH_US = 946684800000000
MIN_DATE_DAYS = -2440588              # 4714-11-24 BC
MAX_DATE_DAYS = 2145042905            # 5874897-12-31
MAX_TS_US = 9224318015999999999       # 294276-12-31 23:59:59.999999
DUCK_TS_MAX = 9223372036854775806     # largest finite DuckDB TIMESTAMP


def civil_from_days(z: int):
    """(year (astronomical), month, day) of days since 1970-01-01, proleptic Gregorian."""
    z += 719468
    era = z // 146097                 # Python floors, so no adjustment is needed
    doe = z - era * 146097
    yoe = (doe - doe // 1460 + doe // 36524 - doe // 146096) // 365
    y = yoe + era * 400
    doy = doe - (365 * yoe + yoe // 4 - yoe // 100)
    mp = (5 * doy + 2) // 153
    d = doy - (153 * mp + 2) // 5 + 1
    m = mp + 3 if mp < 10 else mp - 9
    return y + (m <= 2), m, d


def days_from_civil(y: int, m: int, d: int) -> int:
    y -= m <= 2
    era = y // 400
    yoe = y - era * 400
    doy = (153 * (m + (-3 if m > 2 else 9)) + 2) // 5 + d - 1
    doe = yoe * 365 + yoe // 4 - yoe // 100 + doy
    return era * 146097 + doe - 719468


def _year(y):
    return ("%04d" % y, "") if y > 0 else ("%04d" % (1 - y), " BC")


def date_text(days: int) -> str:
    y, m, d = civil_from_days(days)
    ys, bc = _year(y)
    return "%s-%02d-%02d%s" % (ys, m, d, bc)


def _frac(us: int) -> str:
    return "" if us == 0 else "." + ("%06d" % us).rstrip("0")


def ts_text(us: int, tz: bool = False) -> str:
    days, rem = divmod(us, DAY_US)
    y, m, d = civil_from_days(days)
    ys, bc = _year(y)
    h, rem = divmod(rem, 3600000000)
    mi, rem = divmod(rem, 60000000)
    s, f = divmod(rem, 1000000)
    return "%s-%02d-%02d %02d:%02d:%02d%s%s%s" % (ys, m, d, h, mi, s, _frac(f), "+00" if tz else "", bc)


def time_text(us: int) -> str:
    h, rem = divmod(us, 3600000000)
    mi, rem = divmod(rem, 60000000)
    s, f = divmod(rem, 1000000)
    return "%02d:%02d:%02d%s" % (h, mi, s, _frac(f))


def timetz_text(us: int, offset_s: int) -> str:
    sign = "-" if offset_s < 0 else "+"
    a = abs(offset_s)
    oh, rem = divmod(a, 3600)
    om, os_ = divmod(rem, 60)
    off = "%s%02d" % (sign, oh)
    if om or os_:
        off += ":%02d" % om
    if os_:
        off += ":%02d" % os_
    return time_text(us) + off


def _trunc_div(a: int, b: int):
    q = abs(a) // abs(b)
    q = q if (a >= 0) == (b >= 0) else -q
    return q, a - q * b


def interval_iso(months: int, days: int, us: int) -> str:
    """IntervalStyle iso_8601 (EncodeInterval): every field truncated toward zero."""
    if months == 0 and days == 0 and us == 0:
        return "PT0S"
    y, mo = _trunc_div(months, 12)
    h, rem = _trunc_div(us, 3600000000)
    mi, rem = _trunc_div(rem, 60000000)
    s, f = _trunc_div(rem, 1000000)
    out = "P"
    for v, c in ((y, "Y"), (mo, "M"), (days, "D")):
        if v:
            out += "%d%s" % (v, c)
    if h or mi or s or f:
        out += "T"
    for v, c in ((h, "H"), (mi, "M")):
        if v:
            out += "%d%s" % (v, c)
    if s or f:
        out += ("-" if s < 0 or f < 0 else "") + "%d%s" % (abs(s), _frac(abs(f))) + "S"
    return out


def interval_postgres(months: int, days: int, us: int) -> str:
    """IntervalStyle postgres (EncodeInterval, INTSTYLE_POSTGRES): the read sessions' style."""
    y, mo = _trunc_div(months, 12)
    h, rem = _trunc_div(us, 3600000000)
    mi, rem = _trunc_div(rem, 60000000)
    s, f = _trunc_div(rem, 1000000)
    parts, is_zero, is_before = [], True, False
    for v, unit in ((y, "year"), (mo, "mon"), (days, "day")):
        if v == 0:
            continue
        parts.append("%s%d %s%s" % ("+" if is_before and v > 0 else "", v, unit, "" if v == 1 else "s"))
        is_before, is_zero = v < 0, False
    if is_zero or h or mi or s or f:
        minus = h < 0 or mi < 0 or s < 0 or f < 0
        sign = "-" if minus else ("+" if is_before else "")
        parts.append("%s%02d:%02d:%02d%s" % (sign, abs(h), abs(mi), abs(s), _frac(abs(f))))
    return " ".join(parts)


def interval_transport(months: int, days: int, us: int) -> str:
    return "%d months %d days %d microseconds" % (months, days, us)


def float8_text(v: float) -> str:
    if v != v:
        return "NaN"
    if v in (float("inf"), float("-inf")):
        return "Infinity" if v > 0 else "-Infinity"
    r = repr(v)
    return r[:-2] if r.endswith(".0") else r


def float4_value(bits: int) -> float:
    return struct.unpack("<f", struct.pack("<I", bits))[0]


def float4_text(v: float) -> str:
    """Shortest text that reads back as the same float32."""
    if v != v:
        return "NaN"
    if v in (float("inf"), float("-inf")):
        return "Infinity" if v > 0 else "-Infinity"
    for p in range(1, 10):
        s = "%.*g" % (p, v)
        try:
            if struct.pack("<f", float(s)) == struct.pack("<f", v):
                return s
        except OverflowError:               # a short text rounded above the largest float32
            continue
    return repr(v)


def bytea_text(b: bytes) -> str:
    return "\\x" + b.hex()


def numeric_fits(text: str, w: int = 38, s: int = 10) -> bool:
    if text in ("NaN", "Infinity", "-Infinity"):
        return False
    ip, _, fp = text.lstrip("-").partition(".")
    return len(ip.lstrip("0")) <= w - s and len(fp.rstrip("0")) <= s
