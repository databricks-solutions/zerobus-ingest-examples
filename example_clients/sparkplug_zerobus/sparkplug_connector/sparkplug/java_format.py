"""Java and Jackson output rules that Eclipse Tahu's JSON depends on.

- ``java_float_to_string`` / ``java_double_to_string``: ``Float.toString`` / ``Double.toString``.
  Default ``jdk17=True``: as on Java 17 (measured against it): the
  shortest digits that round back to the value, except a whole number below 2^63, which Java 17
  prints from its exact digits less ``floor(p * log10(2))`` low digits (``p`` = binary exponent
  minus the significand's bits minus 1, rounded half up), e.g. ``-3.30283392E9``, not
  ``-3.302834E9``; and a power of two, which Java 17 gives the shortest digits within half its
  upper gap on both sides (``1.17549435E-38`` for Float.MIN_NORMAL, not ``1.1754944E-38``).
  Known remainder, measured: from 2^63 (9.2e18) up, about 0.02% of random floats and 0.001% of
  random doubles get a different last digit on Java 17; and 3 of the 10,000 smallest subnormals
  (around 1e-43 and 1e-322). Everything below 2^63 and above them matches.
  ``jdk17=False``: JDK 19+ (always the shortest).
- ``java_utf8_decode``: ``new String(bytes, UTF_8)``, which replaces invalid UTF-8 with U+FFFD in
  different places than Python's ``errors="replace"`` (e.g. one U+FFFD for an encoded surrogate);
  matched Java on 300,000 random byte strings.
- ``json_string``: Jackson's default string escaping.
- ``java_hashmap_order``: the iteration order of a ``java.util.HashMap`` built like Tahu builds
  property sets (Jackson writes maps in that order).

Reimplements observed Java/Jackson behaviour; not derived from Tahu's or the JDK's code.
"""
from __future__ import annotations

import math
import struct
from decimal import Decimal
from fractions import Fraction
from typing import Iterable, TypeVar

V = TypeVar("V")

_THOUSANDTH = Fraction(1, 1000)
_TEN_MILLION = Fraction(10_000_000)
_FLOAT32_INFINITY_BITS = 0x7F800000


# ----------------------------------------------------------------------------- numbers

def java_float_to_string(value: float, jdk17: bool = True) -> str:
    """``Float.toString`` of a float32 value (held in a Python float)."""
    special = _special(value)
    if special is not None:
        return special
    magnitude = abs(value)
    digits_exponent = (_whole_number_digits(magnitude, 24) or _power_of_two_digits(magnitude, 23, -126)) if jdk17 else None
    digits, exponent = digits_exponent or _float32_digits(magnitude)
    return _render(value < 0, digits, exponent, _THOUSANDTH <= Fraction(magnitude) < _TEN_MILLION)


def java_double_to_string(value: float, jdk17: bool = True) -> str:
    """``Double.toString`` of a double."""
    special = _special(value)
    if special is not None:
        return special
    magnitude = abs(value)
    digits_exponent = (_whole_number_digits(magnitude, 53) or _power_of_two_digits(magnitude, 52, -1022)) if jdk17 else None
    if digits_exponent:
        digits, exponent = digits_exponent
    else:
        digits, exponent = _decimal_digits(repr(magnitude))
        if len(digits) == 1:
            # Java picks the closest decimal of length 1 or 2 when the shortest has one digit
            # (Double.MIN_VALUE is "4.9E-324", not "5E-324")
            digits, exponent = _decimal_digits(f"{magnitude:.1e}")
    return _render(value < 0, digits, exponent, _THOUSANDTH <= Fraction(magnitude) < _TEN_MILLION)


def json_float(value: float, jdk17: bool = True) -> str:
    """A float32 as Jackson writes it: NaN and infinities become JSON strings."""
    return _quote_non_numeric(java_float_to_string(value, jdk17))


def json_double(value: float, jdk17: bool = True) -> str:
    """A double as Jackson writes it: NaN and infinities become JSON strings."""
    return _quote_non_numeric(java_double_to_string(value, jdk17))


def _whole_number_digits(magnitude: float, significand_bits: int) -> tuple[str, int] | None:
    """Java 17's digits for a whole number in [1, 2^63): its exact digits less the insignificant ones."""
    if not 1 <= magnitude < 2.0 ** 63 or magnitude != math.floor(magnitude):
        return None
    n = int(magnitude)
    p = n.bit_length() - 1 - significand_bits - 1
    drop = _insignificant_digits(p)
    if drop:
        scale = 10 ** drop
        n, residue = divmod(n, scale)
        if residue >= scale // 2:
            n += 1
    return _decimal_digits(f"{n}e{drop}")


def _power_of_two_digits(magnitude: float, fraction_bits: int, min_exponent: int) -> tuple[str, int] | None:
    """Java 17's digits for a power of two: the shortest decimal within half the upper gap either side
    (Java narrows the interval as if the gap below were half the gap above, for subnormals too)."""
    mantissa, exponent2 = math.frexp(magnitude)
    if mantissa != 0.5:
        return None
    exact = Fraction(magnitude)
    upper_gap_exponent = max(exponent2 - 1, min_exponent) - fraction_bits
    half_gap = Fraction(2) ** (upper_gap_exponent - 2)
    for length in range(1, 20):
        candidate = f"{magnitude:.{length - 1}e}"
        if abs(Fraction(candidate) - exact) < half_gap:
            break
    if length == 1:
        candidate = f"{magnitude:.1e}"
    return _decimal_digits(candidate)


def _insignificant_digits(p: int) -> int:
    """floor(p * log10(2)) for p > 1, else 0."""
    if p <= 1:
        return 0
    power, digits = 1 << p, 0
    while 10 ** (digits + 1) <= power:
        digits += 1
    return digits


def _special(value: float) -> str | None:
    if math.isnan(value):
        return "NaN"
    if math.isinf(value):
        return "Infinity" if value > 0 else "-Infinity"
    if value == 0:
        return "-0.0" if math.copysign(1.0, value) < 0 else "0.0"
    return None


def _quote_non_numeric(text: str) -> str:
    return f'"{text}"' if text in ("NaN", "Infinity", "-Infinity") else text


def _float32_digits(magnitude: float) -> tuple[str, int]:
    """Shortest decimal that rounds to this float32 (closest of length 1 or 2 if the shortest has one digit)."""
    for length in range(1, 10):
        candidate = f"{magnitude:.{length - 1}e}"      # correctly rounded from the exact binary value
        if _rounds_to_float32(Fraction(candidate), magnitude):
            break
    if length == 1:
        candidate = f"{magnitude:.1e}"
    return _decimal_digits(candidate)


def _rounds_to_float32(decimal: Fraction, value: float) -> bool:
    """Whether ``decimal`` rounds (to nearest, ties to even) to the positive float32 ``value``."""
    bits = struct.unpack("<I", struct.pack("<f", value))[0]
    exact = Fraction(value)
    below = Fraction(_float32_from_bits(bits - 1)) if bits > 0 else Fraction(0)
    above_bits = bits + 1
    above = Fraction(_float32_from_bits(above_bits)) if above_bits < _FLOAT32_INFINITY_BITS else exact + (exact - below)
    low, high = (exact + below) / 2, (exact + above) / 2
    if low < decimal < high:
        return True
    return bits % 2 == 0 and decimal in (low, high)


def _float32_from_bits(bits: int) -> float:
    return struct.unpack("<f", struct.pack("<I", bits))[0]


def _decimal_digits(text: str) -> tuple[str, int]:
    """Significant digits (trailing zeros removed) and the exponent of the first digit."""
    _, digits, exponent = Decimal(text).as_tuple()
    joined = "".join(map(str, digits))
    return joined.rstrip("0") or "0", len(joined) - 1 + exponent


def _render(negative: bool, digits: str, exponent: int, plain: bool) -> str:
    if plain:
        if exponent >= 0:
            whole = digits[: exponent + 1].ljust(exponent + 1, "0")
            fraction = digits[exponent + 1:] or "0"
        else:
            whole, fraction = "0", "0" * (-exponent - 1) + digits
        text = f"{whole}.{fraction}"
    else:
        text = f"{digits[0]}.{digits[1:] or '0'}E{exponent}"
    return "-" + text if negative else text


# ----------------------------------------------------------------------------- strings

def _cont(b: int) -> bool:
    return b & 0xC0 == 0x80

def java_utf8_decode(raw: bytes) -> str:
    """``new String(raw, StandardCharsets.UTF_8)``: valid UTF-8 decoded, each malformed sequence as Java delimits it -> U+FFFD."""
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        pass
    out, i, n = [], 0, len(raw)
    while i < n:
        b1 = raw[i]
        if b1 < 0x80:
            out.append(chr(b1)); i += 1
        elif 0xC2 <= b1 <= 0xDF:
            if i + 1 < n and _cont(raw[i + 1]):
                out.append(raw[i:i + 2].decode("utf-8")); i += 2
            else:
                out.append("�"); i += 1
        elif 0xE0 <= b1 <= 0xEF:
            if i + 2 < n:
                b2, b3 = raw[i + 1], raw[i + 2]
                if (b1 == 0xE0 and b2 & 0xE0 == 0x80) or not _cont(b2):
                    out.append("�"); i += 1
                elif not _cont(b3):
                    out.append("�"); i += 2
                else:
                    c = ((b1 & 0x0F) << 12) | ((b2 & 0x3F) << 6) | (b3 & 0x3F)
                    out.append("�" if 0xD800 <= c <= 0xDFFF else chr(c)); i += 3
            else:  # truncated at the end
                if i + 1 < n and ((b1 == 0xE0 and raw[i + 1] & 0xE0 == 0x80) or not _cont(raw[i + 1])):
                    out.append("�"); i += 1
                else:
                    out.append("�"); i = n
        elif 0xF0 <= b1 <= 0xF7:
            if i + 3 < n:
                b2, b3, b4 = raw[i + 1], raw[i + 2], raw[i + 3]
                if b1 > 0xF4 or not _cont(b2) or (b1 == 0xF0 and b2 < 0x90) or (b1 == 0xF4 and b2 >= 0x90):
                    out.append("�"); i += 1
                elif not _cont(b3):
                    out.append("�"); i += 2
                elif not _cont(b4):
                    out.append("�"); i += 3
                else:
                    c = ((b1 & 0x07) << 18) | ((b2 & 0x3F) << 12) | ((b3 & 0x3F) << 6) | (b4 & 0x3F)
                    out.append(chr(c) if 0x10000 <= c <= 0x10FFFF else "�"); i += 4
            else:  # truncated at the end
                rest = raw[i + 1:]
                if b1 > 0xF4 or (rest and ((b1 == 0xF0 and not 0x90 <= rest[0] <= 0xBF) or (b1 == 0xF4 and rest[0] & 0xF0 != 0x80) or not _cont(rest[0]))):
                    out.append("�"); i += 1
                elif len(rest) >= 2 and not _cont(rest[1]):
                    out.append("�"); i += 2
                else:
                    out.append("�"); i = n
        else:
            out.append("�"); i += 1
    return "".join(out)


_ESCAPES = {code: f"\\u{code:04X}" for code in range(0x20)}
_ESCAPES.update({0x08: "\\b", 0x09: "\\t", 0x0A: "\\n", 0x0C: "\\f", 0x0D: "\\r", 0x22: '\\"', 0x5C: "\\\\"})


def json_string(text: str) -> str:
    """A JSON string as Jackson writes it: control characters escaped (uppercase hex), nothing else."""
    return '"' + text.translate(_ESCAPES) + '"'


# ----------------------------------------------------------------------------- HashMap order

def java_string_hash(text: str) -> int:
    """``String.hashCode()`` as an unsigned 32-bit value (over UTF-16 code units)."""
    units = text.encode("utf-16-le", "surrogatepass")
    h = 0
    for i in range(0, len(units), 2):
        h = (31 * h + (units[i] | units[i + 1] << 8)) & 0xFFFFFFFF
    return h


def java_hashmap_order(entries: Iterable[tuple[str, V]], ceil_presize: bool = False) -> list[tuple[str, V]]:
    """Iteration order of Tahu's property map: ``new HashMap()`` filled with ``entries`` (a repeated
    key keeps its first position and takes the last value), then copied with ``putAll`` into another
    ``new HashMap()``. ``ceil_presize``: False (default) = JDK 17, which sizes the copy with ``n / 0.75 + 1``; True = JDK 19+ (``ceil(n / 0.75)``).
    Ignores bucket treeification (8+ keys in one bucket)."""
    first = dict(entries)
    if not first:
        return []
    keys = list(first)
    hashes = {key: _spread(java_string_hash(key)) for key in keys}

    order = sorted(keys, key=lambda k: hashes[k] & (_grown(16, len(keys)) - 1))
    n = len(order)
    presize = math.ceil(n / 0.75) if ceil_presize else int(n / 0.75 + 1.0)
    capacity = _grown(_table_size_for(presize), n)
    order = sorted(order, key=lambda k: hashes[k] & (capacity - 1))
    return [(key, first[key]) for key in order]


def _spread(h: int) -> int:
    return h ^ (h >> 16)


def _table_size_for(n: int) -> int:
    capacity = 1
    while capacity < n:
        capacity <<= 1
    return capacity


def _grown(capacity: int, size: int) -> int:
    """Table capacity after inserting ``size`` keys (it doubles whenever size exceeds 0.75 x capacity)."""
    while size > int(capacity * 0.75):
        capacity <<= 1
    return capacity
