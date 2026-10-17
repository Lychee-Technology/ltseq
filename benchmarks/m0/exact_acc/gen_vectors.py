#!/usr/bin/env python3
"""Reference vectors for src/m0/exact_acc.rs, computed independently with exact
rational arithmetic (fractions.Fraction / Python ints).

Rounding is done two ways and cross-checked: CPython's correctly rounded
int/int true division gives the candidate for a rational, and every result
(rational or square root) is then verified against the exact midpoints to its
neighbours, with ties going to the even mantissa. The square root candidate
comes from math.isqrt and is moved by midpoint tests until it verifies.

Large inputs are described by a generator spec (splitmix64 + integer-only
construction of each double) that tests/vectors.rs reimplements bit for bit;
the first values of each generated input are stored so a generator mismatch
fails loudly. Floats are stored as 16-hex-digit IEEE-754 bit patterns.

Usage: python gen_vectors.py > vectors.json
"""
import json
import math
import random
import struct
import sys
from fractions import Fraction as F

MAXF = sys.float_info.max
TWO1024 = F(2**1024)
OVF = F(2**1024 - 2**970)  # exact values >= this round to inf (tie -> inf)
INF = math.inf
NAN = math.nan
MASK64 = 2**64 - 1


def f2b(x):
    return struct.unpack("<Q", struct.pack("<d", x))[0]


def b2f(b):
    return struct.unpack("<d", struct.pack("<Q", b))[0]


def hx(x):
    return None if x is None else "%016x" % f2b(x)


def is_even(r):
    return f2b(r) & 1 == 0


# ---------------------------------------------------------------- rounding

def round_dir(r, a):
    """For positive rational a and candidate float r >= 0: 0 if r is the
    correctly rounded value of a (ties to even), +1/-1 if it is too low/high."""
    R = F(r)
    hi = TWO1024 if r == MAXF else F(math.nextafter(r, INF))
    mhi = (R + hi) / 2
    if a > mhi or (a == mhi and not is_even(r)):
        return 1
    if r > 0:
        mlo = (R + F(math.nextafter(r, -INF))) / 2
        if a < mlo or (a == mlo and not is_even(r)):
            return -1
    return 0


def round_q(q):
    """Correctly rounded float of an exact rational; exact zero -> +0.0,
    a nonzero value that underflows keeps its sign."""
    if q == 0:
        return 0.0
    neg = q < 0
    a = -q if neg else q
    if a >= OVF:
        r = INF
    else:
        r = a.numerator / a.denominator  # CPython: correctly rounded
        assert round_dir(r, a) == 0, ("int/int rounding check failed", a)
    return -r if neg else r


def sqrt_dir(r, a):
    """Same as round_dir but for sqrt(a): compares a with squared midpoints."""
    R = F(r)
    hi = TWO1024 if r == MAXF else F(math.nextafter(r, INF))
    mhi = (R + hi) / 2
    if a > mhi * mhi or (a == mhi * mhi and not is_even(r)):
        return 1
    if r > 0:
        mlo = (R + F(math.nextafter(r, -INF))) / 2
        if a < mlo * mlo or (a == mlo * mlo and not is_even(r)):
            return -1
    return 0


def sqrt_q(a):
    """Correctly rounded sqrt of an exact non-negative rational."""
    assert a >= 0
    if a == 0:
        return 0.0
    if a >= OVF * OVF:
        return INF
    p, q = a.numerator, a.denominator
    k = (130 - (p.bit_length() - q.bit_length())) // 2 + 1
    X = (p << (2 * k)) // q if k >= 0 else p // (q << (-2 * k))
    s = math.isqrt(X)
    r = round_q(F(s) / F(2) ** k)
    for _ in range(8):
        d = sqrt_dir(r, a)
        if d == 0:
            return r
        r = math.nextafter(r, INF if d > 0 else -INF)
    raise AssertionError("sqrt candidate did not converge")


# ---------------------------------------------------------------- exact sums

def exact_sum(vals):
    """Exact sum of finite floats as a Fraction (common power-of-two scale)."""
    if not vals:
        return F(0)
    pairs = [v.as_integer_ratio() for v in vals]
    den = max(d for _, d in pairs)
    return F(sum(n * (den // d) for n, d in pairs), den)


def exact_sum_products(xs, ys):
    if not xs:
        return F(0)
    pairs = []
    for x, y in zip(xs, ys):
        a, b = x.as_integer_ratio()
        c, d = y.as_integer_ratio()
        pairs.append((a * c, b * d))
    den = max(d for _, d in pairs)
    return F(sum(n * (den // d) for n, d in pairs), den)


def finite(x):
    return math.isfinite(x)


def nonfinite_sum(vals):
    """Sum rules for non-finite values; None when every value is finite."""
    nan = any(math.isnan(v) for v in vals)
    pinf = any(v == INF for v in vals)
    ninf = any(v == -INF for v in vals)
    if nan or (pinf and ninf):
        return NAN
    if pinf:
        return INF
    if ninf:
        return -INF
    return None


def product_class(x, y):
    """Non-finite class of x*y, or None if both factors are finite."""
    if math.isnan(x) or math.isnan(y):
        return NAN
    if math.isinf(x) or math.isinf(y):
        if x == 0 or y == 0:
            return NAN
        return INF if math.copysign(1, x) * math.copysign(1, y) > 0 else -INF
    return None


def sum_rule(xs):
    r = nonfinite_sum(xs)
    return r if r is not None else round_q(exact_sum(xs))


def mean_rule(xs):
    if not xs:
        return None
    r = nonfinite_sum(xs)
    return r if r is not None else round_q(exact_sum(xs) / len(xs))


def product_sum_rule(xs, ys):
    classes = [c for c in (product_class(x, y) for x, y in zip(xs, ys)) if c is not None]
    r = nonfinite_sum(classes)
    if r is not None:
        return r
    return round_q(exact_sum_products(xs, ys))


def var_std(xs):
    n = len(xs)
    if n < 2:
        return None, None
    if not all(finite(x) for x in xs):
        return NAN, NAN
    s1 = exact_sum(xs)
    s2 = exact_sum_products(xs, xs)
    v = (n * s2 - s1 * s1) / (n * (n - 1))
    assert v >= 0
    return round_q(v), sqrt_q(v)


def cov_corr(xs, ys):
    n = len(xs)
    if n < 2:
        return None, None
    if not all(finite(v) for v in xs + ys):
        return NAN, NAN
    sa, sb = exact_sum(xs), exact_sum(ys)
    sab = exact_sum_products(xs, ys)
    saa = exact_sum_products(xs, xs)
    sbb = exact_sum_products(ys, ys)
    N = n * sab - sa * sb
    da = n * saa - sa * sa
    db = n * sbb - sb * sb
    cov = round_q(N / (n * (n - 1)))
    if da == 0 or db == 0:
        return cov, NAN
    if N == 0:
        return cov, 0.0
    r = sqrt_q(N * N / (da * db))
    assert 0 < r <= 1
    return cov, (-r if N < 0 else r)


# ---------------------------------------------------------------- generators

class SplitMix64:
    def __init__(self, seed):
        self.s = seed & MASK64

    def next(self):
        self.s = (self.s + 0x9E3779B97F4A7C15) & MASK64
        z = self.s
        z = ((z ^ (z >> 30)) * 0xBF58476D1CE4E5B9) & MASK64
        z = ((z ^ (z >> 27)) * 0x94D049BB133111EB) & MASK64
        return z ^ (z >> 31)


def generate(spec):
    """Must match gen_values() in tests/vectors.rs."""
    kind, n = spec["gen"], spec["n"]
    if kind == "repeat":
        return [b2f(int(spec["value"], 16))] * n
    if kind == "spike":
        return [b2f(int(spec["value"], 16))] + [0.0] * (n - 1)
    rng = SplitMix64(spec["seed"])
    out = []
    for _ in range(n):
        if kind == "uniform2dp":
            # round(uniform(0, 10000), 2): k / 100 with k an integer < 10^6
            out.append((rng.next() % 1_000_000) / 100)
        elif kind == "normal_mag":
            # random sign, exponent in [-20, 20], random 52-bit fraction
            u, v = rng.next(), rng.next()
            bexp = 1023 - 20 + (u % 41)
            out.append(b2f(((u >> 63) << 63) | (bexp << 52) | (v & (2**52 - 1))))
        elif kind == "full_range":
            # any finite double (biased exponent 0..2046), random sign
            u, v = rng.next(), rng.next()
            bexp = u % 2047
            out.append(b2f(((u >> 63) << 63) | (bexp << 52) | (v & (2**52 - 1))))
        else:
            raise ValueError(kind)
    return out


# ---------------------------------------------------------------- cases

def case(name, x, y=None):
    """x / y: list of floats or a generator spec dict."""
    xs = generate(x) if isinstance(x, dict) else x
    c = {"name": name}

    def enc(spec, vals):
        if isinstance(spec, dict):
            d = dict(spec)
            d["head"] = [hx(v) for v in vals[:4]]
            return d
        return {"bits": [hx(v) for v in vals]}

    c["x"] = enc(x, xs)
    var, std = var_std(xs)
    c.update(
        n=len(xs),
        sum=hx(sum_rule(xs)),
        mean=hx(mean_rule(xs)),
        sum_xx=hx(product_sum_rule(xs, xs)),
        var=hx(var),
        std=hx(std),
    )
    if y is not None:
        ys = generate(y) if isinstance(y, dict) else y
        assert len(ys) == len(xs)
        c["y"] = enc(y, ys)
        cov, corr = cov_corr(xs, ys)
        c.update(
            sum_y=hx(sum_rule(ys)),
            sum_yy=hx(product_sum_rule(ys, ys)),
            sum_xy=hx(product_sum_rule(xs, ys)),
            cov=hx(cov),
            corr=hx(corr),
        )
    return c


SPECIALS = [
    0.0, -0.0, 5e-324, -5e-324, 1e-323, 2.2250738585072014e-308,
    2.225073858507201e-308, -2.225073858507201e-308, MAXF, -MAXF,
    1.0, -1.0, 2.0**-53, 2.0**-52, 1 + 2.0**-52, 0.1, 0.2, 0.3, 1e308,
    -1e308, 1e-300, -1e-300, 2.0**970, 2.0**969, 2.0**1023, 3.0,
    9007199254740992.0, 9007199254740993.0, 1e16, -1e16, 1e-160, 1e154,
]


def nasty_values(rng, n, allow_nonfinite):
    out = []
    for _ in range(n):
        r = rng.random()
        if allow_nonfinite and r < 0.04:
            out.append(rng.choice([NAN, INF, -INF]))
        elif r < 0.25:
            out.append(rng.choice(SPECIALS))
        elif r < 0.45:
            out.append(b2f(rng.getrandbits(63) % (0x7FF << 52) | (rng.getrandbits(1) << 63)))
        elif r < 0.6:
            out.append(rng.uniform(-1e3, 1e3))
        elif r < 0.8 and out:
            p = rng.choice(out)
            if math.isfinite(p):
                out.append(rng.choice([
                    -p, math.nextafter(p, INF), math.nextafter(p, -INF), p, -p * 0.5,
                    p * 2 if abs(p) < 1e307 else p,
                ]))
            else:
                out.append(1.0)
        else:
            out.append(float(rng.randint(-5, 5)))
    return out


# ---------------------------------------------------------------- near midpoints
#
# Random data almost never puts a statistic within a hair of a rounding
# midpoint, which is exactly where a lost sticky bit or a short quotient would
# show. These searches keep inputs whose exact result lies within 2^-15 ulp of
# a midpoint (either side). Detection only selects inputs; the expected values
# are computed by case() like every other case.

NEAR_BITS = 20
NEAR_WINDOW = 2**5  # |frac - half| < 2^5 / 2^20 ulp  ==  2^-15 ulp


def near_quotient(P, Q):
    """True if P/Q > 0 rounds to a normal double and lies near a midpoint."""
    r = P / Q
    if not (2.2250738585072014e-308 <= r < MAXF):
        return False
    ue = math.frexp(r)[1] - 53  # exponent of r's last mantissa bit
    sh = NEAR_BITS - ue
    X = (P << sh) // Q if sh >= 0 else P // (Q << -sh)
    frac = X & (2**NEAR_BITS - 1)
    return abs(frac - 2 ** (NEAR_BITS - 1)) < NEAR_WINDOW


def near_sqrt(P, Q):
    """True if sqrt(P/Q) (normal range) lies near a midpoint."""
    k = (2 * (53 + NEAR_BITS + 8) - (P.bit_length() - Q.bit_length())) // 2
    X = (P << (2 * k)) // Q if k >= 0 else P // (Q << (-2 * k))
    s = math.isqrt(X)
    L = s.bit_length()
    if L < 53 + NEAR_BITS + 2:
        return False
    r = s / 2**k if k < 1000 else 0.0
    if not (2.2250738585072014e-308 <= r < MAXF):
        return False
    frac = (s >> (L - 53 - NEAR_BITS)) & (2**NEAR_BITS - 1)
    return abs(frac - 2 ** (NEAR_BITS - 1)) < NEAR_WINDOW


def scaled_ints(vals, E):
    """Each value times 2^E as an exact integer (E large enough)."""
    out = []
    for v in vals:
        a, b = v.as_integer_ratio()
        out.append(a * (2**E // b))
    return out


def search_value(rng):
    r = rng.random()
    if r < 0.4:
        return rng.uniform(-1000, 1000)
    if r < 0.7:
        return rng.randrange(-10**6, 10**6) / 100
    return rng.uniform(1, 2) * 2.0 ** rng.randint(-30, 30) * rng.choice([1, -1])


def near_midpoint_cases(rng, per_stat=25):
    E = 1200  # 2^E clears every denominator of the values search_value makes
    cases = []
    found = {k: 0 for k in ("mean", "var", "std", "cov", "corr")}
    trials = 0
    while min(found.values()) < per_stat:
        trials += 1
        assert trials < 5_000_000, found
        stat = min(found, key=found.get)
        n = rng.choice({"mean": (3, 5, 6, 7, 10), "var": (3, 4, 5),
                        "std": (2, 2, 3, 4), "cov": (3, 4, 5),
                        "corr": (3, 4, 5)}[stat])
        xs = [search_value(rng) for _ in range(n)]
        ys = [search_value(rng) for _ in range(n)]
        I = scaled_ints(xs, E)
        s1 = sum(I)
        if stat == "mean":
            hit = s1 > 0 and near_quotient(s1, n << E)
        elif stat in ("var", "std"):
            P = n * sum(i * i for i in I) - s1 * s1
            Q = (n * (n - 1)) << (2 * E)
            hit = P > 0 and (near_quotient(P, Q) if stat == "var" else near_sqrt(P, Q))
        else:
            J = scaled_ints(ys, E)
            t1 = sum(J)
            N = n * sum(i * j for i, j in zip(I, J)) - s1 * t1
            if stat == "cov":
                hit = N != 0 and near_quotient(abs(N), (n * (n - 1)) << (2 * E))
            else:
                da = n * sum(i * i for i in I) - s1 * s1
                db = n * sum(j * j for j in J) - t1 * t1
                hit = N != 0 and da > 0 and db > 0 and near_sqrt(N * N, da * db)
        if hit:
            cases.append(case(f"near_mid_{stat}_{found[stat]}", xs, ys))
            found[stat] += 1
    print(f"near-midpoint search: {trials} trials", file=sys.stderr)
    return cases


def near_midpoint_spike_cases(rng, count=20):
    """mean([v, 0, ..., 0]) = v / n near a midpoint, with a short mantissa and
    a large n: the quotient is then exact up to its remainder, the only
    source of the sticky bit. (For small n, v / n has a binary period under
    n bits and cannot come within 2^-15 ulp of a midpoint unless exact.)"""
    E = 1200
    cases = []
    trials = 0
    while len(cases) < count:
        trials += 1
        n = rng.randrange(1001, 20000)
        v = search_value(rng)
        I = scaled_ints([v], E)[0]
        if I > 0 and near_quotient(I, n << E):
            spec = {"gen": "spike", "value": hx(v), "n": n}
            cases.append(case(f"near_mid_spike_mean_{len(cases)}", spec))
    print(f"spike search: {trials} trials", file=sys.stderr)
    return cases


def build():
    cases = []
    cases.extend(near_midpoint_cases(random.Random(777)))
    cases.extend(near_midpoint_spike_cases(random.Random(778)))
    # random data of several sizes
    for n in (10, 1000, 100_000):
        cases.append(case(f"uniform2dp_{n}",
                          {"gen": "uniform2dp", "seed": n, "n": n},
                          {"gen": "uniform2dp", "seed": n + 7, "n": n}))
        cases.append(case(f"normal_mag_{n}",
                          {"gen": "normal_mag", "seed": n + 1, "n": n},
                          {"gen": "normal_mag", "seed": n + 2, "n": n}))
    cases.append(case("full_range_1000",
                      {"gen": "full_range", "seed": 11, "n": 1000},
                      {"gen": "full_range", "seed": 12, "n": 1000}))
    cases.append(case("full_range_10",
                      {"gen": "full_range", "seed": 13, "n": 10},
                      {"gen": "full_range", "seed": 14, "n": 10}))
    # 0.1 repeated: naive summation drifts
    for n in (10, 1000, 100_000):
        spec = {"gen": "repeat", "value": hx(0.1), "n": n}
        cases.append(case(f"repeat_0.1_{n}", spec, spec))  # zero variance -> corr NaN
    # massive cancellation
    cases.append(case("cancel_1e308", [1e308, -1e308, 1e-300]))
    cases.append(case("cancel_mixed", [1e308, 1e-300, -1e308, -1e-300, 5e-324]))
    cases.append(case("cancel_one", [1e308, 1.0, -1e308], [1.0, 2.0, 3.0]))
    cases.append(case("cancel_max", [MAXF, -MAXF, MAXF, 2.0**-1074, -MAXF]))
    rng = random.Random(1234)
    big = [rng.uniform(1e300, 1e308) * rng.choice([1, -1]) for _ in range(200)]
    small = [rng.uniform(-1, 1) * 2.0**rng.randint(-1074, -900) for _ in range(20)]
    vals = big + [-v for v in big] + small
    rng.shuffle(vals)
    cases.append(case("cancel_random_extremes", vals, list(reversed(vals))))
    # subnormals and signed zeros
    cases.append(case("empty", []))
    cases.append(case("single", [3.5]))
    cases.append(case("single_neg_zero", [-0.0]))
    cases.append(case("neg_zeros", [-0.0, -0.0], [0.0, -0.0]))
    cases.append(case("zero_sum_subnormal", [5e-324, -5e-324]))
    cases.append(case("zero_sum_mixed", [1.5, -0.0, -1.5, 0.0]))
    cases.append(case("subnormal_to_normal", [2.225073858507201e-308, 5e-324]))
    cases.append(case("subnormal_var", [5e-324, 0.0], [0.0, 5e-324]))  # var -> 0, std -> 5e-324, cov -> -0.0
    cases.append(case("tiny_var", [1e-160, 2e-160, 4e-160]))
    sub = [b2f(rng.getrandbits(52)) * rng.choice([1, -1]) for _ in range(1000)]
    cases.append(case("subnormals_1000", sub, sub[::-1]))
    # rounding ties (exact value halfway between two doubles)
    e53 = 2.0**-53
    cases.append(case("tie_even_down", [1.0, e53]))
    cases.append(case("tie_even_up", [1 + 2.0**-52, e53]))
    cases.append(case("tie_sticky_up", [1.0, e53, 2.0**-1074]))
    cases.append(case("tie_sticky_down", [1.0, e53, -(2.0**-1074)]))
    cases.append(case("tie_neg", [-1.0, -e53]))
    cases.append(case("tie_2p53", [2.0**53, 1.0]))
    cases.append(case("tie_2p53_3", [2.0**53, 3.0]))
    cases.append(case("tie_mean_down", [2.0**53, 2.0**53 + 2]))
    cases.append(case("tie_mean_up", [2.0**53 + 2, 2.0**53 + 4]))
    cases.append(case("tie_overflow", [MAXF, 2.0**970]))
    cases.append(case("below_overflow", [MAXF, 2.0**969]))
    cases.append(case("tie_overflow_sticky_down", [MAXF, 2.0**970, -(2.0**-1074)]))
    cases.append(case("tie_overflow_neg", [-MAXF, -(2.0**970)]))
    # exact ties for var and cov: var([m, 0]) = m^2 / 2 and
    # cov([a, 0], [b, 0]) = a*b / 2 have a 54-bit odd mantissa when m^2
    # (a*b) is an odd integer in [2^53, 2^54), so they sit exactly on a midpoint
    tie_rng = random.Random(4242)
    made = 0
    while made < 12:
        a = tie_rng.randrange(2**26 + 1, 2**27, 2)
        b = tie_rng.randrange(2**26 + 1, 2**27, 2) if made % 2 else a
        if not (2**53 <= a * b < 2**54):
            continue
        scale = 2.0 ** tie_rng.choice([-500, -40, 0, 0, 30, 400])
        sign = tie_rng.choice([1.0, -1.0])
        xs, ys = [a * scale, 0.0], [sign * b * scale, 0.0]
        v = abs(F(xs[0]) * F(ys[0])) / 2
        r = round_q(v)
        assert v in ((F(r) + F(math.nextafter(r, INF))) / 2,
                     (F(r) + F(math.nextafter(r, -INF))) / 2), "not an exact tie"
        cases.append(case(f"tie_var_cov_{made}", xs, ys))
        made += 1
    # many random near-ties: v + half ulp(v) split into pieces
    for i in range(40):
        v = rng.uniform(1, 2) * 2.0**rng.randint(-1000, 1000)
        half = (math.nextafter(v, INF) - v) / 2
        extra = rng.choice([0.0, half * 2.0**-60, -half * 2.0**-60])
        cases.append(case(f"near_tie_{i}", [v, half / 3, half / 3, half - 2 * (half / 3), extra]))
    # near-constant data: n*S2 - S1^2 cancels catastrophically
    cases.append(case("near_const_1e9",
                      [1e9 + rng.randint(0, 3) * 2.0**-22 for _ in range(1000)],
                      [1e9 + rng.randint(0, 3) * 2.0**-22 for _ in range(1000)]))
    nc = [100000000.1, 100000000.2, 100000000.3, 100000000.1]
    cases.append(case("near_const_decimal", nc, [v * 2 for v in nc]))
    cases.append(case("near_const_1e15", [1e15 + k for k in (0, 1, 2, 1, 0, 3)]))
    cases.append(case("constant", [3.3] * 100, [rng.random() for _ in range(100)]))
    # correlation
    a = generate({"gen": "normal_mag", "seed": 99, "n": 500})
    cases.append(case("corr_self", a, a))
    cases.append(case("corr_neg_self", a, [-v for v in a]))
    cases.append(case("corr_scaled", a, [3 * v + 1 for v in a]))
    cases.append(case("corr_two_points", [1.0, 2.0], [5.0, -7.0]))
    # extremes from the contract
    cases.append(case("std_1e308", [1e308, -1e308]))
    cases.append(case("mean_1e308", [1e308, 1e308]))
    cases.append(case("std_overflow", [MAXF, -MAXF]))
    cases.append(case("overflow_neg", [-1.5e308, -1.5e308, 1e300]))
    cases.append(case("cov_huge", [1e308, -1e308, 1e308], [1e308, 1e308, -1e308]))
    # non-finite rules
    cases.append(case("nan_in_sum", [1.0, NAN]))
    cases.append(case("pos_inf", [INF, 1.0]))
    cases.append(case("neg_inf", [-INF, 5.0]))
    cases.append(case("both_inf", [INF, -INF]))
    cases.append(case("two_pos_inf", [INF, INF]))
    cases.append(case("only_nan", [NAN]))
    cases.append(case("inf_with_y", [1.0, 2.0, INF], [1.0, 2.0, 3.0]))
    cases.append(case("zero_times_inf", [0.0, 1.0], [INF, 2.0]))
    cases.append(case("neg_inf_product", [2.0, 1.0], [-INF, 2.0]))
    cases.append(case("neg_zero_times_neg_inf", [-0.0, 3.0], [-INF, 1.0]))
    # fuzz: small cases drawn from special-value-heavy distributions
    for i in range(3000):
        n = rng.randint(0, 12)
        nf = i % 10 == 0
        xs = nasty_values(rng, n, nf)
        ys = nasty_values(rng, n, nf)
        cases.append(case(f"fuzz_{i}", xs, ys))
    return cases


def self_check():
    # sanity checks of the reference itself
    assert round_q(F(1) + F(1, 2**53)) == 1.0
    assert round_q(F(MAXF) + F(2**970)) == INF
    assert round_q(F(MAXF) + F(2**969)) == MAXF
    assert sqrt_q(F(2)) == math.sqrt(2.0)
    for v in [0.5, 2.0, 3.0, 1e-300, 1e300, 5e-324, 7.0, 1e-310]:
        assert sqrt_q(F(v)) == math.sqrt(v), v  # libm sqrt is correctly rounded
    assert sqrt_q(F(2) * F(10**308) ** 2) == 1.4142135623730951e308
    assert sqrt_q(F(1, 2**2149)) == 5e-324
    r = random.Random(5)
    for _ in range(20000):
        v = b2f(r.getrandbits(63) % (0x7FF << 52))
        assert sqrt_q(F(v)) == math.sqrt(v), v
        w = b2f(r.getrandbits(63) % (0x7FF << 52))
        q = F(v) / F(w) if w != 0 else F(v)
        if q < OVF:
            assert round_q(q) == (v / w if w != 0 else v)


if __name__ == "__main__":
    self_check()
    json.dump({"cases": build()}, sys.stdout, separators=(",", ":"))
    sys.stdout.write("\n")
