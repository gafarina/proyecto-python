"""
_numba_iv.py
============
Motor numérico para IV de call americanas via árbol Leisen-Reimer + Brent.

  - Árbol LR (Leisen-Reimer 1996), n pasos impares (ej. 101)
  - Dividendos continuos q (div_yield_cont) — yield ya disponible en el parquet
  - Inversión σ con método de Brent (robusto en alas OTM sin vega)
  - compute_iv_batch: numba.prange paralelo sobre filas
"""

import math
import numpy as np

try:
    import numba
    _HAVE_NUMBA = True
except ImportError:
    _HAVE_NUMBA = False
    numba = None

# ═══════════════════════════════════════════════════════════
# PATH A: Motor Numba
# ═══════════════════════════════════════════════════════════
if _HAVE_NUMBA:

    @numba.njit(fastmath=True, nogil=True)
    def _h(x: float, n: int) -> float:
        """Peizer-Pratt h(x,n) — aproximación normal usada por árbol LR."""
        if abs(x) < 1e-15:
            return 0.5
        c   = x / (n + 1.0 / 3.0 + 0.1 / (n + 1.0))
        val = 0.25 - 0.25 * math.exp(-(c * c) * (n + 1.0 / 6.0))
        if val < 0.0:
            val = 0.0
        return 0.5 + math.copysign(math.sqrt(val), x)

    @numba.njit(fastmath=True, nogil=True)
    def _lr_american_option(S: float, K: float, r: float, q: float,
                           T: float, sigma: float, n: int, is_call: bool) -> float:
        """
        Árbol binomial Leisen-Reimer (1996) para call americano con
        dividendo continuo q.

        Args:
            S     : Precio del subyacente
            K     : Strike
            r     : Tasa libre de riesgo continua
            q     : Dividend yield continuo (div_yield_cont)
            T     : DTE / 365
            sigma : Volatilidad (variable a resolver)
            n     : Pasos del árbol (impar recomendado, ej. 101)
        """
        if T <= 1e-10:
            return max(0.0, S - K) if is_call else max(0.0, K - S)
        if sigma <= 1e-10 or S <= 0.0 or K <= 0.0:
            return max(0.0, S * math.exp(-q * T) - K * math.exp(-r * T)) if is_call else max(0.0, K * math.exp(-r * T) - S * math.exp(-q * T))

        sqT = math.sqrt(T)
        d1  = (math.log(S / K) + (r - q + 0.5 * sigma * sigma) * T) / (sigma * sqT)
        d2  = d1 - sigma * sqT

        p_u = _h(d2, n)     # prob. riesgo-neutral de suba
        p_b = _h(d1, n)     # prob. medida stock de suba

        if p_u < 1e-10 or p_u > 1.0 - 1e-10:
            return max(0.0, S * math.exp(-q * T) - K * math.exp(-r * T)) if is_call else max(0.0, K * math.exp(-r * T) - S * math.exp(-q * T))

        dt   = T / n
        Rnet = math.exp((r - q) * dt)   # crecimiento neto del stock
        disc = math.exp(-r * dt)         # factor de descuento

        u   = Rnet * p_b / p_u
        ddn = (Rnet - p_u * u) / (1.0 - p_u)

        if u <= 0.0 or ddn <= 0.0 or u <= ddn:
            return max(0.0, S * math.exp(-q * T) - K * math.exp(-r * T)) if is_call else max(0.0, K * math.exp(-r * T) - S * math.exp(-q * T))

        # Precomputar potencias u^k y d^k
        u_pow = np.empty(n + 1)
        d_pow = np.empty(n + 1)
        u_pow[0] = 1.0
        d_pow[0] = 1.0
        for k in range(1, n + 1):
            u_pow[k] = u_pow[k - 1] * u
            d_pow[k] = d_pow[k - 1] * ddn

        # Payoffs terminales
        prices = np.empty(n + 1)
        for j in range(n + 1):
            St = S * u_pow[n - j] * d_pow[j]
            prices[j] = max(0.0, St - K) if is_call else max(0.0, K - St)

        # Backward induction con ejercicio anticipado
        for step in range(n - 1, -1, -1):
            for j in range(step + 1):
                Snode = S * u_pow[step - j] * d_pow[j]
                cont  = disc * (p_u * prices[j] + (1.0 - p_u) * prices[j + 1])
                intr  = max(0.0, Snode - K) if is_call else max(0.0, K - Snode)
                prices[j] = max(cont, intr)

        return prices[0]

    @numba.njit(fastmath=True, nogil=True)
    def _brent_iv(S: float, K: float, r: float, q: float, T: float,
                   mkt: float, n: int, lo: float, hi: float,
                   tol: float, max_iter: int, is_call: bool) -> float:
        """Brent's method: halla σ tal que LR_price(σ) = mkt."""
        fa = _lr_american_option(S, K, r, q, T, lo, n, is_call) - mkt
        fb = _lr_american_option(S, K, r, q, T, hi, n, is_call) - mkt

        if fa * fb > 0.0:
            return math.nan

        if abs(fa) < abs(fb):
            lo, hi = hi, lo
            fa, fb = fb, fa

        c  = lo;  fc = fa
        mflag = True;  d = 0.0;  s = 0.0

        for _ in range(max_iter):
            if abs(fb) < tol:
                return hi
            if abs(hi - lo) < tol:
                return (lo + hi) * 0.5

            if fa != fc and fb != fc:
                s = (lo * fb * fc / ((fa - fb) * (fa - fc))
                     + hi * fa * fc / ((fb - fa) * (fb - fc))
                     + c  * fa * fb / ((fc - fa) * (fc - fb)))
            else:
                s = hi - fb * (hi - lo) / (fb - fa)

            cond = (
                (s < (3.0 * lo + hi) * 0.25 or s > hi) or
                (mflag      and abs(s - hi) >= 0.5 * abs(hi - c)) or
                (not mflag  and abs(s - hi) >= 0.5 * abs(c - d)) or
                (mflag      and abs(hi - c) < tol) or
                (not mflag  and abs(c - d)  < tol)
            )
            if cond:
                s = (lo + hi) * 0.5
                mflag = True
            else:
                mflag = False

            fs = _lr_american_option(S, K, r, q, T, s, n, is_call) - mkt
            d = c;  c = hi;  fc = fb

            if fa * fs < 0.0:
                hi = s;  fb = fs
            else:
                lo = s;  fa = fs

            if abs(fa) < abs(fb):
                lo, hi = hi, lo
                fa, fb = fb, fa

        return (lo + hi) * 0.5

    @numba.njit(parallel=True, fastmath=True)
    def compute_iv_batch(S_arr:    np.ndarray,
                          K_arr:    np.ndarray,
                          r_arr:    np.ndarray,
                          q_arr:    np.ndarray,
                          T_arr:    np.ndarray,
                          mkt_arr:  np.ndarray,
                          n_steps:  int,
                          lo:       float,
                          hi:       float,
                          tol:      float,
                          max_iter: int,
                          is_call:  bool) -> np.ndarray:
        """
        Calcula IV para todas las filas en paralelo (numba.prange).
        Retorna NaN donde el cálculo no converge o los inputs son inválidos.
        """
        m  = len(S_arr)
        iv = np.full(m, math.nan)

        for i in numba.prange(m):
            S   = S_arr[i]
            K   = K_arr[i]
            r   = r_arr[i]
            q   = q_arr[i]
            T   = T_arr[i]
            mkt = mkt_arr[i]

            if T <= 0.0 or S <= 0.0 or K <= 0.0 or mkt <= 0.0:
                continue
            if is_call and mkt >= S:
                continue

            iv[i] = _brent_iv(S, K, r, q, T, mkt, n_steps, lo, hi, tol, max_iter, is_call)

        return iv

    def warmup(n_steps: int = 101) -> None:
        """Pre-compila las funciones JIT."""
        print("[IV] Compilando JIT (~5-10s)...", end="", flush=True)
        _S   = np.array([100.0], dtype=np.float64)
        _K   = np.array([100.0], dtype=np.float64)
        _r   = np.array([0.05],  dtype=np.float64)
        _q   = np.array([0.02],  dtype=np.float64)
        _T   = np.array([0.5],   dtype=np.float64)
        _mkt = np.array([5.0],   dtype=np.float64)
        compute_iv_batch(_S, _K, _r, _q, _T, _mkt, n_steps, 0.001, 20.0, 1e-5, 100, True)
        print(" listo.")

# ═══════════════════════════════════════════════════════════
# PATH B: Fallback scipy (sin numba)
# ═══════════════════════════════════════════════════════════
else:
    from scipy.optimize import brentq
    from scipy.stats import norm as _norm

    def _bs_call(S, K, r, q, T, sigma):
        """Black-Scholes europeo (fallback — aproximación para americanas)."""
        if T <= 0 or sigma <= 0 or S <= 0 or K <= 0:
            return max(0.0, S - K)
        sqT = math.sqrt(T)
        d1  = (math.log(S / K) + (r - q + 0.5 * sigma ** 2) * T) / (sigma * sqT)
        d2  = d1 - sigma * sqT
        return (S * math.exp(-q * T) * _norm.cdf(d1)
                - K * math.exp(-r * T) * _norm.cdf(d2))

    def compute_iv_batch(S_arr, K_arr, r_arr, q_arr, T_arr, mkt_arr,
                          n_steps, lo, hi, tol, max_iter):
        m  = len(S_arr)
        iv = np.full(m, np.nan)
        for i in range(m):
            S, K, r, q, T, mkt = (float(S_arr[i]), float(K_arr[i]),
                                   float(r_arr[i]), float(q_arr[i]),
                                   float(T_arr[i]), float(mkt_arr[i]))
            if T <= 0 or S <= 0 or mkt <= 0 or mkt >= S:
                continue
            try:
                fa = _bs_call(S, K, r, q, T, lo) - mkt
                fb = _bs_call(S, K, r, q, T, hi) - mkt
                if fa * fb < 0:
                    iv[i] = brentq(
                        lambda sig: _bs_call(S, K, r, q, T, sig) - mkt,
                        lo, hi, xtol=tol, maxiter=max_iter)
            except Exception:
                pass
        return iv

    def warmup(n_steps: int = 101) -> None:
        print("[IV] numba no disponible — usando fallback scipy.")
