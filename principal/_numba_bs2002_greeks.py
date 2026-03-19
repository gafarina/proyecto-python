import numpy as np
import numba as nb
import math

# ────────────────────────────────────────────────────────────────────────
# 1. CORE MATEMÁTICO RÁPIDO: NORMALES CUMULATIVAS (1D)
# ────────────────────────────────────────────────────────────────────────

@nb.njit(cache=True, fastmath=False)
def _norm_cdf(x: float) -> float:
    """Distribución Normal Acumulada estándar."""
    if x < -8.0: return 0.0
    if x > 8.0: return 1.0
    g = 0.2316419
    a1 = 0.31938153
    a2 = -0.356563782
    a3 = 1.781477937
    a4 = -1.821255978
    a5 = 1.330274429
    k = 1.0 / (1.0 + g * abs(x))
    pdf = math.exp(-0.5 * x * x) / math.sqrt(2.0 * math.pi)
    cdf1 = 1.0 - pdf * (a1*k + a2*(k**2) + a3*(k**3) + a4*(k**4) + a5*(k**5))
    return 1.0 - cdf1 if x < 0.0 else cdf1

@nb.njit(cache=True, fastmath=False)
def safe_div(num: float, den: float, fallback: float = 1e-12) -> float:
    if abs(den) < fallback:
        den = fallback if den >= 0 else -fallback
    return num / den

@nb.njit(cache=True, fastmath=False)
def safe_log(x: float) -> float:
    return math.log(max(x, 1e-12))

# ────────────────────────────────────────────────────────────────────────
# 2. MOTOR VALORACIÓN CERRADA: BJERKSUND-STENSLAND (1993) - Exact Implementation
# ────────────────────────────────────────────────────────────────────────

@nb.njit(cache=True, fastmath=False)
def _phi_core(S: float, T: float, gamma: float, H: float, I: float, r: float, b: float, v: float) -> float:
    v_sqrt_T = v * math.sqrt(T) + 1e-14
    
    # Safe limits to avoid div by zero inside log
    S_safe = max(S, 1e-12)
    H_safe = max(H, 1e-12)
    I_safe = max(I, 1e-12)
    
    d1 = -(safe_log(S_safe / H_safe) + (b + (gamma - 0.5) * (v ** 2)) * T) / v_sqrt_T
    d2 = d1 - 2.0 * safe_log(I_safe / S_safe) / v_sqrt_T
    lambda1 = -r + gamma * b + 0.5 * gamma * (gamma - 1.0) * (v ** 2)
    
    v2_safe = max(v ** 2, 1e-12)
    kappa = safe_div(2.0 * b, v2_safe) + (2.0 * gamma - 1.0)
    
    I_over_S = I_safe / S_safe
    prob2 = _norm_cdf(d2)
    
    # Secure bound against float64 IEEE numeric overflow (max 10^308)
    if I_over_S > 1.0 and kappa > 0.0:
        if kappa * math.log10(I_over_S) > 280.0:
            term2 = 0.0
        else:
            term2 = (I_over_S ** kappa) * prob2
    else:
        term2 = (I_over_S ** kappa) * prob2
        
    return math.exp(lambda1 * T) * (_norm_cdf(d1) - term2)


@nb.njit(cache=True, fastmath=False)
def _bs2002_call(S: float, K: float, T: float, r: float, b: float, v: float) -> float:
    # Bjerksund-Stensland 1993 core (re-used name for FDM linkage)
    
    # European BS Boundary when Early Exercise is not optimal OR Volatility is too low (Overflow Prevention)
    if b >= r or v < 0.035:
        if T <= 0.0: return max(0.0, S - K)
        S_safe_eu = max(S, 1e-12)
        K_safe_eu = max(K, 1e-12)
        d1 = (safe_log(S_safe_eu / K_safe_eu) + (b + 0.5 * v**2) * T) / (v * math.sqrt(T) + 1e-14)
        d2 = d1 - v * math.sqrt(T)
        eu_val = S * math.exp((b - r) * T) * _norm_cdf(d1) - K * math.exp(-r * T) * _norm_cdf(d2)
        return max(eu_val, S - K, 0.0)
    
    if T <= 0.0: return max(0.0, S - K)

    v2 = max(v ** 2, 1e-12)
    sqrt_t = math.sqrt(T)

    beta_inside = abs(((b / v2 - 0.5) ** 2) + 2.0 * r / v2)
    beta = (0.5 - b / v2) + math.sqrt(beta_inside)
    
    b_infinity = safe_div(beta, beta - 1.0) * K
    b_zero = max(K, safe_div(r, r - b) * K)

    denom = b_infinity - b_zero
    if abs(denom) < 1e-8:
        h1 = 0.0
    else:
        h1 = -(b * T + 2.0 * v * sqrt_t) * (b_zero / denom)
        
    I = b_zero + (b_infinity - b_zero) * (1.0 - math.exp(h1))
    alpha = (I - K) * (I ** (-beta))

    if S >= I:
        value = S - K
    else:
        S_safe_bs = max(S, 1e-12)
        I_safe_bs = max(I, 1e-12)
        S_ratio = S_safe_bs / I_safe_bs
        S_ratio_beta = S_ratio ** beta
        
        phi_beta_I_I = _phi_core(S, T, beta, I, I, r, b, v)
        phi_1_I_I    = _phi_core(S, T, 1.0, I, I, r, b, v)
        phi_1_K_I    = _phi_core(S, T, 1.0, K, I, r, b, v)
        phi_0_I_I    = _phi_core(S, T, 0.0, I, I, r, b, v)
        phi_0_K_I    = _phi_core(S, T, 0.0, K, I, r, b, v)
        
        value = ((I - K) * S_ratio_beta
                 - (I - K) * S_ratio_beta * phi_beta_I_I
                 + S * phi_1_I_I
                 - S * phi_1_K_I
                 - K * phi_0_I_I
                 + K * phi_0_K_I)

    # Boundary conditions enforcement compared to European value
    S_safe_e2 = max(S, 1e-12)
    K_safe_e2 = max(K, 1e-12)
    e_d1 = (safe_log(S_safe_e2 / K_safe_e2) + (b + 0.5 * v2) * T) / (v * math.sqrt(T) + 1e-14)
    e_d2 = e_d1 - v * math.sqrt(T)
    e_value = S * math.exp((b - r) * T) * _norm_cdf(e_d1) - K * math.exp(-r * T) * _norm_cdf(e_d2)
                 
    return max(value, e_value, 0.0)


@nb.njit(fastmath=False)
def _bs2002_option(S: float, K: float, T: float, r: float, b: float, v: float, is_call: bool) -> float:
    if is_call:
        return _bs2002_call(S, K, T, r, b, v)
    else:
        # Bjerksund-Stensland symmetry for Puts
        return _bs2002_call(K, S, T, r - b, -b, v)

# ────────────────────────────────────────────────────────────────────────
# 3. MÁQUINA DE DIFERENCIAS FINITAS MULTIDIMENSIONAL (FDM) (13 Griegas)
# ────────────────────────────────────────────────────────────────────────

@nb.njit(fastmath=False)
def calc_greeks_fdm(S: float, K: float, T: float, r: float, q: float, v: float, is_call: bool) -> tuple:
    b = r - q
    h_S = max(S * 1e-4, 0.01) 
    h_v = 0.01 
    h_r = 0.0001
    h_t = 1.0 / 365.25
    
    # 0 return si expiró o Vol 0 (Cortafuegos Inicial) con proteccion extrema K<=0 y S<=0
    if T <= 0.0 or v <= 1e-4 or S <= 1e-4 or K <= 1e-4:
        if is_call:
            de = 1.0 if S > K else 0.0
            return (max(0.0, S - K), de, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)
        else:
            de = -1.0 if S < K else 0.0
            return (max(0.0, K - S), de, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)
            
    price = _bs2002_option(S, K, T, r, b, v, is_call)

    # 1. Delta (1st Order S)
    p_up_S = _bs2002_option(S + h_S, K, T, r, r-q, v, is_call)
    p_dn_S = _bs2002_option(S - h_S, K, T, r, r-q, v, is_call)
    delta = (p_up_S - p_dn_S) / (2.0 * h_S)
    
    # Cortafuegos FDM para Griegas de Orden Superior
    if is_call:
        if delta < 1e-4 or delta > 0.9999:
            return (price, delta, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)
    else:
        if delta > -1e-4 or delta < -0.9999:
            return (price, delta, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)
    
    # 2. Gamma (2nd Order S)
    gamma = (p_up_S - 2.0 * price + p_dn_S) / (h_S**2)
    
    # 3. Vega (1st Order V)
    p_up_v = _bs2002_option(S, K, T, r, r-q, v + h_v, is_call)
    p_dn_v = _bs2002_option(S, K, T, r, r-q, max(1e-4, v - h_v), is_call)
    vega = (p_up_v - p_dn_v) / (2.0 * h_v) / 100.0
    
    # 4. Rho (1st Order R)
    p_up_r = _bs2002_option(S, K, T, r + h_r, (r+h_r)-q, v, is_call)
    p_dn_r = _bs2002_option(S, K, T, max(0.0, r - h_r), max(0.0, r-h_r)-q, v, is_call)
    rho = (p_up_r - p_dn_r) / (2.0 * h_r) / 100.0
    
    # 5. Theta (Forward Dif Time)
    t_fwd = max(0.0, T - h_t)
    p_fwd_t = _bs2002_option(S, K, t_fwd, r, r-q, v, is_call)
    theta = (p_fwd_t - price) / h_t / 252.0
    
    # 6. Vanna (Cross Dif S/V)
    p_upS_upV = _bs2002_option(S + h_S, K, T, r, b, v + h_v, is_call)
    p_upS_dnV = _bs2002_option(S + h_S, K, T, r, b, max(1e-4, v - h_v), is_call)
    p_dnS_upV = _bs2002_option(S - h_S, K, T, r, b, v + h_v, is_call)
    p_dnS_dnV = _bs2002_option(S - h_S, K, T, r, b, max(1e-4, v - h_v), is_call)
    vanna = (p_upS_upV - p_upS_dnV - p_dnS_upV + p_dnS_dnV) / (4.0 * h_S * h_v)
    
    # 7. Charm (Cross Dif S/T)
    p_upS_fwdT = _bs2002_option(S + h_S, K, t_fwd, r, b, v, is_call)
    p_dnS_fwdT = _bs2002_option(S - h_S, K, t_fwd, r, b, v, is_call)
    delta_fwd = (p_upS_fwdT - p_dnS_fwdT) / (2.0 * h_S)
    charm = -(delta_fwd - delta) / h_t / 252.0
    
    # 8. Speed (3rd Order S - 5 Points)
    p_upS2 = _bs2002_option(S + 2.0*h_S, K, T, r, b, v, is_call)
    p_dnS2 = _bs2002_option(max(0.001, S - 2.0*h_S), K, T, r, b, v, is_call)
    speed = (-p_dnS2 + 2.0*p_dn_S - 2.0*p_up_S + p_upS2) / (2.0 * (h_S**3))
    
    # 9. Zomma (Cross Dif 3rd Order S2/V)
    g_up_v = (p_upS_upV - 2.0 * p_up_v + p_dnS_upV) / (h_S**2)
    g_dn_v = (p_upS_dnV - 2.0 * p_dn_v + p_dnS_dnV) / (h_S**2)
    zomma = (g_up_v - g_dn_v) / (2.0 * h_v) / 100.0

    # 10. Color (-Cross Dif Gamma/Time Forward)
    gamma_fwdT = (p_upS_fwdT - 2.0*p_fwd_t + p_dnS_fwdT) / (h_S**2)
    color = -(gamma_fwdT - gamma) / h_t / 252.0
    
    # 11. Vomma / Volga (2nd Order V)
    vomma = (p_up_v - 2.0*price + p_dn_v) / (h_v**2) / 100.0
    
    # 12. Ultima (3rd Order V - 5 Points)
    p_upV2 = _bs2002_option(S, K, T, r, r-q, v + 2.0*h_v, is_call)
    p_dnV2 = _bs2002_option(S, K, T, r, r-q, max(1e-4, v - 2.0*h_v), is_call)
    ultima = (-p_dnV2 + 2.0*p_dn_v - 2.0*p_up_v + p_upV2) / (2.0 * (h_v**3)) / 100.0

    return (price, delta, gamma, theta, vega, rho, vanna, charm, speed, zomma, color, vomma, ultima)

@nb.njit(parallel=True, fastmath=True)
def compute_all_greeks_batch(
    spot_arr: np.ndarray,
    strike_arr: np.ndarray,
    dte_arr: np.ndarray,       
    rate_arr: np.ndarray,
    div_yield_arr: np.ndarray,
    iv_arr: np.ndarray,
    is_call: bool
) -> tuple:
    N = len(spot_arr)
    
    out_price = np.empty(N, dtype=np.float64)
    out_delta = np.empty(N, dtype=np.float64)
    out_gamma = np.empty(N, dtype=np.float64)
    out_theta = np.empty(N, dtype=np.float64)
    out_vega  = np.empty(N, dtype=np.float64)
    out_rho   = np.empty(N, dtype=np.float64)
    out_vanna = np.empty(N, dtype=np.float64)
    out_charm = np.empty(N, dtype=np.float64)
    out_speed = np.empty(N, dtype=np.float64)
    out_zomma = np.empty(N, dtype=np.float64)
    out_color = np.empty(N, dtype=np.float64)
    out_vomma = np.empty(N, dtype=np.float64)
    out_ultima = np.empty(N, dtype=np.float64)
    
    for i in nb.prange(N):
        S = spot_arr[i]
        K = strike_arr[i]
        T = dte_arr[i] / 252.0  
        r = rate_arr[i]
        q = min(div_yield_arr[i], 0.40) 
        v = max(iv_arr[i], 0.0001)      
        
        pr, de, ga, th, ve, rh, va, ch, sp, zo, co, vo, ul = calc_greeks_fdm(S, K, T, r, q, v, is_call)
        
        out_price[i] = pr
        out_delta[i] = de
        out_gamma[i] = ga
        out_theta[i] = th
        out_vega[i]  = ve
        out_rho[i]   = rh
        out_vanna[i] = va
        out_charm[i] = ch
        out_speed[i] = sp
        out_zomma[i] = zo
        out_color[i] = co
        out_vomma[i] = vo
        out_ultima[i] = ul
        
    return (out_price, out_delta, out_gamma, out_theta, out_vega, 
            out_rho, out_vanna, out_charm, out_speed, out_zomma,
            out_color, out_vomma, out_ultima)
