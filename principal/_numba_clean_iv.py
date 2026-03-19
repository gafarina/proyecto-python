"""
_numba_clean_iv.py
==================
Motor Numba para Extirpación del Riesgo de Evento (Earnings Variance Stripping).

- Usa Días Hábiles (Business Days) base 252 en 
  vez de Días Calendario para cuantificar correctamente
  el tiempo transcurrido en el salto (el fin de semana 
  no diluye vol implied per-night).
- Calcula la Forward Variance ATM para despejar el w_evento.
- Retrata (Sustracción Vectorial) la varianza implicita 
  inyectada por el Earnings a todos los tenores de la curva posteriores.
"""

import math
import numpy as np

try:
    import numba
    _HAVE_NUMBA = True
except ImportError:
    _HAVE_NUMBA = False
    numba = None


# ====================================================================
# PRE-CALCULO FECHAS (VECTORIZADO FUERA DE JIT POR NP.DATETIME64)
# ====================================================================

def get_biz_days_array(start_dates: np.ndarray, end_dates: np.ndarray) -> np.ndarray:
    """
    Computa los business days (lunes viernes) entre dos numpy arrays de fechas datetime64[D].
    Retorna float64 con valor mínimo 0.001 (para evitar divide by zero intradía).
    """
    biz = np.busday_count(start_dates, end_dates).astype(np.float64)
    # Reemplazamos intraday / <=0 por una fracción minúscula 
    # (representando time-value remanente del día 0)
    return np.maximum(biz, 0.001)


# ====================================================================
# NUMBA LOGIC (C-LEVEL) FOR VARIANCE STRIPPING
# ====================================================================

if _HAVE_NUMBA:
    @numba.njit(parallel=True, fastmath=True)
    def compute_clean_iv_batch(
        iv_arr     : np.ndarray,  # array float64 (iv_call or iv_put)
        mkt_arr    : np.ndarray,  # array float64 callMidPrice
        K_arr      : np.ndarray,  # strike
        S_arr      : np.ndarray,  # stockPrice
        dte_arr    : np.ndarray,  # dias calendario de cada fila (ayuda general)
        biz_arr    : np.ndarray,  # dias habiles hasta expiracion
        group_idx  : np.ndarray,  # int32 array (id unico por (ticker, tradeDate))
        has_earning: np.ndarray,  # bool array: el grupo tiene un earning inminente?
        is_post_ern: np.ndarray,  # bool array: la expir_date de la fila > next_earning_date
    ) -> np.ndarray:
        """
        Extirpación iterativa del Event Risk.
        1) Busca por grupo el contrato ATM Pre-Earning mas cercano.
        2) Busca por grupo el contrato ATM Post-Earning mas cercano.
        3) Calcula $\Delta TV_{evento}$ como el delta de varianza a plazo (Forward Variance).
        4) Reduce la varianza total de cada contrato en is_post_ern removiendo el salto.
        """
        
        n_rows   = len(iv_arr)
        # Identificamos el máximo id de grupo para dimensionar buffers O(G)
        n_groups = int(np.max(group_idx)) + 1
        
        iv_clean = np.copy(iv_arr)
        
        # Buffers intermedios por grupo
        best_diff_pre  = np.full(n_groups, 1e9)
        best_diff_post = np.full(n_groups, 1e9)
        
        atm_iv_pre     = np.full(n_groups, math.nan)
        atm_iv_post    = np.full(n_groups, math.nan)
        
        biz_pre        = np.full(n_groups, math.nan)
        biz_post       = np.full(n_groups, math.nan)
        
        # 1. PASO: Busqueda del Contrato ATM representativo PRE y POST Earnings
        # No podemos usar prange para reduccion en arrs por grupo por race conditions, loop secuencial.
        # Es solo 10-15k iteraciones por archivo, toma microsegundos.
        for i in range(n_rows):
            g = group_idx[i]
            
            # Filtros basura:
            if not has_earning[g]: continue
            if math.isnan(iv_arr[i]) or iv_arr[i] <= 0.0: continue
            
            # Queremos usar el contrato mas liquidity/ATM
            # Idealmente delta near 50 (S~K)
            diff = abs(S_arr[i] - K_arr[i])
            
            # Pre-earning (antes o en la fecha de earnings)
            if not is_post_ern[i]:
                # Optimizamos fuertemente por cercania de TIME (dte penalizado x 1000), luego por ATM (diff)
                score = dte_arr[i] * 1000.0 + diff
                if score < best_diff_pre[g]:
                    best_diff_pre[g] = score
                    atm_iv_pre[g]    = iv_arr[i]
                    biz_pre[g]       = biz_arr[i]
                    
            # Post-earning (la primer expiracion luego del earning)
            else:
                score = dte_arr[i] * 1000.0 + diff
                if score < best_diff_post[g]:
                    best_diff_post[g] = score
                    atm_iv_post[g]    = iv_arr[i]
                    biz_post[g]       = biz_arr[i]


        # 2. PASO: Calcular el Salto de Varianza del Earning (w_evento)
        event_var_drop = np.zeros(n_groups)
        
        for g in range(n_groups):
            if not has_earning[g]: continue
            
            iv1 = atm_iv_pre[g]
            iv2 = atm_iv_post[g]
            t1  = biz_pre[g]   / 252.0
            t2  = biz_post[g]  / 252.0
            
            if not math.isnan(iv1) and not math.isnan(iv2) and t2 > t1:
                # Total Variance TV = IV^2 * T
                tv1 = (iv1 ** 2) * t1
                tv2 = (iv2 ** 2) * t2
                
                # La Varianza total que se añadio en (t1, t2] (aqui yace el salto)
                fw_tv = (tv2 - tv1)
                
                # Suponemos un base_variance_rate (vol normal diaria) de IV pre-earning.
                # Lo que resto de TV natural asumiendo la IV base, es el salto del evento
                tv_natural = (iv1 ** 2) * (t2 - t1)
                
                jump = fw_tv - tv_natural
                
                if jump > 0.0:
                    event_var_drop[g] = jump


        # 3. PASO: Stripping paralelo (Restar evento a todos los contratos post)
        for i in numba.prange(n_rows):
            g = group_idx[i]
            jump = event_var_drop[g]
            
            if jump > 0.0 and is_post_ern[i] and not math.isnan(iv_arr[i]):
                t = biz_arr[i] / 252.0
                tv_total = (iv_arr[i] ** 2) * t
                
                # Varianza limpia (residual post extirpacion)
                # Piso minimo 0.01 de TV empirico
                tv_clean = max(0.0001, tv_total - jump)
                
                # Convert back to IV
                iv_clean[i] = math.sqrt(tv_clean / t)
                
        return iv_clean

    def warmup() -> None:
        """Pre-compila la función JIT."""
        print("[IV CLEAN] Compilando motor JIT Stripping... ", end="", flush=True)
        _iv     = np.array([0.5, 0.6], dtype=np.float64)
        _mkt    = np.array([1.0, 1.0], dtype=np.float64)
        _K      = np.array([100, 100], dtype=np.float64)
        _S      = np.array([100, 100], dtype=np.float64)
        _dte    = np.array([5.0, 10.0], dtype=np.float64)
        _biz    = np.array([4.0,  8.0], dtype=np.float64)
        _grp    = np.array([0, 0], dtype=np.int32)
        _hs     = np.array([True], dtype=np.bool_)
        _pst    = np.array([False, True], dtype=np.bool_)
        
        compute_clean_iv_batch(_iv, _mkt, _K, _S, _dte, _biz, _grp, _hs, _pst)
        print("listo.")

else:
    # ------------------------------------------------------------------
    # FALLBACK (Sin Numba) 
    # Mismo algoritmo en Python puro
    # ------------------------------------------------------------------
    def compute_clean_iv_batch(iv_arr, mkt_arr, K_arr, S_arr, dte_arr, biz_arr, group_idx, has_earning, is_post_ern):
        n_rows   = len(iv_arr)
        n_groups = int(np.max(group_idx)) + 1 if n_rows > 0 else 0
        iv_clean = np.copy(iv_arr)
        
        best_diff_pre  = np.full(n_groups, 1e9)
        best_diff_post = np.full(n_groups, 1e9)
        atm_iv_pre     = np.full(n_groups, math.nan)
        atm_iv_post    = np.full(n_groups, math.nan)
        biz_pre        = np.full(n_groups, math.nan)
        biz_post       = np.full(n_groups, math.nan)
        
        for i in range(n_rows):
            g = group_idx[i]
            if not has_earning[g]: continue
            if math.isnan(iv_arr[i]) or iv_arr[i] <= 0: continue
            
            diff = abs(S_arr[i] - K_arr[i])
            if not is_post_ern[i]:
                score = dte_arr[i] * 1000.0 + diff
                if score < best_diff_pre[g]:
                    best_diff_pre[g] = score
                    atm_iv_pre[g]    = iv_arr[i]
                    biz_pre[g]       = biz_arr[i]
            else:
                score = dte_arr[i] * 1000.0 + diff
                if score < best_diff_post[g]:
                    best_diff_post[g] = score
                    atm_iv_post[g]    = iv_arr[i]
                    biz_post[g]       = biz_arr[i]

        event_var_drop = np.zeros(n_groups)
        for g in range(n_groups):
            if not has_earning[g]: continue
            iv1, iv2 = atm_iv_pre[g], atm_iv_post[g]
            t1, t2   = biz_pre[g] / 252.0, biz_post[g] / 252.0
            
            if not math.isnan(iv1) and not math.isnan(iv2) and t2 > t1:
                tv1 = (iv1 ** 2) * t1
                tv2 = (iv2 ** 2) * t2
                fw_tv = (tv2 - tv1)
                tv_natural = (iv1 ** 2) * (t2 - t1)
                jump = fw_tv - tv_natural
                if jump > 0.0:
                    event_var_drop[g] = jump

        for i in range(n_rows):
            g = group_idx[i]
            jump = event_var_drop[g]
            if jump > 0.0 and is_post_ern[i] and not math.isnan(iv_arr[i]):
                t = biz_arr[i] / 252.0
                tv_total = (iv_arr[i] ** 2) * t
                tv_clean = max(0.0001, tv_total - jump)
                iv_clean[i] = math.sqrt(tv_clean / t)
                
        return iv_clean

    def warmup() -> None:
        pass
