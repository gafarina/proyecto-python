import os
import glob
import time
import numpy as np
import polars as pl
from _numba_bs2002_greeks import compute_all_greeks_batch


def process_file(filepath: str, option_type: str = "call"):
    opt_type_l = option_type.lower()
    is_call = (opt_type_l == "call")
    
    try:
        df = pl.read_parquet(filepath)
    except Exception as e:
        print(f"[ERROR] Corrupt file {filepath}: {e}")
        return False
        
    iv_col = f"iv_{opt_type_l}"
    req_cols = ["stockPrice", "strike", "dte", "risk_free_rate", "div_yield_cont", iv_col]
    missing = [c for c in req_cols if c not in df.columns]
    
    if missing:
        print(f"[SKIP] Falta {missing} en {os.path.basename(filepath)}")
        return False

    # Skip files that already have the new FDM greeks (Incremental check)
    if f"speed_clean_fdm" in df.columns \
       or f"ultima_clean_fdm" in df.columns:
        return True

    N = len(df)
    if N == 0:
        return True
        
    # Extraer Tensores Float64 strictly
    spot_arr   = df["stockPrice"].to_numpy().astype(np.float64)
    strike_arr = df["strike"].to_numpy().astype(np.float64)
    dte_arr    = df["dte"].to_numpy().astype(np.float64)
    rate_arr   = df["risk_free_rate"].to_numpy().astype(np.float64)
    div_arr    = df["div_yield_cont"].to_numpy().astype(np.float64)
    iv_opt_arr = df[iv_col].to_numpy().astype(np.float64)
    
    if "iv_clean" in df.columns:
        iv_clean_arr = df["iv_clean"].to_numpy().astype(np.float64)
    else:
        iv_clean_arr = iv_opt_arr.copy()

    # Evaluar Bjerksund-Stensland 1993 con FDM sobre IV Base
    results_base = compute_all_greeks_batch(
        spot_arr, strike_arr, dte_arr, rate_arr, div_arr, iv_opt_arr, is_call
    )
    
    # Evaluar Bjerksund-Stensland 1993 con FDM sobre IV Clean
    results_clean = compute_all_greeks_batch(
        spot_arr, strike_arr, dte_arr, rate_arr, div_arr, iv_clean_arr, is_call
    )
    
    prefixes = [
        "price_bs2002", "delta_fdm", "gamma_fdm", "theta_fdm", "vega_fdm",
        "rho_fdm", "vanna_fdm", "charm_fdm", "speed_fdm", "zomma_fdm",
        "color_fdm", "vomma_fdm", "ultima_fdm"
    ]
    
    cols = []
    for i, pref in enumerate(prefixes):
        if pref in ["vanna_fdm", "speed_fdm", "ultima_fdm"]:
            c_base = np.clip(results_base[i], -100.0, 100.0)
            c_clean = np.clip(results_clean[i], -100.0, 100.0)
        else:
            c_base = results_base[i]
            c_clean = results_clean[i]
            
        base_name = pref.replace("_fdm", f"_{opt_type_l}_fdm").replace("price_bs2002", f"price_{opt_type_l}_bs2002")
        clean_name = pref.replace("_fdm", "_clean_fdm").replace("price_bs2002", "price_clean_bs2002")
        
        cols.append(pl.Series(base_name, c_base))
        cols.append(pl.Series(clean_name, c_clean))
        
    out_df = df.with_columns(cols)
    
    # Purgar viejas columnas sin prefijo de IV
    old_cols = [c for c in out_df.columns if c in [
        "price_bs2002", "delta_fdm", "gamma_fdm", "theta_fdm", "vega_fdm",
        "rho_fdm", "vanna_fdm", "charm_fdm", "speed_fdm", "zomma_fdm"
    ]]
    if old_cols:
        out_df = out_df.drop(old_cols)
    
    # Escritura Atómica
    tmp_path = filepath + ".tmp"
    out_df.write_parquet(tmp_path, compression="snappy")
    os.replace(tmp_path, filepath)
    
    return True

def process_pipeline_files(filepaths: list, option_type: str = "call"):
    opt_type_l = option_type.lower()
    is_call = (opt_type_l == "call")

    print(f"============================================================")
    print(f"   Iniciando Pipeline de Griegas FDM Alto Orden BS1993 ({opt_type_l.upper()})")
    print(f"============================================================")
    print(f"Directorios: {len(filepaths)} archivos `.parquet` detectados.")
    print("Pre-compilando C-JIT Core... (por favor espere)")
    
    # Warmup compiler
    _ = compute_all_greeks_batch(
        np.array([100.0]), np.array([100.0]), np.array([30.0]),
        np.array([0.05]), np.array([0.0]), np.array([0.2]), is_call
    )
    
    t0 = time.time()
    processed = 0
    errors = 0
    
    for idx, f in enumerate(filepaths):
        success = process_file(f, option_type=opt_type_l)
        if success:
            processed += 1
        else:
            errors += 1
            
        if (idx + 1) % 50 == 0 or (idx + 1) == len(filepaths):
            print(f"   Progreso: {idx + 1}/{len(filepaths)} archivos...")
            
    t1 = time.time()
    
    print(f"============================================================")
    print(f"✓ FDM {opt_type_l.upper()} Pipeline Completado en {(t1-t0):.2f} segundos.")
    print(f"  Procesados/Omitidos : {processed}")
    print(f"  Errores             : {errors}")
    print(f"============================================================")

def run_greeks_batch():
    base_dir = r"C:\datos_proyecto\ruedas_call_earn"
    all_files = sorted(glob.glob(os.path.join(base_dir, "call_*.parquet")))
    process_pipeline_files(all_files, "call")
    
    base_dir_put = r"C:\datos_proyecto\ruedas_put_earn"
    all_files_put = sorted(glob.glob(os.path.join(base_dir_put, "put_*.parquet")))
    if all_files_put:
        process_pipeline_files(all_files_put, "put")

if __name__ == "__main__":
    run_greeks_batch()
