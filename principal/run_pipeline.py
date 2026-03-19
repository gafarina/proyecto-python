import os
import sys

# ==============================================================================
# CONFIGURACIÓN GLOBAL DEL PIPELINE
# ==============================================================================
API_KEY       = "43af86de-fd09-4fc4-b780-6a301d267cb2"
FMP_API_KEY   = "xjnhJX6n8NP06Igh3DhHjA8qLOl4i09I"
ROOT_DIR      = r"C:\datos_proyecto"
BASE_DATA_DIR = os.path.join(ROOT_DIR, "datos_cores")
LIVE_DATA_DIR = os.path.join(ROOT_DIR, "datos_live")


def main():
    """
    Pipeline de datos ORATS — orquestación diaria.

    Pasos:
        1. Descarga Histórica Incremental  – días faltantes desde 2024.
        2. Verificación de Integridad      – detecta gaps y archivos corruptos.
        3. Universo Global (Top 2000)      – universo amplio por volumen.
        4. Universo Diario Top 200         – {fecha:[tickers]}, propagación 90d.
        5. Descarga de Earnings            – maestro desde 2019 (Benzinga).
        6. Generación de Ruedas Filtradas  – call_*.parquet con filtros de calidad.
        7. Filtro min-strikes               – elimina (ticker,dte) con <4 strikes.
        8. Enriquecimiento con Earnings    – prev/next_earning_date (join_asof).
        9. Enriquecimiento FMP             – div_yield_cont, risk_free_rate,
                                            next_div_date (Clamped Cubic Spline).
       10. Volatilidad Implícita           – iv_call via árbol LR-Brent + Numba.
       11. Volatilidad Limpia              – iv_clean (Earnings Variance Stripping).
    """
    print("=" * 60)
    print("       PIPELINE DE DATOS ORATS")
    print("=" * 60)

    from orats_data_manager import (
        OratsDataManager,
        OratsEarningsDownloader,
        UniverseBuilder,
        WheelsCallEarnBuilder,
        WheelsPutEarnBuilder,
        FMPDataEnricher,
        IVEnricher,
        CleanIVEnricher,
    )
    from quantitative_antigravity_protocol import process_single_day

    manager = OratsDataManager(API_KEY, BASE_DATA_DIR, LIVE_DATA_DIR)

    # ------------------------------------------------------------------
    # PASO 1 – Descarga Histórica Incremental
    # ------------------------------------------------------------------
    print("\n>>> PASO 1: Actualizando Historial Core (desde 2019-06-01)...")
    manager.download_history(start_date_str="2019-06-01")

    # ------------------------------------------------------------------
    # PASO 2 – Verificación de Integridad
    # ------------------------------------------------------------------
    print("\n>>> PASO 2: Verificando Integridad de Datos...")
    manager.check_integrity(start_date_str="2019-06-01")



    # ------------------------------------------------------------------
    # PASO 4 – Universo Global Histórico Top 50 (Volumen)
    # Lista de todos los tickers que alguna vez estuvieron en el top 50
    # de mayor volumen histórico (calls + puts).
    # ------------------------------------------------------------------
    print("\n>>> PASO 4: Construyendo Universo Global Histórico (Top 50 por Volumen, Mínimo 20 días)...")
    import polars as pl
    import glob
    import json
    import collections
    
    core_files = glob.glob(os.path.join(BASE_DATA_DIR, "*.parquet"))
    ticker_counts = collections.Counter()
    
    print(f"Escaneando {len(core_files)} archivos históricos para extraer el Top 50 Volume...")
    for f in core_files:
        try:
            lf = pl.scan_parquet(f)
            schema = lf.collect_schema().names()
            
            cvol_col = "callVolume" if "callVolume" in schema else "cVolu" if "cVolu" in schema else "cVol" if "cVol" in schema else None
            pvol_col = "putVolume" if "putVolume" in schema else "pVolu" if "pVolu" in schema else "pVol" if "pVol" in schema else None
            
            alias_exprs = []
            if cvol_col: alias_exprs.append(pl.col(cvol_col).fill_null(0).alias("cVol"))
            else: alias_exprs.append(pl.lit(0).alias("cVol"))
            if pvol_col: alias_exprs.append(pl.col(pvol_col).fill_null(0).alias("pVol"))
            else: alias_exprs.append(pl.lit(0).alias("pVol"))
            
            top_50_day = (
                lf.with_columns(alias_exprs)
                .group_by("ticker")
                .agg([
                    (pl.col("cVol") + pl.col("pVol")).sum().alias("total_vol")
                ])
                .filter(~pl.col("ticker").str.contains("_C"))
                .sort("total_vol", descending=True)
                .limit(50)
                .select("ticker")
                .collect()
            )
            ticker_counts.update(top_50_day["ticker"].to_list())
        except Exception as e:
            print(f"Error parseando {f}: {e}")
            
    # Filtrar solo aquellos que tienen >= 20 días de presencia
    lista_final = sorted([ticker for ticker, count in ticker_counts.items() if count >= 20])
    
    print(f"\n[ÉXITO] Número final de tickers en el universo top 50 (>20 días): {len(lista_final)}")
    print(f"Muestra extraídos: {lista_final[:50]}...\n")
    
    universo_path = os.path.join(ROOT_DIR, "universo", "universo.json")
    os.makedirs(os.path.dirname(universo_path), exist_ok=True)
    with open(universo_path, "w", encoding="utf-8") as fh:
        json.dump(lista_final, fh, indent=4)



    # ------------------------------------------------------------------
    # PASO 4.5 – Descargar Ruedas de Opciones para el Universo
    # ------------------------------------------------------------------
    print("\n>>> PASO 4.5: Descargando Ruedas (Option Chains) para el Universo...")
    from orats_data_manager import OratsWheelsDownloader
    import datetime

    wheels_mgr = OratsWheelsDownloader(api_key=API_KEY, base_dir=ROOT_DIR)
    
    with open(universo_path, "r", encoding="utf-8") as fh:
        universo_diario = json.load(fh)

    ruedas_dir = os.path.join(ROOT_DIR, "ruedas")
    os.makedirs(ruedas_dir, exist_ok=True)

    if isinstance(universo_diario, list):
        fechas_core = [os.path.basename(f).replace(".parquet", "") for f in core_files]
        universo_iterable = {d: universo_diario for d in fechas_core}.items()
    else:
        universo_iterable = universo_diario.items()

    # ------------------------------------------------------------------
    # CONFIGURACIÓN DE AUDITORÍA WHEELS
    # Cambiar a True para revisar exhaustivamente tickers faltantes (equivalente a "s").
    # Cambiar a False para salto rápido si el archivo ya existe (equivalente a "n").
    # ------------------------------------------------------------------
    do_deep_check = False  # <-- Cambiar a True para realizar parcheo profundo

    if do_deep_check:
        print("\n[WHEELS] Modo Auditoría Profunda ACTIVADO (Revisando cada ticker individualmente).")
    else:
        print("\n[WHEELS] Modo Rápido ACTIVADO (Saltando días que ya tengan un archivo creado).")

    for date_str, tickers in universo_iterable:
        try:
            dt = datetime.datetime.strptime(date_str, "%Y-%m-%d").date()
            wheels_mgr.process_day(
                dt, 
                tickers, 
                endpoint="hist", 
                save_mode="append", 
                fast_check=not do_deep_check
            )
        except ValueError:
            pass

    # ------------------------------------------------------------------
    # PASO 5 – Descarga de Earnings (Incremental, desde 2019)
    # Nuevos tickers: descarga completa desde 2019-01-01.
    # Tickers existentes: últimos 45 días.
    # ------------------------------------------------------------------
    print("\n>>> PASO 5: Actualizando Earnings (desde 2019-01-01)...")
    earnings_mgr = OratsEarningsDownloader(api_key=API_KEY, base_dir=ROOT_DIR)
    earnings_mgr.download_earnings_history(start_date="2019-01-01")

    # ------------------------------------------------------------------
    # Rutas compartidas para los pasos 6, 7, 8 y 9
    # ------------------------------------------------------------------
    RUEDAS_DIR    = os.path.join(ROOT_DIR, "ruedas")
    CALL_EARN_DIR = os.path.join(ROOT_DIR, "ruedas_call_earn")
    PUT_EARN_DIR  = os.path.join(ROOT_DIR, "ruedas_put_earn")
    UNIVERSO_PATH = os.path.join(ROOT_DIR, "universo", "universo.json")
    EARNINGS_PATH = os.path.join(ROOT_DIR, "earnings", "universe_earnings.parquet")
    CACHE_DIR     = os.path.join(ROOT_DIR, "enrichment_cache")

    put_builder = WheelsPutEarnBuilder(
        universo_path=UNIVERSO_PATH,
        ruedas_dir=os.path.join(ROOT_DIR, "ruedas"),
        output_dir=PUT_EARN_DIR,
    )

    call_builder = WheelsCallEarnBuilder(
        universo_path=UNIVERSO_PATH,
        ruedas_dir=RUEDAS_DIR,
        output_dir=CALL_EARN_DIR,
    )

    # ------------------------------------------------------------------
    # PASO 6 – Generación de Ruedas Filtradas
    # Para cada fecha del universo: lee rueda_{fecha}.parquet, filtra al
    # universo del día y aplica criterios de calidad sobre calls:
    #   · Strike >= stockPrice (ATM/OTM)
    #   · callOpenInterest > 0, callVolume > 0
    #   · callBidPrice > 0, callAskPrice > 0
    #   · spread: callAsk/callBid <= 1.40
    # Calcula callMidPrice = (bid + ask) / 2.
    # Guarda call_{fecha}.parquet en ruedas_call_earn/ (incremental).
    # ------------------------------------------------------------------
    print("\n>>> PASO 6: Generando Ruedas Filtradas (calls con calidad)...")
    call_builder.build()
    print("\n>>> PASO 6 (Puts): Generando Ruedas Filtradas (puts con calidad)...")
    put_builder.build()

    # ------------------------------------------------------------------
    # PASO 7 – Filtro de mínimo de strikes por (ticker, dte)
    # Garantiza que cada par (ticker, dte) tenga al menos 4 puntos
    # de strike. Si no cumple, se elimina ese dte completo del archivo.
    # Incremental: sólo sobreescribe si hubo cambios en el archivo.
    # ------------------------------------------------------------------
    print("\n>>> PASO 7: Filtrando mínimo 4 strikes por (ticker, dte)...")
    call_builder.filter_min_strikes(min_strikes=4)
    put_builder.filter_min_strikes(min_strikes=4)

    # ------------------------------------------------------------------
    # PASO 8 – Enriquecimiento con Fechas de Earnings
    # Agrega prev_earning_date y next_earning_date a cada call_*.parquet
    # usando join_asof de Polars (incremental).
    # ------------------------------------------------------------------
    print("\n>>> PASO 8: Enriqueciendo con Fechas de Earnings...")
    call_builder.enrich_with_earnings(earnings_path=EARNINGS_PATH)
    put_builder.enrich_with_earnings(earnings_path=EARNINGS_PATH)

    # ------------------------------------------------------------------
    # PASO 8.5 – Actualización de Precios Históricos FMP (Stocks)
    # Descarga/Actualiza el precio diario de los subyacentes.
    # Necesario para cálculos posteriores sin depender sólo de la rueda.
    # ------------------------------------------------------------------
    print("\n>>> PASO 8.5 (A): Actualizando Precios de Acciones FMP (Universo Base)...")
    try:
        from fmp_ticker_updater import run_fmp_update
        run_fmp_update(mode='daily')
    except Exception as e:
        print(f"[ERROR] Falló la actualización de precios FMP Universo Base: {e}")

    print("\n>>> PASO 8.5 (B): Actualizando Precios de Acciones FMP (Top 3000 desde 2019)...")
    try:
        from fmp_ticker_updater import run_fmp_update
        run_fmp_update(mode='top3000')
    except Exception as e:
        print(f"[ERROR] Falló la actualización de precios FMP Top 3000: {e}")

    # El pipeline ahora continuará hacia el Paso 9

    # ------------------------------------------------------------------
    # PASO 9 – Enriquecimiento con Dividendos y Tasas (FMP)
    # Agrega:
    #   · div_yield_cont  = ln(1 + ΣadjDivTTM / stockPrice)  [continua]
    #   · next_div_date   = próximo pago de dividendo post tradeDate
    #   · risk_free_rate  = tasa del tesoro continua interpolada por DTE
    #                       BEY → APY = (1+BEY/2)^2-1 → rc = ln(1+APY)
    #                       Clamped Cubic Spline (Actual/365)
    # Caché: enrichment_cache/dividends/ (TTL 7d) y treasury/ (permanente).
    # Incremental: omite archivos que ya tienen 'risk_free_rate'.
    # ------------------------------------------------------------------
    print("\n>>> PASO 9: Enriqueciendo con Dividendos y Tasas (FMP)...")
    fmp_enricher = FMPDataEnricher(
        fmp_api_key=FMP_API_KEY,
        input_dir=CALL_EARN_DIR,
        option_type="call",
        cache_dir=CACHE_DIR,
    )
    fmp_enricher.enrich()

    fmp_enricher_put = FMPDataEnricher(
        fmp_api_key=FMP_API_KEY,
        input_dir=PUT_EARN_DIR,
        option_type="put",
        cache_dir=CACHE_DIR,
    )
    fmp_enricher_put.enrich()

    # El pipeline continuará hacia el Paso 10

    # ------------------------------------------------------------------
    # PASO 10 – Volatilidad Implícita (LR-Brent + Numba)
    # Agrega iv_call via árbol Leisen-Reimer (101 pasos) con:
    #   · Dividendos discretos: S_adj = stockPrice - PV(divs en (t0, T])
    #   · Inversión con Brent's method (robusto en alas OTM, sin vega)
    #   · numba.prange paralelo sobre todas las filas del archivo
    # Incremental: omite archivos que ya tienen columna iv_call.
    # ------------------------------------------------------------------
    print("\n>>> PASO 10: Calculando Volatilidad Implícita (LR-Brent + Numba)...")
    iv_enricher = IVEnricher(
        input_dir=CALL_EARN_DIR,
        option_type="call",
        cache_dir=CACHE_DIR,
    )
    iv_enricher.enrich()
    
    iv_enricher_puts = IVEnricher(
        input_dir=PUT_EARN_DIR,
        option_type="put",
        cache_dir=CACHE_DIR,
    )
    iv_enricher_puts.enrich()

    # El pipeline continuará hacia el Paso 11

    # ------------------------------------------------------------------
    # PASO 11 – Extirpación Volatilidad de Evento (Clean IV)
    # Crea 'iv_clean' restando la Forward Variance asociada al Evento
    # midiendo Días Hábiles vs Días Calendario.
    # ------------------------------------------------------------------
    print("\n>>> PASO 11: Extirpación de Varianza de Evento (Clean IV)...")
    clean_iv_enricher = CleanIVEnricher(
        input_dir=CALL_EARN_DIR,
        option_type="call"
    )
    clean_iv_enricher.enrich()

    clean_iv_enricher_puts = CleanIVEnricher(
        input_dir=PUT_EARN_DIR,
        option_type="put"
    )
    clean_iv_enricher_puts.enrich()
    
    # El pipeline continuará hacia el Paso 11.5

    # ------------------------------------------------------------------
    # PASO 11.5 – Precios y Griegas Multi-Diferencias (FDM + BS2002)
    # Genera todas las griegas 1er/2do/3er orden
    # ------------------------------------------------------------------
    print("\n>>> PASO 11.5: Generando Precios BS2002 y Griegas FDM...")
    from process_greeks_fdm import run_greeks_batch
    run_greeks_batch()

    print("\n[INFO] Ejecución detenida después del Paso 11.5 por configuración actual.")
    return


    # ------------------------------------------------------------------
    # PASO 11.6 - Superficies Históricas (SSVI / Anti-Gravedad)
    # Genera la cuadrícula densa para cada día histórico (Incremental)
    # ------------------------------------------------------------------
    print("\n>>> PASO 11.6: Generando Superficies SSVI Históricas (Incremental)...")
    from quantitative_antigravity_protocol import process_single_day
    import pandas as pd
    
    out_dir_calls = os.path.join(ROOT_DIR, "volatility_surface")
    out_dir_puts = os.path.join(ROOT_DIR, "volatility_surface_put")
    os.makedirs(out_dir_calls, exist_ok=True)
    os.makedirs(out_dir_puts, exist_ok=True)
    
    target_deltas_call = [0.50, 0.40, 0.30, 0.20, 0.10]
    target_deltas_put = [0.50, 0.40, 0.30, 0.20, 0.10] # Target Deltas inside mapped Grid expect Positive Absolute values
    target_dtes = [20, 30, 45, 60, 90]
    max_workers_surf = min(os.cpu_count() or 4, 8)

    def is_surface_missing(parquet_file, out_dir, opt_type):
        base_name = os.path.basename(parquet_file)
        date_str = base_name.replace(f"{opt_type}_", "").replace(".parquet", "")
        expected_out = os.path.join(out_dir, f"volatility_surface_target_grid_{date_str}.parquet")
        if not os.path.exists(expected_out): return True
        try:
            df_check = pd.read_parquet(expected_out)
            if 'grid_type' not in df_check.columns: return True
            col_type = 'iv_call' if opt_type=='call' else 'iv_put'
            if col_type in df_check['iv_type'].values:
                subset = df_check[df_check['iv_type'] == col_type]
                counts = subset.groupby(['ticker', 'Target_DTE']).size()
                max_rows = counts.max() if not counts.empty else 0
                if max_rows < 5: return True # Old discrete format or incomplete
            return False
        except Exception:
            return True

    # Process Calls
    for f in sorted(glob.glob(os.path.join(CALL_EARN_DIR, "call_*.parquet"))):
        if "live" in os.path.basename(f): continue
        if is_surface_missing(f, out_dir_calls, "call"):
            date_str = os.path.basename(f).replace("call_", "").replace(".parquet", "")
            print(f"[{date_str}] Procesando Superficie Call Histórica...")
            try:
                process_single_day(f, out_dir_calls, target_deltas_call, target_dtes, max_workers_surf, option_type='Call')
            except Exception as e:
                print(f"[ERROR] {date_str} Call Surface: {e}")

    # Process Puts
    for f in sorted(glob.glob(os.path.join(PUT_EARN_DIR, "put_*.parquet"))):
        if "live" in os.path.basename(f): continue
        if is_surface_missing(f, out_dir_puts, "put"):
            date_str = os.path.basename(f).replace("put_", "").replace(".parquet", "")
            print(f"[{date_str}] Procesando Superficie Put Histórica...")
            try:
                process_single_day(f, out_dir_puts, target_deltas_put, target_dtes, max_workers_surf, option_type='Put')
            except Exception as e:
                print(f"[ERROR] {date_str} Put Surface: {e}")


    print("\n" + "=" * 60)
    print("       FINALIZADO PIPELINE HISTORICO " + "=" * 60)
    print("=" * 60)

    # ==================================================================
    #                      PIPELINE LIVE (SNAPSHOT)
    # ==================================================================
    print("\n============================================================")
    print("       PIPELINE LIVE (TOP 200 OI)")
    print("============================================================")

    # 1. Actualizar Precios de Acciones FMP del Último Día
    print("\n>>> PASO 12.0: Actualizando Precios de Acciones FMP (Daily EOD)...")
    try:
        from fmp_ticker_updater import run_fmp_update
        run_fmp_update(mode='daily')
    except Exception as e:
        print(f"[ERROR] Falló la actualización de precios FMP Live: {e}")

    # 2. Descargar Live Wheels
    print("\n>>> PASO 12.1: Descargar Live Snapshot (Opciones)...")
    wheels_mgr.download_live_wheels()

    # 2. Generar formato live_top200_wheels.parquet (Calls y Puts)
    print("\n>>> PASO 12.2: Construir Wheels_Call y Wheels_Put (Filtros básicos)...")
    call_builder.build_live()
    put_builder.build_live()

    # 3. Enriquecer con Earnings
    print("\n>>> PASO 12.3: Enriquecimiento de Earnings...")
    call_builder.enrich_with_earnings(earnings_path=EARNINGS_PATH, live_mode=True)
    put_builder.enrich_with_earnings(earnings_path=EARNINGS_PATH, live_mode=True)

    # 4. Enriquecer con FMP (Dividendos/Tasas)
    print("\n>>> PASO 12.4: Enriquecimiento de Dividendos y Tasas (FMP)...")
    # For Calls
    fmp_enricher_call = FMPDataEnricher(fmp_api_key=FMP_API_KEY, input_dir=CALL_EARN_DIR, option_type="Call", cache_dir=CACHE_DIR)
    fmp_enricher_call.enrich(live_mode=True)
    # For Puts
    fmp_enricher_put = FMPDataEnricher(fmp_api_key=FMP_API_KEY, input_dir=PUT_EARN_DIR, option_type="Put", cache_dir=CACHE_DIR)
    fmp_enricher_put.enrich(live_mode=True)

    # 5. Volatilidad Implicita Base (IV)
    print("\n>>> PASO 12.5: Calculando Volatilidad Implícita (IV)...")
    iv_enricher_call = IVEnricher(input_dir=CALL_EARN_DIR, option_type="call", cache_dir=CACHE_DIR)
    iv_enricher_call.enrich(live_mode=True)
    iv_enricher_put = IVEnricher(input_dir=PUT_EARN_DIR, option_type="put", cache_dir=CACHE_DIR)
    iv_enricher_put.enrich(live_mode=True)

    # 6. Extirpación de Evento (Clean IV)
    print("\n>>> PASO 12.6: Extirpación de Varianza de Evento (Clean IV)...")
    clean_iv_enricher_call = CleanIVEnricher(input_dir=CALL_EARN_DIR, option_type="call")
    clean_iv_enricher_call.enrich(live_mode=True)
    clean_iv_enricher_put = CleanIVEnricher(input_dir=PUT_EARN_DIR, option_type="put")
    clean_iv_enricher_put.enrich(live_mode=True)

    # 6.5. Griegas FDM y Precios BS2002
    print("\n>>> PASO 12.6.5: Generando Griegas FDM y Precios BS2002...")
    from process_greeks_fdm import process_pipeline_files
    call_live_wheels_path = os.path.join(CALL_EARN_DIR, "call_live_top200_wheels.parquet")
    put_live_wheels_path = os.path.join(PUT_EARN_DIR, "put_live_top200_wheels.parquet")
    if os.path.exists(call_live_wheels_path):
        process_pipeline_files([call_live_wheels_path], option_type="call")
    if os.path.exists(put_live_wheels_path):
        process_pipeline_files([put_live_wheels_path], option_type="put")


    # 7. Filtro Liquidez (Min Strikes)
    print("\n>>> PASO 12.7: Filtro Final de Liquidez (>4 strikes)...")
    call_builder.filter_min_strikes(min_strikes=4, live_mode=True)
    put_builder.filter_min_strikes(min_strikes=4, live_mode=True)

    # 8. Protocolo Cuantitativo Anti-Gravedad (Superficies de Volatilidad y Griegas)
    print("\n>>> PASO 12.8: Protocolo Cuantitativo Anti-Gravedad (SSVI y Griegas Target)...")
    live_wheels_path = os.path.join(CALL_EARN_DIR, "call_live_top200_wheels.parquet")
    output_surfaces_dir = os.path.join(ROOT_DIR, "volatility_surface")
    
    if os.path.exists(live_wheels_path):
        target_deltas = [0.50, 0.40, 0.30, 0.20, 0.10]
        target_dtes = [20, 30, 45, 60, 90]
        max_workers = min(os.cpu_count() or 4, 8)
        process_single_day(live_wheels_path, output_surfaces_dir, target_deltas, target_dtes, max_workers, option_type="Call")
    else:
        print(f"[WARN] No se encontró el archivo live wheels para procesar: {live_wheels_path}")

    print("\n>>> PASO 12.9: Protocolo Cuantitativo Anti-Gravedad PUTS (SSVI y Griegas Target)...")
    put_live_wheels_path = os.path.join(PUT_EARN_DIR, "put_live_top200_wheels.parquet")
    output_put_surfaces_dir = os.path.join(ROOT_DIR, "volatility_surface_put")
    os.makedirs(output_put_surfaces_dir, exist_ok=True)
    
    if os.path.exists(put_live_wheels_path):
        target_deltas_put = [0.50, 0.40, 0.30, 0.20, 0.10] # Must be positive
        max_workers = min(os.cpu_count() or 4, 8)
        process_single_day(put_live_wheels_path, output_put_surfaces_dir, target_deltas_put, target_dtes, max_workers, option_type="Put")
    else:
        print(f"[WARN] No se encontró el archivo live wheels para procesar: {put_live_wheels_path}")

    # ------------------------------------------------------------------
    # PASO 13 - Enriquecimiento final de tradeDate
    # ------------------------------------------------------------------
    print("\n>>> PASO 13: Añadiendo 'tradeDate' a los archivos de superficie (Incremental)...")
    import glob
    import pyarrow.parquet as pq
    import pandas as pd
    from datetime import datetime

    surfaces_dirs = [
        os.path.join(ROOT_DIR, "volatility_surface"),
        os.path.join(ROOT_DIR, "volatility_surface_put")
    ]
    current_date_str = datetime.now().strftime("%Y-%m-%d")
    updated_count = 0
    
    for s_dir in surfaces_dirs:
        if not os.path.exists(s_dir): continue
        for f in glob.glob(os.path.join(s_dir, "*.parquet")):
            try:
                schema = pq.read_schema(f)
                if 'tradeDate' not in schema.names:
                    df = pd.read_parquet(f)
                    bname = os.path.basename(f)
                    if "live" in bname:
                        t_date = current_date_str
                    else:
                        t_date = bname.replace("volatility_surface_target_grid_", "").replace(".parquet", "")
                    
                    df["tradeDate"] = pd.to_datetime(t_date)
                    df.to_parquet(f, index=False)
                    updated_count += 1
            except Exception as e:
                print(f"[ERROR] procesando tradeDate en {os.path.basename(f)}: {e}")
                
    if updated_count > 0:
        print(f"[INFO] Se añadieron fechas de tradeDate a {updated_count} archivos de superficie.")
    else:
        print("[INFO] Todos los archivos de superficie ya tenían su tradeDate.")

    # ------------------------------------------------------------------
    # PASO 14 - Backtesting Multiestrategia Incremental
    # ------------------------------------------------------------------
    print("\n>>> PASO 14: Ejecutando Backtesting Incremental (6 estrategias)...")
    try:
        import run_incremental_pipeline
        run_incremental_pipeline.main()
    except Exception as e:
        print(f"[ERROR] Falló la ejecución del backtesting incremental: {e}")

    # ------------------------------------------------------------------
    # PASO 15 - Detección HFT de Estados de Mercado (Lateral, Swing, Squeeze)
    # ------------------------------------------------------------------
    print("\n>>> PASO 15: Scoring HFT Incremental de Regímenes de Mercado...")
    try:
        from run_market_regimes import run_market_regimes_incremental
        run_market_regimes_incremental()
    except Exception as e:
        print(f"[ERROR] Falló la actualización de Estados de Mercado: {e}")

    # ------------------------------------------------------------------
    # PASO 16 - Inyección Machine Learning (Data Warehouses HFT vs PnL)
    # ------------------------------------------------------------------
    print("\n>>> PASO 16: Construyendo Data Warehouses (Meta-Modelos de IA)...")
    try:
        from build_meta_model_datasets import build_meta_model_datasets
        build_meta_model_datasets()
    except Exception as e:
        print(f"[ERROR] Falló la creación de Datasets Machine Learning: {e}")

    # ------------------------------------------------------------------
    # PASO 17 - Generación del HFT Live Dashboard (Scores Cuantitativos)
    # ------------------------------------------------------------------
    print("\n>>> PASO 17: Extrayendo Radar HFT Completo (Live Scores)...")
    try:
        import subprocess
        dashboard_script = os.path.join(os.path.dirname(os.path.abspath(__file__)), "generate_live_dashboard.py")
        if os.path.exists(dashboard_script):
            # Usamos subprocess para que cargue toda la memoria de generate_live_dashboard limpia
            subprocess.run([sys.executable, dashboard_script], check=True)
        else:
            print(f"[WARN] No se encontró el script de dashboard en {dashboard_script}")
    except Exception as e:
        print(f"[ERROR] Falló la generación del Dashboard Quant: {e}")

    print("\n============================================================")
    print("       PIPELINE TOTAL FINALIZADO")
    print("============================================================")


if __name__ == "__main__":
    current_dir = os.path.dirname(os.path.abspath(__file__))
    if current_dir not in sys.path:
        sys.path.append(current_dir)
    main()
