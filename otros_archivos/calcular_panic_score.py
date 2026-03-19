import pandas as pd
import numpy as np
import os
import time
import concurrent.futures

def calculate_technical_indicators_panic(df: pd.DataFrame) -> pd.DataFrame:
    """
    Step 1: Technical Analysis & SMC Bajista (per ticker).
    Takes a chunk of data (one or more tickers) and computes the time-series variables.
    """
    df = df.sort_values(by=['ticker', 'date']).reset_index(drop=True)
    g_ticker = df.groupby('ticker', as_index=False, group_keys=False)
    
    close_prev = g_ticker['close'].shift(1)
    
    # --- 1. Análisis Técnico Bajista ---
    ema_20 = g_ticker['close'].transform(lambda x: x.ewm(span=20, adjust=False).mean())
    df['down_stretch'] = (ema_20 - df['close']) / (ema_20 + 1e-10)
    
    vol_sma_20 = g_ticker['volume'].transform(lambda x: x.rolling(20, min_periods=5).mean())
    df['rvol'] = df['volume'] / (vol_sma_20 + 1e-10)
    
    # RSI 14 Optimizado
    delta = df['close'] - close_prev
    df['_gain'] = np.where(delta > 0, delta, 0)
    df['_loss'] = np.where(delta < 0, -delta, 0)
    
    avg_gain = g_ticker['_gain'].transform(lambda x: x.ewm(alpha=1/14, adjust=False).mean())
    avg_loss = g_ticker['_loss'].transform(lambda x: x.ewm(alpha=1/14, adjust=False).mean())
    
    rs = avg_gain / (avg_loss + 1e-10)
    df['rsi'] = np.where(avg_loss == 0, 100, 100 - (100 / (1 + rs)))
    df['inv_rsi'] = 100 - df['rsi']
    
    # --- 2. Smart Money Concepts (SMC Bajista) ---
    candle_range = df['high'] - df['low']
    df['bear_disp'] = np.where(candle_range > 0, (df['high'] - df['close']) / candle_range, 0.5)
    
    low_t2 = g_ticker['low'].shift(2)
    fvg_size = low_t2 - df['high']
    df['bear_fvg'] = np.where(fvg_size > 0, fvg_size / df['close'], 0)
    
    # --- 3. Volatilidad Histórica (Expansión por Miedo) ---
    hc = np.abs(df['high'] - close_prev)
    lc = np.abs(df['low'] - close_prev)
    df['_tr'] = np.maximum(candle_range, np.maximum(hc, lc))
    
    atr_5 = g_ticker['_tr'].transform(lambda x: x.rolling(5, min_periods=2).mean())
    atr_20 = g_ticker['_tr'].transform(lambda x: x.rolling(20, min_periods=5).mean())
    df['atr_exp'] = atr_5 / (atr_20 + 1e-10)
    
    return df

def process_chunk_panic(df_chunk: pd.DataFrame) -> pd.DataFrame:
    """Wrapper function for multiprocessing."""
    return calculate_technical_indicators_panic(df_chunk)

def calculate_panic_score(df: pd.DataFrame, num_workers: int = None) -> pd.DataFrame:
    """
    Retorna el DataFrame original añadiendo la columna 'PANIC_SCORE' (0-100).
    Procesado de forma paralela y vectorizada.
    """
    print("1/4 Ordenando Panel y calculando variables crudas de pánico en paralelo...")
    df = df.sort_values(by=['ticker', 'date']).reset_index(drop=True)
    
    tickers = df['ticker'].unique()
    num_workers = num_workers or (os.cpu_count() or 4)
    chunks = np.array_split(tickers, num_workers)
    
    df_chunks = [df[df['ticker'].isin(chunk)] for chunk in chunks]
    
    results = []
    with concurrent.futures.ProcessPoolExecutor(max_workers=num_workers) as executor:
        for res in executor.map(process_chunk_panic, df_chunks):
            results.append(res)
            
    df = pd.concat(results, axis=0).reset_index(drop=True)

    print("2/4 Estandarización Transversal por Fecha (0 a 100)...")
    g_date = df.groupby('date')
    
    cs_stretch = g_date['down_stretch'].rank(pct=True, na_option='bottom') * 100
    cs_rvol    = g_date['rvol'].rank(pct=True, na_option='bottom') * 100
    cs_atr_exp = g_date['atr_exp'].rank(pct=True, na_option='bottom') * 100
    cs_inv_rsi = g_date['inv_rsi'].rank(pct=True, na_option='bottom') * 100
    cs_disp    = g_date['bear_disp'].rank(pct=True, na_option='bottom') * 100
    cs_fvg     = g_date['bear_fvg'].rank(pct=True, na_option='bottom') * 100
    
    df['base_rank'] = (
        cs_stretch * 0.20 + 
        cs_rvol * 0.20 + 
        cs_atr_exp * 0.20 + 
        cs_inv_rsi * 0.15 + 
        cs_disp * 0.15 + 
        cs_fvg * 0.10
    )

    print("3/4 Filtros de Realidad Absoluta (Gatekeepers Bajistas)...")
    gate_stretch = np.clip((df['down_stretch'] - 0.05) / (0.15 - 0.05), 0, 1) 
    gate_rsi = np.clip((40 - df['rsi']) / (40 - 30), 0, 1)         
    gate_rvol = np.clip((df['rvol'] - 1.0) / (2.0 - 1.0), 0, 1)      
    
    print("4/4 Ensamblando PANIC SCORE Final...")
    df['PANIC_SCORE'] = (df['base_rank'] * gate_stretch * gate_rsi * gate_rvol).round(2).fillna(0)
    
    cols_to_drop = ['_gain', '_loss', '_tr', 'down_stretch', 'rvol', 'rsi', 'inv_rsi', 'bear_disp', 'bear_fvg', 'atr_exp', 'base_rank']
    df.drop(columns=cols_to_drop, inplace=True)
    
    return df

def main():
    input_dir = r"C:\datos_proyecto\datos_stocks"
    output_dir = r"C:\datos_proyecto\Ranking_fear"
    
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        
    input_files = [f for f in os.listdir(input_dir) if f.endswith('.parquet')]
    
    if not input_files:
        print("No se encontraron archivos parquet en el directorio de entrada.")
        return

    # Retroceso de 150 días calendario (~100 días de trading) para variables históricas
    lookback_days = pd.Timedelta(days=150)

    for file in input_files:
        input_path = os.path.join(input_dir, file)
        output_path = os.path.join(output_dir, file.replace('.parquet', '_fear.parquet'))
        
        print(f"\n==============================================")
        print(f"Procesando: {file}")
        
        print(f"-> Leyendo datos originales... ({file})")
        start_time = time.time()
        
        df_raw = pd.read_parquet(input_path)
        if 'date' in df_raw.columns and not pd.api.types.is_datetime64_any_dtype(df_raw['date']):
            df_raw['date'] = pd.to_datetime(df_raw['date'])

        max_output_date = pd.NaT
        df_existing = None
        if os.path.exists(output_path):
            print(f"-> Archivo de salida encontrado. Determinando actualización incremental...")
            df_existing = pd.read_parquet(output_path)
            if 'date' in df_existing.columns and not pd.api.types.is_datetime64_any_dtype(df_existing['date']):
                df_existing['date'] = pd.to_datetime(df_existing['date'])
                
            if not df_existing.empty:
                max_output_date = df_existing['date'].max()
                print(f"-> Última fecha procesada: {max_output_date.strftime('%Y-%m-%d')}")
        
        if pd.notna(max_output_date):
            max_input_date = df_raw['date'].max()
            if max_output_date >= max_input_date:
                print(f"-> No hay datos nuevos requeridos (Max input {max_input_date.strftime('%Y-%m-%d')} <= Max output {max_output_date.strftime('%Y-%m-%d')}). Se omite este archivo.")
                continue
                
            cut_date = max_output_date - lookback_days
            print(f"-> Filtrando inputs: tomando registros desde {cut_date.strftime('%Y-%m-%d')} para calentar variables...")
            df_process = df_raw[df_raw['date'] >= cut_date].copy()
        else:
            print("-> Calculando historial completo desde cero...")
            df_process = df_raw.copy()

        df_scored = calculate_panic_score(df_process)
        
        if pd.notna(max_output_date):
            print(f"-> Descartando el período de calentamiento. Reteniendo días > {max_output_date.strftime('%Y-%m-%d')}")
            df_new = df_scored[df_scored['date'] > max_output_date]
            print(f"-> Insertando {len(df_new)} filas nuevas...")
            df_final = pd.concat([df_existing, df_new], ignore_index=True)
            df_final = df_final.sort_values(by=['ticker', 'date']).reset_index(drop=True)
        else:
            df_final = df_scored

        print(f"-> Guardando salida en {output_path}...")
        df_final.to_parquet(output_path)
        
        end_time = time.time()
        print(f"Completado en {end_time - start_time:.2f} segundos.")

if __name__ == "__main__":
    main()
