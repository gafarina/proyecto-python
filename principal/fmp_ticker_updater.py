import os
import requests
import pandas as pd
import numpy as np
import logging
import argparse
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import glob
import polars as pl

# ==============================================================================
# CONFIGURACIÓN GLOBAL
# ==============================================================================

# Rutas de Salida
# ------------------------------------------------------------------------------
# Archivo Parquet donde se guardan los precios diarios (EOD)
DEFAULT_DAILY_OUTPUT = r"C:\datos_proyecto\datos_stocks\fmp_prices.parquet"

# Archivo Parquet donde se guardan precios intradía (5min) - opcional
DEFAULT_INTRADAY_OUTPUT = r"C:\datos_proyecto\datos_stocks\fmp_intraday.parquet"

# Credenciales y Endpoints
# ------------------------------------------------------------------------------
# API Key de Financial Modeling Prep (FMP)
DEFAULT_API_KEY = os.getenv("FMP_API_KEY", "xjnhJX6n8NP06Igh3DhHjA8qLOl4i09I") 

# Endpoints base
DAILY_BASE_URL = "https://financialmodelingprep.com/stable/historical-price-eod/full/"
INTRADAY_BASE_URL = "https://financialmodelingprep.com/stable/historical-chart/5min/"

# Configuración de Logging
# ------------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("fmp_unified.log"),
        logging.StreamHandler()
    ]
)

# ==============================================================================
# CLASE: PROVEEDOR DE UNIVERSO
# ==============================================================================

class UniverseProvider:
    """
    Clase responsable de determinar qué tickers se deben descargar.
    Lee el archivo 'universo_tickers.json' donde está definido el portafolio objetivo.
    """
    def __init__(self):
        pass

    def get_universe(self):
        """
        Carga la lista de tickers activos.
        
        Returns:
            list: Lista de tickers (strings) ordenados alfabéticamente.
        """
        # Ruta al archivo maestro del universo
        target_file = os.path.join(r"C:\datos_proyecto\universo", "universo.json")
        
        if os.path.exists(target_file):
            logging.info(f"Cargando Universo desde JSON: {target_file}")
            try:
                import json
                with open(target_file, 'r') as f:
                    data = json.load(f)
                    
                    universe_set = set()
                    if isinstance(data, list):
                        # Caso antiguo: simple lista
                        universe_set = set([str(x).strip().upper() for x in data if isinstance(x, str)])
                    elif isinstance(data, dict):
                        # Nuevo formato: diccionario de fechas -> listas de tickers
                        for date_key, ticker_list in data.items():
                            if isinstance(ticker_list, list):
                                for x in ticker_list:
                                    if isinstance(x, str):
                                        universe_set.add(x.strip().upper())
                                        
                    logging.info(f"Se cargaron {len(universe_set)} tickers únicos del archivo.")
                    return sorted(list(universe_set))
            except Exception as e:
                logging.error(f"Error leyendo archivo de universo {target_file}: {e}")
        
        logging.error("No se encontró un archivo de universo válido. Abortando.")
        return []

class OratsTopVolumeUniverseProvider:
    def __init__(self, cores_dir=r"C:\datos_proyecto\datos_cores", limit=3000, days_to_look=10):
        self.cores_dir = cores_dir
        self.limit = limit
        self.days_to_look = days_to_look

    def get_universe(self):
        logging.info(f"Escaneando archivos CORE ORATS en {self.cores_dir} para extraer el Top {self.limit} tickers por volumen...")
        files = sorted(glob.glob(os.path.join(self.cores_dir, "*.parquet")))
        if not files:
            logging.error("No se encontraron archivos en la carpeta de cores.")
            return []
        
        recent_files = files[-self.days_to_look:]
        logging.info(f"Analizando los últimos {len(recent_files)} días de mercado...")
        
        queries = []
        for f in recent_files:
            try:
                q = (
                    pl.scan_parquet(f)
                    .select(['ticker', 'stkVolu'])
                    .rename({'stkVolu': 'vol'})
                )
                queries.append(q)
            except Exception as e:
                logging.warning(f"No se pudo escanear {f}: {e}")
            
        if not queries:
            return []
            
        df_all = pl.concat(queries)
        
        logging.info(f"Calculando volumen promedio para la selección del Top {self.limit}...")
        df_avg = (
            df_all
            .group_by('ticker')
            .agg(pl.col('vol').mean().alias('avg_vol'))
            .sort('avg_vol', descending=True)
            .limit(self.limit)
            .collect()
        )
        
        df_valid = df_avg.filter(pl.col('ticker').is_not_null())
        top_tickers = df_valid['ticker'].to_list()
        
        logging.info(f"Se extrajeron exitosamente {len(top_tickers)} tickers principales.")
        return sorted(top_tickers)

# ==============================================================================
# CLASE BASE: DESCARGADOR GENÉRICO
# ==============================================================================

class BaseFMPDownloader:
    """
    Clase base que gestiona la conexión HTTP con reintentos robustos.
    """
    def __init__(self, api_key=None):
        self.api_key = api_key if api_key else DEFAULT_API_KEY
    
    def _get_session(self):
        """
        Crea una sesión HTTP con política de reintentos (Retries).
        Útil para evitar fallos por intermitencias de red o límites de API (Rate Limits).
        """
        session = requests.Session()
        retry = Retry(
            total=5, # Intentar hasta 5 veces
            backoff_factor=1, # Espera exponencial: 1s, 2s, 4s...
            status_forcelist=[429, 500, 502, 503, 504], # Reintentar en estos errores
            allowed_methods=["GET"]
        )
        adapter = HTTPAdapter(
            max_retries=retry,
            pool_connections=50, # Conexiones simultáneas permitidas
            pool_maxsize=50
        )
        session.mount("https://", adapter)
        return session

# ==============================================================================
# CLASE: DESCARGADOR DIARIO (EOD)
# ==============================================================================

class FMPDailyDownloader(BaseFMPDownloader):
    """
    Descarga precios diarios (End of Day - EOD) de forma incremental.
    - Si el ticker ya existe: Descarga solo desde la última fecha registrada.
    - Si es nuevo: Descarga desde la fecha base (2019-06-01 por defecto).
    """
    def __init__(self, api_key=None, output_file=None, tickers=None):
        super().__init__(api_key)
        self.output_file = output_file if output_file else DEFAULT_DAILY_OUTPUT
        self.tickers = tickers if tickers else []
        self.base_url = DAILY_BASE_URL
        self.failed_tickers = set()
    
    def _get_existing_data(self):
        """Lee el archivo parquet actual para saber qué datos ya tenemos."""
        if os.path.exists(self.output_file):
            try:
                df = pd.read_parquet(self.output_file)
                logging.info(f"Datos existentes cargados: {len(df)} filas.")
                return df
            except Exception as e:
                logging.error(f"Error leyendo archivo existente: {e}. Se iniciará de cero.")
                return pd.DataFrame()
        return pd.DataFrame()

    def _get_date_ranges(self, df):
        """Calcula la fecha mínima y máxima que tenemos guardada para cada ticker."""
        if df.empty or 'ticker' not in df.columns or 'date' not in df.columns:
            return {}, {}, {}
        
        # Asegurar tipo datetime
        if not pd.api.types.is_datetime64_any_dtype(df['date']):
            df['date'] = pd.to_datetime(df['date'])
        
        # Agrupar por ticker y obtener limites y sets de fechas
        grouped = df.groupby('ticker')['date']
        min_dates = grouped.min().to_dict()
        max_dates = grouped.max().to_dict()
        
        # Optimización: Set de fechas (solo date component) por ticker
        dates_by_ticker = {
            t: set(dates.dt.date) for t, dates in grouped
        }
        
        return min_dates, max_dates, dates_by_ticker

    def fetch_ticker_data(self, session, ticker, start_date=None):
        """
        Consulta la API para un solo ticker.
        Args:
            start_date: Fecha de inicio (YYYY-MM-DD). Si es None, trae todo el historial.
        """
        import time
        # Timestmap para evitar cache
        ts = int(time.time() * 1000)
        url = f"{self.base_url}?symbol={ticker}&apikey={self.api_key}&_={ts}"
        
        if start_date:
            url += f"&from={start_date.strftime('%Y-%m-%d')}"
            # '&to=' es opcional, por defecto es hoy
        
        try:
            response = session.get(url, timeout=10)
            
            # Manejo de ticker no encontrado
            if response.status_code == 404:
                return ticker, pd.DataFrame()
            
            response.raise_for_status()
            data = response.json()
            
            rows = []
            if isinstance(data, list):
                rows = data
            elif isinstance(data, dict) and 'historical' in data:
                rows = data['historical']
            
            if not rows: 
                return ticker, pd.DataFrame()
            
            df = pd.DataFrame(rows)
            df['ticker'] = ticker
            return ticker, df
        except Exception as e:
            # logging.warning(f"Fallo al descargar {ticker}: {e}")
            return ticker, None

    def download_prices(self, max_workers=20, start_year=2019):
        """
        Método principal que orquesta la descarga paralela.
        """
        if not self.tickers:
            logging.warning("No hay tickers definidos para descarga.")
            return

        # 1. Analizar estado actual
        existing_df = self._get_existing_data()
        min_dates, max_dates, dates_by_ticker = self._get_date_ranges(existing_df)
        
        session = self._get_session()
        new_data_frames = []
        
        # Fecha base dinámica según parámetro
        default_start_date = datetime(start_year, 1, 1) 
        
        logging.info(f"Iniciando descarga DIARIA para {len(self.tickers)} tickers (Start Default: 2019-06-01)...")
        
        successful_tickers = set()

        # 2. Ejecución Paralela (ThreadPool)
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_ticker = {}
            
            for ticker in self.tickers:
                last_date = max_dates.get(ticker)
                first_date = min_dates.get(ticker)
                known_dates = dates_by_ticker.get(ticker, set())
                
                req_start = default_start_date
                
                if last_date:
                    # Lógica Inteligente de Gaps
                    # Generamos rango esperado de días laborables desde el inicio conocido (o default) hasta hoy
                    # Usamos pandas bdate_range para días de semana (L-V), ignorando feriados específicos por simplicidad (FMP no devuelve nada igual)
                    
                    # 1. Detectar Gaps INTERNOS (Días que deberían estar y no están entre First y Last)
                    # Solo chequeamos gaps si tenemos historial suficiente
                    
                    # Definimos el inicio de chequeo: El menor entre First Date real y Default Start
                    check_start = min(first_date, default_start_date) if first_date else default_start_date
                    
                    expected_range = pd.bdate_range(start=check_start, end=datetime.now())
                    expected_dates = set(expected_range.date)
                    
                    # Missing = Esperados - Conocidos
                    # Filtramos solo los que son anteriores a hoy (no futuros)
                    missing_dates = sorted(list(expected_dates - known_dates))
                    missing_dates = [d for d in missing_dates if d < datetime.now().date()]
                    
                    if missing_dates:
                        # Si hay huecos, pedimos desde el primer hueco
                        first_gap = missing_dates[0]
                        # Buffer: Si el gap es muy antiguo (antes de 2000?), quizás es error de datos. Ignoramos pre-default?
                        # Regla: Si el gap es POST default_start_date, lo llenamos.
                        if first_gap >= default_start_date.date():
                            logging.debug(f"Gap detectado para {ticker} en {first_gap}. Refetching desde ahí.")
                            req_start = datetime.combine(first_gap, datetime.min.time())
                        else:
                            # Si el gap es muy viejo (antes de 2019), y nuestra data empieza después, 
                            # tal vez queremos historia completa.
                            if first_date > default_start_date + timedelta(days=30):
                                logging.debug(f"Historia incompleta para {ticker} (Empieza {first_date}). Pidiendo full history.")
                                req_start = default_start_date
                            else:
                                # Gap viejo irrelevante o feriado, seguimos incremental normal
                                # Pero cuidado: si el gap es 'ayer' (y last_date fue anteayer), esto lo cubre la lógica normal (last_date)
                                # La lógica normal es req_start = last_date.
                                # Si hay gaps intermedios (ej: me falta 3 de feb, pero tengo 4 de feb), last_date es 4 de feb.
                                # Entonces req_start seria 4 de feb y NO bajamos a 3 de feb.
                                # AQUI ESTA LA CLAVE: Si hay gaps ANTERIORES a last_date, forzamos la descarga.
                                
                                # Gaps anteriores a last_date
                                internal_gaps = [d for d in missing_dates if d < last_date.date()]
                                if internal_gaps:
                                     # Tomamos el primero
                                     target_gap = internal_gaps[0]
                                     if target_gap >= default_start_date.date():
                                         logging.info(f"Gap INTERNO detectado para {ticker}: {target_gap}. Re-descargando...")
                                         req_start = datetime.combine(target_gap, datetime.min.time())
                                     else:
                                         req_start = last_date
                                else:
                                     req_start = last_date
                    else:
                        req_start = last_date
                
                # Optimización: Si la fecha requerida es hoy/futuro, saltamos.
                if req_start.date() >= datetime.now().date():
                    successful_tickers.add(ticker) 
                    continue

                future = executor.submit(self.fetch_ticker_data, session, ticker, req_start)
                future_to_ticker[future] = ticker

            # 3. Procesar Resultados
            for future in as_completed(future_to_ticker):
                ticker = future_to_ticker[future]
                try:
                    r_ticker, df = future.result()
                    
                    if df is None:
                        self.failed_tickers.add(ticker)
                    elif df.empty:
                        # Si está vacío y no teníamos datos, es un fallo. Si ya teníamos, quizás no hubo markets hoy.
                        if ticker not in max_dates:
                            self.failed_tickers.add(ticker)
                        else:
                            successful_tickers.add(ticker)
                    else:
                        # Debug específico para AAPL
                        if ticker == 'AAPL':
                            logging.info(f"DEBUG: AAPL rows: {len(df)}. Último: {df.iloc[-1]['date']} Close: {df.iloc[-1]['close']}")
                        
                        new_data_frames.append(df)
                        successful_tickers.add(ticker)
                        
                    if len(successful_tickers) % 100 == 0:
                        logging.info(f"Progreso: {len(successful_tickers)}/{len(self.tickers)} tickers procesados.")
                        
                except Exception as e:
                    logging.error(f"Error crítico en hilo {ticker}: {e}")
                    self.failed_tickers.add(ticker)

        # 4. Guardado y Merge
        final_dataset = None
        
        if new_data_frames:
            new_combined = pd.concat(new_data_frames, ignore_index=True)
            if 'date' in new_combined.columns:
                 new_combined['date'] = pd.to_datetime(new_combined['date'])
            
            # Merge inteligente:
            # 1. Si tenemos datos viejos, eliminamos filas que se solapan con los nuevos datos (updates).
            # 2. Concatenamos.
            if not existing_df.empty:
                if not pd.api.types.is_datetime64_any_dtype(existing_df['date']):
                    existing_df['date'] = pd.to_datetime(existing_df['date'])
                
                # Antijoin: Quedarse con filas de existing_df que NO (ticker, date) match en new_combined
                keys_temp = new_combined[['ticker', 'date']].drop_duplicates()
                existing_df = existing_df.merge(keys_temp, on=['ticker', 'date'], how='left', indicator=True)
                existing_df = existing_df[existing_df['_merge'] == 'left_only'].drop(columns=['_merge'])
                
                final_dataset = pd.concat([existing_df, new_combined], ignore_index=True)
            else:
                final_dataset = new_combined
            
            # Orden final
            final_dataset = final_dataset.sort_values(['ticker', 'date'])
            
            # Guardado Atómico
            os.makedirs(os.path.dirname(self.output_file), exist_ok=True)
            final_dataset.to_parquet(self.output_file, index=False)
            logging.info(f"Guardado Exitoso: {self.output_file}. Filas Totales: {len(final_dataset)} (Nuevas: {len(new_combined)})")
        else:
            logging.info("No se descargaron datos nuevos (Todo actualizado).")
            final_dataset = existing_df

        # 5. Resumen Final (Solicitado)
        if final_dataset is not None and not final_dataset.empty:
            print("\n--- RESUMEN DE ACTUALIZACIÓN ---")
            
            # A) Últimas 10 fechas procesadas (presentes en el archivo)
            if 'date' in final_dataset.columns:
                # Ordenar fechas únicas
                all_dates = sorted(final_dataset['date'].unique())
                last_10 = all_dates[-10:]
                print(f"Últimas 10 fechas en archivo: {[d.strftime('%Y-%m-%d') for d in last_10]}")
            
            # B) Muestra aleatoria de un ticker (últimos 4 días) VIGENTE
            import random
            if 'date' in final_dataset.columns:
                 max_date = final_dataset['date'].max()
                 # Filtrar tickers que tengan dato para la última fecha
                 active_tickers_df = final_dataset[final_dataset['date'] == max_date]
                 active_tickers = active_tickers_df['ticker'].unique()
                 
                 if len(active_tickers) > 0:
                     sample_ticker = random.choice(active_tickers)
                     df_sample = final_dataset[final_dataset['ticker'] == sample_ticker].sort_values('date')
                     print(f"\nMuestra para {sample_ticker} (Últimos 4 registros - Vigente al {max_date.date()}):")
                     print(df_sample.tail(4)[['date', 'ticker', 'close', 'volume']].to_string(index=False))
                     print("-" * 40)

        return self.failed_tickers


# ==============================================================================
# CLASE: DESCARGADOR INTRADÍA (5min)
# ==============================================================================

class FMPIntradayDownloader(BaseFMPDownloader):
    """
    Descarga precios Intradía (velas de 5 minutos).
    Nota: La API suele dar histórico limitado (últimos X días) salvo planes Enterprise.
    Generalmente sobreescribe o hace append simple.
    """
    def __init__(self, api_key=None, output_file=None, tickers=None):
        super().__init__(api_key)
        self.output_file = output_file if output_file else DEFAULT_INTRADAY_OUTPUT
        self.tickers = tickers if tickers else []
        self.base_url = INTRADAY_BASE_URL
        self.failed_tickers = set()

    def fetch_ticker_data(self, session, ticker):
        base = self.base_url.rstrip("/")
        # Formato URL: .../5min/{SYMBOL}?apikey=...
        url = f"{base}/{ticker}?apikey={self.api_key}"
        try:
            response = session.get(url, timeout=10)
            if response.status_code == 404:
                 return ticker, pd.DataFrame()
            
            response.raise_for_status()
            data = response.json()
            if not data or not isinstance(data, list): 
                return ticker, pd.DataFrame()
            
            df = pd.DataFrame(data)
            df['ticker'] = ticker
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date'])
            return ticker, df
        except Exception as e:
            return ticker, None

    def download_prices(self, max_workers=20):
        if not self.tickers:
             return
        
        session = self._get_session()
        all_data_frames = []
        successful = 0
        
        logging.info(f"Iniciando descarga INTRADÍA (5min) para {len(self.tickers)} tickers...")
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_ticker = {executor.submit(self.fetch_ticker_data, session, t): t for t in self.tickers}
            
            for future in as_completed(future_to_ticker):
                ticker = future_to_ticker[future]
                try:
                    r_ticker, df = future.result()
                    if df is not None and not df.empty:
                        all_data_frames.append(df)
                        successful += 1
                        
                    if successful % 100 == 0 and successful > 0:
                        logging.info(f"Progreso Intradía: {successful} completados.")
                except:
                    self.failed_tickers.add(ticker)
        
        if all_data_frames:
            final_df = pd.concat(all_data_frames, ignore_index=True)
            final_df = final_df.sort_values(['ticker', 'date'])
            os.makedirs(os.path.dirname(self.output_file), exist_ok=True)
            final_df.to_parquet(self.output_file, index=False)
            logging.info(f"Intradía Guardado: {self.output_file}. Filas: {len(final_df)}")
        else:
            logging.info("No se descargaron datos intradía.")
            
        return self.failed_tickers

# ==============================================================================
# FUNCIÓN PRINCIPAL (ENTRY POINT)
# ==============================================================================

def run_fmp_update(mode='daily', start_year=2020):
    logging.info(f"--- Iniciando FMP Stock Update (Modo: {mode}) ---")
    
    # 1. Obtener Universo
    output_file = DEFAULT_DAILY_OUTPUT
    if mode == 'top3000':
        provider = OratsTopVolumeUniverseProvider(limit=3000)
        tickers = provider.get_universe()
        output_file = r"C:\datos_proyecto\datos_stocks\fmp_prices_top3000.parquet"
        start_year = 2019
    else:
        provider = UniverseProvider()
        tickers = provider.get_universe()
    
    if not tickers:
        logging.error("No se encontraron tickers. Saliendo.")
        return
        
    logging.info(f"Universo activo: {len(tickers)} tickers.")
    
    # 2. Ejecutar Descargas
    failed_daily = set()
    failed_intraday = set()
    
    if mode in ['daily', 'all', 'top3000']:
        daily = FMPDailyDownloader(tickers=tickers, output_file=output_file)
        failed_daily = daily.download_prices(start_year=start_year)
        
    if mode in ['intraday', 'all']:
        intraday = FMPIntradayDownloader(tickers=tickers)
        failed_intraday = intraday.download_prices()
        
    # 3. Reporte de Fallos
    logging.info("--- REPORTE FINAL ---")
    
    if mode in ['daily', 'all']:
        if failed_daily:
            logging.warning(f"Modo Diario falló en {len(failed_daily)} tickers: {sorted(list(failed_daily))}")
        else:
             logging.info("Modo Diario: 100% Exitoso.")
            
    if mode in ['intraday', 'all']:
         if failed_intraday:
            logging.warning(f"Modo Intradía falló en {len(failed_intraday)} tickers.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Actualizador de Precios FMP (Incremental)")
    parser.add_argument("--mode", choices=['daily', 'intraday', 'all', 'top3000'], default='daily', help="Modo de descarga")
    parser.add_argument("--start-year", type=int, default=2020, help="Año de inicio para backfills")
    args = parser.parse_args()
    
    run_fmp_update(mode=args.mode, start_year=args.start_year)
