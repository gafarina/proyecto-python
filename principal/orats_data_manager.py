import os
import math
import datetime
import asyncio
import aiohttp
import polars as pl
import pandas as pd
import json
import glob
import time
import requests
from typing import List, Set, Optional, Dict
from dataclasses import dataclass
from pandas.tseries.holiday import USFederalHolidayCalendar, GoodFriday
from pandas.tseries.offsets import CustomBusinessDay
import concurrent.futures
try:
    from massive import RESTClient
except ImportError:
    print("[WARN] 'massive' library not found. Earnings download will fail.")

# ==============================================================================
# CONFIGURACIÓN
# ==============================================================================
API_KEY = "43af86de-fd09-4fc4-b780-6a301d267cb2"
BASE_DATA_DIR = r"C:\datos_proyecto\datos_cores"
LIVE_DATA_DIR = r"C:\datos_proyecto\datos_live"
EARNINGS_DIR = r"C:\datos_proyecto\earnings"

# Crear directorios si no existen
os.makedirs(BASE_DATA_DIR, exist_ok=True)
os.makedirs(LIVE_DATA_DIR, exist_ok=True)

class OratsDataManager:
    """
    Gestor integral de datos de ORATS.
    
    Responsabilidades:
    1.  Descarga incremental de datos históricos Core.
    2.  Verificación de integridad de datos.
    3.  Cálculo de Universo de tickers (Top N por liquidez).
    4.  Descarga de datos Live para el universo seleccionado.
    """

    def __init__(self, api_key: str, data_dir: str, live_dir: str):
        """
        Inicializa el gestor de datos.

        Args:
            api_key (str): Clave de API de ORATS.
            data_dir (str): Directorio para datos históricos.
            live_dir (str): Directorio para datos en tiempo real.
        """
        self.api_key = api_key
        self.data_dir = data_dir
        self.live_dir = live_dir
        self.banned_suffixes = ["_C", "_P", "_U", "_W"] # Sufijos a excluir si aparecen en tickers raíz
        
        # Endpoints
        self.hist_url = "https://api.orats.io/datav2/hist/cores"
        self.live_url = "https://api.orats.io/datav2/live/cores" # Endpoint para datos live (cores)

    # ==============================================================================
    # 1. DESCARGA HISTÓRICA INCREMENTAL
    # ==============================================================================
    
    def get_existing_dates(self) -> Set[datetime.date]:
        """Retorna un conjunto de fechas (date objects) que ya tienen archivo .parquet."""
        files = glob.glob(os.path.join(self.data_dir, "*.parquet"))
        dates = set()
        for f in files:
            try:
                date_str = os.path.basename(f).replace(".parquet", "")
                d = datetime.datetime.strptime(date_str, "%Y-%m-%d").date()
                dates.add(d)
            except ValueError:
                continue
        return dates

    def get_trading_days(self, start_date: datetime.date, end_date: datetime.date) -> List[datetime.date]:
        """Genera lista de días hábiles excluyendo fines de semana y feriados de mercado (aprox)."""
        # 1. Obtener Feriados Federales en el rango extendido (para cubrir bordes)
        cal = USFederalHolidayCalendar()
        # Ampliamos el rango de feriados para asegurar cobertura
        holidays_range_start = pd.to_datetime(start_date) - pd.Timedelta(days=10)
        holidays_range_end = pd.to_datetime(end_date) + pd.Timedelta(days=10)
        federal_holidays = cal.holidays(start=holidays_range_start, end=holidays_range_end)

        # 2. Obtener Good Friday (Viernes Santo)
        # GoodFriday en pandas.tseries.holiday es una regla (Holiday), no una fecha.
        # Necesitamos instanciarlo para sacar las fechas en el rango
        good_friday_rule = GoodFriday
        good_friday_dates = good_friday_rule.dates(start_date=holidays_range_start, end_date=holidays_range_end)
        
        # 3. Feriados Especiales (One-offs)
        # Duelo Nacional por Jimmy Carter
        special_holidays = pd.DatetimeIndex(['2025-01-09'])
        
        # 4. Combinar Feriados
        # Unimos los índices de fecha
        all_holidays = federal_holidays.union(good_friday_dates).union(special_holidays)

        # 5. Crear offset de Días de Negocio Personalizado
        # CustomBusinessDay recibe 'holidays' como lista de fechas (array-like), no reglas.
        bday_us = CustomBusinessDay(holidays=all_holidays)
        
        # 6. Generar rango
        dt_range = pd.date_range(start=start_date, end=end_date, freq=bday_us)
        return [d.date() for d in dt_range]

    async def fetch_day(self, session: aiohttp.ClientSession, day: datetime.date) -> tuple[str, Optional[List[dict]]]:
        """
        Descarga datos de un día específico.
        Retorna: (fecha_str, lista_datos o None)
        """
        date_str = day.strftime("%Y-%m-%d")
        params = {"token": self.api_key, "tradeDate": date_str}
        
        try:
            async with session.get(self.hist_url, params=params, timeout=60) as response:
                if response.status == 200:
                    data = await response.json()
                    # La API retorna {'data': [...]} o similar. Validar estructura.
                    if isinstance(data, dict) and 'data' in data and data['data']:
                         return date_str, data['data']
                    else:
                         return date_str, None # Día vacío (posible feriado)
                elif response.status == 404:
                    return date_str, None # No encontrado
                else:
                    print(f"[WARN] Error {response.status} al descargar {date_str}")
                    return date_str, None
        except Exception as e:
            print(f"[ERROR] Excepción en {date_str}: {e}")
            return date_str, None

    async def download_history_async(self, start_year: int = 2024, start_date_str: str = None):
        """Versión asíncrona de la descarga histórica."""
        if start_date_str:
            start_date = datetime.datetime.strptime(start_date_str, "%Y-%m-%d").date()
        else:
            start_date = datetime.date(start_year, 1, 1)
        end_date = datetime.date.today() - datetime.timedelta(days=1) # Hasta ayer
        
        if start_date > end_date:
            print("[INFO] La fecha de inicio es posterior a ayer. Nada que descargar.")
            return

        existing = self.get_existing_dates()
        target_days = self.get_trading_days(start_date, end_date)
        missing_days = [d for d in target_days if d not in existing]
        
        if not missing_days:
            print("[INFO] Historial al día. No faltan archivos.")
            return
            
        print(f"[INFO] Descargando {len(missing_days)} días faltantes desde {start_date}...")
        
        chunk_size = 5 # Descargar de a 5 días para no saturar
        async with aiohttp.ClientSession() as session:
            for i in range(0, len(missing_days), chunk_size):
                chunk = missing_days[i : i + chunk_size]
                tasks = [self.fetch_day(session, d) for d in chunk]
                results = await asyncio.gather(*tasks)
                
                for date_str, records in results:
                    if records:
                        self.save_parquet(date_str, records)
                    else:
                        print(f"[SKIP] {date_str} (Sin datos/Feriado)")
                
                # Breve pausa para respetar rate limits
                await asyncio.sleep(0.5)

    def save_parquet(self, date_str: str, records: List[dict]):
        """Guarda la lista de diccionarios como parquet usando Polars."""
        try:
            # Inferir esquema seguro convirtiendo primero todo a string si falla, 
            # pero Polars suele ser bueno infiriendo.
            # Para mayor robustez en producción, definir schema explícito sería ideal.
            df = pl.DataFrame(records)
            
            # Normalización rápida de columnas si es necesario (ej. stkpX -> stkPx)
            # (Basado en lógica de universe.py)
            schema = df.columns
            # Mapeo de correcciones comunes si la API es inconsistente
            corrections = {
                'price': 'stkPx', 'spot': 'stkPx', 'priorCls': 'stkPx', 'underlyingPrice': 'stkPx',
                'callVolume': 'cVolu', 'putVolume': 'pVolu'
            }
            renames = {k:v for k,v in corrections.items() if k in schema and v not in schema}
            if renames:
                df = df.rename(renames)

            out_path = os.path.join(self.data_dir, f"{date_str}.parquet")
            df.write_parquet(out_path, compression="snappy")
            print(f"[OK] Guardado {date_str} - {len(df)} registros")
        except Exception as e:
            print(f"[ERROR] Guardando archivo {date_str}: {e}")

    def download_history(self, start_year: int = 2024, start_date_str: str = None):
        """Wrapper síncrono para ejecutar la descarga."""
        try:
            asyncio.run(self.download_history_async(start_year, start_date_str))
        except RuntimeError:
            # Si ya hay un event loop corriendo (ej. Jupyter)
            loop = asyncio.get_event_loop()
            loop.run_until_complete(self.download_history_async(start_year, start_date_str))

    # ==============================================================================
    # 2. VERIFICACIÓN DE INTEGRIDAD
    # ==============================================================================

    def check_integrity(self, start_year: int = 2024, start_date_str: str = None):
        """
        Analiza la continuidad y coherencia de los datos descargados.
        Detecta gaps de fechas y archivos con tamaño sospechoso (muy pequeños).
        """
        if start_date_str:
            start_date = datetime.datetime.strptime(start_date_str, "%Y-%m-%d").date()
            print(f"\n[INTEGRITY] Iniciando chequeo desde {start_date_str}...")
        else:
            start_date = datetime.date(start_year, 1, 1)
            print(f"\n[INTEGRITY] Iniciando chequeo desde {start_year}...")
        end_date = datetime.date.today() - datetime.timedelta(days=1)
        
        trading_days = set(self.get_trading_days(start_date, end_date))
        existing_days = self.get_existing_dates()
        
        # 1. Chequeo de Gaps
        missing = sorted(list(trading_days - existing_days))
        if missing:
            print(f"[WARN] Faltan {len(missing)} días hábiles (posibles feriados o fallos):")
            # Mostrar primeros 10 y últimos 10 si son muchos
            if len(missing) > 20:
                print(f"   {missing[:5]} ... {missing[-5:]}")
            else:
                print(f"   {missing}")
        else:
            print("[OK] No hay gaps de fechas en días hábiles.")

        # 2. Chequeo de Consistencia de Archivos
        files = glob.glob(os.path.join(self.data_dir, "*.parquet"))
        suspicious = []
        for f in files:
            # Si el archivo pesa menos de 10KB es sospechoso para datos de todo el mercado
            if os.path.getsize(f) < 10 * 1024: 
                suspicious.append(os.path.basename(f))
        
        if suspicious:
             print(f"[WARN] Se encontraron {len(suspicious)} archivos sospechosamente pequeños (<10KB):")
             print(f"   {suspicious[:10]}...")
        else:
             print("[OK] Tamaños de archivos parecen consistentes > 10KB.")

    # ==============================================================================
    # 3. CÁLCULO DE UNIVERSO (FILTROS)
    # ==============================================================================

    def get_universe(self, top_n: int = 2000, lookback_days: int = 5) -> List[str]:
        """
        Calcula el universo de tickers basado en los últimos 'lookback_days' disponibles.
        
        Criterios:
        1. Promedio de volumen total (Call + Put) más alto.
        2. Precio > $10.0
        3. Exclusion de tickers con sufijos raros.
        """
        print(f"\n[UNIVERSE] Calculando Top {top_n} tickers (Promedio últimos {lookback_days} días)...")
        
        # Obtener los archivos más recientes
        files = glob.glob(os.path.join(self.data_dir, "*.parquet"))
        files.sort() # Ordenar por fecha (nombre archivo)
        recent_files = files[-lookback_days:]
        
        if not recent_files:
            print("[ERROR] No hay datos suficientes para calcular universo.")
            return []

        lfs = []
        for f in recent_files:
            lf = pl.scan_parquet(f)
            schema_names = lf.collect_schema().names()
            
            # --- Normalización de Columnas en Lectura ---
            # Identificar columna de precio
            price_candidates = ['stkPx', 'price', 'spot', 'priorCls', 'underlyingPrice', 'close']
            price_col = next((c for c in price_candidates if c in schema_names), None)
            
            if price_col:
                if price_col != 'stkPx':
                    lf = lf.rename({price_col: 'stkPx'})
            else:
                 lf = lf.with_columns(pl.lit(0.0).alias('stkPx'))

            # Asegurar volumenes
            if 'cVolu' not in schema_names: 
                 if 'callVolume' in schema_names: lf = lf.rename({'callVolume': 'cVolu'})
                 else: lf = lf.with_columns(pl.lit(0).alias('cVolu'))
            
            if 'pVolu' not in schema_names: 
                 if 'putVolume' in schema_names: lf = lf.rename({'putVolume': 'pVolu'})
                 else: lf = lf.with_columns(pl.lit(0).alias('pVolu'))
            
            # Normalizar nombres y seleccionar
            lf = lf.select(['ticker', 'stkPx', 'cVolu', 'pVolu'])
            lfs.append(lf)
        
        # Concatenar y procesar
        try:
            # Combinamos todos los días
            combined = pl.concat(lfs)
            
            # Agrupar por ticker para sacar promedios
            # Filtramos primero filas inválidas para optimizar
            filtered = combined.filter(
                (pl.col("stkPx") > 10.0) & 
                (pl.col("ticker").is_not_null()) &
                (~pl.col("ticker").str.contains("_")) # Excluir derivados raros con guiones bajos si es la convención
            )
            
            # Agregación
            agg = filtered.group_by("ticker").agg([
                (pl.col("cVolu").sum() + pl.col("pVolu").sum()).alias("total_vol_period"),
                pl.col("stkPx").mean().alias("avg_price")
            ])
            
            # Ranking final
            top_tickers = agg.sort("total_vol_period", descending=True).limit(top_n)
            
            # Recopilar lista
            universe_list = top_tickers.select("ticker").collect().to_series().to_list()
            
            # Guardar el universo para referencia
            uni_path = os.path.join(os.path.dirname(self.data_dir), "universe_top2000.json")
            with open(uni_path, 'w') as f:
                json.dump(universe_list, f, indent=4)
                
            print(f"[OK] Universo generado con {len(universe_list)} tickers. Guardado en {uni_path}")
            return universe_list

        except Exception as e:
            print(f"[ERROR] Calculando universo: {e}")
            import traceback
            traceback.print_exc()
            return []

    # ==============================================================================
    # 4. DESCARGA LIVE (ACTUALIZACIÓN CONTINUA)
    # ==============================================================================

    async def fetch_live_snapshot(self, session: aiohttp.ClientSession, tickers: List[str]):
        """
        Descarga snapshot live para una lista de tickers.
        Nota: La API live de Orats suele aceptar múltiples tickers separados por coma.
        """
        # Dividir en chunks si la URL se hace muy larga (ej. 50 tickers por request)
        chunk_size = 50 
        all_data = []
        
        for i in range(0, len(tickers), chunk_size):
            chunk = tickers[i: i+chunk_size]
            ticker_str = ",".join(chunk)
            params = {"token": self.api_key, "ticker": ticker_str} # Verificar parámetro exacto 'ticker' o 'symbol'
            
            try:
                async with session.get(self.live_url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        if 'data' in data:
                            all_data.extend(data['data'])
            except Exception as e:
                print(f"[ERROR] Live fetch chunk {i}: {e}")
            
            await asyncio.sleep(0.1) # Pequeño delay
            
        return all_data

    async def update_live_data(self, tickers: List[str], interval_seconds: int = 600):
        """
        Loop de actualización de datos en vivo.
        
        Args:
            tickers: Lista de tickers a monitorear.
            interval_seconds: Tiempo entre actualizaciones (default 10 mins).
        """
        print(f"\n[LIVE] Iniciando monitoreo para {len(tickers)} tickers. Intervalo: {interval_seconds}s")
        
        while True:
            try:
                start_time = time.time()
                timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
                
                async with aiohttp.ClientSession() as session:
                    data = await self.fetch_live_snapshot(session, tickers)
                
                if data:
                    # Guardar snapshot
                    df = pl.DataFrame(data)
                    filename = f"live_snapshot_{timestamp}.parquet"
                    path = os.path.join(self.live_dir, filename)
                    df.write_parquet(path)
                    print(f"[LIVE] {timestamp} - Descargados {len(df)} registros.")
                    
                    # Opcional: Mantener solo el último snapshot "current.parquet"
                    current_path = os.path.join(self.live_dir, "current_live.parquet")
                    df.write_parquet(current_path)

                else:
                    print(f"[LIVE] {timestamp} - No se recibieron datos.")

                # Esperar hasta el siguiente intervalo
                elapsed = time.time() - start_time
                wait = max(0, interval_seconds - elapsed)
                await asyncio.sleep(wait)

            except KeyboardInterrupt:
                print("[LIVE] Detenido por usuario.")
                break
            except Exception as e:
                print(f"[ERROR] Ciclo Live: {e}")
                await asyncio.sleep(60) # Esperar 1 min antes de reintentar si hay error

    def run_live_monitor(self, top_n: int = 2000):
        """Wrapper para iniciar el monitor live con el universo actual."""
        universe = self.get_universe(top_n=top_n)
        if not universe:
            print("[ERROR] No se pudo obtener universo validos para Live Data.")
            return
            
        try:
            asyncio.run(self.update_live_data(universe))
        except KeyboardInterrupt:
            pass
        except RuntimeError:
             loop = asyncio.get_event_loop()
             loop.run_until_complete(self.update_live_data(universe))


if __name__ == "__main__":
    # --- EJECUCIÓN DEL SCRIPT ---
    
    manager = OratsDataManager(API_KEY, BASE_DATA_DIR, LIVE_DATA_DIR)
    
    print("=== ORATS DATA MANAGER ===")
    print("1. Descargando Historial Faltante (2024+)...")
    manager.download_history(start_year=2024)
    
    print("\n2. Verificando Integridad...")
    manager.check_integrity(start_year=2024)
    
    print("\n3. Calculando Universo (Top 2000)...")
    # Genera el JSON de universo, útil para verificar sin correr el live loop
    manager.get_universe(top_n=2000)
    
    # Comentar/Descomentar para correr modo Live

class OratsWheelsDownloader:
    """
    Gestor de descarga de Cadenas de Opciones (Wheels).
    """
    def __init__(self, api_key: str, base_dir: str):
        self.api_key = api_key
        self.base_dir = base_dir
        self.ruedas_dir = os.path.join(base_dir, "ruedas")
        self.core_dir = os.path.join(base_dir, "datos_core")
        
        os.makedirs(self.ruedas_dir, exist_ok=True)

    async def fetch_tickers_batch(self, session: aiohttp.ClientSession, tickers: List[str], trade_date: datetime.date, endpoint: str = "hist"):
        """Descarga batch de tickers."""
        date_str = trade_date.strftime("%Y-%m-%d")
        ticker_str = ",".join(tickers)
        
        if endpoint == "hist":
            url = "https://api.orats.io/datav2/hist/strikes"
            params = {"token": self.api_key, "ticker": ticker_str, "tradeDate": date_str}
        else:
            url = "https://api.orats.io/datav2/strikes"
            params = {"token": self.api_key, "ticker": ticker_str}

        for attempt in range(5):
            try:
                async with session.get(url, params=params, timeout=45) as response:
                    if response.status == 200:
                        data = await response.json()
                        return data.get('data', [])
                    elif response.status == 404:
                        return []
                    elif response.status == 429:
                        await asyncio.sleep(1.0 * (2 ** attempt))
                        continue
            except Exception:
                await asyncio.sleep(1)
        return None

    async def _download_batch(self, tickers: List[str], trade_date: datetime.date, endpoint: str) -> List[dict]:
        """Orquesta descarga masiva."""
        all_records = []
        if endpoint == "live":
            batch_size = 3
            concurrent_limit = 5
        else:
            batch_size = 10
            concurrent_limit = 10
        
        batches = [tickers[i:i+batch_size] for i in range(0, len(tickers), batch_size)]
        
        async with aiohttp.ClientSession() as session:
            for i in range(0, len(batches), concurrent_limit):
                current_block = batches[i:i+concurrent_limit]
                tasks = [self.fetch_tickers_batch(session, b, trade_date, endpoint) for b in current_block]
                results = await asyncio.gather(*tasks)
                for res in results:
                    if res: all_records.extend(res)
                await asyncio.sleep(0.1)
                
        return all_records

    def get_existing_tickers_for_date(self, trade_date: datetime.date) -> Set[str]:
        """Check tickers ya descargados en parquet."""
        date_str = trade_date.strftime("%Y-%m-%d")
        path = os.path.join(self.ruedas_dir, f"rueda_{date_str}.parquet")
        if not os.path.exists(path): return set()
        try:
            return set(pl.scan_parquet(path).select("ticker").collect()["ticker"].to_list())
        except: return set()

    def process_day(self, trade_date: datetime.date, target_tickers: List[str], endpoint: str = "hist", save_mode: str = "append", output_name: str = None, fast_check: bool = False):
        """Procesa un día completo."""
        date_str = trade_date.strftime("%Y-%m-%d")
        
        # 1. Filtrar validez con Core (DESACTIVADO para forzar universo fijo)
        # if endpoint == "hist":
        #    core_path = os.path.join(self.core_dir, f"{date_str}.parquet")
        #    if os.path.exists(core_path):
        #        try:
        #            core_df = pl.scan_parquet(core_path).select("ticker").collect()
        #            valid_on_date = set(core_df["ticker"].to_list())
        #            
        #            # Intersección
        #            target_tickers = [t for t in target_tickers if t in valid_on_date]
        #        except: pass

        
        target_set = set(target_tickers)
        if not target_set: return

        # 2. Incremental
        if save_mode == "append":
            path = os.path.join(self.ruedas_dir, f"rueda_{date_str}.parquet")
            
            # Fast Check: Si el archivo existe, asumimos que está completo y saltamos
            if fast_check:
                if os.path.exists(path):
                    print(f"[SKIP] {date_str}: Rueda existente (Fast Check).")
                    return

            # Purga de Tickers Obsoletos
            if os.path.exists(path):
                try:
                    df_rueda = pl.read_parquet(path)
                    tickers_presentes = set(df_rueda["ticker"].unique().to_list())
                    # Si existen tickers en el disco que no están en target_set (Universo 258), los purgamos
                    obsoletos = tickers_presentes - target_set
                    if obsoletos:
                        print(f"[PURGA] {date_str}: Eliminando {len(obsoletos)} tickers obsoletos del parquet existente.")
                        df_limpio = df_rueda.filter(pl.col("ticker").is_in(list(target_set)))
                        
                        temp_path = path + ".tmp"
                        df_limpio.write_parquet(temp_path)
                        os.remove(path)
                        os.rename(temp_path, path)
                except Exception as e:
                    print(f"[WARN] Error al purgar {path}: {e}")

            existing = self.get_existing_tickers_for_date(trade_date)
            to_download = list(target_set - existing)
            if not to_download:
                print(f"[SKIP] {date_str}: Ruedas completas.")
                return
            print(f"[INFO] {date_str}: Descargando {len(to_download)} ruedas faltantes...")
        else:
            to_download = target_tickers
            print(f"[INFO] {date_str}: Descargando snapshot de {len(to_download)} ruedas...")
            # print(f"   Tickers: {to_download}")

        # 3. Descargar
        try:
            try: loop = asyncio.get_running_loop()
            except RuntimeError: loop = None
            
            if loop and loop.is_running():
                import nest_asyncio
                nest_asyncio.apply()
                records = loop.run_until_complete(self._download_batch(to_download, trade_date, endpoint))
            else:
                records = asyncio.run(self._download_batch(to_download, trade_date, endpoint))
        except Exception as e:
            print(f"[ERROR] Asyncio: {e}")
            return

        if not records: return

        # 4. Guardar
        self._save_records(records, date_str, save_mode, output_name)

    def _save_records(self, records: List[dict], date_str: str, save_mode: str, output_name: str):
        try:
            # Aumentar infer_schema_length a 10000 para evitar que Polars se equivoque
            # al inferir en los primeros 100 registros si aparece un numero gigante luego.
            df = pl.DataFrame(records, infer_schema_length=10000)
            
            # --- DEFINICIÓN DE ESQUEMA ROBUSTO ---
            # 1. Definimos los tipos ideales para las columnas conocidas
            schema_map = {
                "callVolume": pl.Int64, "putVolume": pl.Int64, 
                "callOpenInterest": pl.Int64, "putOpenInterest": pl.Int64, 
                "volume": pl.Int64, "openInterest": pl.Int64,
                "strike": pl.Float64, "stockPrice": pl.Float64, 
                "bid": pl.Float64, "ask": pl.Float64, 
                "impliedVol": pl.Float64, 
                "delta": pl.Float64, "gamma": pl.Float64, 
                "theta": pl.Float64, "vega": pl.Float64, "rho": pl.Float64
            }
            
            # 2. Forzamos tipos en el NUEVO dataframe (prioridad)
            cast_exprs = []
            for col_name, dtype in schema_map.items():
                if col_name in df.columns:
                    if dtype == pl.Int64:
                        cast_exprs.append(pl.col(col_name).cast(pl.Float64, strict=False).fill_null(0).cast(pl.Int64, strict=False))
                    else:
                        cast_exprs.append(pl.col(col_name).cast(dtype, strict=False))
            if cast_exprs:
                df = df.with_columns(cast_exprs)

            if output_name: path = os.path.join(self.ruedas_dir, output_name)
            else: path = os.path.join(self.ruedas_dir, f"rueda_{date_str}.parquet")
            
            temp_path = path + ".tmp"

            if save_mode == "append" and os.path.exists(path):
                old_df = pl.read_parquet(path)
                
                # --- ALINEACIÓN GENERAL DE ESQUEMA ---
                # Recorremos TODAS las columnas del NUEVO dataframe.
                # Si existen en el VIEJO, forzamos que el VIEJO tenga el tipo del NUEVO.
                old_casts = []
                for col_name in df.columns:
                    if col_name in old_df.columns:
                        try:
                            target_type = df.schema[col_name]
                            current_type = old_df.schema[col_name]
                            
                            if current_type != target_type:
                                # print(f"[DEBUG] FixType {col_name}: {current_type} -> {target_type}")
                                if target_type == pl.Int64:
                                     old_casts.append(pl.col(col_name).cast(pl.Float64, strict=False).fill_null(0).cast(pl.Int64, strict=False))
                                else:
                                     old_casts.append(pl.col(col_name).cast(target_type, strict=False))
                        except: pass
                
                if old_casts:
                    old_df = old_df.with_columns(old_casts)

                combined = pl.concat([old_df, df], how="diagonal")
                combined.write_parquet(temp_path)
            else:
                df.write_parquet(temp_path)
            
            if os.path.exists(path): os.remove(path)
            os.rename(temp_path, path)
            print(f"[OK] Guardado {os.path.basename(path)}")
            
        except Exception as e:
            print(f"[ERROR] Guardando ruedas {date_str}: {e}")
            # Se eliminó la generación de backup .ERR_DUMP a pedido del usuario
            # para no contaminar la carpeta de descargas.

    def get_top_200_oi(self) -> List[str]:
        """Calcula Top 200 OI usando el último archivo Core."""
        files = glob.glob(os.path.join(self.core_dir, "*.parquet"))
        if not files: return []
        files.sort()
        last = files[-1]
        
        try:
            lf = pl.scan_parquet(last)
            schema = lf.collect_schema().names()
            
            # Normalizar
            if 'cOi' in schema and 'callOpenInterest' not in schema: lf = lf.rename({'cOi': 'callOpenInterest'})
            if 'pOi' in schema and 'putOpenInterest' not in schema: lf = lf.rename({'pOi': 'putOpenInterest'})
            
            price_col = 'stkPx'
            if 'stkPx' not in schema:
                for c in ['price', 'close', 'priorCls']:
                    if c in schema: 
                        ls = lf.rename({c: 'stkPx'})
                        break
            
            lf = lf.with_columns([
                pl.col('callOpenInterest').fill_null(0),
                pl.col('putOpenInterest').fill_null(0)
            ])
            
            return lf.with_columns(
                (pl.col("callOpenInterest") + pl.col("putOpenInterest")).alias("total_oi")
            ).sort("total_oi", descending=True).limit(200).select("ticker").collect()["ticker"].to_list()
        except: return []

    def download_history_wheels(self, universe: List[str], start_year: int = 2024, fast_check: bool = True):
        """Descarga ruedas históricas para el universo."""
        start_date = datetime.date(start_year, 1, 1)
        end_date = datetime.date.today() - datetime.timedelta(days=1)
        
        # Usar el OratsDataManager para obtener días hábiles (hack: instanciar temporalmente o duplicar logica)
        # Duplicamos logica simple o usamos pandas si está disponible
        from pandas.tseries.holiday import USFederalHolidayCalendar, GoodFriday
        from pandas.tseries.offsets import CustomBusinessDay
        
        # Generar lista de feriados robusta
        holidays_range_start = pd.to_datetime(start_date) - pd.Timedelta(days=10)
        holidays_range_end = pd.to_datetime(end_date) + pd.Timedelta(days=10)
        
        cal = USFederalHolidayCalendar()
        federal_holidays = cal.holidays(start=holidays_range_start, end=holidays_range_end)
        
        good_friday_rule = GoodFriday
        good_friday_dates = good_friday_rule.dates(start_date=holidays_range_start, end_date=holidays_range_end)
        
        all_holidays = federal_holidays.union(good_friday_dates)
        
        bday = CustomBusinessDay(holidays=all_holidays)
        days = pd.date_range(start_date, end_date, freq=bday)
        
        print(f"\n[WHEELS] Verificando {len(days)} días para {len(universe)} tickers (Fast Check={fast_check})...")
        for ts in days:
            d = ts.date()
            self.process_day(d, universe, endpoint="hist", save_mode="append", fast_check=fast_check)
    def download_live_wheels(self):
        """Descarga ruedas live para el Universo Global Histórico."""
        import json
        print(f"\n[WHEELS] Cargando Universo Global para Snapshot Live...")
        
        universo_path = os.path.join(self.base_dir, "universo", "universo.json")
        try:
            with open(universo_path, "r", encoding="utf-8") as fh:
                data = json.load(fh)
            
            if isinstance(data, list):
                universe = data
            else:
                universe = data[list(data.keys())[-1]]
                
        except Exception as e:
            print(f"[WARN] No se pudo cargar Universo Global: {e}")
            return
            
        if not universe:
             print("[WARN] El universo está vacío.")
             return
        
        print(f"[WHEELS] Descargando Snapshot Live para {len(universe)} tickers...")
        self.process_day(datetime.date.today(), universe, endpoint="live", save_mode="overwrite", output_name="live_top200_wheels.parquet")


class OratsEarningsDownloader:
    """
    Gestor de descarga y enriquecimiento de Earnings.
    """
    def __init__(self, api_key: str, base_dir: str):
        self.api_key = api_key
        self.base_dir = base_dir
        self.earnings_dir = os.path.join(base_dir, "earnings")
        self.ruedas_dir = os.path.join(base_dir, "ruedas")
        self.master_earnings_path = os.path.join(self.earnings_dir, "universe_earnings.parquet")
        
        os.makedirs(self.earnings_dir, exist_ok=True)

    async def fetch_earnings(self, session: aiohttp.ClientSession, start_date: datetime.date) -> List[dict]:
        """Descarga todos los earnings históricos desde una fecha."""
        url = "https://api.orats.io/datav2/hist/earnings"
        date_str = start_date.strftime("%Y-%m-%d")
        params = {"token": self.api_key, "tradeDate": date_str} 
        try:
            async with session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    return data.get('data', [])
                else:
                    text = await response.text()
                    print(f"[DEBUG] Error {response.status} fetching {date_str}: {text[:100]}")
        except Exception as e:
             print(f"[DEBUG] Exception fetching {date_str}: {e}")
        return []



    def _get_universe_from_wheels(self) -> Set[str]:
        """Obtiene todos los tickers únicos presentes en la carpeta de ruedas."""
        files = glob.glob(os.path.join(self.ruedas_dir, "rueda_*.parquet"))
        universe = set()
        print(f"[EARNINGS] Escaneando universo en {len(files)} archivos de ruedas...")
        
        # Optimización: Escanear solo el ultimo archivo si asumimos universo reciente, 
        # o escanear todos si queremos ser exhaustivos. 
        # El script original usaba 'universo_tickers.json'. Aquí usaremos los tickers encontrados en ruedas recientes.
        # Escaneemos los ultimos 5 archivos para tener un universo robusto pero rápido.
        files.sort()
        recent_files = files[-5:] if len(files) > 5 else files
        
        for f in recent_files:
            try:
                tickers = pl.scan_parquet(f).select("ticker").collect()["ticker"].to_list()
                universe.update(tickers)
            except: pass
            
        print(f"    Universo detectado: {len(universe)} tickers.")
        return universe

    def fetch_batch_worker(self, tickers_batch, date_from):
        """Worker para descarga paralela de Benzinga."""
        try:
            # Usar API KEY de Benzinga Hardcoded o pasada.
            # El usuario dio: JTm_3R45MwCC6fDqfG7fVDmw0YERgkBT
            # self.api_key viene de run_pipeline, que probablemente es la de ORATS.
            # Debemos usar la de Benzinga aquí.
            BENZINGA_KEY = "JTm_3R45MwCC6fDqfG7fVDmw0YERgkBT"
            client = RESTClient(BENZINGA_KEY)
            
            # Llamada a Benzinga
            # Nota: 'date_gte' filtra eventos desde esa fecha.
            api_data = client.list_benzinga_earnings(ticker_any_of=tickers_batch, date_gte=date_from, limit=1000)
            
            results = []
            batch_set = set(tickers_batch)
            start_ts = pd.Timestamp(date_from)

            for e in api_data:
                if e.ticker not in batch_set: continue
                
                record = {
                    'ticker': getattr(e, 'ticker', None),
                    'date': getattr(e, 'date', None),
                    'time': getattr(e, 'time', None),
                    'eps_estimate': getattr(e, 'eps_estimate', None),
                    'eps_actual': getattr(e, 'actual_eps', None),
                    'revenue_estimate': getattr(e, 'revenue_estimate', None),
                    'revenue_actual': getattr(e, 'actual_revenue', None),
                    'period': getattr(e, 'period', None),
                    'period_year': getattr(e, 'period_year', None)
                }

                if record['ticker'] and record['date']:
                    if pd.to_datetime(record['date']) >= start_ts:
                        results.append(record)
            return results
        except Exception as e:
            # print(f"   [ERROR] Batch Worker: {e}")
            return []

    def fetch_data_parallel(self, tickers, date_from, batch_size=30, max_workers=5):
        """Orquesta descarga paralela."""
        if not tickers: return []
        
        chunks = [tickers[i:i + batch_size] for i in range(0, len(tickers), batch_size)]
        total_records = []
        
        print(f"    -> Descargando {len(tickers)} tickers desde {date_from} (Batch={batch_size}, Hilos={max_workers})...")
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_chunk = {executor.submit(self.fetch_batch_worker, chunk, date_from): chunk for chunk in chunks}
            
            completed = 0
            for future in concurrent.futures.as_completed(future_to_chunk):
                try:
                    data = future.result()
                    total_records.extend(data)
                    completed += 1
                    if completed % 10 == 0:
                        print(f"       Progreso: {completed}/{len(chunks)}...", end='\r')
                except: pass
        
        print(f"\n    -> Obtenidos {len(total_records)} registros.")
        return total_records

    def download_earnings_history(self, start_date: str = "2019-01-01"):
        """
        Descarga el histórico de earnings desde Benzinga (Massive RESTClient).

        Por defecto descarga desde 2019-01-01 para cubrir todo el período
        histórico disponible en los datos de ruedas. En ejecuciones posteriores
        sólo actualiza los tickers ya conocidos con los últimos 45 días.

        Args:
            start_date: Fecha de inicio para tickers nuevos (formato YYYY-MM-DD).
                        Default: '2019-01-01' para cubrir el histórico completo.
        """
        print(f"\n[EARNINGS] Iniciando descarga desde BENZINGA (desde {start_date})...")
        
        # 1. Cargar Universo (desde ruedas recientes)
        target_universe = self._get_universe_from_wheels()
        if not target_universe: 
            print("[EARNINGS] Universo vacío. Abortando.")
            return

        # 2. Cargar Maestro Existente
        master = self.load_master_earnings()
        existing_tickers = set()
        if master is not None:
            existing_tickers = set(master["ticker"].unique().to_list())
            
        # 3. Estrategia:
        #   - Nuevos tickers → descarga completa desde start_date
        #   - Tickers existentes → actualización sólo de los últimos 45 días
        new_tickers    = list(target_universe - existing_tickers)
        update_tickers = list(target_universe.intersection(existing_tickers))
        
        print(f"[EARNINGS] Nuevos (Full desde {start_date}): {len(new_tickers)}")
        print(f"[EARNINGS] Actualización (últimos 45d)     : {len(update_tickers)}")
        
        new_data_list = []
        
        # 4. Descargar
        if new_tickers:
            print(f"[EARNINGS] Descargando historia completa para nuevos...")
            # Usamos el start_date parametrizado (default 2019-01-01)
            data = self.fetch_data_parallel(new_tickers, date_from=start_date, batch_size=30, max_workers=10)
            new_data_list.extend(data)
            
        if update_tickers:
            lookback = (datetime.date.today() - datetime.timedelta(days=45)).strftime("%Y-%m-%d")
            print(f"[EARNINGS] Actualizando existentes desde {lookback}...")
            data = self.fetch_data_parallel(update_tickers, date_from=lookback, batch_size=50, max_workers=10)
            new_data_list.extend(data)
            
        if not new_data_list:
            print("[EARNINGS] No hay nuevos datos.")
            return

        # 5. Guardar
        print(f"[EARNINGS] Procesando {len(new_data_list)} nuevos registros...")
        new_df = pl.DataFrame(new_data_list)
        
        # Asegurar tipos y conversión de fecha
        if "date" in new_df.columns:
            if new_df.schema["date"] == pl.String:
                 new_df = new_df.with_columns(pl.col("date").str.to_date("%Y-%m-%d", strict=False))
        
        # Concatenar con Master
        if master is not None:
            # Alineación básica
            # Polars concat requiere mismos tipos. 
            # new_df viene de dicts, sus tipos son inferidos. master viene de parquet.
            # Forzamos conversión si falla
            try:
                combined = pl.concat([master, new_df], how="diagonal")
            except Exception as e:
                print(f"[WARN] Schema mismatch en concat: {e}. Reintentando con cast str para seguridad...")
                # Fallback to string for mismatched cols if needed, but diagonal handles missing
                combined = pl.concat([master, new_df], how="diagonal_relaxed") 
        else:
            combined = new_df
            
        # Deduplicate
        # Keep last (most recent download info for same event)
        combined = combined.unique(subset=["ticker", "date"], keep="last").sort("date")
        
        combined.write_parquet(self.master_earnings_path)
        print(f"[EARNINGS] Maestro actualizado: {self.master_earnings_path} ({combined.height} rows)")

    def load_master_earnings(self) -> Optional[pl.DataFrame]:
        """Carga el archivo maestro de earnings y lo prepara para joins."""
        paths = [
            self.master_earnings_path,
            os.path.join(self.earnings_dir, "universe_earnings_2023.parquet")
        ]
        
        df = None
        for p in paths:
            if os.path.exists(p):
                print(f"[EARNINGS] Cargando maestro: {os.path.basename(p)}")
                try:
                    df = pl.read_parquet(p)
                    break
                except Exception as e:
                     print(f"[WARN] Error leyendo {p}: {e}")
        
        if df is None:
            print("[ERROR] No se encontró archivo maestro de earnings.")
            return None

        # Limpieza y preparación para join
        if "date" in df.columns:
            # Asegurar tipo Date
            try:
                df = df.with_columns(pl.col("date").cast(pl.Date, strict=False))
            except:
                # Si falla cast directo, puede ser string
                df = df.with_columns(pl.col("date").str.to_date("%Y-%m-%d", strict=False))
        
        # Ordenar por fecha para asof join (requerido por polars)
        # Nota: join_asof requiere que la columna 'on' esté ordenada.
        # Ordenamos por ticker y date para seguridad
        df = df.sort("date")
        return df

    def enrich_wheel_file(self, wheel_path: str, earnings_df: pl.DataFrame):
        """
        Agrega next_earning_date y prev_earning_date a un archivo de ruedas.
        """
        try:
            # 1. Leer Rueda
            df_wheel = pl.read_parquet(wheel_path)
            
            # Obtener fecha de la rueda 
            if "tradeDate" not in df_wheel.columns:
                # print(f"[SKIP] {os.path.basename(wheel_path)} - No tiene tradeDate")
                return

            # Convertir tradeDate a Date si es string
            if df_wheel.schema["tradeDate"] == pl.String:
                df_wheel = df_wheel.with_columns(pl.col("tradeDate").str.to_date("%Y-%m-%d", strict=False).alias("trade_date_dt"))
            elif df_wheel.schema["tradeDate"] == pl.Date:
                 df_wheel = df_wheel.with_columns(pl.col("tradeDate").alias("trade_date_dt"))
            else:
                 return # Tipo desconocido

            # Ordenar wheel para asof join
            df_wheel = df_wheel.sort("trade_date_dt") 
            
            # Preparar Earnings
            # Renombramos 'date' en earnings para clarity y evitar colisión
            earn_base = earnings_df.rename({"date": "earning_date"}).select(["ticker", "earning_date"])
            
            # 2. Join ASOF para Previous Earning (Earnings <= TradeDate)
            # strategy='backward' busca el valor mas cercano hacia atras (<=)
            
            # Join Prev
            combined = df_wheel.join_asof(
                earn_base,
                left_on="trade_date_dt",
                right_on="earning_date",
                by="ticker",
                strategy="backward"
            ).rename({"earning_date": "prev_earning_date"})
            
            # 3. Join ASOF para Next Earning (Earnings >= TradeDate)
            # strategy='forward' busca el valor mas cercano hacia adelante (>=)
            
            combined = combined.join_asof(
                earn_base,
                left_on="trade_date_dt",
                right_on="earning_date",
                by="ticker",
                strategy="forward"
            ).rename({"earning_date": "next_earning_date"})
            
            # Limpieza y Guardado
            combined = combined.drop("trade_date_dt")
            
            # Sobreescribir archivo (Atómico)
            temp_path = wheel_path + ".tmp_earn"
            combined.write_parquet(temp_path)
            
            if os.path.exists(wheel_path): os.remove(wheel_path)
            os.rename(temp_path, wheel_path)
            
        except Exception as e:
            print(f"[ERROR] Enriqueciendo {os.path.basename(wheel_path)}: {e}")

    def enrich_wheels_incremental(self, start_year: int = 2024):
        """
        Enriquece ruedas con datos de earnings y las guarda en una carpeta separada 'ruedas_earnings'.
        Solo procesa archivos desde start_year que no existan en el destino.
        """
        print(f"\n[EARNINGS] Iniciando enriquecimiento incremental (>= {start_year}) -> 'ruedas_earnings'...")
        
        dest_dir = os.path.join(self.base_dir, "ruedas_earnings")
        os.makedirs(dest_dir, exist_ok=True)
        
        master = self.load_master_earnings()
        if master is None: return
        
        # Filtramos master para tener solo date y ticker valido
        master = master.filter(pl.col("date").is_not_null()).sort("date")
        # Renombramos 'date' en earnings para clarity y evitar colisión
        earn_base = master.rename({"date": "earning_date"}).select(["ticker", "earning_date"])

        # Listar archivos fuente
        source_files = glob.glob(os.path.join(self.ruedas_dir, "rueda_*.parquet"))
        source_files.sort()
        
        count = 0
        skipped = 0
        
        for f in source_files:
            # Filtro fecha >= start_year
            try:
                date_str = os.path.basename(f).replace("rueda_", "").replace(".parquet", "")
                file_date = datetime.datetime.strptime(date_str, "%Y-%m-%d").date()
                if file_date.year < start_year:
                    continue
            except: continue
            
            # Check existencia en destino
            dest_path = os.path.join(dest_dir, f"rueda_{date_str}.parquet")
            if os.path.exists(dest_path):
                skipped += 1
                continue

            # Procesar
            try:
                # 1. Leer Rueda Fuente
                df_wheel = pl.read_parquet(f)
                
                # Obtener fecha de la rueda 
                if "tradeDate" not in df_wheel.columns:
                    continue

                # Convertir tradeDate a Date si es string
                if df_wheel.schema["tradeDate"] == pl.String:
                    df_wheel = df_wheel.with_columns(pl.col("tradeDate").str.to_date("%Y-%m-%d", strict=False).alias("trade_date_dt"))
                elif df_wheel.schema["tradeDate"] == pl.Date:
                     df_wheel = df_wheel.with_columns(pl.col("tradeDate").alias("trade_date_dt"))
                else:
                     continue

                # Ordenar wheel para asof join
                df_wheel = df_wheel.sort("trade_date_dt") 
                
                # 2. Join ASOF para Previous Earning
                combined = df_wheel.join_asof(
                    earn_base,
                    left_on="trade_date_dt",
                    right_on="earning_date",
                    by="ticker",
                    strategy="backward"
                ).rename({"earning_date": "prev_earning_date"})
                
                # 3. Join ASOF para Next Earning
                combined = combined.join_asof(
                    earn_base,
                    left_on="trade_date_dt",
                    right_on="earning_date",
                    by="ticker",
                    strategy="forward"
                ).rename({"earning_date": "next_earning_date"})
                
                # Limpieza
                combined = combined.drop("trade_date_dt")
                
                # Guardar en DESTINO
                combined.write_parquet(dest_path)
                # print(f"[OK] Guardado {os.path.basename(dest_path)}")
                count += 1
                
                if count % 10 == 0:
                    print(f"   ... Procesados {count} nuevos archivos.")
                    
            except Exception as e:
                print(f"[ERROR] Enriqueciendo {os.path.basename(f)}: {e}")

        print(f"[EARNINGS] Finalizado. Procesados: {count}, Saltados (Ya existen): {skipped}")

    def enrich_live_wheels(self):
        """
        Enriquece el archivo live_top200_wheels.parquet y lo guarda en 'ruedas_earnings'.
        """
        print(f"\n[EARNINGS] Enriqueciendo LIVE Top 200 Wheels...")
        
        live_file = "live_top200_wheels.parquet"
        source_path = os.path.join(self.ruedas_dir, live_file)
        dest_dir = os.path.join(self.base_dir, "ruedas_earnings")
        dest_path = os.path.join(dest_dir, live_file)
        
        if not os.path.exists(source_path):
            print(f"[WARN] No se encontró {live_file} en ruedas. Saltando.")
            return

        master = self.load_master_earnings()
        if master is None: return
        
        # Filtramos master
        master = master.filter(pl.col("date").is_not_null()).sort("date")
        earn_base = master.rename({"date": "earning_date"}).select(["ticker", "earning_date"])

        try:
            # 1. Leer Rueda Fuente
            df_wheel = pl.read_parquet(source_path)
            
            # Validar tradeDate
            if "tradeDate" not in df_wheel.columns:
                 print(f"[WARN] {live_file} no tiene columna tradeDate.")
                 return

            # Convertir tradeDate
            if df_wheel.schema["tradeDate"] == pl.String:
                df_wheel = df_wheel.with_columns(pl.col("tradeDate").str.to_date("%Y-%m-%d", strict=False).alias("trade_date_dt"))
            elif df_wheel.schema["tradeDate"] == pl.Date:
                 df_wheel = df_wheel.with_columns(pl.col("tradeDate").alias("trade_date_dt"))
            else:
                 print(f"[WARN] Tipo de tradeDate desconocido en {live_file}.")
                 return

            # Ordenar
            df_wheel = df_wheel.sort("trade_date_dt") 
            
            # 2. Join ASOF Prev
            combined = df_wheel.join_asof(
                earn_base,
                left_on="trade_date_dt",
                right_on="earning_date",
                by="ticker",
                strategy="backward"
            ).rename({"earning_date": "prev_earning_date"})
            
            # 3. Join ASOF Next
            combined = combined.join_asof(
                earn_base,
                left_on="trade_date_dt",
                right_on="earning_date",
                by="ticker",
                strategy="forward"
            ).rename({"earning_date": "next_earning_date"})
            
            # Limpieza
            combined = combined.drop("trade_date_dt")
            
            # Guardar en DESTINO
            os.makedirs(dest_dir, exist_ok=True)
            combined.write_parquet(dest_path)
            print(f"[OK] Live Wheels enriquecido guardado en {dest_path}")
                
        except Exception as e:
            print(f"[ERROR] Enriqueciendo LIVE: {e}")

# ==============================================================================
# UNIVERSE BUILDER
# Construye un diccionario diario {fecha: [tickers]} a partir de los archivos
# core descargados en datos_core. Aplicación de filtros, propagación hacia
# atrás de 90 días calendario, validación de consistencia y persistencia
# incremental en universo/universo.json.
# ==============================================================================

class UniverseBuilder:
    """
    Construye y mantiene el universo de tickers negociables día a día.

    Flujo completo
    --------------
    1. Escanea `datos_core` y detecta fechas ya procesadas en universo.json
       (actualización incremental: sólo se procesan fechas nuevas).
    2. Para cada fecha nueva lee el Parquet y aplica filtros vectorizados:
         a. Top N por volumen total (cOi + pOi).
         b. Precio stkPx >= MIN_PRICE (default $5).
         c. Excluye tickers que contengan '_C'.
    3. Propaga tickers hacia atrás: si un ticker existe en el día X,
       se añade a todos los días dentro de los 90 días calendarios anteriores.
    4. Valida consistencia: si un ticker está en el día X pero no aparece
       en ninguno de los 90 días anteriores, se elimina de X.
    5. Fusiona con universo.json existente y guarda (escritura atómica).

    Optimizaciones
    --------------
    - Lectura Parquet con Polars lazy (push-down de predicados).
    - Filtros y agregaciones vectorizados sin bucles Python sobre filas.
    - ThreadPoolExecutor para I/O paralelo de archivos Parquet.
    - Propagación y validación usando operaciones de set Python (O(1) lookup).
    - Búsqueda de ventanas de 90 días con bisect_left (O(log N)).
    """

    # ------------------------------------------------------------------
    # Constantes configurables de clase
    # ------------------------------------------------------------------
    MIN_PRICE   = 5.0       # Precio mínimo del subyacente (USD)
    LOOKBACK    = 90        # Días calendario de look-back para propagación
    OUTPUT_FILE = "universo.json"  # Nombre del archivo JSON de salida

    def __init__(self, core_dir: str, root_dir: str, max_workers: int = 8):
        """
        Inicializa el UniverseBuilder.

        Args:
            core_dir   : Ruta absoluta a datos_core (archivos YYYY-MM-DD.parquet).
            root_dir   : Raíz de datos_modelo_portfolio. Dentro se crea 'universo/'.
            max_workers: Número máximo de hilos para lectura paralela de Parquets.
        """
        self.core_dir    = core_dir
        self.output_dir  = os.path.join(root_dir, "universo")
        self.output_path = os.path.join(self.output_dir, self.OUTPUT_FILE)
        self.max_workers = max_workers

        # Crear directorio de salida si no existe
        os.makedirs(self.output_dir, exist_ok=True)

    # ------------------------------------------------------------------
    # Métodos de soporte
    # ------------------------------------------------------------------

    def _load_existing_universe(self) -> dict:
        """
        Carga universo.json desde disco si existe.

        Returns:
            dict  {fecha_str: [tickers]}, o {} si no existe el archivo.
        """
        if os.path.exists(self.output_path):
            try:
                with open(self.output_path, "r", encoding="utf-8") as fh:
                    data = json.load(fh)
                    print(f"[UNIVERSE] Cargado universo existente con "
                          f"{len(data)} fechas desde: {self.output_path}")
                    return data
            except Exception as exc:
                print(f"[WARN] No se pudo leer universo existente: {exc}")
        return {}

    def _get_core_dates(self) -> List[str]:
        """
        Obtiene la lista ordenada de fechas (YYYY-MM-DD) disponibles
        en datos_core como archivos .parquet.

        Returns:
            List[str]: Fechas válidas ordenadas ascendentemente.
        """
        files = glob.glob(os.path.join(self.core_dir, "*.parquet"))
        dates = []
        for f in files:
            base = os.path.basename(f).replace(".parquet", "")
            try:
                datetime.datetime.strptime(base, "%Y-%m-%d")  # Validar formato
                dates.append(base)
            except ValueError:
                continue  # Ignorar archivos con nombre no válido
        return sorted(dates)

    def _filter_tickers_for_date(self, date_str: str, top_n: int) -> List[str]:
        """
        Lee el Parquet de una fecha y retorna los tickers que pasan los 3 filtros.

        Filtros aplicados vectorialmente con Polars:
          a. Top N por volumen total (cOi + pOi) – las N líneas con mayor OI.
          b. Precio máximo del ticker ese día >= MIN_PRICE (stkPx).
          c. El nombre del ticker NO contiene '_C'.

        Normalización automática:
          - cOi / callOpenInterest → 'cOi'
          - pOi / putOpenInterest  → 'pOi'
          - stkPx y varios alias   → 'stkPx'

        Args:
            date_str : Fecha en formato YYYY-MM-DD.
            top_n    : Máximo de tickers a seleccionar.

        Returns:
            List[str]: Tickers seleccionados, o [] si fallo o sin datos.
        """
        path = os.path.join(self.core_dir, f"{date_str}.parquet")
        if not os.path.exists(path):
            return []

        try:
            # Lectura lazy (Polars no carga todo en RAM hasta .collect())
            lf = pl.scan_parquet(path)
            schema_names = lf.collect_schema().names()

            # ---------------------------------------------------------------
            # Normalización de columnas OI (cOi / pOi)
            # La API de ORATS puede usar nombres alternativos según la versión.
            # ---------------------------------------------------------------
            rename_map: dict = {}

            # Columna de call open interest
            if "cOi" not in schema_names:
                if "callOpenInterest" in schema_names:
                    rename_map["callOpenInterest"] = "cOi"
                else:
                    # Crear columna cero si no hay información de OI de calls
                    lf = lf.with_columns(pl.lit(0).alias("cOi"))

            # Columna de put open interest
            if "pOi" not in schema_names:
                if "putOpenInterest" in schema_names:
                    rename_map["putOpenInterest"] = "pOi"
                else:
                    # Crear columna cero si no hay información de OI de puts
                    lf = lf.with_columns(pl.lit(0).alias("pOi"))

            # ---------------------------------------------------------------
            # Normalización de columna de precio (stkPx)
            # ---------------------------------------------------------------
            if "stkPx" not in schema_names:
                price_candidates = [
                    "stockPrice", "price", "spot",
                    "priorCls", "underlyingPrice", "close"
                ]
                found_price = next(
                    (c for c in price_candidates if c in schema_names), None
                )
                if found_price:
                    rename_map[found_price] = "stkPx"
                else:
                    # Sin precio conocido: asignar 0 para que sea filtrado
                    lf = lf.with_columns(pl.lit(0.0).alias("stkPx"))

            # Aplicar todos los renombramientos acumulados en una sola pasada
            if rename_map:
                lf = lf.rename(rename_map)

            # Verificar que 'ticker' exista después de las transformaciones
            updated_schema = lf.collect_schema().names()
            if "ticker" not in updated_schema:
                return []

            # ---------------------------------------------------------------
            # Pipeline vectorizado de filtrado y selección (Polars lazy):
            # 1. group_by ticker → sumar cOi+pOi y max(stkPx) por ticker.
            # 2. Filtro b: max_price >= MIN_PRICE.
            # 3. Filtro c: excluir '_C' en el nombre del ticker.
            # 4. Ordenar descendente por volumen total.
            # 5. Limitar a top_n tickers.
            # Todo se ejecuta como un único plan de ejecución optimizado.
            # ---------------------------------------------------------------
            result = (
                lf
                # Agrupar por ticker para consolidar múltiples strikes/exp en uno
                .group_by("ticker")
                .agg([
                    # Volumen total = Open Interest Calls + Open Interest Puts
                    (
                        pl.col("cOi")
                          .cast(pl.Float64, strict=False)
                          .fill_null(0)
                          .sum()
                        +
                        pl.col("pOi")
                          .cast(pl.Float64, strict=False)
                          .fill_null(0)
                          .sum()
                    ).alias("total_vol"),
                    # Precio máximo del ticker ese día (representativo)
                    pl.col("stkPx")
                      .cast(pl.Float64, strict=False)
                      .fill_null(0)
                      .max()
                      .alias("max_price"),
                ])
                # Filtro b: precio mínimo $5
                .filter(pl.col("max_price") >= self.MIN_PRICE)
                # Filtro c: excluir tickers con '_C' (p.ej. ETFs de cobertura)
                .filter(~pl.col("ticker").str.contains("_C"))
                # Filtro a: top N por volumen (sort desc + limit)
                .sort("total_vol", descending=True)
                .limit(top_n)
                # Solo necesitamos el nombre del ticker
                .select("ticker")
                .collect()  # Materializar el lazy frame
            )

            return result["ticker"].to_list()

        except Exception as exc:
            # Reportar error pero no interrumpir el proceso completo
            print(f"[ERROR] Leyendo/filtrando {date_str}: {exc}")
            return []

    # ------------------------------------------------------------------
    # Método principal de construcción
    # ------------------------------------------------------------------

    def build(self, top_n: int = 200) -> None:
        """
        Ejecuta el pipeline completo de construcción y actualización del universo.

        Pasos internos:
            A. Cargar universo existente + detectar fechas nuevas (incremental).
            B. Leer y filtrar tickers en paralelo (ThreadPoolExecutor).
            C. Fusionar nuevas fechas con el universo existente.
            D. Propagación hacia atrás de 90 días para cada ticker.
            E. Validación de consistencia: eliminar tickers sin historia previa.
            F. Serializar y guardar universo.json (escritura atómica).

        Args:
            top_n: Número máximo de tickers por día (default 200).
        """
        import bisect  # Solo necesario aquí; import local para mantener módulo ligero

        print(f"\n[UNIVERSE] ── Construcción del Universo Diario "
              f"(top_n={top_n}) ──")

        # ----------------------------------------------------------------
        # PASO A: Detectar fechas nuevas vs. ya procesadas
        # ----------------------------------------------------------------
        existing_universe: dict = self._load_existing_universe()
        processed_dates: set   = set(existing_universe.keys())

        all_core_dates: List[str] = self._get_core_dates()

        # Sólo procesamos fechas que no están en el universo existente
        new_dates: List[str] = [
            d for d in all_core_dates if d not in processed_dates
        ]

        if not new_dates:
            print("[UNIVERSE] Universo al día. No hay fechas nuevas.")
            return

        print(f"[UNIVERSE] Total fechas en disco   : {len(all_core_dates):>6}")
        print(f"[UNIVERSE] Ya procesadas           : {len(processed_dates):>6}")
        print(f"[UNIVERSE] A procesar (nuevas)     : {len(new_dates):>6}")

        # ----------------------------------------------------------------
        # PASO B: Lectura y filtrado paralelo de archivos nuevos.
        # ThreadPoolExecutor aprovecha concurrencia de I/O de disco.
        # Polars libera el GIL durante operaciones de lectura/cómputo.
        # ----------------------------------------------------------------
        print(f"\n[UNIVERSE] Leyendo {len(new_dates)} archivos "
              f"({self.max_workers} hilos en paralelo)...")

        new_day_tickers: dict = {}  # { "YYYY-MM-DD": [tickers] }

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=self.max_workers
        ) as executor:
            # Despachar todas las tareas de filtrado
            future_to_date = {
                executor.submit(self._filter_tickers_for_date, d, top_n): d
                for d in new_dates
            }
            # Recolectar resultados a medida que van completando
            completed = 0
            for future in concurrent.futures.as_completed(future_to_date):
                date_key = future_to_date[future]
                try:
                    tickers = future.result()
                    # Guardar incluso listas vacías para marcar como procesado
                    new_day_tickers[date_key] = tickers
                except Exception as exc:
                    print(f"[ERROR] Tarea para {date_key}: {exc}")
                    new_day_tickers[date_key] = []

                completed += 1
                if completed % 20 == 0 or completed == len(new_dates):
                    pct = completed / len(new_dates) * 100
                    print(f"   ... {completed}/{len(new_dates)} ({pct:.0f}%)")

        days_with_tickers = sum(
            1 for v in new_day_tickers.values() if v
        )
        print(f"[UNIVERSE] Lectura completada. "
              f"{days_with_tickers}/{len(new_dates)} días con tickers.")

        # ----------------------------------------------------------------
        # PASO C: Fusionar existente + nuevas fechas en un dict de sets.
        # Usamos sets para que las operaciones de unión e intersección
        # sean O(n) en lugar de O(n²) con listas.
        # ----------------------------------------------------------------
        full_universe: dict = {}

        # Convertir universo existente a sets
        for date_k, tickers_list in existing_universe.items():
            full_universe[date_k] = set(tickers_list)

        # Agregar fechas nuevas
        for date_k, tickers_list in new_day_tickers.items():
            full_universe[date_k] = set(tickers_list)

        # Lista de fechas ordenada ascendentemente (usada en D y E)
        sorted_dates: List[str] = sorted(full_universe.keys())

        # Precomputar objetos date para evitar N parseos dentro de bucles
        date_objects: dict = {
            d: datetime.datetime.strptime(d, "%Y-%m-%d").date()
            for d in sorted_dates
        }
        time_delta_90 = datetime.timedelta(days=self.LOOKBACK)

        # ----------------------------------------------------------------
        # PASO D: Propagación hacia atrás de 90 días calendario.
        #
        # Por cada fecha X con tickers T(X):
        #   Por cada fecha Y tal que  0 < (X - Y) <= 90 días:
        #     full_universe[Y] = full_universe[Y] ∪ T(X)
        #
        # Esto garantiza que si un ticker entra en el universo en el día X,
        # ya está disponible en los 90 días previos (necesario para backtesting
        # y para la ventana de opciones de 3 meses).
        #
        # Optimización: bisect_left para encontrar el inicio de la ventana
        # en O(log N) en lugar de recorrer todas las fechas.
        # ----------------------------------------------------------------
        print(f"\n[UNIVERSE] Propagando tickers {self.LOOKBACK} días hacia atrás...")
        propagation_adds = 0

        for idx, date_x in enumerate(sorted_dates):
            tickers_x = full_universe[date_x]
            if not tickers_x:
                continue  # Nada que propagar si no hay tickers

            obj_x      = date_objects[date_x]
            cutoff_dt  = obj_x - time_delta_90
            cutoff_str = cutoff_dt.strftime("%Y-%m-%d")

            # Índice del primer elemento >= cutoff_str (búsqueda binaria)
            left_idx = bisect.bisect_left(sorted_dates, cutoff_str)

            # Propagar a todos los días anteriores a X dentro de la ventana
            for prev_idx in range(left_idx, idx):
                date_y = sorted_dates[prev_idx]
                added  = tickers_x - full_universe[date_y]
                if added:
                    full_universe[date_y] |= added  # Unión in-place (set)
                    propagation_adds += len(added)

        print(f"[UNIVERSE] Propagación completada. "
              f"Adiciones totales: {propagation_adds:,}.")

        # ----------------------------------------------------------------
        # PASO E: Validación de consistencia (DESACTIVADO A PEDIDO DEL USUARIO)
        # Se elimina la regla de "huérfanos" para garantizar que si un ticker entra
        # al ranking hoy (top_n), se conserve SIEMPRE (y se inyectó a los 90 días 
        # anteriores en el Paso D), sin recortar el mínimo esperado por día.
        # ----------------------------------------------------------------
        pass


        # ----------------------------------------------------------------
        # PASO F: Convertir sets → listas ordenadas y guardar JSON.
        # Escritura atómica: escribir en .tmp y renombrar al final para
        # evitar archivos corruptos en caso de fallo durante la escritura.
        # ----------------------------------------------------------------
        final_universe: dict = {
            date_k: sorted(tickers_set)  # Ordenar para reproducibilidad
            for date_k, tickers_set in full_universe.items()
        }

        # Ordenar por fecha para que el JSON sea determinista y legible
        final_universe_sorted = dict(sorted(final_universe.items()))

        try:
            temp_path = self.output_path + ".tmp"

            # Escribir en archivo temporal primero
            with open(temp_path, "w", encoding="utf-8") as fh:
                json.dump(final_universe_sorted, fh,
                          indent=2, ensure_ascii=False)

            # Reemplazo atómico: eliminar archivo anterior → renombrar temporal
            if os.path.exists(self.output_path):
                os.remove(self.output_path)
            os.rename(temp_path, self.output_path)

            # Log de resultados finales
            total_dates   = len(final_universe_sorted)
            sample_date   = next(iter(final_universe_sorted), "N/A")
            sample_count  = len(final_universe_sorted.get(sample_date, []))

            print(f"\n[UNIVERSE] ✓ universo.json guardado: {self.output_path}")
            print(f"[UNIVERSE]   Total fechas            : {total_dates:>6}")
            print(f"[UNIVERSE]   Ejemplo ({sample_date}) : "
                  f"{sample_count} tickers")

        except Exception as exc:
            print(f"[ERROR] Guardando universo.json: {exc}")
            # Si falló, intentar borrar el .tmp si existe
            if os.path.exists(temp_path):
                os.remove(temp_path)


# ==============================================================================
# WHEELS CALL EARN BUILDER
# A partir del universo.json y los archivos de ruedas, genera un dataset filtra-
# do de opciones CALL listo para análisis de earnings / wheel strategy.
#
# Filtros de calidad aplicados por fecha:
#   1. Solo tickers del universo ese día (joins con universo.json).
#   2. Elimina strikes menores al precio del subyacente (calls OTM profundo).
#   3. Elimina filas con callOpenInterest == 0 o callVolume == 0.
#   4. Elimina filas con callBidPrice == 0 o callAskPrice == 0.
#   5. Elimina filas donde callAskPrice / callBidPrice > 1.40 (spread excesivo).
#
# Columnas de salida:
#   ticker, tradeDate, expirDate, dte, strike, stockPrice,
#   callVolume, callOpenInterest, callMidPrice   (promedio bid/ask)
#
# Optimizaciones:
#   - Polars lazy scan con predicate pushdown.
#   - ProcessPoolExecutor para paralelismo real por CPU (no comparte GIL).
#   - Escritura atómica (.tmp → rename) para evitar archivos corruptos.
#   - Incremental: omite fechas ya procesadas en ruedas_call_earn/.
# ==============================================================================

class WheelsCallEarnBuilder:
    """
    Filtra las cadenas de opciones (ruedas) para el universo de tickers
    seleccionado en universo.json y aplica criterios de calidad sobre
    opciones CALL, generando un dataset limpio por fecha en
    `ruedas_call_earn/`.

    Columnas de salida
    ------------------
    ticker, tradeDate, expirDate, dte, strike, stockPrice,
    callVolume, callOpenInterest, callMidPrice

    Donde callMidPrice = (callBidPrice + callAskPrice) / 2.

    Filtros de calidad (aplicados en orden vectorizado)
    ---------------------------------------------------
    a. strike >= stockPrice                  → sólo calls ATM u OTM
    b. callOpenInterest > 0 AND callVolume > 0  → liquidez mínima
    c. callBidPrice > 0 AND callAskPrice > 0    → precios válidos
    d. callAskPrice / callBidPrice <= 1.40      → spread razonable
    """

    # Columnas que deben preservarse en el output
    OUTPUT_COLS = [
        "ticker", "tradeDate", "expirDate", "dte", "strike",
        "stockPrice", "callVolume", "callOpenInterest", "callMidPrice"
    ]

    # Máximo ratio Ask/Bid permitido (filtro de spread)
    MAX_ASK_BID_RATIO = 1.40

    def __init__(
        self,
        universo_path: str,
        ruedas_dir: str,
        output_dir: str,
        max_workers: int = 4,
    ):
        """
        Inicializa el builder.

        Args:
            universo_path : Ruta a universo.json  { fecha: [tickers] }.
            ruedas_dir    : Carpeta con archivos rueda_YYYY-MM-DD.parquet.
            output_dir    : Carpeta de salida ruedas_call_earn/.
            max_workers   : Procesos paralelos (ProcessPoolExecutor).
                            Usar 4 como default conservador para I/O intensivo.
        """
        self.universo_path = universo_path
        self.ruedas_dir    = ruedas_dir
        self.output_dir    = output_dir
        self.max_workers   = max_workers

        # Crear carpeta de salida si no existe
        os.makedirs(self.output_dir, exist_ok=True)

    # ------------------------------------------------------------------
    # Métodos de soporte
    # ------------------------------------------------------------------

    def _load_universe(self) -> dict:
        """
        Carga universo.json del disco.
        Si es una lista plana, construye un dict {fecha: [tickers]}
        para todas las fechas disponibles de ruedas.

        Returns:
            dict: { "YYYY-MM-DD": ["AAPL", "MSFT", ...] }
        """
        with open(self.universo_path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        
        if isinstance(data, list):
            # Convert list back to date->list format assuming all available wheels dates
            wheel_files = glob.glob(os.path.join(self.ruedas_dir, "rueda_*.parquet"))
            fake_dict = {}
            for f in wheel_files:
                base = os.path.basename(f).replace("rueda_", "").replace(".parquet", "")
                fake_dict[base] = data
            return fake_dict
        return data

    def _get_processed_dates(self) -> set:
        """
        Devuelve el conjunto de fechas que ya fueron procesadas
        (existen como archivo en output_dir).

        Returns:
            set: Fechas ya procesadas como strings YYYY-MM-DD.
        """
        files = glob.glob(os.path.join(self.output_dir, "call_*.parquet"))
        processed = set()
        for f in files:
            base = os.path.basename(f).replace("call_", "").replace(".parquet", "")
            try:
                datetime.datetime.strptime(base, "%Y-%m-%d")
                processed.add(base)
            except ValueError:
                continue
        return processed

    @staticmethod
    def _process_single_date(args: tuple) -> tuple:
        """
        Worker estático que procesa UNA fecha: filtra la rueda por universo
        y aplica los criterios de calidad sobre opciones CALL.

        Diseñado para ejecutarse en un proceso separado (ProcessPoolExecutor).
        Al ser estático y recibir todo por argumento, es picklable por defecto.

        Args:
            args: Tupla (date_str, tickers, ruedas_dir, output_dir, max_ratio)

        Returns:
            Tupla (date_str, n_rows_salvadas, n_rows_totales, mensaje_error)
            Si hay error, n_rows = -1 y mensaje_error describe el fallo.
        """
        import polars as pl  # Import dentro del worker (proceso separado)
        import os

        date_str, tickers, ruedas_dir, output_dir, max_ratio = args

        # ----------------------------------------------------------------
        # Verificar existencia del archivo de rueda para esta fecha
        # ----------------------------------------------------------------
        if date_str == "live":
             rueda_path = os.path.join(ruedas_dir, "live_top200_wheels.parquet")
             out_path   = os.path.join(output_dir, "call_live_top200_wheels.parquet")
        else:
             rueda_path = os.path.join(ruedas_dir, f"rueda_{date_str}.parquet")
             out_path   = os.path.join(output_dir, f"call_{date_str}.parquet")

        if not os.path.exists(rueda_path):
            return (date_str, 0, 0, f"Archivo no encontrado: {rueda_path}")

        try:
            # ------------------------------------------------------------
            # PASO 1: Leer rueda y filtrar por universo de tickers.
            # Usamos scan_parquet + filter para leer sólo las filas del
            # universo (Polars aplica predicate pushdown sobre el parquet).
            # ------------------------------------------------------------
            # Convertir lista a set de Polars para filtro eficiente
            # SI tickers es None, procesamos todo el archivo (Modo Live completo)
            if tickers is not None:
                ticker_set = pl.Series("ticker", tickers, dtype=pl.Utf8)
                lf = (
                    pl.scan_parquet(rueda_path)
                    # Filtrar solo tickers del universo (inner join implícito)
                    .filter(pl.col("ticker").is_in(ticker_set))
                )
            else:
                lf = pl.scan_parquet(rueda_path)

            # ------------------------------------------------------------
            # Verificar que las columnas requeridas existen en el schema
            # Para manejar posibles variaciones del archivo histórico
            # ------------------------------------------------------------
            schema_names = lf.collect_schema().names()
            required_cols = [
                "ticker", "tradeDate", "expirDate", "dte",
                "strike", "stockPrice",
                "callVolume", "callOpenInterest",
                "callBidPrice", "callAskPrice",
            ]
            missing = [c for c in required_cols if c not in schema_names]
            if missing:
                return (date_str, -1, -1,
                        f"Columnas faltantes en {date_str}: {missing}")

            # ------------------------------------------------------------
            # PASO 2: Aplicar filtros de calidad en pipeline vectorizado.
            # Todos los predicados se evalúan en paralelo por Polars
            # antes de materializar (lazy → collect).
            # ------------------------------------------------------------
            filtered = (
                lf
                # --- Filtro a: eliminar strikes < stockPrice ---
                # Solo queremos calls ATM o Out-of-The-Money (strike >= spot)
                # ya que son las candidatas para wheel strategy (covered calls)
                .filter(pl.col("strike") >= pl.col("stockPrice"))

                # --- Filtro b: liquidez mínima ---
                # Elimina opciones sin interés abierto o sin volumen del día.
                # Un callOpenInterest=0 indica que nadie mantiene esa posición.
                # Un callVolume=0 indica que no hubo transacciones ese día.
                .filter(
                    (pl.col("callOpenInterest").cast(pl.Float64, strict=False)
                                               .fill_null(0) > 0)
                    &
                    (pl.col("callVolume").cast(pl.Float64, strict=False)
                                        .fill_null(0) > 0)
                )

                # --- Filtro c: precios válidos ---
                # Un precio bid o ask de cero significa que no hay mercado
                # activo para esa opción (o error de datos).
                .filter(
                    (pl.col("callBidPrice").cast(pl.Float64, strict=False)
                                           .fill_null(0) > 0)
                    &
                    (pl.col("callAskPrice").cast(pl.Float64, strict=False)
                                           .fill_null(0) > 0)
                )

                # --- Filtro d: spread razonable (Ask/Bid <= 1.40) ---
                # Un ratio Ask/Bid > 1.40 indica un spread excesivo (>40%)
                # que hace prohibitivo operar esa opción de forma eficiente.
                .filter(
                    (
                        pl.col("callAskPrice").cast(pl.Float64, strict=False)
                        /
                        pl.col("callBidPrice").cast(pl.Float64, strict=False)
                    ) <= max_ratio
                )

                # --- Generar columna callMidPrice ---
                # El precio mid es el punto de referencia estándar para
                # valorar una opción: promedio entre bid y ask.
                .with_columns(
                    (
                        (pl.col("callBidPrice").cast(pl.Float64, strict=False)
                         + pl.col("callAskPrice").cast(pl.Float64, strict=False))
                        / 2.0
                    ).alias("callMidPrice")
                )

                # --- Seleccionar solo columnas de salida ---
                .select([
                    "ticker", "tradeDate", "expirDate", "dte",
                    "strike", "stockPrice",
                    "callVolume", "callOpenInterest", "callMidPrice"
                ])

                # --- Materializar el lazy frame ---
                .collect()
            )

            rows_out = len(filtered)

            if rows_out == 0:
                # Si no quedan filas después de filtros, registramos la fecha
                # con un parquet vacío para marcarla como procesada.
                # Esto evita re-procesar en ejecuciones futuras.
                pass

            # ------------------------------------------------------------
            # PASO 3: Guardar resultado en output_dir con escritura atómica.
            # Escribimos a un .tmp primero; si el proceso se interrumpe,
            # el archivo original (si existía) no queda corrupto.
            # ------------------------------------------------------------
            temp_path = out_path + ".tmp"

            filtered.write_parquet(temp_path, compression="snappy")

            # Reemplazo atómico
            if os.path.exists(out_path):
                os.remove(out_path)
            os.rename(temp_path, out_path)

            return (date_str, rows_out, rows_out, None)

        except Exception as exc:
            import traceback
            return (date_str, -1, -1, traceback.format_exc())

    def build_live(self) -> None:
        print(f"\n[WHEELS_CALL] ── Generación Ruedas Live (Calls) ──")
        self._process_single_date(("live", None, self.ruedas_dir, self.output_dir, self.MAX_ASK_BID_RATIO))
        print(f"[WHEELS_CALL] ✓ Proceso live completado.")

    # ------------------------------------------------------------------
    # Método principal de construcción
    # ------------------------------------------------------------------

    def build(
        self,
        date_filter: Optional[str] = None,
        years: Optional[List[int]] = None,
    ) -> None:
        """
        Ejecuta el pipeline de generación de ruedas_call_earn.

        Soporta dos modos:
        - Completo    : procesa todas las fechas comunes universo/ruedas.
        - Filtrado    : solo un año específico (`years=[2020]`) o una fecha
                        exacta (`date_filter="2020-03-15"`).

        El proceso es INCREMENTAL: salta fechas ya procesadas.

        Args:
            date_filter : Procesar sólo esta fecha exacta (YYYY-MM-DD).
            years       : Lista de años a procesar (e.g. [2020, 2021]).
                          Si ambos son None, procesa todo.
        """
        print(f"\n[WHEELS_CALL] ── Generación de Ruedas Filtradas (Calls) ──")
        print(f"[WHEELS_CALL] Salida: {self.output_dir}")

        # ----------------------------------------------------------------
        # A: Cargar universo y detectar fechas ya procesadas
        # ----------------------------------------------------------------
        print("[WHEELS_CALL] Cargando universo.json...")
        universe = self._load_universe()
        processed_dates = self._get_processed_dates()

        print(f"[WHEELS_CALL] Fechas en universo   : {len(universe):>6}")
        print(f"[WHEELS_CALL] Ya procesadas         : {len(processed_dates):>6}")

        # ----------------------------------------------------------------
        # B: Determinar fechas a procesar según filtros del usuario
        # ----------------------------------------------------------------
        all_dates = sorted(universe.keys())

        # Aplicar filtros de selección
        if date_filter:
            # Modo fecha única (útil para pruebas o reprocesar una fecha)
            target_dates = [date_filter] if date_filter in universe else []
            if not target_dates:
                print(f"[WARN] Fecha {date_filter} no existe en universo.json")
                return
        elif years:
            # Filtrar por año(s) específico(s)
            year_strs = [str(y) for y in years]
            target_dates = [d for d in all_dates if d[:4] in year_strs]
        else:
            # Todas las fechas
            target_dates = all_dates

        # Intersección con archivos de ruedas disponibles en disco
        ruedas_available = set(
            os.path.basename(f).replace("rueda_", "").replace(".parquet", "")
            for f in glob.glob(os.path.join(self.ruedas_dir, "rueda_*.parquet"))
        )
        target_dates = [d for d in target_dates if d in ruedas_available]

        # Quitar fechas ya procesadas (incremental)
        new_dates = [d for d in target_dates if d not in processed_dates]

        if not new_dates:
            print("[WHEELS_CALL] Todo al día. No hay fechas nuevas.")
            return

        print(f"[WHEELS_CALL] Fechas objetivo       : {len(target_dates):>6}")
        print(f"[WHEELS_CALL] A procesar (nuevas)   : {len(new_dates):>6}")

        # ----------------------------------------------------------------
        # C: Construir argumentos para el worker (una tupla por fecha)
        # ----------------------------------------------------------------
        worker_args = [
            (
                date_str,           # fecha
                universe[date_str], # lista de tickers del universo ese día
                self.ruedas_dir,    # directorio de ruedas
                self.output_dir,    # directorio de salida
                self.MAX_ASK_BID_RATIO,  # ratio máximo Ask/Bid
            )
            for date_str in new_dates
        ]

        # ----------------------------------------------------------------
        # D: Ejecutar en paralelo con ProcessPoolExecutor.
        # Se usa ProcessPoolExecutor (no Thread) porque el procesamiento
        # de Polars es CPU-bound una vez cargados los datos.
        # Cada proceso tiene su propia copia del GIL → true parallelism.
        # ----------------------------------------------------------------
        print(f"\n[WHEELS_CALL] Procesando {len(new_dates)} fechas "
              f"({self.max_workers} procesos en paralelo)...")

        total_rows  = 0
        errors      = 0
        completed   = 0

        with concurrent.futures.ProcessPoolExecutor(
            max_workers=self.max_workers
        ) as executor:
            # Mapear cada tarea al argumento correspondiente
            future_to_date = {
                executor.submit(self._process_single_date, args): args[0]
                for args in worker_args
            }

            for future in concurrent.futures.as_completed(future_to_date):
                date_key = future_to_date[future]
                try:
                    date_str, rows_out, rows_total, err = future.result()
                    if err and rows_out == -1:
                        # Error en el worker
                        print(f"[ERROR] {date_str}: {err[:120]}...")
                        errors += 1
                    else:
                        total_rows += rows_out
                except Exception as exc:
                    print(f"[ERROR] Future {date_key}: {exc}")
                    errors += 1

                completed += 1
                if completed % 20 == 0 or completed == len(new_dates):
                    pct = completed / len(new_dates) * 100
                    print(f"   ... {completed}/{len(new_dates)} ({pct:.0f}%) "
                          f"| Filas guardadas: {total_rows:,} | Errores: {errors}")

        # ----------------------------------------------------------------
        # E: Reporte final
        # ----------------------------------------------------------------
        success = completed - errors
        print(f"\n[WHEELS_CALL] ✓ Proceso completado.")
        print(f"[WHEELS_CALL]   Fechas procesadas  : {success:>6}")
        print(f"[WHEELS_CALL]   Errores            : {errors:>6}")
        print(f"[WHEELS_CALL]   Total filas salida : {total_rows:>12,}")
        print(f"[WHEELS_CALL]   Directorio salida  : {self.output_dir}")

    def build_test(self, year: int = 2020) -> None:
        """
        Modo prueba: procesa UNA fecha disponible del año indicado.
        Muestra el resultado en consola para validación rápida.

        Args:
            year: Año del que tomar la fecha de prueba (default 2020).
        """
        print(f"\n[WHEELS_CALL TEST] Modo Prueba – año {year}")

        # Cargar universo
        universe = self._load_universe()

        # Buscar la primera fecha del año con rueda disponible
        all_dates = sorted(universe.keys())
        ruedas_available = set(
            os.path.basename(f).replace("rueda_", "").replace(".parquet", "")
            for f in glob.glob(os.path.join(self.ruedas_dir, "rueda_*.parquet"))
        )

        test_date = next(
            (d for d in all_dates
             if d.startswith(str(year)) and d in ruedas_available),
            None
        )

        if not test_date:
            print(f"[WARN] No hay fechas de {year} con rueda disponible.")
            return

        print(f"[WHEELS_CALL TEST] Fecha de prueba: {test_date}")
        print(f"[WHEELS_CALL TEST] Tickers del universo: "
              f"{len(universe[test_date])} tickers")
        print(f"[WHEELS_CALL TEST] Muestra: {universe[test_date][:10]}")

        # Ejecutar el worker de forma SINCRÓNICA (sin multiproceso) para prueba
        args = (
            test_date,
            universe[test_date],
            self.ruedas_dir,
            self.output_dir,
            self.MAX_ASK_BID_RATIO,
        )
        date_str, rows_out, rows_total, err = self._process_single_date(args)

        if err and rows_out == -1:
            print(f"[ERROR] {err}")
            return

        # Cargar resultado para mostrarlo en consola
        out_path = os.path.join(self.output_dir, f"call_{date_str}.parquet")
        if os.path.exists(out_path):
            import polars as pl
            df = pl.read_parquet(out_path)
            print(f"\n[WHEELS_CALL TEST] ✓ Resultado para {date_str}:")
            print(f"   Filas guardadas : {len(df):,}")
            print(f"   Tickers únicos  : {df['ticker'].n_unique()}")
            print(f"\n{'─'*80}")
            print(df.head(20).to_string())
            print(f"{'─'*80}")
            print(f"\n   Estadísticas de callMidPrice:")
            desc = df.select("callMidPrice").describe()
            print(desc)

    # ------------------------------------------------------------------
    # Enriquecimiento con Earnings (prev/next earning date)
    # ------------------------------------------------------------------

    def enrich_with_earnings(
        self,
        earnings_path: str,
        date_filter: Optional[str] = None,
        years: Optional[List[int]] = None,
        force: bool = False,
        live_mode: bool = False,
    ) -> None:
        """
        Enriquece los archivos {option_type}_*.parquet con las fechas de earnings
        anterior y siguiente para cada ticker/tradeDate.

        Agrega dos columnas a cada archivo de ruedas_call_earn/:
          - prev_earning_date : Último earning ANTES o EN la fecha de trade.
          - next_earning_date : Próximo earning DESPUÉS o EN la fecha de trade.

        Utiliza join_asof de Polars (coincidencia más cercana hacia atrás/
        adelante) para asignar eficientemente las fechas sin bucles Python.

        El proceso es INCREMENTAL: por defecto no reprocesa archivos que ya
        contengan la columna 'prev_earning_date', salvo que force=True.

        Args:
            earnings_path : Ruta al archivo parquet maestro de earnings
                            (universe_earnings.parquet o similar).
            date_filter   : Procesar sólo esta fecha exacta (YYYY-MM-DD).
            years         : Lista de años a procesar (e.g. [2020]).
                            Si ambos son None, procesa todos.
            force         : Si True, reprocesa incluso archivos ya enriquecidos.
        """
        print(f"\n[EARNINGS ENRICH] ── Enriquecimiento con Earnings ──")
        print(f"[EARNINGS ENRICH] Maestro: {earnings_path}")

        # ----------------------------------------------------------------
        # A: Cargar el maestro de earnings y preparar para join_asof
        # ----------------------------------------------------------------
        if not os.path.exists(earnings_path):
            print(f"[ERROR] No se encontró el maestro de earnings: {earnings_path}")
            return

        try:
            ear_df = pl.read_parquet(earnings_path)
        except Exception as exc:
            print(f"[ERROR] Leyendo earnings: {exc}")
            return

        # Asegurar que 'date' sea tipo Date para join_asof
        if "date" in ear_df.columns:
            try:
                ear_df = ear_df.with_columns(
                    pl.col("date").cast(pl.Date, strict=False)
                )
            except Exception:
                ear_df = ear_df.with_columns(
                    pl.col("date").str.to_date("%Y-%m-%d", strict=False)
                )

        # Filtrar nulos y ordenar por fecha (requisito de join_asof)
        ear_df = (
            ear_df
            .filter(pl.col("date").is_not_null() & pl.col("ticker").is_not_null())
            .select(["ticker", "date"])   # Solo las columnas necesarias
            .sort("date")                  # Orden ascendente (requerido por join_asof)
        )

        # Renombramos 'date' para evitar colisión con columnas de la rueda
        # en el join. Luego renombraremos de vuelta.
        ear_df = ear_df.rename({"date": "earning_date"})

        print(f"[EARNINGS ENRICH] Earnings cargados: {len(ear_df):,} registros")
        print(f"[EARNINGS ENRICH] Tickers únicos   : {ear_df['ticker'].n_unique()}")

        # ----------------------------------------------------------------
        # B: Listar archivos call_*.parquet a procesar
        # ----------------------------------------------------------------
        all_files = sorted(glob.glob(os.path.join(self.output_dir, "call_*.parquet")))

        # Aplicar filtros de selección
        if date_filter:
            # Fecha exacta
            all_files = [
                f for f in all_files
                if os.path.basename(f) == f"call_{date_filter}.parquet"
            ]
        elif years:
            # Filtrar por año(s)
            year_strs = [str(y) for y in years]
            all_files = [
                f for f in all_files
                if any(os.path.basename(f).startswith(f"call_{yr}") for yr in year_strs)
            ]

        if not all_files:
            print("[EARNINGS ENRICH] No hay archivos que procesar.")
            return

        # Filtro incremental: omitir archivos ya enriquecidos (tienen la columna)
        if not force:
            pending = []
            for f in all_files:
                try:
                    schema = pl.read_parquet_schema(f)
                    if "prev_earning_date" not in schema:
                        pending.append(f)
                except Exception:
                    pending.append(f)  # Si no se puede leer, reprocesar
            all_files = pending

        print(f"[EARNINGS ENRICH] Archivos a enriquecer: {len(all_files)}")
        if not all_files:
            print("[EARNINGS ENRICH] Todo al día.")
            return

        # ----------------------------------------------------------------
        # C: Procesar cada archivo con join_asof.
        #
        # join_asof de Polars busca, para cada fila de la izquierda (rueda),
        # la fila de la derecha (earnings) más cercana según la estrategia:
        #   - 'backward': el earning más reciente <= tradeDate (prev)
        #   - 'forward' : el earning más próximo >= tradeDate (next)
        #
        # Al usar by='ticker', la búsqueda se restringe al mismo ticker.
        # Esto es equivalente a un GROUP BY ticker + asof join.
        # ----------------------------------------------------------------
        processed = 0
        errors    = 0

        for filepath in all_files:
            date_str = (
                os.path.basename(filepath)
                .replace("call_", "")
                .replace(".parquet", "")
            )
            try:
                # 1. Leer el archivo de calls
                df_call = pl.read_parquet(filepath)

                # 2. Asegurar que tradeDate sea tipo Date para join_asof
                if "tradeDate" not in df_call.columns:
                    print(f"[WARN] {date_str}: sin columna tradeDate, saltando.")
                    errors += 1
                    continue

                if df_call.schema["tradeDate"] == pl.Utf8:
                    # Convertir string a Date
                    df_call = df_call.with_columns(
                        pl.col("tradeDate")
                          .str.to_date("%Y-%m-%d", strict=False)
                          .alias("trade_date_dt")
                    )
                elif df_call.schema["tradeDate"] == pl.Date:
                    df_call = df_call.with_columns(
                        pl.col("tradeDate").alias("trade_date_dt")
                    )
                else:
                    # Intentar cast genérico
                    df_call = df_call.with_columns(
                        pl.col("tradeDate").cast(pl.Date, strict=False).alias("trade_date_dt")
                    )

                # Ordenar por la columna de fecha del trade (requisito join_asof)
                df_call = df_call.sort("trade_date_dt")

                # 3. JOIN ASOF → prev_earning_date
                # strategy='backward': el earning más reciente en o antes del tradeDate
                df_enriched = df_call.join_asof(
                    ear_df,
                    left_on="trade_date_dt",
                    right_on="earning_date",
                    by="ticker",
                    strategy="backward",
                    suffix="_prev",
                ).rename({"earning_date": "prev_earning_date"})

                # 4. JOIN ASOF → next_earning_date
                # strategy='forward': el earning más próximo en o después del tradeDate
                df_enriched = df_enriched.join_asof(
                    ear_df,
                    left_on="trade_date_dt",
                    right_on="earning_date",
                    by="ticker",
                    strategy="forward",
                    suffix="_next",
                ).rename({"earning_date": "next_earning_date"})

                # 5. Eliminar la columna auxiliar de conversión de fecha
                df_enriched = df_enriched.drop("trade_date_dt")

                # 6. Escritura atómica (.tmp → rename)
                temp_path = filepath + ".earn_tmp"
                df_enriched.write_parquet(temp_path, compression="snappy")
                if os.path.exists(filepath):
                    os.remove(filepath)
                os.rename(temp_path, filepath)

                processed += 1
                if processed % 20 == 0 or processed == len(all_files):
                    pct = processed / len(all_files) * 100
                    print(
                        f"   ... {processed}/{len(all_files)} ({pct:.0f}%) "
                        f"| Último: {date_str}"
                    )

            except Exception as exc:
                import traceback
                print(f"[ERROR] {date_str}: {exc}")
                traceback.print_exc()
                errors += 1

        print(f"\n[EARNINGS ENRICH] ✓ Proceso completado.")
        print(f"[EARNINGS ENRICH]   Archivos procesados: {processed}")
        print(f"[EARNINGS ENRICH]   Errores            : {errors}")

    # ------------------------------------------------------------------
    def filter_min_strikes(
        self,
        min_strikes: int = 4,
        date_filter: Optional[str] = None,
        years: Optional[List[int]] = None,
        live_mode: bool = False,
    ) -> None:
        """
        [FILTRO DESACTIVADO POR USUARIO]
        Originalmente: Filtra los archivos parquet para garantizar que cada par
        (ticker, dte) tenga al menos `min_strikes` filas.
        """
        print(f"\n[MIN-STRIKES] ── Filtro mínimo {min_strikes} strikes por (ticker, dte) ──")
        print("[MIN-STRIKES] Filtro DESACTIVADO (Passthrough completo de todos los datos).")
        return



class WheelsPutEarnBuilder:
    """
    Filtra las cadenas de opciones (ruedas) para el universo de tickers
    seleccionado en universo.json y aplica criterios de calidad sobre
    opciones PUT, generando un dataset limpio por fecha en
    `ruedas_put_earn/`.

    Columnas de salida
    ------------------
    ticker, tradeDate, expirDate, dte, strike, stockPrice,
    putVolume, putOpenInterest, putMidPrice

    Donde putMidPrice = (putBidPrice + putAskPrice) / 2.

    Filtros de calidad (aplicados en orden vectorizado)
    ---------------------------------------------------
    a. strike <= stockPrice                  → sólo calls ATM u OTM
    b. putOpenInterest > 0 AND putVolume > 0  → liquidez mínima
    c. putBidPrice > 0 AND putAskPrice > 0    → precios válidos
    d. putAskPrice / putBidPrice <= 1.40      → spread razonable
    """

    # Columnas que deben preservarse en el output
    OUTPUT_COLS = [
        "ticker", "tradeDate", "expirDate", "dte", "strike",
        "stockPrice", "putVolume", "putOpenInterest", "putMidPrice"
    ]

    # Máximo ratio Ask/Bid permitido (filtro de spread)
    MAX_ASK_BID_RATIO = 1.40

    def __init__(
        self,
        universo_path: str,
        ruedas_dir: str,
        output_dir: str,
        max_workers: int = 4,
    ):
        """
        Inicializa el builder.

        Args:
            universo_path : Ruta a universo.json  { fecha: [tickers] }.
            ruedas_dir    : Carpeta con archivos rueda_YYYY-MM-DD.parquet.
            output_dir    : Carpeta de salida ruedas_put_earn/.
            max_workers   : Procesos paralelos (ProcessPoolExecutor).
                            Usar 4 como default conservador para I/O intensivo.
        """
        self.universo_path = universo_path
        self.ruedas_dir    = ruedas_dir
        self.output_dir    = output_dir
        self.max_workers   = max_workers

        # Crear carpeta de salida si no existe
        os.makedirs(self.output_dir, exist_ok=True)

    # ------------------------------------------------------------------
    # Métodos de soporte
    # ------------------------------------------------------------------

    def _load_universe(self) -> dict:
        """
        Carga universo.json del disco.
        Si es una lista, la expande a todas las fechas de ruedas conocidas.
        """
        with open(self.universo_path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        if isinstance(data, list):
            wheel_files = glob.glob(os.path.join(self.ruedas_dir, "rueda_*.parquet"))
            return {
                os.path.basename(f).replace("rueda_", "").replace(".parquet", ""): data
                for f in wheel_files
            }
        return data

    def _get_processed_dates(self) -> set:
        """
        Devuelve el conjunto de fechas que ya fueron procesadas
        (existen como archivo en output_dir).

        Returns:
            set: Fechas ya procesadas como strings YYYY-MM-DD.
        """
        files = glob.glob(os.path.join(self.output_dir, "put_*.parquet"))
        processed = set()
        for f in files:
            base = os.path.basename(f).replace("put_", "").replace(".parquet", "")
            try:
                datetime.datetime.strptime(base, "%Y-%m-%d")
                processed.add(base)
            except ValueError:
                continue
        return processed

    @staticmethod
    def _process_single_date(args: tuple) -> tuple:
        """
        Worker estático que procesa UNA fecha: filtra la rueda por universo
        y aplica los criterios de calidad sobre opciones PUT.

        Diseñado para ejecutarse en un proceso separado (ProcessPoolExecutor).
        Al ser estático y recibir todo por argumento, es picklable por defecto.

        Args:
            args: Tupla (date_str, tickers, ruedas_dir, output_dir, max_ratio)

        Returns:
            Tupla (date_str, n_rows_salvadas, n_rows_totales, mensaje_error)
            Si hay error, n_rows = -1 y mensaje_error describe el fallo.
        """
        import polars as pl  # Import dentro del worker (proceso separado)
        import os

        date_str, tickers, ruedas_dir, output_dir, max_ratio = args

        # ----------------------------------------------------------------
        # Verificar existencia del archivo de rueda para esta fecha
        # ----------------------------------------------------------------
        if date_str == "live":
             rueda_path = os.path.join(ruedas_dir, "live_top200_wheels.parquet")
             out_path   = os.path.join(output_dir, "put_live_top200_wheels.parquet")
        else:
             rueda_path = os.path.join(ruedas_dir, f"rueda_{date_str}.parquet")
             out_path   = os.path.join(output_dir, f"put_{date_str}.parquet")

        if not os.path.exists(rueda_path):
            return (date_str, 0, 0, f"Archivo no encontrado: {rueda_path}")

        try:
            # ------------------------------------------------------------
            # PASO 1: Leer rueda y filtrar por universo de tickers.
            # Usamos scan_parquet + filter para leer sólo las filas del
            # universo (Polars aplica predicate pushdown sobre el parquet).
            # ------------------------------------------------------------
            # Convertir lista a set de Polars para filtro eficiente
            # SI tickers es None, procesamos todo el archivo (Modo Live completo)
            if tickers is not None:
                ticker_set = pl.Series("ticker", tickers, dtype=pl.Utf8)
                lf = (
                    pl.scan_parquet(rueda_path)
                    # Filtrar solo tickers del universo (inner join implícito)
                    .filter(pl.col("ticker").is_in(ticker_set))
                )
            else:
                lf = pl.scan_parquet(rueda_path)

            # ------------------------------------------------------------
            # Verificar que las columnas requeridas existen en el schema
            # Para manejar posibles variaciones del archivo histórico
            # ------------------------------------------------------------
            schema_names = lf.collect_schema().names()
            required_cols = [
                "ticker", "tradeDate", "expirDate", "dte",
                "strike", "stockPrice",
                "putVolume", "putOpenInterest",
                "putBidPrice", "putAskPrice",
            ]
            missing = [c for c in required_cols if c not in schema_names]
            if missing:
                return (date_str, -1, -1,
                        f"Columnas faltantes en {date_str}: {missing}")

            # ------------------------------------------------------------
            # PASO 2: Aplicar filtros de calidad en pipeline vectorizado.
            # Todos los predicados se evalúan en paralelo por Polars
            # antes de materializar (lazy → collect).
            # ------------------------------------------------------------
            filtered = (
                lf
                # --- Filtro a: eliminar strikes < stockPrice ---
                # Solo queremos calls ATM o Out-of-The-Money (strike >= spot)
                # ya que son las candidatas para wheel strategy (covered calls)
                .filter(pl.col("strike") <= pl.col("stockPrice"))

                # --- Filtro b: liquidez mínima ---
                # Elimina opciones sin interés abierto o sin volumen del día.
                # Un putOpenInterest=0 indica que nadie mantiene esa posición.
                # Un putVolume=0 indica que no hubo transacciones ese día.
                .filter(
                    (pl.col("putOpenInterest").cast(pl.Float64, strict=False)
                                               .fill_null(0) > 0)
                    &
                    (pl.col("putVolume").cast(pl.Float64, strict=False)
                                        .fill_null(0) > 0)
                )

                # --- Filtro c: precios válidos ---
                # Un precio bid o ask de cero significa que no hay mercado
                # activo para esa opción (o error de datos).
                .filter(
                    (pl.col("putBidPrice").cast(pl.Float64, strict=False)
                                           .fill_null(0) > 0)
                    &
                    (pl.col("putAskPrice").cast(pl.Float64, strict=False)
                                           .fill_null(0) > 0)
                )

                # --- Filtro d: spread razonable (Ask/Bid <= 1.40) ---
                # Un ratio Ask/Bid > 1.40 indica un spread excesivo (>40%)
                # que hace prohibitivo operar esa opción de forma eficiente.
                .filter(
                    (
                        pl.col("putAskPrice").cast(pl.Float64, strict=False)
                        /
                        pl.col("putBidPrice").cast(pl.Float64, strict=False)
                    ) <= max_ratio
                )

                # --- Generar columna putMidPrice ---
                # El precio mid es el punto de referencia estándar para
                # valorar una opción: promedio entre bid y ask.
                .with_columns(
                    (
                        (pl.col("putBidPrice").cast(pl.Float64, strict=False)
                         + pl.col("putAskPrice").cast(pl.Float64, strict=False))
                        / 2.0
                    ).alias("putMidPrice")
                )

                # --- Seleccionar solo columnas de salida ---
                .select([
                    "ticker", "tradeDate", "expirDate", "dte",
                    "strike", "stockPrice",
                    "putVolume", "putOpenInterest", "putMidPrice"
                ])

                # --- Materializar el lazy frame ---
                .collect()
            )

            rows_out = len(filtered)

            if rows_out == 0:
                # Si no quedan filas después de filtros, registramos la fecha
                # con un parquet vacío para marcarla como procesada.
                # Esto evita re-procesar en ejecuciones futuras.
                pass

            # ------------------------------------------------------------
            # PASO 3: Guardar resultado en output_dir con escritura atómica.
            # Escribimos a un .tmp primero; si el proceso se interrumpe,
            # el archivo original (si existía) no queda corrupto.
            # ------------------------------------------------------------
            temp_path = out_path + ".tmp"

            filtered.write_parquet(temp_path, compression="snappy")

            # Reemplazo atómico
            if os.path.exists(out_path):
                os.remove(out_path)
            os.rename(temp_path, out_path)

            return (date_str, rows_out, rows_out, None)

        except Exception as exc:
            import traceback
            return (date_str, -1, -1, traceback.format_exc())

    def build_live(self) -> None:
        print(f"\n[WHEELS_PUT] ── Generación Ruedas Live (Calls) ──")
        self._process_single_date(("live", None, self.ruedas_dir, self.output_dir, self.MAX_ASK_BID_RATIO))
        print(f"[WHEELS_PUT] ✓ Proceso live completado.")

    # ------------------------------------------------------------------
    # Método principal de construcción
    # ------------------------------------------------------------------

    def build(
        self,
        date_filter: Optional[str] = None,
        years: Optional[List[int]] = None,
    ) -> None:
        """
        Ejecuta el pipeline de generación de ruedas_put_earn.

        Soporta dos modos:
        - Completo    : procesa todas las fechas comunes universo/ruedas.
        - Filtrado    : solo un año específico (`years=[2020]`) o una fecha
                        exacta (`date_filter="2020-03-15"`).

        El proceso es INCREMENTAL: salta fechas ya procesadas.

        Args:
            date_filter : Procesar sólo esta fecha exacta (YYYY-MM-DD).
            years       : Lista de años a procesar (e.g. [2020, 2021]).
                          Si ambos son None, procesa todo.
        """
        print(f"\n[WHEELS_PUT] ── Generación de Ruedas Filtradas (Calls) ──")
        print(f"[WHEELS_PUT] Salida: {self.output_dir}")

        # ----------------------------------------------------------------
        # A: Cargar universo y detectar fechas ya procesadas
        # ----------------------------------------------------------------
        print("[WHEELS_PUT] Cargando universo.json...")
        universe = self._load_universe()
        processed_dates = self._get_processed_dates()

        print(f"[WHEELS_PUT] Fechas en universo   : {len(universe):>6}")
        print(f"[WHEELS_PUT] Ya procesadas         : {len(processed_dates):>6}")

        # ----------------------------------------------------------------
        # B: Determinar fechas a procesar según filtros del usuario
        # ----------------------------------------------------------------
        all_dates = sorted(universe.keys())

        # Aplicar filtros de selección
        if date_filter:
            # Modo fecha única (útil para pruebas o reprocesar una fecha)
            target_dates = [date_filter] if date_filter in universe else []
            if not target_dates:
                print(f"[WARN] Fecha {date_filter} no existe en universo.json")
                return
        elif years:
            # Filtrar por año(s) específico(s)
            year_strs = [str(y) for y in years]
            target_dates = [d for d in all_dates if d[:4] in year_strs]
        else:
            # Todas las fechas
            target_dates = all_dates

        # Intersección con archivos de ruedas disponibles en disco
        ruedas_available = set(
            os.path.basename(f).replace("rueda_", "").replace(".parquet", "")
            for f in glob.glob(os.path.join(self.ruedas_dir, "rueda_*.parquet"))
        )
        target_dates = [d for d in target_dates if d in ruedas_available]

        # Quitar fechas ya procesadas (incremental)
        new_dates = [d for d in target_dates if d not in processed_dates]

        if not new_dates:
            print("[WHEELS_PUT] Todo al día. No hay fechas nuevas.")
            return

        print(f"[WHEELS_PUT] Fechas objetivo       : {len(target_dates):>6}")
        print(f"[WHEELS_PUT] A procesar (nuevas)   : {len(new_dates):>6}")

        # ----------------------------------------------------------------
        # C: Construir argumentos para el worker (una tupla por fecha)
        # ----------------------------------------------------------------
        worker_args = [
            (
                date_str,           # fecha
                universe[date_str], # lista de tickers del universo ese día
                self.ruedas_dir,    # directorio de ruedas
                self.output_dir,    # directorio de salida
                self.MAX_ASK_BID_RATIO,  # ratio máximo Ask/Bid
            )
            for date_str in new_dates
        ]

        # ----------------------------------------------------------------
        # D: Ejecutar en paralelo con ProcessPoolExecutor.
        # Se usa ProcessPoolExecutor (no Thread) porque el procesamiento
        # de Polars es CPU-bound una vez cargados los datos.
        # Cada proceso tiene su propia copia del GIL → true parallelism.
        # ----------------------------------------------------------------
        print(f"\n[WHEELS_PUT] Procesando {len(new_dates)} fechas "
              f"({self.max_workers} procesos en paralelo)...")

        total_rows  = 0
        errors      = 0
        completed   = 0

        with concurrent.futures.ProcessPoolExecutor(
            max_workers=self.max_workers
        ) as executor:
            # Mapear cada tarea al argumento correspondiente
            future_to_date = {
                executor.submit(self._process_single_date, args): args[0]
                for args in worker_args
            }

            for future in concurrent.futures.as_completed(future_to_date):
                date_key = future_to_date[future]
                try:
                    date_str, rows_out, rows_total, err = future.result()
                    if err and rows_out == -1:
                        # Error en el worker
                        print(f"[ERROR] {date_str}: {err[:120]}...")
                        errors += 1
                    else:
                        total_rows += rows_out
                except Exception as exc:
                    print(f"[ERROR] Future {date_key}: {exc}")
                    errors += 1

                completed += 1
                if completed % 20 == 0 or completed == len(new_dates):
                    pct = completed / len(new_dates) * 100
                    print(f"   ... {completed}/{len(new_dates)} ({pct:.0f}%) "
                          f"| Filas guardadas: {total_rows:,} | Errores: {errors}")

        # ----------------------------------------------------------------
        # E: Reporte final
        # ----------------------------------------------------------------
        success = completed - errors
        print(f"\n[WHEELS_PUT] ✓ Proceso completado.")
        print(f"[WHEELS_PUT]   Fechas procesadas  : {success:>6}")
        print(f"[WHEELS_PUT]   Errores            : {errors:>6}")
        print(f"[WHEELS_PUT]   Total filas salida : {total_rows:>12,}")
        print(f"[WHEELS_PUT]   Directorio salida  : {self.output_dir}")

    def build_test(self, year: int = 2020) -> None:
        """
        Modo prueba: procesa UNA fecha disponible del año indicado.
        Muestra el resultado en consola para validación rápida.

        Args:
            year: Año del que tomar la fecha de prueba (default 2020).
        """
        print(f"\n[WHEELS_PUT TEST] Modo Prueba – año {year}")

        # Cargar universo
        universe = self._load_universe()

        # Buscar la primera fecha del año con rueda disponible
        all_dates = sorted(universe.keys())
        ruedas_available = set(
            os.path.basename(f).replace("rueda_", "").replace(".parquet", "")
            for f in glob.glob(os.path.join(self.ruedas_dir, "rueda_*.parquet"))
        )

        test_date = next(
            (d for d in all_dates
             if d.startswith(str(year)) and d in ruedas_available),
            None
        )

        if not test_date:
            print(f"[WARN] No hay fechas de {year} con rueda disponible.")
            return

        print(f"[WHEELS_PUT TEST] Fecha de prueba: {test_date}")
        print(f"[WHEELS_PUT TEST] Tickers del universo: "
              f"{len(universe[test_date])} tickers")
        print(f"[WHEELS_PUT TEST] Muestra: {universe[test_date][:10]}")

        # Ejecutar el worker de forma SINCRÓNICA (sin multiproceso) para prueba
        args = (
            test_date,
            universe[test_date],
            self.ruedas_dir,
            self.output_dir,
            self.MAX_ASK_BID_RATIO,
        )
        date_str, rows_out, rows_total, err = self._process_single_date(args)

        if err and rows_out == -1:
            print(f"[ERROR] {err}")
            return

        # Cargar resultado para mostrarlo en consola
        out_path = os.path.join(self.output_dir, f"put_{date_str}.parquet")
        if os.path.exists(out_path):
            import polars as pl
            df = pl.read_parquet(out_path)
            print(f"\n[WHEELS_PUT TEST] ✓ Resultado para {date_str}:")
            print(f"   Filas guardadas : {len(df):,}")
            print(f"   Tickers únicos  : {df['ticker'].n_unique()}")
            print(f"\n{'─'*80}")
            print(df.head(20).to_string())
            print(f"{'─'*80}")
            print(f"\n   Estadísticas de putMidPrice:")
            desc = df.select("putMidPrice").describe()
            print(desc)

    # ------------------------------------------------------------------
    # Enriquecimiento con Earnings (prev/next earning date)
    # ------------------------------------------------------------------

    def enrich_with_earnings(
        self,
        earnings_path: str,
        date_filter: Optional[str] = None,
        years: Optional[List[int]] = None,
        force: bool = False,
        live_mode: bool = False,
    ) -> None:
        """
        Enriquece los archivos put_*.parquet con las fechas de earnings
        anterior y siguiente para cada ticker/tradeDate.

        Agrega dos columnas a cada archivo de ruedas_put_earn/:
          - prev_earning_date : Último earning ANTES o EN la fecha de trade.
          - next_earning_date : Próximo earning DESPUÉS o EN la fecha de trade.

        Utiliza join_asof de Polars (coincidencia más cercana hacia atrás/
        adelante) para asignar eficientemente las fechas sin bucles Python.

        El proceso es INCREMENTAL: por defecto no reprocesa archivos que ya
        contengan la columna 'prev_earning_date', salvo que force=True.

        Args:
            earnings_path : Ruta al archivo parquet maestro de earnings
                            (universe_earnings.parquet o similar).
            date_filter   : Procesar sólo esta fecha exacta (YYYY-MM-DD).
            years         : Lista de años a procesar (e.g. [2020]).
                            Si ambos son None, procesa todos.
            force         : Si True, reprocesa incluso archivos ya enriquecidos.
        """
        print(f"\n[EARNINGS ENRICH] ── Enriquecimiento con Earnings ──")
        print(f"[EARNINGS ENRICH] Maestro: {earnings_path}")

        # ----------------------------------------------------------------
        # A: Cargar el maestro de earnings y preparar para join_asof
        # ----------------------------------------------------------------
        if not os.path.exists(earnings_path):
            print(f"[ERROR] No se encontró el maestro de earnings: {earnings_path}")
            return

        try:
            ear_df = pl.read_parquet(earnings_path)
        except Exception as exc:
            print(f"[ERROR] Leyendo earnings: {exc}")
            return

        # Asegurar que 'date' sea tipo Date para join_asof
        if "date" in ear_df.columns:
            try:
                ear_df = ear_df.with_columns(
                    pl.col("date").cast(pl.Date, strict=False)
                )
            except Exception:
                ear_df = ear_df.with_columns(
                    pl.col("date").str.to_date("%Y-%m-%d", strict=False)
                )

        # Filtrar nulos y ordenar por fecha (requisito de join_asof)
        ear_df = (
            ear_df
            .filter(pl.col("date").is_not_null() & pl.col("ticker").is_not_null())
            .select(["ticker", "date"])   # Solo las columnas necesarias
            .sort("date")                  # Orden ascendente (requerido por join_asof)
        )

        # Renombramos 'date' para evitar colisión con columnas de la rueda
        # en el join. Luego renombraremos de vuelta.
        ear_df = ear_df.rename({"date": "earning_date"})

        print(f"[EARNINGS ENRICH] Earnings cargados: {len(ear_df):,} registros")
        print(f"[EARNINGS ENRICH] Tickers únicos   : {ear_df['ticker'].n_unique()}")

        # ----------------------------------------------------------------
        # B: Listar archivos put_*.parquet a procesar
        # ----------------------------------------------------------------
        all_files = sorted(glob.glob(os.path.join(self.output_dir, "put_*.parquet")))

        # Aplicar filtros de selección
        if date_filter:
            # Fecha exacta
            all_files = [
                f for f in all_files
                if os.path.basename(f) == f"put_{date_filter}.parquet"
            ]
        elif years:
            # Filtrar por año(s)
            year_strs = [str(y) for y in years]
            all_files = [
                f for f in all_files
                if any(os.path.basename(f).startswith(f"put_{yr}") for yr in year_strs)
            ]

        if not all_files:
            print("[EARNINGS ENRICH] No hay archivos que procesar.")
            return

        # Filtro incremental: omitir archivos ya enriquecidos (tienen la columna)
        if not force:
            pending = []
            for f in all_files:
                try:
                    schema = pl.read_parquet_schema(f)
                    if "prev_earning_date" not in schema:
                        pending.append(f)
                except Exception:
                    pending.append(f)  # Si no se puede leer, reprocesar
            all_files = pending

        print(f"[EARNINGS ENRICH] Archivos a enriquecer: {len(all_files)}")
        if not all_files:
            print("[EARNINGS ENRICH] Todo al día.")
            return

        # ----------------------------------------------------------------
        # C: Procesar cada archivo con join_asof.
        #
        # join_asof de Polars busca, para cada fila de la izquierda (rueda),
        # la fila de la derecha (earnings) más cercana según la estrategia:
        #   - 'backward': el earning más reciente <= tradeDate (prev)
        #   - 'forward' : el earning más próximo >= tradeDate (next)
        #
        # Al usar by='ticker', la búsqueda se restringe al mismo ticker.
        # Esto es equivalente a un GROUP BY ticker + asof join.
        # ----------------------------------------------------------------
        processed = 0
        errors    = 0

        for filepath in all_files:
            date_str = (
                os.path.basename(filepath)
                .replace("put_", "")
                .replace(".parquet", "")
            )
            try:
                # 1. Leer el archivo de calls
                df_call = pl.read_parquet(filepath)

                # 2. Asegurar que tradeDate sea tipo Date para join_asof
                if "tradeDate" not in df_call.columns:
                    print(f"[WARN] {date_str}: sin columna tradeDate, saltando.")
                    errors += 1
                    continue

                if df_call.schema["tradeDate"] == pl.Utf8:
                    # Convertir string a Date
                    df_call = df_call.with_columns(
                        pl.col("tradeDate")
                          .str.to_date("%Y-%m-%d", strict=False)
                          .alias("trade_date_dt")
                    )
                elif df_call.schema["tradeDate"] == pl.Date:
                    df_call = df_call.with_columns(
                        pl.col("tradeDate").alias("trade_date_dt")
                    )
                else:
                    # Intentar cast genérico
                    df_call = df_call.with_columns(
                        pl.col("tradeDate").cast(pl.Date, strict=False).alias("trade_date_dt")
                    )

                # Ordenar por la columna de fecha del trade (requisito join_asof)
                df_call = df_call.sort("trade_date_dt")

                # 3. JOIN ASOF → prev_earning_date
                # strategy='backward': el earning más reciente en o antes del tradeDate
                df_enriched = df_call.join_asof(
                    ear_df,
                    left_on="trade_date_dt",
                    right_on="earning_date",
                    by="ticker",
                    strategy="backward",
                    suffix="_prev",
                ).rename({"earning_date": "prev_earning_date"})

                # 4. JOIN ASOF → next_earning_date
                # strategy='forward': el earning más próximo en o después del tradeDate
                df_enriched = df_enriched.join_asof(
                    ear_df,
                    left_on="trade_date_dt",
                    right_on="earning_date",
                    by="ticker",
                    strategy="forward",
                    suffix="_next",
                ).rename({"earning_date": "next_earning_date"})

                # 5. Eliminar la columna auxiliar de conversión de fecha
                df_enriched = df_enriched.drop("trade_date_dt")

                # 6. Escritura atómica (.tmp → rename)
                temp_path = filepath + ".earn_tmp"
                df_enriched.write_parquet(temp_path, compression="snappy")
                if os.path.exists(filepath):
                    os.remove(filepath)
                os.rename(temp_path, filepath)

                processed += 1
                if processed % 20 == 0 or processed == len(all_files):
                    pct = processed / len(all_files) * 100
                    print(
                        f"   ... {processed}/{len(all_files)} ({pct:.0f}%) "
                        f"| Último: {date_str}"
                    )

            except Exception as exc:
                import traceback
                print(f"[ERROR] {date_str}: {exc}")
                traceback.print_exc()
                errors += 1

        print(f"\n[EARNINGS ENRICH] ✓ Proceso completado.")
        print(f"[EARNINGS ENRICH]   Archivos procesados: {processed}")
        print(f"[EARNINGS ENRICH]   Errores            : {errors}")

    # ------------------------------------------------------------------
    def filter_min_strikes(
        self,
        min_strikes: int = 4,
        date_filter: Optional[str] = None,
        years: Optional[List[int]] = None,
        live_mode: bool = False,
    ) -> None:
        """
        [FILTRO DESACTIVADO POR USUARIO]
        Originalmente: Filtra los archivos parquet para garantizar que cada par
        (ticker, dte) tenga al menos `min_strikes` filas.
        """
        print(f"\n[MIN-STRIKES] ── Filtro mínimo {min_strikes} strikes por (ticker, dte) ──")
        print("[MIN-STRIKES] Filtro DESACTIVADO (Passthrough completo de todos los datos).")
        return


# ==============================================================================
# FMP DATA ENRICHER
# Enriquece los archivos {option_type}_*.parquet con dos columnas cuantitativas
# fundamentales para la valoración de opciones bajo BSM:
#
#   div_yield_cont  : Tasa de dividendos continua  q = ln(1 + ΣDivTTM/S₀)
#   risk_free_rate  : Tasa libre de riesgo continua r = ln(1 + APY)
#                    interpolada exactamente en el DTE de cada opción
#                    usando un Clamped Cubic Spline (Actual/365).
#
# Fuente de datos   : Financial Modeling Prep (FMP) API
#   - Dividendos    : /v3/historical-price-full/stock_dividend/{ticker}
#   - Tasas tesoro  : /v4/treasury
#
# Conversión de tasas (BEY → continua):
#   bey (decimal) = bey_pct / 100
#   APY  = (1 + bey/2)^2 - 1          ← USA bonds son semi-anuales
#   r_c  = ln(1 + APY)                 ← capitalización continua
#
# Interpolación: CubicSpline(bc_type='clamped') de scipy
#   - Clamped = primera derivada = 0 en los extremos del grid
#   - Evita el overshoot de Runge en los extremos corto y largo de la curva
#   - Convención Actual/365 para todos los plazos
#
# Caché:
#   enrichment_cache/dividends/{TICKER}.parquet  → TTL 7 días
#   enrichment_cache/treasury/{YYYY-MM-DD}.json  → permanente (fecha pasada)
#
# Incremental: omite archivos que ya contienen 'risk_free_rate'.
# ==============================================================================

class FMPDataEnricher:
    """
    Descarga dividendos y tasas del tesoro de FMP y los agrega como columnas
    a los archivos call_*.parquet en ruedas_call_earn/.

    Columnas agregadas
    ------------------
    div_yield_cont : float
        Tasa de dividendos anual, capitalización continua.
        Si el ticker no paga dividendos → 0.0.
        q = ln(1 + ΣadjDividend(TTM) / stockPrice)

    risk_free_rate : float
        Tasa del tesoro interpolada por DTE (Actual/365), continua.
        r_c = ln(1 + APY) donde APY = (1 + BEY/2)^2 - 1.
        Interpolación mediante Clamped Cubic Spline.
    """

    # Endpoint base FMP — API estable (non-legacy)
    FMP_BASE = "https://financialmodelingprep.com/stable"

    # Vencimientos del tesoro USA y su equivalente en días (Actual/365)
    # order: de corto a largo (requerido por CubicSpline)
    TREASURY_GRID = [
        ("month1",  30),
        ("month2",  61),
        ("month3",  91),
        ("month6",  182),
        ("year1",   365),
        ("year2",   730),
        ("year3",   1095),
        ("year5",   1825),
        ("year7",   2555),
        ("year10",  3650),
        ("year20",  7300),
        ("year30",  10950),
    ]

    def __init__(
        self,
        fmp_api_key: str,
        input_dir: str,
        cache_dir: str,
        option_type: str = "call",
        max_workers: int = 8,
    ):
        """
        Args:
            fmp_api_key   : API Key de Financial Modeling Prep.
            call_earn_dir : Carpeta con los archivos call_*.parquet a enriquecer.
            cache_dir     : Carpeta raíz para caché de dividendos y tasas.
            max_workers   : Hilos para las descargas de dividendos en paralelo.
        """
        self.api_key       = fmp_api_key
        self.input_dir = input_dir
        self.option_type = option_type.lower()
        self.cache_dir     = cache_dir
        self.max_workers   = max_workers

        # Sub-carpetas de caché
        self.div_cache_dir  = os.path.join(cache_dir, "dividends")
        self.tsy_cache_dir  = os.path.join(cache_dir, "treasury")
        os.makedirs(self.div_cache_dir,  exist_ok=True)
        os.makedirs(self.tsy_cache_dir,  exist_ok=True)

    # ------------------------------------------------------------------
    # Utilidades de conversión de tasas
    # ------------------------------------------------------------------

    @staticmethod
    def _bey_to_continuous(bey_pct: float) -> float:
        """
        Convierte una tasa BEY expresada en porcentaje (p.ej. 5.25 = 5.25%)
        a tasa de capitalización continua.

        Pasos matemáticos:
            1. decimal  = bey_pct / 100
            2. APY      = (1 + decimal/2)^2 - 1   # compounding semi-anual USA
            3. r_cont   = ln(1 + APY)              # capitalización continua

        Args:
            bey_pct: Tasa en formato Bond Equivalent Yield, en porcentaje.

        Returns:
            Tasa continua como número decimal (ej. 0.0518 para ~5.18%).
        """
        if bey_pct is None or bey_pct <= 0:
            return 0.0
        decimal = bey_pct / 100.0
        apy     = (1.0 + decimal / 2.0) ** 2 - 1.0   # semi-anual → anual
        r_cont  = math.log(1.0 + apy)                 # continua
        return r_cont

    # ------------------------------------------------------------------
    # Descarga y caché de Tasas del Tesoro
    # ------------------------------------------------------------------

    def _fetch_treasury_rates(self, date_str: str) -> Optional[dict]:
        """
        Descarga las tasas del tesoro USA de FMP para una fecha específica.
        Las guarda en caché como JSON (permanente para fechas pasadas).

        Endpoint: GET /v4/treasury?from=DATE&to=DATE&apikey=KEY

        Args:
            date_str: Fecha en formato YYYY-MM-DD.

        Returns:
            dict: { "month1": 5.12, "month3": 5.28, ..., "year30": 4.55 }
                  Valores en porcentaje (BEY%). None si no hay datos.
        """
        # 1. Revisar caché
        cache_path = os.path.join(self.tsy_cache_dir, f"{date_str}.json")
        if os.path.exists(cache_path):
            with open(cache_path, "r", encoding="utf-8") as fh:
                return json.load(fh)

        # 2. Descargar de FMP
        url = f"{self.FMP_BASE}/treasury-rates"
        params = {
            "from":   date_str,
            "to":     date_str,
            "apikey": self.api_key,
        }
        try:
            resp = requests.get(url, params=params, timeout=15)
            if resp.status_code != 200:
                print(f"[WARN] Treasury API HTTP {resp.status_code} para {date_str}")
                return None

            data = resp.json()
            if not data or not isinstance(data, list):
                # FMP puede retornar [] para fines de semana / festivos
                # Buscar la fecha anterior más cercana (hasta 5 días anteriores)
                trade_dt = datetime.datetime.strptime(date_str, "%Y-%m-%d").date()
                for lag in range(1, 6):
                    prev_date = (trade_dt - datetime.timedelta(days=lag)).strftime("%Y-%m-%d")
                    prev_cache = os.path.join(self.tsy_cache_dir, f"{prev_date}.json")
                    if os.path.exists(prev_cache):
                        with open(prev_cache, "r") as fh:
                            return json.load(fh)
                print(f"[WARN] Sin datos del tesoro para {date_str} ni días previos")
                return None

            # Buscar el registro más cercano a la fecha solicitada
            # (puede haber varios si el rango incluye varios días)
            rates = data[0]

            # Guardar en caché
            with open(cache_path, "w", encoding="utf-8") as fh:
                json.dump(rates, fh)

            return rates

        except Exception as exc:
            print(f"[ERROR] Descargando treasury {date_str}: {exc}")
            return None

    def _build_rate_spline(self, date_str: str):
        """
        Construye un Clamped Cubic Spline de la curva de tasas del tesoro.

        La estrategia 'clamped' fija la primera derivada = 0 en ambos extremos,
        lo que evita el oscilamiento polinómico (fenómeno de Runge) que aparece
        con splines naturales en los extremos de tramo corto y largo.

        Convención de días: Actual/365.

        Args:
            date_str: Fecha de la rueda (YYYY-MM-DD).

        Returns:
            CubicSpline callable si los datos están disponibles, None si error.
        """
        from scipy.interpolate import CubicSpline

        rates_raw = self._fetch_treasury_rates(date_str)
        if not rates_raw:
            return None

        days_list  = []
        rates_cont = []

        for key, days in self.TREASURY_GRID:
            bey_pct = rates_raw.get(key)
            if bey_pct is None or bey_pct <= 0:
                continue  # Si falta un punto, lo omitimos del grid
            rc = self._bey_to_continuous(float(bey_pct))
            days_list.append(days)
            rates_cont.append(rc)

        if len(days_list) < 2:
            print(f"[WARN] Insuficientes puntos de curva para {date_str}: {len(days_list)}")
            return None

        # Clamped: dy/dx = 0 en ambos extremos
        spline = CubicSpline(
            days_list,
            rates_cont,
            bc_type="clamped",   # primera derivada = 0 en bordes
        )
        return spline

    # ------------------------------------------------------------------
    # Descarga y caché de Dividendos
    # ------------------------------------------------------------------

    def _fetch_dividends(self, ticker: str) -> Optional[pl.DataFrame]:
        """
        Descarga el histórico de dividendos ajustados por splits desde FMP.
        Usa el campo 'adjDividend' que ya está corregido por splits históricos.

        Los resultados se guardan en caché por ticker (TTL = 7 días).

        Endpoint: GET /v3/historical-price-full/stock_dividend/{ticker}

        Args:
            ticker: Símbolo bursátil (e.g. 'AAPL').

        Returns:
            pl.DataFrame con columnas ['date', 'adjDividend'] o None si error.
        """
        import polars as pl

        cache_path = os.path.join(self.div_cache_dir, f"{ticker}.parquet")

        # TTL de 7 días: si el caché existe y tiene menos de 7 días, usarlo
        if os.path.exists(cache_path):
            age_days = (
                datetime.datetime.now()
                - datetime.datetime.fromtimestamp(os.path.getmtime(cache_path))
            ).days
            if age_days < 7:
                try:
                    return pl.read_parquet(cache_path)
                except Exception:
                    pass  # Si el caché está corrupto, re-descargamos

        # Descargar de FMP — endpoint estable (non-legacy)
        url = f"{self.FMP_BASE}/dividends"
        params = {"symbol": ticker, "apikey": self.api_key}
        try:
            resp = requests.get(url, params=params, timeout=15)
            if resp.status_code != 200:
                print(f"[WARN] Dividendos HTTP {resp.status_code} para {ticker}")
                # Guardar DataFrame vacío en caché para no reintentar
                empty_df = pl.DataFrame({"date": pl.Series([], dtype=pl.Date), "adjDividend": pl.Series([], dtype=pl.Float64)})
                empty_df.write_parquet(cache_path, compression="snappy")
                return empty_df

            data = resp.json()
            # /stable/dividends retorna lista directa (no dict con 'historical')
            if not data or not isinstance(data, list):
                # Ticker sin dividendos
                empty_df = pl.DataFrame({"date": pl.Series([], dtype=pl.Date), "adjDividend": pl.Series([], dtype=pl.Float64)})
                empty_df.write_parquet(cache_path, compression="snappy")
                return empty_df

            # Normalizar a DataFrame Polars
            df = pl.DataFrame({
                "date":        [r.get("date", "") for r in data],
                "adjDividend": [float(r.get("adjDividend", 0)) for r in data],
            })
            df = df.with_columns(
                pl.col("date").str.to_date("%Y-%m-%d", strict=False)
            ).filter(pl.col("date").is_not_null()).sort("date", descending=True)

            # Guardar caché
            df.write_parquet(cache_path, compression="snappy")
            return df

        except Exception as exc:
            print(f"[WARN] Dividendos {ticker}: {exc}")
            return None

    def _compute_div_yield_cont(
        self,
        ticker: str,
        trade_date_str: str,
        stock_price: float,
    ) -> float:
        """
        Calcula la tasa de dividendos continua para un ticker en una fecha.

        Metodología TTM (Trailing Twelve Months):
            1. Sumar todos los adjDividend en los 365 días anteriores a trade_date.
            2. Dividir por stock_price → div_yield_raw.
            3. q = ln(1 + div_yield_raw)   # capitalización continua.

        Para tickers sin dividendos o sin historial → retorna 0.0.

        Args:
            ticker          : Símbolo bursátil.
            trade_date_str  : Fecha de trade (YYYY-MM-DD).
            stock_price     : Precio del subyacente ese día.

        Returns:
            float: Tasa de dividendos continua.
        """
        if not stock_price or stock_price <= 0:
            return 0.0

        div_df = self._fetch_dividends(ticker)
        if div_df is None or len(div_df) == 0:
            return 0.0

        try:
            trade_dt  = datetime.datetime.strptime(trade_date_str, "%Y-%m-%d").date()
            cutoff_dt = trade_dt - datetime.timedelta(days=365)

            # Filtrar dividendos dentro del TTM
            ttm_divs = div_df.filter(
                (pl.col("date") <= pl.lit(trade_dt).cast(pl.Date))
                & (pl.col("date") > pl.lit(cutoff_dt).cast(pl.Date))
            )

            total_div = ttm_divs["adjDividend"].sum()
            if total_div <= 0:
                return 0.0

            # Tasa continua
            div_yield_raw  = total_div / stock_price
            div_yield_cont = math.log(1.0 + div_yield_raw)
            return round(div_yield_cont, 8)

        except Exception as exc:
            print(f"[WARN] div_yield {ticker}/{trade_date_str}: {exc}")
            return 0.0

    def _get_next_div_date(
        self,
        ticker: str,
        trade_date_str: str,
    ) -> Optional[str]:
        """
        Devuelve la fecha del próximo dividendo DESPUÉS de trade_date_str.

        Busca en el historial de dividendos cacheado la primera fecha de
        pago que sea POSTERIOR a trade_date (estrategia forward).

        Args:
            ticker          : Símbolo bursátil.
            trade_date_str  : Fecha de trade (YYYY-MM-DD).

        Returns:
            str | None : Fecha en formato YYYY-MM-DD, o None si no hay
                         registro futuro disponible.
        """
        div_df = self._fetch_dividends(ticker)
        if div_df is None or len(div_df) == 0:
            return None

        try:
            # Filtra dividendos posteriores al tradeDate y ordena ascendente
            future_divs = (
                div_df
                .filter(pl.col("date") > pl.lit(trade_date_str).str.to_date("%Y-%m-%d"))
                .sort("date")  # ascendente → el primero es el más cercano
            )
            if len(future_divs) == 0:
                return None

            # Primera fecha futura
            next_date = future_divs["date"][0]
            return str(next_date)  # ISO format YYYY-MM-DD

        except Exception as exc:
            print(f"[WARN] next_div_date {ticker}/{trade_date_str}: {exc}")
            return None

    # ------------------------------------------------------------------
    # Método principal de enriquecimiento
    # ------------------------------------------------------------------

    def _fetch_treasury_batch(self, from_date: str, to_date: str) -> dict:
        """
        Descarga Y cachea todas las tasas del tesoro en un rango de fechas.

        La API /stable/treasury-rates acepta rangos → una sola llamada HTTP
        por año en lugar de una llamada por fecha.

        Args:
            from_date : Fecha inicial (YYYY-MM-DD).
            to_date   : Fecha final   (YYYY-MM-DD).

        Returns:
            dict: { "YYYY-MM-DD": {"month1": 5.12, ...}, ... }
        """
        url    = f"{self.FMP_BASE}/treasury-rates"
        params = {"from": from_date, "to": to_date, "apikey": self.api_key}
        result = {}
        try:
            resp = requests.get(url, params=params, timeout=30)
            if resp.status_code != 200:
                print(f"[WARN] Treasury batch HTTP {resp.status_code} ({from_date}→{to_date})")
                return result
            data = resp.json()
            if not isinstance(data, list):
                return result
            for row in data:
                d = row.get("date")
                if d:
                    result[d] = row
                    # Caché individual
                    cache_path = os.path.join(self.tsy_cache_dir, f"{d}.json")
                    if not os.path.exists(cache_path):
                        with open(cache_path, "w", encoding="utf-8") as fh:
                            json.dump(row, fh)
        except Exception as exc:
            print(f"[ERROR] Treasury batch ({from_date}→{to_date}): {exc}")
        return result

    def _build_spline_from_rates(self, rates: dict):
        """Construye el CubicSpline desde un dict de tasas BEY%."""
        from scipy.interpolate import CubicSpline
        days_list, rates_cont = [], []
        for key, days in self.TREASURY_GRID:
            bey_pct = rates.get(key)
            if bey_pct and bey_pct > 0:
                days_list.append(days)
                rates_cont.append(self._bey_to_continuous(float(bey_pct)))
        if len(days_list) < 2:
            return None
        return CubicSpline(days_list, rates_cont, bc_type="clamped")

    def enrich(
        self,
        date_filter: Optional[str] = None,
        years: Optional[List[int]] = None,
        force: bool = False,
        live_mode: bool = False,
    ) -> None:
        """
        Enriquece los archivos {option_type}_*.parquet con:
          - div_yield_cont  : tasa de dividendos continua (TTM / stockPrice)
          - next_div_date   : próximo pago de dividendo post tradeDate
          - risk_free_rate  : tasa del tesoro continua interpolada por DTE

        OPTIMIZACIONES vs versión anterior:
          1. Treasury en BATCH: 1 llamada HTTP/año en lugar de 1/fecha.
             Antes: 1688 llamadas | Ahora: ~7 llamadas para 2019-2025.
          2. Pre-carga de dividendos: todos los tickers únicos de todos los
             archivos pendientes se descargan ANTES del loop principal, en
             paralelo (ThreadPoolExecutor). Dentro del loop → sólo lecturas
             de caché en disco (≈0ms por ticker).
          3. Splines pre-construidos: dict date→CubicSpline. Lookup O(1)
             dentro del loop.
          4. Incremental: omite archivos que ya contienen 'risk_free_rate'.

        Args:
            date_filter : Procesar sólo esta fecha (YYYY-MM-DD).
            years       : Lista de años (e.g. [2020]) a procesar.
            force       : Reprocesar aunque ya estén enriquecidos.
        """
        import polars as pl
        import numpy as np

        print(f"\n[FMP ENRICH] ── Enriquecimiento con Dividendos y Tasas ──")

        # ──────────────────────────────────────────────────────────────
        # A: Listar archivos a procesar
        # ──────────────────────────────────────────────────────────────
        all_files = sorted(glob.glob(
            os.path.join(self.input_dir, f"{self.option_type}_*.parquet")
        ))

        if live_mode:
            all_files = [f for f in all_files if 'live_top200_wheels' in f]
        elif date_filter:
            all_files = [f for f in all_files
                         if os.path.basename(f) == f"{self.option_type}_{date_filter}.parquet"]
        elif years:
            year_strs = [str(y) for y in years]
            all_files = [f for f in all_files
                         if any(os.path.basename(f).startswith(f"call_{yr}")
                                for yr in year_strs)]

        if not all_files:
            print("[FMP ENRICH] No hay archivos que procesar.")
            return

        # Filtro incremental
        if not force:
            pending = []
            for f in all_files:
                try:
                    schema = pl.read_parquet_schema(f)
                    if "risk_free_rate" not in schema:
                        pending.append(f)
                except Exception:
                    pending.append(f)
            all_files = pending

        if not all_files:
            print("[FMP ENRICH] Todo al día. No hay archivos pendientes.")
            return

        n_files = len(all_files)
        print(f"[FMP ENRICH] Archivos a enriquecer: {n_files}")

        # ──────────────────────────────────────────────────────────────
        # B: PRE-CARGA TREASURY (batch por año)
        # ──────────────────────────────────────────────────────────────
        # Extraer todas las fechas únicas
        all_dates = sorted({
            (datetime.date.today().strftime('%Y-%m-%d') if 'live_top200_wheels' in f else os.path.basename(f).replace(f"{self.option_type}_", "").replace(".parquet", ""))
            for f in all_files
        })

        # Separar las que ya están en caché de las que hay que descargar
        cached_treasury  = {}
        missing_by_year: dict = {}
        for d in all_dates:
            cache_path = os.path.join(self.tsy_cache_dir, f"{d}.json")
            if os.path.exists(cache_path):
                with open(cache_path, "r") as fh:
                    cached_treasury[d] = json.load(fh)
            else:
                yr = d[:4]
                missing_by_year.setdefault(yr, []).append(d)

        # Descargar sólo los años con fechas faltantes (1 llamada/año)
        batch_treasury: dict = {}
        for yr, dates_needed in sorted(missing_by_year.items()):
            from_dt = f"{yr}-01-01"
            to_dt   = f"{yr}-12-31"
            print(f"[FMP ENRICH] Descargando tasas del tesoro {yr}...")
            batch = self._fetch_treasury_batch(from_dt, to_dt)
            batch_treasury.update(batch)

        # Mapa unificado date → raw rates dict
        all_treasury = {**cached_treasury, **batch_treasury}

        # Fallback para fines de semana / festivos: usar la fecha anterior
        def _get_rates(date_str: str) -> Optional[dict]:
            if date_str in all_treasury:
                return all_treasury[date_str]
            # Buscar hasta 5 días hábiles previos
            dt = datetime.datetime.strptime(date_str, "%Y-%m-%d").date()
            for lag in range(1, 6):
                prev = (dt - datetime.timedelta(days=lag)).strftime("%Y-%m-%d")
                if prev in all_treasury:
                    return all_treasury[prev]
            return None

        # ──────────────────────────────────────────────────────────────
        # C: PRE-CONSTRUIR todos los splines  (dict date → CubicSpline)
        # ──────────────────────────────────────────────────────────────
        print(f"[FMP ENRICH] Construyendo {len(all_dates)} splines...")
        splines: dict = {}
        for d in all_dates:
            rates = _get_rates(d)
            splines[d] = self._build_spline_from_rates(rates) if rates else None

        dates_with_spline = sum(1 for s in splines.values() if s is not None)
        print(f"[FMP ENRICH] Splines listos: {dates_with_spline}/{len(all_dates)}")

        # ──────────────────────────────────────────────────────────────
        # D: PRE-CARGA DIVIDENDOS (un scan de todos los tickers únicos)
        # ──────────────────────────────────────────────────────────────
        print(f"[FMP ENRICH] Escaneando tickers únicos en {n_files} archivos...")
        all_tickers: set = set()
        for f in all_files:
            try:
                tickers_in_file = (
                    pl.scan_parquet(f)
                    .select("ticker")
                    .collect()["ticker"]
                    .unique()
                    .to_list()
                )
                all_tickers.update(tickers_in_file)
            except Exception as e:
                import traceback
                traceback.print_exc()
                print(f"Error scanning {f}: {e}")

        # Separar tickers ya en caché de los que hay que descargar
        tickers_to_fetch = [
            t for t in all_tickers
            if not os.path.exists(os.path.join(self.div_cache_dir, f"{t}.parquet"))
            or (
                datetime.datetime.now()
                - datetime.datetime.fromtimestamp(
                    os.path.getmtime(os.path.join(self.div_cache_dir, f"{t}.parquet"))
                )
            ).days >= 7
        ]

        print(
            f"[FMP ENRICH] Tickers únicos: {len(all_tickers)} "
            f"({len(tickers_to_fetch)} a descargar, "
            f"{len(all_tickers) - len(tickers_to_fetch)} en caché)"
        )

        # Descargar dividendos en paralelo (I/O-bound)
        if tickers_to_fetch:
            def _download_ticker(ticker: str) -> str:
                self._fetch_dividends(ticker)   # guarda en caché automáticamente
                return ticker

            with concurrent.futures.ThreadPoolExecutor(
                max_workers=min((os.cpu_count() or 4) * 4, 32)
            ) as executor:
                futures_list = {
                    executor.submit(_download_ticker, t): t
                    for t in tickers_to_fetch
                }
                done_count = 0
                for future in concurrent.futures.as_completed(futures_list):
                    try:
                        future.result()
                    except Exception as exc:
                        t = futures_list[future]
                        print(f"[WARN] div batch {t}: {exc}")
                    done_count += 1
                    if done_count % 50 == 0 or done_count == len(tickers_to_fetch):
                        print(f"   [DIV] {done_count}/{len(tickers_to_fetch)} descargados")

        # ──────────────────────────────────────────────────────────────
        # E: LOOP PRINCIPAL — ahora todo está en caché local; sólo I/O
        # ──────────────────────────────────────────────────────────────
        print(f"\n[FMP ENRICH] Procesando {n_files} archivos (caché local)...")
        processed = 0
        errors    = 0

        for filepath in all_files:
            date_str = (
                datetime.date.today().strftime('%Y-%m-%d') if 'live_top200_wheels' in filepath else
                os.path.basename(filepath)
                .replace(f"{self.option_type}_", "")
                .replace(".parquet", "")
            )
            try:
                df = pl.read_parquet(filepath)

                # Limpiar columnas FMP anteriores si se fuerza reprocesado
                fmp_cols = ["risk_free_rate", "div_yield_cont", "next_div_date",
                            "div_yield_cont_right", "next_div_date_right"]
                cols_to_drop = [c for c in fmp_cols if c in df.columns]
                if cols_to_drop:
                    df = df.drop(cols_to_drop)

                # --- Risk-free rate via spline pre-construido ---
                spline = splines.get(date_str)
                if spline is None:
                    spline = self._build_rate_spline(date_str)
                if spline is None:
                    df = df.with_columns(pl.lit(0.0).alias("risk_free_rate"))
                else:
                    dte_arr  = np.clip(df["dte"].to_numpy().astype(float), 1.0, 10950.0)
                    rate_arr = np.maximum(0.0, spline(dte_arr))
                    df = df.with_columns(
                        pl.Series("risk_free_rate", rate_arr.tolist(), dtype=pl.Float64)
                    )

                # --- Dividendos y next_div_date desde caché local ---
                ticker_price_map = (
                    df.group_by("ticker")
                    .agg(pl.col("stockPrice").max().alias("stock_price"))
                    .to_dicts()
                )

                div_yield_map: dict = {}
                next_div_map:  dict = {}
                for row in ticker_price_map:
                    tkr   = row["ticker"]
                    price = row["stock_price"]
                    div_yield_map[tkr] = self._compute_div_yield_cont(tkr, date_str, price)
                    next_div_map[tkr]  = self._get_next_div_date(tkr, date_str)

                div_meta_df = pl.DataFrame({
                    "ticker":         list(div_yield_map.keys()),
                    "div_yield_cont": [float(v) for v in div_yield_map.values()],
                    "next_div_date":  [
                        datetime.datetime.strptime(next_div_map[t], "%Y-%m-%d").date()
                        if next_div_map.get(t) else None
                        for t in div_yield_map.keys()
                    ],
                }).with_columns(pl.col("next_div_date").cast(pl.Date))

                df = df.join(div_meta_df, on="ticker", how="left")
                df = df.with_columns(pl.col("div_yield_cont").fill_null(0.0))

                # Guardar atómicamente
                temp_path = filepath + ".fmp_tmp"
                df.write_parquet(temp_path, compression="snappy")
                if os.path.exists(filepath):
                    os.remove(filepath)
                os.rename(temp_path, filepath)

                processed += 1
                if processed % 50 == 0 or processed == n_files:
                    pct = processed / n_files * 100
                    print(f"   ... {processed}/{n_files} ({pct:.0f}%) | Último: {date_str}")

            except Exception as exc:
                import traceback
                print(f"[ERROR] {date_str}: {exc}")
                traceback.print_exc()
                errors += 1

        print(f"\n[FMP ENRICH] ✓ Proceso completado.")
        print(f"[FMP ENRICH]   Procesados : {processed}")
        print(f"[FMP ENRICH]   Errores    : {errors}")

    def build_test(

        self,
        date_filter: str,
        show_rows: int = 20,
    ) -> None:
        """
        Modo prueba: enriquece una fecha específica y muestra el resultado
        en consola sin necesidad de correr el pipeline completo.

        Args:
            date_filter : Fecha a procesar (YYYY-MM-DD).
            show_rows   : Número de filas a mostrar en la preview.
        """
        import polars as pl

        print(f"\n[FMP ENRICH TEST] Fecha: {date_filter}")

        # Ejecutar (fuerza reprocesado si existe)
        self.enrich(date_filter=date_filter, force=True)

        # Leer resultado
        out_path = os.path.join(self.input_dir, f"{self.option_type}_{date_filter}.parquet")
        if not os.path.exists(out_path):
            print(f"[ERROR] No se encontró el archivo enriquecido: {out_path}")
            return

        df = pl.read_parquet(out_path)

        print(f"\n[FMP ENRICH TEST] Columnas: {df.columns}")
        print(f"[FMP ENRICH TEST] Filas    : {len(df):,}")

        # Mostrar resumen por ticker de las columnas nuevas
        summary = (
            df.group_by("ticker")
            .agg([
                pl.col("div_yield_cont").mean().round(6).alias("div_yield"),
                pl.col("risk_free_rate").mean().round(6).alias("avg_rfr"),
                pl.col("risk_free_rate").min().round(6).alias("min_rfr"),
                pl.col("risk_free_rate").max().round(6).alias("max_rfr"),
            ])
            .sort("ticker")
        )

        print(f"\n{'─'*70}")
        print(f"  Resumen por ticker (primeros {show_rows})")
        print(f"{'─'*70}")
        with pl.Config(tbl_width_chars=100, tbl_rows=show_rows):
            print(summary.head(show_rows))

        # Verificación de la curva de tasas
        print(f"\n{'─'*70}")
        print(f"  Curva de tasas para {date_filter} (muestra de DTEs)")
        print(f"{'─'*70}")
        spline = self._build_rate_spline(date_filter)
        if spline:
            test_dtes = [7, 14, 30, 60, 90, 180, 365, 730]
            print(f"  {'DTE':>6} | {'r_continua':>12} | {'r_anual_aprox%':>15}")
            print(f"  {'':->6}-+-{'':->12}-+-{'':->15}")
            for dte in test_dtes:
                rc  = max(0.0, float(spline(dte)))
                pct = (math.exp(rc) - 1) * 100
                print(f"  {dte:>6} | {rc:>12.6f} | {pct:>14.3f}%")



# ==============================================================================
# IV ENRICHER
# Agrega iv_call usando árbol LR-Brent americano con div_yield_cont (q continuo).
# ==============================================================================

class IVEnricher:
    """
    Enriquece call_*.parquet con columna iv_call.

    Motor: _numba_iv.py — LR-101 + Brent + numba.prange paralelo.
    Dividendos: usa div_yield_cont (yield continuo ya calculado por FMPDataEnricher).
    Incremental: omite archivos que ya tienen columna iv_call.
    Escritura atómica: .iv_tmp → rename.
    """

    N_STEPS  : int   = 101
    IV_LO    : float = 0.001
    IV_HI    : float = 20.0
    IV_TOL   : float = 1e-5
    IV_MAXITER: int  = 100

    def __init__(self, input_dir: str, option_type: str = "Call", cache_dir: str = "") -> None:
        self.input_dir = input_dir
        self.option_type = option_type.lower()
        self.option_type = option_type.lower()
        self._engine       = None

    def _get_engine(self):
        """Carga y pre-compila el motor JIT la primera vez."""
        if self._engine is None:
            import importlib, sys
            mod_dir = os.path.dirname(os.path.abspath(__file__))
            if mod_dir not in sys.path:
                sys.path.insert(0, mod_dir)
            iv_mod = importlib.import_module("_numba_iv")
            iv_mod.warmup(self.N_STEPS)
            self._engine = iv_mod.compute_iv_batch
        return self._engine

    def enrich(
        self,
        date_filter: Optional[str]       = None,
        years:       Optional[List[int]] = None,
        force:       bool                = False,
        live_mode:   bool                = False,
    ) -> None:
        """
        Añade columna iv_call a cada call_*.parquet (incremental).

        Requiere que el archivo ya tenga: risk_free_rate, div_yield_cont,
        stockPrice, strike, dte, callMidPrice.
        """
        import polars as pl
        import numpy as np

        print("\n[IV ENRICH] ── Volatilidad Implícita (LR-Brent + Numba) ──")

        all_files = sorted(glob.glob(os.path.join(self.input_dir, f"{self.option_type}_*.parquet")))
        if date_filter:
            all_files = [f for f in all_files
                         if os.path.basename(f) == f"{self.option_type}_{date_filter}.parquet"]
        elif years:
            ys = [str(y) for y in years]
            all_files = [f for f in all_files
                         if any(os.path.basename(f).startswith(f"{self.option_type}_{y}") for y in ys)]

        if not all_files:
            print("[IV ENRICH] Sin archivos que procesar."); return

        if not force:
            pending = []
            for f in all_files:
                try:
                    if f"iv_{self.option_type}" not in pl.read_parquet_schema(f):
                        pending.append(f)
                except Exception:
                    pending.append(f)
            all_files = pending

        if not all_files:
            print("[IV ENRICH] Todo al día. Sin archivos pendientes."); return

        n_files = len(all_files)
        print(f"[IV ENRICH] Archivos a procesar: {n_files}")

        engine    = self._get_engine()
        processed = 0
        errors    = 0

        print(f"[IV ENRICH] Calculando iv_call en {n_files} archivos...")

        for filepath in all_files:
            date_str = (
                datetime.date.today().strftime('%Y-%m-%d') if 'live_top200_wheels' in filepath else
                os.path.basename(filepath)
                .replace(f"{self.option_type}_", "").replace(".parquet", "")
            )
            try:
                df = pl.read_parquet(filepath)

                iv_col_name = f"iv_{self.option_type}"
                if iv_col_name in df.columns:
                    df = df.drop(iv_col_name)

                # Validar insumos requeridos
                mid_price_col = f"{self.option_type}MidPrice"
                required = {"risk_free_rate", "div_yield_cont", "stockPrice", "strike", "dte", mid_price_col}
                missing = required - set(df.columns)
                if missing:
                    print(f"[WARN] {date_str}: faltan columnas {missing} — skip")
                    errors += 1
                    continue

                # Arrays numpy (float64)
                S_arr   = df["stockPrice"]    .to_numpy(allow_copy=True).astype(np.float64)
                K_arr   = df["strike"]        .to_numpy(allow_copy=True).astype(np.float64)
                r_arr   = df["risk_free_rate"].to_numpy(allow_copy=True).astype(np.float64)
                q_arr   = df["div_yield_cont"].to_numpy(allow_copy=True).astype(np.float64)
                T_arr   = df["dte"]           .to_numpy(allow_copy=True).astype(np.float64) / 365.0
                mkt_arr = df[mid_price_col]  .to_numpy(allow_copy=True).astype(np.float64)

                # Determine Option type bool mapping
                is_call = True if self.option_type == 'call' else False
                
                # IV en paralelo (numba.prange)
                iv_arr = engine(
                    S_arr, K_arr, r_arr, q_arr, T_arr, mkt_arr,
                    self.N_STEPS, self.IV_LO, self.IV_HI,
                    self.IV_TOL, self.IV_MAXITER, is_call
                )

                df = df.with_columns(
                    pl.Series(iv_col_name, iv_arr, dtype=pl.Float64)
                )

                # Escritura atómica
                tmp = filepath + ".iv_tmp"
                df.write_parquet(tmp, compression="snappy")
                if os.path.exists(filepath):
                    os.remove(filepath)
                os.rename(tmp, filepath)

                processed += 1
                if processed % 50 == 0 or processed == n_files:
                    n_ok = int(np.sum(~np.isnan(iv_arr)))
                    print(f"   ... {processed}/{n_files} ({processed/n_files*100:.0f}%) "
                          f"| {date_str} | iv_ok={n_ok}/{len(iv_arr)}")

            except Exception as exc:
                import traceback
                print(f"[ERROR] {date_str}: {exc}")
                traceback.print_exc()
                errors += 1

        print(f"\n[IV ENRICH] ✓ Completado. Procesados={processed} Errores={errors}")


# ==============================================================================
# CLEAN IV ENRICHER (Earnings Variance Stripping)
# ==============================================================================

class CleanIVEnricher:
    """
    Enriquece call_*.parquet con iv_clean (Volatilidad Limpia Ex-Evento).

    Motor Numérico: _numba_clean_iv.py (Implied Forward Variance ATM Stripping).
    Aísla la varianza inyectada por los earnings midiendo el salto de TV
    entre opciones pre-evento y post-evento en Días Hábiles (252).
    """
    
    def __init__(self, input_dir: str, option_type: str = "Call") -> None:
        self.input_dir = input_dir
        self.option_type = option_type.lower()
        self.option_type = option_type.lower()
        self._engine       = None
        self._get_biz_days = None

    def _get_engine(self):
        """Carga Numba JIT."""
        if self._engine is None:
            import importlib, sys
            mod_dir = os.path.dirname(os.path.abspath(__file__))
            if mod_dir not in sys.path:
                sys.path.insert(0, mod_dir)
            iv_mod = importlib.import_module("_numba_clean_iv")
            iv_mod.warmup()
            self._engine       = iv_mod.compute_clean_iv_batch
            self._get_biz_days = iv_mod.get_biz_days_array
        return self._engine, self._get_biz_days

    def enrich(
        self,
        date_filter: Optional[str]       = None,
        years:       Optional[List[int]] = None,
        force:       bool                = False,
        live_mode:   bool                = False,
    ) -> None:
        import polars as pl
        import numpy as np

        print("\n[CLEAN IV] ── Extirpación de Varianza de Evento (Earnings Stripping) ──")

        all_files = sorted(glob.glob(os.path.join(self.input_dir, f"{self.option_type}_*.parquet")))
        if date_filter:
            all_files = [f for f in all_files
                         if os.path.basename(f) == f"{self.option_type}_{date_filter}.parquet"]
        elif years:
            ys = [str(y) for y in years]
            all_files = [f for f in all_files
                         if any(os.path.basename(f).startswith(f"{self.option_type}_{y}") for y in ys)]

        if not all_files:
            print("[CLEAN IV] Sin archivos que procesar."); return

        if not force:
            pending = []
            for f in all_files:
                try:
                    if "iv_clean" not in pl.read_parquet_schema(f):
                        pending.append(f)
                except Exception:
                    pending.append(f)
            all_files = pending

        if not all_files:
            print("[CLEAN IV] Todo al día. Sin archivos pendientes."); return

        n_files = len(all_files)
        print(f"[CLEAN IV] Archivos a procesar: {n_files}")

        engine, get_biz_days = self._get_engine()
        processed = 0
        errors    = 0

        for filepath in all_files:
            date_str = (datetime.date.today().strftime('%Y-%m-%d') if 'live_top200_wheels' in filepath else os.path.basename(filepath).replace(f"{self.option_type}_", "").replace(".parquet", ""))
            try:
                df = pl.read_parquet(filepath)

                if "iv_clean" in df.columns:
                    df = df.drop("iv_clean")

                iv_col_name = f"iv_{self.option_type}"
                mid_price_col = f"{self.option_type}MidPrice"
                required = {iv_col_name, "stockPrice", "strike", "dte", mid_price_col, "next_earning_date"}
                missing = required - set(df.columns)
                if missing:
                    print(f"[WARN] {date_str}: faltan columnas {missing}")
                    errors += 1
                    continue

                # Preparar IDs de grupo (por ticker) para el JIT numérico
                # Esto es más eficiente que el pl.partition_by de polars para 
                # numba que requiere contiguedad de memoria
                df = df.cast({"next_earning_date": pl.Date})
                # Parseo seguro de fechas, las inválidas o nulas se omiten
                df = df.with_columns([
                    pl.col("tradeDate").str.to_date("%Y-%m-%d", strict=False).alias("tradeDate_dt"),
                    pl.col("expirDate").str.to_date("%Y-%m-%d", strict=False).alias("expirDate_dt")
                ]).drop_nulls(subset=["tradeDate_dt", "expirDate_dt"])

                if len(df) == 0:
                    print(f"[WARN] {date_str}: Todas las fechas son inválidas.")
                    continue

                # Asignar ID unico por ticker
                tickers = df["ticker"].to_numpy()
                unique_tickers, group_idx = np.unique(tickers, return_inverse=True)

                trade_date_dt64 = df["tradeDate_dt"].cast(pl.Datetime).to_numpy()
                expir_date_dt64 = df["expirDate_dt"].cast(pl.Datetime).to_numpy()
                # Biz days
                biz_arr = get_biz_days(
                    trade_date_dt64.astype('datetime64[D]'),
                    expir_date_dt64.astype('datetime64[D]')
                )
                
                # Earnings logic array
                has_earning = df["next_earning_date"].is_not_null().to_numpy()
                # Cast the datetime64 inside numpy for safe > op
                p_next_earn = df["next_earning_date"].cast(pl.Datetime).to_numpy()
                
                # Relleno seguro para la comparación (dado los nulos)
                safe_earn_arr = np.where(pd.isna(p_next_earn), np.datetime64('2099-01-01'), p_next_earn)
                
                # Es post-earning si Expira DESPUÉS o el MISMO DÍA (event risk pasa overnight)
                is_post_ern = (expir_date_dt64 >= safe_earn_arr)
                
                # El grupo como entidad tiene earning? (Agregación)
                # Group logic (un_ticker x) tiene has_earning?
                grp_has_earning = np.zeros(len(unique_tickers), dtype=np.bool_)
                np.logical_or.at(grp_has_earning, group_idx, has_earning)

                # Variables para Numpy
                iv_arr  = df[iv_col_name]     .to_numpy(allow_copy=True).astype(np.float64)
                mkt_arr = df[mid_price_col].to_numpy(allow_copy=True).astype(np.float64)
                K_arr   = df["strike"]      .to_numpy(allow_copy=True).astype(np.float64)
                S_arr   = df["stockPrice"]  .to_numpy(allow_copy=True).astype(np.float64)
                dte_arr = df["dte"]         .to_numpy(allow_copy=True).astype(np.float64)
                group_idx = group_idx.astype(np.int32)
                
                # Stripping!
                iv_clean = engine(
                    iv_arr, mkt_arr, K_arr, S_arr, dte_arr, biz_arr,
                    group_idx, grp_has_earning, is_post_ern
                )

                df = df.with_columns(
                    pl.Series("iv_clean", iv_clean, dtype=pl.Float64)
                )

                # Escritura 
                tmp = filepath + ".cln_tmp"
                df.write_parquet(tmp, compression="snappy")
                if os.path.exists(filepath):
                    os.remove(filepath)
                os.rename(tmp, filepath)

                processed += 1
                if processed % 50 == 0 or processed == n_files:
                    n_mod = int(np.sum(iv_clean < iv_arr))
                    print(f"   ... {processed}/{n_files} ({processed/n_files*100:.0f}%) "
                          f"| {date_str} | vol_limpiadas={n_mod}/{len(iv_arr)}")

            except Exception as exc:
                import traceback
                print(f"[ERROR] {date_str}: {exc}")
                traceback.print_exc()
                errors += 1

        print(f"\n[CLEAN IV] ✓ Completado. Procesados={processed} Errores={errors}")
