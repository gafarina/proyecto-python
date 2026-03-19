import polars as pl
import random
import os

file_path = r"C:\datos_proyecto\datos_stocks\fmp_prices_top3000.parquet"

if not os.path.exists(file_path):
    print(f"El archivo {file_path} no existe. Has ejecutado el Paso 8.5?")
else:
    try:
        df = pl.read_parquet(file_path)
        # Unique tickers
        tickers = df['ticker'].unique().to_list()
        if not tickers:
            print("El archivo Parquet está vacío.")
        else:
            # Elijo un seed para no variar en cada corrida mientras debuggeo, o al azar si prefieres
            random_ticker = random.choice(tickers)
            print(f"Ticker seleccionado al azar: {random_ticker}")
            print(f"Total de activos actualmente en tu archivo: {len(tickers)}")
            
            # Filter for random ticker
            df_ticker = df.filter(pl.col('ticker') == random_ticker).sort('date')
            print(f"Cantidad de dias historicos para {random_ticker}: {len(df_ticker)}")
            
            # Convert to dict to avoid charmap printing issues in windows cmd
            head_dicts = df_ticker.head(5).to_dicts()
            tail_dicts = df_ticker.tail(5).to_dicts()
            
            print("\n--- Primeros 5 registros ---")
            for row in head_dicts:
                # Mostrar solo algunas columnas relevantes
                print(f"Date: {str(row['date'])[:10]} | Open: {row['open']} | High: {row['high']} | Low: {row['low']} | Close: {row['close']} | Vol: {row['volume']}")
                
            print("\n--- Últimos 5 registros ---")
            for row in tail_dicts:
                print(f"Date: {str(row['date'])[:10]} | Open: {row['open']} | High: {row['high']} | Low: {row['low']} | Close: {row['close']} | Vol: {row['volume']}")
                
    except Exception as e:
        print(f"Error al leer el archivo Parquet: {e}")
