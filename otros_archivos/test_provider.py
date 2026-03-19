import sys
import logging
from fmp_ticker_updater import OratsTopVolumeUniverseProvider

logging.basicConfig(level=logging.INFO)

def test_provider():
    provider = OratsTopVolumeUniverseProvider(limit=3000, days_to_look=2)
    tickers = provider.get_universe()
    print("Total tickers returned from ORATS:", len(tickers))
    if len(tickers) > 0:
        print("First 10:", tickers[:10])

if __name__ == "__main__":
    test_provider()
