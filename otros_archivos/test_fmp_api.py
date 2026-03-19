import requests

API_KEY = "xjnhJX6n8NP06Igh3DhHjA8qLOl4i09I"

def test_search():
    url = f"https://financialmodelingprep.com/api/v3/search?query=A&limit=100&exchange=NASDAQ,NYSE,AMEX&apikey={API_KEY}"
    res = requests.get(url).json()
    print("search length A:", len(res))
    if len(res) > 0 and isinstance(res, list):
        print("Keys:", res[0].keys())
        
    url2 = f"https://financialmodelingprep.com/api/v3/stock_market/actives?apikey={API_KEY}"
    res2 = requests.get(url2).json()
    print("actives length:", len(res2))

test_search()
