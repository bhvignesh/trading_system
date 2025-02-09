# trading_system/main.py

import src.collectors.data_collector as data_collector
import pandas as pd

tickers = pd.read_excel("data/ticker.xlsx")
tickers = tickers[~tickers["Security Name"].duplicated()]
tickers.reset_index(drop=True, inplace=True)

def add_ticker_suffix(x):
    if x["Exchange"]=="BSE":
        return x["Security Name"] + ".BO"
    else:
        return x["Security Name"] + ".NS"

tickers["Ticker"] = tickers.apply(add_ticker_suffix, axis = 1)
all_tickers = tickers["Ticker"].tolist()

all_tickers = all_tickers[:10]

print(all_tickers)

data_collector.main(all_tickers)



