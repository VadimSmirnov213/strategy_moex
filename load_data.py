import pandas as pd
import concurrent.futures
import datetime
from pathlib import Path

from moexalgo import Ticker


DATA_DIRECTORY = Path('data/')
DATA_DIRECTORY.mkdir(exist_ok=True)
START_DATE = '2020-01-01'
FINISH_DATE = '2023-11-30'


def _download_stat(ticker: str, stat: str, force_recompute: bool) -> tuple[str, pd.DataFrame]:
    file = DATA_DIRECTORY / ticker / f'{stat}.csv'
    if file.exists() and not force_recompute:
        return stat, pd.read_csv(file, index_col=0)
    function = getattr(Ticker(ticker), stat)
    if stat in ['info', 'marketdata']:
        df = function()
    else:
        dfs = []
        date = START_DATE
        while True:
            df = function(date=date, till_date=FINISH_DATE, limit=50000)
            dfs.append(df)
            if len(df) < 49999:
                break
            date = str(df.iloc[-1]['tradedate'] + datetime.timedelta(days=1))
        df = pd.concat(dfs).reset_index(drop=True)
    df.to_csv(file)
    return stat, df


def load_ticker(ticker: str, stats: list[str] | None = None, force_recompute: bool = False):
    (DATA_DIRECTORY / ticker).mkdir(exist_ok=True)
    all_stats = ['info', 'tradestats', 'orderstats', 'obstats', 'marketdata']
    if stats is None:
        stats = all_stats
    assert set(stats).issubset(all_stats)
    results = {}

    with concurrent.futures.ThreadPoolExecutor() as executor:
        # Using submit to start each function in parallel
        futures = [executor.submit(_download_stat, ticker, stat, force_recompute) for stat in stats]

        for future in concurrent.futures.as_completed(futures):
            try:
                stat, result = future.result()
                results[stat] = result
            except Exception as e:
                print(f"Exception: {e}")
    return results
