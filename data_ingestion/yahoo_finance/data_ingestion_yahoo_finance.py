# Importações
import yfinance as yf
import requests
import pandas as pd

from typing import List
import time

from utils.log_message import run_log_message


def search_tickers_brapi() -> List:
    ''' Procura todas as ações listadas na B3 '''
    
    response = requests.get('https://brapi.dev/api/available', timeout=10)
    response.raise_for_status()
    list_tickers = response.json()
    
    list_tickers = [ticker + '.SA' for ticker in list_tickers['stocks']]

    return list_tickers


def chunk_list(lst, size):
    for i in range(0, len(lst), size):
        yield lst[i:i + size]


def data_extraction_yahoo_finance(list_tickers: List) -> pd.DataFrame:
    batch = []

    for chunk in chunk_list(list_tickers, 50):
        try:
            run_log_message.info(f'[Ingestão de Dados Yahoo Finance] Baixando chunk com {len(chunk)} tickers')

            data = yf.download(
                tickers=chunk,
                period="5d",
                group_by="ticker",
                threads=True
            )

            multi_index = isinstance(data.columns, pd.MultiIndex)

            for ticker in chunk:
                try:
                    df = data[ticker].copy() if multi_index else data.copy()

                    if df.empty:
                        continue

                    df['ticker_name'] = ticker
                    df['process_date'] = time.strftime('%Y-%m-%d')

                    df = df.reset_index()
                    batch.append(df)

                except Exception as e:
                    run_log_message.warning(f'[Ingestão de Dados Yahoo Finance] Erro no ticker {ticker}: {e}')

        except Exception as e:
            run_log_message.error(f'[Ingestão de Dados Yahoo Finance] Erro no chunk: {e}')

    if not batch:
        return pd.DataFrame()

    return pd.concat(batch, ignore_index=True)


def run_extration_yahoo_finance():
    list_tickers = search_tickers_brapi()
    
    process_start = time.strftime('%H:%M:%S')
    
    df = data_extraction_yahoo_finance(list_tickers)

    if df.empty:
        run_log_message.warning('[Ingestão de Dados Yahoo Finance] DataFrame vazio. Nada foi salvo.')
        return df

    process_end = time.strftime('%H:%M:%S')

    run_log_message.info(f'[Ingestão de Dados Yahoo Finance] Inicio: {process_start} | Fim: {process_end}')

    return df