from pathlib import Path
import pandas as pd

from data_ingestion.fundamentus.data_ingestion_fundamentus import run_extration_fundamentus
from data_ingestion.yahoo_finance.data_ingestion_yahoo_finance import run_extration_yahoo_finance
from utils.storage import save_parquet
from utils.log_message import get_logger

BASE_PATH = Path('data/bronze')
logger = get_logger(__name__)

def add_metadata(df: pd.DataFrame, source: str) -> pd.DataFrame:
    df['source'] = source
    return df


def main():
    logger.info('Ingestão de Dados Iniciada')
    df_fundamentus = add_metadata(run_extration_fundamentus(), 'fundamentus')
    df_yahoo = add_metadata(run_extration_yahoo_finance(), 'yahoo_finance')

    save_parquet(df_fundamentus, BASE_PATH / 'fundamentus' / 'data.parquet')
    save_parquet(df_yahoo, BASE_PATH / 'yahoo_finance' / 'data.parquet')
    
    logger.info('Ingestão de Dados Finalizada')


if __name__ == '__main__':
    main()