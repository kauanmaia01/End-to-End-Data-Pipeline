import pandas as pd
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

import os
import time

from utils.log_message import get_logger

logger = get_logger(__name__)
load_dotenv()


def get_html_fundamentus(url, user_agent) -> str:
    ''' Extraí o conteúdo bruto do site '''
    try:
        response = requests.get(url, headers=user_agent, timeout=10)
        response.raise_for_status()
        return response.text
    
    except requests.RequestException as e:
        logger.error(f'[Fundamentus] Erro ao acessar {url}: {e}')
        return None


def html_table_extract_fundamentus(html_content: str) -> pd.DataFrame:
    if not html_content:
        return pd.DataFrame()
    
    soup = BeautifulSoup(html_content, 'html.parser')
    
    try:
        table = soup.find('table')

        if not table:
            logger.error('[Fundamentus] Tabela não encontrada no HTML')
            return pd.DataFrame()

        table_head = table.find('thead')
        table_columns = [col.get_text(strip=True) for col in table_head.find_all('th')]

        table_body = table.find('tbody')
        table_rows = []

        for row in table_body.find_all('tr'):
            table_rows.append([col.get_text(strip=True) for col in row.find_all('td')])
        
        return pd.DataFrame(data=table_rows, columns=table_columns)
    
    except Exception as e:
        logger.error(f'[Ingestão de Dados Fundamentus] Ao extrair os dados: {e}')


def run_extration_fundamentus() -> pd.DataFrame:
    process_start = time.strftime('%H:%M:%S')

    MY_USER_AGENT = os.getenv('MY_USER_AGENT')

    if not MY_USER_AGENT:
        raise ValueError('USER_AGENT não definido')

    user_agent = {'USER-AGENT': MY_USER_AGENT}
    
    response_html = get_html_fundamentus(
        'https://www.fundamentus.com.br/resultado.php', 
        user_agent
    )

    if not response_html:
        return pd.DataFrame()
    
    df = html_table_extract_fundamentus(response_html)

    if df.empty:
        logger.warning('[Ingestão de Dados Fundamentus] DataFrame vazio.')
        return df

    process_end = time.strftime('%H:%M:%S')

    logger.info(f'[Ingestão de Dados Fundamentus] Inicio: {process_start} | Fim: {process_end}')

    return df