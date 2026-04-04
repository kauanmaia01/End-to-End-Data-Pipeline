import pandas as pd
import requests
from bs4 import BeautifulSoup
import dotenv 

import os
import time

from utils.log_message import run_log_message


dotenv.load_dotenv()


def get_html_fundamentus(url, user_agent) -> str:
    ''' Extraí o conteúdo bruto do site '''
    try:
        response = requests.get(url, headers=user_agent)
        return response.text
    
    except Exception as e:
        run_log_message.error(f'[Ingestão de Dados Fundamentus] Ao acessar o site {url}, Detalhe do erro: {e}')


def html_table_extract_fundamentus(html_content: str) -> pd.DataFrame:
    soup = BeautifulSoup(html_content)
    
    try:
        table = soup.find('table')

        table_head = table.find('thead')
        table_columns = [col.text for col in table_head.find_all('th')]

        table_body = table.find('tbody')
        table_rows = []

        for row in table_body.find_all('tr'):
            table_rows.append([x.getText() for x in row.children if x != '\n'])
        
        return pd.DataFrame(data=table_rows, columns=table_columns)
    
    except Exception as e:
        run_log_message.error(f'[Ingestão de Dados Fundamentus] Ao extrair os dados, Detalhe do erro: {e}')


def run_extration_fundamentus():
    
    process_start = time.strftime('%H:%M:%S')

    MY_USER_AGENT = os.getenv('MY_USER_AGENT')

    user_agent = {
    'USER-AGENT': MY_USER_AGENT
    }
    
    response_html = get_html_fundamentus('https://www.fundamentus.com.br/resultado.php', user_agent)
    df = html_table_extract_fundamentus(response_html)

    if df.empty:
        run_log_message.warning('[Ingestão de Dados Fundamentus] DataFrame vazio. Nada foi salvo.')
        return df

    process_end = time.strftime('%H:%M:%S')

    run_log_message.info(f'[Ingestão de Dados Fundamentus] Inicio: {process_start} | Fim: {process_end}')

    return df