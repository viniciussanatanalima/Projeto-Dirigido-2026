import requests
import pandas as pd
from datetime import datetime, timedelta
from google.cloud import bigquery

PROJECT_ID = 'dados-financeiros-ufabc'
DATASET = 'indicadores_bcb'
TABELA = 'cambio_ipca_selic'
TABELA_DESTINO = f'{PROJECT_ID}.{DATASET}.{TABELA}'

client = bigquery.Client()

data_inicio = (datetime.today() - timedelta(days=10*365)).strftime('%d/%m/%Y')

url1 = (
    f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.1/dados"
    f"?formato=json&dataInicial={data_inicio}"
)

url2 = (
    f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.433/dados"
    f"?formato=json&dataInicial={data_inicio}"
)

url3 = (
    f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.432/dados"
    f"?formato=json&dataInicial={data_inicio}"
)

response1 = requests.get(url1)
response2 = requests.get(url2)
response3 = requests.get(url3)

if response1.status_code != 200:
    print("Erro:", response1.status_code, response1.json())
else:
    df_cambio = pd.DataFrame(response1.json())
    df_cambio['data'] = pd.to_datetime(df_cambio['data'], dayfirst=True)
    df_cambio['valor'] = pd.to_numeric(df_cambio['valor'])

if response2.status_code != 200:
    print("Erro:", response2.status_code, response2.json())
else:
    df_ipca = pd.DataFrame(response2.json())
    df_ipca['data'] = pd.to_datetime(df_ipca['data'], dayfirst=True)
    df_ipca['valor'] = pd.to_numeric(df_ipca['valor'])

if response3.status_code != 200:
    print("Erro:", response3.status_code, response3.json())
else:
    df_selic = pd.DataFrame(response3.json())
    df_selic['data'] = pd.to_datetime(df_selic['data'], dayfirst=True)
    df_selic['valor'] = pd.to_numeric(df_selic['valor'])

# Extrai ano e mês de cada DataFrame
df_cambio['ano_mes'] = df_cambio['data'].dt.to_period('M')
df_ipca['ano_mes'] = df_ipca['data'].dt.to_period('M')
df_selic['ano_mes'] = df_selic['data'].dt.to_period('M')

# Agrega câmbio por mês (média mensal)
df_cambio_mensal = df_cambio.groupby('ano_mes')['valor'].mean().reset_index()
df_cambio_mensal.columns = ['ano_mes', 'cambio_medio']

# Renomeia IPCA
df_ipca_mensal = df_ipca[['ano_mes', 'valor']].rename(columns={'valor': 'ipca'})

# Renomeia SELIC
df_selic_mensal = df_selic.groupby('ano_mes')['valor'].mean().reset_index()
df_selic_mensal.columns = ['ano_mes', 'selic_media']

# Merge por ano_mes
df_final = pd.merge(df_cambio_mensal, df_ipca_mensal, on='ano_mes')
df_final = pd.merge(df_final, df_selic_mensal, on='ano_mes')

# Definição da função — só o nome dos parâmetros, sem valores
def carregar_bigquery(df, tabela_destino):
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )
    job = client.load_table_from_dataframe(df, tabela_destino, job_config=job_config)
    job.result()

# Chamada da função — aqui sim você passa os valores reais
df_final['ano_mes'] = df_final['ano_mes'].astype(str)
carregar_bigquery(df_final, TABELA_DESTINO)
tabela = client.get_table(TABELA_DESTINO)

tabela = client.get_table('dados-financeiros-ufabc.indicadores_bcb.cambio_ipca_selic')
print(f"Carregadas {tabela.num_rows} linhas no BigQuery.")
