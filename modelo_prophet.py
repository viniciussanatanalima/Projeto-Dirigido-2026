import pandas as pd
from google.cloud import bigquery
from prophet import Prophet
from sklearn.metrics import mean_absolute_error

PROJECT_ID = 'dados-financeiros-ufabc'
DATASET = 'indicadores_bcb'
TABELA = 'cambio_ipca_selic'
TABELA_DESTINO = f'{PROJECT_ID}.{DATASET}.{TABELA}'

client = bigquery.Client()

query = """
    SELECT ano_mes, cambio_medio, ipca, selic_media
    FROM `dados-financeiros-ufabc.indicadores_bcb.cambio_ipca_selic`
    ORDER BY ano_mes
"""

df_final = client.query(query).to_dataframe()
df_final['ano_mes'] = pd.to_datetime(df_final['ano_mes'], format='%Y-%m')

data_corte = '2024-12'

df_treino = df_final[df_final['ano_mes'] <= data_corte].copy()
df_teste = df_final[df_final['ano_mes'] > data_corte].copy()

df_treino_prophet = pd.DataFrame({
    'ds': df_treino['ano_mes'],
    'y': df_treino['ipca'],
    'cambio_medio': df_treino['cambio_medio'],
    'selic_media': df_treino['selic_media']
})

model = Prophet()
model.add_regressor('cambio_medio')
model.add_regressor('selic_media')
model.fit(df_treino_prophet)

futuro = model.make_future_dataframe(periods=6, freq='MS')
futuro['cambio_medio'] = df_final['cambio_medio'].mean()
futuro['selic_media'] = df_final['selic_media'].mean()

df_teste_prophet = pd.DataFrame({
    'ds': df_teste['ano_mes'],
    'cambio_medio': df_teste['cambio_medio'],
    'selic_media': df_teste['selic_media']
})

previsao_teste = model.predict(df_teste_prophet)

df_comparacao = df_teste.copy()
df_comparacao['ipca_previsto'] = previsao_teste['yhat'].values

mae = mean_absolute_error(df_comparacao['ipca'], df_comparacao['ipca_previsto'])
inicio = df_teste['ano_mes'].min().strftime('%Y-%m')
fim = df_teste['ano_mes'].max().strftime('%Y-%m')
print(f"\nBacktesting — período {inicio} a {fim}:")
for _, row in df_comparacao.iterrows():
    print(f"{row['ano_mes'].strftime('%Y-%m')}  real: {row['ipca']:.2f}  previsto: {row['ipca_previsto']:.2f}  erro: {abs(row['ipca'] - row['ipca_previsto']):.2f}")
print(f"MAE: {mae:.4f}")