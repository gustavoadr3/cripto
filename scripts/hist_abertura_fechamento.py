import os
import requests
import pandas as pd
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import time
from conectar_banco import conexão_banco
from mysql.connector import connect, Error

df_moedas = pd.read_csv(r'data/processed/id_moedas.csv')
moedas = df_moedas['id'].tolist()
df_historico_ohlc = pd.DataFrame()

# Função para extrair dados históricos OHLC (Open, High, Low, Close) Obs..: A API retorna em um intervalo de 4 dias 
def obter_dados_hist_ohlc(cripto_id, vs_currency='usd', days=365):
    url = f'https://api.coingecko.com/api/v3/coins/{cripto_id}/ohlc'
    params = {
        'vs_currency': vs_currency,
        'days': days  
    }
    response = requests.get(url, params=params)
    if response.status_code == 200:
        data = response.json()
        df_hist = pd.DataFrame(data, columns=['timestamp', 'abertura', 'maior_valor', 'menor_valor', 'fechamento'])
        df_hist['data'] = pd.to_datetime(df_hist['timestamp'], unit='ms').dt.date
        df_hist['nome'] = cripto_id
        return df_hist[['nome','data', 'abertura', 'maior_valor', 'menor_valor', 'fechamento']]
    elif response.status_code == 429:
        print(f"Limite de tempo excedido para {cripto_id}. Aguarde antes de tentar novamente...")
        time.sleep(60) 
        return obter_dados_hist_ohlc(cripto_id)  
    else:
        print(f"Erro ao buscar dados para {cripto_id}: {response.status_code}")
        return None

# Função para salvar os dados     
def salvar_dados_ohlc(df, filename='hist_ohlc.csv'):
    raw_data_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data', 'processed')
    file_path = os.path.join(raw_data_path, filename)
    df.to_csv(file_path, index=False)
    print(f"Dados salvos em: {file_path}")   

# Função para inserir dados na tabela 
def inserir_dados_hist_ohlc(connection, df):
    try:
        cursor = connection.cursor()
        for _, row in df.iterrows():
            insert_query = """
            INSERT INTO historicos_ohlc (nome,data,abertura, maior_valor, menor_valor, fechamento)
            VALUES (%s, %s, %s, %s, %s, %s)
            """
            cursor.execute(insert_query, tuple(row))
        connection.commit()
        print("Dados inseridos com sucesso na tabela 'historicos_ohlc'.")
    except Error as e:
        print(f"Erro ao inserir dados na tabela: {e}")

if __name__ == "__main__":

# Executando as funções 
    for moeda in moedas:
            df_precos_hist = obter_dados_hist_ohlc(moeda)
            if df_precos_hist is not None:
                df_historico_ohlc = pd.concat([df_historico_ohlc,df_precos_hist], ignore_index=True)
    conect = conexão_banco()
    if conect:
        salvar_dados_ohlc(df_historico_ohlc)
        inserir_dados_hist_ohlc(conect, df_historico_ohlc)
        conect.close()