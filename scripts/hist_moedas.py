import os
import requests
import pandas as pd
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import time
from conectar_banco import conexão_banco
from mysql.connector import connect, Error

df_id_moedas = pd.read_csv(r'data/processed/top_10_criptos.csv')
df_historico = pd.DataFrame()

# Função para extrair os historicos das moedas
def hist_moedas(cripto_id, vs_currency='usd', interval='daily'):
    url = f'https://api.coingecko.com/api/v3/coins/{cripto_id}/market_chart'
    params = {
        'vs_currency': vs_currency,
        'days': 365,  
        'interval': interval
    }
    response = requests.get(url, params=params)
    if response.status_code == 200:
        data = response.json()
        df_prices = pd.DataFrame(data['prices'], columns=['timestamp', 'price'])
        df_prices['date'] = pd.to_datetime(df_prices['timestamp'], unit='ms').dt.date
        df_prices['id_moeda'] = cripto_id
        return df_prices[['id_moeda', 'date', 'price']]
    elif response.status_code == 429:
        print(f"Limite de tempo excedido para {cripto_id}. Aguarde antes de tentar novamente...")
        time.sleep(15)  
        return hist_moedas(cripto_id)  
    else:
        print(f"Erro ao buscar dados históricos para {cripto_id}: {response.status_code}")
        return None
    
# Função para puxar os nomes das moedas
def nomes_criptos(df):
    return df['id'].tolist()  

# Função para salvar os dados
def salvar_dados_historico(df, filename='hist_criptos.csv'):
    raw_data_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data', 'processed')
    file_path = os.path.join(raw_data_path, filename)
    df.to_csv(file_path, index=False)
    print(f"Dados salvos em: {file_path}")

# Função para inserir dados na tabela 
def inserir_dados_hist(connection, df):
    try:
        cursor = connection.cursor()
        for _, row in df.iterrows():
            insert_query = """
            INSERT INTO valores_historicos (nome,data,preco)
            VALUES (%s, %s, %s)
            """
            cursor.execute(insert_query, tuple(row))
        connection.commit()
        print("Dados inseridos com sucesso na tabela 'valores_historicos'.")
    except Error as e:
        print(f"Erro ao inserir dados na tabela: {e}")

if __name__ == "__main__":

    # Executando as funções
    moedas = nomes_criptos(df_id_moedas)
    for moeda in moedas:
        df_precos_hist = hist_moedas(moeda)
        if df_precos_hist is not None:
            df_historico = pd.concat([df_historico,df_precos_hist], ignore_index=True)
    conect = conexão_banco()
    if conect:
        salvar_dados_historico(df_historico)
        inserir_dados_hist(conect, df_historico)
    conect.close()


    


