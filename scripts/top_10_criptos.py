import os
import requests
import pandas as pd
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from conectar_banco import conexão_banco


# Função para extrair os dados da API
def top_10_criptos():
    url = 'https://api.coingecko.com/api/v3/coins/markets'
    params = {
        'vs_currency': 'usd',
        'order': 'market_cap_desc',
        'per_page': 10,
        'page': 1
    }

    response = requests.get(url, params=params)
    if response.status_code == 200:
        data = response.json()
        return pd.DataFrame(data)
    else:
        print(f"Erro ao buscar dados da API: {response.status_code}")
        return None

# Função para salvar os dados brutos 
def salvar_dados_brutos_top10(df, filename='top_10_criptos.csv'):
    raw_data_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data', 'raw')
    file_path = os.path.join(raw_data_path, filename)
    df.to_csv(file_path, index=False)
    print(f"Dados brutos salvos em: {file_path}")

# Função para tratar os dados
def tratar_dados_top10(df):
    df = df[['symbol', 'name', 'current_price', 'market_cap', 'market_cap_rank', 'total_volume', 'high_24h','low_24h', 'price_change_24h', 'price_change_percentage_24h', 'ath', 'ath_date', 'atl', 'atl_date','last_updated']]
    df[['symbol', 'name']] = df[['symbol', 'name']].astype('string')
    df['symbol'] = df['symbol'].str.upper()
    df['ath_date'] = pd.to_datetime(df['ath_date']).dt.date
    df['atl_date'] = pd.to_datetime(df['atl_date']).dt.date
    df['last_updated'] = pd.to_datetime(df['last_updated']).dt.date
    df.columns = ['sigla', 'nome', 'preco_atual', 'capitalizacao_mercado', 'posicao_mercado', 'volume_total', 'preco_max_24h', 'preco_min_24h', 'variacao_preco_24h', 'percentual_variacao_24h', 'max_historico', 'data_max_historico', 'min_historico', 'data_min_historico', 'ultima_atualizacao']
    return df


# Função para salvar os dados tratados 
def salvar_dados_tratados_top10(df, filename='top_10_criptos.csv'):
    dados_processados = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data', 'processed')
    file_path = os.path.join(dados_processados, filename)
    df.to_csv(file_path, index=False)
    print(f"Dados tratados salvos em: {file_path}")

# Função para criar tabela no banco 
def criar_tabela_top10(connection):
    try:
        cursor = connection.cursor()
        tabela_sql = """
        CREATE TABLE IF NOT EXISTS criptos (
            sigla VARCHAR(10),
            nome VARCHAR(100),
            preco_atual DECIMAL(18, 8),
            capitalizacao_mercado DECIMAL(18, 2),
            posicao_mercado INT,
            volume_total DECIMAL(18, 2),
            preco_max_24h DECIMAL(18, 8),
            preco_min_24h DECIMAL(18, 8),
            variacao_preco_24h DECIMAL(18, 8),
            percentual_variacao_24h DECIMAL(18, 2),
            max_historico DECIMAL(18, 8),
            data_max_historico DATE,
            min_historico DECIMAL(18, 8),
            data_min_historico DATE,
            ultima_atualizacao DATE
        )
        """
        cursor.execute(tabela_sql)
        connection.commit()
        print("Tabela 'criptos' criada ou já existe.")
    except Error as e:
        print(f"Erro ao criar a tabela: {e}")
    finally:
        if cursor:
            cursor.close()

# Função para escrever os dados na tabela 
def inserir_dados_criptos(connection, df):
    try:
        cursor = connection.cursor()
        for _, row in df.iterrows():
            insert_query = """
            INSERT INTO criptos (sigla, nome, preco_atual, capitalizacao_mercado, posicao_mercado,
                                volume_total, preco_max_24h, preco_min_24h, variacao_preco_24h,
                                percentual_variacao_24h, max_historico, data_max_historico, 
                                min_historico, data_min_historico, ultima_atualizacao)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(insert_query, tuple(row))
        connection.commit()
        print("Dados inseridos com sucesso na tabela 'criptos'.")
    except Error as e:
        print(f"Erro ao inserir dados na tabela: {e}")
if __name__ == "__main__":

# Executando as funções
    conect = conexão_banco()
    if conect:
        criar_tabela_top10(conect)
    df_criptos = top_10_criptos()
    if df_criptos is not None:
        salvar_dados_brutos_top10(df_criptos)
        df_tratado = tratar_dados_top10(df_criptos)
        salvar_dados_tratados_top10(df_tratado)
        inserir_dados_criptos(conect, df_tratado)
    conect.close()
