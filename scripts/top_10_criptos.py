import os
import requests
import pandas as pd

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
def dados_brutos_top10(df, filename='top_10_criptos.csv'):
    raw_data_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data', 'raw')
    file_path = os.path.join(raw_data_path, filename)
    df.to_csv(file_path, index=False)
    print(f"Dados brutos salvos em: {file_path}")
if __name__ == "__main__":

# Extraindo e salvando os dados brutos    
    df_criptos = top_10_criptos()
    if df_criptos is not None:
        dados_brutos_top10(df_criptos)
        print("Dados brutos extraidos e salvos com sucesso.")
