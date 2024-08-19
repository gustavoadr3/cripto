import requests

def buscar_top_10_cripto():
    # URL da API CoinGecko para as 10 maiores moedas
    url = 'https://api.coingecko.com/api/v3/coins/markets'
    
    # Parâmetros da requisição
    params = {
        'vs_currency': 'usd',
        'order': 'market_cap_desc',
        'per_page': 10,
        'page': 1
    }
    
    # Fazer a requisição para a API
    response = requests.get(url, params=params)
    
    # Verificar se a requisição foi bem-sucedida
    if response.status_code == 200:
        return response.json()  # Retorna os dados no formato JSON
    else:
        print(f"Erro ao buscar dados da API: {response.status_code}")
        return None

if __name__ == "__main__":
    # Buscar os dados das 10 maiores criptomoedas
    dados_cripto = buscar_top_10_cripto()
    
    if dados_cripto:
        # Exibir os dados
        for moeda in dados_cripto:
            print(f"Nome: {moeda['name']}, Símbolo: {moeda['symbol']}, Preço: {moeda['current_price']}, "
                  f"Capitalização de Mercado: {moeda['market_cap']}, Volume: {moeda['total_volume']}, "
                  f"Variação 24h: {moeda['price_change_percentage_24h']}%")
