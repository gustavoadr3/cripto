import os
import pandas as pd
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
df_moedas = pd.read_csv(r'C:\Users\gusta\Desktop\Estudos\Projeto\data\processed\top_10_criptos.csv')

# Função para extrair o ID da moeda
def nomes_criptos(df):
    return df['id'].tolist()

# Salvar o ID da moeda
def salvar_id(df):
    df = pd.DataFrame(df, columns=['id'])
    df.to_csv(r'C:\Users\gusta\Desktop\Estudos\Projeto\data\processed\id_moedas.csv', index=False)
    print('ID da moeda salvo com sucesso!')

if __name__ == "__main__":
    # Executando a função
    moedas = nomes_criptos(df_moedas)
    salvar_id(moedas)
