import os
import sys
import pandas as pd
# Adiciona o caminho para a pasta 'scripts' no sys.path
sys.path.append(os.path.abspath('scripts'))

from top_10_criptos import top_10_criptos

df = pd.read_csv(r'C:\Users\gusta\Desktop\Estudos\Projeto\data\raw\top_10_criptos.csv')

df = df.drop(['id', 'name'], axis=1)
print(df.head())