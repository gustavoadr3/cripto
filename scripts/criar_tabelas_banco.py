from conectar_banco import conexão_banco
from mysql.connector import connect, Error


# Criar tabela hist OHLC 
def criar_tabela_historico_ohlc(connection):
    try:
        cursor = connection.cursor()
        tabela_sql = """
        CREATE TABLE IF NOT EXISTS historicos_ohlc (
            nome VARCHAR(100),
            data DATE,
            abertura DECIMAL(18, 8),
            maior_valor DECIMAL(18, 8),
            menor_valor DECIMAL(18, 8),
            fechamento DECIMAL(18, 8)
        )
        """
        cursor.execute(tabela_sql)
        connection.commit()
        print("Tabela 'historicos_ohlc' criada ou já existe.")
    except Error as e:
        print(f"Erro ao criar a tabela: {e}")
    finally:
        if cursor:
            cursor.close()
            
            
# Criar tabela valores históricos
def criar_tabela_historico(connection):
    try:
        cursor = connection.cursor()
        tabela_sql = """
        CREATE TABLE IF NOT EXISTS valores_historicos (
            nome VARCHAR(100),
            data DATE,
            preco DECIMAL(18, 8)
        )
        """
        cursor.execute(tabela_sql)
        connection.commit()
        print("Tabela 'valores_historicos' criada ou já existe.")
    except Error as e:
        print(f"Erro ao criar a tabela: {e}")
    finally:
        if cursor:
            cursor.close()         
            
# Criar tabela criptos
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
            
# Executando funções 
conect = conexão_banco()
if conect:
    criar_tabela_historico_ohlc(conect)
    criar_tabela_historico(conect)
    criar_tabela_top10(conect)