import sys
import os
from mysql.connector import Error
from conectar_banco import conexão_banco

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def create_tables():
    try:
        # Obter a conexão do banco de dados
        connection = conexão_banco()
        
        if connection:
            cursor = connection.cursor()

            # SQL para criar a tabela 'moedas'
            moedas = """
            CREATE TABLE IF NOT EXISTS moedas (
                id INT AUTO_INCREMENT PRIMARY KEY,
                nome VARCHAR(100) NOT NULL,
                simbolo VARCHAR(10) NOT NULL,
                descricao TEXT,
                data_criacao DATE
            )
            """
            cursor.execute(moedas)

            # SQL para criar a tabela 'precos_historicos'
            precos_historicos = """
            CREATE TABLE IF NOT EXISTS precos_historicos (
                id INT AUTO_INCREMENT PRIMARY KEY,
                moeda_id INT,
                data DATE,
                preco DECIMAL(18, 8),
                volume DECIMAL(18, 8),
                market_cap DECIMAL(18, 8),
                FOREIGN KEY (moeda_id) REFERENCES moedas(id)
            )
            """
            cursor.execute(precos_historicos)

            # SQL para criar a tabela 'metricas_atualizadas'
            metricas_atualizadas = """
            CREATE TABLE IF NOT EXISTS metricas_atualizadas (
                id INT AUTO_INCREMENT PRIMARY KEY,
                moeda_id INT,
                data DATE,
                preco_atual DECIMAL(18, 8),
                variacao_24h DECIMAL(5, 2),
                volume_24h DECIMAL(18, 8),
                market_cap DECIMAL(18, 8),
                FOREIGN KEY (moeda_id) REFERENCES moedas(id)
            )
            """
            cursor.execute(metricas_atualizadas)

            # Confirmar as mudanças
            connection.commit()
            print("Tabelas criadas com sucesso.")
    
    except Error as e:
        print(f"Erro ao criar tabelas: {e}")
    
    finally:
        if connection:
            cursor.close()
            connection.close()

if __name__ == "__main__":
    create_tables()
