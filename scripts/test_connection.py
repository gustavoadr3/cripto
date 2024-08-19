import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from mysql.connector import connect
from config.db_config import DB_CONFIG

def test_connection():
    connection = None
    try:
        # Estabelecer a conexão 
        connection = connect(
            host=DB_CONFIG['host'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password'],
            database=DB_CONFIG['database'],
            port=DB_CONFIG['port']
        )
        print("Conexão bem-sucedida!")
        return connection  # Retorna a conexão para uso posterior
    except Exception as e:
        print(f"Erro ao conectar ao banco de dados: {e}")
        return None

def create_table(connection):
    cursor = None
    try:
        cursor = connection.cursor()
        
        # Verificar se a tabela já existe
        cursor.execute("""
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema = %s
        AND table_name = %s
        """, (DB_CONFIG['database'], 'crypto_prices'))
        
        table_exists = cursor.fetchone()[0] > 0
        
        if table_exists:
            print("A tabela 'crypto_prices' já existe.")
        else:
            # Criar a tabela se não existir
            create_table_query = """
            CREATE TABLE crypto_prices (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(100),
                symbol VARCHAR(10),
                current_price DECIMAL(10, 2),
                market_cap BIGINT,
                total_volume BIGINT,
                price_change_percentage_24h DECIMAL(5, 2),
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
            cursor.execute(create_table_query)
            print("Tabela 'crypto_prices' criada com sucesso.")
    except Exception as e:
        print(f"Erro ao verificar/criar a tabela: {e}")
    finally:
        if cursor:
            cursor.close()  # Fechar o cursor se ele foi criado

if __name__ == "__main__":
    conn = test_connection()
    if conn:
        create_table(conn)
        conn.close()  # Fechar a conexão depois de criar a tabela
