import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from mysql.connector import connect, Error
from config.db_config import DB_CONFIG


def conexão_banco():
    try:
        connection = connect(
            host=DB_CONFIG['host'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password'],
            database=DB_CONFIG['database'],
            port=DB_CONFIG['port']
        )
        print("Conexão bem-sucedida!")
        return connection
    except Error as e:
        print(f"Erro ao conectar ao banco de dados: {e}")
        return None

if __name__ == "__main__":
    conn = conexão_banco()
    if conn:
        conn.close()