from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

# Carregar as variáveis do arquivo .env
load_dotenv()

# Configurações do banco de dados usando as variáveis de ambiente
host = os.getenv("DB_HOST")
port = os.getenv("DB_PORT")
user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
database_name = os.getenv("DB_NAME")

DATABASE_URL = f'mysql+pymysql://{user}:{password}@{host}:{port}/{database_name}'
engine = create_engine(DATABASE_URL)
