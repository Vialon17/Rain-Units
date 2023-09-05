import yaml
from utils import create_pool
from utils.sql_config import create_db_connection

if __name__ == "__main__":
    pool = create_pool(create_db_connection)
    pass