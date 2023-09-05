import pymysql
from dbutils.pooled_db import PooledDB
import yaml
import os

def load_yaml(
        file_name: str, 
        encoding = "utf-8", 
        yaml_loader = yaml.FullLoader) -> dict:
    if not os.path.isfile(file_name):
        raise ValueError("Invalid file name.")
    with open(file_name, 'r', encoding = encoding) as f:
        re_dict = yaml.load(f, Loader = yaml_loader)
    return re_dict

def create_db_connection(config: dict | None):
    if config is None:
        config = load_yaml("config.yaml")["mysql"]
    return pymysql.connect(
        **config
    )

def create_pool(connection, maxconn = 20, **other_para) -> PooledDB:
    return PooledDB(
        creator = connection,
        maxconnections = maxconn
        **other_para
    )