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

def get_pool(connection = pymysql, maxconn = 20, **config_para) -> PooledDB:
    config_para = load_yaml("config.yaml").get("mysql") if len(config_para) == 0 else config_para
    return PooledDB(
        creator = connection,
        maxconnections = maxconn,
        **config_para
    )