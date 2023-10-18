import pymysql, os
from dbutils.pooled_db import PooledDB

def get_pool(connection = pymysql, maxconn = 20, **config_para) -> PooledDB:
    config_para = load_yaml("config.yaml").get("mysql") if len(config_para) == 0 else config_para
    return PooledDB(
        creator = connection,
        maxconnections = maxconn,
        **config_para
    )
