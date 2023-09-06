import yaml
from utils import get_pool, load_yaml
from func import Table

if __name__ == "__main__":
    table = Table("mysql", "default_roles")
    print(table.tables, table.show_databases)