from dbutils.persistent_db import PersistentDB
import sys
sys.path.append("../util")
import os
from utils import get_pool
import traceback
from functools import wraps
import pandas as pd

def exec(connection: PersistentDB.connection,
         sql:str) ->str:
    cursor = connection.cursor()
    try:
        cursor.execute(sql)
        re_info = cursor.fetchall()
    except:
        re_info = traceback.format_exc() + sql
        raise Exception(re_info)
    return re_info
'''
    some note from fluent python 2rd edi:
    Humans use text. Computers speak bytes.
        —Esther Nam and Travis Fischer, Character Encoding and Unicode in Python
'''
# format decorator
def formatter(func: object):
    '''
        decorate the return SQL query string format,
        change tuple to list.
    '''
    @wraps(func)
    def inner(*args, **kwargs) -> list:
        re_tuple = func(*args, **kwargs)
        format_list = []
        info_index = [('Field', 'Type', 'NuLL', 'Key', 'Default', 'Extra')]
        if re_tuple is None:
            return None
        for i in re_tuple:
            if "Traceback" in re_tuple:
                return re_tuple
            if isinstance(i, tuple) and len(i) == 1:
                format_list.append(i[0]); continue
            format_list.append(i)
        if func.__name__ == 'info':
            return info_index + format_list
        else:
            return format_list
    return inner

# SQL Part
@formatter
def show_databases(conn: PersistentDB.connection) -> tuple:
    return exec(conn, "show databases;")

@formatter
def show_database(conn):
    return exec(conn, "select database();")

@formatter
def show_tables(conn, database) -> tuple[tuple]:
    return exec(conn, f"\
                select table_name from information_schema.tables \
                where table_schema = '{database}' and table_type = 'BASE TABLE';")

@formatter
def show_columns(conn: PersistentDB.connection, table) -> tuple[str]:
    # exec(conn, f"select column_name from information_schema.columns where table_schema = '{database}' and table_name = '{table}';")
    cursor = conn.cursor()
    cursor.execute(f"select * from {table};")
    return [column[0] for column in cursor.description]

@formatter
def select_database(conn, database = '') -> list[tuple]:
    return exec(conn, f"use {database};")

@formatter
def insert_table(conn, table, columns, data) -> None:
    # print(f"insert ignore into {table} ({columns}) values ({data});")
    return exec(conn, f"insert ignore into {table} ({columns}) values ({data});")

@formatter
def update_table(conn, table, update_string, condition) -> None:
    return exec(conn, f"update {table} set {update_string} where {condition};")

@formatter
def create_table(conn, table, columns):
    return exec(conn, "create table if not exists %s (%s);"  % (table, columns))

@formatter
def condition_query(conn, table, columns, condition) -> list[str]:
    if condition is None:
        return exec(conn, "select %s from %s;"\
            % (columns, table))
    return exec(conn, "select %s from %s where %s;"\
            % (columns, table, condition))

@formatter
def create_column(conn, table, columns) -> list | None:
    return exec(conn, f"alter table {table} add {columns};")

def delete_data(conn, table, condition):
    return exec(conn, f"delete from {table} where {condition};")

def delete_column(conn, table, column):
    return exec(conn, f"alter table {table} drop column {column};")

@formatter
def head(conn, table, limited = 5) -> list[str]:
    if limited is None:
        return exec(conn, f"select * from {table};")
    return exec(conn, f"select * from {table} limit {limited};")

class Table:
    '''
        Simulate SQL Table Object.
        ---------
            Default use mysql engine when initialization, its instance will get a new connection from sql_config file's dbutils DataBaseConnectionPool.
        
        Parameters:
        ---------
            `database` & `table_name` -> database and table that wanna operate;

            `connection` -> database connection object(dafault: `PersistentDB.connection` from `sql_config`);

            `columns` -> initialtable columns, just used in initial creating table, like sql format: `alter table table_name add column_1 type_1, column_2 type_2;` defalut `null string`, usually should be default.
        
        Arguments:
        --------
            Functional Argument:

                `to_dataframe` -> transform current SQL table to `pandas.DataFrame`;

                `to_csv` -> transform current SQL table to csv file base on pandas, and solve it as 'table_name.csv' in current folder path; 

                `commit` -> commit current SQL query to server;

                `close` -> close current SQL connection;

                `clear` -> clear current table info;(careful with it!!!)

            Informative Argument:

                `show_databases` -> show all databases in SQL server;

                `current` -> show current database;

                `tables` -> show current database's table list;
                
                `columns` -> show current table's column list;
                
                `info` -> show current table's structure info;

                `head` -> show first 5 row information in current table;

        Others:
        --------
            When operating the column target, only deal with one column once time.

            self.columns will execute a sql query to get the current table's column dynamicly , when this property is called frequently, may slow down the process's efficiency.
    '''
    # Hinting: database has been designated (sql_config file);

    def __init__(
            self,
            database: str = '',
            table_name: str = '',
            columns: str = '',
            connection: PersistentDB.connection = get_pool().connection(),
            ):
        self.conn = connection
        self.database = database
        self.table_name = table_name
        self._static_tables = self.tables

        # change current database
        if database != '':
            select_database(self.conn, self.database)
        
        if table_name != '' and table_name not in self.tables:
            self.use_table(table_name, True, cols = columns)
            self.commit
        
    @property
    def commit(self):
        try:
            self.conn.commit()
        except:
            print(traceback.print_exc())
            self.conn.rollback()

    @property
    def tables(self):
        '''return all table name in current database point;'''
        return show_tables(self.conn, self.database)
    
    @property
    def show_databases(self):
        return show_databases(self.conn)

    @property
    def current(self):
        return show_database(self.conn)
    
    @property
    def columns(self):
        if self.table_name == '':
            raise ValueError("need select one table first, use `use_table` method.")
        return show_columns(self.conn, self.table_name)
    
    @property
    def clear(self):
        if input("Sure about clearing the table? Y/N") == 'Y':
            return exec(f"truncate table {self.table_name};")
    
    @property
    def head(self):
        return head(self.conn, self.table_name)
    
    @property
    def to_dataframe(self) -> pd.DataFrame:
        columns =  self.columns
        data = head(self.conn, self.table_name, limited = None)
        return pd.DataFrame(data = data, columns = columns)

    @property
    def to_csv(self) -> None:
        '''
            save current table as `table_name.csv in current folder`
        '''
        file_name = os.path.join(os.getcwd(), f'{self.table_name}.csv')
        df = self.to_dataframe
        df.to_csv(file_name)

    @property
    @formatter
    def info(self):
        return exec(self.conn, f"desc {self.table_name};")
    
    @property
    def close(self):
        self.conn.close()

    def insert(self, col: list[str], value: list[str]):
        '''
            insert data into specific table. will skip insert operation when check duplicated data.
        '''
        #preprocess the sql
        if len(col) != len(value):
            raise ValueError("mismatched number of columns and value.")
        if self.table_name == '':
            raise ValueError("need select one table first, use `use_table` method.")
        col = ', '.join(col); value = ', '.join(value)
        temp_info = insert_table(self.conn, self.table_name, col, value)
        if "Traceback" in temp_info:
            raise Exception(temp_info)

    def query(self, cols: str = '*', condition: str = None) -> pd.DataFrame:
        if self.table_name == '':
            raise ValueError("need select one table first, use `use_table` method.")
        columns = self.columns if cols == '*' else cols.split(',')
        return pd.DataFrame(
                condition_query(self.conn, self.table_name, cols, condition), 
                columns = columns)
    
    def full_query(
            self, 
            condition: str, 
            select_tables: str = None,
            filter_none = True,
            corr_column = "file_name"
            ) -> dict[str: pd.DataFrame]:
        '''
            Do full condition query for all `select_table` in current database.

            `condition` -> the sql query condition. 

                e.g: "column1 = 'string1' and column2 = 'string2'"
            
            `select_table` -> the selected tables(split by blank), default `None`.

                e.g: "table_name1 table_name2 table_name3"

            `filter_none` -> whether to filter null value, default `True`.

            `corr_column` -> table join primary key, should exist in every table. default equals `file_name`(Important!).

        '''
        corr_value = condition_query(self.conn, self.table_name, corr_column, condition)[0]
        cor_condition = f"{corr_column} = '{corr_value}'"
        if select_tables is None:
            tables = self.tables
        else:
            select_tables = select_tables.split(' ')
            tables = [t for t in self.tables if t in select_tables]
        results = {}
        for table in tables:
            self.use_table(table, dynamic = False)
            value = condition_query(self.conn, self.table_name, '*', cor_condition)
            column = self.columns
            if filter_none:
                if value != []:
                    results[table] = pd.DataFrame(value, columns = column)
            else:
                results[table] = pd.DataFrame(value, columns = column) \
                    if value != [] else None
        return results
    
    def delete(self, condition: str):
        '''
            if condition is a column name, will try to delete column from table first;
        '''
        if condition in self.columns:
            return delete_column(self.conn, self.table_name, condition)
        return delete_data(self.conn, self.table_name, condition)
    
    def add_columns(self, cols: list[str], types: list[str]):
        current_columns = self.columns
        for i in range(len(cols)):
            if cols[i] not in current_columns:
                create_column(self.conn, self.table_name, f"{cols[i]} {types[i]}")
    
    def get_type(self, col: str, val: str = None) -> tuple[str, str]:
        '''
            Get the column's type from SQL table and return column type info and formatted value.

            `val`: the value needed to be formatted, default `None`.

            here just use to format value when update data into SQL.
        '''
        if col not in self.columns:
            raise ValueError(f"Unexisted column:{col}")
        sql = "\
            select data_type from information_schema.columns \
                where table_name = '%s' and column_name = '%s'" % (self.table_name, col)
        col_type = exec(self.conn, sql)[0][0]
        if val is not None:
            if col_type in ('datetime', 'time'):
                val = time_format(val)
            elif col_type == 'int':
                val = val
            else:
                val = "'"+val+"'"
        return col_type, val
    
    def update(
            self, 
            cols: list[str], 
            values: list[str],
            condition: str):
        '''
            `condition` -> style: `"column_n = value_n"`

            prefer retrining the row format of the condition.
        '''
        if condition == '' or '=' not in condition:
            raise ValueError(f"Invalid constraint condition: {condition}")
        if len(cols) != len(values):
            raise Exception("the length of column and values should stay same.")
        if (temp_set := set(cols) - set(self.columns)) != set():
            temp_set = ', '.join(temp_set)
            raise ValueError(f"Have checked unexist column: {temp_set}.")
        update_string = []
        for col, val in zip(cols, values):
            _, val = self.get_type(col, val)
            update_string.append(col + '=' + val)
        update_string = ', '.join(update_string)
        update_table(self.conn, self.table_name, update_string, condition)
        self.commit
        
    def use(self, database: str):
        self.database = database
        return select_database(self.conn, database)
    
    def use_table(
            self, 
            table: str, 
            created = False, 
            cols: str | dict = None,
            dynamic = True) -> bool | str:
        '''
            Return True when found existed table or Return string when created a new table;

            Parameter:

                `cols` -> "column_name1 sql_type1, column_name2 sql_type2, column_name3 sql_type3"
                        
                       -> {'column_name1': 'sql_type1', 'column_name2': 'sql_type2'};

                `dynamic` -> If `True`, will compare `table`(table name) with the table name list in current status, else will compare `table` with the table name list at initialization, dfault `Ture`, 
            
        '''
        if (dynamic is False and table in self._static_tables)\
            or (dynamic is True and table in self.tables):
            self.table_name = table
            return True
        
        elif created is True and table != '':
            self.table_name = table
            if cols is None:
                create_table(
                    self.conn, 
                    self.table_name, 
                    "id int primary key, first_name varchar(50), last_name varchar(50), salary float"
                    )
            else:
                if isinstance(cols, dict):
                    cols = ", ".join([f"{key} {value}" for key, value in cols.items()])
                create_table(
                    self.conn, 
                    self.table_name, 
                    cols)
            self.commit
            return f"Have create table {table} with {cols}."
        raise ValueError(
            f"Can't find {table} in table list and the parameter `created` is False."
            )

class Info:

    ''' Simulate pamdas.DataFrame and recombinate data'''

    def __init__(self, data: dict[str, pd.DataFrame]):
        for table_name, df in data.items():
            if not hasattr(self, table_name):
                setattr(self, table_name, df)
            else:
                raise ValueError(f"Invalid column name: {table_name}.")
        self._tables = [i for i in data.keys()]
    
    @property
    def tables(self):
        return self._tables
    
