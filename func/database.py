import redis, json, re
import pandas as pd

from typing import Literal

from sqlalchemy import create_engine, URL, text, Connection, Engine, \
    MetaData, Table, Column, case, String, Integer, Text, Float, DateTime, \
    Date
from sqlalchemy.sql import update
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import SQLAlchemyError

from urllib.parse import quote_plus


class Rain_Dis:
    '''
    Create Redis Connection

    Example:
    ------
    >>> cursor = Rain_Dis('localhost', 8080)
    # put key-value: a -- 1 into Redis, set its livetime 2000 seconds
    >>> cursor['a'] = '1', 2000
    >>> cursor['b'] = [2, 4], 1000
    >>> cursor.drop(['a', 'b'])
    '''
    key_types_map = (b'string', b'list', b'hash', b'set')

    def __init__(self, host: str, port: int, db = 0, pw = None, timeout = 5000, encoding = 'utf-8') -> None:
        self.encoding = encoding
        self.cursor = redis.StrictRedis(host, port, db, pw, timeout, encoding = encoding)
        
    @property
    def keys(self):
        return tuple(i.decode(self.encoding) for i in self.cursor.keys('*'))

    def __getitem__(self, key):
        if not self.cursor.exists(key):
            raise KeyError('Can\'t find the Key in Redis!')

        key_type: bytes = self.cursor.type(key)

        match key_type:
            case b'string':
                value: bytes = self.cursor.get(key)
                return value.decode(self.encoding)

            case b'list':
                return tuple(i.decode(self.encoding) for i in self.cursor.lrange(key, 0, -1))

            case b'hash':
                re_dict = {}
                for k, v in self.cursor.hgetall(key).items():
                    key, value = k.decode(self.encoding), v.decode(self.encoding)
                    re_dict[key] = value
                return re_dict

            case b'set':
                return set(i.decode(self.encoding) for i in self.cursor.smembers(key))

    def __setitem__(self, key, value, livetime = None):
        '''
        livetime -> the key-value live seconds
        '''
        match type(value).__name__:
            case 'int' | 'str':
                self.cursor.set(key, value, ex = livetime)
            case 'tuple' | 'list':
                for i in value:
                    self.cursor.lpush(key, i)
            case 'dict':
                for k, v in value.items():
                    if not isinstance(k, str):
                        raise KeyError('Redis can\'t store complexed')
                    self.cursor.hset(key, k, v)

    def drop(self, keys) -> bool:
        if keys not in list(self.keys):
            raise KeyError('Can\'t find the key in Redis!')
        return True if self.cursor.delete(keys) == 1 else False

    def close(self) -> None:
        return self.cursor.close()

class Rain_DB:
    '''
    A Database Cursor Object Based On SqlAlchemy.

    config-url(SqlAlchemy URL): 
    
        `dialect+driver://username:password@host:port/database`

        mysql: `mysql+pymysql://scott:tiger@localhost/foo`

        sqlite: `sqlite:////absolute/path/to/foo.db` (unix)

        oracle: `oracle+cx_oracle://scott:tiger@tnsname`

        postgresql: `postgresql+psycopg2://scott:tiger@localhost/mydatabase`s

    ------
    Parameter:

        `databases` --> return all databases name tuple;

        `tables` ---> return all tablas name tuple;

        `curr_database` --> return current database name;

        `curr_table`[can be set] --> return current table's name;

        `column` --> return current table columns tuple;

        `head` --> return head 5 rows info of current table;

        `dataframe` --> return current table dataframe object;
    '''
    type_mapping = {
        'int': Integer, 'float': Float,
        'str': String, 'object': String, 'varchar': String,
        'text': Text,
        'date': Date, 'datetime': DateTime,
    }

    def __init__(self, config: URL | dict | str, 
                 table_name: str = '',
                 encoding: str = 'utf8') -> None:
        # configuration
        self._curr_database = None
        self.system = ''

        # initialize
        self.conn, self.engine = self._get_conn(config, encoding)
        self._update_metadata()
        
        if table_name in self.tables:
            self._curr_table = table_name
            self.column = tuple(self._metadata.tables[table_name].columns.keys())

    def _get_conn(self, config, encoding) -> tuple[Connection, Engine]:
        '''
        Initialize Connection
        '''
        from sqlalchemy.engine.url import make_url

        if isinstance(config, str) or isinstance(config, URL):
            conn_url = config
        elif isinstance(config, dict):
            if 'password' in config:
                config['password'] = quote_plus(config['password'])
            conn_url = URL.create(**config)
        else:
            raise ValueError('Unsupported Configure type!')
        conn_para = { 'charset': encoding }
        if 'oracle' in conn_url:
            conn_para = {
                "encoding": encoding,
                "nencoding": encoding,
            }
        self.engine = create_engine(conn_url, connect_args = conn_para)

        self.system = self.engine.dialect.name
        self._curr_database = make_url(self.engine.url).database
        return self.engine.connect(), self.engine

    def _update_metadata(self) -> None:
        '''
        Update SqlAlchemy Metadata
        '''
        # Check if the engine and connection are already initialized
        if not self.engine:
            raise RuntimeError("Engine is not initialized, cannot update metadata.")

        # Initialize or update the metadata object
        if not hasattr(self, '_metadata') or self._metadata is None:
            self._metadata = MetaData()

        # Use reflect to populate metadata
        try:
            self._metadata.reflect(bind=self.engine, schema=None, views=True)
        except Exception as e:
            raise RuntimeError(f"Failed to reflect metadata: {e}")

        # Optionally, update the current table information if it's already set
        if hasattr(self, '_curr_table') and self._curr_table in self._metadata.tables:
            self.column = tuple(self._metadata.tables[self._curr_table].columns.keys())

    def _check_cols(self, data: pd.DataFrame | list[dict]) -> bool:
        '''
        Check whether the data column is the child of current table columns
        '''
        if not isinstance(data, list) and not isinstance(data, pd.DataFrame):
            return False

        db_column_set = set(self.column)

        if isinstance(data, list):
            data_column_set = set().union(*(tiny.keys() for tiny in data))
        else:
            data_column_set = set(data.columns)

        return data_column_set <= db_column_set
    
    def _gene_table(self, table_name: str, data: list[dict]) -> Table:
        attrs = {'__tablename__': table_name}
        cols = self.get_table(table_name).dtypes
        for col_name, col_type in cols.to_dict():
            if col_type not in self.type_mapping:
                raise TypeError(f'column {col_name} {col_type} not matched!')
            col_type = self.type_mapping.get(col_type)
            attrs[col_name] = Column(col_type)
        
        model_table = type(table_name.capitalize(), (declarative_base(),), attrs)
        print(type(model_table))
        return model_table
    
    @property
    def curr_table(self):
        if not hasattr(self, '_curr_table'):
            return 'Need Select Table First!'
        return self._curr_table
    
    @curr_table.setter
    def curr_table(self, table_name: str):
        self._update_metadata()
        if table_name in self.tables:
            self._curr_table = table_name
            self.column = tuple(self._metadata.tables[table_name].columns.keys())
            return
        raise KeyError("Can't find the target table in current databse;")

    @property
    def curr_database(self):
        return self._curr_database

    @property
    def databases(self):
        return tuple(i[0] for i in self.exec('show databases')[1:])
    
    @property
    def tables(self):
        return tuple(i for i in self._metadata.tables)
    
    @property
    def head(self) -> pd.DataFrame:
        data = self.exec(f'select * from {self.curr_table} limit 5;')
        return pd.DataFrame(data[1:], columns = self.column)

    @property
    def dataframe(self) -> pd.DataFrame:
        data = self.exec(f'select * from {self.curr_table}')
        return pd.DataFrame(data[1:], columns = self.column)

    def close(self):
        self.conn.close()
        self.engine.dispose()

    def commit(self):
        return self.conn.commit()

    def get_table(self, 
        table_name: str = '', 
        need_type: Literal['dataframe', 'table'] = 'dataframe'
        ) -> pd.DataFrame | Table:
        '''
        Get Target Table Object.
        '''
        if table_name != '' and table_name not in self.tables:
            raise ValueError(f'Can\'t Found {table_name} In Current Database.')

        if table_name == '': table_name = self.curr_table

        if need_type == 'dataframe':
            data = self.exec(f'select * from {self.curr_database}.{table_name}')

            if data is None:
                return 
            
            cols = tuple(self._metadata.tables[table_name].columns.keys())
            return pd.DataFrame(data[1:], columns = cols)

        elif need_type == 'table':
            return Table(table_name, self._metadata)

    def exec(self, sql: str, trans_df: bool = False, commit: bool = False) -> list | pd.DataFrame | bool:
        '''
        Execute SQL Query

        Param:
        ----
            `sql` -- the sql query sentence;

            `trans_df` -- whether transmit the result to DataFrame;

        Example:
        ----
            >>> cursor.exec('select name, age from person_info limit 3;')
                [['name', 'age'], ['Alice', '16'], ['Bob', '12'], ['Criss', '25'],]
            
            >>> df = cursor.exec('select name, age from person_info limit 3;', True)
            >>> df
                name   age
            0   Alice  16
            1   Bob    12
            2   Criss  25

        '''
        no_result_keywords = ('create table', 'insert into', 'use')
        try:
            result = self.conn.execute(text(sql))
            final_info = False

            # Check if the SQL statement produces results
            if any(keyword in sql.lower() for keyword in no_result_keywords):
                final_info = True
                self._update_metadata()
                
            else:
                column_names = result.keys()
                data = [tuple(column_names)] + [tuple(row) for row in result]
                final_info = pd.DataFrame(data[1:], columns = data[0]) if trans_df else data
                
            self.commit() if commit else ...

            return final_info

        except Exception as e:
            print(f'Running SQL Code:{sql}\n', f'Error Info: {e}')

            if any(i in sql for i in no_result_keywords):
                return False
    
    def create_table(self, table_name: str, columns: dict) -> bool:
        '''
        Create New Table in current Database

        With automatically creating `id` column as the primary key.
        '''
        assert table_name not in self.tables, f"Found Existed Table!"

        meta = MetaData(); col_list = [Column('id', Integer, primary_key=True, autoincrement=True)]
        type_pattern = re.compile(r'(\w+)\((\d+)\)')

        for col_name, col_type in columns.items():
            # abstract and update limit length for varchar or string
            limit_len = None
            if (match := type_pattern.match(col_type)) is not None:
                col_type, limit_len = match.groups()
            
            # parse column info and add to table
            if col_type in self.type_mapping:
                col_type = self.type_mapping[col_type]
                if limit_len is not None:
                    col_type = col_type(length = int(limit_len))
                col_list.append(Column(col_name, col_type))
            else:
                print(f"Error parsing column type for '{col_name}': {col_type}")
                continue

        # create table
        try:
            table = Table(table_name, meta, *col_list)
            meta.create_all(self.conn)
            self.curr_table = table_name
            return True
        except Exception as e:
            print(e); return False

    def insert(self, data: list[dict] | pd.DataFrame, table_name: str = '',) -> tuple[bool, str]:
        '''
        Insert Data Into Existed Table

        Attention:

            When checking existing data, replacement will be performed;
        '''
        if not self._check_cols(data):
            return False, 'Check Column Error'
        elif table_name != '' and table_name not in self.tables:
            return False, 'Check Unexisted Table'

        if table_name == '':
            table_name = self.curr_table

        if isinstance(data, pd.DataFrame):
            data.to_sql(table_name, self.engine, if_exists = 'append', index = False)
        elif isinstance(data, dict):
            pre_table = self.curr_table
            self.curr_table = table_name
        return True, ''

    def update(self, 
        data: list[dict] | pd.DataFrame, 
        primary_key: str,
        table_name: str = '', ) -> bool:
        # Create SQLAlchemy metadata and reflect the existing table structure
        if table_name == '': 
            table_name = self.curr_table
        assert table_name not in self.tables, f'Nonexistent Table {table_name}'

        table = Table(table_name, MetaData(bind = self.engine), autoload_with = self.engine)

        if isinstance(data, pd.DataFrame):
            data = data.to_dict(orient = 'records')
        data_dict = {row[primary_key]: row for row in data}
        
        case_conditions = {
            col: case(
                [(table.c[primary_key] == key, row[col]) for key, row in data_dict.items()],
                else_ = table.c[col]
            )
            for col in data_dict[next(iter(data_dict))] if col != primary_key
        }
        
        stmt = (
            update(table)
            .values(case_conditions)
            .where(table.c[primary_key].in_(data_dict.keys()))
        )
        
        try:
            # Execute the update statement
            with self.engine.connect() as conn:
                conn.execute(stmt)
                conn.commit()
            return True
        except SQLAlchemyError as e:
            print(f"Error updating data: {e}")
            return False
        

if __name__ == '__main__':
    
    conn = Rain_DB('mysql+pymysql://root:123456@localhost:3306/record_via')
    # conn = Rain_DB('mysql+pymysql://yym:yym123456@192.168.3.12:3306/nsyy_yym')
    conn.curr_table = '病患信息'
    # print(conn.databases)
    # print(conn.curr_database)
    # print(conn.curr_table)
    # print(conn.column)
    # print(conn.head)
    # print(conn.dataframe)
    # conn.curr_table = '转出记录'
    print(conn._gene_table('病患信息', []))
    # conn.insert([
    #     {'name': '123', 'gender': 1, 'event': '1234123'},
    #     {'name': '321', 'gender': 0, 'event': '21123'},
    # ])
    # conn.create_table('temp', {
    #     'name': 'varchar(255)',
    #     'gender': 'int',
    #     'event': 'text',
    # })

    conn.close()