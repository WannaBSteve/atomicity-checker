from typing import List, Dict, Union, Optional
from enum import Enum
import random
import pymysql


# 模拟MySQLDataType枚举，定义MySQL常见的数据类型
class MySQLDataType(Enum):
    INT = "INT"
    VARCHAR = "VARCHAR"
    FLOAT = "FLOAT"
    DOUBLE = "DOUBLE"
    DECIMAL = "DECIMAL"

    @classmethod
    def get_random(cls, uses_pqs: bool):
        if uses_pqs:
            return random.choice([cls.INT, cls.VARCHAR])
        return random.choice(list(cls))

    def is_numeric(self):
        return self in [self.INT, self.DOUBLE, self.FLOAT, self.DECIMAL]


# 模拟CollateSequence枚举（原Java代码中MySQLColumn内部的枚举）
class CollateSequence(Enum):
    NOCASE = "NOCASE"
    RTRIM = "RTRIM"
    BINARY = "BINARY"

    @classmethod
    def random(cls):
        return random.choice(list(cls))


# 模拟MySQLEngine枚举（原Java代码中MySQLTable内部的枚举）
class MySQLEngine(Enum):
    INNO_DB = "InnoDB"
    MY_ISAM = "MyISAM"
    MEMORY = "MEMORY"
    HEAP = "HEAP"
    CSV = "CSV"
    MERGE = "MERGE"
    ARCHIVE = "ARCHIVE"
    FEDERATED = "FEDERATED"

    @classmethod
    def get(cls, val: str):
        for engine in cls:
            if engine.value == val:
                return engine
        raise ValueError(f"Invalid engine value: {val}")


# 模拟表中的列信息
class MySQLColumn:
    def __init__(self, name: str, column_type: MySQLDataType, is_primary_key: bool, precision: int):
        self.name = name
        self.column_type = column_type
        self.is_primary_key = is_primary_key
        self.precision = precision


# 模拟表的索引信息
class MySQLIndex:
    def __init__(self, index_name: str):
        self.index_name = index_name

    def get_index_name(self):
        if self.index_name == "PRIMARY":
            return "`PRIMARY`"
        return self.index_name


# 模拟表信息
class MySQLTable:
    def __init__(self, table_name: str, columns: List[MySQLColumn], indexes: List[MySQLIndex], engine: MySQLEngine):
        self.table_name = table_name
        self.columns = columns
        self.indexes = indexes
        self.engine = engine

    def has_primary_key(self):
        return any(column.is_primary_key for column in self.columns)


# 模拟表集合，对应原Java代码中的MySQLTables类的部分功能
class MySQLTables:
    def __init__(self, tables: List[MySQLTable]):
        self.tables = tables

    def get_random_row_value(self, con: pymysql.connections.Connection):
        # 构建随机获取一行数据的SQL语句（示例，可能需要根据实际情况完善）
        random_row_sql = "SELECT {} FROM {} ORDER BY RAND() LIMIT 1".format(
            ", ".join([f"{table.name}.{col.name} AS {table.name}{col.name}" for table in self.tables for col in table.columns]),
            ", ".join([table.name for table in self.tables])
        )
        with con.cursor() as cursor:
            cursor.execute(random_row_sql)
            result = cursor.fetchone()
            if not result:
                raise AssertionError(f"Could not find random row! {random_row_sql}")
            values = {}
            index = 0
            for table in self.tables:
                for col in table.columns:
                    index += 1
                    if result[index - 1] is None:
                        value = None
                    elif col.column_type == MySQLDataType.INT:
                        value = result[index - 1]
                    elif col.column_type == MySQLDataType.VARCHAR:
                        value = result[index - 1]
                    else:
                        raise AssertionError(f"Unsupported data type: {col.column_type}")
                    values[col] = value
            return values


# 模拟整个数据库模式信息
class MySQLSchema:
    NR_SCHEMA_READ_TRIES = 10

    def __init__(self, database_tables: List[MySQLTable]):
        self.database_tables = database_tables

    @classmethod
    def from_connection(cls, con: pymysql.connections.Connection, database_name: str) -> "MySQLSchema":
        for _ in range(cls.NR_SCHEMA_READ_TRIES):
            try:
                database_tables = []
                with con.cursor() as cursor:
                    # 查询获取所有表信息
                    cursor.execute(
                        "SELECT TABLE_NAME, ENGINE from INFORMATION_SCHEMA.TABLES where table_schema = %s",
                        (database_name,)
                    )
                    table_results = cursor.fetchall()
                    for table_result in table_results:
                        table_name, table_engine_str = table_result
                        engine = MySQLEngine.get(table_engine_str)
                        columns = cls.get_table_columns(con, table_name, database_name)
                        indexes = cls.get_indexes(con, table_name, database_name)
                        table = MySQLTable(table_name, columns, indexes, engine)
                        database_tables.append(table)
                return cls(database_tables)
            except Exception as e:
                print(f"Error reading schema, retrying. Error: {e}")
        raise AssertionError("Failed to read schema after multiple tries")

    @staticmethod
    def get_indexes(con: pymysql.connections.Connection, table_name: str, database_name: str) -> List[MySQLIndex]:
        indexes = []
        with con.cursor() as cursor:
            cursor.execute(
                "SELECT INDEX_NAME FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s",
                (database_name, table_name)
            )
            result = cursor.fetchall()
            for r in result:
                index_name = r[0]
                indexes.append(MySQLIndex(index_name))
        return indexes

    @staticmethod
    def get_table_columns(con: pymysql.connections.Connection, table_name: str, database_name: str) -> List[MySQLColumn]:
        columns = []
        with con.cursor() as cursor:
            cursor.execute(
                "SELECT COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION, COLUMN_KEY from INFORMATION_SCHEMA.COLUMNS where table_schema = %s AND TABLE_NAME = %s",
                (database_name, table_name)
            )
            result = cursor.fetchall()
            for r in result:
                column_name, data_type, precision, is_primary_key_str = r
                is_primary_key = is_primary_key_str == "PRI"
                column_type = MySQLDataType.get_column_type(data_type)
                columns.append(MySQLColumn(column_name, column_type, is_primary_key, precision))
        return columns

    def get_random_table_non_empty_tables(self):
        return MySQLTables(random.sample(self.database_tables, len(self.database_tables)))

    @staticmethod
    def get_column_type(type_string: str) -> MySQLDataType:
        type_mapping = {
            "tinyint": MySQLDataType.INT,
            "smallint": MySQLDataType.INT,
            "mediumint": MySQLDataType.INT,
            "int": MySQLDataType.INT,
            "bigint": MySQLDataType.INT,
            "varchar": MySQLDataType.VARCHAR,
            "tinytext": MySQLDataType.VARCHAR,
            "mediumtext": MySQLDataType.VARCHAR,
            "text": MySQLDataType.VARCHAR,
            "longtext": MySQLDataType.VARCHAR,
            "double": MySQLDataType.DOUBLE,
            "float": MySQLDataType.FLOAT,
            "decimal": MySQLDataType.DECIMAL,
        }
        return type_mapping.get(type_string, None)