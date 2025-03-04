import random
import mysql.connector
from typing import List, Dict, Tuple, Optional
from enum import Enum

class MySQLDataType(Enum):
    TINYINT = "TINYINT"
    SMALLINT = "SMALLINT"
    MEDIUMINT = "MEDIUMINT"
    INT = "INT"
    BIGINT = "BIGINT"
    
    FLOAT = "FLOAT"
    DOUBLE = "DOUBLE"
    DECIMAL = "DECIMAL"
    
    BINARY = "BINARY"
    VARBINARY = "VARBINARY"
    
    CHAR = "CHAR"
    VARCHAR = "VARCHAR"
    TINYTEXT = "TINYTEXT"
    TEXT = "TEXT"
    MEDIUMTEXT = "MEDIUMTEXT"
    LONGTEXT = "LONGTEXT"
    
    BLOB = "BLOB"
    MEDIUMBLOB = "MEDIUMBLOB"
    LONGBLOB = "LONGBLOB"
    
    DATE = "DATE"
    TIME = "TIME"
    DATETIME = "DATETIME"
    TIMESTAMP = "TIMESTAMP"
    
    JSON = "JSON"
    ENUM = "ENUM"
    SET = "SET"

    @staticmethod
    def get_random_data_type():
        return random.choice([
            MySQLDataType.INT,
            MySQLDataType.FLOAT,
            MySQLDataType.DOUBLE,
            MySQLDataType.CHAR,
            MySQLDataType.VARCHAR,
            MySQLDataType.TEXT
        ])

    @staticmethod
    def is_numeric(data_type: str) -> bool:
        numeric_types = [
            MySQLDataType.TINYINT,
            MySQLDataType.SMALLINT,
            MySQLDataType.MEDIUMINT,
            MySQLDataType.INT,
            MySQLDataType.BIGINT,
            MySQLDataType.FLOAT,
            MySQLDataType.DOUBLE,
            MySQLDataType.DECIMAL
        ]
        return any(dt.value in data_type for dt in numeric_types)

    @staticmethod
    def is_string(data_type: str) -> bool:
        string_types = [
            MySQLDataType.CHAR,
            MySQLDataType.VARCHAR,
            MySQLDataType.TINYTEXT,
            MySQLDataType.TEXT,
            MySQLDataType.MEDIUMTEXT,
            MySQLDataType.LONGTEXT
        ]
        return any(dt.value in data_type for dt in string_types)

    @staticmethod
    def has_len(data_type: str) -> bool:
        return any(dt.value in data_type for dt in [MySQLDataType.CHAR, MySQLDataType.VARCHAR])

class MySQLColumn:
    def __init__(self, table_name: str, column_name: str, data_type: str, 
                 is_primary: bool, is_unique: bool, is_not_null: bool, size: int = 0):
        self.table_name = table_name
        self.column_name = column_name
        self.data_type = data_type
        self.is_primary = is_primary
        self.is_unique = is_unique
        self.is_not_null = is_not_null
        self.size = size
        self.appeared_values = []  # 对应 Java 中的 appearedValues

    def get_random_value(self) -> str:
        if self.is_primary:
            return "NULL"  # 自增主键

        # 完全对齐 Java 实现的数据类型判断
        data_type_upper = self.data_type.upper()
        
        if "TINYINT" in data_type_upper:
            return str(random.randint(-128, 127))
        elif "SMALLINT" in data_type_upper:
            return str(random.randint(-32768, 32767))
        elif "MEDIUMINT" in data_type_upper:
            return str(random.randint(-8388608, 8388607))
        elif "INT" in data_type_upper or "BIGINT" in data_type_upper:
            return str(random.randint(-2147483648, 2147483647))
        elif "FLOAT" in data_type_upper or "DOUBLE" in data_type_upper or "DECIMAL" in data_type_upper:
            return str(round(random.uniform(-100, 100), 2))
        elif any(t in data_type_upper for t in ["CHAR", "VARCHAR", "TEXT", "TINYTEXT", "MEDIUMTEXT", "LONGTEXT"]):
            if self.size == 0:
                self.size = 20
            random_str = ''.join(random.choice('abcdefghijklmnopqrstuvwxyz') for _ in range(random.randint(1, self.size)))
            return f'"{random_str}"'
        elif any(t in data_type_upper for t in ["BLOB", "MEDIUMBLOB", "LONGBLOB"]):
            return self._random_hex_str()
        else:
            raise ValueError(f"Unexpected data type: {self.data_type}")

    def _random_hex_str(self) -> str:
        """对应 Java 中的 randomHexStr 方法"""
        size = 8
        hex_chars = "0123456789ABCDEF"
        hex_str = ''.join(random.choice(hex_chars) for _ in range(size))
        return f"0x{hex_str}"

class MySQLInitializer:
    def __init__(self, connection, database):
        self.conn = connection
        self.database = database
        self.cursor = self.conn.cursor()
        self.columns: List[MySQLColumn] = []
        self.table_columns: Dict[str, List[Tuple[str, str]]] = {}  # 存储表的列信息
        
    def initialize_database(self):
        """初始化数据库"""
        self.cursor.execute(f"DROP DATABASE IF EXISTS {self.database}")
        self.cursor.execute(f"CREATE DATABASE {self.database}")
        self.cursor.execute(f"USE {self.database}")
    
    def generate_tables_with_data_and_index(self):
        """生成表结构、数据和索引"""
        table_name = "table_0"  # 固定生成一个表
        
        # 生成列定义
        columns = self._generate_columns()
        create_table_sql = self._generate_create_table_sql(table_name, columns)
        print(f"Creating table: {create_table_sql}")
        self.cursor.execute(create_table_sql)
        
        # 保存列信息
        self.table_columns[table_name] = columns
        
        # 插入数据
        self._populate_table(table_name)
        
        # 创建索引
        self._create_indexes(table_name)
        
    def _generate_columns(self) -> List[Tuple[str, str, bool, bool, bool, int]]:
        """生成列定义"""
        columns = []
        # 添加id列作为主键
        columns.append(("id", "INT", True, False, True, 0))
        self.columns.append(MySQLColumn("table_0", "id", "INT", True, False, True, 0))
        
        # 生成2-9个随机列
        num_columns = random.randint(2, 9)
        for i in range(num_columns):
            col_name = f"c{i}"
            data_type = MySQLDataType.get_random_data_type().value
            
            # 确定列的约束
            is_primary = False
            is_unique = random.choice([True, False]) if MySQLDataType.is_numeric(data_type) else False
            is_not_null = random.choice([True, False])
            
            size = 0
            if MySQLDataType.has_len(data_type):
                size = random.randint(1, 20)
                data_type = f"{data_type}({size})"
            
            # 创建并存储列对象
            column = MySQLColumn("table_0", col_name, data_type, is_primary, is_unique, is_not_null, size)
            self.columns.append(column)
            columns.append((col_name, data_type, is_primary, is_unique, is_not_null, size))
            
        return columns

    def _generate_create_table_sql(self, table_name: str, columns: List[Tuple[str, str, bool, bool, bool, int]]) -> str:
        """生成建表SQL"""
        column_defs = []
        for col_name, data_type, is_primary, is_unique, is_not_null, size in columns:
            col_def = f"{col_name} {data_type}"
            if is_primary:
                col_def += " PRIMARY KEY AUTO_INCREMENT"
            if is_unique and not is_primary:
                col_def += " UNIQUE"
            if is_not_null and not is_primary:
                col_def += " NOT NULL"
            column_defs.append(col_def)
            
        # 添加表选项
        table_options = self._generate_table_options()
        
        return f"CREATE TABLE {table_name} ({', '.join(column_defs)}) {table_options}"
    
    def _generate_table_options(self) -> str:
        """生成表选项"""
        options = []
        if random.random() < 0.3:  # 30%概率添加选项
            possible_options = {
                "AUTO_INCREMENT": lambda: random.randint(1, 1000),
                "CHECKSUM": lambda: 1,
                "DELAY_KEY_WRITE": lambda: random.choice([0, 1]),
                "MAX_ROWS": lambda: random.randint(1000, 1000000),
                "MIN_ROWS": lambda: random.randint(1, 100),
                "PACK_KEYS": lambda: random.choice(["1", "0", "DEFAULT"]),
                "STATS_AUTO_RECALC": lambda: random.choice(["1", "0", "DEFAULT"]),
                "COMMENT": lambda: "'comment info'"
            }
            
            # 随机选择1-3个选项
            num_options = random.randint(1, 3)
            selected_options = random.sample(list(possible_options.items()), num_options)
            
            for option_name, value_generator in selected_options:
                options.append(f"{option_name}={value_generator()}")
                
        if options:
            return "ENGINE=InnoDB " + " ".join(options)
        return "ENGINE=InnoDB"

    def _populate_table(self, table_name: str):
        """填充表数据"""
        num_rows = random.randint(5, 15)
        for _ in range(num_rows):
            values = []
            for column in self.columns:
                values.append(column.get_random_value())
            
            col_names = [col.column_name for col in self.columns]
            insert_sql = f"INSERT INTO {table_name} ({', '.join(col_names)}) VALUES ({', '.join(values)})"
            
            print(f"Inserting data: {insert_sql}")
            self.cursor.execute(insert_sql)
    
    def _create_indexes(self, table_name: str):
        """创建索引，对齐Java实现"""
        # 筛选可作为索引的列（非TEXT类型的列）
        indexable_columns = [col for col in self.columns 
                           if not any(t in col.data_type.upper() 
                                    for t in ["TEXT", "BLOB"])]
        
        if not indexable_columns:
            return
            
        # 随机创建1-3个索引
        num_indexes = random.randint(1, 3)
        for i in range(num_indexes):
            # 随机决定是否创建复合索引
            is_composite = random.choice([True, False])
            num_columns = random.randint(2, 3) if is_composite else 1
            
            # 随机选择列，但确保不重复选择相同的列组合
            selected_columns = random.sample(indexable_columns, 
                                          min(num_columns, len(indexable_columns)))
            
            # 随机决定是否为唯一索引
            is_unique = random.choice([True, False])
            
            index_name = f"idx_{i}"
            index_type = "UNIQUE INDEX" if is_unique else "INDEX"
            
            # 使用列名创建索引
            column_names = [col.column_name for col in selected_columns]
            index_sql = (f"CREATE {index_type} {index_name} ON {table_name} "
                        f"({', '.join(column_names)})")
            
            try:
                print(f"Creating index: {index_sql}")
                self.cursor.execute(index_sql)
            except Exception as e:
                print(f"Error creating index: {e}")
                # 继续创建其他索引
    
    def commit_and_close(self):
        """提交更改并关闭游标"""
        try:
            self.conn.commit()
        except Exception as e:
            print(f"Error committing changes: {e}")
        finally:
            if self.cursor:
                self.cursor.close()


