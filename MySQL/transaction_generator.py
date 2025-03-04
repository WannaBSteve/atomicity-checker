import random
from typing import Dict, List, Tuple, Set
from enum import Enum
import logging

logger = logging.getLogger('atomicity-checker')

class MySQLFunction(Enum):
    ABS = 1
    ACOS = 1
    ASCII = 1
    ASIN = 1
    ATAN = 1
    ATAN2 = 1
    BIN = 1
    CEIL = 1
    CEILING = 1
    CHAR = 1
    COMPRESS = 1
    CONCAT = 2
    CONV = 3
    COS = 1
    COT = 1
    CRC32 = 1
    CURTIME = 0
    DATABASE = 0
    DEGREES = 1
    EXP = 1
    FLOOR = 1
    FROM_BASE64 = 1
    HEX = 1
    IF = 3
    IFNULL = 2
    ISNULL = 1
    LCASE = 1
    LEAST = 3
    LEFT = 2
    LENGTH = 1
    LN = 1
    LOG = 2
    LOG10 = 1
    LOG2 = 1
    LOWER = 1
    LPAD = 3
    LTRIM = 1
    MD5 = 1
    MID = 3
    MOD = 2
    NULLIF = 2
    OCT = 1
    ORD = 1
    PI = 0
    RAND = 0
    REVERSE = 1
    RIGHT = 2
    ROUND = 1
    RPAD = 3
    RTRIM = 1
    SCHEMA = 0
    SHA1 = 1
    SIGN = 1
    SIN = 1
    SPACE = 1
    SQRT = 1
    STRCMP = 2
    TAN = 1
    TO_BASE64 = 1
    UPPER = 1
    USER = 0
    UUID = 0
    VERSION = 0

    def __init__(self, arg_count):
        self.arg_count = arg_count

    @classmethod
    def get_random_func(cls):
        return random.choice(list(cls))

class MySQLExprGen:
    def __init__(self, columns: Dict[str, Tuple], depth_limit: int = 3):
        self.columns = columns
        self.depth_limit = depth_limit
        # 按类型分类列
        self.numeric_columns = []
        self.string_columns = []
        for name, info in columns.items():
            col_type = info[1].upper()
            if any(t in col_type for t in ['INT', 'FLOAT', 'DOUBLE', 'DECIMAL']):
                self.numeric_columns.append(name)
            elif any(t in col_type for t in ['CHAR', 'VARCHAR', 'TEXT']):
                self.string_columns.append(name)

    def gen_predicate(self) -> str:
        expr = self.gen_expr(0)
        if not expr:
            return "True"
        return expr

    def gen_expr(self, depth: int) -> str:
        if random.random() < 0.5 or depth > self.depth_limit:
            return self.gen_leaf()

        operators = [
            self.gen_column,
            self.gen_constant,
            self.gen_unary_prefix_op,
            self.gen_unary_postfix_op,
            self.gen_binary_logical_op,
            self.gen_binary_bit_op,
            self.gen_binary_math_op,
            self.gen_binary_comp_op,
            self.gen_in_op,
            self.gen_between_op,
            self.gen_cast_op,
            self.gen_function
        ]
        
        return random.choice(operators)(depth)

    def gen_leaf(self) -> str:
        if random.random() < 0.5:
            return self.gen_column(0)
        return self.gen_constant(0)

    def gen_column(self, depth: int) -> str:
        return random.choice(list(self.columns.keys()))

    def gen_constant(self, depth: int) -> str:
        const_type = random.choice(["INT", "NULL", "STRING", "DOUBLE"])
        if const_type == "INT":
            return str(random.randint(-1000, 1000))
        elif const_type == "NULL":
            return "NULL" if random.random() < 0.5 else str(random.randint(-1000, 1000))
        elif const_type == "STRING":
            return f'"{self._gen_random_string()}"'
        elif const_type == "DOUBLE":
            return str(round(random.uniform(-1000, 1000), 2))
        return "0"

    def gen_unary_prefix_op(self, depth: int) -> str:
        op = random.choice(["NOT", "!", "+", "-"])
        return f"{op}({self.gen_expr(depth+1)})"

    def gen_unary_postfix_op(self, depth: int) -> str:
        op = random.choice(["IS NULL", "IS FALSE", "IS TRUE"])
        return f"({self.gen_expr(depth+1)}) {op}"

    def gen_binary_logical_op(self, depth: int) -> str:
        op = random.choice(["AND", "OR", "XOR"])
        return f"({self.gen_expr(depth+1)}) {op} ({self.gen_expr(depth+1)})"

    def gen_binary_bit_op(self, depth: int) -> str:
        op = random.choice(["&", "|", "^", ">>", "<<"])
        return f"({self.gen_expr(depth+1)}) {op} ({self.gen_expr(depth+1)})"

    def gen_binary_math_op(self, depth: int) -> str:
        op = random.choice(["+", "-", "*", "/", "%"])
        return f"({self.gen_expr(depth+1)}) {op} ({self.gen_expr(depth+1)})"

    def gen_binary_comp_op(self, depth: int) -> str:
        """生成比较操作，确保类型匹配"""
        op = random.choice(["=", "!=", "<", "<=", ">", ">="])
        
        # 随机选择一个数值列进行比较
        if self.numeric_columns and random.random() < 0.7:
            col = random.choice(self.numeric_columns)
            value = str(random.randint(-1000, 1000))
            return f"({col}) {op} ({value})"
        
        # 或者选择一个字符串列进行比较
        elif self.string_columns:
            col = random.choice(self.string_columns)
            value = f'"{self._gen_random_string(5)}"'
            if op in ["=", "!="]:  # 字符串只用相等或不相等比较
                return f"({col}) {op} ({value})"
            else:
                return f"({col}) = ({value})"  # 对字符串使用相等比较
        
        # 如果没有合适的列，返回一个简单的数值比较
        return f"(1) {op} (0)"

    def gen_in_op(self, depth: int) -> str:
        """生成IN操作，确保类型匹配"""
        if self.numeric_columns:
            col = random.choice(self.numeric_columns)
            values = [str(random.randint(-1000, 1000)) for _ in range(3)]
            return f"({col}) IN ({', '.join(values)})"
        elif self.string_columns:
            col = random.choice(self.string_columns)
            values = [f'"{self._gen_random_string(5)}"' for _ in range(3)]
            return f"({col}) IN ({', '.join(values)})"
        return "1 IN (0, 1, 2)"

    def gen_between_op(self, depth: int) -> str:
        """生成BETWEEN操作，确保类型匹配"""
        if self.numeric_columns:
            col = random.choice(self.numeric_columns)
            val1 = random.randint(-1000, 1000)
            val2 = random.randint(-1000, 1000)
            if val1 > val2:
                val1, val2 = val2, val1
            return f"({col}) BETWEEN ({val1}) AND ({val2})"
        return "1 BETWEEN 0 AND 2"

    def gen_cast_op(self, depth: int) -> str:
        cast_type = random.choice(["INT", "FLOAT", "DOUBLE", "CHAR"])
        return f"CAST(({self.gen_expr(depth + 1)}) AS {cast_type})"

    def gen_function(self, depth: int) -> str:
        function = MySQLFunction.get_random_func()
        arg_list = []
        for _ in range(function.arg_count):
            arg_list.append(self.gen_expr(depth+1))
        return f"{function.name}({', '.join(f'({arg})' for arg in arg_list)})"

    def _gen_random_string(self, length: int = 10) -> str:
        return ''.join(random.choice('abcdefghijklmnopqrstuvwxyz') for _ in range(length))

class TransactionGenerator:
    def __init__(self, table_name: str, columns: Dict[str, Tuple], isolation_level: str):
        self.table_name = table_name
        self.columns = columns
        self.isolation_level = isolation_level
        self.expr_generator = MySQLExprGen(columns)

    def gen_select_statement(self) -> str:
        predicate = self.expr_generator.gen_predicate()
        selected_columns = random.sample(list(self.columns.keys()), 
                                      random.randint(1, len(self.columns)))
        
        postfix = ""
        if random.random() < 0.5:
            if random.random() < 0.5:
                postfix = " FOR UPDATE"
            else:
                postfix = " LOCK IN SHARE MODE"
                
        return (f"SELECT {', '.join(selected_columns)} FROM {self.table_name} "
                f"WHERE {predicate}{postfix}")

    def gen_insert_statement(self) -> str:
        inserted_cols = random.sample(list(self.columns.keys()),
                                    random.randint(1, len(self.columns)))
        
        # 确保必需的列被包含
        for col_name, col_info in self.columns.items():
            if (col_info[2] or col_info[4]) and col_name not in inserted_cols:  # primary key or not null
                inserted_cols.append(col_name)
        
        values = []
        for col_name in inserted_cols:
            col_info = self.columns[col_name]
            col_type = col_info[1].upper()
            
            # 完全对齐 Java 的 MySQLColumn.getRandomVal() 实现
            if col_info[2]:  # 主键
                values.append("NULL")
            elif "TINYINT" in col_type:
                values.append(str(random.randint(-128, 127)))
            elif "SMALLINT" in col_type:
                values.append(str(random.randint(-32768, 32767)))
            elif "MEDIUMINT" in col_type:
                values.append(str(random.randint(-8388608, 8388607)))
            elif "INT" in col_type or "BIGINT" in col_type:
                values.append(str(random.randint(-2147483648, 2147483647)))
            elif "FLOAT" in col_type or "DOUBLE" in col_type or "DECIMAL" in col_type:
                values.append(str(random.uniform(-100, 100)))
            elif any(t in col_type for t in ["CHAR", "VARCHAR", "TEXT", "TINYTEXT", "MEDIUMTEXT", "LONGTEXT"]):
                size = 20  # 默认大小
                if "(" in col_type:
                    size = int(col_type.split("(")[1].rstrip(")"))
                random_str = ''.join(random.choice('abcdefghijklmnopqrstuvwxyz') 
                                   for _ in range(random.randint(1, min(10, size))))
                values.append(f'"{random_str}"')
            elif any(t in col_type for t in ["BLOB", "MEDIUMBLOB", "LONGBLOB"]):
                hex_chars = "0123456789ABCDEF"
                hex_str = ''.join(random.choice(hex_chars) for _ in range(8))
                values.append(f"0x{hex_str}")
            else:
                values.append("NULL")
        
        ignore = "IGNORE " if random.random() < 0.5 else ""
        return (f"INSERT {ignore}INTO {self.table_name}({', '.join(inserted_cols)}) "
                f"VALUES ({', '.join(values)})")

    def gen_update_statement(self) -> str:
        """生成UPDATE语句，确保值符合列的大小限制"""
        predicate = self.expr_generator.gen_predicate()
        updated_cols = random.sample(list(self.columns.keys()),
                                   random.randint(1, len(self.columns)))
        
        set_pairs = []
        for col_name in updated_cols:
            col_info = self.columns[col_name]
            col_type = col_info[1].upper()
            
            # 完全对齐 Java 的 MySQLColumn.getRandomVal() 实现
            if col_info[2]:  # 主键
                continue  # 跳过主键列
            elif "TINYINT" in col_type:
                value = str(random.randint(-128, 127))
            elif "SMALLINT" in col_type:
                value = str(random.randint(-32768, 32767))
            elif "MEDIUMINT" in col_type:
                value = str(random.randint(-8388608, 8388607))
            elif "INT" in col_type or "BIGINT" in col_type:
                value = str(random.randint(-2147483648, 2147483647))
            elif "FLOAT" in col_type or "DOUBLE" in col_type or "DECIMAL" in col_type:
                value = str(random.uniform(-100, 100))
            elif any(t in col_type for t in ["CHAR", "VARCHAR", "TEXT", "TINYTEXT", "MEDIUMTEXT", "LONGTEXT"]):
                size = 20  # 默认大小
                if "(" in col_type:
                    size = int(col_type.split("(")[1].rstrip(")"))
                random_str = ''.join(random.choice('abcdefghijklmnopqrstuvwxyz') 
                                   for _ in range(random.randint(1, min(10, size))))
                value = f'"{random_str}"'
            elif any(t in col_type for t in ["BLOB", "MEDIUMBLOB", "LONGBLOB"]):
                hex_chars = "0123456789ABCDEF"
                hex_str = ''.join(random.choice(hex_chars) for _ in range(8))
                value = f"0x{hex_str}"
            else:
                continue  # 跳过不支持的类型
                
            set_pairs.append(f"{col_name}={value}")
            
        if not set_pairs:  # 如果没有可更新的列
            return f"UPDATE {self.table_name} SET c0=c0 WHERE {predicate}"  # 生成一个无效更新
            
        return f"UPDATE {self.table_name} SET {', '.join(set_pairs)} WHERE {predicate}"

    def gen_delete_statement(self) -> str:
        predicate = self.expr_generator.gen_predicate()
        return f"DELETE FROM {self.table_name} WHERE {predicate}"

    def gen_statement(self) -> str:
        while True:
            if random.random() < 0.4:
                return self.gen_select_statement()
            elif random.random() < 0.7:
                if random.random() < 0.5:
                    return self.gen_insert_statement()
                else:
                    return self.gen_update_statement()
            elif random.random() < 0.9:
                return self.gen_delete_statement()

    def gen_transaction(self, tx_id: int) -> List[str]:
        statements = ["BEGIN"]
        n = random.randint(1, 5)  # 生成1-5条语句
        
        for _ in range(n):
            statements.append(self.gen_statement())
            
        statements.append("COMMIT" if random.random() < 0.7 else "ROLLBACK")
        return statements

    def _gen_random_string(self, length: int = 10) -> str:
        return ''.join(random.choice('abcdefghijklmnopqrstuvwxyz') for _ in range(length)) 