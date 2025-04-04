import random
from typing import Dict, List, Tuple, Set, Optional
from enum import Enum
import logging
from MySQLInitializer import MySQLColumn, MySQLDataType

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
    def __init__(self, tables: Dict[str, List[Tuple]], depth_limit: int = 5):
        self.tables = tables
        self.depth_limit = depth_limit
        # 按类型分类列，使用字典存储每个表的列
        self.numeric_columns = {}
        self.string_columns = {}
        
        for table_name in tables:
            self.numeric_columns[table_name] = []
            self.string_columns[table_name] = []
            columns = tables[table_name]
            for col in columns:
                col_name = col[0]
                col_type = col[1].upper()
                if any(t in col_type for t in ['INT', 'FLOAT', 'DOUBLE', 'DECIMAL']):
                    self.numeric_columns[table_name].append(col_name)
                elif any(t in col_type for t in ['CHAR', 'VARCHAR', 'TEXT']):
                    self.string_columns[table_name].append(col_name)

    def gen_predicate(self, target_table=None) -> str:
        expr = self.gen_expr(0, target_table)
        if not expr:
            return "True"
        return expr

    def gen_expr(self, depth: int, target_table=None) -> str:
        if random.random() < 0.5 or depth > self.depth_limit:
            return self.gen_leaf()

        # 如果传入了target_table，则避免生成子查询
        if target_table and random.random() < 0.7:
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
        else:
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
                self.gen_function,
                lambda d: self.gen_subquery(d, target_table)
            ]
        
        # 选择一个操作符并传递target_table参数
        op = random.choice(operators)
        if op == self.gen_subquery:
            return op(depth, target_table)
        return op(depth)

    def gen_leaf(self) -> str:
        if random.random() < 0.5:
            return self.gen_column(0)
        return self.gen_constant(0)

    def gen_column(self, depth: int) -> str:
        # return random.choice(list(self.columns.keys()))
        # 随机选择一个表和列
        table_name = random.choice(list(self.tables.keys()))
        columns = self.tables[table_name]
        columns_names = [col[0] for col in columns]

        column_name = random.choice(list(columns_names))
        return f"{table_name}.{column_name}"

    def gen_constant(self, depth: int) -> str:
        const_type = random.choice(["INT", "NULL", "STRING", "DOUBLE"])
        if const_type == "INT":
            return str(random.randint(-1000, 1000))
        elif const_type == "NULL":
            return "NULL" if random.random() < 0.5 else str(random.randint(-1000, 1000))
        elif const_type == "STRING":
            s = self._gen_random_string()
            return f"'{s}'"  # 简单字符串常量，不包含等号
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
        return f"(({self.gen_expr(depth+1)}) {op} ({self.gen_expr(depth+1)}))"  # 增加外层括号

    def gen_binary_math_op(self, depth: int) -> str:
        """生成数学运算，确保只对数值类型的列进行操作"""
        op = random.choice(["+", "-", "*", "/", "%"])
        
        # 生成左操作数
        left = self.gen_numeric_expr(depth + 1)
        # 生成右操作数
        right = self.gen_numeric_expr(depth + 1)
        
        # 如果是除法，确保不会除以0
        if op == "/" and right == "0":
            right = "1"
        
        return f"{left} {op} {right}"

    def gen_numeric_expr(self, depth: int) -> str:
        """生成数值类型的表达式"""
        if depth > self.depth_limit or random.random() < 0.5:
            # 生成叶子节点
            if random.random() < 0.7:  # 70%概率使用列
                table_name = random.choice(list(self.tables.keys()))
                if self.numeric_columns[table_name]:
                    col = random.choice(self.numeric_columns[table_name])
                    return f"({table_name}.{col})"
            return str(random.randint(-100, 100))
        
        # 生成复杂表达式
        operators = [
            lambda: self.gen_binary_math_op(depth),
            lambda: self.gen_function_numeric(depth),
            lambda: f"({self.gen_numeric_expr(depth + 1)})"
        ]
        return random.choice(operators)()

    def gen_function_numeric(self, depth: int) -> str:
        """生成数值类型的函数调用"""
        numeric_functions = [
            ("ABS", 1), ("CEIL", 1), ("FLOOR", 1),
            ("ROUND", 1), ("SIGN", 1), ("SQRT", 1)
        ]
        func_name, arg_count = random.choice(numeric_functions)
        args = [self.gen_numeric_expr(depth + 1) for _ in range(arg_count)]
        return f"{func_name}({', '.join(args)})"

    def gen_binary_comp_op(self, depth: int) -> str:
        """生成比较操作，确保类型匹配且支持多表"""
        op = random.choice(["=", "!=", "<", "<=", ">", ">="])
        
        # 随机选择一个表
        table_name = random.choice(list(self.tables.keys()))
        
        # 随机选择一个数值列进行比较
        if self.numeric_columns[table_name] and random.random() < 0.7:
            col = random.choice(self.numeric_columns[table_name])
            value = str(random.randint(-1000, 1000))
            return f"({table_name}.{col}) {op} ({value})"
        
        # 或者选择一个字符串列进行比较
        elif self.string_columns[table_name]:
            col = random.choice(self.string_columns[table_name])
            value = f'"{self._gen_random_string(5)}"'
            if op in ["=", "!="]:  # 字符串只用相等或不相等比较
                return f"({table_name}.{col}) {op} ({value})"
            else:
                return f"({table_name}.{col}) = ({value})"  # 对字符串使用相等比较
        
        return f"(1) {op} (0)"  # 如果没有合适的列，返回一个简单的数值比较

    def gen_in_op(self, depth: int) -> str:
        """生成IN操作，确保类型匹配且支持多表"""
        table_name = random.choice(list(self.tables.keys()))
        
        if self.numeric_columns[table_name]:
            col = random.choice(self.numeric_columns[table_name])
            values = [str(random.randint(-1000, 1000)) for _ in range(3)]
            return f"({table_name}.{col}) IN ({', '.join(values)})"
        elif self.string_columns[table_name]:
            col = random.choice(self.string_columns[table_name])
            values = [f'"{self._gen_random_string(5)}"' for _ in range(3)]
            return f"({table_name}.{col}) IN ({', '.join(values)})"
        return "1 IN (0, 1, 2)"

    def gen_between_op(self, depth: int) -> str:
        """生成BETWEEN操作，确保类型匹配且支持多表"""
        table_name = random.choice(list(self.tables.keys()))
        
        if self.numeric_columns[table_name]:
            col = random.choice(self.numeric_columns[table_name])
            val1 = random.randint(-1000, 1000)
            val2 = random.randint(-1000, 1000)
            if val1 > val2:
                val1, val2 = val2, val1
            return f"({table_name}.{col}) BETWEEN ({val1}) AND ({val2})"
        return "1 BETWEEN 0 AND 2"

    def gen_cast_op(self, depth: int) -> str:
        cast_type = random.choice(["SIGNED", "CHAR", "DATE", "DATETIME", "DECIMAL", "TIME"])  # 修正CAST类型
        expr = self.gen_expr(depth + 1)
        return f"CAST({expr} AS {cast_type})"  # 移除多余括号

    def gen_function(self, depth: int) -> str:
        function = MySQLFunction.get_random_func()
        arg_list = []
        for _ in range(function.arg_count):
            arg_list.append(self.gen_expr(depth+1))
        return f"{function.name}({', '.join(f'({arg})' for arg in arg_list)})"

    def gen_subquery(self, depth: int, target_table=None) -> str:
        """生成一个简单的子查询表达式，避免引用正在更新/删除的表"""
        if depth > self.depth_limit:
            return self.gen_column(0)  # 防止过深嵌套
        
        # 获取可用的表，排除当前正在操作的表
        available_tables = [t for t in self.tables.keys() if t != target_table]
        
        # 如果没有其他表可用，返回一个常量表达式
        if not available_tables:
            return str(random.randint(0, 100))
        
        table_name = random.choice(available_tables)
        column_name = random.choice([col[0] for col in self.tables[table_name]])
        
        subquery_type = random.choice(["EXISTS", "IN", "="])
        
        inner_predicate = self.gen_predicate(target_table)  # 递归生成内部条件，传入target_table
        
        if subquery_type == "EXISTS":
            return f"EXISTS (SELECT 1 FROM {table_name} WHERE {inner_predicate})"
        elif subquery_type == "IN":
            return f"{self.gen_column(depth+1)} IN (SELECT {column_name} FROM {table_name} WHERE {inner_predicate})"
        else:  # "="
            return f"{self.gen_column(depth+1)} = (SELECT {column_name} FROM {table_name} WHERE {inner_predicate} LIMIT 1)"

    def _gen_random_string(self, length: int = 10) -> str:
        return ''.join(random.choice('abcdefghijklmnopqrstuvwxyz') for _ in range(length))

class TransactionGenerator:
    def __init__(self, tables: Dict[str, List[Tuple]], columns: Dict[str, List[MySQLColumn]], isolation_level: str):
        self.tables = tables
        self.columns = columns
        self.isolation_level = isolation_level
        self.expr_generator = MySQLExprGen(tables)

    def gen_join_type(self):
        # 常见JOIN类型及其概率权重（INNER和LEFT更常见）
        join_types = ["INNER JOIN", "LEFT JOIN", "RIGHT JOIN", "CROSS JOIN"]
        weights = [4, 4, 1, 1]  # 调整权重分布
        return random.choices(join_types, weights=weights, k=1)[0]
    
    def gen_join_condition(self, left_table: str, right_table: str) -> str:
        """生成两个表之间的连接条件"""
        # 寻找两个表的公共列名
        left_columns = [col[0] for col in self.tables[left_table]]
        right_columns = [col[0] for col in self.tables[right_table]]
        common_columns = list(set(left_columns) & set(right_columns))
        
        # 优先使用公共列
        if common_columns:
            col = random.choice(common_columns)
            return f"{left_table}.{col} = {right_table}.{col}"
        
        # 没有公共列则随机选择列组合（即使列名不同）
        left_col = random.choice(left_columns)
        right_col = random.choice(right_columns)
        # 50%概率使用列比较，50%概率使用常量比较
        if random.random() < 0.5:
            return f"{left_table}.{left_col} = {right_table}.{right_col}"
        else:
            # 生成一个常量比较（如1=1）
            const_comp = random.choice([
                (1, 1), (0, 0), ("'a'", "'a'"), ("NULL", "NULL")
            ])
            return f"{const_comp[0]} = {const_comp[1]}"

    def gen_join_clause(self, selected_tables: List[str]) -> str:
        """生成JOIN子句
        Args:
            selected_tables: 要JOIN的表列表
        Returns:
            完整的JOIN子句字符串
        """
        if len(selected_tables) == 1:
            return selected_tables[0]
            
        join_clause = selected_tables[0]
        for i in range(1, len(selected_tables)):
            join_type = self.gen_join_type()
            join_condition = self.gen_join_condition(selected_tables[i-1], selected_tables[i])
            if join_type == "CROSS JOIN":
                join_clause += f" {join_type} {selected_tables[i]}"
            else:
                join_clause += f" {join_type} {selected_tables[i]} ON {join_condition}"
        return join_clause
    
    def gen_select_statement(self) -> str:
        try:
            # 随机选择要JOIN的表的数量（1-3个）
            table_count = random.randint(1, min(3, len(self.tables)))
            selected_tables = random.sample(list(self.tables.keys()), table_count)
            
            # 生成SELECT的列
            selected_columns = []
            for table in selected_tables:
                columns = self.tables[table]
                columns_names = [col[0] for col in columns]
                selected_columns_names = random.sample(columns_names,
                                                       random.randint(1, len(columns_names)))

                selected_columns.extend([f"{table}.{col}" for col in selected_columns_names])
                
            # 构建JOIN子句
            join_clause = self.gen_join_clause(selected_tables)

            predicate = self.expr_generator.gen_predicate()
            
            postfix = ""
            if random.random() < 0.5:
                if random.random() < 0.5:
                    postfix = " FOR UPDATE"
                else:
                    postfix = " LOCK IN SHARE MODE"
                    
            return (f"SELECT {', '.join(selected_columns)} FROM {join_clause} "
                    f"WHERE {predicate}{postfix}")
        except Exception as e:
            logger.error(f"生成select语句失败：{e}")
            print(f"生成select语句失败：{e}")

    def gen_insert_statement(self) -> str:
        """生成INSERT语句，随机选择一个表进行插入"""
        try:
            # 随机选择一个表
            table_name = random.choice(list(self.tables.keys()))
            columns = self.tables[table_name]
            
            inserted_cols = random.sample(list(columns),
                                        random.randint(1, len(columns)))
            
            # 确保必需的列被包含
            for col in columns:
                col_name = col[0]
                if (col[2] or col[4]) and col not in inserted_cols:  # primary key or not null
                    inserted_cols.append(col)

            values = []
            for col in inserted_cols:
                col_name = col[0]
                col_type = col[1].upper()
                
                # 完全对齐 Java 的 MySQLColumn.getRandomVal() 实现
                if col[2]:  # 主键
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
                    # 使用_gen_column_value方法保持一致性
                    string_value = self._gen_column_value(col)
                    if string_value:
                        values.append(string_value)
                    else:
                        values.append("NULL")
                elif any(t in col_type for t in ["BLOB", "MEDIUMBLOB", "LONGBLOB"]):
                    hex_chars = "0123456789ABCDEF"
                    hex_str = ''.join(random.choice(hex_chars) for _ in range(8))
                    values.append(f"0x{hex_str}")
                else:
                    values.append("NULL")
            
            ignore = "IGNORE " if random.random() < 0.5 else ""
            inserted_col_names = [col[0] for col in inserted_cols]
            return (f"INSERT {ignore}INTO {table_name}({', '.join(inserted_col_names)}) "
                    f"VALUES ({', '.join(values)})")
        except Exception as e:
            logger.error(f"生成insert语句失败：{e}")
            print(f"生成insert语句失败：{e}")

    def gen_update_statement(self) -> str:
        """生成UPDATE语句，支持多表更新"""
        try:
            # 随机决定是否使用多表更新
            is_multi_table = random.random() < 0.3 and len(self.tables) > 1
            
            if is_multi_table:
                # 随机选择2个表进行多表更新
                table_count = random.randint(2, min(3, len(self.tables)))
                selected_tables = random.sample(list(self.tables.keys()), table_count)
                
                # 构建JOIN子句
                join_clause = self.gen_join_clause(selected_tables)
                
                # 随机选择一个表进行更新
                update_table = random.choice(selected_tables)
                columns = self.tables[update_table]
            
                set_pairs = []
                updated_cols = random.sample(columns,
                                            random.randint(1, len(columns)))
                
                for col in updated_cols:
                    col_name = col[0]
                    if not col[2]:  # 不更新主键
                        value = self._gen_column_value(col)
                        if value is not None:
                                set_pairs.append(f"{update_table}.{col_name}={value}")
                    if not set_pairs:
                        return self.gen_update_statement()  # 重试
                    
                    predicate = self.expr_generator.gen_predicate(target_table=update_table)
                    return f"UPDATE {join_clause} SET {', '.join(set_pairs)} WHERE {predicate}"
                
            else:
                # 单表更新
                table_name = random.choice(list(self.tables.keys()))
                columns = self.tables[table_name]
                
                updated_cols = random.sample(columns,
                                        random.randint(1, len(columns)))
                
                set_pairs = []
                for col in updated_cols:
                    col_name = col[0]
                    if not col[2]:  # 不更新主键
                        value = self._gen_column_value(col)
                        if value is not None:
                            set_pairs.append(f"{col_name}={value}")
                
                if not set_pairs:
                    return self.gen_update_statement()  # 重试
                    
                predicate = self.expr_generator.gen_predicate(target_table=table_name)
                return f"UPDATE {table_name} SET {', '.join(set_pairs)} WHERE {predicate}"
        except Exception as e:
            logger.error(f"生成update语句失败：{e}")
            print(f"生成update语句失败：{e}")


    def gen_delete_statement(self) -> str:
        """生成DELETE语句，支持多表删除"""
        try:
            # 随机决定是否使用多表删除
            is_multi_table = random.random() < 0.3 and len(self.tables) > 1
            
            if is_multi_table:
                # 随机选择2-3个表
                table_count = random.randint(2, min(3, len(self.tables)))
                selected_tables = random.sample(list(self.tables.keys()), table_count)
                
                # 构建JOIN子句
                join_clause = self.gen_join_clause(selected_tables)
                
                # 随机选择要删除的表
                delete_tables = random.sample(selected_tables, random.randint(1, len(selected_tables)))
                
                predicate = self.expr_generator.gen_predicate(target_table=selected_tables[0])
                return f"DELETE {', '.join(delete_tables)} FROM {join_clause} WHERE {predicate}"
            else:
                # 单表删除
                table_name = random.choice(list(self.tables.keys()))
                predicate = self.expr_generator.gen_predicate(target_table=table_name)
                return f"DELETE FROM {table_name} WHERE {predicate}"
        except Exception as e:
            logger.error(f"生成delete语句失败：{e}")
            print(f"生成delete语句失败：{e}")

    def _get_max_type_length(self, col_type: str) -> int:
        """获取数据类型的最大长度限制"""
        col_type = col_type.upper()
        if "TINYTEXT" in col_type:
            return 255
        elif "TEXT" in col_type:
            return 65535
        elif "MEDIUMTEXT" in col_type:
            return 16777215
        elif "LONGTEXT" in col_type:
            return 4294967295
        elif "VARCHAR" in col_type:
            # VARCHAR最大长度限制
            return 65535
        elif "CHAR" in col_type:
            # CHAR最大长度限制
            return 255
        else:
            # 其他类型默认值
            return 100

    def _gen_column_value(self, col_info: Tuple) -> Optional[str]:
        """根据列信息生成对应的随机值"""
        col_type = col_info[1].upper()
        
        if "TINYINT" in col_type:
            return str(random.randint(-128, 127))
        elif "SMALLINT" in col_type:
            return str(random.randint(-32768, 32767))
        elif "MEDIUMINT" in col_type:
            return str(random.randint(-8388608, 8388607))
        elif "INT" in col_type or "BIGINT" in col_type:
            return str(random.randint(-2147483648, 2147483647))
        elif "FLOAT" in col_type or "DOUBLE" in col_type or "DECIMAL" in col_type:
            return str(round(random.uniform(-100, 100), 2))
        elif any(t in col_type for t in ["CHAR", "VARCHAR", "TEXT", "TINYTEXT", "MEDIUMTEXT", "LONGTEXT"]):
            # 使用定义的size作为最大长度限制
            user_size = col_info[5]  # 获取用户定义的size属性
            if user_size == 0:
                user_size = 20  # 默认大小

            # 获取数据类型本身的最大长度限制
            type_max_size = self._get_max_type_length(col_type)
            
            # 使用较小的值作为实际限制
            actual_max_size = min(user_size, type_max_size)
            
            # 边界值测试：
            # 10%概率：生成极限边界值（最大长度的95-100%）
            # 20%概率：生成接近边界值（最大长度的80-95%）
            # 70%概率：生成普通值（最大长度的1-50%）
            rand_val = random.random()
            if rand_val < 0.1:
                # 极限边界测试（95-100%的最大长度）
                length = max(1, int(actual_max_size * random.uniform(0.95, 1.0)))
            elif rand_val < 0.3:
                # 接近边界值（80-95%的最大长度）
                length = max(1, int(actual_max_size * random.uniform(0.8, 0.95)))
            else:
                # 普通值（1-50%的最大长度）
                length = random.randint(1, max(1, int(actual_max_size * 0.5)))

            # 限制长度，避免生成过长的字符串导致性能问题
            # 对于非常大的类型(如TEXT)，我们限制最大测试长度为1000
            length = min(length, 1000)
            
            random_str = ''.join(random.choice('abcdefghijklmnopqrstuvwxyz') 
                              for _ in range(length))
            return f'"{random_str}"'
        elif any(t in col_type for t in ["BLOB", "MEDIUMBLOB", "LONGBLOB"]):
            hex_chars = "0123456789ABCDEF"
            hex_str = ''.join(random.choice(hex_chars) for _ in range(8))
            return f"0x{hex_str}"
        
        return None
    
    def gen_call_procedure_statement(self) -> str:
        """生成调用存储过程的语句"""
        # 随机选择一个表和操作类型
        table_name = random.choice(list(self.tables.keys()))
        operation = random.choice(["get", "insert", "update", "delete"])
        proc_name = f"sp_{operation}_{table_name}"
        
        if operation == "get":
            # 简单的ID参数
            return f"CALL {proc_name}({random.randint(1, 100)})"
        
        elif operation == "insert":
            # 为插入生成所有必要的参数
            non_primary_cols = [col for col in self.columns[table_name] if not col.is_primary]
            if not non_primary_cols:
                return f"CALL sp_get_{table_name}({random.randint(1, 100)})"
            
            params = []
            for col in non_primary_cols:
                if MySQLDataType.is_numeric(col.data_type):
                    params.append(str(random.randint(-100, 1000)))
                elif MySQLDataType.is_string(col.data_type):
                    # 获取列的最大长度
                    max_length = col.size if col.size else 8
                    
                    # 获取数据类型的最大限制
                    type_max_length = self._get_max_type_length(col.data_type)
                    
                    # 使用较小的值作为实际限制
                    actual_max_length = min(max_length, type_max_length)
                    
                    # 边界值测试策略
                    rand_val = random.random()
                    if rand_val < 0.1:
                        # 极限边界测试（95-100%的最大长度）
                        length = max(1, int(actual_max_length * random.uniform(0.95, 1.0)))
                    elif rand_val < 0.3:
                        # 接近边界值（80-95%的最大长度）
                        length = max(1, int(actual_max_length * random.uniform(0.8, 0.95)))
                    else:
                        # 普通值（1-50%的最大长度）
                        length = random.randint(1, max(1, int(actual_max_length * 0.5)))
                    
                    # 限制长度，避免过长
                    length = min(length, 1000)
                    
                    rand_str = ''.join(random.choice('abcdefghijklmnopqrstuvwxyz') 
                                      for _ in range(length))
                    params.append(f"'{rand_str}'")
                else:
                    params.append("NULL")
            
            return f"CALL {proc_name}({', '.join(params)})"
        
        elif operation == "update":
            # 为更新生成ID和其他列参数
            non_primary_cols = [col for col in self.columns[table_name] if not col.is_primary]
            if not non_primary_cols:
                return f"CALL sp_get_{table_name}({random.randint(1, 100)})"
            
            # ID参数
            params = [str(random.randint(1, 100))]
            
            # 其他列参数 - 使用与insert相同的逻辑
            for col in non_primary_cols:
                if MySQLDataType.is_numeric(col.data_type):
                    params.append(str(random.randint(-100, 1000)))
                elif MySQLDataType.is_string(col.data_type):
                    # 获取列的最大长度
                    max_length = col.size if col.size else 8
                    
                    # 获取数据类型的最大限制
                    type_max_length = self._get_max_type_length(col.data_type)
                    
                    # 使用较小的值作为实际限制
                    actual_max_length = min(max_length, type_max_length)
                    
                    # 边界值测试策略
                    rand_val = random.random()
                    if rand_val < 0.1:
                        # 极限边界测试（95-100%的最大长度）
                        length = max(1, int(actual_max_length * random.uniform(0.95, 1.0)))
                    elif rand_val < 0.3:
                        # 接近边界值（80-95%的最大长度）
                        length = max(1, int(actual_max_length * random.uniform(0.8, 0.95)))
                    else:
                        # 普通值（1-50%的最大长度）
                        length = random.randint(1, max(1, int(actual_max_length * 0.5)))
                    
                    # 限制长度，避免过长
                    length = min(length, 1000)
                    
                    rand_str = ''.join(random.choice('abcdefghijklmnopqrstuvwxyz') 
                                      for _ in range(length))
                    params.append(f"'{rand_str}'")
                else:
                    params.append("NULL")
            
            return f"CALL {proc_name}({', '.join(params)})"
        
        elif operation == "delete":
            # 删除存储过程需要ID和输出参数
            # 修正调用方式，先设置变量，然后调用存储过程
            random_id = random.randint(1, 100)
            # 这里不能直接返回三条语句，所以只返回CALL语句，
            # 在gen_transaction方法中需要特殊处理delete存储过程调用
            return f"CALL {proc_name}({random_id}, @rows_affected)"
        
        # 默认情况下调用查询存储过程
        return f"CALL sp_get_{table_name}({random.randint(1, 100)})"

    def gen_statement(self) -> str:
        while True:
            rand = random.random()
            if rand < 0.2:
                return self.gen_select_statement()
            elif rand < 0.65:  # 稍微调整概率
                if random.random() < 0.5:
                    return self.gen_insert_statement()
                else:
                    return self.gen_update_statement()
            elif rand < 0.85:
                return self.gen_delete_statement()
            else:  # 15%概率生成存储过程调用
                statement = self.gen_call_procedure_statement()
                # 特殊处理带输出参数的存储过程调用
                if "sp_delete_" in statement:
                    # 提取表名和ID
                    parts = statement.split("(")
                    proc_part = parts[0].strip()
                    table_name = proc_part.replace("CALL sp_delete_", "")
                    id_part = parts[1].split(",")[0].strip()
                    
                    # 构建完整的调用序列
                    return f"SET @rows_affected = 0;\n{statement};\nSELECT @rows_affected AS affected_rows"
                return statement

    def gen_transaction(self, tx_id: int) -> List[str]:
        statements = ["BEGIN"]
        n = random.randint(1, 10)  # 生成1-10条语句
        
        savepoints = []  # 用于存储保存点名称
        has_rollback_to = False  # 标记是否已经使用过ROLLBACK TO
        
        for i in range(n):
            # 随机决定是否插入SAVEPOINT（不限制数量）
            if random.random() < 0.3:
                savepoint_name = f"sp{tx_id}_{i}"
                statements.append(f"SAVEPOINT {savepoint_name}")
                savepoints.append(savepoint_name)
            
            # 生成语句
            statement = self.gen_statement()
            
            # 处理多行语句
            if ";\n" in statement:
                multi_statements = statement.split(";\n")
                for s in multi_statements:
                    if s:  # 确保不添加空语句
                        statements.append(s.strip())
            else:
                statements.append(statement)
            
            # 随机决定是否回滚到某个保存点（最多一次）
            if savepoints and random.random() < 0.2 and not has_rollback_to:
                savepoint_to_rollback = random.choice(savepoints)
                statements.append(f"ROLLBACK TO {savepoint_to_rollback}")
                has_rollback_to = True  # 标记已使用过ROLLBACK TO
        
        # 最终提交或回滚
        statements.append("COMMIT" if random.random() < 0.7 else "ROLLBACK")
        return statements

    def gen_condition(self) -> str:
        """生成一个条件表达式"""
        conditions = [
            f"{random.randint(-1000, 1000)} > 0",
            f"{random.randint(-1000, 1000)} < 0",
            f"{random.randint(-1000, 1000)} = 0",
            f"table_0.c{random.randint(0, 7)} IS NULL",
            f"table_0.c{random.randint(0, 7)} IS NOT NULL",
            f"table_0.c{random.randint(0, 7)} > {random.randint(-1000, 1000)}",
            f"table_0.c{random.randint(0, 7)} < {random.randint(-1000, 1000)}",
            f"table_0.c{random.randint(0, 7)} = {random.randint(-1000, 1000)}",
            f"table_0.c{random.randint(0, 7)} BETWEEN {random.randint(-1000, 1000)} AND {random.randint(-1000, 1000)}",
            f"table_0.c{random.randint(0, 7)} IN ({random.randint(-1000, 1000)}, {random.randint(-1000, 1000)})"
        ]
        return random.choice(conditions)

    def _gen_random_string(self, length: int = 10) -> str:
        return ''.join(random.choice('abcdefghijklmnopqrstuvwxyz') for _ in range(length)) 