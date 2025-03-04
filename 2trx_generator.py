import random
import threading
import mysql.connector
from MySQLTable import MySQLTableGenerator
from MySQLInitializer import MySQLInitializer
from MySQLParameter import SQLParamGenerator
from MySQLTemplateGen import MySQLTemplateGen
from MySQLASTGen import MySQLASTGen
import logging
import re
import os
import time
from typing import List, Tuple, Dict, Any, Optional, Set, Callable
import queue

log_filename = 'atomicity-checker.log'
if not os.path.exists(log_filename):
    open(log_filename, 'a').close()

database_save_dir = "db_save"
# 确保数据库保存目录存在
if not os.path.exists(database_save_dir):
    os.makedirs(database_save_dir + "/mod")
    os.makedirs(database_save_dir + "/no_mod")

logger = logging.getLogger('atomicity-checker')
logger.setLevel(logging.DEBUG)
handler = logging.FileHandler(log_filename, encoding='utf-8')
formatter = logging.Formatter('%(asctime)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


# 这是一个MySQL死锁场景构建器，生成一对事务T1和T2，T1和T2会互相等待对方的锁，从而形成死锁
# 统一命名规范：使用全大写表示常量
ISOLATION_LEVELS = {
    "RU": "READ UNCOMMITTED",
    "RC": "READ COMMITTED",
    "RR": "REPEATABLE READ",
    "SER": "SERIALIZABLE"
}

# 锁的层次结构重新组织
LOCK_HIERARCHY = {
    "row": {
        "S": "shared",           # 共享锁
        "X": "exclusive",        # 排他锁
        "GAP": "gap",           # 间隙锁
        "NK": "next-key",       # 临键锁
        "II": "insert-intent"   # 插入意向锁
    },
    "table": {
        "TR": "table_read",     # 表读锁
        "TW": "table_write",    # 表写锁
        "IS": "intent_shared",  # 意向共享锁
        "IX": "intent_excl",    # 意向排他锁
        "AI": "auto_inc",       # 自增锁
        "MDL_S": "mdl_shared",  # 元数据共享锁
        "MDL_X": "mdl_exclusive" # 元数据排他锁
    },
    "global": {
        "GL": "global"          # 全局锁
    }
}

# 锁模板重新设计
LOCK_TEMPLATES = {
    # 行级锁模板 (row level)
    "S": {  # 共享锁
        "basic": "SELECT {select_cols} FROM {table} WHERE {cond} LOCK IN SHARE MODE",
        "index": "SELECT {select_cols} FROM {table} FORCE INDEX({idx}) WHERE {cond} LOCK IN SHARE MODE",
        # "join": "SELECT {cols} FROM {table1} INNER JOIN {table2} ON {join_cond} WHERE {cond} LOCK IN SHARE MODE",
        "range": "SELECT {select_cols} FROM {table} WHERE id BETWEEN {v1} AND {v2} LOCK IN SHARE MODE"
    },
    "X": {  # 排他锁
        "basic": "SELECT {select_cols} FROM {table} WHERE {cond} FOR UPDATE",
        "update": "UPDATE {table} SET {set_expr} WHERE {cond}",
        "delete": "DELETE FROM {table} WHERE {cond}",
        "index": "SELECT {select_cols} FROM {table} FORCE INDEX({idx}) WHERE {cond} FOR UPDATE",
        "range": "SELECT {select_cols} FROM {table} WHERE id BETWEEN {v1} AND {v2} FOR UPDATE" # TODO:range模板目前只支持id列
    },
    "GAP": {  # 间隙锁
        "basic": "SELECT {select_cols} FROM {table} WHERE {gap_lock_cond} FOR UPDATE",
        "range": "SELECT {select_cols} FROM {table} WHERE id BETWEEN {v1} AND {v2} FOR UPDATE",
        "index": "SELECT {select_cols} FROM {table} FORCE INDEX({idx}) WHERE {gap_lock_cond} FOR UPDATE",
        "insert": "INSERT INTO {table} ({insert_cols}) VALUES ({insert_vals})"
    },
    "NK": {  # 临键锁
        "range": "SELECT {select_cols} FROM {table} WHERE id >= {v1} AND id <= {v2} FOR UPDATE",
        "insert": "INSERT INTO {table} ({insert_cols}) VALUES ({insert_vals})"
    },
    "II": {  # 插入意向锁
        "basic": "INSERT INTO {table} ({insert_cols}) VALUES ({insert_vals})",
        "insert": "INSERT INTO {table} ({insert_cols}) VALUES ({insert_vals}) ON DUPLICATE KEY UPDATE {update_expr}",
        "range": "SELECT {select_cols} FROM {table} WHERE id BETWEEN {v1} AND {v2} FOR UPDATE"
    },
    # 表级锁模板 (table level)
    "TR": {  # 表读锁
        "basic": "LOCK TABLES {table} READ",
        "multiple": "LOCK TABLES {table1} READ, {table2} READ",
        "as": "LOCK TABLES {table} AS {alias} READ"
    },
    "TW": {  # 表写锁
        "basic": "LOCK TABLES {table} WRITE",
        "multiple": "LOCK TABLES {table1} WRITE, {table2} WRITE",
        "as": "LOCK TABLES {table} AS {alias} WRITE"
    },
    "IS": {  # 意向共享锁
        "basic": "SELECT {select_cols} FROM {table} WHERE {cond} LOCK IN SHARE MODE",
        # "index": "SELECT {select_cols} FROM {table} FORCE INDEX({idx}) WHERE {cond} LOCK IN SHARE MODE",
        "range": "SELECT {select_cols} FROM {table} WHERE {col} BETWEEN {v1} AND {v2} LOCK IN SHARE MODE"
    },
    "IX": {  # 意向排他锁
        "basic": "SELECT {select_cols} FROM {table} WHERE {cond} FOR UPDATE",
        "update": "UPDATE {table} SET {set_expr} WHERE {cond}",
        "delete": "DELETE FROM {table} WHERE {cond}",
    },
    "AI": {  # 自增锁
        "basic": "INSERT INTO {table} ({auto_inc_col}, {cols}) VALUES (NULL, {vals})",
        # "batch": "INSERT INTO {table} ({cols}) VALUES {multi_vals}"
    },
    "MDL_S": {  # 元数据共享锁，做crud操作时
        "basic": "SELECT {select_cols} FROM {table} WHERE {cond} LOCK IN SHARE MODE",
        "delete": "DELETE FROM {table} WHERE {cond}",
        "update": "UPDATE {table} SET {set_expr} WHERE {cond}",
        "insert": "INSERT INTO {table} ({cols}) VALUES ({vals})"
    },
    "MDL_X": {  # 元数据排他锁，修改表结构时    
        "add_column": "ALTER TABLE {table} ADD COLUMN {col_def}",
        "modify_column": "ALTER TABLE {table} MODIFY COLUMN {col_def}",
        "add_index": "ALTER TABLE {table} ADD INDEX {idx_name}({cols})",
        "drop_index": "ALTER TABLE {table} DROP INDEX {idx_name}",
        "rename": "RENAME TABLE {old_table} TO {new_table}",
        "truncate": "TRUNCATE TABLE {table}"
    },
    
    # 全局锁模板 (global level)
    "GL": {  # 全局锁
        "basic": "FLUSH TABLES WITH READ LOCK",
        "unlock": "UNLOCK TABLES",
    }
}
iso_lock_support = {
    "RU": {
        "row": ["X"],  # READ UNCOMMITTED 只支持排他锁(X锁)
        "table": ["S", "X", "MDL_S", "MDL_X"],  # 支持表级读写锁和元数据锁
        "global": ["GL"]  # 支持全局锁
    },
    "RC": {
        "row": ["S", "X", "II"],  # READ COMMITTED 支持共享锁、排他锁和插入意向锁
        "table": ["TR", "TW", "IS", "IX", "AI", "MDL_S", "MDL_X"],  
        "global": ["GL"]  # 支持全局锁
    }, 
    "RR": {
        "row": ["S", "X", "GAP", "NK", "II"],  # REPEATABLE READ 支持所有类型的行锁
        "table": ["TR", "TW", "IS", "IX", "AI", "MDL_S", "MDL_X"],  
        "global": ["GL"]  # 支持全局锁
    },
    "SER": {
        "row": ["S", "X", "GAP", "NK", "II"],  # SERIALIZABLE 支持所有类型的行锁
        "table": ["TR", "TW", "IS", "IX", "AI", "MDL_S", "MDL_X"],  
        "global": ["GL"]  # 支持全局锁
    }
}

iso_lock_template = {
    "RU": {
        "row": {"X": LOCK_TEMPLATES["X"]},
        "table": {"TR": LOCK_TEMPLATES["TR"], "TW": LOCK_TEMPLATES["TW"], "MDL_S": LOCK_TEMPLATES["MDL_S"], "MDL_X": LOCK_TEMPLATES["MDL_X"]},
        "global": {"GL": LOCK_TEMPLATES["GL"]}
    },
    "RC": {
        "row": {"S": LOCK_TEMPLATES["S"], "X": LOCK_TEMPLATES["X"], "II": LOCK_TEMPLATES["II"]},
        "table": {"TR": LOCK_TEMPLATES["TR"], "TW": LOCK_TEMPLATES["TW"], "IS": LOCK_TEMPLATES["IS"], "IX": LOCK_TEMPLATES["IX"], "AI": LOCK_TEMPLATES["AI"], "MDL_S": LOCK_TEMPLATES["MDL_S"], "MDL_X": LOCK_TEMPLATES["MDL_X"]},
        "global": {"GL": LOCK_TEMPLATES["GL"]}
    },
    "RR": {
        "row": {"S": LOCK_TEMPLATES["S"], "X": LOCK_TEMPLATES["X"], "GAP": LOCK_TEMPLATES["GAP"], "NK": LOCK_TEMPLATES["NK"], "II": LOCK_TEMPLATES["II"]},
        "table": {"TR": LOCK_TEMPLATES["TR"], "TW": LOCK_TEMPLATES["TW"], "IS": LOCK_TEMPLATES["IS"], "IX": LOCK_TEMPLATES["IX"], "AI": LOCK_TEMPLATES["AI"], "MDL_S": LOCK_TEMPLATES["MDL_S"], "MDL_X": LOCK_TEMPLATES["MDL_X"]},
        "global": {"GL": LOCK_TEMPLATES["GL"]}
    },
    "SER": {
        "row": {"S": LOCK_TEMPLATES["S"], "X": LOCK_TEMPLATES["X"], "GAP": LOCK_TEMPLATES["GAP"], "NK": LOCK_TEMPLATES["NK"], "II": LOCK_TEMPLATES["II"]},
        "table": {"TR": LOCK_TEMPLATES["TR"], "TW": LOCK_TEMPLATES["TW"], "IS": LOCK_TEMPLATES["IS"], "IX": LOCK_TEMPLATES["IX"], "AI": LOCK_TEMPLATES["AI"], "MDL_S": LOCK_TEMPLATES["MDL_S"], "MDL_X": LOCK_TEMPLATES["MDL_X"]},
        "global": {"GL": LOCK_TEMPLATES["GL"]}
    }   
}

# True表示兼容（可以同时持有），False表示不兼容（互斥）
# 行级锁的兼容性矩阵
# 横向是已经持有的锁，纵向是正在请求的锁
row_lock_compatibility = {
    #        S      X      GAP    NK     II
    "S":  {"S": True,  "X": False, "GAP": True,  "NK": False, "II": True},   # 共享锁
    "X":  {"S": False, "X": False, "GAP": True,  "NK": False, "II": True},   # 排他锁
    "GAP":{"S": True,  "X": True,  "GAP": True,  "NK": True,  "II": True},   # 间隙锁
    "NK": {"S": False, "X": False, "GAP": True,  "NK": False, "II": True},   # Next-Key锁
    "II": {"S": True,  "X": True,  "GAP": False, "NK": False, "II": False}   # 插入意向锁 - 修改这里，II锁之间互斥
}

# 表级锁的兼容性矩阵
table_lock_compatibility = {
    #        IS     IX      S      X      AI
    "IS": {"IS": True, "IX": True,  "S": True,  "X": False, "AI": True},
    "IX": {"IS": True, "IX": True,  "S": False, "X": False, "AI": True},
    "S":  {"IS": True, "IX": False, "S": True,  "X": False, "AI": False},
    "X":  {"IS": False,"IX": False, "S": False, "X": False, "AI": False},
    "AI": {"IS": True, "IX": True,  "S": False, "X": False, "AI": False}  # AUTO-INC锁
}

def get_iso_template(isolation_level: str):
    """获取隔离级别和锁层次的模板"""
    return iso_lock_template[isolation_level]

class DatabaseConnection:
    """数据库连接管理类"""
    def __init__(self, host: str, user: str, password: str, database: str, port: int):
        self.config = {
            "host": host,
            "user": user,
            "password": password,
            "database": database,
            "port": port
        }
    def create_connection(self):
       try:
           return mysql.connector.connect(**self.config)
       except mysql.connector.Error as err:
           logger.error(f"数据库连接失败: {err}")
           raise
    def get_result(self, sql: str) -> List[Tuple]:
       conn = self.create_connection()
       try:
           with conn.cursor() as cursor:
               cursor.execute(sql)
               return cursor.fetchall()
       finally:
           conn.close()

class DeadlockGenerator:
    """死锁场景生成器"""
    def __init__(self, isolation_level: str, lock_hierarchy: dict, 
                lock_templates: dict, total_rows_num: int, host: str, user: str, 
                password: str, database: str, port: int):
       """
       初始化死锁生成器
       """
       self.isolation_level = isolation_level
       self.lock_hierarchy = lock_hierarchy
       self.lock_templates = lock_templates
       self.table_name = "table_0"
       self.trx1_lock_rows_num = None
       self.trx2_lock_rows_num = None
       self.predicted_rollback_trx_id = None
       self.intersection_size = None
       self.total_rows_num = total_rows_num

       # 数据库连接管理
       self.db = DatabaseConnection(host, user, password, database, port)
       self.conn1 = self.db.create_connection()
       self.conn2 = self.db.create_connection()
       self.cursor1 = self.conn1.cursor()
       self.cursor2 = self.conn2.cursor()
        # 事务SQL语句列表
       self.trx1: List[str] = []
       self.trx2: List[str] = []
    
       # 初始化兼容/不兼容锁对
       self.compatible_lock_pairs = self._init_compatible_lock_pairs()
       self.incompatible_lock_pairs = self._init_incompatible_lock_pairs()
    
    def _get_table_metadata(self) -> Tuple[List[str], List[str], List[str], List[str]]:
        """获取表的元数据信息
        
        Returns:
            Tuple[List[str], List[str], List[str], List[str]]: 返回(列名列表, 列类型列表, 主键列表, 索引名称列表)
        """
        try:
            # 获取列信息
            self.cursor1.execute(f"DESCRIBE {self.table_name}")
            columns = [(row[0], row[1]) for row in self.cursor1.fetchall()]
            column_names = [col[0] for col in columns]
            column_types = [col[1] for col in columns]
            
            # 获取主键信息
            self.cursor1.execute(f"""
                SELECT COLUMN_NAME 
                FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                WHERE TABLE_SCHEMA = '{self.db.config['database']}'
                    AND TABLE_NAME = '{self.table_name}'
                    AND CONSTRAINT_NAME = 'PRIMARY'
                ORDER BY ORDINAL_POSITION
            """)
            primary_keys = [row[0] for row in self.cursor1.fetchall()]
            
            # 获取索引信息
            self.cursor1.execute(f"""
                SELECT DISTINCT INDEX_NAME
                FROM INFORMATION_SCHEMA.STATISTICS 
                WHERE TABLE_SCHEMA = '{self.db.config['database']}'
                    AND TABLE_NAME = '{self.table_name}'
                    AND INDEX_NAME != 'PRIMARY'
            """)
            indexes = [row[0] for row in self.cursor1.fetchall()]
            
            return column_names, column_types, primary_keys, indexes
        
        except mysql.connector.Error as err:
            logger.error(f"获取表元数据失败: {err}")
            raise
    
    def verify_lock(self, target_rows, generated_sql):
        conn = self.db.create_connection()
        cursor = conn.cursor()
        cursor.execute(generated_sql)
        actual_rows = cursor.fetchall()
        cursor.close()
        conn.close()
        return set(actual_rows) == set(target_rows)
       
    def _generate_lock_sql(self, template_key: str, lock_level: str, 
                        lock_type: str, row_idx: int, 
                        is_continuous: bool = False, 
                        range_end_idx: int = None) -> str:
        """统一的SQL生成方法"""
        try:            
            column_names, column_types, primary_keys, indexes = self._get_table_metadata()
            
            # 获取行数据
            if is_continuous:
                self.cursor1.execute(
                    f"SELECT * FROM {self.table_name} LIMIT {range_end_idx - row_idx + 1} OFFSET {row_idx - 1}"
                )
            else:
                self.cursor1.execute(
                    f"SELECT * FROM {self.table_name} LIMIT 1 OFFSET {row_idx - 1}"
                )
            rows = self.cursor1.fetchall()
            
            use_template = False
            use_ast = True
            
            if use_ast:
                ast_gen = MySQLASTGen(self.table_name, rows, column_names, column_types, primary_keys, lock_type)
                # 根据锁类型选择合适的语句类型
                if lock_type in ["GAP", "NK", "II"]:
                    stmt_type = random.choice(["INSERT", "UPDATE", "SELECT", "DELETE"])
                else:
                    stmt_type = random.choice(["SELECT", "UPDATE", "DELETE"])  # 其他锁类型可以随机选择
        
                return ast_gen.generate_lock_sql(stmt_type)
            else:
                # 使用模板生成器生成SQL
                template_gen = MySQLTemplateGen(self.lock_templates, self.table_name)
                return template_gen.generate_lock_sql(
                    template_key, lock_level, lock_type, row_idx,
                    is_continuous, range_end_idx,
                    column_names, column_types, primary_keys, indexes, rows
                )
                
        except Exception as e:
            logger.error(f"生成锁SQL失败: {e}")
            raise

    def _init_resource_distribution(self):
        """初始化资源分配场景"""
        try:
            
            # 获取表的总行数
            # total_rows_num = self.db.get_result(f"SELECT COUNT(*) FROM {self.table_name}")[0][0]
            print(f"table {self.table_name} has {self.total_rows_num} rows")
            logger.info(f"table {self.table_name} has {self.total_rows_num} rows")
            self.lock_same_resource = random.choice([True, False])
            
            # 随机决定是否使用连续锁定
            self.trx1_lock_rows_continuous = random.choice([True, False])
            self.trx2_lock_rows_continuous = random.choice([True, False])
            if self.lock_same_resource is True:
                res_dict =  self._init_intersection_scenario()
            else:
                res_dict =  self._init_non_intersection_scenario()
            return res_dict,self.lock_same_resource
        except Exception as e:
            logger.error(f"初始化资源分配失败: {e}")
            raise
        
    def _init_intersection_scenario(self) -> Dict:
        """初始化有交集的场景"""

        # max_intersection = min(self.trx1_lock_rows_num, self.trx2_lock_rows_num)
        # intersection_size = random.randint(1, max_intersection)

        # if total_rows_num < (self.trx1_lock_rows_num + self.trx2_lock_rows_num - intersection_size):
        #     print(f"total_rows_num: {total_rows_num}, trx1_lock_rows_num: {self.trx1_lock_rows_num}, trx2_lock_rows_num: {self.trx2_lock_rows_num}, intersection_size: {intersection_size}")
        #     logger.info(f"total_rows_num: {total_rows_num}, trx1_lock_rows_num: {self.trx1_lock_rows_num}, trx2_lock_rows_num: {self.trx2_lock_rows_num}, intersection_size: {intersection_size}")
        #     raise ValueError("请求锁定的总行数超过表中可用行数")
            
        if self.trx1_lock_rows_continuous and self.trx2_lock_rows_continuous:
            return self._generate_continuous_intersection()
        elif self.trx1_lock_rows_continuous:
            return self._generate_mixed_intersection(trx1_continuous=True)
        elif self.trx2_lock_rows_continuous:
            return self._generate_mixed_intersection(trx1_continuous=False)
        else:
            return self._generate_discrete_intersection()
        
    def _init_non_intersection_scenario(self) -> Dict:
        """初始化无交集的场景"""
        if self.trx1_lock_rows_continuous and self.trx2_lock_rows_continuous:
            return self._generate_continuous_nonintersection()
        elif self.trx1_lock_rows_continuous:
            return self._generate_mixed_nonintersection(trx1_continuous=True)
        elif self.trx2_lock_rows_continuous:
            return self._generate_mixed_nonintersection(trx1_continuous=False)
        else:
            return self._generate_discrete_nonintersection()
    
    def _generate_continuous_intersection(self) -> Dict:
        """生成连续锁定场景，两个事务都是连续锁定"""
        try:
            total_needed_rows = random.randint(1, self.total_rows_num)
            
            # 随机选择起始位置
            start_id = random.randint(1, self.total_rows_num - total_needed_rows + 1)
            end_id = start_id + total_needed_rows - 1

            trx1_start_id = start_id
            trx1_end_id = random.randint(trx1_start_id, end_id)
            trx2_start_id = random.randint(trx1_start_id, trx1_end_id)
            trx2_end_id = end_id

            intersection_start_id = trx2_start_id
            intersection_end_id = trx1_end_id

            intersection_size = intersection_end_id - intersection_start_id + 1
            
            # 设置事务1的范围
            self.trx1_start_id = start_id
            self.trx1_end_id = intersection_end_id
            self.trx1_lock_rows_num = trx1_end_id - trx1_start_id + 1
            self.trx1_exclusive_start_id = start_id
            self.trx1_exclusive_end_id = intersection_start_id - 1

            # 设置事务2的范围，确保包含交集部分
            self.trx2_start_id = trx2_start_id
            self.trx2_end_id = trx2_end_id
            self.trx2_exclusive_start_id = intersection_end_id + 1
            self.trx2_exclusive_end_id = end_id
            self.trx2_lock_rows_num = trx2_end_id - trx2_start_id + 1

            self.intersection_size = intersection_size

            # 计算交集
            self.intersection_ids = list(range(
                intersection_start_id,
                intersection_end_id + 1
            ))
            
            # 计算独占行
            self.trx1_exclusive_ids = list(range(self.trx1_exclusive_start_id, self.trx1_exclusive_end_id + 1)) if self.trx1_exclusive_start_id <= self.trx1_exclusive_end_id else []
            self.trx2_exclusive_ids = list(range(self.trx2_exclusive_start_id, self.trx2_exclusive_end_id + 1)) if self.trx2_exclusive_start_id <= self.trx2_exclusive_end_id else []
            
            # 设置完整的行ID列表
            self.trx1_lock_row_ids = list(range(self.trx1_start_id, self.trx1_end_id + 1))
            self.trx2_lock_row_ids = list(range(self.trx2_start_id, self.trx2_end_id + 1))

            # 打印全部参数
            logger.info(f"trx1_start_id: {self.trx1_start_id}")
            logger.info(f"trx1_end_id: {self.trx1_end_id}")
            logger.info(f"trx1_exclusive_start_id: {self.trx1_exclusive_start_id}")
            logger.info(f"trx1_exclusive_end_id: {self.trx1_exclusive_end_id}")
            logger.info(f"trx1_exclusive_ids: {self.trx1_exclusive_ids}")
            logger.info(f"trx2_start_id: {self.trx2_start_id}")
            logger.info(f"trx2_end_id: {self.trx2_end_id}")
            logger.info(f"trx2_exclusive_start_id: {self.trx2_exclusive_start_id}")
            logger.info(f"trx2_exclusive_end_id: {self.trx2_exclusive_end_id}")
            logger.info(f"trx2_exclusive_ids: {self.trx2_exclusive_ids}")
            logger.info(f"intersection_ids: {self.intersection_ids}")
            return {
                'trx1_lock_row_ids': self.trx1_lock_row_ids,
                'trx2_lock_row_ids': self.trx2_lock_row_ids,
                'intersection_row_ids': self.intersection_ids
            }
        except Exception as e:
            logger.error(f"生成连续锁定场景失败: {e}")
            raise

    def _generate_mixed_intersection(self, trx1_continuous: bool) -> Dict:
        """
        生成混合锁定场景，一个事务连续锁定，另一个事务离散锁定
        """
        try:
            if trx1_continuous:
                # 事务1连续锁定
                trx1_lock_rows_num = random.randint(1, self.total_rows_num)
                trx1_start_id = random.randint(1, self.total_rows_num - trx1_lock_rows_num + 1)
                trx1_end_id = trx1_start_id + trx1_lock_rows_num - 1
                trx1_rows = list(range(trx1_start_id, trx1_end_id + 1))
                
                # 从事务1的行中随机选择交集
                intersection_size = random.randint(1, trx1_lock_rows_num)
                intersection_rows = sorted(random.sample(trx1_rows, intersection_size))

                trx1_exclusive = [id for id in trx1_rows if id not in intersection_rows]
                
                # 为事务2选择剩余的行
                remaining_rows = [id for id in range(1, self.total_rows_num + 1) if id not in trx1_rows]
                trx2_exclusive_num = random.randint(0, len(remaining_rows))
                trx2_exclusive = sorted(random.sample(remaining_rows, trx2_exclusive_num)) if trx2_exclusive_num > 0 else []
                trx2_rows = sorted(intersection_rows + trx2_exclusive)

                self.trx1_start_id = trx1_start_id
                self.trx1_end_id = trx1_end_id

                self.trx1_lock_rows_num = trx1_lock_rows_num
                self.trx1_exclusive_ids = trx1_exclusive

                self.trx2_lock_rows_num = len(trx2_rows)
                self.trx2_exclusive_ids = trx2_exclusive
                                
            else:
                # 事务2连续锁定
                trx2_lock_rows_num = random.randint(1, self.total_rows_num)
                trx2_start_id = random.randint(1, self.total_rows_num - trx2_lock_rows_num + 1)
                trx2_end_id = trx2_start_id + trx2_lock_rows_num - 1
                trx2_rows = list(range(trx2_start_id, trx2_end_id + 1))

                # 从事务2的行中随机选择交集
                intersection_size = random.randint(1, trx2_lock_rows_num)
                intersection_rows = sorted(random.sample(trx2_rows, intersection_size))

                trx2_exclusive = [id for id in trx2_rows if id not in intersection_rows]
                
                remaining_rows = [id for id in range(1, self.total_rows_num + 1) if id not in trx2_rows]
                trx1_exclusive_num = random.randint(0, len(remaining_rows))
                trx1_exclusive = sorted(random.sample(remaining_rows, trx1_exclusive_num)) if trx1_exclusive_num > 0 else []
                trx1_rows = sorted(intersection_rows + trx1_exclusive)

                self.trx2_start_id = trx2_start_id
                self.trx2_end_id = trx2_end_id

                self.trx2_lock_rows_num = trx2_lock_rows_num
                self.trx2_exclusive_ids = trx2_exclusive

                self.trx1_lock_rows_num = len(trx1_rows)
                self.trx1_exclusive_ids = trx1_exclusive

            # 设置类属性
            self.trx1_lock_row_ids = trx1_rows
            self.trx2_lock_row_ids = trx2_rows
            self.intersection_ids = intersection_rows
            self.intersection_size = intersection_size
            
            # 打印全部参数
            logger.info(f"trx1_lock_row_ids: {self.trx1_lock_row_ids}")
            logger.info(f"trx2_lock_row_ids: {self.trx2_lock_row_ids}")
            logger.info(f"intersection_row_ids: {self.intersection_ids}")
            logger.info(f"trx1_exclusive_ids: {self.trx1_exclusive_ids}")
            logger.info(f"trx2_exclusive_ids: {self.trx2_exclusive_ids}")

            return {
                'trx1_lock_row_ids': self.trx1_lock_row_ids,
                'trx2_lock_row_ids': self.trx2_lock_row_ids,
                'intersection_row_ids': self.intersection_ids
            }
        except Exception as e:
            logger.error(f"生成混合锁定场景失败: {e}")
            raise

    def _generate_discrete_intersection(self) -> Dict:
        """生成离散有交集场景，两个事务都是离散锁定"""
        try:
            # 首先生成交集行
            intersection_size = random.randint(1, self.total_rows_num)
            intersection_rows = sorted(random.sample(range(1, self.total_rows_num + 1), intersection_size))
            
            # 为事务1选择额外的行
            remaining_rows = [id for id in range(1, self.total_rows_num + 1) if id not in intersection_rows]
            trx1_exclusive_num = random.randint(0, len(remaining_rows))
            trx1_exclusive = sorted(random.sample(remaining_rows, trx1_exclusive_num)) if trx1_exclusive_num > 0 else []
            trx1_rows = sorted(list(set(intersection_rows + trx1_exclusive)))  # 使用set去重
            
            # 为事务2选择额外的行
            remaining_rows = [id for id in range(1, self.total_rows_num + 1) if id not in trx1_rows]
            trx2_exclusive_num = random.randint(0, len(remaining_rows))
            trx2_exclusive = sorted(random.sample(remaining_rows, trx2_exclusive_num)) if trx2_exclusive_num > 0 else []
            trx2_rows = sorted(list(set(intersection_rows + trx2_exclusive)))  # 使用set去重
            
            # 设置类属性
            self.intersection_ids = intersection_rows
            self.trx1_exclusive_ids = trx1_exclusive
            self.trx2_exclusive_ids = trx2_exclusive
            self.trx1_lock_row_ids = trx1_rows
            self.trx2_lock_row_ids = trx2_rows
            self.intersection_size = intersection_size
            self.trx1_lock_rows_num = len(trx1_rows)
            self.trx2_lock_rows_num = len(trx2_rows)
            
            # 打印全部参数
            logger.info(f"trx1_lock_row_ids: {self.trx1_lock_row_ids}")
            logger.info(f"trx2_lock_row_ids: {self.trx2_lock_row_ids}")
            logger.info(f"intersection_row_ids: {self.intersection_ids}")
            logger.info(f"trx1_exclusive_ids: {self.trx1_exclusive_ids}")
            logger.info(f"trx2_exclusive_ids: {self.trx2_exclusive_ids}")
            
            return {
                'trx1_lock_row_ids': self.trx1_lock_row_ids,
                'trx2_lock_row_ids': self.trx2_lock_row_ids,
                'intersection_row_ids': self.intersection_ids
            }
        except Exception as e:
            logger.error(f"生成离散有交集场景失败: {e}")
            raise

    def _generate_deadlock_with_intersection(self) -> List[Tuple]:
        """处理有交集情况的死锁生成"""
        serial = [(1, "BEGIN"), (2, "BEGIN")]

        # Phase 1: 对交集资源加锁
        serial = self._handle_intersection_phase1(serial)

        # Phase 2: 对独占资源加锁
        serial = self._handle_intersection_phase2(serial)

        # Phase 3: 形成死锁
        serial = self._handle_intersection_phase3(serial)
        return serial
    
    def _generate_deadlock_without_intersection(self) -> List[Tuple]:
        """处理无交集情况的死锁生成"""
        serial = [(1, "BEGIN"), (2, "BEGIN")]

        # Phase 1: 各自对资源加锁
        serial = self._handle_non_intersection_phase1(serial)

        # Phase 2: 形成死锁
        serial = self._handle_non_intersection_phase2(serial)
        return serial
    
    def _generate_deadlock_trx_serial(self, max_statements: int) -> Tuple[List[str], List[str], List[Tuple]]:
        """生成死锁事务序列的主方法"""
        try:
            
            self.trx1 = ["BEGIN"]
            self.trx2 = ["BEGIN"]
            
            if self.lock_same_resource is True:
                serial = self._generate_deadlock_with_intersection()
            else:
                serial = self._generate_deadlock_without_intersection()
                
            # 添加提交语句
            self.trx1.append("COMMIT;")
            self.trx2.append("COMMIT;")
            who_commit_first = random.choice([0, 1])
            serial.append((who_commit_first + 1, "COMMIT"))
            # 另一个事务提交
            serial.append((2-who_commit_first, "COMMIT"))
            logger.info(f"生成死锁事务序列成功如下")
            logger.info(f"serial: {serial}")
            return self.trx1, self.trx2, serial
            
        except Exception as e:
            logger.error(f"生成死锁事务序列失败: {e}")
            logger.error("")
            raise

    def _handle_intersection_phase1(self, serial: List[Tuple]) -> List[Tuple]:
        """第一阶段：对交集资源加锁"""
        print("intersection phase1")
        logger.info("intersection phase1")
        first_lock, second_lock = self._get_compatible_lock_pair()
        
        # 准备两个事务的SQL语句
        trx1_sql = []
        trx2_sql = []
        
        # 生成两个事务的加锁语句
        if self.trx1_lock_rows_continuous:
            trx1_sql.append(self._generate_lock_sql("range", "row", first_lock, 
                                              self.trx1_start_id, True, self.trx1_end_id))
             
        if self.trx2_lock_rows_continuous:
            trx2_sql.append(self._generate_lock_sql("range", "row", second_lock,
                                              self.trx2_start_id, True, self.trx2_end_id))
        
        # 处理非连续锁定的情况
        if not self.trx1_lock_rows_continuous:
            for row_id in self.intersection_ids:
                lock_template_key = random.choice(list(self.lock_templates["row"][first_lock].keys()))
                trx1_sql.append(self._generate_lock_sql(lock_template_key, "row", first_lock, row_id, False))
                
        if not self.trx2_lock_rows_continuous:
            for row_id in self.intersection_ids:
                lock_template_key = random.choice(list(self.lock_templates["row"][second_lock].keys()))
                trx2_sql.append(self._generate_lock_sql(lock_template_key, "row", second_lock, row_id, False))
        
        # 随机交错两个事务的语句组成序列
        while trx1_sql or trx2_sql:
            if not trx1_sql:
                # 只剩T2的语句
                sql = trx2_sql.pop(0)
                self.trx2.append(sql)
                serial.append((2, sql))
            elif not trx2_sql:
                # 只剩T1的语句
                sql = trx1_sql.pop(0)
                self.trx1.append(sql)
                serial.append((1, sql))
            else:
                # 随机选择执行T1或T2的语句
                if random.choice([True, False]):
                    sql = trx1_sql.pop(0)
                    self.trx1.append(sql)
                    serial.append((1, sql))
                else:
                    sql = trx2_sql.pop(0)
                    self.trx2.append(sql)
                    serial.append((2, sql))

        print("intersection phase1 end")
        print()
        logger.info("intersection phase1 end")
        logger.info("")
        return serial

    def _handle_intersection_phase2(self, serial: List[Tuple]) -> List[Tuple]:
        """第二阶段：对独占资源加锁，锁类型可以改变"""
        print("intersection phase2")
        logger.info("intersection phase2")
        # 为两个事务分别获取一对不兼容的锁
        trx1_phase2_lock, trx2_phase3_lock = self._get_lock_pair(compatible=False)
        trx2_phase2_lock, trx1_phase3_lock = self._get_lock_pair(compatible=False)
        
        print(f"事务1在第二阶段自己加的锁: {trx1_phase2_lock}")
        print(f"事务2在第二阶段自己加的锁: {trx2_phase2_lock}")
        print(f"事务1在第三阶段请求对方资源的锁: {trx1_phase3_lock}")
        print(f"事务2在第三阶段请求对方资源的锁: {trx2_phase3_lock}")

        logger.info(f"事务1在第二阶段自己加的锁: {trx1_phase2_lock}")
        logger.info(f"事务2在第二阶段自己加的锁: {trx2_phase2_lock}")
        logger.info(f"事务1在第三阶段请求对方资源的锁: {trx1_phase3_lock}")
        logger.info(f"事务2在第三阶段请求对方资源的锁: {trx2_phase3_lock}")

        # 准备两个事务的SQL语句
        trx1_sql = []
        trx2_sql = []
        
        # 生成T1的独占资源加锁语句
        if self.trx1_lock_rows_continuous and self.trx2_lock_rows_continuous:
            # 生成T1的独占资源加锁语句
            if self.trx1_exclusive_ids is []:
                sql = None
            else:
                sql = self._generate_lock_sql("range", "row", trx1_phase2_lock,
                                        self.trx1_exclusive_start_id, True, 
                                        self.trx1_exclusive_end_id)
            trx1_sql.append(sql)
        
            # 生成T2的独占资源加锁语句
            if self.trx2_exclusive_ids is []:
                sql = None
            else:
                sql = self._generate_lock_sql("range", "row", trx2_phase2_lock,
                            self.trx2_exclusive_start_id, True, 
                            self.trx2_exclusive_end_id)
            trx2_sql.append(sql)
        else:
            for row_id in self.trx1_exclusive_ids:
                lock_template_keys = list(self.lock_templates["row"][trx1_phase2_lock].keys())
                lock_template_key = random.choice(lock_template_keys)
                sql = self._generate_lock_sql(lock_template_key, "row", trx1_phase2_lock,
                                            row_id, False)
                trx1_sql.append(sql)
        
            # 生成T2的独占资源加锁语句
            for row_id in self.trx2_exclusive_ids:
                lock_template_keys = list(self.lock_templates["row"][trx2_phase2_lock].keys())
                lock_template_key = random.choice(lock_template_keys)
                sql = self._generate_lock_sql(lock_template_key, "row", trx2_phase2_lock,
                                            row_id, False)
                trx2_sql.append(sql)
        
        while trx1_sql or trx2_sql:
            if not trx1_sql:
                # 只剩T2的语句
                sql = trx2_sql.pop(0)
                self.trx2.append(sql)
                serial.append((2, sql))
            elif not trx2_sql:
                # 只剩T1的语句
                sql = trx1_sql.pop(0)
                self.trx1.append(sql)
                serial.append((1, sql))
            else:
                # 随机选择执行T1或T2的语句
                if random.choice([True, False]):
                    sql = trx1_sql.pop(0)
                    self.trx1.append(sql)
                    serial.append((1, sql))
                else:
                    sql = trx2_sql.pop(0)
                    self.trx2.append(sql)
                    serial.append((2, sql))
        
        # 保存阶段三将要使用的锁
        self._phase3_locks = (trx1_phase3_lock, trx2_phase3_lock)
        print("intersection phase2 end")
        print()
        logger.info("intersection phase2 end")
        logger.info("")
        return serial

    def _handle_intersection_phase3(self, serial: List[Tuple]) -> List[Tuple]:
        """第三阶段：形成死锁"""
        # 使用阶段二中保存的锁对
        print("intersection phase3")
        logger.info("intersection phase3")
        trx1_phase3_lock, trx2_phase3_lock = self._phase3_locks
        
        # 准备两个事务的SQL语句
        trx1_sql = None
        trx2_sql = None
        
        # T1请求T2的独占资源
        if len(self.trx2_exclusive_ids) > 0:  # 确保T2有独占资源
            if self.trx2_lock_rows_continuous and self.trx1_lock_rows_continuous:
                ask_for_continuous_lock = random.choice([True, False])
                if ask_for_continuous_lock:
                    start_id = random.randint(self.trx2_exclusive_start_id, self.trx2_exclusive_end_id)
                    end_id = random.randint(start_id, self.trx2_exclusive_end_id)
                    trx1_sql = self._generate_lock_sql("range", "row", trx1_phase3_lock,
                                        start_id, True, end_id)
                else:
                    lock_template_key = random.choice(list(self.lock_templates["row"][trx1_phase3_lock].keys()))
                    trx1_sql = self._generate_lock_sql(lock_template_key, "row", trx1_phase3_lock,
                                        random.choice(self.trx2_exclusive_ids), False)
            elif self.trx2_lock_rows_continuous and not self.trx1_lock_rows_continuous:
                ask_for_continuous_lock = random.choice([True, False])
                if ask_for_continuous_lock:
                    start_id = random.randint(self.trx2_start_id, self.trx2_end_id)
                    end_id = random.randint(start_id, self.trx2_end_id)
                    trx1_sql = self._generate_lock_sql("range", "row", trx1_phase3_lock,
                                        start_id, True, end_id)
                else:
                    lock_template_key = random.choice(list(self.lock_templates["row"][trx1_phase3_lock].keys()))
                    trx1_sql = self._generate_lock_sql(lock_template_key, "row", trx1_phase3_lock,
                                        random.choice(self.trx2_exclusive_ids), False)
            else:
                lock_template_key = random.choice(list(self.lock_templates["row"][trx1_phase3_lock].keys()))
                trx1_sql = self._generate_lock_sql(lock_template_key, "row", trx1_phase3_lock,
                                    random.choice(self.trx2_exclusive_ids), False)
        else:
            # 如果T2没有独占资源，则请求一个交集资源
            lock_template_key = random.choice(list(self.lock_templates["row"][trx1_phase3_lock].keys()))
            trx1_sql = self._generate_lock_sql(lock_template_key, "row", trx1_phase3_lock,
                                    random.choice(self.intersection_ids), False)
        
        # T2请求T1的独占资源
        if len(self.trx1_exclusive_ids) > 0:  # 确保T1有独占资源
            if self.trx1_lock_rows_continuous and self.trx2_lock_rows_continuous:
                ask_for_continuous_lock = random.choice([True, False])
                if ask_for_continuous_lock:
                    start_id = random.randint(self.trx1_exclusive_start_id, self.trx1_exclusive_end_id)
                    end_id = random.randint(start_id, self.trx1_exclusive_end_id)
                    trx2_sql = self._generate_lock_sql("range", "row", trx2_phase3_lock,
                                        start_id, True, end_id)
                else:
                    lock_template_key = random.choice(list(self.lock_templates["row"][trx2_phase3_lock].keys()))
                    trx2_sql = self._generate_lock_sql(lock_template_key, "row", trx2_phase3_lock,
                                        random.choice(self.trx1_exclusive_ids), False)
            elif self.trx1_lock_rows_continuous and not self.trx2_lock_rows_continuous:
                ask_for_continuous_lock = random.choice([True, False])
                if ask_for_continuous_lock:
                    start_id = random.randint(self.trx1_start_id, self.trx1_end_id)
                    end_id = random.randint(start_id, self.trx1_end_id)
                    trx2_sql = self._generate_lock_sql("range", "row", trx2_phase3_lock,
                                        start_id, True, end_id)
                else:
                    lock_template_key = random.choice(list(self.lock_templates["row"][trx2_phase3_lock].keys()))
                trx2_sql = self._generate_lock_sql(lock_template_key, "row", trx2_phase3_lock,
                                    random.choice(self.trx1_exclusive_ids), False)
        else:
            # 如果T1没有独占资源，则请求一个交集资源
            lock_template_key = random.choice(list(self.lock_templates["row"][trx2_phase3_lock].keys()))
            trx2_sql = self._generate_lock_sql(lock_template_key, "row", trx2_phase3_lock,
                                    random.choice(self.intersection_ids), False)
        
        # 随机决定执行顺序
        if random.choice([True, False]):
            self.trx1.append(trx1_sql)
            serial.append((1, trx1_sql))
            self.trx2.append(trx2_sql)
            serial.append((2, trx2_sql))
        else:
            self.trx2.append(trx2_sql)
            serial.append((2, trx2_sql))
            self.trx1.append(trx1_sql)
            serial.append((1, trx1_sql))

        print("intersection phase3 end")
        print()
        logger.info("intersection phase3 end")
        logger.info("")
        return serial

    def _handle_non_intersection_phase1(self, serial: List[Tuple]) -> List[Tuple]:
        """处理无交集场景的第一阶段：各自对资源加锁"""
        print("non intersection phase1")
        logger.info("non intersection phase1")
        try:
            # 为两个事务分别获取一对不兼容的锁
            trx1_phase1_lock, trx2_phase2_lock = self._get_lock_pair(compatible=False)
            trx2_phase1_lock, trx1_phase2_lock = self._get_lock_pair(compatible=False)
            
            print(f"事务1在第一阶段自己加的锁: {trx1_phase1_lock}")
            print(f"事务2在第一阶段自己加的锁: {trx2_phase1_lock}")
            print(f"事务1在第二阶段请求对方资源的锁: {trx1_phase2_lock}")
            print(f"事务2在第二阶段请求对方资源的锁: {trx2_phase2_lock}")

            logger.info(f"事务1在第一阶段自己加的锁: {trx1_phase1_lock}")
            logger.info(f"事务2在第一阶段自己加的锁: {trx2_phase1_lock}")
            logger.info(f"事务1在第二阶段请求对方资源的锁: {trx1_phase2_lock}")
            logger.info(f"事务2在第二阶段请求对方资源的锁: {trx2_phase2_lock}")
            
            # 准备两个事务的SQL语句
            trx1_sql = []
            trx2_sql = []
            
            # 根据是否连续锁定选择不同的加锁策略
            if self.trx1_lock_rows_continuous:
                sql = self._generate_lock_sql("range", "row", trx1_phase1_lock,
                                            self.trx1_start_id, True, self.trx1_end_id)
                trx1_sql.append(sql)
            else:
                for row_id in self.trx1_lock_row_ids:
                    lock_template_keys = list(self.lock_templates["row"][trx1_phase1_lock].keys())
                    lock_template_key = random.choice(lock_template_keys)
                    sql = self._generate_lock_sql(lock_template_key, "row", trx1_phase1_lock,
                                                row_id, False)
                    trx1_sql.append(sql)
            
            if self.trx2_lock_rows_continuous:
                sql = self._generate_lock_sql("range", "row", trx2_phase1_lock,
                                            self.trx2_start_id, True, self.trx2_end_id)
                trx2_sql.append(sql)
            else:
                for row_id in self.trx2_lock_row_ids:
                    lock_template_keys = list(self.lock_templates["row"][trx2_phase1_lock].keys())
                    lock_template_key = random.choice(lock_template_keys)
                    sql = self._generate_lock_sql(lock_template_key, "row", trx2_phase1_lock,
                                                row_id, False)
                    trx2_sql.append(sql)
            
            while trx1_sql or trx2_sql:
                if not trx1_sql:
                    # 只剩T2的语句
                    sql = trx2_sql.pop(0)
                    self.trx2.append(sql)
                    serial.append((2, sql))
                elif not trx2_sql:
                    # 只剩T1的语句
                    sql = trx1_sql.pop(0)
                    self.trx1.append(sql)
                    serial.append((1, sql))
                else:
                    # 随机选择执行T1或T2的语句
                    if random.choice([True, False]):
                        sql = trx1_sql.pop(0)
                        self.trx1.append(sql)
                        serial.append((1, sql))
                    else:
                        sql = trx2_sql.pop(0)
                        self.trx2.append(sql)
                        serial.append((2, sql))
            
            # 保存阶段二将要使用的锁
            self._phase2_locks = (trx1_phase2_lock, trx2_phase2_lock)

            print("non intersection phase1 end")
            print()
            logger.info("non intersection phase1 end")
            logger.info("")
            return serial
            
        except Exception as e:
            logger.error(f"处理无交集第一阶段失败: {e}")
            raise

    def _handle_non_intersection_phase2(self, serial: List[Tuple]) -> List[Tuple]:
        """处理无交集场景的第二阶段：形成死锁"""
        print("non intersection phase2")
        logger.info("non intersection phase2")
        try:
            # 使用阶段一中保存的锁对
            trx1_phase2_lock, trx2_phase2_lock = self._phase2_locks
            
            # 准备两个事务的SQL语句
            trx1_sql = None
            trx2_sql = None
            
            # 生成T1请求T2资源的语句
            if self.trx2_lock_rows_continuous:
                start_id = random.randint(self.trx2_start_id, self.trx2_end_id)
                end_id = random.randint(start_id, self.trx2_end_id)
                trx1_sql = self._generate_lock_sql("range", "row", trx1_phase2_lock,
                                         start_id, True, end_id)
            else:
                lock_template_key = random.choice(list(self.lock_templates["row"][trx1_phase2_lock].keys()))
                trx1_sql = self._generate_lock_sql(lock_template_key, "row", trx1_phase2_lock,
                                         random.choice(self.trx2_lock_row_ids), False)
            
            # 生成T2请求T1资源的语句
            if self.trx1_lock_rows_continuous:
                start_id = random.randint(self.trx1_start_id, self.trx1_end_id)
                end_id = random.randint(start_id, self.trx1_end_id)
                trx2_sql = self._generate_lock_sql("range", "row", trx2_phase2_lock,
                                         start_id, True, end_id)
            else:
                lock_template_key = random.choice(list(self.lock_templates["row"][trx2_phase2_lock].keys()))
                trx2_sql = self._generate_lock_sql(lock_template_key, "row", trx2_phase2_lock,
                                         random.choice(self.trx1_lock_row_ids), False)
            
            # 随机决定执行顺序
            if random.choice([True, False]):
                self.trx1.append(trx1_sql)
                serial.append((1, trx1_sql))
                self.trx2.append(trx2_sql)
                serial.append((2, trx2_sql))
            else:
                self.trx2.append(trx2_sql)
                serial.append((2, trx2_sql))
                self.trx1.append(trx1_sql)
                serial.append((1, trx1_sql))
            
            print("non intersection phase2 end")
            print()
            logger.info("non intersection phase2 end")
            logger.info("")
            return serial
            
        except Exception as e:
            logger.error(f"处理无交集第二阶段失败: {e}")
            raise


    def _init_compatible_lock_pairs(self) -> Dict[str, List[Tuple[str, str]]]:
        """初始化各资源类型的兼容锁对"""
        compatible_pairs = {}
        # for resource_type in ["row", "table", "global"]:
        for resource_type in ["row"]:
            pairs = []
            available_locks = iso_lock_support[self.isolation_level][resource_type]
            for lock1 in available_locks:
                for lock2 in available_locks:
                    if row_lock_compatibility[lock1][lock2]:  # 使用对应的兼容性矩阵
                        pairs.append((lock1, lock2))
            if pairs:
                compatible_pairs[resource_type] = pairs
        return compatible_pairs

    def _init_incompatible_lock_pairs(self) -> Dict[str, List[Tuple[str, str]]]:
        """初始化各资源类型的不兼容锁对"""
        incompatible_pairs = {}
        # for resource_type in ["row", "table", "global"]:
        for resource_type in ["row"]:
            pairs = []
            available_locks = iso_lock_support[self.isolation_level][resource_type]
            for lock1 in available_locks:
                for lock2 in available_locks:
                    if not row_lock_compatibility[lock1][lock2]:  # 使用对应的兼容性矩阵
                        pairs.append((lock1, lock2))
            if pairs:
                incompatible_pairs[resource_type] = pairs
        return incompatible_pairs

    def _get_compatible_lock_pair(self, resource_type: str = "row") -> Tuple[str, str]:
        """
        获取一对兼容的锁，考虑锁的顺序性
        
        Args:
            resource_type: 资源类型，默认为"row"
            
        Returns:
            Tuple[str, str]: (first_lock, second_lock)，其中first_lock是先获取的锁，
                            second_lock是后获取的锁，且second_lock与first_lock兼容
        """
        if resource_type not in self.compatible_lock_pairs:
            raise ValueError(f"在{self.isolation_level}隔离级别下找不到{resource_type}资源的兼容锁对")
        
        # 从可用的锁对中随机选择一对，但保持顺序
        # 第一个锁是已持有的锁（矩阵的列），第二个锁是请求的锁（矩阵的行）
        first_lock = random.choice(iso_lock_support[self.isolation_level][resource_type])
        
        # 找出所有与first_lock兼容的锁
        compatible_second_locks = [
            lock_type for lock_type in iso_lock_support[self.isolation_level][resource_type]
            if row_lock_compatibility[lock_type][first_lock]  # 注意这里的顺序：[请求锁][已持有锁]
        ]
        
        if not compatible_second_locks:
            raise ValueError(f"找不到与{first_lock}兼容的锁")
        
        second_lock = random.choice(compatible_second_locks)
        return first_lock, second_lock

    def _get_incompatible_lock_pair(self, resource_type: str = "row") -> Tuple[str, str]:
        """
        获取一对不兼容的锁，考虑锁的顺序性
        
        Args:
            resource_type: 资源类型，默认为"row"
            
        Returns:
            Tuple[str, str]: (first_lock, second_lock)，其中first_lock是先获取的锁，
                            second_lock是后获取的锁，且second_lock与first_lock不兼容
        """
        if resource_type not in self.incompatible_lock_pairs:
            raise ValueError(f"在{self.isolation_level}隔离级别下找不到{resource_type}资源的不兼容锁对")
        
        # 从可用的锁中随机选择第一个锁
        first_lock = random.choice(iso_lock_support[self.isolation_level][resource_type])
        
        # 找出所有与first_lock不兼容的锁
        incompatible_second_locks = [
            lock_type for lock_type in iso_lock_support[self.isolation_level][resource_type]
            if not row_lock_compatibility[lock_type][first_lock]  # 注意这里的顺序：[请求锁][已持有锁]
        ]
        
        if not incompatible_second_locks:
            raise ValueError(f"找不到与{first_lock}不兼容的锁")
        
        second_lock = random.choice(incompatible_second_locks)
        return first_lock, second_lock

    def _get_lock_pair(self, compatible: bool = True, resource_type: str = "row") -> Tuple[str, str]:
        """
        获取一对锁
        
        Args:
            compatible: 是否获取兼容的锁对
            resource_type: 资源类型，默认为"row"
        
        Returns:
            Tuple[str, str]: 一对锁类型
        """
        try:
            if compatible:
                return self._get_compatible_lock_pair(resource_type)
            else:
                return self._get_incompatible_lock_pair(resource_type)
        except Exception as e:
            logger.error(f"获取锁对失败: {e}")
            logger.error("")
            raise

    def _generate_continuous_nonintersection(self) -> Dict:
        """生成连续无交集场景，两个事务都是连续锁定"""
        try:

            # 随机选择第一个事务的起始位置
            pivot = int(self.total_rows_num/2) + 1
            trx1_end_id = random.randint(1, pivot - 1)
            trx2_start_idx = random.randint(pivot, self.total_rows_num)
            
            trx1_start_id = random.randint(1, trx1_end_id)
            trx2_end_id = random.randint(trx2_start_idx, self.total_rows_num)

            trx1_lock_rows_num = trx1_end_id - trx1_start_id + 1
            trx2_lock_rows_num = trx2_end_id - trx2_start_idx + 1

            trx1_lock_row_ids = list(range(trx1_start_id, trx1_end_id + 1))
            trx2_lock_row_ids = list(range(trx2_start_idx, trx2_end_id + 1))

            self.trx1_lock_rows_num = trx1_lock_rows_num
            self.trx2_lock_rows_num = trx2_lock_rows_num
            self.trx1_lock_row_ids = trx1_lock_row_ids
            self.trx2_lock_row_ids = trx2_lock_row_ids
            self.trx1_start_id = trx1_start_id
            self.trx1_end_id = trx1_end_id
            self.trx2_start_id = trx2_start_idx
            self.trx2_end_id = trx2_end_id
            self.trx1_exclusive_ids = trx1_lock_row_ids
            self.trx2_exclusive_ids = trx2_lock_row_ids
            self.intersection_ids = []
            return {
                'trx1_lock_row_ids': self.trx1_lock_row_ids,
                'trx2_lock_row_ids': self.trx2_lock_row_ids,
                'intersection_row_ids': []  # 无交集
            }
        except Exception as e:
            logger.error(f"生成连续无交集场景失败: {e}")
            logger.error("")
            raise

    def _generate_mixed_nonintersection(self, trx1_continuous: bool) -> Dict:
        """生成混合无交集场景，一个事务连续锁定，另一个离散锁定"""
        try:
            if trx1_continuous:
                # 事务1连续锁定
                trx1_lock_rows_num = random.randint(1, self.total_rows_num - 1)
                trx1_start_id = random.randint(1, self.total_rows_num - trx1_lock_rows_num + 1)
                trx1_end_id = trx1_start_id + trx1_lock_rows_num - 1
                trx1_lock_row_ids = list(range(trx1_start_id, trx1_end_id + 1))
                
                # 为事务2选择不在事务1范围内的行
                available_rows = [id for id in range(1, self.total_rows_num + 1) if id not in trx1_lock_row_ids]
                
                trx2_lock_rows_num = random.randint(1, len(available_rows))
                trx2_lock_row_ids = random.sample(available_rows, trx2_lock_rows_num)
                trx2_lock_row_ids.sort()  # 保持顺序

                self.trx1_start_id = trx1_start_id
                self.trx1_end_id = trx1_end_id
                
            else:
                # 事务2连续锁定
                trx2_lock_rows_num = random.randint(1, self.total_rows_num - 1) # 至少留一个给trx1
                trx2_start_id = random.randint(1, self.total_rows_num - trx2_lock_rows_num + 1)
                trx2_end_id = trx2_start_id + trx2_lock_rows_num - 1
                trx2_lock_row_ids = list(range(trx2_start_id, trx2_end_id + 1))
                
                # 为事务1选择不在事务2范围内的行
                available_rows = [id for id in range(1, self.total_rows_num + 1) if id not in trx2_lock_row_ids]                
                
                trx1_lock_rows_num = random.randint(1, len(available_rows))
                trx1_lock_row_ids = random.sample(available_rows, trx1_lock_rows_num)
                trx1_lock_row_ids.sort()  # 保持顺序
                
                self.trx2_start_id = trx2_start_id
                self.trx2_end_id = trx2_end_id
                
            self.trx1_lock_rows_num = trx1_lock_rows_num
            self.trx2_lock_rows_num = trx2_lock_rows_num
            self.trx1_lock_row_ids = trx1_lock_row_ids
            self.trx2_lock_row_ids = trx2_lock_row_ids
            self.trx1_exclusive_ids = trx1_lock_row_ids
            self.trx2_exclusive_ids = trx2_lock_row_ids
            self.intersection_ids = []
            return {
                'trx1_lock_row_ids': self.trx1_lock_row_ids,
                'trx2_lock_row_ids': self.trx2_lock_row_ids,
                'intersection_row_ids': []  # 无交集
            }
        except Exception as e:
            logger.error(f"生成混合无交集场景失败: {e}")
            logger.error("")
            raise

    def _generate_discrete_nonintersection(self) -> Dict:
        """生成离散无交集场景，两个事务都是离散锁定"""
        try:
            # 为两个事务随机选择不重叠的行
            trx1_lock_rows_num = random.randint(1, self.total_rows_num - 1)
            all_rows = list(range(1, self.total_rows_num + 1))
            trx1_rows = sorted(random.sample(all_rows, trx1_lock_rows_num))

            remaining_rows = [r for r in all_rows if r not in trx1_rows]
            trx2_lock_rows_num = random.randint(1, len(remaining_rows))
            trx2_rows = sorted(random.sample(remaining_rows, trx2_lock_rows_num))
            
            # 检查生成的行是否实际上是连续的
            def is_continuous(row_ids):
                if not row_ids:
                    return False
                return max(row_ids) - min(row_ids) + 1 == len(row_ids)
            
            # 更新连续性标志
            if is_continuous(trx1_rows):
                self.trx1_lock_rows_continuous = True
                self.trx1_start_id = min(trx1_rows)
                self.trx1_end_id = max(trx1_rows)
                
            if is_continuous(trx2_rows):
                self.trx2_lock_rows_continuous = True
                self.trx2_start_id = min(trx2_rows)
                self.trx2_end_id = max(trx2_rows)
                
            # 设置类属性
            self.trx1_lock_rows_num = trx1_lock_rows_num
            self.trx2_lock_rows_num = trx2_lock_rows_num
            self.trx1_lock_row_ids = trx1_rows
            self.trx2_lock_row_ids = trx2_rows
            self.trx1_exclusive_ids = trx1_rows
            self.trx2_exclusive_ids = trx2_rows

            self.intersection_ids = []
            
            return {
                'trx1_lock_row_ids': self.trx1_lock_row_ids,
                'trx2_lock_row_ids': self.trx2_lock_row_ids,
                'intersection_row_ids': []
            }
        except Exception as e:
            logger.error(f"生成离散无交集场景失败: {e}")
            logger.error("")
            raise

class AtomicityChecker:
    def __init__(self, host: str, user: str, password: str, database: str, port: int,
                 trx1: List[str], trx2: List[str], serial: List[Tuple[int, str]]):
        """
        初始化原子性检查器
        
        Args:
            host: 数据库主机
            user: 数据库用户名
            password: 数据库密码
            database: 数据库名
            port: 数据库端口
            trx1: 事务1的SQL语句列表
            trx2: 事务2的SQL语句列表
            serial: 死锁场景的执行序列，格式为[(事务ID, SQL语句), ...]
        """
        self.db_config = {
            "host": host,
            "user": user,
            "password": password,
            "database": database,
            "port": port,
            "connect_timeout": 60,
            "use_pure": True,
            "buffered": True,
            "autocommit": False
        }
        self.trx1 = trx1
        self.trx2 = trx2
        self.serial = serial
        self.snapshot_before = None
        self.snapshot_trx1 = None
        self.snapshot_trx2 = None
        self.snapshot_serial = None
        self.executed_serial = None
        
        self.conn1 = None
        self.conn2 = None
        self.conn_locks = {
            1: threading.Lock(),  # 事务1的连接锁
            2: threading.Lock()   # 事务2的连接锁
        }

    def _init_connections(self):
        """初始化事务连接"""
        self.conn1 = mysql.connector.connect(**self.db_config)
        self.conn2 = mysql.connector.connect(**self.db_config)

    def _create_connection(self):
        """创建数据库连接"""
        try:
            return mysql.connector.connect(**self.db_config)
        except mysql.connector.Error as err:
            logger.error(f"数据库连接失败: {err}")
            logger.error("")
            raise

    def _take_snapshot(self, conn) -> Dict[str, List[Tuple]]:
        """获取数据库表的快照"""
        try:
            snapshot = {}
            cursor = conn.cursor()
            
            # 获取所有表名
            cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = %s
            """, (self.db_config['database'],))
            tables = cursor.fetchall()
            
            # 获取每个表的数据
            for (table_name,) in tables:
                cursor.execute(f"SELECT * FROM {table_name} ORDER BY id")
                snapshot[table_name] = cursor.fetchall()
            
            cursor.close()
            return snapshot
        except mysql.connector.Error as err:
            logger.error(f"获取数据库快照失败: {err}")
            logger.error("")
            raise

    def _execute_transaction(self, conn, statements: List[str], trx_id: int) -> bool:
        try:
            cursor = conn.cursor()
            for stmt in statements:
                if stmt:
                    cursor.execute(stmt)
                    if stmt.strip().upper().startswith("SELECT"):
                        cursor.fetchall()
                    logger.info(f"执行事务{trx_id}语句: {stmt}")  # 记录执行成功的SQL
            conn.commit()
            cursor.close()
            return True
        except mysql.connector.Error as err:
            # conn.rollback()
            cursor.close()
            logger.error(f"执行SQL失败: {stmt}")  # 记录失败的SQL
            logger.error(f"错误详情: {err}")
            return False

    def execute_stmt_async(self, trx_id, stmt, result_queue):
        """异步执行SQL语句"""
        def _execute():
            # nonlocal cursor
            # nonlocal conn
            conn = self.conn1 if trx_id == 1 else self.conn2
            lock = self.conn_locks[trx_id]
            try:
                with lock:
                    cursor = conn.cursor(buffered=True)
                    cursor.execute(stmt)
                    if stmt.strip().upper().startswith("SELECT"):
                        cursor.fetchall()
                result_queue.put(("success", None))
                cursor.close()
            except mysql.connector.Error as err:
                logger.error(f"执行语句错误: {err}, stmt: {stmt}")
                # 检查是否是连接丢失错误
                if err.errno == 2013 or "Lost connection" in str(err):
                    try:
                        # 重新连接
                        cursor.close()
                        conn.reconnect(attempts=3, delay=1)
                        cursor = conn.cursor(buffered=True)
                        # 重试执行语句
                        cursor.execute(stmt)
                        if stmt.strip().upper().startswith("SELECT"):
                            cursor.fetchall()
                        result_queue.put(("success", None))
                        logger.info(f"重连执行成功, stmt: {stmt}")
                        return
                    except mysql.connector.Error as reconnect_err:
                        logger.error(f"重连执行失败: {reconnect_err}, stmt: {stmt}")
                        result_queue.put(("error", reconnect_err))
                        return
                result_queue.put(("error", err))
            except Exception as e:
                logger.error(f"未知错误: {e}")
                result_queue.put(("error", e))
                
        thread = threading.Thread(target=_execute)
        # thread.daemon = True
        thread.start()
        # thread.join(timeout=1)
        return thread
    
    def _execute_serial(self) -> Tuple[bool, Optional[int], List[Tuple]]:
        """使用两个独立连接按序执行死锁场景"""

        try:
            self._init_connections()
                
            # 记录每个事务当前执行到的位置
            idx = 0
            executed_serial = []

            pending_stmts = {}
            result_queues = {}
            
            # 记录被阻塞的事务ID
            rollback_trx_id = None
            blocked_trx_id = None
            blocked_stmts = {}

            while idx < len(self.serial) or pending_stmts:
                try:
                    if idx >= len(self.serial):
                        logger.info(f"序列遍历完成，开始查看剩余阻塞的语句：{pending_stmts}")
                    
                    # 启动新的语句执行
                    if idx < len(self.serial):
                        trx_id, stmt = self.serial[idx]
                        if stmt is None:
                            print(f"stmt为None, trx_id: {trx_id}, stmt: {stmt}")
                            logger.error(f"stmt为None, trx_id: {trx_id}, stmt: {stmt}")
                            idx += 1
                            continue

                        # 如果该事务已被标记为blocked，跳过不执行
                        if rollback_trx_id and trx_id == rollback_trx_id:
                            executed_serial.append((trx_id, f"--Skipped, trx_id: {trx_id}, stmt: {stmt}"))
                            idx += 1
                            continue
                            
                        if blocked_trx_id and trx_id == blocked_trx_id:
                            # 先不给他分配线程，加入等待队列blocked_stmts
                            blocked_stmts[idx] = (trx_id, stmt)
                            idx += 1
                            continue

                        if stmt.strip().upper() == "BEGIN":
                            with self.conn_locks[trx_id]:
                                cursor = self.conn1.cursor() if trx_id == 1 else self.conn2.cursor()
                                try:
                                    cursor.execute("BEGIN")
                                    executed_serial.append((trx_id, "BEGIN"))
                                except Exception as e:
                                    logger.error(f"开始事务失败: {e}")
                                cursor.close()
                            idx += 1
                            continue
                        if stmt.strip().upper() in ("BEGIN", "COMMIT"):
                            with self.conn_locks[trx_id]:
                                cursor = self.conn1.cursor() if trx_id == 1 else self.conn2.cursor()
                                try:
                                    cursor.execute("COMMIT")
                                    executed_serial.append((trx_id, "COMMIT"))
                                except Exception as e:
                                    logger.error(f"提交事务失败: {e}")
                                cursor.close()
                            idx += 1
                            continue
                        
                        # 为新语句创建结果队列并启动执行
                        result_queue = queue.Queue()
                        thread = self.execute_stmt_async(trx_id, stmt, result_queue)
                        pending_stmts[idx] = (trx_id, stmt, thread, time.time())
                        result_queues[idx] = result_queue
                        idx += 1
                    
                    # 检查所有pending语句的执行结果
                    logger.info("")
                    logger.info(f"执行前idx: {idx}, pending_stmts: {pending_stmts}")
                    
                    completed_stmts = []
                    
                    stmt_indices = list(pending_stmts.keys())
                    for stmt_idx in stmt_indices:
                        try:
                            # 非阻塞方式检查结果
                            time.sleep(0.1)
                            trx_id, stmt, thread, _ = pending_stmts[stmt_idx]
                            status, result = result_queues[stmt_idx].get_nowait()
                            logger.info(f"stmt_idx: {stmt_idx}, status: {status}, result: {result}")
                            if status == "success":
                                executed_serial.append((trx_id, stmt))
                                logger.info(f"成功执行序列中事务{trx_id}语句: {stmt}")
                                completed_stmts.append(stmt_idx)
                                del pending_stmts[stmt_idx]
                                del result_queues[stmt_idx]

                                if blocked_trx_id and trx_id == blocked_trx_id:
                                    # 结束了阻塞
                                    logger.info(f"结束了阻塞，blocked_trx_id: {blocked_trx_id}, stmt: {stmt}")
                                    blocked_trx_id = None
                                    for stmt_idx, (trx_id, stmt) in blocked_stmts.items():
                                        # 分配线程
                                        result_queue = queue.Queue()
                                        thread = self.execute_stmt_async(trx_id, stmt, result_queue)
                                        pending_stmts[stmt_idx] = (trx_id, stmt, thread, time.time())
                                        result_queues[stmt_idx] = result_queue
                                        
                            elif status == "error":
                                if result.errno == 1213:  # 死锁
                                    logger.error(f"死锁错误: {result}，事务{trx_id}语句: {stmt}")
                                    # 获取实际被回滚的事务ID
                                    # rollback_trx_id = get_deadlock_info()
                                    rollback_trx_id = trx_id
                                    executed_serial.append((rollback_trx_id, "ROLLBACK"))
                                    completed_stmts.append(stmt_idx)
                                    # 终止那个被回滚的事务的pending线程们
                                    for stmt_idx, (trx_id, stmt, thread, _) in pending_stmts.items():
                                        if trx_id == rollback_trx_id:
                                            thread.join(timeout=0.001)
                                            del pending_stmts[stmt_idx]
                                            del result_queues[stmt_idx]
                                    # 继续执行,让未被回滚的事务完成
                                else:
                                    executed_serial.append((trx_id, f"-- Error: {stmt}"))
                                    logger.error(f"执行语句失败，非死锁错误: {result}，事务{trx_id}语句: {stmt}")
                                    return False, trx_id, executed_serial
                        except queue.Empty:
                            # 说明wait for lock
                            print(f"wait for lock, trx_id: {trx_id}, stmt: {stmt}")
                            logger.info(f"wait for lock, trx_id: {trx_id}, stmt: {stmt}")
                            blocked_trx_id = trx_id # 标记为blocked
                            continue
                    
                    # 短暂休眠避免CPU占用过高
                    logger.info(f"执行后idx: {idx}, pending_stmts: {pending_stmts}")
                    logger.info("")
                    time.sleep(0.1)
                except Exception as e:
                    logger.error(f"错误: {e}")
            

            # 所有语句执行完毕后,根据是否发生死锁返回结果
            if rollback_trx_id:
                return False, rollback_trx_id, executed_serial
            return True, None, executed_serial
        
        except Exception as e:
            logger.error(f"执行序列发生错误: {e}")
            print(f"执行序列发生错误: {e}")
            return False, None, []
        finally:
            # 清理资源
            if self.conn1:
                try:
                    self.conn1.close()
                except Exception as e:
                    logger.error(f"关闭事务1连接失败: {e}")
                    pass
            if self.conn2:
                try:
                    self.conn2.close()
                except Exception as e:
                    logger.error(f"关闭事务2连接失败: {e}")
                    pass

    def _compare_snapshots(self, snapshot1: Dict[str, List[Tuple]], 
                          snapshot2: Dict[str, List[Tuple]]) -> bool:
        """比较两个数据库快照是否相同，忽略自增ID的差异"""
        if snapshot1.keys() != snapshot2.keys():
            return False
        
        for table_name in snapshot1.keys():
            rows1 = snapshot1[table_name]
            rows2 = snapshot2[table_name]
            
            # 检查行数是否相同
            if len(rows1) != len(rows2):
                return False
            
            # TODO: 目前忽略的auto_increment列，后续需要考虑
            # 比较每一行，忽略ID（第一列）
            for row1, row2 in zip(rows1, rows2):
                # 比较除ID外的所有列
                if row1[1:] != row2[1:]:
                    return False
        
        return True
    
    def _restore_initial_state(self, conn):
        """彻底恢复数据库到初始状态，包括重建表结构和重置自增序列"""
        try:
            # 首先确保所有已有连接都被清理
            self._cleanup_all_connections()
            
            # 创建新连接
            new_conn = mysql.connector.connect(**self.db_config)
            cursor = new_conn.cursor()
            
            # # 设置更短的超时时间
            # cursor.execute("SET SESSION innodb_lock_wait_timeout = 3")
            # cursor.execute("SET SESSION wait_timeout = 5")
            
            # 强制结束所有活跃事务
            cursor.execute("""
                SELECT trx_id, trx_mysql_thread_id 
                FROM information_schema.innodb_trx
            """)
            for trx_id, thread_id in cursor.fetchall():
                try:
                    cursor.execute(f"KILL {thread_id}")
                except:
                    pass
                
            # 等待一小段时间确保事务真正结束
            time.sleep(1)
            
            # 获取所有表名
            cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = %s
            """, (self.db_config['database'],))
            tables = cursor.fetchall()
            
            # 禁用外键检查
            cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
            
            # 删除并重建每个表
            for (table_name,) in tables:
                try:
                    # 先获取表的创建语句
                    cursor.execute(f"SHOW CREATE TABLE {table_name}")
                    _, create_stmt = cursor.fetchone()
                    
                    # 尝试删除表
                    for attempt in range(3):
                        try:
                            cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
                            new_conn.commit()
                            break
                        except Exception as e:
                            if attempt == 2:  # 最后一次尝试失败
                                raise
                            time.sleep(1)
                    
                    # 重新创建表
                    cursor.execute(create_stmt)
                    new_conn.commit()
                    
                except Exception as e:
                    logger.error(f"处理表 {table_name} 时发生错误: {e}")
                    raise
            
            # 重新启用外键检查
            cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
            
            # 重新插入初始数据
            if hasattr(self, 'snapshot_before') and self.snapshot_before:
                for table_name, rows in self.snapshot_before.items():
                    for row in rows:
                        placeholders = ','.join(['%s'] * len(row))
                        cursor.execute(
                            f"INSERT INTO {table_name} VALUES ({placeholders})",
                            row
                        )
            
            new_conn.commit()
            cursor.close()
            
            # 如果原连接还在，关闭它
            if conn:
                try:
                    conn.close()
                except:
                    pass
            
            return new_conn
            
        except Exception as e:
            logger.error(f"恢复初始状态时发生错误: {e}")
            logger.error("")
            raise

    def _cleanup_all_connections(self):
        """清理所有数据库连接和事务"""
        try:
            # 创建一个临时连接来清理其他连接
            cleanup_conn = mysql.connector.connect(**self.db_config)
            cleanup_cursor = cleanup_conn.cursor()
            
            # 终止所有其他连接
            cleanup_cursor.execute("""
                SELECT id FROM information_schema.processlist 
                WHERE user = %s AND id != CONNECTION_ID()
            """, (self.db_config['user'],))
            
            for (process_id,) in cleanup_cursor.fetchall():
                try:
                    cleanup_cursor.execute(f"KILL {process_id}")
                except:
                    pass
            
            cleanup_cursor.close()
            cleanup_conn.close()
            
        except Exception as e:
            logger.warning(f"清理连接时发生错误: {e}")

    def check_atomicity(self) -> Tuple[bool, str]:
        """
        检查事务的原子性
        
        Returns:
            Tuple[bool, str]: (是否满足原子性, 详细信息)
        """
        try:
            # 获取初始状态的快照
            conn = self._create_connection()
            self.snapshot_before = self._take_snapshot(conn)
            conn.close()

            # 执行事务1
            conn = self._create_connection()
            logger.info(f"创建事务1连接成功")
            trx1_success = self._execute_transaction(conn, self.trx1, 1)
            self.snapshot_trx1 = self._take_snapshot(conn)
            conn.close()
            logger.info(f"执行事务1成功") if trx1_success else logger.error(f"执行事务1失败")

            # 执行事务2
            
            
            self._restore_initial_state(conn)
            logger.info(f"恢复初始状态成功")
            conn = self._create_connection()
            logger.info(f"创建事务2连接成功")
            trx2_success = self._execute_transaction(conn, self.trx2, 2)
            self.snapshot_trx2 = self._take_snapshot(conn)
            conn.close()
            logger.info(f"执行事务2成功") if trx2_success else logger.error(f"执行事务2失败")


            conn = self._create_connection()
            self._restore_initial_state(conn)
            
            serial_success, rollback_trx_id, executed_serial = self._execute_serial()
            self.executed_serial = executed_serial
            logger.info(f"执行序列成功")
            
            # 使用新连接获取最终状态快照
            conn = self._create_connection()
            self.snapshot_serial = self._take_snapshot(conn)
            
            # 关闭所有连接
            conn.close()

            # 检查是否是无效的测试用例
            if serial_success is True:
                return True, "序列被完整成功执行，无效的测试用例：未发生死锁或锁等待"
            
            # 检查结果
            if not serial_success:  # 发生死锁，某个事务被回滚
                if rollback_trx_id == 1:
                    # 事务1被回滚，检查事务2的结果
                    if self._compare_snapshots(self.snapshot_serial, self.snapshot_trx2):
                        return True, "死锁发生，事务1被回滚，事务2的结果与单独执行时一致，满足原子性"
                    else:
                        return False, "死锁发生，事务1被回滚，但最终状态与事务2单独执行的结果不一致，不满足原子性"
                else:  # rollback_trx_id == 2
                    # 事务2被回滚，检查事务1的结果
                    if self._compare_snapshots(self.snapshot_serial, self.snapshot_trx1):
                        return True, "死锁发生，事务2被回滚，事务1的结果与单独执行时一致，满足原子性"
                    else:
                        return False, "死锁发生，事务2被回滚，但最终状态与事务1单独执行的结果不一致，不满足原子性"

        except Exception as e:
            logger.error(f"检查原子性失败: {e}")
            return False, f"检查过程发生错误: {str(e)}"

    def get_snapshots(self) -> Dict[str, Dict[str, List[Tuple]]]:
        """获取所有快照数据，用于调试"""
        return {
            "before": self.snapshot_before,
            "trx1": self.snapshot_trx1,
            "trx2": self.snapshot_trx2,
            "serial": self.snapshot_serial
        }

    def _normalize_sql(self, sql: str) -> str:
        """标准化SQL语句以便比较"""
        if not sql:
            return ""
        # 移除多余的空白字符
        sql = ' '.join(sql.split())
        # 移除可能的引号差异
        sql = sql.replace('"', "'")
        return sql


# 修改连接池配置
dbconfig = {
    "host": "localhost",
    "user": "root", 
    "password": "123456",
    "database": "test",
    "port": 3308,
    "connection_timeout": 60,
    "use_pure": True,            # 使用纯Python实现
    "buffered": True,            # 使用buffered模式
    "raise_on_warnings": True,   # 立即抛出警告
    "get_warnings": True,        # 获取警告信息
    "consume_results": True,     # 自动消费结果
    "autocommit": False,         # 显式控制事务
}

# # 创建连接池
# connection_pool = mysql.connector.pooling.MySQLConnectionPool(
#     pool_name="mypool",
#     pool_size=30,
#     **dbconfig
# )

# 设置全局锁等待超时
try:
    conn = mysql.connector.connect(**dbconfig)
    cursor = conn.cursor()
    cursor.execute("SET GLOBAL innodb_lock_wait_timeout = 600000")
    cursor.execute("SET GLOBAL net_write_timeout = 600000")
    cursor.execute("SET GLOBAL net_read_timeout = 600000")
    # 设置隔离级别
    cursor.execute("set global transaction isolation level READ COMMITTED")
    # 设置全局超时参数
    # cursor.execute("SET GLOBAL wait_timeout = 180")
    # cursor.execute("SET GLOBAL interactive_timeout = 180")
    cursor.close()
    conn.close()
except mysql.connector.Error as err:
    logger.warning(f"设置锁等待超时失败: {err}")

# 进行实验
num_of_runs = 100
logger.info("INFO TEST")
logger.debug("DEBUG TEST")
logger.error("ERROR TEST")
bug_count = 1
for i in range(num_of_runs):
    # 添加日志记录
    print(f"iter: {i}") 
    logger.info(f"iter: {i}")
    conn = None
    try:
        # 从连接池获取连接
        # conn = connection_pool.get_connection()
        conn = mysql.connector.connect(**dbconfig)
        
        # 使用同一个连接初始化
        initializer = MySQLInitializer(
            connection=conn,  # 传入连接而不是创建新连接
            database="test"
        )
        
        # 初始化数据库
        initializer.initialize_database()

        # 创建表
        initializer.generate_tables()

        # 插入数据
        initializer.populate_tables()

        # 执行随机化操作
        initializer.execute_random_actions()

        # 提交并关闭连接
        initializer.commit_and_close()

        # 生成死锁场景
        RR_Template = get_iso_template("RR")
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM table_0")
        total_rows_num = cursor.fetchone()[0]
        cursor.close()

        dlGenerator = DeadlockGenerator("RR", LOCK_HIERARCHY, RR_Template, total_rows_num, "localhost", "root", "123456", "test", 3308)
        res_dict, lock_same_resource = dlGenerator._init_resource_distribution()
        print(dlGenerator.trx1_lock_rows_num, dlGenerator.trx2_lock_rows_num, dlGenerator.intersection_size)

        logger.info(f"res_dict: {res_dict}")
        logger.info(f"trx1_continuous: {dlGenerator.trx1_lock_rows_continuous},trx2_continuous: {dlGenerator.trx2_lock_rows_continuous}")
        logger.info(f"lock_same_resource: {lock_same_resource}")
        logger.info("")

        # 生成事务
        trx1, trx2, serial = dlGenerator._generate_deadlock_trx_serial(10)

        # 进行实验
        atomicity_checker = AtomicityChecker("localhost", "root", "123456", "test", 3308, trx1, trx2, serial)
        is_atomic, info = atomicity_checker.check_atomicity()

        print('is_atomic:', is_atomic)
        print()
        print('info:', info)
        print()
        print('executed_serial:', atomicity_checker.executed_serial)
        print()
        print('snapshots:', atomicity_checker.get_snapshots())
        print()

        logger.info(f"is_atomic: {is_atomic}")
        logger.info(f"info: {info}")
        logger.info(f"executed_serial: {atomicity_checker.executed_serial}")
        logger.info(f"snapshots: {atomicity_checker.get_snapshots()}")
        logger.info("")

        if not is_atomic:
            # 创建保存bug case的目录
            bug_case_dir = f"{database_save_dir}/bug_case_{bug_count}"
            bug_count += 1
            os.makedirs(bug_case_dir, exist_ok=True)
            
            # 先还原数据库到初始状态
            conn = mysql.connector.connect(**dbconfig)
            conn = atomicity_checker._restore_initial_state(conn)  # 使用返回的新连接
            conn.close()
            
            # 导出还原后的数据库状态
            dump_cmd = f"mysqldump -h localhost -P 3308 -u root -p123456 test > {bug_case_dir}/initial_state.sql"
            os.system(dump_cmd)

            
            # 保存事务和序列信息
            with open(f"{bug_case_dir}/transactions.sql", 'w') as f:
                f.write("-- Transaction 1\n")
                f.write("\n".join(trx1) + "\n\n")
                f.write("-- Transaction 2\n")
                f.write("\n".join(trx2) + "\n\n")
                f.write("-- Planned Serial\n")
                f.write("\n".join([f"-- Transaction {t[0]}: {t[1]}" for t in serial]) + "\n\n")
                f.write("-- Actually Executed Serial\n")
                f.write("\n".join([f"-- Transaction {t[0]}: {t[1]}" for t in atomicity_checker.executed_serial]))
            
            # 记录其他相关信息
            with open(f"{bug_case_dir}/metadata.txt", 'w') as f:
                f.write(f"Bug Info: {info}\n")
                f.write(f"Resource Distribution: {res_dict}\n")
                f.write(f"Lock Same Resource: {lock_same_resource}\n")
                f.write(f"T1 Continuous: {dlGenerator.trx1_lock_rows_continuous}\n")
                f.write(f"T2 Continuous: {dlGenerator.trx2_lock_rows_continuous}\n")
                f.write(f"T1 Lock Row IDs: {dlGenerator.trx1_lock_row_ids}\n")
                f.write(f"T2 Lock Row IDs: {dlGenerator.trx2_lock_row_ids}\n")
                f.write(f"Intersection IDs: {getattr(dlGenerator, 'intersection_ids', 'None')}\n")
                f.write(f"T1 Exclusive IDs: {getattr(dlGenerator, 'trx1_exclusive_ids', 'None')}\n")
                f.write(f"T2 Exclusive IDs: {getattr(dlGenerator, 'trx2_exclusive_ids', 'None')}\n")
                f.write(f"Snapshots: {atomicity_checker.get_snapshots()}\n")
            
            # 继续记录日志
            logger.error(f"BUG FOUND: {info}")
            logger.info(f"iter: {i}")
            logger.info(f"resource distribution: {res_dict}")
            logger.info(f"lock same resource: {lock_same_resource}")
            logger.info(f"T1是否连续：{dlGenerator.trx1_lock_rows_continuous}")
            logger.info(f"T2是否连续：{dlGenerator.trx2_lock_rows_continuous}")
            logger.info(f"事务1锁定的行ID：{dlGenerator.trx1_lock_row_ids}")
            logger.info(f"事务2锁定的行ID：{dlGenerator.trx2_lock_row_ids}")
            logger.info(f"交集行ID：{getattr(dlGenerator, 'intersection_ids', 'None')}")
            logger.info(f"事务1独占行：{getattr(dlGenerator, 'trx1_exclusive_ids', 'None')}")
            logger.info(f"事务2独占行：{getattr(dlGenerator, 'trx2_exclusive_ids', 'None')}")
            logger.info(f"事务1：{trx1}")
            logger.info(f"事务2：{trx2}")
            logger.info(f"序列：{serial}")
            logger.info(f"实际执行序列：{atomicity_checker.executed_serial}")
            logger.info(f"快照：{atomicity_checker.get_snapshots()}")
            logger.info("")

            print(f"BUG FOUND: {info}")
            print(f"iter: {i}")
            print(f"resource distribution: {res_dict}")
            print(f"lock same resource: {lock_same_resource}")
            print(f"T1是否连续：{dlGenerator.trx1_lock_rows_continuous}")
            print(f"T2是否连续：{dlGenerator.trx2_lock_rows_continuous}")
            print(f"事务1锁定的行ID：{dlGenerator.trx1_lock_row_ids}")
            print(f"事务2锁定的行ID：{dlGenerator.trx2_lock_row_ids}")
            print(f"交集行ID：{getattr(dlGenerator, 'intersection_ids', 'None')}")
            print(f"事务1独占行：{getattr(dlGenerator, 'trx1_exclusive_ids', 'None')}")
            print(f"事务2独占行：{getattr(dlGenerator, 'trx2_exclusive_ids', 'None')}")
            print(f"事务1：{trx1}")
            print(f"事务2：{trx2}")
            print(f"序列：{serial}")
            print(f"实际执行序列：{atomicity_checker.executed_serial}")
            print(f"快照：{atomicity_checker.get_snapshots()}")
            print("")
            continue

    except Exception as e:
        # 已有对应的日志记录
        print(f"Error in iteration {i}: {e}")
        print("")
        logger.error(f"Error in iteration {i}: {e}")
        logger.error("")
    finally:
        # 确保资源释放
        if conn:
            try:
                conn.close()
            except:
                pass

        # 显式清理实例
        try:
            if 'initializer' in locals():
                del initializer
            if 'dlGenerator' in locals():
                del dlGenerator
            if 'atomicity_checker' in locals():
                del atomicity_checker
        except:
            pass

        # 强制垃圾回收
        import gc
        gc.collect()
