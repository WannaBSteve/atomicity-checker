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
import itertools
import threading
import mysql

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
                password: str, database: str, port: int, num_transactions: int = 3):
        """初始化死锁生成器"""
        self.isolation_level = isolation_level
        self.lock_hierarchy = lock_hierarchy
        self.lock_templates = lock_templates
        self.total_rows_num = total_rows_num
        self.num_transactions = num_transactions
        
        # 数据库连接配置
        self.db_config = {
            "host": host,
            "user": user,
            "password": password,
            "database": database,
            "port": port
        }
        
        # 初始化主连接和游标（用于获取元数据等）
        self.conn1 = mysql.connector.connect(**self.db_config)
        self.cursor1 = self.conn1.cursor(buffered=True)
        self.cursor1.execute(f"SET SESSION TRANSACTION ISOLATION LEVEL {ISOLATION_LEVELS[isolation_level]}")
        
        # 初始化事务连接和游标
        self.connections = []
        self.cursors = []
        for i in range(num_transactions):
            conn = mysql.connector.connect(**self.db_config)
            cursor = conn.cursor(buffered=True)
            cursor.execute(f"SET SESSION TRANSACTION ISOLATION LEVEL {ISOLATION_LEVELS[isolation_level]}")
            self.connections.append(conn)
            self.cursors.append(cursor)
        
        # 设置表名
        self.table_name = "table_0"  # 假设使用默认表名

    def create_connection(self):
        return mysql.connector.connect(**self.db_config)

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
            
            # 使用 AST 生成器
            ast_gen = MySQLASTGen(self.table_name, rows, column_names, column_types, primary_keys, lock_type)
            
            # 根据锁类型选择合适的语句类型
            if lock_type in ["GAP", "NK", "II"]:
                stmt_type = random.choice(["INSERT", "UPDATE", "SELECT", "DELETE"])
            else:
                stmt_type = random.choice(["SELECT", "UPDATE", "DELETE"])  # 其他锁类型可以随机选择
        
            return ast_gen.generate_lock_sql(stmt_type)
                
        except Exception as e:
            logger.error(f"生成锁SQL失败: {e}")
            raise

    def _generate_deadlock_trx_serial(self, max_statements: int) -> Tuple[Dict[int, List[str]], List[Tuple]]:
        """生成死锁事务序列"""
        try:
            # 初始化每个事务的SQL列表
            self.trx_sqls = {i: ["BEGIN"] for i in range(self.num_transactions)}
            serial = [(i, "BEGIN") for i in range(self.num_transactions)]
            
            # 第一阶段：每个事务锁定自己的独占资源
            for i in range(self.num_transactions):
                if self.trx_resources[i]['exclusive_ids']:
                    lock_type = random.choice(["S", "X"])  # 简化锁类型选择
                    if self.trx_resources[i]['continuous']:
                        sql = self._generate_lock_sql("range", "row", lock_type,
                                                   min(self.trx_resources[i]['exclusive_ids']),
                                                   True,
                                                   max(self.trx_resources[i]['exclusive_ids']))
                    else:
                        sql = self._generate_lock_sql("basic", "row", lock_type,
                                                   self.trx_resources[i]['exclusive_ids'][0],
                                                   False)
                    self.trx_sqls[i].append(sql)
                    serial.append((i, sql))
            
            # 第二阶段：每个事务请求其他事务的共享资源
            for i in range(self.num_transactions):
                if self.trx_resources[i]['shared_ids']:
                    lock_type = random.choice(["S", "X"])  # 简化锁类型选择
                    for shared_id in self.trx_resources[i]['shared_ids']:
                        sql = self._generate_lock_sql("basic", "row", lock_type,
                                                   shared_id,
                                                   False)
                        self.trx_sqls[i].append(sql)
                        serial.append((i, sql))
            
            # 添加提交语句
            for i in range(self.num_transactions):
                self.trx_sqls[i].append("COMMIT")
                serial.append((i, "COMMIT"))
                
            return self.trx_sqls, serial
            
        except Exception as e:
            logger.error(f"生成死锁事务序列失败: {e}")
            raise

    def cleanup(self):
        """清理资源"""
        # 关闭主连接和游标
        if hasattr(self, 'cursor1'):
            try:
                self.cursor1.close()
            except:
                pass
        if hasattr(self, 'conn1'):
            try:
                self.conn1.close()
            except:
                pass
            
        # 关闭事务连接和游标
        for cursor in self.cursors:
            try:
                cursor.close()
            except:
                pass
        for conn in self.connections:
            try:
                conn.close()
            except:
                pass

    def generate_deadlock(self) -> Tuple[List[str], List[str], List[Tuple]]:
        """
        生成死锁场景
        
        Returns:
            Tuple[List[str], List[str], List[Tuple]]: 返回(事务1的SQL列表, 事务2的SQL列表, 执行序列)
        """
        try:
            print("开始生成死锁场景")
            logger.info("开始生成死锁场景")
            
            # 初始化资源分配
            self.trx_resources, has_intersection = self._init_resource_distribution()
            
            print(f"资源分配结果: {self.trx_resources}")
            print(f"分配模式: {self.distribution_mode}")
            logger.info(f"资源分配结果: {self.trx_resources}")
            logger.info(f"分配模式: {self.distribution_mode}")
            
            # 生成死锁事务序列
            max_statements = 20  # 限制最大语句数
            trx_sqls, serial = self._generate_deadlock_trx_serial(max_statements)
            
            print("生成的事务序列:")
            for trx_id, sqls in trx_sqls.items():
                print(f"事务{trx_id}: {sqls}")
            print(f"执行序列: {serial}")
            
            
            return trx_sqls, serial
            
        except Exception as e:
            logger.error(f"生成死锁场景失败: {e}")
            raise

    def execute_deadlock_serial(self, serial: List[Tuple[int, str]]) -> Tuple[bool, List[int]]:
        """
        执行死锁序列
        
        Args:
            serial: 执行序列，每个元素为(事务ID, SQL语句)的元组
        
        Returns:
            Tuple[bool, List[int]]: (是否成功执行完所有语句, 被回滚的事务ID列表)
        """
        try:
            print("开始执行死锁序列")
            logger.info("开始执行死锁序列")
            
            rollback_trxs = []
            executed_statements = []
            
            for trx_id, sql in serial:
                print(f"执行事务{trx_id}: {sql}")
                logger.info(f"执行事务{trx_id}: {sql}")
                
                try:
                    if sql.upper() == "BEGIN":
                        continue
                    elif sql.upper() == "COMMIT":
                        self.connections[trx_id].commit()
                    else:
                        self.cursors[trx_id].execute(sql)
                    executed_statements.append((trx_id, sql))
                    
                except mysql.connector.Error as err:
                    print(f"事务{trx_id}执行失败: {err}")
                    logger.error(f"事务{trx_id}执行失败: {err}")
                    
                    if err.errno == 1213:  # 死锁错误码
                        rollback_trxs.append(trx_id)
                        self.connections[trx_id].rollback()
                        print(f"事务{trx_id}被回滚")
                        logger.info(f"事务{trx_id}被回滚")
                    else:
                        raise
            
            success = len(rollback_trxs) > 0
            print(f"执行完成，成功状态: {success}, 被回滚的事务: {rollback_trxs}")
            logger.info(f"执行完成，成功状态: {success}, 被回滚的事务: {rollback_trxs}")
            
            return success, rollback_trxs
            
        except Exception as e:
            logger.error(f"执行死锁序列失败: {e}")
            raise

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
                WHERE TABLE_SCHEMA = '{self.db_config['database']}'
                    AND TABLE_NAME = '{self.table_name}'
                    AND CONSTRAINT_NAME = 'PRIMARY'
                ORDER BY ORDINAL_POSITION
            """)
            primary_keys = [row[0] for row in self.cursor1.fetchall()]
            
            # 获取索引信息
            self.cursor1.execute(f"""
                SELECT DISTINCT INDEX_NAME
                FROM INFORMATION_SCHEMA.STATISTICS 
                WHERE TABLE_SCHEMA = '{self.db_config['database']}'
                    AND TABLE_NAME = '{self.table_name}'
                    AND INDEX_NAME != 'PRIMARY'
            """)
            indexes = [row[0] for row in self.cursor1.fetchall()]
            
            return column_names, column_types, primary_keys, indexes
        
        except mysql.connector.Error as err:
            logger.error(f"获取表元数据失败: {err}")
            raise
    
    def verify_lock(self, target_rows, generated_sql):
        conn = self.db_config.create_connection()
        cursor = conn.cursor()
        cursor.execute(generated_sql)
        actual_rows = cursor.fetchall()
        cursor.close()
        conn.close()
        return set(actual_rows) == set(target_rows)
       
    def _init_resource_distribution(self) -> Tuple[Dict, bool]:
        """初始化资源分配，使用迭代而不是递归"""
        max_attempts = 3  # 最大尝试次数
        
        for attempt in range(max_attempts):
            try:
                # 随机选择分配模式
                self.distribution_mode = random.choice(["chain", "star", "mesh"])
                logger.info(f"选择资源分配模式: {self.distribution_mode}")
                
                # 随机决定每个事务是连续还是离散锁定
                self.trx_continuous = {i: random.choice([True, False]) for i in range(self.num_transactions)}
                
                result = {}
                all_rows = set(range(1, self.total_rows_num + 1))
                available_rows = all_rows.copy()
                
                # 为每个事务分配独占资源
                for i in range(self.num_transactions):
                    if len(available_rows) < 1:
                        logger.warning(f"尝试 {attempt + 1}: 没有足够的资源分配给事务{i}")
                        break
                        
                    exclusive_size = random.randint(1, min(3, len(available_rows)))
                    exclusive = set(random.sample(list(available_rows), exclusive_size))
                    available_rows -= exclusive
                    
                    result[i] = {
                        'row_ids': sorted(list(exclusive)),
                        'exclusive_ids': sorted(list(exclusive)),
                        'shared_ids': [],
                        'continuous': self.trx_continuous[i]
                    }
                
                # 如果所有事务都获得了独占资源，继续分配共享资源
                if len(result) == self.num_transactions:
                    success = False
                    
                    if self.distribution_mode == "chain":
                        success = self._add_chain_shared_resources(result, available_rows)
                    elif self.distribution_mode == "star":
                        success = self._add_star_shared_resources(result, available_rows)
                    else:  # mesh
                        success = self._add_mesh_shared_resources(result, available_rows)
                    
                    if success:
                        self.trx_resources = result
                        logger.info(f"资源分配结果: {result}")
                        logger.info(f"分配模式: {self.distribution_mode}")
                        return result, True
                        
                logger.warning(f"尝试 {attempt + 1}: 资源分配失败")
                
            except Exception as e:
                logger.error(f"尝试 {attempt + 1}: 资源分配出错: {e}")
                continue
        
        raise Exception("资源分配失败：达到最大尝试次数")

    def _add_chain_shared_resources(self, result: Dict, available_rows: Set[int]) -> bool:
        """添加链式共享资源，不使用递归"""
        try:
            if len(available_rows) < self.num_transactions:
                return False
                
            # 为每对相邻事务添加共享资源
            for i in range(self.num_transactions):
                next_trx = (i + 1) % self.num_transactions
                
                if len(available_rows) < 1:
                    return False
                    
                shared_size = random.randint(1, min(2, len(available_rows)))
                shared_rows = set(random.sample(list(available_rows), shared_size))
                
                # 更新两个事务的共享资源
                result[i]['shared_ids'].extend(sorted(list(shared_rows)))
                result[next_trx]['shared_ids'].extend(sorted(list(shared_rows)))
                result[i]['row_ids'].extend(sorted(list(shared_rows)))
                result[next_trx]['row_ids'].extend(sorted(list(shared_rows)))
                
                available_rows -= shared_rows
            
            return True
            
        except Exception as e:
            logger.error(f"添加链式共享资源失败: {e}")
            return False

    def _add_star_shared_resources(self, result: Dict, available_rows: Set[int]) -> bool:
        """添加星形共享资源，不使用递归"""
        try:
            # 随机选择一个中心事务
            self.center_trx = random.randint(0, self.num_transactions - 1)
            
            # 为中心事务和每个其他事务添加共享资源
            for other_trx in range(self.num_transactions):
                if other_trx == self.center_trx:
                    continue
                    
                if len(available_rows) < 1:
                    return False
                    
                shared_size = random.randint(1, min(2, len(available_rows)))
                shared_rows = set(random.sample(list(available_rows), shared_size))
                
                # 更新共享资源
                result[other_trx]['shared_ids'].extend(sorted(list(shared_rows)))
                result[self.center_trx]['shared_ids'].extend(sorted(list(shared_rows)))
                result[other_trx]['row_ids'].extend(sorted(list(shared_rows)))
                result[self.center_trx]['row_ids'].extend(sorted(list(shared_rows)))
                
                available_rows -= shared_rows
            
            return True
            
        except Exception as e:
            logger.error(f"添加星形共享资源失败: {e}")
            return False

    def _add_mesh_shared_resources(self, result: Dict, available_rows: Set[int]) -> bool:
        """添加网状共享资源，不使用递归"""
        try:
            # 随机生成事务间的共享关系
            for i in range(self.num_transactions):
                for j in range(i + 1, self.num_transactions):
                    if random.random() < 0.5 and len(available_rows) > 0:  # 50%概率生成共享关系
                        shared_size = random.randint(1, min(2, len(available_rows)))
                        shared_rows = set(random.sample(list(available_rows), shared_size))
                        
                        # 更新共享资源
                        result[i]['shared_ids'].extend(sorted(list(shared_rows)))
                        result[j]['shared_ids'].extend(sorted(list(shared_rows)))
                        result[i]['row_ids'].extend(sorted(list(shared_rows)))
                        result[j]['row_ids'].extend(sorted(list(shared_rows)))
                        
                        available_rows -= shared_rows
            
            return True
            
        except Exception as e:
            logger.error(f"添加网状共享资源失败: {e}")
            return False

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
    """
    多事务原子性检查器  
    
    test oracle：
    1. 在多事务并发执行的场景中，无论有多少事务参与，只要某些事务最终被提交（Commit）、某些被回滚（Rollback），则：
    数据库的最终状态应等同于所有被提交的事务按某种顺序单独执行后的状态，且被回滚的事务不留下任何影响。
    

    简化test oracle：
    2. 如果存在一个模拟并发的执行序列serial，某些事务最终被提交（Commit）、某些被回滚（Rollback），则：
    serial执行后的数据库状态，应等同于serial剔除被回滚的事务后的状态。
    """
    def __init__(self, host: str, user: str, password: str, database: str, port: int, isolation_level: str,
                 trx_sqls: Dict[int, List[str]], serial: List[Tuple[int, str]]):
        """
        初始化原子性检查器
        
        Args:
            host: 数据库主机
            user: 数据库用户名
            password: 数据库密码
            database: 数据库名
            port: 数据库端口
            trx_sqls: 事务SQL语句字典，格式为{事务ID: [SQL语句列表]}
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
        self.table_name = "table_0"
        self.trx_sqls = trx_sqls
        self.num_transactions = len(trx_sqls)
        self.serial = serial
        self.isolation_level = isolation_level
        self.snapshot_before = None
        self.snapshot_serial = None
        self.executed_serial = None
        
        # 为每个事务创建连接和锁
        self.connections = {}
        self.conn_locks = {}
        for i in range(self.num_transactions):
            self.conn_locks[i] = threading.Lock()

    def _init_connections(self):
        """为每个事务创建独立的数据库连接"""
        for trx_id in range(self.num_transactions):
            if trx_id not in self.connections:
                conn = mysql.connector.connect(**self.db_config)
                cursor = conn.cursor()
                cursor.execute(f"SET SESSION TRANSACTION ISOLATION LEVEL {ISOLATION_LEVELS[self.isolation_level]}")
                cursor.close()
                self.connections[trx_id] = conn

    def execute_stmt_async(self, trx_id: int, stmt: str, result_queue: queue.Queue):
        """异步执行SQL语句"""
        def _execute():
            conn = self.connections[trx_id]
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
                if err.errno == 2013 or "Lost connection" in str(err):
                    try:
                        cursor.close()
                        conn.reconnect(attempts=3, delay=1)
                        cursor = conn.cursor(buffered=True)
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
        thread.start()
        return thread

    def _execute_serial(self, serial: List[Tuple[int, str]]) -> Tuple[bool, Optional[int], List[Tuple]]:
        """使用多个独立连接按序执行事务场景
        
        Args:
            serial: 死锁场景的执行序列，格式为[(事务ID, SQL语句), ...]
        
        Returns:
            Tuple[bool, Optional[int], List[Tuple]]: 是否成功执行所有语句(死锁或错误会返回False), 回滚的事务ID, 执行序列
        """

        try:
            self._init_connections()
                
            # 记录每个事务当前执行到的位置
            idx = 0
            executed_serial = []

            pending_stmts = {}
            result_queues = {}
            
            # 记录被阻塞的事务ID
            rollback_trx_ids = set()
            blocked_trx_ids = set()
            blocked_stmts = {}

            serial = self.serial if serial is None else serial
            while idx < len(serial) or pending_stmts:
                try:
                    if idx >= len(serial):
                        logger.info(f"序列遍历完成，开始查看剩余阻塞的语句：{pending_stmts}")
                    
                    # 启动新的语句执行
                    if idx < len(serial):
                        trx_id, stmt = serial[idx]
                        if stmt is None:
                            print(f"stmt为None, trx_id: {trx_id}, stmt: {stmt}")
                            logger.error(f"stmt为None, trx_id: {trx_id}, stmt: {stmt}")
                            idx += 1
                            continue

                        # 如果该事务已被标记为blocked，跳过不执行
                        if rollback_trx_ids and trx_id in rollback_trx_ids:
                            executed_serial.append((trx_id, f"--Skipped, trx_id: {trx_id}, stmt: {stmt}"))
                            idx += 1
                            continue
                            
                        if blocked_trx_ids and trx_id in blocked_trx_ids:
                            # 先不给他分配线程，加入等待队列blocked_stmts
                            blocked_stmts[idx] = (trx_id, stmt)
                            idx += 1
                            continue

                        # 处理事务控制语句
                        if stmt.strip().upper() in ("BEGIN", "COMMIT"):
                            with self.conn_locks[trx_id]:
                                cursor = self.connections[trx_id].cursor()
                                try:
                                    cursor.execute(stmt)
                                    executed_serial.append((trx_id, stmt))
                                except Exception as e:
                                    logger.error(f"事务控制语句执行失败: {e}")
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

                                if blocked_trx_ids and trx_id in blocked_trx_ids:
                                    # 结束了阻塞
                                    logger.info(f"结束了阻塞，blocked_trx_ids: {blocked_trx_ids}, stmt: {stmt}")
                                    blocked_trx_ids.remove(trx_id)
                                    for stmt_idx, (trx_id, stmt) in blocked_stmts.items():
                                        # 分配线程
                                        result_queue = queue.Queue()
                                        thread = self.execute_stmt_async(trx_id, stmt, result_queue)
                                        pending_stmts[stmt_idx] = (trx_id, stmt, thread, time.time())
                                        result_queues[stmt_idx] = result_queue
                                        
                            elif status == "error":
                                if result.errno == 1213:  # 死锁
                                    logger.error(f"死锁错误: {result}，事务{trx_id}语句: {stmt}")
                                    rollback_trx_ids.add(trx_id)
                                    executed_serial.append((trx_id, "ROLLBACK"))
                                    completed_stmts.append(stmt_idx)
                                    # 终止那个被回滚的事务的pending线程们
                                    for stmt_idx, (trx_id, stmt, thread, _) in pending_stmts.items():
                                        if trx_id in rollback_trx_ids:
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
                            blocked_trx_ids.add(trx_id)  # 标记为blocked
                            continue
                    
                    # 短暂休眠避免CPU占用过高
                    logger.info(f"执行后idx: {idx}, pending_stmts: {pending_stmts}")
                    logger.info("")
                    time.sleep(0.1)
                except Exception as e:
                    logger.error(f"错误: {e}")
            
            # 所有语句执行完毕后,根据是否发生死锁返回结果
            if rollback_trx_ids:
                return False, rollback_trx_ids, executed_serial
            return True, None, executed_serial
        
        except Exception as e:
            logger.error(f"执行序列发生错误: {e}")
            print(f"执行序列发生错误: {e}")
            return False, None, []
        finally:
            # 清理资源
            for conn in self.connections.values():
                try:
                    conn.close()
                except Exception as e:
                    logger.error(f"关闭连接失败: {e}")

    def _restore_initial_state(self) -> bool:
        """还原数据库到初始状态"""
        conn = None
        cursor = None
        try:
            conn = mysql.connector.connect(**self.db_config)
            cursor = conn.cursor(buffered=True)
            
            # 还原表数据
            cursor.execute(f"TRUNCATE TABLE {self.table_name}")
            cursor.execute(f"""
                INSERT INTO {self.table_name} (id, col_0)
                VALUES {', '.join([f'({i}, 0)' for i in range(1, self.total_rows_num + 1)])}
            """)
            conn.commit()
            return True
            
        except mysql.connector.Error as err:
            logger.error(f"还原初始状态失败: {err}")
            return False
            
        finally:
            if cursor:
                try:
                    cursor.close()
                except:
                    pass
            if conn:
                try:
                    if conn.is_connected():
                        conn.close()
                except:
                    pass

    # def _generate_all_serials(self, trx_sqls: Dict[int, List[str]], 
    #                      committed_trxs: Set[int], 
    #                      rolled_back_trxs: Set[int]) -> List[List[Tuple[int, str]]]:
    #     """
    #     生成所有可能的执行序列
        
    #     Args:
    #         trx_sqls: 事务SQL语句字典 {事务ID: [SQL语句列表]}
    #         committed_trxs: 成功提交的事务ID集合
    #         rolled_back_trxs: 被回滚的事务ID集合
        
    #     Returns:
    #         List[List[Tuple[int, str]]]: 所有可能的执行序列列表
    #     """
    #     try:
    #         # 只考虑成功提交的事务
    #         valid_trxs = committed_trxs - rolled_back_trxs
    #         if not valid_trxs:
    #             return []

    #         # 构建每个事务的语句序列
    #         trx_sequences = {}
    #         for trx_id in valid_trxs:
    #             if trx_id in trx_sqls:
    #                 sequence = []
    #                 sequence.append((trx_id, "BEGIN"))
    #                 for stmt in trx_sqls[trx_id]:
    #                     sequence.append((trx_id, stmt))
    #                 sequence.append((trx_id, "COMMIT"))
    #                 trx_sequences[trx_id] = sequence

    #         # 使用递归生成所有可能的序列组合
    #         def generate_combinations(remaining_trxs: Set[int], current_serial: List[Tuple[int, str]], 
    #                                 result: List[List[Tuple[int, str]]]):
    #             if not remaining_trxs:
    #                 result.append(current_serial[:])
    #                 return
                
    #             # 对于每个剩余的事务，尝试将其完整序列插入当前位置
    #             for trx_id in remaining_trxs:
    #                 # 将当前事务的所有语句添加到序列中
    #                 new_serial = current_serial + trx_sequences[trx_id]
    #                 # 递归处理剩余事务
    #                 generate_combinations(
    #                     remaining_trxs - {trx_id}, 
    #                     new_serial, 
    #                     result
    #                 )

    #         all_serials = []
    #         generate_combinations(valid_trxs, [], all_serials)

    #         # 生成交错执行的序列
    #         def generate_interleaved_serials(trx_sequences: Dict[int, List[Tuple[int, str]]]) -> List[List[Tuple[int, str]]]:
    #             # 获取所有事务的语句
    #             all_stmts = []
    #             for trx_id, sequence in trx_sequences.items():
    #                 all_stmts.extend(sequence)
                
    #             # 使用递归生成所有可能的交错序列
    #             def generate_valid_permutations(remaining_stmts: List[Tuple[int, str]], 
    #                                          current_serial: List[Tuple[int, str]],
    #                                          trx_states: Dict[int, str],
    #                                          result: List[List[Tuple[int, str]]]):
    #                 if not remaining_stmts:
    #                     result.append(current_serial[:])
    #                     return
                    
    #                 # 找出当前可以执行的语句
    #                 for i, (trx_id, stmt) in enumerate(remaining_stmts):
    #                     can_execute = False
                        
    #                     # 检查事务状态
    #                     if stmt == "BEGIN":
    #                         can_execute = trx_id not in trx_states
    #                     elif stmt == "COMMIT":
    #                         can_execute = trx_states.get(trx_id) == "ACTIVE"
    #                     else:
    #                         can_execute = trx_states.get(trx_id) == "ACTIVE"
                        
    #                     if can_execute:
    #                         # 更新事务状态
    #                         new_trx_states = trx_states.copy()
    #                         if stmt == "BEGIN":
    #                             new_trx_states[trx_id] = "ACTIVE"
    #                         elif stmt == "COMMIT":
    #                             new_trx_states[trx_id] = "COMMITTED"
                            
    #                         # 递归生成剩余序列
    #                         new_remaining = remaining_stmts[:i] + remaining_stmts[i+1:]
    #                         generate_valid_permutations(
    #                             new_remaining,
    #                             current_serial + [(trx_id, stmt)],
    #                             new_trx_states,
    #                             result
    #                         )

    #             interleaved_serials = []
    #             generate_valid_permutations(all_stmts, [], {}, interleaved_serials)
    #             return interleaved_serials

    #         # 合并顺序执行和交错执行的序列
    #         all_serials.extend(generate_interleaved_serials(trx_sequences))

    #         # 移除重复的序列
    #         unique_serials = []
    #         seen = set()
    #         for serial in all_serials:
    #             serial_tuple = tuple(serial)
    #             if serial_tuple not in seen:
    #                 seen.add(serial_tuple)
    #                 unique_serials.append(serial)

    #         return unique_serials

    #     except Exception as e:
    #         logger.error(f"生成执行序列失败: {e}")
    #         return []

    def check_atomicity(self, trx_sqls: Dict[int, List[str]], 
                       serial: List[Tuple[int, str]], 
                       ) -> Tuple[bool, str, List[Tuple[int, str]]]:
        """检查事务序列的原子性"""
        try:
            logger.info(f"执行序列: {serial}")
            logger.info(f"trx_sqls: {trx_sqls}")

            try:
                conn = self._create_connection()
                self.snapshot_before = self._take_snapshot(conn)
                conn.close()
                
                # 执行原始序列
                success, rollback_trx_ids, executed_serial = self._execute_serial(serial)
            except Exception as e:
                logger.error(f"执行序列失败: {e}")
                return False, str(e), []
                
            # 如果执行成功且没有回滚的事务，即未发生死锁，直接返回，这个测例无效，如果发生死锁，才需要检查原子性
            if success and rollback_trx_ids is None:
                logger.info(f"执行序列成功，未发生死锁，无效test case")
                print(f"执行序列成功，未发生死锁，无效test case")
                return True, None, serial

            # 保存并发执行后的数据库状态
            conn = self._create_connection()
            self.snapshot_serial = self._take_snapshot(conn)
            conn.close()

            # 获取成功和失败的事务集合
            rolled_back_trxs = set(rollback_trx_ids) if rollback_trx_ids else set()
            committed_trxs = {trx_id for trx_id, sql in executed_serial if sql.upper() == "COMMIT"}
            self.executed_serial = executed_serial
                        
            logger.info(f"被回滚的事务: {sorted(list(rolled_back_trxs))}")
            logger.info(f"成功提交的事务: {sorted(list(committed_trxs))}")
            
            cleaned_serial = []
            for trx_id, stmt in executed_serial:
                if trx_id in rolled_back_trxs:
                    cleaned_serial.append((trx_id, stmt))
            
            # 检查cleaned_serial是否等价于serial
            self._restore_initial_state()
            cleaned_success, cleaned_rollback_trx_ids, cleaned_executed_serial = self._execute_serial(cleaned_serial)

            if not cleaned_success:
                error_msg = "清理后的序列执行失败，这可能表明存在原子性问题"
                logger.error(error_msg)
                return False, error_msg, cleaned_serial
                
            # 获取清理后序列的执行状态
            conn = self._create_connection()
            clean_state = self._take_snapshot(conn)
            conn.close()
            
            # 比较两次执行的结果
            if not self._compare_snapshots(self.snapshot_serial, clean_state):
                error_msg = (
                    "违反原子性：\n"
                    f"serial执行状态: {self.snapshot_serial}\n"
                    f"cleaned_serial执行状态: {clean_state}"
                )
                logger.error(error_msg)
                return False, error_msg, cleaned_serial
            return True, None, serial
            
        except Exception as e:
            logger.error(f"检查原子性过程发生错误: {e}")
            return False, str(e), []

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

    def _execute_transaction(self, conn: mysql.connector.MySQLConnection, 
                           statements: List[str], trx_id: int) -> bool:
        """
        执行单个事务的所有语句
        
        Args:
            conn: 数据库连接
            statements: SQL语句列表
            trx_id: 事务ID
        
        Returns:
            bool: 是否成功执行所有语句
        """
        try:
            cursor = conn.cursor()
            for stmt in statements:
                if stmt.strip().upper() == "BEGIN":
                    cursor.execute("BEGIN")
                elif stmt.strip().upper() == "COMMIT":
                    cursor.execute("COMMIT")
                else:
                    cursor.execute(stmt)
                    if stmt.strip().upper().startswith("SELECT"):
                        cursor.fetchall()
            cursor.close()
            return True
        except Exception as e:
            logger.error(f"执行事务{trx_id}失败: {e}")
            try:
                conn.rollback()
            except:
                pass
            return False

    def _restore_initial_state(self) -> bool:
        """还原数据库到初始状态"""
        conn = None
        cursor = None
        try:
            # 创建新连接
            conn = mysql.connector.connect(**self.db_config)
            cursor = conn.cursor(buffered=True)
            
            # 还原表数据
            cursor.execute(f"TRUNCATE TABLE {self.table_name}")
            cursor.execute(f"""
                INSERT INTO {self.table_name} (id, col_0)
                VALUES {', '.join([f'({i}, 0)' for i in range(1, self.total_rows_num + 1)])}
            """)
            conn.commit()
            return True
            
        except mysql.connector.Error as err:
            logger.error(f"还原初始状态失败: {err}")
            return False
            
        finally:
            if cursor:
                try:
                    cursor.close()
                except:
                    pass
            if conn:
                try:
                    if conn.is_connected():
                        conn.close()
                except:
                    pass

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

    def get_snapshots(self) -> Dict[str, Dict[str, List[Tuple]]]:
        """获取所有快照数据，用于调试"""
        return {
            "before": self.snapshot_before,
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

    def _compare_snapshots(self, snapshot1: Dict[str, List[Tuple]], 
                          snapshot2: Dict[str, List[Tuple]]) -> bool:
        """
        比较两个数据库快照是否相同
        
        Args:
            snapshot1: 第一个快照
            snapshot2: 第二个快照
        
        Returns:
            bool: 两个快照是否相同
        """
        try:
            if snapshot1.keys() != snapshot2.keys():
                return False
                
            for table_name in snapshot1.keys():
                if len(snapshot1[table_name]) != len(snapshot2[table_name]):
                    return False
                
                # 对行进行排序以确保比较的一致性
                rows1 = sorted(snapshot1[table_name])
                rows2 = sorted(snapshot2[table_name])
                
                if rows1 != rows2:
                    return False
                    
            return True
        except Exception as e:
            logger.error(f"比较快照失败: {e}")
            return False


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
    cursor.execute("SET GLOBAL innodb_lock_wait_timeout = 10")
    cursor.execute("SET GLOBAL net_write_timeout = 600000")
    cursor.execute("SET GLOBAL net_read_timeout = 600000")
    # 设置隔离级别
    cursor.execute("set global transaction isolation level REPEATABLE READ")
    cursor.close()
    conn.close()
except mysql.connector.Error as err:
    logger.warning(f"设置锁等待超时失败: {err}")

# 进行实验
num_of_runs = 1
logger.info("INFO TEST")
logger.debug("DEBUG TEST")
logger.error("ERROR TEST")
bug_count = 1

isolation_level = "RR"
for i in range(num_of_runs):
    # 添加日志记录
    print(f"iter: {i}") 
    logger.info(f"iter: {i}")
    conn = None
    try:
        # 创建数据库连接
        conn = mysql.connector.connect(**dbconfig)
        
        # 使用同一个连接初始化
        initializer = MySQLInitializer(
            connection=conn,
            database="test"
        )
        
        # 初始化数据库
        initializer.initialize_database()
        initializer.generate_tables()
        initializer.populate_tables()
        initializer.execute_random_actions()
        initializer.commit_and_close()

        # 生成死锁场景
        RR_Template = get_iso_template("RR")
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM table_0")
        total_rows_num = cursor.fetchone()[0]
        cursor.close()

        # 创建死锁生成器，设置事务数量为3-5之间的随机数
        num_transactions = random.randint(3, 5)
        dlGenerator = DeadlockGenerator(
            isolation_level, LOCK_HIERARCHY, RR_Template, total_rows_num, 
            "localhost", "root", "123456", "test", 3308,
            num_transactions=num_transactions
        )
        
        # 生成死锁场景
        trx_sqls, serial = dlGenerator.generate_deadlock()

        # 创建原子性检查器
        atomicity_checker = AtomicityChecker(
            "localhost", "root", "123456", "test", 3308, isolation_level,
            trx_sqls, serial
        )
        
        # 检查原子性
        is_atomic, info, executed_serial = atomicity_checker.check_atomicity(trx_sqls, serial)
        snapshots = atomicity_checker.get_snapshots()
        print('is_atomic:', is_atomic)
        print('info:', info)
        print('executed_serial:', executed_serial)
        print('snapshots:', snapshots)
        print()

        logger.info(f"is_atomic: {is_atomic}")
        logger.info(f"info: {info}")
        logger.info(f"executed_serial: {executed_serial}")
        logger.info(f"snapshots: {snapshots}")
        logger.info("")

        if not is_atomic:
            # 创建保存bug case的目录
            bug_case_dir = f"{database_save_dir}/bug_case_{bug_count}"
            bug_count += 1
            os.makedirs(bug_case_dir, exist_ok=True)
            
            # 导出数据库状态
            atomicity_checker._restore_initial_state()
            
            dump_cmd = f"mysqldump -h localhost -P 3308 -u root -p123456 test > {bug_case_dir}/initial_state.sql"
            os.system(dump_cmd)

            # 保存事务和序列信息
            with open(f"{bug_case_dir}/transactions.sql", 'w') as f:
                for trx_id, sqls in trx_sqls.items():
                    f.write(f"-- Transaction {trx_id}\n")
                    f.write("\n".join(sqls) + "\n\n")
                f.write("-- Planned Serial\n")
                f.write("\n".join([f"-- Transaction {t[0]}: {t[1]}" for t in serial]) + "\n\n")
                f.write("-- Actually Executed Serial\n")
                f.write("\n".join([f"-- Transaction {t[0]}: {t[1]}" for t in executed_serial]))
            
            # 记录其他相关信息
            with open(f"{bug_case_dir}/metadata.txt", 'w') as f:
                f.write(f"Number of Transactions: {num_transactions}\n")
                f.write(f"Bug Info: {info}\n")
                f.write(f"Distribution Mode: {dlGenerator.distribution_mode}\n")
                f.write(f"Resource Distribution: {dlGenerator.trx_resources}\n")
                f.write(f"Snapshots: {snapshots}\n")
            
            # 记录日志
            logger.error(f"BUG FOUND: {info}")
            logger.info(f"Number of Transactions: {num_transactions}")
            logger.info(f"Distribution Mode: {dlGenerator.distribution_mode}")
            logger.info(f"Resource Distribution: {dlGenerator.trx_resources}")
            logger.info(f"Transactions: {trx_sqls}")
            logger.info(f"Planned Serial: {serial}")
            logger.info(f"Actually Executed Serial: {executed_serial}")
            logger.info(f"Snapshots: {snapshots}")
            logger.info("")

            print(f"BUG FOUND: {info}")
            print(f"Number of Transactions: {num_transactions}")
            print(f"Distribution Mode: {dlGenerator.distribution_mode}")
            print(f"Resource Distribution: {dlGenerator.trx_resources}")
            print(f"Transactions: {trx_sqls}")
            print(f"Planned Serial: {serial}")
            print(f"Actually Executed Serial: {executed_serial}")
            print(f"Snapshots: {snapshots}")
            print("")
            continue

    except Exception as e:
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
