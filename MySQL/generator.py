import random
import threading
import mysql.connector
from MySQLInitializer import MySQLInitializer
from transaction_generator import TransactionGenerator
from datetime import datetime
import argparse
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


class AtomicityChecker:
    """
    多事务原子性检查器  
    
    test oracle：
    1. 在多事务并发执行的场景中，无论有多少事务参与，只要某些事务最终被提交（Commit）、某些被回滚（Rollback），则：
    数据库的最终状态应等同于所有被提交的事务按某种顺序单独执行后的状态，且被回滚的事务不留下任何影响。
    

    简化test oracle：
    2. 如果存在一个模拟出的真实并发执行序列serial，某些事务最终被提交（Commit）、某些被回滚（Rollback），则：
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
        self.slave_db_config = {
            "host": host,
            "user": user,
            "password": password,
            "database": database,
            "port": port+2,
            "connect_timeout": 60,
            "use_pure": True,
            "buffered": True,
            "autocommit": False
        }

        self.trx_sqls = trx_sqls
        self.num_transactions = len(trx_sqls)
        self.serial = serial
        self.isolation_level = isolation_level
        self.snapshot_before = None
        self.snapshot_serial = None
        self.snapshot_cleaned = None
        # self.sp_snapshot = {}
        self.rollback_snapshot = {}
        self.sub_checkpoint_pair = []
        self.executed_serial = []
        self.cleaned_executed_serial = []
        self.rollback_trx_ids = set()
        self.result_queue = None
        self.txs = None
        self.terminate_events = {}

        # 为每个事务创建连接和锁
        self.connections = {}
        self.conn_locks = {}
        for i in range(1, self.num_transactions + 1):
            self.conn_locks[i] = threading.Lock()
        
        # for i in range(1, self.num_transactions + 1):
        #     self.sp_snapshot[i] = {}

    def _init_connections(self, actual_trx_ids: Set[int]):
        """为每个事务创建独立的数据库连接"""
        # 首先将self.connections清空
        self.connections = {}
        # 然后重新初始化
        for trx_id in actual_trx_ids:
            if trx_id not in self.connections:
                conn = mysql.connector.connect(**self.db_config)
                cursor = conn.cursor()
                cursor.execute(f"SET SESSION TRANSACTION ISOLATION LEVEL {ISOLATION_LEVELS[self.isolation_level]}")
                cursor.close()
                self.connections[trx_id] = conn
        logger.info(f"初始化连接完成, connections: {self.connections}")

    def _cleanup_connections(self, actual_trx_ids: Set[int]):
        """清理所有连接"""
        for trx_id in actual_trx_ids:
            self.connections[trx_id].close()

    def _execute_stmt_async(self, trx_id: int, stmt: str, result_queue: queue.Queue, terminate_event: threading.Event, 
                            cleaned: bool, local_executed_serial: List, local_cleaned_executed_serial: List):
        """异步执行SQL语句"""
        def _execute():
            nonlocal trx_id
            nonlocal stmt
            nonlocal local_executed_serial
            nonlocal local_cleaned_executed_serial
            
            conn = self.connections[trx_id]
            lock = self.conn_locks[trx_id]
            try:
                with lock:
                    cursor = conn.cursor(buffered=True)
                    if stmt.strip().upper().startswith("CALL"):
                        # 解析存储过程调用语句
                        # 去除CALL和括号，得到存储过程名称和参数
                        proc_parts = stmt.strip()[5:].strip().split('(', 1)
                        proc_name = proc_parts[0].strip()
                        
                        # 提取参数字符串并移除最后的括号
                        params_str = proc_parts[1].rsplit(')', 1)[0] if len(proc_parts) > 1 else ""
                        
                        # 将参数字符串转换为参数列表
                        params = []
                        if params_str:
                            # 分割参数并处理每个参数
                            for param in params_str.split(','):
                                param = param.strip()
                                # 尝试转换为数字
                                try:
                                    # 尝试转换为整数
                                    params.append(int(param))
                                except ValueError:
                                    try:
                                        # 尝试转换为浮点数
                                        params.append(float(param))
                                    except ValueError:
                                        # 如果不是数字，移除引号后作为字符串
                                        params.append(param.strip('"\''))
                    
                        try:
                            # 执行存储过程
                            result = cursor.callproc(proc_name, tuple(params))
                        
                        except mysql.connector.Error as err:
                            logger.error(f"执行存储过程失败: {err}, stmt: {stmt}")
                            raise
                    else:
                        try:
                            if stmt.strip().upper().startswith("ROLLBACK --"):
                                cursor.execute("ROLLBACK")
                            else:
                                cursor.execute(stmt)
                                
                            if stmt.strip().upper().startswith("SELECT"):
                                cursor.fetchall()
                            # 如果是SAVEPOINT语句，需要此时保存快照，以便后续对比
                            if "ROLLBACK TO" in stmt.strip().upper():
                                sp_id = stmt.strip().split()[-1]
                                # 尝试获取savepoint，如果不存在则继续而不是失败
                                try:
                                    # snapshot = self._take_snapshot(conn)
                                    # self.sp_snapshot[trx_id][sp_id] = snapshot
                                    # logger.info(f"     事务{trx_id}回滚至savepoint {sp_id}后保存快照")
                                    pass
                                except mysql.connector.Error as sp_err:
                                    if sp_err.errno == 1305:  # Savepoint不存在错误
                                        logger.warning(f"     SAVEPOINT {sp_id}不存在，跳过此回滚操作")
                                        # 记录一个空快照或使用最新的快照
                                        # snapshot = self._take_snapshot(conn)
                                        # self.sp_snapshot[trx_id][sp_id] = snapshot
                                    else:
                                        raise
                            elif "ROLLBACK" == stmt.strip().upper():
                                # snapshot = self._take_snapshot(conn)
                                # self.rollback_snapshot[trx_id] = snapshot
                                # logger.info(f"     事务{trx_id}回滚后保存快照")
                                pass
                        except mysql.connector.Error as err:
                            if err.errno == 1305:  # Savepoint不存在错误
                                logger.warning(f"     不存在: {err}, 继续执行")
                                # 这里不抛出异常，而是视为成功执行
                                # 记录当前数据库状态作为rollback的结果
                                if "ROLLBACK TO" in stmt.strip().upper():
                                    # sp_id = stmt.strip().split()[-1]
                                    # snapshot = self._take_snapshot(conn)
                                    # self.sp_snapshot[trx_id][sp_id] = snapshot
                                    pass
                            elif err.errno == 1292:# 1292是数据类型错误(Truncated incorrect DOUBLE value)，1062是重复插入错误(Duplicate entry)
                                logger.warning(f"     执行失败: {err}，视为成功执行")
                                # 这里不抛出异常，而是视为成功执行
                            elif err.errno == 1062:
                                logger.warning(f"     执行失败: {err}，视为成功执行")
                                # 这里不抛出异常，而是视为成功执行
                            else:
                                raise

                result_queue.put(("success", None))

                # 使用传入的局部变量记录执行序列
                if not cleaned:
                    local_executed_serial.append((trx_id, stmt))
                else:
                    local_cleaned_executed_serial.append((trx_id, stmt))
                cursor.close()
            except mysql.connector.Error as err:
                if err.errno == 1213:
                    logger.error(f"     异步执行死锁: {err}, stmt: {stmt}")
                    if not cleaned:
                        local_executed_serial.append((trx_id, f"ROLLBACK --{stmt}"))
                    else:
                        local_cleaned_executed_serial.append((trx_id, f"ROLLBACK --{stmt}"))
                    terminate_event.set()
                    # 快照
                    # snapshot = self._take_snapshot(conn)
                    # self.rollback_snapshot[trx_id] = snapshot
                    # logger.info(f"     事务{trx_id}死锁回滚后保存快照")

                else:
                    logger.error(f"     异步执行非死锁错误: {err}, stmt: {stmt}")
                result_queue.put(("error", err))
            except Exception as e:
                logger.error(f"     异步执行未知错误: {e}")
                result_queue.put(("error", e))
            finally:
                if terminate_event.is_set():
                    logger.info(f"      终止事务{trx_id}的pending语句: {stmt}")
                    return
            
        thread = threading.Thread(target=_execute)
        thread.start()
        return thread
    

    def _execute_serial(self, serial: List[Tuple[int, str]], cleaned: bool, sub_check_point: bool) -> Tuple[bool, Set[int], List[Tuple[int, str]], List[Tuple[int, str]]]:
        """使用多个独立连接按序执行事务场景
        
        Args:
            serial: 死锁场景的执行序列，格式为[(事务ID, SQL语句), ...]
        
        Returns:
            Tuple[bool, Set[int], List[Tuple[int, str]], List[Tuple[int, str]]]: 是否成功执行所有语句, 回滚的事务ID, 执行序列, 实际提交序列
        """
        
        # 为每个子检查点创建独立的执行序列记录
        local_executed_serial = []
        local_cleaned_executed_serial = []
        
        try:
            actual_trx_ids = set(tx_id for tx_id, _ in serial)
            self._init_connections(actual_trx_ids)
            # 有几个事务就创建几个terminate_event
            self.terminate_events = {trx_id: threading.Event() for trx_id in actual_trx_ids}

            # 记录每个事务当前执行到的位置
            idx = 0
            actual_submitted_serial = []
            logged_serial = []

            # 使用局部变量而不是类成员变量
            pending_stmts = {}
            trx_in_pending = set()
            result_queues = {}
            rollback_trx_ids = set()

            # 记录被阻塞的事务ID
            blocked_trx_ids = set()
            blocked_stmts = {}

            serial = self.serial if serial is None else serial

            while idx < len(serial):
                logger.info(f"idx: {idx}")

                # 记录pending_stmts中是否已有对应事务语句
                logger.info(f"-------------1. 检查是否可调度阻塞语句-------------")
                logger.info(f"检查前：阻塞队列:{blocked_stmts}")
                diaodu_flag = False
                diaodu_sql = None
                diaodu_trx = None
                if len(blocked_stmts)>0:
                    # 如果阻塞语句队列能被调度，优先执行阻塞的语句
                    blocked_stmts_copy = blocked_stmts.copy()
                    for stmt_idx, (trx_id, stmt) in blocked_stmts_copy.items():
                        if trx_id not in blocked_trx_ids:
                            # 为那些阻塞结束的事务分配线程，每个事务一次最多一条语句
                            result_queue = queue.Queue()
                            terminate_event = self.terminate_events[trx_id]
                            thread = self._execute_stmt_async(trx_id, stmt, result_queue, terminate_event, 
                                                             cleaned, local_executed_serial, local_cleaned_executed_serial)
                            pending_stmts[stmt_idx] = (trx_id, stmt, thread, time.time())
                            result_queues[stmt_idx] = result_queue
                            actual_submitted_serial.append((trx_id, stmt))
                            diaodu_sql = stmt
                            diaodu_trx = trx_id
                            logger.info(f"调度阻塞语句成功，blocked_stmts: {blocked_stmts}")
                            del blocked_stmts[stmt_idx]
                            # 设置本轮已调度标志
                            diaodu_flag = True
                            break
                if diaodu_flag:
                    logger.info(f"成功调度{diaodu_trx}:{diaodu_sql}调度后阻塞队列：{blocked_stmts}")
                else:
                    if len(blocked_trx_ids) == 0 and len(blocked_stmts) == 0:
                        logger.info("当前无阻塞事务和阻塞语句")
                    else:
                        logger.info(f"{blocked_trx_ids}都正被阻塞，无法调度")
                    
                logger.info(f"-------------调度阻塞语句结束-------------")
                logger.info("")

                try:
                    if idx >= len(serial):
                        logger.info(f"序列遍历完成，开始查看剩余阻塞的语句：{pending_stmts}")
                        break
                    
                    # 启动新的语句执行
                    if idx < len(serial) and diaodu_flag == False: # 如果本轮没有调度任何语句，则开始遍历新语句
                        logger.info(f"-------------2. 遍历新语句-------------")
                        trx_id, stmt = serial[idx]
                        if stmt is None:
                            logger.error(f"stmt为None, trx_id: {trx_id}, stmt: {stmt}")
                            idx += 1
                            continue

                        # 如果该事务已被回滚，跳过不执行
                        if rollback_trx_ids and trx_id in rollback_trx_ids:
                            logged_serial.append((trx_id, f"--Skipped, trx_id: {trx_id}, stmt: {stmt}"))
                            logger.error(f"事务{trx_id}已回滚，跳过执行{stmt}")

                            pending_stmts_copy = pending_stmts.copy()
                            for s_idx, (trx_id1, stmt1, thread1, _) in pending_stmts_copy.items():
                                if trx_id1 == trx_id:
                                    del pending_stmts[s_idx]
                                    del result_queues[s_idx]
                            trx_in_pending.remove(trx_id) if trx_id in trx_in_pending else None
                            
                            # 移除那个被回滚事务的blocking语句
                            blocked_stmts_copy = blocked_stmts.copy()
                            for s_idx, (trx_id1, stmt1) in blocked_stmts_copy.items():
                                if trx_id1 in rollback_trx_ids and trx_id1 in blocked_trx_ids:
                                    blocked_trx_ids.remove(trx_id1)
                                    del blocked_stmts[s_idx]
                            idx += 1
                            continue
                        
                        # 如果该事务当前已被阻塞
                        if blocked_trx_ids and trx_id in blocked_trx_ids:
                            # 先不给他分配线程，加入等待队列blocked_stmts
                            blocked_stmts[idx] = (trx_id, stmt)
                            idx += 1
                            continue
                        
                        # 为新语句创建结果队列并启动执行
                        result_queue = queue.Queue()
                        terminate_event = self.terminate_events[trx_id]
                        thread = self._execute_stmt_async(trx_id, stmt, result_queue, terminate_event, 
                                                         cleaned, local_executed_serial, local_cleaned_executed_serial)
                        pending_stmts[idx] = (trx_id, stmt, thread, time.time())
                        result_queues[idx] = result_queue
                        trx_in_pending.add(trx_id)
                        actual_submitted_serial.append((trx_id, stmt))
                        idx += 1
                        logger.info(f"创建新语句任务成功，trx_id: {trx_id}, stmt: {stmt}")
                        logger.info(f"-------------2. 遍历新语句结束-------------")
                        logger.info("")

                    logger.info(f"-------------3. 检查pending语句的执行结果-------------")
                    logger.info(f"检查前pending_stmts: {pending_stmts}")
                    
                    
                    stmt_indices = list(pending_stmts.keys())
                    for key in stmt_indices:
                        # 检查key是否还存在于pending_stmts中
                        if key not in pending_stmts:
                            continue
                        
                        # 非阻塞方式检查结果
                        time.sleep(0.1)
                        t, sql, thd, _ = pending_stmts[key]
                        result_queue = result_queues[key]
                        if result_queue.empty():
                            # 说明wait for lock
                            print(f"wait for lock, trx_id: {t}, stmt: {sql}")
                            logger.info(f"wait for lock, trx_id: {t}, stmt: {sql}")
                            try:
                                blocked_trx_ids.add(t)  # 标记为blocked
                            except:
                                logger.error(f"添加blocked_trx_ids失败, trx_id: {t}")
                            continue
                        
                        # 从result_queue中获取结果
                        status, result = result_queue.get_nowait()
                        
                        if status == "success":
                            logged_serial.append((t, sql))
                            logger.info(f"成功执行序列中事务{t}语句: {sql}")

                            del pending_stmts[key]
                            del result_queues[key]

                            logger.info(f"trx_in_pending_before: {trx_in_pending}")
                            logger.info(f"blocked_trx_ids_before: {blocked_trx_ids}")

                            trx_in_pending.remove(t) if t in trx_in_pending else None
                            blocked_trx_ids.remove(t) if t in blocked_trx_ids else None

                            logger.info(f"trx_in_pending_after: {trx_in_pending}")
                            logger.info(f"blocked_trx_ids_after: {blocked_trx_ids}")

                            if sql == "ROLLBACK":
                                rollback_trx_ids.add(t)

                            if blocked_trx_ids and t in blocked_trx_ids:
                                # 结束了阻塞
                                logger.info(f"结束了阻塞，blocked_trx_ids: {blocked_trx_ids}, stmt: {sql}")
                                blocked_trx_ids.remove(t)
                                    
                        elif status == "error":
                            if result.errno == 1213:  # 死锁
                                logger.error(f"死锁错误: {result}，事务{t}语句: {sql}")
                                rollback_trx_ids.add(t)

                                pending_stmts_copy = pending_stmts.copy()
                                for s_idx, (trx_id1, stmt1, thread1, _) in pending_stmts_copy.items():
                                    if trx_id1 == t:
                                        del pending_stmts[s_idx]
                                        del result_queues[s_idx]
                                trx_in_pending.remove(t) if t in trx_in_pending else None
                                blocked_trx_ids.add(t)
                                
                                # 移除那个被回滚事务的blocking语句
                                blocked_stmts_copy = blocked_stmts.copy()
                                for s_idx, (trx_id1, stmt1) in blocked_stmts_copy.items():
                                    if trx_id1 in rollback_trx_ids and trx_id1 in blocked_trx_ids:
                                        del blocked_stmts[s_idx]
                                blocked_trx_ids.remove(trx_id1)

                                if not cleaned and not sub_check_point:
                                    # 第一次执行的完整主序列
                                    self.rollback_trx_ids = rollback_trx_ids

                                logged_serial.append((t, f"-- Cause Deadlock, Rollback. {sql}"))
                                
                            else:
                                logged_serial.append((t, f"-- Error and skipped: {sql}, {result}"))
                                # 跳过该语句,并终止该事务的pending线程
                                thread.join(timeout=0.001)
                                del pending_stmts[key]
                                del result_queues[key]
                                trx_in_pending.remove(t) if t in trx_in_pending else None
                                blocked_trx_ids.remove(t) if t in blocked_trx_ids else None
                                logger.error(f"执行语句失败，非死锁错误: {result}，事务{t}语句: {sql}")
                                
                                if result.errno == 1205:
                                    return False, set(), [], []


                    # 短暂休眠避免CPU占用过高
                    logger.info(f"检查后pending_stmts: {pending_stmts}")
                    logger.info(f"-------------3. 检查结果队列结束-------------")
                    logger.info("")
                    time.sleep(0.1)
                except Exception as e:
                    logger.error(f"错误: {e}")

            while len(pending_stmts)>0 or len(blocked_stmts)>0:
                # 记录pending_stmts中是否已有对应事务语句
                logger.info(f"-------------4. 检查是否可调度阻塞语句-------------")
                logger.info(f"检查前阻塞队列：{blocked_stmts}")
                diaodu_flag = False
                diaodu_sql = None
                diaodu_trx = None
                if len(blocked_stmts)>0:
                    # 如果阻塞语句队列能被调度，优先执行阻塞的语句
                    blocked_stmts_copy = blocked_stmts.copy()
                    for stmt_idx, (trx_id, stmt) in blocked_stmts_copy.items():
                        if trx_id not in blocked_trx_ids and trx_id not in trx_in_pending:
                            # 为那些阻塞结束的事务分配线程，每个事务一次最多一条语句
                            result_queue = queue.Queue()
                            terminate_event = self.terminate_events[trx_id]
                            thread = self._execute_stmt_async(trx_id, stmt, result_queue, terminate_event, 
                                                             cleaned, local_executed_serial, local_cleaned_executed_serial)
                            pending_stmts[stmt_idx] = (trx_id, stmt, thread, time.time())
                            result_queues[stmt_idx] = result_queue
                            trx_in_pending.add(trx_id)
                            actual_submitted_serial.append((trx_id, stmt))
                            diaodu_sql = stmt
                            diaodu_trx = trx_id
                            logger.info(f"调度阻塞语句成功，blocked_stmts: {blocked_stmts}")
                            del blocked_stmts[stmt_idx]
                            # 设置本轮已调度标志
                            diaodu_flag = True
                            break

                if diaodu_flag:
                    logger.info(f"成功调度{diaodu_trx}:{diaodu_sql}调度后阻塞队列：{blocked_stmts}")
                else:
                    logger.info(f"{blocked_trx_ids}都正被阻塞，无法调度")
                    
                logger.info(f"-------------4. 调度阻塞语句结束-------------")
                logger.info("")
                # 专注于剩下的pending_stmt
                stmt_indices = list(pending_stmts.keys())
                for key in stmt_indices:
                    # 检查key是否还存在于pending_stmts中
                    if key not in pending_stmts:
                        continue
                        
                    # 非阻塞方式检查结果
                    time.sleep(0.1)
                    t, sql, thd, _ = pending_stmts[key]
                    result_queue = result_queues[key]
                    if result_queue.empty():
                        # 说明wait for lock
                        print(f"wait for lock, trx_id: {t}, stmt: {sql}")
                        logger.info(f"wait for lock, trx_id: {t}, stmt: {sql}")
                        try:
                            blocked_trx_ids.add(t)  # 标记为blocked
                        except:
                            logger.error(f"添加blocked_trx_ids失败, trx_id: {t}")
                        continue
                    
                    # 从result_queue中获取结果
                    status, result = result_queue.get_nowait()
                    
                    if status == "success":
                        logged_serial.append((t, sql))
                        logger.info(f"成功执行序列中事务{t}语句: {sql}")
                        del pending_stmts[key]
                        del result_queues[key]
                        logger.info(f"trx_in_pending_before: {trx_in_pending}")
                        logger.info(f"blocked_trx_ids_before: {blocked_trx_ids}")
                        
                        trx_in_pending.remove(t) if t in trx_in_pending else None
                        blocked_trx_ids.remove(t) if t in blocked_trx_ids else None

                        logger.info(f"trx_in_pending_after: {trx_in_pending}")
                        logger.info(f"blocked_trx_ids_after: {blocked_trx_ids}")

                        if sql == "ROLLBACK":
                            rollback_trx_ids.add(t)

                        if blocked_trx_ids and t in blocked_trx_ids:
                            # 结束了阻塞
                            logger.info(f"结束了阻塞，blocked_trx_ids: {blocked_trx_ids}, stmt: {sql}")
                            blocked_trx_ids.remove(t) if t in blocked_trx_ids else None
                                
                    elif status == "error":
                        if result.errno == 1213:  # 死锁
                            logger.error(f"死锁错误: {result}，事务{t}语句: {sql}")
                            rollback_trx_ids.add(t)

                            pending_stmts_copy = pending_stmts.copy()
                            for s_idx, (trx_id1, stmt1, thread1, _) in pending_stmts_copy.items():
                                if trx_id1 == t:
                                    del pending_stmts[s_idx]
                                    del result_queues[s_idx]
                            trx_in_pending.remove(t) if t in trx_in_pending else None
                            blocked_trx_ids.add(t)
                            
                            # 移除那个被回滚事务的blocking语句
                            blocked_stmts_copy = blocked_stmts.copy()
                            for s_idx, (trx_id1, stmt1) in blocked_stmts_copy.items():
                                if trx_id1 in rollback_trx_ids and trx_id1 in blocked_trx_ids:
                                    del blocked_stmts[s_idx]
                                blocked_trx_ids.remove(trx_id1)

                            if not cleaned and not sub_check_point:
                                self.rollback_trx_ids = rollback_trx_ids

                            logged_serial.append((t, f"-- Cause Deadlock, Rollback. {sql}"))
                            
                        
                        else:
                            logged_serial.append((t, f"-- Error and skipped: {sql}, {result}"))
                            # 跳过该语句,并终止该事务的pending线程
                            thread.join(timeout=0.001)
                            del pending_stmts[key]
                            del result_queues[key]
                            trx_in_pending.remove(t) if t in trx_in_pending else None
                            blocked_trx_ids.remove(t) if t in blocked_trx_ids else None
                            logger.error(f"执行语句失败，非死锁错误: {result}，事务{t}语句: {sql}")

                            if result.errno == 1205:
                                return False, set(), [], []

            if not sub_check_point:
                if not cleaned:
                    self.executed_serial = local_executed_serial
                else:
                    self.cleaned_executed_serial = local_cleaned_executed_serial
            
            # 所有语句执行完毕后,根据是否发生死锁返回结果
            if rollback_trx_ids:
                return False, set(rollback_trx_ids), local_executed_serial, actual_submitted_serial
            else:
                return True, set(), local_executed_serial, actual_submitted_serial
        
        except Exception as e:
            logger.error(f"执行序列发生错误: {e}")
            print(f"执行序列发生错误: {e}")
            return False, set(), [], []
        finally:
            # 清理资源
            for conn in self.connections.values():
                try:
                    conn.close()
                except Exception as e:
                    logger.error(f"关闭连接失败: {e}")

    def check_atomicity(self, trx_sqls: Dict[int, List[str]], serial: List[Tuple[int, str]], temp_env_dir: str) -> Tuple[bool, str, List[Tuple[int, str]], List[Tuple[int, str]], List[Tuple[int, str]]]:
        """检查事务序列的原子性"""

        
        try:
            logger.info(f"执行序列: {serial}")
            logger.info(f"trx_sqls: {trx_sqls}")

            try:
                conn = self._create_slave_connection()
                self.snapshot_before = self._take_snapshot(conn)
                conn.close()
                
                # 执行原始序列
                success, rollback_trx_ids, executed_serial, actual_submitted_serial = self._execute_serial(serial, cleaned=False, sub_check_point=False)
            except Exception as e:
                logger.error(f"执行序列失败: {e}")
                return False, str(e), [], [], []
                
            # 如果执行成功且没有回滚的事务，即未发生死锁，直接返回，这个测例无效
            if success and len(rollback_trx_ids) == 0:
                logger.info(f"执行序列成功，未发生死锁和手动ROLLBACK，无效test case")
                print(f"执行序列成功，未发生死锁和手动ROLLBACK，无效test case")
                return True, None, serial, [], []
            else:
                # 保存并发执行后的数据库状态
                conn = self._create_slave_connection()
                self.snapshot_serial = self._take_snapshot(conn)
                conn.close()

                # 获取成功和失败的事务集合
                rolled_back_trxs = set(rollback_trx_ids) if rollback_trx_ids else set()
                # 从self.num_transactions中减去rolled_back_trxs
                committed_trxs = set(range(1, self.num_transactions + 1)) - rolled_back_trxs
                self.executed_serial = executed_serial
                self.rollback_trx_ids = rolled_back_trxs
                        
                logger.info(f"被回滚的事务: {sorted(list(rolled_back_trxs))}")
                logger.info(f"成功提交的事务: {sorted(list(committed_trxs))}")
                
                # cleaned_serial = []
                # tx_rolled_back = set()
                # 获取cleaned_serial
                # for trx_id, stmt in self.executed_serial:
                #     if trx_id in rolled_back_trxs:
                #         if trx_id not in tx_rolled_back:
                #             if stmt.startswith("ROLLBACK"):
                #                 tx_rolled_back.add(trx_id)
                #             else:
                #                 continue
                #         else:
                #             cleaned_serial.append((trx_id, stmt))
                #     else:
                #         cleaned_serial.append((trx_id, stmt))
                cleaned_serial = []
                del_idx = []
                

                # 首先遍历找出所有回滚操作
                rollback_points = []  # [(idx, trx_id, type, savepoint_id)]
                for idx, (trx_id, stmt) in enumerate(self.executed_serial):
                    if "ROLLBACK TO" in stmt.strip().upper():
                        savepoint_id = stmt.strip().split()[-1]
                        rollback_points.append((idx, trx_id, "ROLLBACK_TO", savepoint_id))
                    elif stmt.startswith("ROLLBACK"): # 手动ROLLBACK或者ROLLBACK --表示因为死锁回滚的
                        rollback_points.append((idx, trx_id, "ROLLBACK", None))

                # 为每个回滚点生成对应的cleaned_serial
                checkpoint_seq_pair = []# 保存sub_serial和sub_cleaned_serial
                for idx, trx_id, rollback_type, savepoint_id in rollback_points:
                    current_del_idx = set()
                    
                    current_sub_serial = self.executed_serial[:idx + 1]

                    if rollback_type == "ROLLBACK_TO":
                        # 找到对应的SAVEPOINT定义点
                        for i in range(idx - 1, -1, -1):
                            prev_trx_id, prev_stmt = current_sub_serial[i]
                            if prev_stmt.startswith(f"SAVEPOINT {savepoint_id}"):
                                sp_idx = i
                                break
                        
                        # 标记需要删除的语句
                        for i in range(sp_idx, idx + 1):
                            if trx_id == current_sub_serial[i][0]:
                                current_del_idx.add(i)
                    
                    elif rollback_type == "ROLLBACK":
                        # 标记该事务之前的所有语句
                        for i in range(0, idx + 1):
                            if trx_id == current_sub_serial[i][0]:
                                current_del_idx.add(i)


                    # 生成当前回滚点的cleaned_serial
                    current_sub_cleaned_serial = []
                    for i, (t_id, stmt) in enumerate(current_sub_serial):
                        if i not in current_del_idx:
                            current_sub_cleaned_serial.append((t_id, stmt))
                    

                    # 更新checkpoint_seq_pair
                    checkpoint_seq_pair.append((current_sub_serial, current_sub_cleaned_serial))

                    # 更新总的del_idx
                    del_idx.extend(current_del_idx)

                # 生成最终的cleaned_serial
                del_idx = set(del_idx)  # 转换为集合去重
                for idx, (trx_id, stmt) in enumerate(self.executed_serial):
                    if idx not in del_idx:
                        cleaned_serial.append((trx_id, stmt))

                
                # # 更新checkpoint_pair并进行子检查点验证
                # self.sub_cleaned_serial_checkpoint_pair = []
                # for idx, (rollback_idx, trx_id, rollback_type, savepoint_id) in enumerate(rollback_points):
                #     key = f"{trx_id}_{rollback_type}_{savepoint_id if savepoint_id else ''}"
                #     sub_cleaned = sub_cleaned_serials[key]
                #     self.sub_cleaned_serial_checkpoint_pair.append((rollback_idx + 1, len(sub_cleaned)))

                # # 补充手动ROLLBACK或ROLLBACK TO SAVEPOINT的checkpoint_pair
                # for i in range(len(self.sub_cleaned_serial_checkpoint_pair)):
                #     checkpoint_idx = self.sub_cleaned_serial_checkpoint_pair[i][0]
                #     rollback_stmt_idx = checkpoint_idx - 1
                #     # 找到rollback语句前面最近的一条语句在cleaned_serial中的位置
                #     c = rollback_stmt_idx - 1
                #     found = False
                #     while c >= 0:
                #         for idx, (trx_id, stmt) in enumerate(cleaned_serial):
                #             if self.executed_serial[c] == (trx_id, stmt):
                #                 self.sub_cleaned_serial_checkpoint_pair[i] = (checkpoint_idx, idx + 1)
                #                 found = True
                #                 break
                #         if found:
                #             break
                #         c -= 1
                #     if not found:
                #         logger.warning(f"无法为checkpoint_pair[{i}]找到合适的cpt2值，原值: {self.sub_cleaned_serial_checkpoint_pair[i]}，赋值为0")
                #         self.sub_cleaned_serial_checkpoint_pair[i] = (checkpoint_idx, 0)

                # logger.info(f"checkpoint_pair: {self.sub_cleaned_serial_checkpoint_pair}")
                self.sub_checkpoint_pair = checkpoint_seq_pair
                logger.info(f"checkpoint_seq_pair: {self.sub_checkpoint_pair}")

                # 检查cleaned_serial是否等价于serial
                try:
                    self._restore_initial_state(temp_env_dir)
                    logger.info("恢复初始状态成功")
                except Exception as e:
                    logger.error(f"恢复初始状态失败: {e}")
                    return False, str(e), [], [], []

                logger.info(f"清理前的序列: {self.executed_serial}")
                logger.info(f"清理后的序列: {cleaned_serial}")
                cleaned_success, cleaned_rollback_trx_ids, cleaned_executed_serial, cleaned_actual_submitted_serial = self._execute_serial(cleaned_serial, cleaned=True, sub_check_point=False)
                logger.info(f"清理后的序列执行成功: {cleaned_success}")
                
                    
                # 获取清理后序列的执行状态
                conn = self._create_slave_connection()
                self.snapshot_cleaned = self._take_snapshot(conn)
                conn.close()
                
                # 1. 整体原子性检查
                res = True  # 用于记录整体检查结果
                error_msg = ""  # 用于累积错误信息
                
                if not self._compare_snapshots(self.snapshot_serial, self.snapshot_cleaned):
                    error_msg += (
                        "整体违反原子性：\n"
                        f"serial执行状态: {self.snapshot_serial}\n"
                        f"cleaned_serial执行状态: {self.snapshot_cleaned}\n"
                        f"执行序列: {self.executed_serial}\n"
                        f"清理后序列: {cleaned_serial}\n\n"
                    )
                    res = False
                    logger.error(error_msg)

                # 2. 子检查点原子性检查
                for idx, (sub_serial, sub_cleaned_serial) in enumerate(self.sub_checkpoint_pair):
                    
                    # 找到对应的sub_cleaned_serial
                    logger.info(f"子检查点{idx}, cpt1: {len(sub_serial)}, cpt2: {len(sub_cleaned_serial)}")
                    logger.info(f"sub_serial: {sub_serial}")
                    logger.info(f"sub_cleaned_serial: {sub_cleaned_serial}")

                    self._restore_initial_state(temp_env_dir)
                    
                    # 分别执行sub_executed_serial和sub_cleaned_serial并获取快照
                    # 执行sub_serial
                    sub_success, sub_rollback_ids, sub_actual_executed, sub_actual_submitted = self._execute_serial(sub_serial, cleaned=False, sub_check_point=True)
                    conn = self._create_slave_connection()
                    sub_snapshot = self._take_snapshot(conn)
                    conn.close()

                    # 恢复初始状态后执行sub_cleaned_serial
                    self._restore_initial_state(temp_env_dir)
                    sub_success, sub_rollback_ids, sub_actual_executed, sub_actual_submitted = self._execute_serial(sub_cleaned_serial, cleaned=False, sub_check_point=True)
                    conn = self._create_slave_connection()
                    sub_cleaned_snapshot = self._take_snapshot(conn)
                    conn.close()

                    # 获取对应的rollback to savepoint的快照
                    last_cell = sub_serial[-1]
                    sp_trx_id = last_cell[0]
                    rollback_stmt = last_cell[1]

                    if "ROLLBACK TO" in rollback_stmt:
                        sp_id = rollback_stmt.strip().split()[-1]
                        # sp_snapshot = self.sp_snapshot[sp_trx_id][sp_id]
                    elif "ROLLBACK" == rollback_stmt or "ROLLBACK --" in rollback_stmt:
                        # sp_snapshot = self.rollback_snapshot[sp_trx_id]
                        pass
                    else:
                        logger.error(f"最后一个语句不是ROLLBACK TO或ROLLBACK: {rollback_stmt}")
                        raise

                    # 比较sub_cleaned_snapshot和sp_snapshot
                    # if not self._compare_snapshots(sub_cleaned_snapshot, sp_snapshot):
                    if not self._compare_snapshots(sub_snapshot, sub_cleaned_snapshot):
                        # 增加更多详细信息，包括是哪个子检查点，以及相关SQL
                        sub_violation_info = (
                            f"{idx}违反原子性：{sp_trx_id}:{sp_id}\n"
                            f"Savepoint {sp_id}\n"
                            f"原始子序列执行状态: {sub_snapshot}\n"
                            f"清理后子序列执行状态: {sub_cleaned_snapshot}\n"
                            f"子检查点事务ID: {sp_trx_id}, Savepoint ID: {sp_id}\n"
                            f"执行到的位置: 第{len(sub_serial)}条语句, 清理后位置: 第{len(sub_cleaned_serial)}条语句\n"
                            f"原始子序列: {sub_serial}\n"
                            f"清理后子序列: {sub_cleaned_serial}\n\n"
                        )
                        error_msg += f"子检查点{sub_violation_info}"
                        res = False
                        logger.error(f"子检查点{idx}违反原子性")
                        logger.error(f"子检查点事务ID: {sp_trx_id}, Savepoint ID: {sp_id}")
                        logger.error(f"子序列对：[0,{len(sub_serial)}], 清理后位置: [0,{len(sub_cleaned_serial)}]")

                # 根据整体检查结果返回
                return res, error_msg, self.executed_serial, cleaned_serial, actual_submitted_serial
        finally:
            # 如果使用的是临时目录且不是bug_case_dir，则删除它
            if temp_env_dir is not None and os.path.exists(temp_env_dir):
                self._restore_initial_state(temp_env_dir)
                import shutil
                try:
                    shutil.rmtree(temp_env_dir)
                    logger.info(f"临时导出目录已删除: {temp_env_dir}")
                except Exception as e:
                    logger.warning(f"删除临时导出目录失败: {e}")

    def _create_connection(self):
        """创建数据库连接"""
        try:
            return mysql.connector.connect(**self.db_config)
        except mysql.connector.Error as err:
            logger.error(f"主节点数据库连接失败: {err}")
            logger.error("")
            raise
    
    def _create_slave_connection(self):
        """创建数据库连接"""
        try:
            return mysql.connector.connect(**self.slave_db_config)
        except mysql.connector.Error as err:
            logger.error(f"从节点数据库连接失败: {err}")
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
                    # 检查是否为存储过程调用
                    if stmt.strip().upper().startswith("CALL"):
                        # 第一步：执行存储过程调用
                        cursor.execute(stmt)
                        # 第二步：如果有结果集，获取结果
                        if cursor.with_rows:
                            results = cursor.fetchall()
                            # 可以将结果保存或处理
                    else:
                        # 其他语句正常执行
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

    def _restore_initial_state(self, bug_case_dir: str):
        """使用导出的 SQL 文件恢复数据库到初始状态"""
        try:
            os.system(f"mysql -h localhost -P {self.db_config['port']} -u {self.db_config['user']} -p{self.db_config['password']} {self.db_config['database']} < {bug_case_dir}/initial_state.sql")
            return
        except Exception as e:
            logger.error(f"使用 SQL 文件恢复初始状态时发生错误: {e}")
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

    def get_snapshots(self) -> Dict[str, Dict[str, List[Tuple]]]:
        """获取所有快照数据，用于调试"""
        return {
            "before": self.snapshot_before,
            "serial": self.snapshot_serial,
            "cleaned": self.snapshot_cleaned
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
            
            SET1 = set()
            SET2 = set()
            for table_name in snapshot1.keys():
                SET1.clear()
                SET2.clear()
                if len(snapshot1[table_name]) != len(snapshot2[table_name]):
                    return False
                
                # 对行进行排序以确保比较的一致性
                rows1 = sorted(snapshot1[table_name])
                rows2 = sorted(snapshot2[table_name])
                
                for row1, row2 in zip(rows1, rows2):
                    SET1.add(row1[1:])
                    SET2.add(row2[1:])

                if SET1 != SET2:
                    return False

            return True
        except Exception as e:
            logger.error(f"比较快照失败: {e}")
            return False


def parse_arguments():
    parser = argparse.ArgumentParser(description='MySQL Atomicity Checker')
    parser.add_argument('--db', type=str, choices=["mysql", "mysql-cluster", "tidb"], default="mysql",
                        help='数据库类型')
    parser.add_argument('--port', type=int, default=4005,
                        help='数据库端口')
    return parser.parse_args()

# 设置全局锁等待超时
args = parse_arguments()
db = args.db
port = args.port
isolation_level = random.choice(["SER", "RR", "RC", "RU"])
# 修改连接池配置

dbconfig = {
    "host": "localhost",
    "user": "root", 
    "password": "123456",
    "database": "test",
    "port": port,
    "connection_timeout": 60,
    "use_pure": True,            # 使用纯Python实现
    "buffered": True,            # 使用buffered模式
    "raise_on_warnings": True,   # 立即抛出警告
    "get_warnings": True,        # 获取警告信息
    "consume_results": True,     # 自动消费结果
    "autocommit": False,         # 显式控制事务
}
conn = mysql.connector.connect(**dbconfig)
cursor = conn.cursor()
cursor.execute("SET GLOBAL innodb_lock_wait_timeout=50")
cursor.execute("SET GLOBAL net_write_timeout = 600000")
cursor.execute("SET GLOBAL net_read_timeout = 600000")
# 设置隔离级别
cursor.execute(f"set global transaction isolation level {ISOLATION_LEVELS[isolation_level]}")
cursor.close()
conn.close()




# 进行实验
num_of_runs = 2000
logger.info("INFO TEST")
logger.debug("DEBUG TEST")
logger.error("ERROR TEST")
bug_count = 1


date_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
SAVE_DIR = f"{database_save_dir}/{isolation_level}/{date_time}"
for i in range(num_of_runs):
    args = parse_arguments()

    # num_of_tables = random.randint(1, 3)
    num_of_tables = random.randint(1, 2)
    num_transactions = random.randint(1, 4)

    # 添加日志记录
    print(f"iter: {i}") 
    logger.info(f"iter: {i}")
    logger.info("----------SETTINGS-----------")
    logger.info(f"Isolation level: {isolation_level}")
    logger.info(f"database: {dbconfig['database']}")
    logger.info(f"port: {dbconfig['port']}")
    logger.info(f"user: {dbconfig['user']}")
    logger.info(f"Num of Tables: {num_of_tables}")
    logger.info(f"Num of Transactions: {num_transactions}")
    logger.info("-----------------------------")
    conn = None
    try:
        # 先不指定数据库连接
        dbconfig_no_db = dbconfig.copy()
        dbconfig_no_db.pop("database")
        conn = mysql.connector.connect(**dbconfig_no_db)
        
        try:
            initializer = MySQLInitializer(
                connection=conn,
                database="test"
            )
        except Exception as e:
            print(f"初始化Initializer失败: {e}")
            print("")
            logger.error(f"初始化Initializer失败: {e}")
            logger.error("")
            raise
        

        # 初始化数据库
        try:
            initializer.initialize_database()
            initializer.generate_tables_with_data_and_index(num_of_tables)
            initializer.commit_and_close()
        except Exception as e:
            print(f"初始化数据库失败: {e}")
            print("")
            logger.error(f"初始化数据库失败: {e}")
            logger.error("")
            raise
        
        # 构建列信息字典，格式需要与 TransactionGenerator 期望的格式匹配
        tables = initializer.tables
        columns = initializer.columns
        
        # 提交初始化更改
        conn.commit()
        cursor.close()

        try:
            # 使用 TransactionGenerator 生成事务
            tx_generator = TransactionGenerator(tables, columns, isolation_level)
        except Exception as e:
            print(f"初始化事务生成器失败: {e}")
            print("")
            logger.error(f"初始化事务生成器失败: {e}")
            logger.error("")
            raise
        
        trx_sqls = {}
        serial = []
        rollback_trx_num = 0
        try:
            for tx_id in range(1, num_transactions + 1):
                statements = tx_generator.gen_transaction(tx_id)
                trx_sqls[tx_id] = statements
                for statement in statements:
                    if "ROLLBACK" in statement:
                        rollback_trx_num += 1
                        break

        except Exception as e:
            print(f"生成事务语句失败: {e}")
            print("")
            logger.error(f"生成事务语句失败: {e}")
            logger.error("")
            raise

        # 至少有一个事务是包含条件回滚rollback，没有就随机选择若干个事务
        if rollback_trx_num == 0:
            rollback_trx_num = random.randint(1, num_transactions)

        # 随机选择rollback_trx_num个事务，将最终的commit改为rollback
        for tx_id in random.sample(range(1, num_transactions + 1), rollback_trx_num):
            trx_sqls[tx_id][-1] = "ROLLBACK"
            
        # 随机组合各个事务的语句成为serial
        tx_positions = {}  # 记录每个事务当前执行到的位置
        available_txs = list(range(1, num_transactions + 1))  # 可选的事务ID列表
        
        # 初始化每个事务的位置为0
        for tx_id in range(1, num_transactions + 1):
            tx_positions[tx_id] = 0
        
        submitted_order = []
        # 随机选择事务并添加其下一条语句，直到所有语句都被添加
        while available_txs:
            # 随机选择一个事务
            tx_id = random.choice(available_txs)
            current_pos = tx_positions[tx_id]
            
            # 添加该事务的下一条语句
            serial.append((tx_id, trx_sqls[tx_id][current_pos]))
            submitted_order.append(tx_id)

            # 更新位置
            tx_positions[tx_id] += 1
            
            # 如果该事务的所有语句都已添加，从可选列表中移除
            if tx_positions[tx_id] >= len(trx_sqls[tx_id]):
                available_txs.remove(tx_id)
        
        logger.info(f"submitted_order: {submitted_order}")
        logger.info(f"serial: {serial}")
            
        # 创建原子性检查器
        atomicity_checker = AtomicityChecker(
            "localhost", "root", "123456", "test", port, isolation_level,
            trx_sqls, serial
        )
        
        temp_env_dir = f"{database_save_dir}/temp_{int(time.time())}"
        os.makedirs(temp_env_dir, exist_ok=True)

        # 先导出当前数据库状态，包含触发器和存储过程，供恢复使用
        temp_env_dump_cmd = f"mysqldump -h localhost -P {dbconfig['port']} -u {dbconfig['user']} -p{dbconfig['password']} --routines --triggers --set-gtid-purged=OFF {dbconfig['database']} > {temp_env_dir}/initial_state.sql"
        os.system(temp_env_dump_cmd)
        

        # 检查原子性
        is_atomic, info, executed_serial, cleaned_serial, actual_submitted_serial = atomicity_checker.check_atomicity(trx_sqls, serial, temp_env_dir)
        snapshots = atomicity_checker.get_snapshots()
        rollback_trx_id = atomicity_checker.rollback_trx_ids
        print('is_atomic:', is_atomic)
        print('info:', info)
        print('executed_serial:', executed_serial)
        print('cleaned_serial:', cleaned_serial)
        print('rollback_trx_id:', rollback_trx_id)
        print('snapshots:', snapshots)
        print()

        logger.info(f"is_atomic: {is_atomic}")
        logger.info(f"info: {info}")
        logger.info(f"executed_serial: {executed_serial}")
        logger.info(f"actual_submitted_serial: {actual_submitted_serial}")
        logger.info(f"cleaned_serial: {cleaned_serial}")
        logger.info(f"rollback_trx_id: {rollback_trx_id}")
        logger.info(f"snapshots: {snapshots}")
        logger.info("")

        if not is_atomic:
            # 创建保存整体bug case的目录
            bug_case_dir = f"{SAVE_DIR}/bug_case_{bug_count}"
            bug_count += 1
            os.makedirs(bug_case_dir, exist_ok=True)
            dump_cmd = f"mysqldump -h localhost -P 4000 -u root -p123456 --routines --triggers --set-gtid-purged=OFF test > {bug_case_dir}/initial_state.sql"
            os.system(dump_cmd)

            # 保存事务和序列信息
            with open(f"{bug_case_dir}/transactions.sql", 'w') as f:
                for trx_id, sqls in trx_sqls.items():
                    f.write(f"-- Transaction {trx_id}\n")
                    f.write("\n".join(sqls) + "\n\n")
                f.write("-- Planned Serial\n")
                f.write("\n".join([f"-- Transaction {t[0]}: {t[1]}" for t in serial]) + "\n\n")
                f.write("-- Actually Executed Serial\n")
                f.write("\n".join([f"-- Transaction {t[0]}: {t[1]}" for t in executed_serial]) + "\n\n")
                f.write("-- Actual Submitted Serial\n")
                f.write("\n".join([f"-- Transaction {t[0]}: {t[1]}" for t in actual_submitted_serial]) + "\n\n")
                f.write("-- Cleaned Serial\n")
                f.write("\n".join([f"-- Transaction {t[0]}: {t[1]}" for t in cleaned_serial]) + "\n\n")
            
            # 记录其他相关信息
            with open(f"{bug_case_dir}/metadata.txt", 'w') as f:
                f.write(f"Number of Transactions: {num_transactions}\n")
                f.write(f"Bug Info: {info}\n")
                
                # 检查是否包含子检查点违反原子性的信息
                if "子检查点" in info:
                    f.write(f"Contains Sub-Checkpoint Violations: True\n")
                else:
                    f.write(f"Contains Sub-Checkpoint Violations: False\n")
                    
                f.write(f"Snapshots: {snapshots}\n")
            
            # 如果有子检查点违反原子性的情况，单独保存子检查点信息
            if "子检查点" in info:
                # 提取子检查点相关信息
                sub_checkpoint_sections = info.split("子检查点")
                for i, section in enumerate(sub_checkpoint_sections):
                    if i == 0:  # 跳过第一部分(整体违反信息)
                        continue
                    
                    # 为每个子检查点创建单独的目录
                    sub_bug_case_dir = f"{SAVE_DIR}/bug_case_{bug_count}_subcheckpoint_{i}"
                    os.makedirs(sub_bug_case_dir, exist_ok=True)
                    
                    # 导出数据库状态（子检查点对应的状态）
                    try:
                        # 恢复初始状态
                        atomicity_checker._restore_initial_state(bug_case_dir)
                        
                        # # 导出初始状态
                        # sub_dump_cmd = f"mysqldump -h localhost -P 4000 -u root -p123456 --routines --triggers --set-gtid-purged=OFF test > {sub_bug_case_dir}/initial_state.sql"
                        # os.system(sub_dump_cmd)
                    except Exception as e:
                        logger.error(f"导出子检查点{i}数据库状态时出错: {e}")
                    
                    # 提取子检查点ID、事务ID和Savepoint ID
                    try:
                        checkpoint_info = section.split('：')[0]
                        sub_idx = checkpoint_info.split('违反原子性')[0]
                        trx_savepoint_info = checkpoint_info.split('违反原子性：')[1].split(':')
                        sub_trx_id = trx_savepoint_info[0]
                        sub_sp_id = trx_savepoint_info[1].split('\n')[0]
                        
                        # 提取原始子序列和清理后子序列
                        sub_executed_serial_str = section.split('原始子序列: ')[1].split('\n')[0]
                        sub_cleaned_serial_str = section.split('清理后子序列: ')[1].split('\n')[0]
                        
                        # 转换字符串表示为实际序列（简化处理）
                        sub_executed_serial = eval(sub_executed_serial_str)
                        sub_cleaned_serial = eval(sub_cleaned_serial_str)
                        
                        # 保存子检查点详细信息
                        with open(f"{sub_bug_case_dir}/metadata.txt", 'w') as f:
                            f.write(f"Sub-Checkpoint: {i}\n")
                            f.write(f"Transaction ID: {sub_trx_id}\n")
                            f.write(f"Savepoint ID: {sub_sp_id}\n")
                            f.write(f"Bug Info: 子检查点{section}\n")
                        
                        # 保存子检查点序列
                        with open(f"{sub_bug_case_dir}/transactions.sql", 'w') as f:
                            f.write(f"-- Sub-Checkpoint {i}\n")
                            f.write(f"-- Transaction ID: {sub_trx_id}\n")
                            f.write(f"-- Savepoint ID: {sub_sp_id}\n\n")
                            
                            # 保存原始事务
                            for trx_id, sqls in trx_sqls.items():
                                f.write(f"-- Transaction {trx_id}\n")
                                f.write("\n".join(sqls) + "\n\n")
                            
                            # 保存子检查点相关序列
                            f.write(f"-- Sub-Executed Serial\n")
                            f.write("\n".join([f"-- Transaction {t[0]}: {t[1]}" for t in sub_executed_serial]) + "\n\n")
                            f.write(f"-- Sub-Cleaned Serial\n")
                            f.write("\n".join([f"-- Transaction {t[0]}: {t[1]}" for t in sub_cleaned_serial]) + "\n\n")
                    except Exception as e:
                        logger.error(f"处理子检查点{i}时出错: {e}")
                        continue
                
                # 总合文件
                with open(f"{bug_case_dir}/sub_checkpoint_violations.txt", 'w') as f:
                    f.write(f"完整的错误信息：\n{info}\n\n")
                    
                    for i, section in enumerate(sub_checkpoint_sections):
                        if i == 0:  # 跳过第一部分(整体违反信息)
                            continue
                        f.write(f"===== 子检查点违反 {i} =====\n")
                        f.write(f"子检查点{section}\n\n")
            
            # 记录日志
            logger.error(f"BUG FOUND: {info}")
            logger.info(f"Number of Transactions: {num_transactions}")
            logger.info(f"Transactions: {trx_sqls}")
            logger.info(f"Planned Serial: {serial}")
            logger.info(f"Actually Executed Serial: {executed_serial}")
            logger.info(f"Actual Submitted Serial: {actual_submitted_serial}")
            
            # 增加子检查点信息的日志记录
            if "子检查点" in info:
                logger.error("=== 子检查点违反原子性详情 ===")
                sub_checkpoint_sections = info.split("子检查点")
                for i, section in enumerate(sub_checkpoint_sections):
                    if i == 0:  # 跳过第一部分(整体违反信息)
                        continue
                    checkpoint_info = section.split('：')[0]
                    logger.error(f"子检查点违反 {i}: {checkpoint_info}")
            
            logger.info(f"Snapshots: {snapshots}")
            logger.info("")

            print(f"BUG FOUND: {info}")
            print(f"Number of Transactions: {num_transactions}")
            print(f"Transactions: {trx_sqls}")
            print(f"Planned Serial: {serial}")
            print(f"Actually Executed Serial: {executed_serial}")
            print(f"Actual Submitted Serial: {actual_submitted_serial}")
            
            # 添加子检查点信息打印
            if "子检查点" in info:
                print("\n=== 子检查点违反原子性情况 ===")
                sub_checkpoint_sections = info.split("子检查点")
                for i, section in enumerate(sub_checkpoint_sections):
                    if i == 0:  # 跳过第一部分(整体违反信息)
                        continue
                    
                    try:
                        checkpoint_info = section.split('：')[0]
                        sub_idx = checkpoint_info.split('违反原子性')[0]
                        trx_savepoint_info = checkpoint_info.split('违反原子性：')[1].split(':')
                        sub_trx_id = trx_savepoint_info[0]
                        sub_sp_id = trx_savepoint_info[1].split('\n')[0]
                        
                        # 提取原始子序列和清理后子序列
                        sub_executed_serial_str = section.split('原始子序列: ')[1].split('\n')[0]
                        sub_cleaned_serial_str = section.split('清理后子序列: ')[1].split('\n')[0]
                        
                        print(f"子检查点 {i} - 事务ID: {sub_trx_id}, Savepoint: {sub_sp_id}")
                        print(f"原始子序列: {sub_executed_serial_str}")
                        print(f"清理后子序列: {sub_cleaned_serial_str}")
                        print("")
                    except Exception as e:
                        print(f"处理子检查点{i}时出错: {e}")
                        continue
            
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
            if 'atomicity_checker' in locals():
                del atomicity_checker
        except:
            pass

        # 强制垃圾回收
        import gc
        gc.collect()
