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

    def execute_stmt_async(self, trx_id: int, stmt: str, result_queue: queue.Queue, terminate_event: threading.Event, cleaned: bool):
        """异步执行SQL语句"""
        def _execute():
            nonlocal trx_id
            nonlocal stmt
            conn = self.connections[trx_id]
            lock = self.conn_locks[trx_id]
            try:
                with lock:
                    cursor = conn.cursor(buffered=True)
                    cursor.execute(stmt)
                    if stmt.strip().upper().startswith("SELECT"):
                        cursor.fetchall()
                result_queue.put(("success", None))
                self.executed_serial.append((trx_id, stmt)) if not cleaned else None
                self.cleaned_executed_serial.append((trx_id, stmt)) if cleaned else None
                cursor.close()
            except mysql.connector.Error as err:
                if err.errno == 1213:
                    logger.error(f"     异步执行死锁: {err}, stmt: {stmt}")
                    self.executed_serial.append((trx_id, f"ROLLBACK --{stmt}")) if not cleaned else None
                    self.cleaned_executed_serial.append((trx_id, f"ROLLBACK --{stmt}")) if cleaned else None
                    terminate_event.set()
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
    

    def _execute_serial(self, serial: List[Tuple[int, str]], cleaned: bool) -> Tuple[bool, Set[int], List[Tuple[int, str]], List[Tuple[int, str]]]:
        """使用多个独立连接按序执行事务场景
        
        Args:
            serial: 死锁场景的执行序列，格式为[(事务ID, SQL语句), ...]
        
        Returns:
            Tuple[bool, Optional[int], List[Tuple]]: 是否成功执行所有语句(死锁或错误会返回False), 回滚的事务ID, 执行序列
        """

        try:
            actual_trx_ids = set(tx_id for tx_id, _ in serial)
            self._init_connections(actual_trx_ids)
            # 有几个事务就创建几个terminate_event
            self.terminate_events = {trx_id: threading.Event() for trx_id in actual_trx_ids}

            # 记录每个事务当前执行到的位置
            idx = 0
            # executed_serial = []
            actual_submitted_serial = []
            logged_serial = []

            self.executed_serial = [] if not cleaned else self.executed_serial
            self.pending_stmts = {}
            self.trx_in_pending = set()
            self.result_queues = {}
            self.rollback_trx_ids = set()

            # 记录被阻塞的事务ID
            rollback_trx_ids = set()
            blocked_trx_ids = set()
            blocked_stmts = {}

            serial = self.serial if serial is None else serial

            while idx < len(serial):
                logger.info(f"idx: {idx}")

                # 记录pending_stmts中是否已有对应事务语句
                logger.info(f"-------------首先检查是否可调度阻塞语句-------------")
                logger.info(f"调度前：阻塞队列:{blocked_stmts}")
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
                            thread = self.execute_stmt_async(trx_id, stmt, result_queue, terminate_event, cleaned)
                            self.pending_stmts[stmt_idx] = (trx_id, stmt, thread, time.time())
                            self.result_queues[stmt_idx] = result_queue
                            actual_submitted_serial.append((trx_id, stmt))
                            diaodu_sql = stmt
                            diaodu_trx = trx_id
                            logger.info(f"调度阻塞语句成功，blocked_stmts: {blocked_stmts}")
                            del blocked_stmts[stmt_idx]
                            # 设置本轮已调度标志
                            diaodu_flag = True
                            break
                if diaodu_flag:
                    logger.info(f"成功调度{diaodu_trx}:{diaodu_sql}调度后：{blocked_stmts}")
                else:
                    if len(blocked_trx_ids) == 0 and len(blocked_stmts) == 0:
                        logger.info("当前无阻塞事务和阻塞语句")
                    else:
                        logger.info(f"{blocked_trx_ids}都正被阻塞，无法调度")
                    
                logger.info(f"-------------调度阻塞语句结束-------------")
                logger.info("")

                try:
                    if idx >= len(serial):
                        logger.info(f"序列遍历完成，开始查看剩余阻塞的语句：{self.pending_stmts}")
                        break
                    
                    # 启动新的语句执行
                    if idx < len(serial) and diaodu_flag == False: # 如果本轮没有调度任何语句，则开始遍历新语句
                        logger.info(f"-------------开始遍历新语句-------------")
                        trx_id, stmt = serial[idx]
                        if stmt is None:
                            logger.error(f"stmt为None, trx_id: {trx_id}, stmt: {stmt}")
                            idx += 1
                            continue

                        # 如果该事务已被回滚，跳过不执行
                        if rollback_trx_ids and trx_id in rollback_trx_ids:
                            # executed_serial.append((trx_id, f"--Skipped, trx_id: {trx_id}, stmt: {stmt}"))
                            logged_serial.append((trx_id, f"--Skipped, trx_id: {trx_id}, stmt: {stmt}"))
                            logger.error(f"事务{trx_id}已回滚，跳过执行{stmt}")

                            pending_stmts_copy = self.pending_stmts.copy()
                            for s_idx, (trx_id1, stmt1, thread1, _) in pending_stmts_copy.items():
                                if trx_id1 == trx_id:
                                    del self.pending_stmts[s_idx]
                                    del self.result_queues[s_idx]
                            self.trx_in_pending.remove(trx_id) if trx_id in self.trx_in_pending else None
                            # blocked_trx_ids.remove(trx_id) if trx_id in blocked_trx_ids else None
                            
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
                        thread = self.execute_stmt_async(trx_id, stmt, result_queue, terminate_event, cleaned)
                        self.pending_stmts[idx] = (trx_id, stmt, thread, time.time())
                        self.result_queues[idx] = result_queue
                        self.trx_in_pending.add(trx_id)
                        actual_submitted_serial.append((trx_id, stmt))
                        idx += 1
                        logger.info(f"创建新语句任务成功，trx_id: {trx_id}, stmt: {stmt}")
                        logger.info(f"-------------遍历新语句结束-------------")
                    logger.info("")

                    logger.info(f"-------------开始周期性检查pending语句的执行结果-------------")
                    logger.info(f"执行前idx: {idx}, pending_stmts: {self.pending_stmts}")
                    
                    
                    stmt_indices = list(self.pending_stmts.keys())
                    for key in stmt_indices:
                        # 检查key是否还存在于pending_stmts中
                        if key not in self.pending_stmts:
                            continue
                            
                        # 非阻塞方式检查结果
                        time.sleep(0.1)
                        t, sql, thd, _ = self.pending_stmts[key]
                        # status, result = self.result_queues[stmt_idx].get_nowait()
                        result_queue = self.result_queues[key]
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
                            # executed_serial.append((t, sql))
                            logged_serial.append((t, sql))
                            logger.info(f"成功执行序列中事务{t}语句: {sql}")

                            del self.pending_stmts[key]
                            del self.result_queues[key]

                            logger.info(f"trx_in_pending: {self.trx_in_pending}")
                            logger.info(f"blocked_trx_ids: {blocked_trx_ids}")
                            self.trx_in_pending.remove(t) if t in self.trx_in_pending else None
                            blocked_trx_ids.remove(t) if t in blocked_trx_ids else None

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

                                pending_stmts_copy = self.pending_stmts.copy()
                                for s_idx, (trx_id1, stmt1, thread1, _) in pending_stmts_copy.items():
                                    if trx_id1 == t:
                                        del self.pending_stmts[s_idx]
                                        del self.result_queues[s_idx]
                                self.trx_in_pending.remove(t) if t in self.trx_in_pending else None
                                blocked_trx_ids.add(t)
                                
                                # 移除那个被回滚事务的blocking语句
                                blocked_stmts_copy = blocked_stmts.copy()
                                for s_idx, (trx_id1, stmt1) in blocked_stmts_copy.items():
                                    if trx_id1 in rollback_trx_ids and trx_id1 in blocked_trx_ids:
                                        del blocked_stmts[s_idx]
                                blocked_trx_ids.remove(trx_id1)

                                if not cleaned:
                                    self.rollback_trx_ids = rollback_trx_ids
                                # executed_serial.append((t, "ROLLBACK"))
                                logged_serial.append((t, f"-- Cause Deadlock, Rollback. {sql}"))
                            else:
                                logged_serial.append((t, f"-- Error and skipped: {sql}, {result}"))
                                # 跳过该语句,并终止该事务的pending线程
                                thread.join(timeout=0.001)
                                del self.pending_stmts[key]
                                del self.result_queues[key]
                                self.trx_in_pending.remove(t) if t in self.trx_in_pending else None
                                blocked_trx_ids.remove(t) if t in blocked_trx_ids else None
                                logger.error(f"执行语句失败，非死锁错误: {result}，事务{t}语句: {sql}")
                                
                                if result.errno == 1205:
                                    return False, set(), [], []


                    # 短暂休眠避免CPU占用过高
                    logger.info(f"执行后idx: {idx}, pending_stmts: {self.pending_stmts}")
                    logger.info(f"-------------周期性检查结果队列结束-------------")
                    logger.info("")
                    time.sleep(0.1)
                except Exception as e:
                    logger.error(f"错误: {e}")

            while len(self.pending_stmts)>0 or len(blocked_stmts)>0:
                # 记录pending_stmts中是否已有对应事务语句
                logger.info(f"-------------首先检查是否可调度阻塞语句-------------")
                logger.info(f"调度前阻塞队列：{blocked_stmts}")
                diaodu_flag = False
                diaodu_sql = None
                diaodu_trx = None
                if len(blocked_stmts)>0:
                    # 如果阻塞语句队列能被调度，优先执行阻塞的语句
                    blocked_stmts_copy = blocked_stmts.copy()
                    for stmt_idx, (trx_id, stmt) in blocked_stmts_copy.items():
                        if trx_id not in blocked_trx_ids and trx_id not in self.trx_in_pending:
                            # 为那些阻塞结束的事务分配线程，每个事务一次最多一条语句
                            result_queue = queue.Queue()
                            terminate_event = self.terminate_events[trx_id]
                            thread = self.execute_stmt_async(trx_id, stmt, result_queue, terminate_event, cleaned)
                            self.pending_stmts[stmt_idx] = (trx_id, stmt, thread, time.time())
                            self.result_queues[stmt_idx] = result_queue
                            self.trx_in_pending.add(trx_id)
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
                    
                logger.info(f"-------------调度阻塞语句结束-------------")
                logger.info("")
                # 专注于剩下的pending_stmt
                stmt_indices = list(self.pending_stmts.keys())
                for key in stmt_indices:
                    # 检查key是否还存在于pending_stmts中
                    if key not in self.pending_stmts:
                        continue
                        
                    # 非阻塞方式检查结果
                    time.sleep(0.1)
                    t, sql, thd, _ = self.pending_stmts[key]
                    # status, result = self.result_queues[stmt_idx].get_nowait()
                    result_queue = self.result_queues[key]
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
                        # executed_serial.append((t, sql))
                        logged_serial.append((t, sql))
                        logger.info(f"成功执行序列中事务{t}语句: {sql}")
                        del self.pending_stmts[key]
                        del self.result_queues[key]
                        logger.info(f"trx_in_pending: {self.trx_in_pending}")
                        logger.info(f"blocked_trx_ids: {blocked_trx_ids}")
                        self.trx_in_pending.remove(t) if t in self.trx_in_pending else None
                        blocked_trx_ids.remove(t) if t in blocked_trx_ids else None

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

                            pending_stmts_copy = self.pending_stmts.copy()
                            for s_idx, (trx_id1, stmt1, thread1, _) in pending_stmts_copy.items():
                                if trx_id1 == t:
                                    del self.pending_stmts[s_idx]
                                    del self.result_queues[s_idx]
                            self.trx_in_pending.remove(t) if t in self.trx_in_pending else None
                            blocked_trx_ids.add(t)
                            
                            # 移除那个被回滚事务的blocking语句
                            blocked_stmts_copy = blocked_stmts.copy()
                            for s_idx, (trx_id1, stmt1) in blocked_stmts_copy.items():
                                if trx_id1 in rollback_trx_ids and trx_id1 in blocked_trx_ids:
                                    blocked_trx_ids.remove(trx_id1)
                                    del blocked_stmts[s_idx]

                            # 如果不是cleaned_serial，即是serial的执行
                            if not cleaned:
                                self.rollback_trx_ids = rollback_trx_ids
                            # executed_serial.append((t, "ROLLBACK"))
                            logged_serial.append((t, f"-- Cause Deadlock, Rollback. {sql}"))
                        else:
                            logged_serial.append((t, f"-- Error and skipped: {sql}, {result}"))
                            # 跳过该语句,并终止该事务的pending线程
                            thread.join(timeout=0.001)
                            del self.pending_stmts[key]
                            del self.result_queues[key]
                            self.trx_in_pending.remove(t) if t in self.trx_in_pending else None
                            blocked_trx_ids.remove(t) if t in blocked_trx_ids else None
                            logger.error(f"执行语句失败，非死锁错误: {result}，事务{t}语句: {sql}")

                            if result.errno == 1205:
                                return False, set(), [], []
            
            # 所有语句执行完毕后,根据是否发生死锁返回结果
            if rollback_trx_ids:
                return False, set(rollback_trx_ids), self.executed_serial if not cleaned else self.cleaned_executed_serial, actual_submitted_serial
            else:
                return True, set(), self.executed_serial if not cleaned else self.cleaned_executed_serial, actual_submitted_serial
        
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
            
    def check_atomicity(self, trx_sqls: Dict[int, List[str]], 
                       serial: List[Tuple[int, str]], 
                       ) -> Tuple[bool, str, List[Tuple[int, str]], List[Tuple[int, str]], List[Tuple[int, str]]]:
        """检查事务序列的原子性"""
        try:
            logger.info(f"执行序列: {serial}")
            logger.info(f"trx_sqls: {trx_sqls}")

            try:
                conn = self._create_slave_connection()
                self.snapshot_before = self._take_snapshot(conn)
                conn.close()
                
                # 执行原始序列
                success, rollback_trx_ids, executed_serial, actual_submitted_serial = self._execute_serial(serial, cleaned=False)
            except Exception as e:
                logger.error(f"执行序列失败: {e}")
                return False, str(e), [], [], []
                
            # 如果执行成功且没有回滚的事务，即未发生死锁，直接返回，这个测例无效，如果发生死锁，才需要检查原子性
            if success and len(rollback_trx_ids) == 0:
                logger.info(f"执行序列成功，未发生死锁和手动ROLLBACK，无效test case")
                print(f"执行序列成功，未发生死锁和手动ROLLBACK，无效test case")
                return True, None, serial, [], []

            # 保存并发执行后的数据库状态
            conn = self._create_slave_connection()
            self.snapshot_serial = self._take_snapshot(conn)
            conn.close()

            # 获取成功和失败的事务集合
            rolled_back_trxs = set(rollback_trx_ids) if rollback_trx_ids else set()
            # 从self.num_transactions中减去rolled_back_trxs
            committed_trxs = set(range(1, self.num_transactions + 1)) - rolled_back_trxs
            self.executed_serial = executed_serial
                        
            logger.info(f"被回滚的事务: {sorted(list(rolled_back_trxs))}")
            logger.info(f"成功提交的事务: {sorted(list(committed_trxs))}")
            
            cleaned_serial = []
            tx_rolled_back = set()
            # for trx_id, stmt in serial:
            for trx_id, stmt in self.executed_serial:
            # for trx_id, stmt in actual_submitted_serial:
                if trx_id in rolled_back_trxs:
                    if trx_id not in tx_rolled_back:
                        if stmt.startswith("ROLLBACK"):
                            tx_rolled_back.add(trx_id)
                        else:
                            continue
                    else:
                        cleaned_serial.append((trx_id, stmt))
                else:
                    cleaned_serial.append((trx_id, stmt))
            
            # 检查cleaned_serial是否等价于serial
            try:
                self._restore_initial_state()
                logger.info("恢复初始状态成功")
            except Exception as e:
                logger.error(f"恢复初始状态失败: {e}")
                return False, str(e), [], []

            logger.info(f"清理前的序列: {self.executed_serial}")
            logger.info(f"清理后的序列: {cleaned_serial}")
            cleaned_success, cleaned_rollback_trx_ids, cleaned_executed_serial, cleaned_actual_submitted_serial = self._execute_serial(cleaned_serial, cleaned=True)
            logger.info(f"清理后的序列执行成功: {cleaned_success}")
            
            if not cleaned_success:
                error_msg = "清理后的序列仍存在死锁"
                logger.error(error_msg)
                
            # 获取清理后序列的执行状态
            conn = self._create_slave_connection()
            self.snapshot_cleaned = self._take_snapshot(conn)
            conn.close()
            
            # 比较两次执行的结果
            if not self._compare_snapshots(self.snapshot_serial, self.snapshot_cleaned):
                error_msg = (
                    "违反原子性：\n"
                    f"serial执行状态: {self.snapshot_serial}\n"
                    f"cleaned_serial执行状态: {self.snapshot_cleaned}"
                )
                logger.error(error_msg)
                return False, error_msg, self.executed_serial, cleaned_serial, actual_submitted_serial
            return True, None, self.executed_serial, cleaned_serial, actual_submitted_serial
            
        except Exception as e:
            logger.error(f"检查原子性过程发生错误: {e}")
            return False, str(e), [], [], []

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

    def _restore_initial_state(self):
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
num_of_runs = 1000
logger.info("INFO TEST")
logger.debug("DEBUG TEST")
logger.error("ERROR TEST")
bug_count = 1


date_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
SAVE_DIR = f"{database_save_dir}/{isolation_level}/{date_time}"
for i in range(num_of_runs):
    args = parse_arguments()
    # 添加日志记录
    print(f"iter: {i}") 
    logger.info(f"iter: {i}")
    logger.info("----------SETTINGS-----------")
    logger.info(f"Isolation level: {isolation_level}")
    logger.info("-----------------------------")
    conn = None
    try:
        # 先不指定数据库连接
        dbconfig_no_db = dbconfig.copy()
        dbconfig_no_db.pop("database")
        conn = mysql.connector.connect(**dbconfig_no_db)
        
        # # 创建test数据库
        # cursor = conn.cursor()
        # cursor.execute("CREATE DATABASE IF NOT EXISTS test")
        # cursor.close()
        
        # # 重新连接到test数据库
        # conn.close()
        # conn = mysql.connector.connect(**dbconfig)
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
        
        # num_of_tables = random.randint(1, 3)
        num_of_tables = 1
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
            num_transactions = random.randint(1, 4)
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
        
        # 检查原子性
        is_atomic, info, executed_serial, cleaned_serial, actual_submitted_serial = atomicity_checker.check_atomicity(trx_sqls, serial)
        snapshots = atomicity_checker.get_snapshots()
        print('is_atomic:', is_atomic)
        print('info:', info)
        print('executed_serial:', executed_serial)
        print('cleaned_serial:', cleaned_serial)
        print('snapshots:', snapshots)
        print()

        logger.info(f"is_atomic: {is_atomic}")
        logger.info(f"info: {info}")
        logger.info(f"executed_serial: {executed_serial}")
        logger.info(f"actual_submitted_serial: {actual_submitted_serial}")
        logger.info(f"cleaned_serial: {cleaned_serial}")
        logger.info(f"snapshots: {snapshots}")
        logger.info("")

        if not is_atomic:
            # 创建保存bug case的目录
            bug_case_dir = f"{SAVE_DIR}/bug_case_{bug_count}"
            bug_count += 1
            os.makedirs(bug_case_dir, exist_ok=True)
            
            # 导出数据库状态
            atomicity_checker._restore_initial_state()
            
            dump_cmd = f"mysqldump -h localhost -P 4000 -u root -p123456 test > {bug_case_dir}/initial_state.sql"
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
                f.write(f"Snapshots: {snapshots}\n")
            
            # 记录日志
            logger.error(f"BUG FOUND: {info}")
            logger.info(f"Number of Transactions: {num_transactions}")
            logger.info(f"Transactions: {trx_sqls}")
            logger.info(f"Planned Serial: {serial}")
            logger.info(f"Actually Executed Serial: {executed_serial}")
            logger.info(f"Actual Submitted Serial: {actual_submitted_serial}")
            logger.info(f"Snapshots: {snapshots}")
            logger.info("")

            print(f"BUG FOUND: {info}")
            print(f"Number of Transactions: {num_transactions}")
            print(f"Transactions: {trx_sqls}")
            print(f"Planned Serial: {serial}")
            print(f"Actually Executed Serial: {executed_serial}")
            print(f"Actual Submitted Serial: {actual_submitted_serial}")
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
