from enum import Enum
from typing import Dict, List, Optional, Tuple
import random
import logging

logger = logging.getLogger('atomicity-checker')

class MySQLLockType(Enum):
    """MySQL锁类型"""
    SHARED = "S"          # 共享锁
    EXCLUSIVE = "X"       # 排他锁
    GAP = "GAP"          # 间隙锁
    NEXT_KEY = "NK"      # 临键锁
    INSERT_INTENT = "II"  # 插入意向锁
    TABLE_READ = "TR"     # 表读锁
    TABLE_WRITE = "TW"    # 表写锁
    INTENT_SHARED = "IS"  # 意向共享锁
    INTENT_EXCL = "IX"    # 意向排他锁
    AUTO_INC = "AI"      # 自增锁
    MDL_SHARED = "MDL_S" # 元数据共享锁
    MDL_EXCL = "MDL_X"   # 元数据排他锁
    GLOBAL = "GL"        # 全局锁

class MySQLIsolationLevel(Enum):
    """MySQL隔离级别"""
    READ_UNCOMMITTED = "RU"
    READ_COMMITTED = "RC"
    REPEATABLE_READ = "RR"
    SERIALIZABLE = "SER"

class MySQLTemplateGenerator:
    """MySQL模板生成器"""
    def __init__(self, table_name: str):
        self.table_name = table_name
        self._init_templates()

    def _init_templates(self):
        """初始化SQL模板"""
        self.templates = {
            # 行级锁模板
            MySQLLockType.SHARED: {
                "basic": "SELECT {cols} FROM {table} WHERE {cond} LOCK IN SHARE MODE",
                "index": "SELECT {cols} FROM {table} FORCE INDEX({idx}) WHERE {cond} LOCK IN SHARE MODE",
                "range": "SELECT {cols} FROM {table} WHERE id BETWEEN {v1} AND {v2} LOCK IN SHARE MODE"
            },
            MySQLLockType.EXCLUSIVE: {
                "basic": "SELECT {cols} FROM {table} WHERE {cond} FOR UPDATE",
                "update": "UPDATE {table} SET {set_expr} WHERE {cond}",
                "delete": "DELETE FROM {table} WHERE {cond}",
                "range": "SELECT {cols} FROM {table} WHERE id BETWEEN {v1} AND {v2} FOR UPDATE"
            },
            # ... 其他锁类型的模板 ...
        }

        # 隔离级别支持的锁类型
        self.iso_lock_support = {
            MySQLIsolationLevel.READ_UNCOMMITTED: {
                "row": [MySQLLockType.EXCLUSIVE],
                "table": [MySQLLockType.TABLE_READ, MySQLLockType.TABLE_WRITE],
                "meta": [MySQLLockType.MDL_SHARED, MySQLLockType.MDL_EXCL],
                "global": [MySQLLockType.GLOBAL]
            },
            # ... 其他隔离级别支持的锁类型 ...
        }

    def generate_lock_sql(self, 
                         lock_type: MySQLLockType,
                         target_rows: List[tuple],
                         column_names: List[str],
                         is_continuous: bool = False) -> str:
        """生成带锁的SQL语句"""
        try:
            template = self._get_template(lock_type)
            condition = self._generate_condition(target_rows, column_names, is_continuous)
            
            return template.format(
                table=self.table_name,
                cols="*",
                cond=condition
            )
            
        except Exception as e:
            logger.error(f"生成锁定SQL失败: {e}")
            raise

    def _get_template(self, lock_type: MySQLLockType) -> str:
        """获取对应锁类型的模板"""
        if lock_type not in self.templates:
            raise ValueError(f"不支持的锁类型: {lock_type}")
        return self.templates[lock_type]["basic"]

    def _generate_condition(self, 
                          target_rows: List[tuple],
                          column_names: List[str],
                          is_continuous: bool) -> str:
        """生成WHERE条件"""
        id_index = column_names.index("id")
        ids = [row[id_index] for row in target_rows]
        
        if is_continuous and len(ids) > 1:
            return f"id BETWEEN {min(ids)} AND {max(ids)}"
        else:
            return f"id IN ({','.join(map(str, ids))})"