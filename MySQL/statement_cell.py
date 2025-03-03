from enum import Enum
import re
import logging
from typing import Dict, List, Optional, Any
import mysql.connector
from mysql.connector.cursor import MySQLCursor
from Transaction import Transaction

logger = logging.getLogger(__name__)

class StatementType(Enum):
    UNKNOWN = "UNKNOWN"
    SELECT = "SELECT"
    SELECT_SHARE = "SELECT_SHARE"
    SELECT_UPDATE = "SELECT_UPDATE"
    UPDATE = "UPDATE"
    DELETE = "DELETE"
    INSERT = "INSERT"
    SET = "SET"
    BEGIN = "BEGIN"
    COMMIT = "COMMIT"
    ROLLBACK = "ROLLBACK"

class StatementCell:
    def __init__(self, tx: Transaction, stmt_id: int, statement: str = None):
        self.tx = tx
        self.stmt_id = stmt_id
        self.statement = statement.replace(";", "") if statement else ""
        self.type = StatementType.UNKNOWN
        if statement:
            try:
                self.type = StatementType[statement.split()[0].upper()]
            except KeyError:
                logger.error(f"Unknown statement type: {statement}")
        
        self.where_prefix = ""
        self.where_clause = ""
        self.for_postfix = ""
        self.values: Dict[str, str] = {}
        self.blocked = False
        self.aborted = False
        self.view = None
        self.result: Optional[List[Any]] = None
        self.new_row_id: Optional[int] = None
        
        if statement:
            self._parse_statement()

    def _parse_statement(self):
        """解析SQL语句"""
        try:
            stmt = self.statement
            real_type = self.type
            
            if self.type in [StatementType.BEGIN, StatementType.COMMIT, StatementType.ROLLBACK]:
                return
                
            if self.type == StatementType.SELECT:
                # 处理FOR子句
                for_idx = stmt.upper().find("FOR ")
                if for_idx == -1:
                    for_idx = stmt.upper().find("LOCK IN SHARE MODE")
                    
                if for_idx != -1:
                    postfix = stmt[for_idx:].strip()
                    stmt = stmt[:for_idx].strip()
                    self.for_postfix = f" {postfix}"
                    
                    if postfix == "FOR UPDATE":
                        real_type = StatementType.SELECT_UPDATE
                    elif postfix in ["FOR SHARE", "LOCK IN SHARE MODE"]:
                        real_type = StatementType.SELECT_SHARE
                    else:
                        raise ValueError(f"Invalid postfix: {self.statement}")
                        
            if self.type == StatementType.UPDATE:
                # 处理SET子句
                set_idx = stmt.find(" SET ")
                if set_idx != -1:
                    where_idx = stmt.find(" WHERE ")
                    if where_idx == -1:
                        set_pairs_str = stmt[set_idx + 5:]
                    else:
                        set_pairs_str = stmt[set_idx + 5:where_idx]
                        
                    set_pairs_str = set_pairs_str.replace(" ", "")
                    for pair in set_pairs_str.split(","):
                        col, val = pair.split("=")
                        if val.startswith('"') and val.endswith('"'):
                            val = val[1:-1]
                        self.values[col] = val
                        
            if self.type in [StatementType.SELECT, StatementType.UPDATE, StatementType.DELETE]:
                # 处理WHERE子句
                where_idx = stmt.find("WHERE")
                if where_idx == -1:
                    self.where_prefix = stmt
                    self.where_clause = "TRUE"
                else:
                    self.where_prefix = stmt[:where_idx - 1]
                    self.where_clause = stmt[where_idx + 6:]
                    
                self.type = real_type
                self._recompute_statement()
                
            elif self.type == StatementType.INSERT:
                # 解析INSERT语句
                pattern = re.compile(
                    r"INTO\s+(\w+)\s*\((.*?)\)\s*VALUES\s*\((.*?)\)"
                )
                match = pattern.search(self.statement)
                if not match:
                    raise ValueError(f"Parse INSERT statement failed: {self.statement}")
                    
                cols = [c.strip() for c in match.group(2).split(",")]
                vals = [v.strip() for v in match.group(3).split(",")]
                
                if len(cols) != len(vals):
                    raise ValueError(f"Parse insert statement failed: {self.statement}")
                    
                for col, val in zip(cols, vals):
                    if val.startswith('"') and val.endswith('"'):
                        val = val[1:-1]
                    self.values[col] = val
                    
        except Exception as e:
            logger.error(f"Parse statement failed: {self.statement}")
            logger.error(str(e))
            raise

    def make_choose_row(self, row_id: int, cursor: MySQLCursor):
        """修改WHERE子句以选择特定行"""
        try:
            # 检查行是否匹配当前条件
            query = f"SELECT * FROM table_0 WHERE ({self.where_clause}) AND id = {row_id}"
            cursor.execute(query)
            if cursor.fetchone():
                return
                
            # 检查条件结果
            query = f"SELECT ({self.where_clause}) FROM table_0 WHERE id = {row_id}"
            cursor.execute(query)
            result = cursor.fetchone()
            
            if not result:
                logger.error(f"Choose row failed, row_id:{row_id}, statement:{self.statement}")
                return
                
            if result[0] is None:
                self.where_clause = f"({self.where_clause}) IS NULL"
            else:
                self.where_clause = f"NOT ({self.where_clause})"
                
            self._recompute_statement()
            
        except mysql.connector.Error as e:
            logger.error(f"Execute query failed: {query}")
            raise RuntimeError("Execution failed") from e

    def negate_condition(self, cursor: MySQLCursor):
        """取反WHERE条件"""
        query = f"SELECT ({self.where_clause}) as yes from table_0 limit 1"
        try:
            cursor.execute(query)
            result = cursor.fetchone()
            if result:
                res = result[0]
                if res is None:
                    self.where_clause = f"({self.where_clause}) IS NULL"
                elif res == 0:
                    self.where_clause = f"NOT ({self.where_clause})"
        except mysql.connector.Error as e:
            logger.error(f"Negate condition failed: {e}")

    def _recompute_statement(self):
        """重新计算完整SQL语句"""
        self.statement = f"{self.where_prefix} WHERE {self.where_clause}{self.for_postfix}"

    def __str__(self):
        res = f"{self.tx.tx_id}-{self.stmt_id}"
        if self.blocked:
            res += "(B)"
        if self.aborted:
            res += "(A)"
        return res

    def __eq__(self, other):
        if not isinstance(other, StatementCell):
            return False
        return (self.tx.tx_id == other.tx.tx_id and 
                self.stmt_id == other.stmt_id)

    def copy(self):
        """创建语句单元的副本"""
        copy = StatementCell(self.tx, self.stmt_id)
        copy.statement = self.statement
        copy.type = self.type
        copy.where_prefix = self.where_prefix
        copy.where_clause = self.where_clause
        copy.for_postfix = self.for_postfix
        copy.values = self.values.copy()
        copy.blocked = False
        copy.result = None
        return copy 