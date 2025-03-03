from dataclasses import dataclass, field
from typing import List, Optional
from enum import Enum
import mysql.connector

class IsolationLevel(Enum):
    RU = "READ UNCOMMITTED"
    RC = "READ COMMITTED"
    RR = "REPEATABLE READ"
    SER = "SERIALIZABLE"

@dataclass
class StatementCell:
    tx_id: int
    statement: str
    stmt_id: int
    result: Optional[List] = None
    blocked: bool = False
    aborted: bool = False
    
    def copy(self):
        return StatementCell(
            self.tx_id,
            self.statement,
            self.stmt_id,
            self.result,
            self.blocked,
            self.aborted
        )

class Transaction:
    def __init__(self, tx_id: int, isolation_level: str = None, conn: mysql.connector.MySQLConnection = None):
        self.tx_id = tx_id
        self.conn = conn
        self.isolation_level = IsolationLevel[isolation_level] if isolation_level else None
        self.statements: List[StatementCell] = []
        
        # 分析状态
        self.snap_txs: List[Transaction] = []
        self.snap_view = None
        self.blocked: bool = False
        self.committed: bool = False
        self.finished: bool = False
        self.aborted: bool = False
        self.locks: List = []
        self.blocked_statements: List[StatementCell] = []
        
    def clear_states(self):
        """重置事务状态"""
        self.snap_txs = []
        self.snap_view = None
        self.blocked = False
        self.committed = False
        self.finished = False
        self.aborted = False
        self.locks = []
        self.blocked_statements = []
        
    def __str__(self):
        stmt_str = "\n\t".join(stmt.statement + ";" for stmt in self.statements) if self.statements else ""
        return f"Transaction{{{self.tx_id}, {self.isolation_level}}}, with statements:\n\t{stmt_str}" 