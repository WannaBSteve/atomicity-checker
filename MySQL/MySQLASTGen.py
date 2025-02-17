from abc import ABC, abstractmethod
from enum import Enum
from typing import List, Optional, Union, Set, Dict
from dataclasses import dataclass
import random
import logging

logger = logging.getLogger('atomicity-checker')

class MySQLBinaryOperator(Enum):
    """MySQL二元操作符"""
    EQUALS = "="
    NOT_EQUALS = "<>"
    GREATER = ">"
    LESS = "<"
    GREATER_EQUALS = ">="
    LESS_EQUALS = "<="
    AND = "AND"
    OR = "OR"
    LIKE = "LIKE"
    NOT_LIKE = "NOT LIKE"

class MySQLUnaryOperator(Enum):
    """MySQL一元操作符"""
    NOT = "NOT"
    IS_NULL = "IS NULL"
    IS_NOT_NULL = "IS NOT NULL"

class MySQLFunction(Enum):
    """MySQL函数"""
    COUNT = "COUNT"
    SUM = "SUM"
    MAX = "MAX"
    MIN = "MIN"
    AVG = "AVG"
    COALESCE = "COALESCE"
    NULLIF = "NULLIF"
    IFNULL = "IFNULL"

class MySQLExpression(ABC):
    """MySQL表达式基类"""
    @abstractmethod
    def get_sql(self) -> str:
        """获取SQL字符串表示"""
        pass

@dataclass
class MySQLConstant(MySQLExpression):
    """常量表达式"""
    value: Union[int, str, float, bool, None]

    def get_sql(self) -> str:
        if self.value is None:
            return "NULL"
        if isinstance(self.value, bool):
            return "TRUE" if self.value else "FALSE"
        if isinstance(self.value, (int, float)):
            return str(self.value)
        return f"'{self.value}'"

@dataclass
class MySQLColumnReference(MySQLExpression):
    """列引用"""
    column: str
    table: Optional[str] = None

    def get_sql(self) -> str:
        if self.table:
            return f"{self.table}.{self.column}"
        return self.column

@dataclass
class MySQLBinaryOperation(MySQLExpression):
    """二元操作"""
    left: MySQLExpression
    right: MySQLExpression
    operator: MySQLBinaryOperator

    def get_sql(self) -> str:
        return f"({self.left.get_sql()} {self.operator.value} {self.right.get_sql()})"

@dataclass
class MySQLUnaryOperation(MySQLExpression):
    """一元操作"""
    expr: MySQLExpression
    operator: MySQLUnaryOperator

    def get_sql(self) -> str:
        if self.operator in [MySQLUnaryOperator.IS_NULL, MySQLUnaryOperator.IS_NOT_NULL]:
            return f"({self.expr.get_sql()} {self.operator.value})"
        return f"({self.operator.value} {self.expr.get_sql()})"

@dataclass
class MySQLInOperation(MySQLExpression):
    """IN操作"""
    left: MySQLExpression
    right: Union[List[MySQLExpression], 'MySQLSelect']
    is_negative: bool = False

    def get_sql(self) -> str:
        if isinstance(self.right, list):
            values = ", ".join(expr.get_sql() for expr in self.right)
            right_sql = f"({values})"
        else:
            right_sql = f"({self.right.get_sql()})"
        op = "NOT IN" if self.is_negative else "IN"
        return f"{self.left.get_sql()} {op} {right_sql}"

@dataclass
class MySQLBetweenOperation(MySQLExpression):
    """BETWEEN操作"""
    expr: MySQLExpression
    left: MySQLExpression
    right: MySQLExpression
    is_negative: bool = False

    def get_sql(self) -> str:
        op = "NOT BETWEEN" if self.is_negative else "BETWEEN"
        return f"{self.expr.get_sql()} {op} {self.left.get_sql()} AND {self.right.get_sql()}"

@dataclass
class MySQLExists(MySQLExpression):
    """EXISTS子查询"""
    subquery: 'MySQLSelect'
    is_negative: bool = False

    def get_sql(self) -> str:
        op = "NOT EXISTS" if self.is_negative else "EXISTS"
        return f"{op} ({self.subquery.get_sql()})"

@dataclass
class MySQLFunctionCall(MySQLExpression):
    """函数调用"""
    function: MySQLFunction
    arguments: List[MySQLExpression]

    def get_sql(self) -> str:
        args = ", ".join(arg.get_sql() for arg in self.arguments)
        return f"{self.function.value}({args})"

@dataclass
class MySQLCaseOperation(MySQLExpression):
    """CASE表达式"""
    conditions: List[MySQLExpression]
    results: List[MySQLExpression]
    else_result: Optional[MySQLExpression] = None

    def get_sql(self) -> str:
        case_str = "CASE"
        for cond, result in zip(self.conditions, self.results):
            case_str += f" WHEN {cond.get_sql()} THEN {result.get_sql()}"
        if self.else_result:
            case_str += f" ELSE {self.else_result.get_sql()}"
        case_str += " END"
        return case_str

@dataclass
class MySQLSelect:
    """SELECT语句"""
    select_list: List[MySQLExpression]
    from_table: str
    where: Optional[MySQLExpression] = None
    table_alias: Optional[str] = None
    group_by: Optional[List[MySQLExpression]] = None
    having: Optional[MySQLExpression] = None
    order_by: Optional[List[MySQLExpression]] = None
    limit: Optional[int] = None
    lock_clause: Optional[str] = None  # 'FOR UPDATE' 或 'LOCK IN SHARE MODE'

    def get_sql(self) -> str:
        select_items = ", ".join(expr.get_sql() for expr in self.select_list)
        sql = f"SELECT {select_items} FROM {self.from_table}"
        if self.table_alias:
            sql += f" AS {self.table_alias}"
        if self.where:
            sql += f" WHERE {self.where.get_sql()}"
        if self.group_by:
            group_items = ", ".join(expr.get_sql() for expr in self.group_by)
            sql += f" GROUP BY {group_items}"
        if self.having:
            sql += f" HAVING {self.having.get_sql()}"
        if self.order_by:
            order_items = ", ".join(expr.get_sql() for expr in self.order_by)
            sql += f" ORDER BY {order_items}"
        if self.limit is not None:
            sql += f" LIMIT {self.limit}"
        if self.lock_clause:
            sql += f" {self.lock_clause}"
        return sql

class MySQLPredicateGenerator:
    """MySQL谓词生成器"""
    def __init__(self, table: str, target_rows: List[tuple], column_names: List[str]):
        self.table = table
        self.target_rows = target_rows
        self.column_names = column_names
        self.id_index = column_names.index("id")
        self.target_ids = [row[self.id_index] for row in self.target_rows]

    def generate_predicate(self, depth: int = 2) -> MySQLExpression:
        """生成随机谓词"""
        if depth <= 0:
            return self._gen_simple_comparison()

        generators = [
            self._gen_simple_comparison,
            self._gen_between,
            self._gen_in_list,
            lambda: self._gen_exists(depth - 1),
            lambda: self._gen_and_or_chain(depth - 1),
            self._gen_case,
            lambda: self._gen_correlated_subquery(depth - 1)
        ]
        return random.choice(generators)()

    def _gen_simple_comparison(self) -> MySQLExpression:
        """生成简单比较"""
        # id_val = random.choice(self.target_ids)
        # 对每一个target_ids，生成一个等价的谓词，and连接
        conditions = []
        for id_val in self.target_ids:
            conditions.append(
                MySQLBinaryOperation(
                    MySQLColumnReference("id", "t1"),
                    MySQLConstant(id_val),
                    MySQLBinaryOperator.EQUALS
                )
            )
        # 将conditions用MySQLBinaryOperator.AND连接
        result = conditions[0]
        if len(conditions) > 1:
            for cond in conditions[1:]:
                result = MySQLBinaryOperation(
                    result, 
                    cond, 
                    MySQLBinaryOperator.AND
                )
        return result

        # return MySQLBinaryOperation(
        #     MySQLColumnReference("id", "t1"),
        #     MySQLConstant(id_val),
        #     MySQLBinaryOperator.EQUALS
        # )

    def _gen_between(self) -> MySQLExpression:
        """生成BETWEEN表达式"""
        ids = sorted(self.target_ids)
        return MySQLBetweenOperation(
            MySQLColumnReference("id", "t1"),
            MySQLConstant(ids[0]),
            MySQLConstant(ids[-1])
        )

    def _gen_in_list(self) -> MySQLExpression:
        """生成IN列表"""
        return MySQLInOperation(
            MySQLColumnReference("id", "t1"),
            [MySQLConstant(id_val) for id_val in self.target_ids]
        )

    def _gen_exists(self, depth: int) -> MySQLExpression:
        """生成EXISTS子查询"""
        subquery = MySQLSelect(
            select_list=[MySQLConstant(1)],
            from_table=self.table,
            table_alias="t2",
            where=self.generate_predicate(depth)
        )
        return MySQLExists(subquery)

    def _gen_and_or_chain(self, depth: int) -> MySQLExpression:
        """生成AND/OR链"""
        num_conditions = random.randint(2, 3)
        conditions = [self.generate_predicate(depth) for _ in range(num_conditions)]
        op = random.choice([MySQLBinaryOperator.AND, MySQLBinaryOperator.OR])
        
        result = conditions[0]
        for cond in conditions[1:]:
            result = MySQLBinaryOperation(result, cond, op)
        return result

    def _gen_case(self) -> MySQLExpression:
        """生成CASE表达式"""
        return MySQLCaseOperation(
            conditions=[self._gen_in_list()],
            results=[MySQLConstant(True)],
            else_result=MySQLConstant(False)
        )

    def _gen_correlated_subquery(self, depth: int) -> MySQLExpression:
        """生成相关子查询"""
        correlation_condition = MySQLBinaryOperation(
            MySQLColumnReference("id", "t2"),
            MySQLColumnReference("id", "t1"),
            MySQLBinaryOperator.EQUALS
        )
        
        subquery_condition = MySQLBinaryOperation(
            correlation_condition,
            self._gen_in_list(),
            MySQLBinaryOperator.AND
        )
        
        subquery = MySQLSelect(
            select_list=[MySQLConstant(1)],
            from_table=self.table,
            table_alias="t2",
            where=subquery_condition
        )
        
        return MySQLExists(subquery)

def generate_lock_sql(table: str, target_rows: List[tuple], column_names: List[str], 
                     lock_type: str) -> str:
    """生成带锁的SQL语句"""
    try:
        generator = MySQLPredicateGenerator(table, target_rows, column_names)
        predicate = generator.generate_predicate()
        
        if lock_type == "X":
            lock_clause = "FOR UPDATE" 
        elif lock_type == "S":
            lock_clause = "LOCK IN SHARE MODE"
        else: 
            lock_clause = None

        # 生成SQL语句
        select = MySQLSelect(
            select_list=[MySQLColumnReference("*", "t1")],
            from_table=table,
            table_alias="t1",
            where=predicate,
            lock_clause=lock_clause
        )
        
        return select.get_sql()
        
    except Exception as e:
        logger.error(f"生成锁定SQL失败: {e}")
        raise

