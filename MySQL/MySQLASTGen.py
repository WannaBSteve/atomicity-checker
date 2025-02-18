from abc import ABC, abstractmethod
from enum import Enum
from typing import List, Optional, Union, Set, Dict, Tuple, Any
from dataclasses import dataclass
import random
import logging
from MySQLParameter import SQLParamGenerator
import re

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
        if isinstance(self.value, str) and self.value.upper() == 'NULL':
            return "NULL"  # 特殊处理字符串形式的"NULL"
        elif self.value is None:
            return "NULL"
        elif isinstance(self.value, (int, float)):
            return str(self.value)
        elif isinstance(self.value, bool):
            return "TRUE" if self.value else "FALSE"
        else:
            # 处理字符串值
            val = str(self.value).strip()
            val = val.lstrip(" '\"")
            val = val.rstrip(" '\"")
            val = val.replace("'", "''")
            return f"'{val}'"

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

@dataclass
class MySQLDelete(MySQLExpression):
    """DELETE语句的AST节点"""
    table: str
    where: Optional[MySQLExpression] = None
    table_alias: Optional[str] = None

    def get_sql(self) -> str:
        # 构建基本的DELETE语句
        if self.table_alias:
            sql = f"DELETE FROM {self.table} AS {self.table_alias}"
        else:
            sql = f"DELETE FROM {self.table}"
        
        # 添加WHERE子句
        if self.where:
            where_sql = self.where.get_sql()
            sql += f" WHERE {where_sql}"
        
        return sql

@dataclass
class MySQLInPredicate(MySQLExpression):
    """IN谓词"""
    column: str
    values: List[Union[int, str, float, None]]
    
    def get_sql(self) -> str:
        if not self.values:  # 空列表
            return "FALSE"  # 空 IN 返回 FALSE
        values_sql = ", ".join(MySQLConstant(v).get_sql() for v in self.values)
        return f"{self.column} IN ({values_sql})"

@dataclass
class MySQLExistsPredicate(MySQLExpression):
    """EXISTS谓词"""
    subquery: MySQLExpression
    
    def get_sql(self) -> str:
        subquery_sql = self.subquery.get_sql()
        # 确保子查询的括号是完整的
        if '(' in subquery_sql:
            left_count = subquery_sql.count('(')
            right_count = subquery_sql.count(')')
            if left_count > right_count:
                subquery_sql += ')' * (left_count - right_count)
        
        # 简化复杂的条件
        if "id = id" in subquery_sql:
            # 将 WHERE ((id = id) AND id IN (7)) 简化为 WHERE id IN (7)
            subquery_sql = re.sub(r'\(\(id = id\) AND (.*?)\)', r'\1)', subquery_sql)
        
        return f"EXISTS ({subquery_sql})"

@dataclass
class MySQLCase(MySQLExpression):
    """CASE表达式"""
    condition: MySQLExpression
    true_value: MySQLExpression
    false_value: MySQLExpression

    def get_sql(self) -> str:
        # 简化 CASE WHEN 表达式
        condition_sql = self.condition.get_sql()
        # 如果条件是简单的 IN 判断，直接返回条件
        if 'IN' in condition_sql:
            return condition_sql
        return f"CASE WHEN {condition_sql} THEN {self.true_value.get_sql()} ELSE {self.false_value.get_sql()} END"

@dataclass
class MySQLSubquery(MySQLExpression):
    """子查询"""
    select_list: List[MySQLExpression]
    from_table: str
    where: Optional[MySQLExpression] = None
    table_alias: Optional[str] = None
    
    def get_sql(self) -> str:
        select_items = ", ".join([expr.get_sql() for expr in self.select_list])
        if self.table_alias:
            from_clause = f"{self.from_table} AS {self.table_alias}"
        else:
            from_clause = self.from_table
            
        sql = f"SELECT {select_items} FROM {from_clause}"
        if self.where:
            where_sql = self.where.get_sql()
            # 确保WHERE子句的括号是完整的
            if '(' in where_sql:
                left_count = where_sql.count('(')
                right_count = where_sql.count(')')
                if left_count > right_count:
                    where_sql += ')' * (left_count - right_count)
            sql += f" WHERE {where_sql}"
        return sql

@dataclass
class MySQLUpdate(MySQLExpression):
    """UPDATE语句的AST节点"""
    table: str
    set_items: List[Tuple[str, Any]]  # [(column, value), ...]
    where: Optional[MySQLExpression] = None
    table_alias: Optional[str] = None

    def get_sql(self) -> str:
        # 构建基本的UPDATE语句
        if self.table_alias:
            sql = f"UPDATE {self.table} AS {self.table_alias}"
        else:
            sql = f"UPDATE {self.table}"
        
        # 构建SET子句
        set_exprs = []
        for col, val in self.set_items:
            set_exprs.append(f"{col} = {val}")
        sql += f" SET {', '.join(set_exprs)}"
        
        # 添加WHERE子句
        if self.where:
            where_sql = self.where.get_sql()
            if '(' in where_sql and not where_sql.endswith(')'):
                where_sql += ')'
            sql += f" WHERE {where_sql}"
        
        return sql

@dataclass
class MySQLInsert(MySQLExpression):
    """INSERT语句的AST节点"""
    table: str
    columns: List[str]
    values_str: str

    def get_sql(self) -> str:
        # 构建基本的INSERT语句
        sql = f"INSERT INTO {self.table}"
        
        # 添加列名
        if self.columns:
            sql += f" ({', '.join(self.columns)})"

        sql += f" VALUES ({self.values_str})"
        return sql


class MySQLPredicateGenerator:
    """MySQL谓词生成器"""
    def __init__(self, table: str, target_rows: List[tuple], column_names: List[str]):
        self.table = table
        self.target_rows = target_rows
        self.column_names = column_names
        self.id_index = column_names.index("id")
        self.target_ids = [row[self.id_index] for row in self.target_rows]

    def generate_predicate(self, depth: int = 2, for_delete: bool = False, for_update: bool = False) -> MySQLExpression:
        """生成匹配目标行的谓词"""
        if for_delete or for_update:
            # DELETE 和 UPDATE 语句使用简单的条件，避免相关子查询
            if len(self.target_ids) > 1:
                return self._gen_in_list()
            elif len(self.target_ids) == 1:
                return MySQLBinaryOperation(
                    MySQLColumnReference("id", "t1"),
                    MySQLConstant(self.target_ids[0]),
                    MySQLBinaryOperator.EQUALS
                )
            else:
                return MySQLConstant(False)
        
        # 原有的谓词生成逻辑
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


@dataclass
class MySQLASTGen:
    """MySQL AST生成器"""
    def __init__(self, table: str, target_rows: List[tuple], column_names: List[str], 
                 column_types: List[str], primary_keys: List[str], lock_type: str):
        self.table = table
        self.target_rows = target_rows
        self.column_names = column_names
        self.column_types = column_types
        self.primary_keys = primary_keys
        self.lock_type = lock_type
        self.param_generator = SQLParamGenerator(
            table, column_names, column_types, primary_keys, target_rows, lock_type
        )

    def generate_lock_sql(self, stmt_type: str = "SELECT") -> str:
        """生成带锁的SQL语句"""
        try:
            if stmt_type == "INSERT":
                return self._generate_insert()
            elif stmt_type == "UPDATE":
                return self._generate_update()
            elif stmt_type == "DELETE":
                return self._generate_delete()
            else:  # SELECT
                return self._generate_select()
        except Exception as e:
            logger.error(f"生成锁定SQL失败: {e}")
            raise

    def _generate_insert(self) -> str:
        """生成INSERT语句"""
        # 从param_generator获取列和值
        columns = self.param_generator._generate_insert_columns().split(", ")
        values_str = self.param_generator._generate_insert_values(self.lock_type)
        
        return MySQLInsert(
            table=self.table,
            columns=columns,
            values_str=values_str
        ).get_sql()

    def _generate_update(self) -> str:
        """生成UPDATE语句"""
        # 从param_generator获取SET表达式
        set_expr = self.param_generator._generate_set_expression()
        set_items = []
        for item in set_expr.split(", "):
            col, val = item.split(" = ")
            set_items.append((col, val))

        # 使用谓词生成器生成WHERE子句
        predicate_gen = MySQLPredicateGenerator(self.table, self.target_rows, self.column_names)
        
        return MySQLUpdate(
            table=self.table,
            table_alias="t1",  # 可选使用别名
            set_items=set_items,
            where=predicate_gen.generate_predicate(for_update=True)
        ).get_sql()

    def _generate_delete(self) -> str:
        """生成DELETE语句"""
        # 使用谓词生成器生成WHERE子句
        predicate_gen = MySQLPredicateGenerator(self.table, self.target_rows, self.column_names)
        
        return MySQLDelete(
            table=self.table,
            table_alias="t1",  # 可选使用别名
            where=predicate_gen.generate_predicate(for_delete=True)
        ).get_sql()

    def _generate_select(self) -> str:
        """生成SELECT语句"""
        predicate_gen = MySQLPredicateGenerator(self.table, self.target_rows, self.column_names)
        lock_clause = "FOR UPDATE" if self.lock_type == "X" else "LOCK IN SHARE MODE"
        
        return MySQLSelect(
            select_list=[MySQLColumnReference("*", "t1")],
            from_table=self.table,
            table_alias="t1",
            where=predicate_gen.generate_predicate(),
            lock_clause=lock_clause
        ).get_sql()

