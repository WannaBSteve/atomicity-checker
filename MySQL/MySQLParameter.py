import random
import logging
from typing import List, Tuple, Dict, Any, Optional, Set

class SQLParamGenerator:
    """SQL参数生成器"""
    def __init__(self, table_name: str, column_names: List[str], 
                column_types: List[str], primary_keys: List[str], 
                rows: List[tuple], lock_type: str, indexes: List[str] = None):  # 添加indexes参数
        self.table_name = table_name
        self.column_names = column_names
        self.column_types = column_types
        self.primary_keys = primary_keys
        self.rows = rows
        self.lock_type = lock_type
        self.indexes = indexes or []  # 如果没有传入索引，使用空列表
        
        # 缓存随机选择的列及其类型
        self._current_random_column = None
        self._current_column_type = None
        self.logger = logging.getLogger('atomicity-checker')

        # 存储列名和类型的映射关系
        self.column_type_map = dict(zip(column_names, column_types))
        
        self.param_generators = {
            "table": lambda: self.table_name,
            "select_cols": lambda: self._generate_select_columns(),
            "cond": lambda: self._generate_condition(),
            "insert_cols": lambda: self._generate_insert_columns(),
            "gap_lock_cond": lambda: self._generate_gap_lock_condition(),
            "col": lambda: self._get_random_column(),  # 返回列名
            "idx": lambda: self._generate_index_name(),  # 修改为使用传入的索引列表
            "set_expr": lambda: self._generate_set_expression(),
            "v1": lambda: self._format_value(self.rows[0][self.column_names.index(self._get_default_column())]),
            "v2": lambda: self._format_value(self.rows[-1][self.column_names.index(self._get_default_column())]),
            "vals": lambda: self._generate_values(),
            "val": lambda: self._generate_random_value(),
            "insert_vals": lambda: self._generate_insert_values(self.lock_type),  # 新增
            "update_expr": lambda: self._generate_update_expression()  # 新增
        }

    def _get_random_column(self) -> str:
        """随机选择一列，但避开主键列，并缓存列类型"""
        if self._current_random_column is None:
            available_columns = [col for col in self.column_names if col not in self.primary_keys]
            if not available_columns:
                available_columns = self.column_names
            
            self._current_random_column = random.choice(available_columns)
            col_idx = self.column_names.index(self._current_random_column)
            self._current_column_type = self.column_types[col_idx]
            
        return self._current_random_column

    def _generate_random_value(self) -> str:
        """为当前随机选择的列生成与其类型匹配的随机值"""
        if self._current_random_column is None:
            self._get_random_column()
            
        # 生成与列类型匹配的随机值
        value = self._generate_value_by_type(self._current_column_type)
        
        # 重置缓存，为下一次SQL准备
        self._current_random_column = None
        self._current_column_type = None
        
        return value

    def generate_params(self, needed_params: Set[str]) -> Dict[str, Any]:
        """统一的参数生成方法"""
        self._current_random_column = None
        self._current_column_type = None
        return {
            param: self.param_generators[param]()
            for param in needed_params
            if param in self.param_generators
        }
    
    def _get_default_column(self) -> str:
        """获取默认的列名，优先使用主键，其次使用第一列"""
        if self.primary_keys:
            return self.primary_keys[0]
        return self.column_names[0]  # 如果没有主键，使用第一列

    def _generate_select_columns(self) -> str:
        """随机生成查询列,有几种可能:
        1. 使用 * 查询所有列
        2. 随机选择部分列
        3. 确保包含主键的随机列
        """
        # 20%的概率返回 *
        if random.random() < 0.2:
            return "*"
            
        # 决定是否必须包含主键 (60%的概率包含主键)
        must_include_pk = random.random() < 0.6
        
        # 随机选择列数 (至少选择1列)
        num_cols = random.randint(1, len(self.column_names))
        
        if must_include_pk and self.primary_keys:
            # 确保包含主键
            selected_cols = set(self.primary_keys)
            # 从剩余列中随机选择
            remaining_cols = set(self.column_names) - selected_cols
            num_additional = min(num_cols - len(selected_cols), len(remaining_cols))
            if num_additional > 0:
                selected_cols.update(random.sample(list(remaining_cols), num_additional))
        else:
            # 完全随机选择列
            selected_cols = set(random.sample(self.column_names, num_cols))
            
        # 将选中的列转换为列表并排序,保持列的顺序与原表一致
        selected_cols = sorted(list(selected_cols), 
                             key=lambda x: self.column_names.index(x))
            
        return ", ".join(selected_cols)

    def _generate_insert_columns(self) -> str:
        """生成INSERT语句使用的列"""
        return ", ".join(self.column_names)

    def _generate_value_by_type(self, col_type: str) -> str:
        """根据列的类型生成随机值"""
        try:
            if "int" in col_type.lower():
                if "auto_increment" in col_type.lower():
                    return "NULL"
                return str(random.randint(1, 100))
            elif "varchar" in col_type.lower() or "text" in col_type.lower():
                return f"'{self._generate_random_string()}'"
            elif "float" in col_type.lower() or "double" in col_type.lower():
                return str(round(random.uniform(1, 100), 2))
            else:
                raise ValueError(f"不支持的列类型: {col_type}")
            
        except Exception as e:
            self.logger.error(f"生成随机值失败: {e}")
            self.logger.error("")
            raise

    def _generate_random_string(self, length: int = 5) -> str:
        """生成随机字符串"""
        letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
        return "".join(random.choice(letters) for _ in range(length))

    def _format_value(self, value: Any) -> str:
        """格式化值，如果是字符串类型则添加引号"""
        return f"'{value}'" if isinstance(value, str) else str(value)
    
    def _format_condition(self, column: str, value: Any) -> str:
        """格式化条件表达式"""
        if value is None:
            return f"{column} IS NULL"
        elif isinstance(value, (int, float)):
            return f"{column} = {value}"
        else:
            return f"{column} = '{value}'"
        
    def _generate_condition(self) -> str:
        """生成WHERE条件，根据行数选择最合适的条件生成方式"""
        try:
            if not self.rows:
                return ""
                
            use_id = random.choice([True, False])
            conditions = []
            if use_id:
                # 使用ID条件
                row_ids = [row[0] for row in self.rows]  # 假设id是第一列
                
                if len(self.rows) == 1:
                    # 单行使用等号
                    conditions.append(self._format_condition('id', row_ids[0]))
                else:
                    # 多行时判断是否连续
                    min_id, max_id = min(row_ids), max(row_ids)
                    if max_id - min_id + 1 == len(row_ids):
                        # 连续行使用BETWEEN
                        template = random.choice([
                            "id BETWEEN {min_id} AND {max_id}",  # 基本BETWEEN语法
                            "id >= {min_id} AND id <= {max_id}",  # 使用大于等于和小于等于
                            "NOT (id < {min_id} OR id > {max_id})",  # 使用NOT和OR的组合
                            "id >= {min_id} AND NOT (id > {max_id})",  # 使用大于等于和NOT大于
                            "NOT id < {min_id} AND NOT id > {max_id}",  # 使用双重NOT
                            "id = {min_id} OR (id > {min_id} AND id < {max_id}) OR id = {max_id}",  # 使用OR拆分
                            "(id - {min_id} >= 0) AND ({max_id} - id >= 0)",  # 使用减法
                            "CASE WHEN id >= {min_id} AND id <= {max_id} THEN 1 ELSE 0 END = 1",  # 使用CASE表达式
                            "EXISTS (SELECT 1 WHERE {min_id} <= id AND id <= {max_id})"  # 使用EXISTS子查询
                        ])
                        conditions.append(template.format(min_id=min_id, max_id=max_id))
                    else:
                        # 非连续行使用IN
                        template = random.choice([
                            "id IN ({ids})",  # 基本IN语法
                            "EXISTS (SELECT 1 FROM (VALUES {value_list}) AS t(v) WHERE t.v = id)",  # 使用EXISTS和VALUES
                            "CASE WHEN id IN ({ids}) THEN 1 ELSE 0 END = 1",  # 使用CASE表达式
                            "(SELECT COUNT(*) FROM (VALUES {value_list}) AS t(v) WHERE t.v = id) > 0",  # 使用COUNT和子查询
                            "FIND_IN_SET(id, '{ids_csv}') > 0",  # 使用FIND_IN_SET函数
                            " OR ".join(f"id = {id}" for id in row_ids)  # 使用OR连接
                        ])
                        
                        ids = ','.join(map(str, row_ids))
                        value_list = ','.join(f"({id})" for id in row_ids)
                        ids_csv = ','.join(map(str, row_ids))
                        
                        condition = template.format(
                            ids=ids,
                            value_list=value_list,
                            ids_csv=ids_csv
                        )
                        conditions.append(condition)

            # 70%的概率只使用id条件
            if use_id and random.random() < 0.7:
                return " AND ".join(f"({cond})" for cond in conditions)

            # 选择随机数量的额外列条件
            available_columns = [
                (col, self.column_type_map[col]) 
                for col in self.column_names 
                if col != 'id' and col not in self.primary_keys
            ]
            
            if available_columns:
                # 如果use_id为True,可以选择0个额外列;否则至少选择1个
                min_extra_cols = 0 if use_id else 1
                max_extra_cols = min(len(available_columns), 3)  # 最多选择3个额外列
                
                if max_extra_cols >= min_extra_cols:
                    num_extra_cols = random.randint(min_extra_cols, max_extra_cols)
                    selected_columns = random.sample(available_columns, num_extra_cols)
                    
                    for col_name, col_type in selected_columns:
                        # 随机选择一行的值
                        row = random.choice(self.rows)
                        col_idx = self.column_names.index(col_name)
                        value = row[col_idx]
                        
                        # 使用已有的格式化方法
                        conditions.append(self._format_condition(col_name, value))
            
            # 如果没有生成任何条件
            if not conditions:
                return "1=1"  # 返回永真条件
                
            # 使用AND连接所有条件
            return " AND ".join(f"({cond})" for cond in conditions)
            
        except Exception as e:
            self.logger.error(f"生成条件失败: {e}")
            self.logger.error("")
            return "1=1"  # 发生错误时返回永真条件
    
    def _generate_gap_lock_condition(self) -> str:
        """生成针对col_gap列的条件，以获取gap锁"""
        try:
            # 获取col_gap列的所有值并排序
            col_gap_values = sorted([row[1] for row in self.rows])  # 假设col_gap是第二列
            
            if not col_gap_values:
                return self._format_condition("col_gap", 0)
                
            # 找出所有的gap区间
            gaps = []
            for i in range(len(col_gap_values) - 1):
                current_val = col_gap_values[i]
                next_val = col_gap_values[i + 1]
                if next_val - current_val > 1:
                    # 找到一个gap，记录区间
                    gaps.append((current_val, next_val))
                    
            # 也考虑最小值之前和最大值之后的gap
            if col_gap_values[0] > 0:
                gaps.insert(0, (0, col_gap_values[0]))
            gaps.append((col_gap_values[-1], col_gap_values[-1] + 10))  # 最大值之后取一个合理范围
            
            if not gaps:
                return self._format_condition("col_gap", col_gap_values[0])
                
            # 随机选择一种gap锁定策略
            strategy = random.choice([
                'exact_gap',      # 精确锁定某个gap中的一个值
                'multi_exact_gap', # 精确锁定多个gap中的值
                'range_gap',      # 范围锁定，覆盖多个gap
                'boundary_gap'    # 使用边界值条件锁定gap
            ])
            
            if strategy == 'exact_gap':
                # 随机选择一个gap，使用等值查询锁定gap中的某个值
                gap = random.choice(gaps)
                gap_value = random.randint(gap[0] + 1, gap[1] - 1)
                return self._format_condition("col_gap", gap_value)
                
            elif strategy == 'multi_exact_gap':
                # 随机选择多个gap，每个gap选一个值进行多点等值查询
                selected_gaps = random.sample(gaps, k=min(random.randint(2, 3), len(gaps)))
                gap_values = [random.randint(gap[0] + 1, gap[1] - 1) for gap in selected_gaps]
                return "col_gap IN (" + ",".join(str(v) for v in gap_values) + ")"
                
            elif strategy == 'range_gap':
                # 随机选择一个起始gap和结束gap，锁定中间的范围
                start_idx = random.randint(0, len(gaps) - 1)
                end_idx = random.randint(start_idx, len(gaps) - 1)
                start_gap = gaps[start_idx]
                end_gap = gaps[end_idx]
                
                # 随机选择范围边界
                range_start = random.randint(start_gap[0] + 1, start_gap[1] - 1)
                range_end = random.randint(end_gap[0] + 1, end_gap[1] - 1)
                
                # 随机选择范围条件的表达方式
                range_template = random.choice([
                    "col_gap BETWEEN {start} AND {end}",
                    "col_gap >= {start} AND col_gap <= {end}",
                    "NOT (col_gap < {start} OR col_gap > {end})"
                ])
                return range_template.format(start=range_start, end=range_end)
                
            else:  # boundary_gap
                # 使用边界值条件锁定gap
                gap = random.choice(gaps)
                if random.choice([True, False]):
                    # 使用小于等于
                    return f"col_gap <= {gap[1] - 1}"
                else:
                    # 使用大于等于
                    return f"col_gap >= {gap[0] + 1}"
                
        except Exception as e:
            self.logger.error(f"生成gap锁条件失败: {e}")
            return self._format_condition("col_gap", 0)  # 发生错误时返回一个基本条件
    
    def _generate_set_expression(self) -> str:
        """生成SET表达式，确保值与列类型匹配"""
        # 排除主键列
        available_columns = [col for col in self.column_names if col not in self.primary_keys]
        if not available_columns:
            return ""
            
        # 随机选择要更新的列数
        num_cols_to_update = random.randint(1, len(available_columns))
        update_cols = random.sample(available_columns, num_cols_to_update)
        
        set_expressions = []
        for col in update_cols:
            col_type = self.column_type_map[col]
            value = self._generate_value_by_type(col_type)
            set_expressions.append(f"{col} = {value}")
            
        return ", ".join(set_expressions)
    
    def _generate_values(self) -> str:
        """生成VALUES表达式,参考MySQLInitializer的实现"""
        try:
            # 为每一列生成对应类型的值
            values = []
            # 如果是INSERT语句，确保values的数量与columns一致
            columns_to_insert = self.column_names
            
            for col_name, col_type in zip(columns_to_insert, self.column_types):
                if col_name == 'id' or 'auto_increment' in col_type.lower():
                    # id列或自增列使用NULL
                    values.append("NULL")
                else:
                    # 根据列类型生成随机值
                    value = self._generate_value_by_type(col_type)
                    values.append(value)
            
            return ", ".join(values)
            
        except Exception as e:
            self.logger.error(f"生成VALUES表达式失败: {e}")
            self.logger.error("")
            raise

    def _generate_insert_values(self, lock_type: str) -> str:
        """生成INSERT语句的VALUES部分"""
        try:
            # 获取col_gap列的值
            col_gap_values = sorted([row[1] for row in self.rows])  # 假设col_gap是第二列
            
            # 根据锁类型选择插入值
            if lock_type == "GAP":
                # 找出所有可用的gap
                gaps = []
                if len(col_gap_values) >= 2:
                    for i in range(len(col_gap_values) - 1):
                        if col_gap_values[i + 1] - col_gap_values[i] > 1:
                            gaps.append((col_gap_values[i], col_gap_values[i + 1]))
                            
                # 随机打乱gaps的顺序
                random.shuffle(gaps)
                
                # 遍历所有gap直到找到合适的值
                insert_value = None
                for start_val, end_val in gaps:
                    insert_value = random.randint(start_val + 1, end_val - 1)
                    break  # 找到第一个合适的值就停止
                    
                # 如果没有找到合适的gap，在最大值之后插入
                if insert_value is None:
                    insert_value = col_gap_values[-1] + 1 if col_gap_values else 1
                    
            elif lock_type == "NK":
                # Next-Key Lock需要在已有值之前的gap中插入
                # 随机选择一个已有值，在它之前的gap中插入
                if col_gap_values:
                    target_value = random.choice(col_gap_values)
                    target_idx = col_gap_values.index(target_value)
                    if target_idx > 0:
                        # 在选中值和它前一个值之间插入
                        insert_value = random.randint(col_gap_values[target_idx-1] + 1, target_value - 1)
                    else:
                        # 如果是第一个值，在它之前插入
                        insert_value = target_value - 1 if target_value > 1 else 1
                else:
                    insert_value = 1
            
            elif lock_type == "II":
                # Insert Intention Lock需要插入一个会导致唯一键冲突的值
                if col_gap_values:
                    # 选择一个已有值直接插入，触发唯一键冲突
                    insert_value = random.choice(col_gap_values)
                else:
                    insert_value = 1
            
            # 生成完整的VALUES部分
            vals_list = self._generate_values().split(',')
            vals_list[1] = str(insert_value)  # 假设col_gap是第二列
            return ','.join(vals_list)
            
        except Exception as e:
            self.logger.error(f"生成INSERT VALUES失败: {e}")
            raise

    def _generate_update_expression(self) -> str:
        """生成UPDATE表达式"""
        try:
            update_cols = [col for col in self.column_names if col not in self.primary_keys and col != 'id']
            update_exprs = []
            for col in update_cols:
                new_val = self._generate_value_by_type(
                    self.column_types[self.column_names.index(col)]
                )
                update_exprs.append(f"{col}={new_val}")
            return ','.join(update_exprs)
            
        except Exception as e:
            self.logger.error(f"生成UPDATE表达式失败: {e}")
            raise

    def _generate_index_name(self) -> str:
        """从传入的索引列表中随机选择一个索引"""
        if self.indexes:
            return random.choice(self.indexes)
        return f"idx_{random.choice(self.column_names)}"  # 如果没有索引，回退到使用列名
