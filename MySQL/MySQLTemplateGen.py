from typing import List, Tuple, Dict, Any, Optional, Set
import random
import logging
import re
from MySQLParameter import SQLParamGenerator

class MySQLTemplateGen:
    """MySQL模板生成器，负责基于模板生成SQL语句"""
    
    def __init__(self, lock_templates: dict, table_name: str):
        """
        初始化MySQL模板生成器
        
        Args:
            lock_templates: 锁模板字典
            table_name: 表名
        """
        self.lock_templates = lock_templates
        self.table_name = table_name
        self.logger = logging.getLogger('atomicity-checker')

    def generate_lock_sql(self, template_key: str, lock_level: str, 
                         lock_type: str, row_idx: int, 
                         is_continuous: bool = False, 
                         range_end_idx: int = None,
                         column_names: List[str] = None,
                         column_types: List[str] = None,
                         primary_keys: List[str] = None,
                         indexes: List[str] = None,
                         rows: List[tuple] = None) -> str:
        """
        统一的SQL生成方法
        
        Args:
            template_key: 模板键名
            lock_level: 锁级别
            lock_type: 锁类型
            row_idx: 行索引
            is_continuous: 是否连续锁定
            range_end_idx: 范围结束索引
            column_names: 列名列表
            column_types: 列类型列表
            primary_keys: 主键列表
            indexes: 索引列表
            rows: 行数据列表
            
        Returns:
            str: 生成的SQL语句
        """
        try:
            template = self.lock_templates[lock_level][lock_type][template_key]
            
            if is_continuous:
                self.logger.info(f"Generating continuous lock SQL with params: "
                               f"template_key={template_key}, lock_level={lock_level}, "
                               f"lock_type={lock_type}, row_idx={row_idx}, "
                               f"range_end_idx={range_end_idx}")
                print(f"Generating continuous lock SQL with params: "
                               f"template_key={template_key}, lock_level={lock_level}, "
                               f"lock_type={lock_type}, row_idx={row_idx}, "
                               f"range_end_idx={range_end_idx}")
                return self._generate_continuous_lock_sql(
                    template, column_names, column_types, 
                    primary_keys, indexes, rows, row_idx, range_end_idx,
                    lock_level, lock_type, template_key
                )
            else:
                self.logger.info(f"Generating discrete lock SQL with params: "
                               f"template_key={template_key}, lock_level={lock_level}, "
                               f"lock_type={lock_type}, row_idx={row_idx}")
                print(f"Generating discrete lock SQL with params: "
                               f"template_key={template_key}, lock_level={lock_level}, "
                               f"lock_type={lock_type}, row_idx={row_idx}")
                return self._generate_discrete_lock_sql(
                    template, column_names, column_types, 
                    primary_keys, indexes, rows, row_idx,
                    lock_level, lock_type, template_key
                )
        except Exception as e:
            self.logger.error(f"生成锁SQL失败: {e}")
            raise

    def _generate_continuous_lock_sql(self, template: str,
                                    column_names: List[str],
                                    column_types: List[str],
                                    primary_keys: List[str],
                                    indexes: List[str],
                                    rows: List[tuple],
                                    start_idx: int,
                                    end_idx: int,
                                    lock_level: str,
                                    lock_type: str,
                                    template_key: str) -> str:
        """
        生成连续锁定的SQL
        
        Args:
            template: SQL模板
            column_names: 列名列表
            column_types: 列类型列表
            primary_keys: 主键列表
            indexes: 索引列表
            rows: 行数据列表
            start_idx: 起始索引
            end_idx: 结束索引
            lock_level: 锁级别
            lock_type: 锁类型
            template_key: 模板键名
            
        Returns:
            str: 生成的SQL语句
        """
        try:
            if not rows:
                self.logger.info(f"找不到从{start_idx}到{end_idx}的行,当前事务没有独占行")
                return None
            
            # 创建SQL参数生成器
            param_generator = SQLParamGenerator(
                self.table_name,
                column_names,
                column_types,
                primary_keys,
                rows,
                lock_type,
                indexes
            )
            
            # 获取模板需要的参数
            needed_params = self._extract_template_params(template)
            params = param_generator.generate_params(needed_params)

            self.logger.info(f"needed_params: {needed_params}")
            self.logger.info(f"params: {params}")

            if lock_type not in ["GAP", "II", "NK"]:
                # 处理范围查询的特殊参数
                if 'v1' in needed_params:
                    params['v1'] = str(start_idx)
                if 'v2' in needed_params:
                    params['v2'] = str(end_idx)
                if 'col' in needed_params:
                    params['col'] = 'id'
            
            return template.format(**params)
            
        except Exception as e:
            self.logger.error(f"生成连续锁定SQL失败: {e}")
            raise

    def _generate_discrete_lock_sql(self, template: str,
                                  column_names: List[str],
                                  column_types: List[str],
                                  primary_keys: List[str],
                                  indexes: List[str],
                                  rows: List[tuple],
                                  row_idx: int,
                                  lock_level: str,
                                  lock_type: str,
                                  template_key: str) -> str:
        """
        生成离散锁定的SQL
        
        Args:
            template: SQL模板
            column_names: 列名列表
            column_types: 列类型列表
            primary_keys: 主键列表
            indexes: 索引列表
            rows: 行数据列表
            row_idx: 行索引
            lock_level: 锁级别
            lock_type: 锁类型
            template_key: 模板键名
            
        Returns:
            str: 生成的SQL语句
        """
        try:
            if len(rows) == 0:
                self.logger.info(f"找不到索引为{row_idx}的行")
                return None

            # 创建SQL参数生成器
            param_generator = SQLParamGenerator(
                self.table_name,
                column_names,
                column_types,
                primary_keys,
                rows,
                lock_type,
                indexes
            )
            
            # 获取模板需要的参数
            needed_params = self._extract_template_params(template)
            params = param_generator.generate_params(needed_params)

            self.logger.info(f"needed_params: {needed_params}")
            self.logger.info(f"params: {params}")
            
            return template.format(**params)
            
        except Exception as e:
            self.logger.error(f"生成离散锁定SQL失败: {e}")
            raise

    def _extract_template_params(self, template: str) -> Set[str]:
        """
        从模板中提取所需参数
        
        Args:
            template: SQL模板
            
        Returns:
            Set[str]: 参数集合
        """
        return set(re.findall(r'\{(\w+)\}', template)) 