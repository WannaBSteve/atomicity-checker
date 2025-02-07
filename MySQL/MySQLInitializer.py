import random
import mysql.connector


class MySQLInitializer:
    def __init__(self, connection, database):
        self.conn = connection
        self.database = database
        self.cursor = self.conn.cursor()
        
        # 动态控制初始化的表数量和操作次数
        self.min_tables = 1
        self.max_tables = 1
        self.max_inserts_per_table = 15
        self.max_actions_per_table = 5

        self.used_ids = set()  # 添加这一行来跟踪已使用的ID

    def initialize_database(self):
        """初始化数据库，删除旧数据库并创建新数据库"""
        self.cursor.execute(f"DROP DATABASE IF EXISTS {self.database}")
        self.cursor.execute(f"CREATE DATABASE {self.database}")
        self.cursor.execute(f"USE {self.database}")

    def generate_tables(self):
        """生成随机数量的表"""
        # num_tables = random.randint(self.min_tables, self.max_tables)
        num_tables = 1
        for i in range(num_tables):
            table_name = f"table_{i}"
            create_table_sql = self._generate_create_table_sql(table_name)
            print(f"Creating Table: {create_table_sql}")
            self.cursor.execute(create_table_sql)

    def _generate_create_table_sql(self, table_name):
        """生成随机建表SQL"""
        num_columns = random.randint(2, 5)  # 每个表随机列数
        # 确保id列为主键和AUTO_INCREMENT
        column_definitions = ["id INT AUTO_INCREMENT PRIMARY KEY"]
        column_types = ["INT", "VARCHAR(255)", "FLOAT", "DOUBLE", "TEXT"]

        for i in range(num_columns):
            column_name = f"col_{i}"
            column_type = random.choice(column_types)
            column_definitions.append(f"{column_name} {column_type}")

        columns_sql = ", ".join(column_definitions)
        return f"CREATE TABLE {table_name} ({columns_sql})"


    def populate_tables(self):
        """随机向每个表插入数据"""
        # self.cursor.execute("SHOW TABLES")
        # tables = [row[0] for row in self.cursor.fetchall()]
        tables = ["table_0"]
        for table in tables:
            num_inserts = random.randint(5, self.max_inserts_per_table)
            for _ in range(num_inserts):
                insert_sql = self._generate_insert_sql(table)
                print(f"Inserting Data: {insert_sql}")
                self.cursor.execute(insert_sql)
    
    def _generate_insert_sql(self, table_name):
        """生成随机插入语句，确保数据类型与列定义匹配"""
        self.cursor.execute(f"DESCRIBE {table_name}")
        columns = [(row[0], row[1]) for row in self.cursor.fetchall()]  # 获取列名和数据类型

        # 不要生成所有列的值，而是跳过id列
        column_names = [col[0] for col in columns]  
        values = [self._generate_value_by_type(col[1]) for col in columns]  # 跳过id列
        # values最前面id列值为null
        values[0] = "NULL"
        # 构建INSERT语句时明确指定列名（除了id列）
        columns_sql = ", ".join(column_names)
        values_sql = ", ".join(values)
        return f"INSERT INTO {table_name} ({columns_sql}) VALUES ({values_sql})"

    def _generate_value_by_type(self, col_type):
        """根据列的类型生成随机值"""
        if "int" in col_type.lower():
            if "auto_increment" in col_type.lower():
                # 为auto_increment列生成NULL，让MySQL自动处理
                return "NULL"
            return str(random.randint(1, 100))
        elif "varchar" in col_type.lower() or "text" in col_type.lower():
            return f"'{self._generate_random_string()}'"
        elif "float" in col_type.lower() or "double" in col_type.lower():
            return str(round(random.uniform(1, 100), 2))
        else:
            raise ValueError(f"Unsupported column type: {col_type}")


    def _generate_random_string(self, length=5):
        """生成随机字符串"""
        letters = "abcdefghijklmnopqrstuvwxyz"
        return "".join(random.choice(letters) for _ in range(length))

    def execute_random_actions(self):
        """执行一系列随机化操作"""
        self.cursor.execute("SHOW TABLES")
        tables = [row[0] for row in self.cursor.fetchall()]
        actions = [
            self._generate_update_sql,
            self._generate_delete_sql,
            self._generate_alter_table_sql
        ]

        for table in tables:
            num_actions = random.randint(0, self.max_actions_per_table)
            for _ in range(num_actions):
                action = random.choice(actions)
                sql = action(table)
                if sql:
                    print(f"Executing: {sql}")
                    self.cursor.execute(sql)

    def _generate_update_sql(self, table_name):
        """生成随机更新语句，确保值与列类型匹配"""
        self.cursor.execute(f"DESCRIBE {table_name}")
        columns = [(row[0], row[1]) for row in self.cursor.fetchall()]  # 获取列名和类型
        if not columns:
            return None

        # 随机选择一个列
        column, col_type = random.choice(columns)
        value = self._generate_value_by_type(col_type)

        # 随机选择条件列
        condition_column, condition_col_type = random.choice(columns)
        condition_value = self._generate_value_by_type(condition_col_type)
        
        return f"UPDATE {table_name} SET {column} = {value} WHERE {condition_column} = {condition_value}"

    def _generate_delete_sql(self, table_name):
        """生成随机删除语句，确保条件值与列类型匹配"""
        self.cursor.execute(f"DESCRIBE {table_name}")
        columns = [(row[0], row[1]) for row in self.cursor.fetchall()]  # 获取列名和类型
        if not columns:
            return None

        # 随机选择条件列
        condition_column, condition_col_type = random.choice(columns)
        condition_value = self._generate_value_by_type(condition_col_type)

        return f"DELETE FROM {table_name} WHERE {condition_column} = {condition_value}"


    def _generate_alter_table_sql(self, table_name):
        """生成随机表结构修改语句"""
        alter_action = random.choice(["ADD", "DROP", "MODIFY"])
        
        if alter_action == "ADD":
            new_column = f"col_{random.randint(100, 999)}"
            new_type = random.choice(["INT", "VARCHAR(255)", "FLOAT"])
            return f"ALTER TABLE {table_name} ADD COLUMN {new_column} {new_type}"
        elif alter_action == "DROP":
            self.cursor.execute(f"DESCRIBE {table_name}")
            columns = [row[0] for row in self.cursor.fetchall()]
            # 移除id列，确保不会被删除
            columns = [col for col in columns if col != 'id']
            if not columns:
                return None
            column_to_drop = random.choice(columns)
            return f"ALTER TABLE {table_name} DROP COLUMN {column_to_drop}"
        elif alter_action == "MODIFY":
            self.cursor.execute(f"DESCRIBE {table_name}")
            columns = [(row[0], row[1]) for row in self.cursor.fetchall()]
            # 移除id列，确保不会被修改
            columns = [(col, type_) for col, type_ in columns if col != 'id']
            if not columns:
                return None
            
            column_to_modify, current_type = random.choice(columns)
            
            # 定义类型兼容性映射
            compatible_types = {
                'text': ['TEXT', 'VARCHAR(255)'],
                'varchar': ['TEXT', 'VARCHAR(255)'],
                'int': ['INT', 'BIGINT'],
                'float': ['FLOAT', 'DOUBLE'],
                'double': ['FLOAT', 'DOUBLE']
            }
            
            # 根据当前类型选择兼容的新类型
            current_base_type = current_type.lower().split('(')[0]  # 提取基本类型
            if current_base_type in compatible_types:
                new_type = random.choice(compatible_types[current_base_type])
                return f"ALTER TABLE {table_name} MODIFY COLUMN {column_to_modify} {new_type}"
            
            return None  # 如果找不到兼容的类型，返回None

    def commit_and_close(self):
        """只提交更改,不关闭连接(由调用者管理)"""
        try:
            self.conn.commit()
        except Exception as e:
            print(f"Error committing changes: {e}")
        finally:
            if self.cursor:
                self.cursor.close()


