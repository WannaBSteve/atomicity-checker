import random
import mysql.connector
import logging
from typing import List, Dict, Tuple, Optional
from enum import Enum

logger = logging.getLogger('atomicity-checker')

class MySQLDataType(Enum):
    TINYINT = "TINYINT"
    SMALLINT = "SMALLINT"
    MEDIUMINT = "MEDIUMINT"
    INT = "INT"
    BIGINT = "BIGINT"
    
    FLOAT = "FLOAT"
    DOUBLE = "DOUBLE"
    DECIMAL = "DECIMAL"
    
    BINARY = "BINARY"
    VARBINARY = "VARBINARY"
    
    CHAR = "CHAR"
    VARCHAR = "VARCHAR"
    TINYTEXT = "TINYTEXT"
    TEXT = "TEXT"
    MEDIUMTEXT = "MEDIUMTEXT"
    LONGTEXT = "LONGTEXT"
    
    BLOB = "BLOB"
    MEDIUMBLOB = "MEDIUMBLOB"
    LONGBLOB = "LONGBLOB"
    
    DATE = "DATE"
    TIME = "TIME"
    DATETIME = "DATETIME"
    TIMESTAMP = "TIMESTAMP"
    
    JSON = "JSON"
    ENUM = "ENUM"
    SET = "SET"

    @staticmethod
    def get_random_data_type():
        return random.choice([
            MySQLDataType.INT,
            MySQLDataType.FLOAT,
            MySQLDataType.DOUBLE,
            MySQLDataType.CHAR,
            MySQLDataType.VARCHAR,
            MySQLDataType.TEXT
        ])

    @staticmethod
    def is_numeric(data_type: str) -> bool:
        numeric_types = [
            MySQLDataType.TINYINT,
            MySQLDataType.SMALLINT,
            MySQLDataType.MEDIUMINT,
            MySQLDataType.INT,
            MySQLDataType.BIGINT,
            MySQLDataType.FLOAT,
            MySQLDataType.DOUBLE,
            MySQLDataType.DECIMAL
        ]
        return any(dt.value in data_type for dt in numeric_types)

    @staticmethod
    def is_string(data_type: str) -> bool:
        string_types = [
            MySQLDataType.CHAR,
            MySQLDataType.VARCHAR,
            MySQLDataType.TINYTEXT,
            MySQLDataType.TEXT,
            MySQLDataType.MEDIUMTEXT,
            MySQLDataType.LONGTEXT
        ]
        return any(dt.value in data_type for dt in string_types)

    @staticmethod
    def has_len(data_type: str) -> bool:
        return any(dt.value in data_type for dt in [MySQLDataType.CHAR, MySQLDataType.VARCHAR])

class MySQLColumn:
    def __init__(self, table_name: str, column_name: str, data_type: str, 
                 is_primary: bool, is_unique: bool, is_not_null: bool, size: int = 0):
        self.table_name = table_name
        self.column_name = column_name
        self.data_type = data_type
        self.is_primary = is_primary
        self.is_unique = is_unique
        self.is_not_null = is_not_null
        self.size = size
        self.appeared_values = []  # 对应 Java 中的 appearedValues

    def get_random_value(self) -> str:
        if self.is_primary:
            return "NULL"  # 自增主键

        # 完全对齐 Java 实现的数据类型判断
        data_type_upper = self.data_type.upper()
        
        if "TINYINT" in data_type_upper:
            return str(random.randint(-128, 127))
        elif "SMALLINT" in data_type_upper:
            return str(random.randint(-32768, 32767))
        elif "MEDIUMINT" in data_type_upper:
            return str(random.randint(-8388608, 8388607))
        elif "INT" in data_type_upper or "BIGINT" in data_type_upper:
            return str(random.randint(-2147483648, 2147483647))
        elif "FLOAT" in data_type_upper or "DOUBLE" in data_type_upper or "DECIMAL" in data_type_upper:
            return str(round(random.uniform(-100, 100), 2))
        elif any(t in data_type_upper for t in ["CHAR", "VARCHAR", "TEXT", "TINYTEXT", "MEDIUMTEXT", "LONGTEXT"]):
            if self.size == 0:
                self.size = 20
            random_str = ''.join(random.choice('abcdefghijklmnopqrstuvwxyz') for _ in range(random.randint(1, self.size)))
            return f'"{random_str}"'
        elif any(t in data_type_upper for t in ["BLOB", "MEDIUMBLOB", "LONGBLOB"]):
            return self._random_hex_str()
        else:
            raise ValueError(f"Unexpected data type: {self.data_type}")

    def _random_hex_str(self) -> str:
        """对应 Java 中的 randomHexStr 方法"""
        size = 8
        hex_chars = "0123456789ABCDEF"
        hex_str = ''.join(random.choice(hex_chars) for _ in range(size))
        return f"0x{hex_str}"

class MySQLInitializer:
    def __init__(self, connection, database):
        self.conn = connection
        self.database = database
        self.cursor = self.conn.cursor()
        self.tables: Dict[str, List[Tuple[str, str, bool, bool, bool, int]]] = {}  # 存储表的列信息
        self.columns: Dict[str, List[MySQLColumn]] = {}  # 存储表的列信息
        self.num_of_tables = 0

    def initialize_database(self):
        """初始化数据库"""
        logger.info(f"正在删除数据库（如果存在）: {self.database}")
        print(f"正在删除数据库（如果存在）: {self.database}")
        self.cursor.execute(f"DROP DATABASE IF EXISTS {self.database}")
        logger.info(f"正在创建数据库: {self.database}")
        print(f"正在创建数据库: {self.database}")
        self.cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.database}")
        self.cursor.execute(f"USE {self.database}")
    
    def generate_tables_with_data_and_index(self, num_of_tables: int):
        """生成表结构、数据和索引"""
        # table_name = "table_0"  # 固定生成一个表
        # 多表
        self.num_of_tables = num_of_tables
        table_names = [f"table_{i}" for i in range(num_of_tables)]
        self.tables = {table_name: [] for table_name in table_names}
        self.columns = {table_name: [] for table_name in table_names}
        for table_name in table_names:
            # 生成列定义
            columns = self._generate_columns(table_name)
            create_table_sql = self._generate_create_table_sql(table_name, columns)
            logger.info(f"创建表: {create_table_sql}")
            print(f"创建表: {create_table_sql}")
            self.cursor.execute(create_table_sql)
            self.tables[table_name] = columns

            # 插入数据
            self._populate_table(table_name)
            
            # 创建索引
            self._create_indexes(table_name)
        
        # 添加新功能调用
        self._generate_foreign_keys()
        self._create_triggers()
        self._create_stored_procedures()
        self._create_complex_stored_procedures()  # 这里已经有调用了
    
    def _generate_columns(self, table_name: str) -> List[Tuple[str, str, bool, bool, bool, int]]:
        """生成列定义"""
        columns = []
        # 添加id列作为主键
        columns.append(("id", "INT", True, False, True, 0))
        self.columns[table_name].append(MySQLColumn(table_name, "id", "INT", True, False, True, 0))

        # 生成2-9个随机列
        num_columns = random.randint(2, 9)
        for i in range(num_columns):
            col_name = f"c{i}"
            data_type = MySQLDataType.get_random_data_type().value
            
            # 确定列的约束
            is_primary = False
            is_unique = random.choice([True, False]) if MySQLDataType.is_numeric(data_type) else False
            is_not_null = random.choice([True, False])
            
            size = 0
            if MySQLDataType.has_len(data_type):
                size = random.randint(1, 20)
                data_type = f"{data_type}({size})"
            
            # 创建并存储列对象
            columns.append((col_name, data_type, is_primary, is_unique, is_not_null, size))
            self.columns[table_name].append(MySQLColumn(table_name, col_name, data_type, is_primary, is_unique, is_not_null, size))

        return columns

    def _generate_create_table_sql(self, table_name: str, columns: List[Tuple[str, str, bool, bool, bool, int]]) -> str:
        """生成建表SQL"""
        column_defs = []
        for col_name, data_type, is_primary, is_unique, is_not_null, size in columns:
            col_def = f"{col_name} {data_type}"
            if is_primary:
                col_def += " PRIMARY KEY AUTO_INCREMENT"
            if is_unique and not is_primary:
                col_def += " UNIQUE"
            if is_not_null and not is_primary:
                col_def += " NOT NULL"
            column_defs.append(col_def)
            
        # 添加表选项
        table_options = self._generate_table_options()
        
        return f"CREATE TABLE {table_name} ({', '.join(column_defs)}) {table_options}"
    
    def _generate_table_options(self) -> str:
        """生成表选项"""
        options = []
        if random.random() < 0.3:  # 30%概率添加选项
            possible_options = {
                "AUTO_INCREMENT": lambda: random.randint(1, 1000),
                "CHECKSUM": lambda: 1,
                "DELAY_KEY_WRITE": lambda: random.choice([0, 1]),
                "MAX_ROWS": lambda: random.randint(1000, 1000000),
                "MIN_ROWS": lambda: random.randint(1, 100),
                "PACK_KEYS": lambda: random.choice(["1", "0", "DEFAULT"]),
                "STATS_AUTO_RECALC": lambda: random.choice(["1", "0", "DEFAULT"]),
                "COMMENT": lambda: "'comment info'"
            }
            
            # 修改选择选项的方式
            option_keys = list(possible_options.keys())
            num_options = random.randint(1, min(3, len(option_keys)))
            selected_keys = random.sample(option_keys, num_options)
            
            for key in selected_keys:
                value = possible_options[key]()
                options.append(f"{key}={value}")
                
        if options:
            return "ENGINE=InnoDB " + " ".join(options)
        return "ENGINE=InnoDB"

    def _populate_table(self, table_name: str):
        """填充表数据"""
        num_rows = random.randint(5, 15)
        columns = self.columns[table_name]
        for _ in range(num_rows):
            values = []
            for column in columns:
                values.append(column.get_random_value())
            
            col_names = [col.column_name for col in columns]
            insert_sql = f"INSERT INTO {table_name} ({', '.join(col_names)}) VALUES ({', '.join(values)})"
            
            logger.info(f"插入数据: {insert_sql}")
            print(f"插入数据: {insert_sql}")
            self.cursor.execute(insert_sql)
    
    def _create_indexes(self, table_name: str):
        """创建索引，对齐Java实现"""
        # 筛选可作为索引的列（非TEXT类型的列）
        columns = self.columns[table_name]
        indexable_columns = [col for col in columns
                           if not any(t in col.data_type.upper() 
                                    for t in ["TEXT", "BLOB"])]
        
        if not indexable_columns:
            return
            
        # 随机创建1-3个索引
        num_indexes = random.randint(1, 3)
        for i in range(num_indexes):
            # 随机决定是否创建复合索引
            is_composite = random.choice([True, False])
            num_columns = random.randint(2, 3) if is_composite else 1
            
            # 随机选择列，但确保不重复选择相同的列组合
            selected_columns = random.sample(indexable_columns, 
                                          min(num_columns, len(indexable_columns)))
            
            # 随机决定是否为唯一索引
            is_unique = random.choice([True, False])
            
            index_name = f"idx_{i}"
            index_type = "UNIQUE INDEX" if is_unique else "INDEX"
            
            # 使用列名创建索引
            column_names = [col.column_name for col in selected_columns]
            index_sql = (f"CREATE {index_type} {index_name} ON {table_name} "
                        f"({', '.join(column_names)})")
            
            try:
                logger.info(f"创建索引: {index_sql}")
                print(f"创建索引: {index_sql}")
                self.cursor.execute(index_sql)
            except Exception as e:
                logger.error(f"创建索引时出错: {e}")
                print(f"创建索引时出错: {e}")
                # 继续创建其他索引
    
    def _generate_foreign_keys(self):
        """为表之间生成外键关系"""
        if self.num_of_tables < 2:  # 至少需要两个表才能创建外键
            return
        
        # 每个表有50%的概率添加外键
        for child_table in list(self.tables.keys()):
            if random.random() < 0.5:
                # 随机选择一个父表（不能是自己）
                potential_parents = [t for t in self.tables.keys() if t != child_table]
                if not potential_parents:
                    continue
                
                parent_table = random.choice(potential_parents)
                
                # 在子表中选择一个非主键列作为外键
                child_columns = [col for col in self.columns[child_table] 
                               if not col.is_primary and MySQLDataType.is_numeric(col.data_type)]
                if not child_columns:
                    continue
                
                child_column = random.choice(child_columns)
                
                # 父表中的主键列通常是参照对象
                parent_column = next((col for col in self.columns[parent_table] if col.is_primary), None)
                if not parent_column:
                    continue
                
                # 创建外键
                fk_name = f"fk_{child_table}_{parent_table}"
                fk_sql = (f"ALTER TABLE {child_table} ADD CONSTRAINT {fk_name} "
                         f"FOREIGN KEY ({child_column.column_name}) REFERENCES "
                         f"{parent_table}({parent_column.column_name})")
                         
                try:
                    logger.info(f"添加外键: {fk_sql}")
                    print(f"添加外键: {fk_sql}")
                    self.cursor.execute(fk_sql)
                except Exception as e:
                    logger.error(f"创建外键时出错: {e}")
                    print(f"创建外键时出错: {e}")

    def _create_triggers(self):
        """为每个表创建更全面的CRUD触发器"""
        for table_name in self.tables:
            # 提高触发器创建概率至90%
            trigger_types = []
            if random.random() < 0.9:  # 从60%提高到90%
                trigger_types.append(("BEFORE", "INSERT"))
            if random.random() < 0.9:  # 从60%提高到90%
                trigger_types.append(("BEFORE", "UPDATE"))
            if random.random() < 0.9:  # 从50%提高到90%
                trigger_types.append(("AFTER", "DELETE"))
            # 添加AFTER INSERT和AFTER UPDATE触发器
            if random.random() < 0.7:
                trigger_types.append(("AFTER", "INSERT"))
            if random.random() < 0.7:
                trigger_types.append(("AFTER", "UPDATE"))
            
            # 为选定的触发器类型创建触发器
            for event_time, event in trigger_types:
                trigger_name = f"trg_{table_name}_{event_time.lower()}_{event.lower()}"
                
                # 生成更复杂的触发器逻辑
                action_body = self._generate_trigger_body(table_name, event_time, event)
                
                trigger_sql = (f"CREATE TRIGGER {trigger_name} "
                              f"{event_time} {event} ON {table_name} "
                              f"FOR EACH ROW "
                              f"BEGIN "
                              f"{action_body} "
                              f"END;")
                
                try:
                    # self.cursor.execute("DELIMITER //")
                    logger.info(f"创建触发器: {trigger_name}, SQL: {trigger_sql}")
                    print(f"创建触发器: {trigger_name}")
                    self.cursor.execute(trigger_sql)
                    # self.cursor.execute("DELIMITER ;")
                except Exception as e:
                    logger.error(f"创建触发器时出错: {e}")
                    print(f"创建触发器时出错: {e}")

    def _generate_trigger_body(self, table_name, event_time, event):
        """生成更复杂的触发器逻辑"""
        # 获取表的列
        columns = self.columns[table_name]
        other_tables = [t for t in self.tables.keys() if t != table_name]
        
        trigger_logic = []
        
        # 1. 添加变量声明
        trigger_logic.append("DECLARE v_count INT;")
        trigger_logic.append("DECLARE v_message VARCHAR(100);")
        
        # 2. 根据事件类型生成主要逻辑
        if event == "INSERT":
            if event_time == "BEFORE":
                # 复杂的数据验证和转换
                numeric_cols = [col for col in columns if MySQLDataType.is_numeric(col.data_type)]
                string_cols = [col for col in columns if MySQLDataType.is_string(col.data_type)]
                
                if numeric_cols:
                    col = random.choice(numeric_cols)
                    trigger_logic.append(f"""
                    IF NEW.{col.column_name} < 0 THEN
                        SET v_message = CONCAT('Invalid value for {col.column_name}: ', NEW.{col.column_name});
                        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_message;
                    END IF;
                    """)
                
                if string_cols:
                    col = random.choice(string_cols)
                    trigger_logic.append(f"""
                    IF NEW.{col.column_name} IS NOT NULL THEN
                        SET NEW.{col.column_name} = CONCAT(UPPER(SUBSTRING(NEW.{col.column_name}, 1, 1)), 
                                                         LOWER(SUBSTRING(NEW.{col.column_name}, 2)));
                    END IF;
                    """)
                
            else:  # AFTER
                if other_tables:
                    target_table = random.choice(other_tables)
                    trigger_logic.append(f"""
                    SELECT COUNT(*) INTO v_count FROM {table_name};
                    IF v_count > 10 THEN
                        INSERT INTO {target_table} (c0, c1)
                        VALUES (NEW.id, CONCAT('Triggered insert at ', NOW()));
                    END IF;
                    """)
        
        elif event == "UPDATE":
            if event_time == "BEFORE":
                # 复杂的更新验证逻辑
                numeric_cols = [col for col in columns if MySQLDataType.is_numeric(col.data_type)]
                if numeric_cols:
                    col = random.choice(numeric_cols)
                    trigger_logic.append(f"""
                    IF NEW.{col.column_name} < OLD.{col.column_name} THEN
                        SET NEW.{col.column_name} = OLD.{col.column_name};
                        SET v_message = '不允许减小值';
                        INSERT INTO audit_log(action, table_name, record_id) 
                        VALUES ('UPDATE_BLOCKED', '{table_name}', OLD.id);
                    END IF;
                    """)
            
            else:  # AFTER
                if other_tables:
                    target_table = random.choice(other_tables)
                    trigger_logic.append(f"""
                    IF NEW.c0 <> OLD.c0 THEN
                        INSERT INTO {target_table} (c0, c1)
                        SELECT NEW.c0 * 2, CONCAT('Changed from ', OLD.c0, ' to ', NEW.c0)
                        WHERE NOT EXISTS (
                            SELECT 1 FROM {target_table} 
                            WHERE c0 = NEW.c0 * 2
                        );
                    END IF;
                    """)
        
        elif event == "DELETE":
            if event_time == "BEFORE":
                trigger_logic.append(f"""
                SELECT COUNT(*) INTO v_count FROM {table_name};
                IF v_count <= 3 THEN
                    SIGNAL SQLSTATE '45000' 
                    SET MESSAGE_TEXT = '表中至少需要保留3条记录';
                END IF;
                """)
            
            else:  # AFTER
                if other_tables:
                    target_table = random.choice(other_tables)
                    trigger_logic.append(f"""
                    INSERT INTO {target_table} (c0, c1)
                    VALUES (OLD.id, CONCAT('Archived from {table_name} at ', NOW()));
                    """)
        
        # 3. 添加审计日志
        trigger_logic.append(f"""
        INSERT INTO audit_log(action, table_name, record_id) 
        VALUES ('{event}', '{table_name}', {
            'NEW.id' if event != 'DELETE' else 'OLD.id'
        });
        """)
        
        return "\n".join(trigger_logic)

    def _create_stored_procedures(self):
        """为每个表创建完整的CRUD存储过程"""
        for table_name in self.tables:
            # 为每个表创建四种基本的CRUD存储过程
            self._create_select_procedure(table_name)
            self._create_insert_procedure(table_name)
            self._create_update_procedure(table_name)
            self._create_delete_procedure(table_name)

    def _create_select_procedure(self, table_name):
        """创建查询存储过程"""
        proc_name = f"sp_get_{table_name}"
        proc_sql = f"""
        CREATE PROCEDURE {proc_name}(IN p_id INT)
        BEGIN
            SELECT * FROM {table_name} WHERE id = p_id;
        END
        """
        try:
            logger.info(f"创建查询存储过程: {proc_name}")
            print(f"创建查询存储过程: {proc_name}")
            # self.cursor.execute("DELIMITER //")
            self.cursor.execute(proc_sql)
            # self.cursor.execute("DELIMITER ;")
        except Exception as e:
            logger.error(f"创建存储过程时出错: {e}")
            print(f"创建存储过程时出错: {e}")

    def _create_insert_procedure(self, table_name):
        """创建插入存储过程"""
        proc_name = f"sp_insert_{table_name}"
        
        # 获取非主键列
        columns = [col for col in self.columns[table_name] if not col.is_primary]
        if not columns:
            return  # 如果没有非主键列，不创建此存储过程
        
        # 创建参数和列列表
        param_list = []
        col_names = []
        param_names = []
        
        for col in columns:
            param_list.append(f"IN p_{col.column_name} {col.data_type}")
            col_names.append(col.column_name)
            param_names.append(f"p_{col.column_name}")
        
        proc_sql = f"""
        CREATE PROCEDURE {proc_name}({', '.join(param_list)})
        BEGIN
            INSERT INTO {table_name} ({', '.join(col_names)})
            VALUES ({', '.join(param_names)});
            SELECT LAST_INSERT_ID() AS new_id;
        END
        """
        
        try:
            logger.info(f"创建插入存储过程: {proc_name}")
            print(f"创建插入存储过程: {proc_name}")
            # self.cursor.execute("DELIMITER //")
            self.cursor.execute(proc_sql)
            # self.cursor.execute("DELIMITER ;")
        except Exception as e:
            logger.error(f"创建存储过程时出错: {e}")
            print(f"创建存储过程时出错: {e}")

    def _create_update_procedure(self, table_name):
        """创建更新存储过程"""
        proc_name = f"sp_update_{table_name}"
        
        # 找到主键和非主键列
        primary_key = next((col for col in self.columns[table_name] if col.is_primary), None)
        if not primary_key:
            return  # 如果没有主键，不创建此存储过程
        
        update_cols = [col for col in self.columns[table_name] if not col.is_primary]
        if not update_cols:
            return  # 如果没有可更新的列，不创建此存储过程
        
        # 创建参数列表和SET子句
        param_list = [f"IN p_id {primary_key.data_type}"]
        set_clauses = []
        
        for col in update_cols:
            param_list.append(f"IN p_{col.column_name} {col.data_type}")
            set_clauses.append(f"{col.column_name} = p_{col.column_name}")
        
        proc_sql = f"""
        CREATE PROCEDURE {proc_name}({', '.join(param_list)})
        BEGIN
            UPDATE {table_name}
            SET {', '.join(set_clauses)}
            WHERE id = p_id;
            SELECT ROW_COUNT() AS rows_affected;
        END
        """
        
        try:
            logger.info(f"创建更新存储过程: {proc_name}")
            print(f"创建更新存储过程: {proc_name}")
            # self.cursor.execute("DELIMITER //")
            self.cursor.execute(proc_sql)
            # self.cursor.execute("DELIMITER ;")
        except Exception as e:
            logger.error(f"创建存储过程时出错: {e}")
            print(f"创建存储过程时出错: {e}")

    def _create_delete_procedure(self, table_name):
        """创建删除存储过程"""
        proc_name = f"sp_delete_{table_name}"
        proc_sql = f"""
        CREATE PROCEDURE {proc_name}(IN p_id INT, OUT p_rows_affected INT)
        BEGIN
            DELETE FROM {table_name} WHERE id = p_id;
            SELECT ROW_COUNT() INTO p_rows_affected;
        END
        """
        
        try:
            logger.info(f"创建删除存储过程: {proc_name}")
            print(f"创建删除存储过程: {proc_name}")
            # self.cursor.execute("DELIMITER //")
            self.cursor.execute(proc_sql)
            # self.cursor.execute("DELIMITER ;")
        except Exception as e:
            logger.error(f"创建存储过程时出错: {e}")
            print(f"创建存储过程时出错: {e}")

    def _create_complex_stored_procedures(self):
        """创建复杂的存储过程，包含事务、条件分支、游标等"""
        if len(self.tables) < 2:
            return
        
        try:
            # 创建审计日志表（如果还没有创建）
            self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS audit_log (
                id INT PRIMARY KEY AUTO_INCREMENT,
                action VARCHAR(100),
                table_name VARCHAR(100),
                record_id INT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """)
            
            # 为每个表创建复杂存储过程
            for table_name in self.tables:
                other_tables = [t for t in self.tables.keys() if t != table_name]
                if not other_tables:
                    continue
                
                target_table = random.choice(other_tables)
                
                # 1. 创建带有事务和游标的复杂存储过程
                proc_name = f"sp_complex_operation_{table_name}"
                proc_sql = f"""
                CREATE PROCEDURE {proc_name}(IN p_id INT, IN p_value INT)
                BEGIN
                    DECLARE v_done INT DEFAULT FALSE;
                    DECLARE v_id INT;
                    DECLARE v_value INT;
                    DECLARE v_error_occurred BOOLEAN DEFAULT FALSE;
                    DECLARE cur CURSOR FOR 
                        SELECT id, c0 FROM {table_name} 
                        WHERE id >= p_id AND c0 IS NOT NULL
                        ORDER BY id LIMIT 5;
                    DECLARE CONTINUE HANDLER FOR NOT FOUND SET v_done = TRUE;
                    DECLARE EXIT HANDLER FOR SQLEXCEPTION 
                    BEGIN
                        SET v_error_occurred = TRUE;
                        ROLLBACK;
                        RESIGNAL;
                    END;

                    START TRANSACTION;
                    
                    SAVEPOINT sp1;
                    
                    UPDATE {table_name} SET c0 = p_value WHERE id = p_id;
                    
                    IF ROW_COUNT() = 0 THEN
                        ROLLBACK TO sp1;
                        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = '更新失败';
                    END IF;
                    
                    OPEN cur;
                    read_loop: LOOP
                        FETCH cur INTO v_id, v_value;
                        IF v_done THEN
                            LEAVE read_loop;
                        END IF;
                        
                        SAVEPOINT sp2;
                        
                        INSERT INTO {target_table} (c0, c1)
                        VALUES (v_value * 2, CONCAT('From SP: ', v_id));
                        
                        IF (v_value * 2) > 10000 THEN
                            ROLLBACK TO sp2;
                            ITERATE read_loop;
                        END IF;
                    END LOOP;
                    
                    CLOSE cur;
                    
                    IF NOT v_error_occurred THEN
                        COMMIT;
                    END IF;
                END"""
                
                # 2. 创建带有递归的存储过程
                proc_name_recursive = f"sp_recursive_{table_name}"
                proc_sql_recursive = f"""
                CREATE PROCEDURE {proc_name_recursive}(IN p_id INT, IN p_depth INT)
                BEGIN
                    DECLARE v_current_value INT;
                    DECLARE v_next_id INT;
                    
                    IF p_depth <= 0 THEN
                        RETURN;
                    END IF;
                    
                    START TRANSACTION;
                    
                    SELECT c0 INTO v_current_value 
                    FROM {table_name} 
                    WHERE id = p_id;
                    
                    IF v_current_value IS NOT NULL THEN
                        UPDATE {table_name}
                        SET c0 = v_current_value * 2
                        WHERE id = p_id;
                        
                        SELECT MIN(id) INTO v_next_id
                        FROM {table_name}
                        WHERE id > p_id;
                        
                        IF v_next_id IS NOT NULL THEN
                            CALL {proc_name_recursive}(v_next_id, p_depth - 1);
                        END IF;
                    END IF;
                    
                    COMMIT;
                END"""
                
                try:
                    # 执行存储过程创建
                    self.cursor.execute(proc_sql)
                    self.cursor.execute(proc_sql_recursive)
                    logger.info(f"创建复杂存储过程: {proc_name}, {proc_name_recursive}")
                except Exception as e:
                    logger.error(f"创建复杂存储过程时出错: {e}")
                    print(f"创建复杂存储过程时出错: {e}")
            
        except Exception as e:
            logger.error(f"创建复杂存储过程整体失败: {e}")
            print(f"创建复杂存储过程整体失败: {e}")

    def commit_and_close(self):
        """提交更改并关闭游标"""
        try:
            self.conn.commit()
        except Exception as e:
            print(f"Error committing changes: {e}")
        finally:
            if self.cursor:
                self.cursor.close()


