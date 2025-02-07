import mysql.connector
from generator import AtomicityChecker as AtomicityChecker

db_config = {
    "host": "localhost",
    "user": "root",
    "password": "123456",
    "database": "test",
    "port": 3308
}

def set_lock_wait_timeout(timeout):
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        cursor.execute(f"SET GLOBAL innodb_lock_wait_timeout = {timeout}")
        cursor.close()
        conn.close()
    except mysql.connector.Error as err:
        print(f"设置锁等待超时失败: {err}")

def _create_connection():
    """创建数据库连接"""
    try:
        return mysql.connector.connect(**db_config)
    except mysql.connector.Error as err:
        print(f"数据库连接失败: {err}")
        raise

def init_db(init_sql):
    """初始化数据库"""
    conn = None
    cursor = None
    try:
        conn = _create_connection()
        cursor = conn.cursor()
        
        # 确保每条SQL语句执行后都获取结果集
        for sql in init_sql:
            cursor.execute(sql)
            if sql.strip().upper().startswith('SELECT'):
                cursor.fetchall()  # 获取SELECT语句的结果集
        
        conn.commit()
        
    except Exception as e:
        print(f"初始化数据库失败: {e}")
        if conn:
            conn.rollback()
        raise
        
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# init_sql = [
# 'CREATE TABLE table_0 (id INT AUTO_INCREMENT PRIMARY KEY, col_0 DOUBLE, col_1 FLOAT, col_2 FLOAT);',
# 'INSERT INTO table_0 (id, col_0, col_1, col_2) VALUES (NULL, 89.46, 74.87, 61.17);',
# 'INSERT INTO table_0 (id, col_0, col_1, col_2) VALUES (NULL, 72.81, 9.67, 42.98);',
# 'INSERT INTO table_0 (id, col_0, col_1, col_2) VALUES (NULL, 30.04, 27.27, 72.28);',       
# 'INSERT INTO table_0 (id, col_0, col_1, col_2) VALUES (NULL, 48.39, 83.67, 37.47);',      
# 'INSERT INTO table_0 (id, col_0, col_1, col_2) VALUES (NULL, 71.78, 77.16, 4.91);',       
# 'INSERT INTO table_0 (id, col_0, col_1, col_2) VALUES (NULL, 64.15, 81.33, 65.88);',
# 'INSERT INTO table_0 (id, col_0, col_1, col_2) VALUES (NULL, 71.57, 89.38, 24.77);', 
# 'ALTER TABLE table_0 ADD COLUMN col_837 INT;',
# 'UPDATE table_0 SET col_0 = 39.6 WHERE col_0 = 88.26;',
# 'DELETE FROM table_0 WHERE col_1 = 53.56;',
# 'DELETE FROM table_0 WHERE col_837 = 18;'
# ]

# trx1 = [
# 'BEGIN', 
# 'SELECT id, col_0, col_1, col_2, col_837 FROM table_0 WHERE id BETWEEN 2 AND 6 FOR UPDATE', 
# # 'UPDATE table_0 SET col_1 = 98.25, col_0 = 12.91, col_837 = 96 WHERE (id = 5)', 
# # 'SELECT id, col_1, col_2 FROM table_0 WHERE id BETWEEN 6 AND 6 FOR UPDATE', 
# # 'SELECT col_0 FROM table_0 WHERE id BETWEEN 7 AND 7 LOCK IN SHARE MODE', 
# # 'COMMIT;'
# ]

# trx2 = [
# 'BEGIN', 
# 'SELECT id, col_837 FROM table_0 WHERE (id = 2) FOR UPDATE', 
# # 'SELECT * FROM table_0 WHERE (id = 3) FOR UPDATE', 
# # 'UPDATE table_0 SET col_2 = 36.99, col_1 = 95.79 WHERE (col_1 = 83.67)', 
# # 'UPDATE table_0 SET col_1 = 77.71, col_2 = 8.21 WHERE (col_837 IS NULL) AND (col_2 = 24.77)', 
# # 'INSERT IGNORE INTO table_0 (id, col_0, col_1, col_2, col_837) VALUES (NULL, 90.42, 16.87, 20.9, 13)', 
# # 'COMMIT;'
# ]

# serial = [
# (1, 'BEGIN'),
# (2, 'BEGIN'),
# (1, 'SELECT id, col_0, col_1, col_2, col_837 FROM table_0 WHERE id BETWEEN 2 AND 6 FOR UPDATE'), 
# (2, 'SELECT id, col_837 FROM table_0 WHERE (id = 2) FOR UPDATE'),
# (2, 'SELECT * FROM table_0 WHERE (id = 3) FOR UPDATE')
# # (2, 'UPDATE table_0 SET col_2 = 36.99, col_1 = 95.79 WHERE (col_1 = 83.67)'), 
# # (2, 'UPDATE table_0 SET col_1 = 77.71, col_2 = 8.21 WHERE (col_837 IS NULL) AND (col_2 = 24.77)'), 
# # (1, 'UPDATE table_0 SET col_1 = 98.25, col_0 = 12.91, col_837 = 96 WHERE (id = 5)'), 
# # (1, 'SELECT id, col_1, col_2 FROM table_0 WHERE id BETWEEN 6 AND 6 FOR UPDATE'), 
# # (1, 'SELECT col_0 FROM table_0 WHERE id BETWEEN 7 AND 7 LOCK IN SHARE MODE'), 
# # (2, 'INSERT IGNORE INTO table_0 (id, col_0, col_1, col_2, col_837) VALUES (NULL, 90.42, 16.87, 20.9, 13)'), 
# # (2, 'COMMIT'), 
# # (1, 'COMMIT')
# ]

init_sql = [
"DROP TABLE IF EXISTS `table_0`;",
"CREATE TABLE `table_0` (`id` int NOT NULL AUTO_INCREMENT,`col_0` varchar(255) DEFAULT NULL,`col_1` text,`col_2` varchar(255) DEFAULT NULL,`col_689` int DEFAULT NULL, PRIMARY KEY (`id`)) ENGINE=InnoDB AUTO_INCREMENT=15 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
# "LOCK TABLES `table_0` WRITE;",
"INSERT INTO `table_0` VALUES (1,'lrhsr','qqqhc','ufmdy',NULL),(2,'hhqgt','guqrp','obqry',NULL),(3,'ljwnq','outtd','bpyew',NULL),(4,'lenwm','ukqhh','nenaf',NULL),(5,'lopiy','nrnuu','hpfcg',NULL),(6,'idxnk','gjqcf','kvlgh',NULL),(7,'nfuhz','jmtye','qjphf',NULL),(8,'unzbk','dpxdb','gdide',NULL);",
# "UNLOCK TABLES;"
]


trx1 = [
    'BEGIN',
    "INSERT INTO table_0 (id, col_0, col_1, col_2, col_689) VALUES (NULL, 'irutA', 'TWHAK', 'BgPBG', 68)",
    "INSERT INTO table_0 (id, col_0, col_1, col_2, col_689) VALUES (NULL, 'wZjQw', 'UvGES', 'zBqBR', 71)",
    "INSERT INTO table_0 (id, col_0, col_1, col_2, col_689) VALUES (NULL, 'nQFnI', 'aMWZu', 'dCKuV', 54)",
    "SELECT * FROM table_0 WHERE (col_0 = 'nfuhz') FOR UPDATE",
    "COMMIT"
]

trx2 = [
    'BEGIN',
    'SELECT * FROM table_0 WHERE id >= 2 AND id <= 2 FOR UPDATE',
    'SELECT id, col_0, col_689 FROM table_0 WHERE id >= 7 AND id <= 7 FOR UPDATE',
    "SELECT id, col_0, col_2, col_689 FROM table_0 WHERE (col_0 = 'lopiy') AND (col_2 = 'hpfcg') AND (col_1 = 'nrnuu') FOR UPDATE",
    'COMMIT'
]

serial = [
    (1, 'BEGIN'),
    (2, 'BEGIN'),
    (2, "SELECT * FROM table_0 WHERE id >= 2 AND id <= 2 FOR UPDATE"),
    (1, "INSERT INTO table_0 (id, col_0, col_1, col_2, col_689) VALUES (NULL, 'irutA', 'TWHAK', 'BgPBG', 68)"),
    (2, 'SELECT id, col_0, col_689 FROM table_0 WHERE id >= 7 AND id <= 7 FOR UPDATE'),
    (1, "INSERT INTO table_0 (id, col_0, col_1, col_2, col_689) VALUES (NULL, 'wZjQw', 'UvGES', 'zBqBR', 71)"),
    (1, "INSERT INTO table_0 (id, col_0, col_1, col_2, col_689) VALUES (NULL, 'nQFnI', 'aMWZu', 'dCKuV', 54)"),
    (2, "SELECT id, col_0, col_2, col_689 FROM table_0 WHERE (col_0 = 'lopiy') AND (col_2 = 'hpfcg') AND (col_1 = 'nrnuu') FOR UPDATE"),
    (1, "SELECT * FROM table_0 WHERE (col_0 = 'nfuhz') FOR UPDATE"),
    (1, 'COMMIT'),
    (2, 'COMMIT')
]

try:
    set_lock_wait_timeout(timeout=10)
    init_db(init_sql)
    atomicity_checker = AtomicityChecker("localhost", "root", "123456", "test", 3308, trx1, trx2, serial)
    is_atomic, info = atomicity_checker.check_atomicity()
    print('is_atomic:', is_atomic)
    print()
    print('info:', info)
    print()
    print('executed_serial:', atomicity_checker.executed_serial)
    print('snapshots:', atomicity_checker.get_snapshots())
except Exception as e:
    print(e)