import random


class MySQLInsertGenerator:
    def __init__(self, global_state, table):
        """
        :param global_state: 全局状态对象（包含随机生成器、schema 等信息）
        :param table: 随机选择的表对象
        """
        self.global_state = global_state
        self.table = table
        self.query = []
        self.errors = []

    @staticmethod
    def insert_row(global_state):
        """
        入口方法：随机生成 INSERT 或 REPLACE 语句
        """
        table = random.choice(global_state["schema"]["tables"])
        if random.choice([True, False]):
            return MySQLInsertGenerator(global_state, table).generate_insert()
        else:
            return MySQLInsertGenerator(global_state, table).generate_replace()

    def generate_replace(self):
        """
        生成 REPLACE 语句
        """
        self.query.append("REPLACE")
        if random.choice([True, False]):
            self.query.append(random.choice(["LOW_PRIORITY", "DELAYED"]))
        return self.generate_into()

    def generate_insert(self):
        """
        生成 INSERT 语句
        """
        self.query.append("INSERT")
        if random.choice([True, False]):
            self.query.append(random.choice(["LOW_PRIORITY", "DELAYED", "HIGH_PRIORITY"]))
        if random.choice([True, False]):
            self.query.append("IGNORE")
        return self.generate_into()

    def generate_into(self):
        """
        构造 INTO 部分，包括随机列选择和随机值生成
        """
        self.query.append("INTO")
        self.query.append(self.table["name"])
        
        # 随机选择非空列子集
        columns = random.sample(self.table["columns"], random.randint(1, len(self.table["columns"])))
        self.query.append("(" + ", ".join(col["name"] for col in columns) + ")")
        self.query.append("VALUES")

        # 随机生成多行插入的数据
        num_rows = 1 if random.choice([True, False]) else random.randint(2, 5)
        for row_idx in range(num_rows):
            if row_idx > 0:
                self.query.append(", ")
            self.query.append("(")
            self.query.append(", ".join(self.generate_random_value(col) for col in columns))
            self.query.append(")")

        # 添加插入/更新相关的可能错误
        self.add_insert_update_errors()
        return "".join(self.query)

    def generate_random_value(self, column):
        """
        根据列类型生成随机值
        """
        data_type = column["type"]
        if data_type == "INT":
            return str(random.randint(0, 1000))
        elif data_type == "VARCHAR":
            return f"'{self.random_string()}'"
        elif data_type == "FLOAT":
            return str(round(random.uniform(0, 1000), 2))
        elif data_type == "DOUBLE":
            return str(round(random.uniform(0, 1000), 4))
        elif data_type == "DECIMAL":
            return str(round(random.uniform(0, 1000), 2))
        else:
            return "NULL"

    def random_string(self, length=10):
        """
        生成随机字符串
        """
        return ''.join(random.choices("abcdefghijklmnopqrstuvwxyz", k=length))

    def add_insert_update_errors(self):
        """
        添加插入/更新相关的错误到 errors 列表
        """
        self.errors.append("Duplicate entry")
        self.errors.append("Data truncated")
        self.errors.append("Cannot add or update a child row")

