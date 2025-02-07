import random


class MySQLIndexGenerator:
    def __init__(self, schema, global_state):
        """
        :param schema: 数据库的 schema 信息
        :param global_state: 全局状态对象（包含随机生成器等）
        """
        self.schema = schema
        self.global_state = global_state
        self.query = []
        self.errors = []
        self.column_is_primary_key = False
        self.contains_inplace = False

    @staticmethod
    def create(global_state):
        """
        入口方法：生成随机的 CREATE INDEX 语句
        """
        schema = global_state["schema"]
        return MySQLIndexGenerator(schema, global_state).generate()

    def generate(self):
        """
        构建 CREATE INDEX 语句
        """
        self.query.append("CREATE")

        # 随机选择 UNIQUE 索引
        if random.choice([True, False]):
            self.query.append("UNIQUE")
            self.errors.append("Duplicate entry")

        self.query.append("INDEX")

        # 随机生成索引名称
        index_name = f"idx_{random.randint(1, 1000)}"
        self.query.append(index_name)

        # 随机选择索引类型
        self.index_type()

        # 随机选择表
        random_table = random.choice(self.schema["tables"])
        self.query.append("ON")
        self.query.append(random_table["name"])

        # 构建索引列
        self.query.append("(")
        if random_table["engine"] == "InnoDB" and random.choice([True, False]):
            # 基于表达式的索引
            for i in range(random.randint(1, 3)):
                if i > 0:
                    self.query.append(", ")
                expr = self.generate_expression()
                self.query.append(f"({expr})")
        else:
            # 普通列索引
            columns = random.sample(random_table["columns"], random.randint(1, len(random_table["columns"])))
            for i, column in enumerate(columns):
                if i > 0:
                    self.query.append(", ")
                self.query.append(column["name"])

                # 对字符串类型的列随机指定索引长度
                if column["type"] == "VARCHAR" and random.choice([True, False]):
                    self.query.append(f"({random.randint(1, 5)})")

                # 随机指定排序方向
                if random.choice([True, False]):
                    self.query.append(random.choice(["ASC", "DESC"]))
        self.query.append(")")

        # 添加索引选项
        self.index_option()

        # 添加算法选项
        self.algorithm_option()

        # 添加可能的错误
        self.add_index_errors()

        return " ".join(self.query)

    def index_type(self):
        """
        随机选择索引类型
        """
        if random.choice([True, False]):
            self.query.append("USING")
            self.query.append(random.choice(["BTREE", "HASH"]))

    def index_option(self):
        """
        随机选择索引可见性
        """
        if random.choice([True, False]):
            if self.column_is_primary_key:
                self.query.append("VISIBLE")
            else:
                self.query.append(random.choice(["VISIBLE", "INVISIBLE"]))

    def algorithm_option(self):
        """
        随机选择索引算法
        """
        if random.choice([True, False]):
            self.query.append("ALGORITHM")
            self.query.append("=" if random.choice([True, False]) else "")
            algorithm = random.choice(["DEFAULT", "INPLACE", "COPY"])
            self.query.append(algorithm)
            if algorithm == "INPLACE":
                self.contains_inplace = True

    def generate_expression(self):
        """
        随机生成表达式（模拟生成器）
        """
        return f"col_{random.randint(1, 10)} + {random.randint(1, 100)}"

    def add_index_errors(self):
        """
        添加可能的索引创建相关错误
        """
        self.errors.extend([
            "Duplicate entry",
            "ALGORITHM=INPLACE is not supported",
            "Table handler doesn't support NULL in given index.",
            "A primary key index cannot be invisible",
            "Functional index on a column is not supported",
            "Specified key was too long",
            "Row size too large",
            "Data truncation"
        ])
