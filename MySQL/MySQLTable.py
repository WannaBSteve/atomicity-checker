import random
from typing import List

class MySQLTableGenerator:
    def __init__(self, schema_tables: List[str]):
        self.schema_tables = schema_tables

    def get_random_boolean(self):
        return random.choice([True, False])

    def get_random_int(self, min_val, max_val):
        return random.randint(min_val, max_val)

    def get_random_choice(self, options):
        return random.choice(options)

    def get_random_subset(self, options, max_size=None):
        max_size = max_size if max_size is not None else len(options)
        return random.sample(options, random.randint(1, max_size))

    def create_table_statement(self, table_name: str) -> str:
        sb = []
        columns = []
        allow_primary_key = self.get_random_boolean()
        set_primary_key = False
        table_has_nullable_column = False
        keys_specified = 0

        # Start CREATE TABLE statement
        sb.append("CREATE TABLE")
        if self.get_random_boolean():
            sb.append("IF NOT EXISTS")
        sb.append(table_name)

        # Optional "LIKE" clause
        if self.get_random_boolean() and self.schema_tables:
            sb.append("LIKE")
            sb.append(self.get_random_choice(self.schema_tables))
            return " ".join(sb)

        sb.append("(")

        # Generate columns
        num_columns = 1 + self.get_random_int(0, 5)  # Random small number of columns
        for i in range(num_columns):
            if i != 0:
                sb.append(",")
            column_name = f"col{i}"
            columns.append(column_name)
            sb.append(column_name)
            sb.append(self.generate_column_definition(i, allow_primary_key, set_primary_key, table_has_nullable_column, keys_specified))

        sb.append(")")

        # Add table options
        sb.append(self.generate_table_options())

        return " ".join(sb)

    def generate_column_definition(self, column_id, allow_primary_key, set_primary_key, table_has_nullable_column, keys_specified):
        sb = []
        column_type = self.get_random_choice(["INT", "VARCHAR(500)", "FLOAT", "DOUBLE", "DECIMAL"])
        sb.append(column_type)

        if column_type == "DECIMAL":
            sb.append(self.generate_decimal_precision())

        column_options = self.get_random_subset([
            "NULL_OR_NOT_NULL", "UNIQUE", "PRIMARY_KEY", "COMMENT", "COLUMN_FORMAT", "STORAGE"
        ])

        for option in column_options:
            if option == "NULL_OR_NOT_NULL":
                if not set_primary_key:
                    if self.get_random_boolean():
                        sb.append("NULL")
                        table_has_nullable_column = True
                    else:
                        sb.append("NOT NULL")
            elif option == "UNIQUE":
                sb.append("UNIQUE")
                keys_specified += 1
                if self.get_random_boolean():
                    sb.append("KEY")
            elif option == "PRIMARY_KEY" and allow_primary_key and not set_primary_key:
                sb.append("PRIMARY KEY")
                set_primary_key = True
            elif option == "COMMENT":
                sb.append(f"COMMENT 'Random comment {column_id}'")
            elif option == "COLUMN_FORMAT":
                sb.append(f"COLUMN_FORMAT {self.get_random_choice(['FIXED', 'DYNAMIC', 'DEFAULT'])}")
            elif option == "STORAGE":
                sb.append(f"STORAGE {self.get_random_choice(['DISK', 'MEMORY'])}")

        return " ".join(sb)

    def generate_decimal_precision(self):
        if self.get_random_boolean():
            m = self.get_random_int(1, 65)
            d = self.get_random_int(1, min(m, 30))
            return f"({m},{d})"
        return ""

    def generate_table_options(self):
        sb = []
        table_options = self.get_random_subset([
            "AUTO_INCREMENT", "AVG_ROW_LENGTH", "CHECKSUM", "COMPRESSION", "DELAY_KEY_WRITE", "ENGINE",
            "INSERT_METHOD", "KEY_BLOCK_SIZE", "MAX_ROWS", "MIN_ROWS", "PACK_KEYS", "STATS_AUTO_RECALC", "STATS_PERSISTENT", "STATS_SAMPLE_PAGES"
        ], max_size=3)

        for option in table_options:
            if option == "AUTO_INCREMENT":
                sb.append(f"AUTO_INCREMENT = {self.get_random_int(1, 10000)}")
            elif option == "AVG_ROW_LENGTH":
                sb.append(f"AVG_ROW_LENGTH = {self.get_random_int(0, 4294967295)}")
            elif option == "CHECKSUM":
                sb.append("CHECKSUM = 1")
            elif option == "COMPRESSION":
                sb.append(f"COMPRESSION = '{self.get_random_choice(['ZLIB', 'LZ4', 'NONE'])}'")
            elif option == "DELAY_KEY_WRITE":
                sb.append(f"DELAY_KEY_WRITE = {self.get_random_choice([0, 1])}")
            elif option == "ENGINE":
                # sb.append(f"ENGINE = {self.get_random_choice(['InnoDB', 'MyISAM', 'MEMORY', 'CSV', 'ARCHIVE'])}")
                sb.append(f"ENGINE = InnoDB")
            elif option == "INSERT_METHOD":
                sb.append(f"INSERT_METHOD = {self.get_random_choice(['NO', 'FIRST', 'LAST'])}")
            elif option == "KEY_BLOCK_SIZE":
                sb.append(f"KEY_BLOCK_SIZE = {self.get_random_int(0, 65535)}")
            elif option == "MAX_ROWS":
                sb.append(f"MAX_ROWS = {self.get_random_int(0, 1000000)}")
            elif option == "MIN_ROWS":
                sb.append(f"MIN_ROWS = {self.get_random_int(1, 100000)}")
            elif option == "PACK_KEYS":
                sb.append(f"PACK_KEYS = {self.get_random_choice(['1', '0', 'DEFAULT'])}")
            elif option == "STATS_AUTO_RECALC":
                sb.append(f"STATS_AUTO_RECALC = {self.get_random_choice(['1', '0', 'DEFAULT'])}")
            elif option == "STATS_PERSISTENT":
                sb.append(f"STATS_PERSISTENT = {self.get_random_choice(['1', '0', 'DEFAULT'])}")
            elif option == "STATS_SAMPLE_PAGES":
                sb.append(f"STATS_SAMPLE_PAGES = {self.get_random_int(1, 32767)}")

        return ", ".join(sb)
