import mysql.connector
from mysql.connector import Error

class DBConnector:
    def __init__(self):
        self.connection = None
        self.cursor = None

    def connect(self, host, port, database, user, password):
        try:
            self.connection = mysql.connector.connect(
                host=host,
                port=int(port),
                database=database,
                user=user,
                password=password
            )
            if self.connection.is_connected():
                self.cursor = self.connection.cursor(dictionary=True)
                return True
        except Error as e:
            return False
        return False

    def disconnect(self):
        if self.connection and self.connection.is_connected():
            self.cursor.close()
            self.connection.close()

    def execute_query(self, query, params=None):
        try:
            self.cursor.execute(query, params)
            return self.cursor
        except Error as e:
            return None

    def fetch_all(self, query, params=None):
        cursor = self.execute_query(query, params)
        if cursor:
            return cursor.fetchall()
        return []

    def fetch_one(self, query, params=None):
        cursor = self.execute_query(query, params)
        if cursor:
            return cursor.fetchone()
        return None

    def execute_update(self, query, params=None):
        try:
            self.cursor.execute(query, params)
            self.connection.commit()
            return self.cursor.rowcount
        except Error as e:
            self.connection.rollback()
            return -1

    def get_databases(self):
        query = "SHOW DATABASES"
        result = self.fetch_all(query)
        return [row['Database'] for row in result]

    def get_tables(self, database=None):
        if database:
            self.execute_query(f"USE {database}")
        query = "SHOW TABLES"
        result = self.fetch_all(query)
        return [list(row.values())[0] for row in result]

    def get_table_schema(self, table_name):
        query = f"DESCRIBE {table_name}"
        result = self.fetch_all(query)
        schema = []
        for row in result:
            schema.append({
                'field': row['Field'],
                'type': row['Type'],
                'null': row['Null'],
                'key': row['Key'],
                'default': row['Default'],
                'extra': row['Extra']
            })
        return schema

    def get_column_types(self, table_name):
        schema = self.get_table_schema(table_name)
        column_types = {}
        for col in schema:
            column_types[col['field']] = col['type']
        return column_types

    def get_time_columns(self, table_name):
        schema = self.get_table_schema(table_name)
        time_columns = []
        for col in schema:
            col_type = col['type'].lower()
            if 'datetime' in col_type or 'date' in col_type or 'timestamp' in col_type:
                time_columns.append(col['field'])
        return time_columns

    def table_exists(self, table_name):
        query = f"SHOW TABLES LIKE '{table_name}'"
        result = self.fetch_one(query)
        return result is not None

    def create_table(self, source_conn, source_table, target_table):
        source_schema = source_conn.get_table_schema(source_table)
        create_sql = f"CREATE TABLE IF NOT EXISTS {target_table} ("
        columns = []
        for col in source_schema:
            col_def = f"{col['field']} {col['type']}"
            if col['null'] == 'NO':
                col_def += ' NOT NULL'
            if col['key'] == 'PRI':
                col_def += ' PRIMARY KEY'
            if col['default'] is not None:
                col_def += f" DEFAULT '{col['default']}'"
            if col['extra']:
                col_def += f" {col['extra']}"
            columns.append(col_def)
        create_sql += ', '.join(columns)
        create_sql += ')'
        return self.execute_update(create_sql)

    def add_partition(self, table_name, partition_name, partition_condition):
        query = f"ALTER TABLE {table_name} ADD PARTITION (PARTITION {partition_name} VALUES {partition_condition})"
        return self.execute_update(query)

    def create_partitioned_table(self, source_conn, source_table, target_table, partition_column, partition_type='RANGE'):
        source_schema = source_conn.get_table_schema(source_table)
        create_sql = f"CREATE TABLE IF NOT EXISTS {target_table} ("
        columns = []
        for col in source_schema:
            col_def = f"{col['field']} {col['type']}"
            if col['null'] == 'NO':
                col_def += ' NOT NULL'
            if col['key'] == 'PRI':
                col_def += ' PRIMARY KEY'
            if col['default'] is not None:
                col_def += f" DEFAULT '{col['default']}'"
            if col['extra']:
                col_def += f" {col['extra']}"
            columns.append(col_def)
        create_sql += ', '.join(columns)
        create_sql += f") PARTITION BY {partition_type} ({partition_column})"
        return self.execute_update(create_sql)

    def delete_table_data(self, table_name, condition=None):
        if condition:
            query = f"DELETE FROM {table_name} WHERE {condition}"
        else:
            query = f"DELETE FROM {table_name}"
        return self.execute_update(query)