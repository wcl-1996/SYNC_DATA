from flask import Flask, render_template, request, jsonify, send_from_directory
from flask_cors import CORS
import threading
import datetime
from dateutil import parser
import mysql.connector
from mysql.connector import Error
import os

app = Flask(__name__, static_folder='static')
CORS(app)

# 数据库类型名称映射
DB_TYPE_NAMES = {
    'mysql': 'MySQL',
    'oracle': 'Oracle',
    'dameng': '达梦',
    'postgresql': 'PostgreSQL',
    'sqlserver': 'SQL Server',
    'redis': 'Redis',
    'sqlite': 'SQLite'
}

sync_status = {
    'running': False,
    'progress': 0,
    'message': '',
    'total_inserted': 0,
    'message_id': 0,
    'logs': []
}

def add_log(message):
    global sync_status
    sync_status['message_id'] += 1
    sync_status['message'] = message
    sync_status['logs'].append(message)

class DBConnector:
    def __init__(self, db_type='mysql'):
        self.connection = None
        self.cursor = None
        self.db_type = db_type

    def connect(self, host, port, database, user, password, db_type=None):
        if db_type:
            self.db_type = db_type
        
        try:
            if self.db_type == 'mysql':
                connect_params = {
                    'host': host,
                    'port': int(port),
                    'user': user,
                    'password': password,
                    'connect_timeout': 10,
                    'charset': 'utf8mb4'
                }
                if database and database.strip():
                    connect_params['database'] = database.strip()
                self.connection = mysql.connector.connect(**connect_params)
                if self.connection.is_connected():
                    self.cursor = self.connection.cursor(dictionary=True)
                    return {'success': True}
            
            elif self.db_type == 'oracle':
                # Oracle 连接 (需要安装 cx_Oracle)
                try:
                    import cx_Oracle
                    dsn = cx_Oracle.makedsn(host, port, service_name=database or 'ORCL')
                    self.connection = cx_Oracle.connect(user, password, dsn)
                    self.cursor = self.connection.cursor()
                    return {'success': True}
                except ImportError:
                    return {'success': False, 'error': 'Oracle 驱动未安装，请安装 cx_Oracle'}
            
            elif self.db_type == 'dameng':
                # 达梦数据库连接 (需要安装 dmPython)
                try:
                    import dmPython
                    self.connection = dmPython.connect(
                        user=user,
                        password=password,
                        server=host,
                        port=int(port)
                    )
                    self.cursor = self.connection.cursor()
                    return {'success': True}
                except ImportError:
                    return {'success': False, 'error': '达梦驱动未安装，请安装 dmPython'}
            
            elif self.db_type == 'postgresql':
                # PostgreSQL 连接 (需要安装 psycopg2)
                try:
                    import psycopg2
                    self.connection = psycopg2.connect(
                        host=host,
                        port=int(port),
                        database=database,
                        user=user,
                        password=password
                    )
                    self.cursor = self.connection.cursor()
                    return {'success': True}
                except ImportError:
                    return {'success': False, 'error': 'PostgreSQL 驱动未安装，请安装 psycopg2'}
            
            elif self.db_type == 'sqlserver':
                # SQL Server 连接 (需要安装 pyodbc)
                try:
                    import pyodbc
                    conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={host},{port};DATABASE={database};UID={user};PWD={password}'
                    self.connection = pyodbc.connect(conn_str)
                    self.cursor = self.connection.cursor()
                    return {'success': True}
                except ImportError:
                    return {'success': False, 'error': 'SQL Server 驱动未安装，请安装 pyodbc'}
            
            elif self.db_type == 'redis':
                # Redis 连接 (需要安装 redis)
                try:
                    import redis
                    self.connection = redis.Redis(
                        host=host,
                        port=int(port) if port else 6379,
                        password=password if password else None,
                        db=int(database) if database else 0,
                        decode_responses=True
                    )
                    self.connection.ping()  # 测试连接
                    self.cursor = None  # Redis 不需要 cursor
                    return {'success': True}
                except ImportError:
                    return {'success': False, 'error': 'Redis 驱动未安装，请安装 redis'}
                except Exception as e:
                    return {'success': False, 'error': f'Redis 连接失败: {str(e)}'}
            
            elif self.db_type == 'sqlite':
                # SQLite 连接 (Python 内置支持)
                try:
                    import sqlite3
                    # host 参数作为文件路径
                    self.connection = sqlite3.connect(host if host else ':memory:')
                    self.connection.row_factory = sqlite3.Row
                    self.cursor = self.connection.cursor()
                    return {'success': True}
                except Exception as e:
                    return {'success': False, 'error': f'SQLite 连接失败: {str(e)}'}
            
            else:
                return {'success': False, 'error': f'不支持的数据库类型: {self.db_type}'}
                
        except Exception as e:
            return {'success': False, 'error': str(e)}
        
        return {'success': False}

    def disconnect(self):
        if self.connection:
            if self.db_type == 'mysql' and self.connection.is_connected():
                self.cursor.close()
                self.connection.close()
            else:
                try:
                    self.cursor.close()
                    self.connection.close()
                except:
                    pass

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
        try:
            if self.db_type == 'mysql':
                query = "SHOW DATABASES"
                result = self.fetch_all(query)
                return [row['Database'] for row in result]
            elif self.db_type == 'oracle':
                query = "SELECT name FROM v$database"
                result = self.fetch_all(query)
                return [row[0] for row in result]
            elif self.db_type == 'postgresql':
                query = "SELECT datname FROM pg_database WHERE datistemplate = false"
                result = self.fetch_all(query)
                return [row[0] for row in result]
            elif self.db_type == 'sqlserver':
                query = "SELECT name FROM sys.databases WHERE database_id > 4"
                result = self.fetch_all(query)
                return [row[0] for row in result]
            elif self.db_type == 'sqlite':
                return ['main']
            elif self.db_type == 'redis':
                return ['0']
            elif self.db_type == 'dameng':
                query = "SELECT name FROM v$database"
                result = self.fetch_all(query)
                return [row[0] for row in result]
            else:
                return []
        except Exception as e:
            print(f"获取数据库列表失败: {e}")
            return []

    def get_tables(self, database=None):
        try:
            if self.db_type == 'mysql':
                if database:
                    self.execute_query(f"USE {database}")
                query = "SHOW TABLES"
                result = self.fetch_all(query)
                return [list(row.values())[0] for row in result]
            elif self.db_type == 'oracle':
                query = "SELECT table_name FROM user_tables"
                result = self.fetch_all(query)
                return [row[0] for row in result]
            elif self.db_type == 'postgresql':
                query = "SELECT tablename FROM pg_tables WHERE schemaname='public'"
                result = self.fetch_all(query)
                return [row[0] for row in result]
            elif self.db_type == 'redis':
                # Redis 返回所有 keys
                return self.connection.keys('*')
            elif self.db_type == 'sqlite':
                query = "SELECT name FROM sqlite_master WHERE type='table'"
                result = self.fetch_all(query)
                return [row['name'] for row in result]
            else:
                return []
        except Exception as e:
            return []

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

    def delete_table_data(self, table_name, condition=None):
        if condition:
            query = f"DELETE FROM {table_name} WHERE {condition}"
        else:
            query = f"DELETE FROM {table_name}"
        return self.execute_update(query)

def sync_data(source_config, target_config, table_mappings, sync_options):
    global sync_status
    sync_status['running'] = True
    sync_status['progress'] = 0
    sync_status['message'] = ''
    sync_status['total_inserted'] = 0
    sync_status['message_id'] = 0
    sync_status['logs'] = []

    source_conn = DBConnector()
    target_conn = DBConnector()
    
    start_time = datetime.datetime.now()
    now = start_time.strftime('%Y-%m-%d %H:%M:%S')
    add_log(f"[{now}] ====== 数据同步任务开始 ======")

    source_result = source_conn.connect(
        source_config['host'],
        source_config['port'],
        source_config['database'],
        source_config['user'],
        source_config['password']
    )
    
    if not source_result['success']:
        add_log(f"Source DB connection failed: {source_result.get('error', '')}")
        sync_status['running'] = False
        return

    target_result = target_conn.connect(
        target_config['host'],
        target_config['port'],
        target_config['database'],
        target_config['user'],
        target_config['password']
    )
    
    if not target_result['success']:
        add_log(f"Target DB connection failed: {target_result.get('error', '')}")
        source_conn.disconnect()
        sync_status['running'] = False
        return

    batch_size = sync_options.get('batch_size', 1000)
    time_format = sync_options.get('time_format', "%Y-%m-%d %H:%M:%S")
    delete_before_insert = sync_options.get('delete_before_insert', False)
    delete_condition = sync_options.get('delete_condition')
    sql_where = sync_options.get('sql_where')
    
    add_partition = sync_options.get('add_partition', False)
    partition_date = sync_options.get('partition_date')
    partition_name = sync_options.get('partition_name')
    partition_field = sync_options.get('partition_field')
    
    enable_vmonth = sync_options.get('enable_vmonth', False)
    source_time_field = sync_options.get('source_time_field')
    target_month_field = sync_options.get('target_month_field', 'v_month')
    target_month_value = sync_options.get('target_month_value')

    total_records = 0
    total_inserted = 0

    for mapping in table_mappings:
        source_table = mapping['source_table']
        target_table = mapping['target_table']
        column_mappings = mapping.get('column_mappings')

        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        add_log(f"[{now}] ------------------------------")
        add_log(f"[{now}] 处理表映射: {source_table} -> {target_table}")

        table_start_time = datetime.datetime.now()
        
        count_query = f"SELECT COUNT(*) as count FROM {source_table}"
        combined_where = []
        if sql_where:
            combined_where.append(sql_where)
        
        if enable_vmonth and source_time_field and target_month_value:
            year = target_month_value[:4]
            month = target_month_value[4:]
            vmonth_condition = f"{source_time_field} >= '{year}-{month}-01 00:00:00' AND {source_time_field} < '{year}-{str(int(month)+1).zfill(2)}-01 00:00:00'"
            combined_where.append(vmonth_condition)
        
        if combined_where:
            count_query += " WHERE " + " AND ".join(combined_where)
        
        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        add_log(f"[{now}] 执行计数查询: {count_query}")
        
        count_result = source_conn.fetch_one(count_query)
        table_count = count_result['count'] if count_result else 0
        total_records += table_count

        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        add_log(f"[{now}] 来源表: {source_table}, 目标表: {target_table}, 数据条数: {table_count}")
        
        if table_count == 0:
            add_log(f"[{now}] 警告: 来源表查询返回0条数据")
            add_log(f"[{now}] SQL条件: {sql_where or '无'}")
            add_log(f"[{now}] v_month筛选: {'启用' if (enable_vmonth and source_time_field and target_month_value) else '未启用'}")
            if enable_vmonth:
                add_log(f"[{now}] 时间字段: {source_time_field}, 月份值: {target_month_value}")
        
        if enable_vmonth and source_time_field and target_month_value:
            now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            add_log(f"[{now}] [{source_table}] 启用 v_month 填充: {target_month_field} = {target_month_value}")
        
        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        add_log(f"[{now}] [{source_table}] -> [{target_table}] 开始同步...")

        if delete_before_insert:
            now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            add_log(f"[{now}] 正在删除 {target_table} 中的现有数据...")
            if delete_condition:
                add_log(f"[{now}] 删除条件: {delete_condition}")
                target_conn.delete_table_data(target_table, delete_condition)
            else:
                target_conn.delete_table_data(target_table)

        if not target_conn.table_exists(target_table):
            now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            add_log(f"[{now}] 目标表不存在，正在创建 {target_table}...")
            if column_mappings:
                create_table_with_columns(target_conn, source_conn, source_table, target_table, column_mappings)
            else:
                target_conn.create_table(source_conn, source_table, target_table)

        if add_partition and partition_name and partition_field and partition_date:
            now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            add_log(f"[{now}] 正在为 {target_table} 添加分区 {partition_name}...")
            add_log(f"[{now}] 分区字段: {partition_field}, 分区日期: {partition_date}")
            add_partition_to_table(target_conn, target_table, partition_name, partition_field, partition_date)

        target_schema = target_conn.get_table_schema(target_table)
        required_fields = [col['field'] for col in target_schema if col.get('null') == 'NO' and col.get('default') is None and col.get('field') != 'id']

        if column_mappings:
            mapped_fields = [cm['target_field'] for cm in column_mappings]
            missing_required = [f for f in required_fields if f not in mapped_fields]
            if missing_required:
                now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                add_log(f"[{now}] 错误: 目标表 {target_table} 缺少必填字段映射: {', '.join(missing_required)}")
                source_conn.disconnect()
                target_conn.disconnect()
                sync_status['running'] = False
                return

        add_log(f"[{source_table}] 开始数据同步...")

        offset = 0
        batch_num = 0
        while offset < table_count:
            fetch_query = f"SELECT * FROM {source_table}"
            if combined_where:
                fetch_query += " WHERE " + " AND ".join(combined_where)
            fetch_query += " LIMIT %s OFFSET %s"

            add_log(f"[{source_table}] 执行查询: {fetch_query} (参数: {batch_size}, {offset})")
            
            batch_data = source_conn.fetch_all(fetch_query, (batch_size, offset))
            batch_num += 1
            
            now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            add_log(f"[{now}] [{source_table}] 第 {batch_num} 批获取数据: {len(batch_data)} 条")
            
            if not batch_data:
                add_log(f"[{source_table}] 无更多数据，退出循环")
                break

            if column_mappings:
                add_log(f"[{source_table}] 应用字段映射，共 {len(column_mappings)} 个映射")
                add_log(f"[{source_table}] v_month配置 - enable_vmonth: {enable_vmonth}, target_month_field: {target_month_field}, target_month_value: {target_month_value}")
                add_log(f"[{source_table}] 分区配置 - add_partition: {add_partition}, partition_field: {partition_field}, partition_date: {partition_date}")
                
                mapped_fields = [cm['target_field'] for cm in column_mappings]
                mapped_data = []
                for row in batch_data:
                    new_row = {}
                    for cm in column_mappings:
                        src_col = cm['source_field']
                        tgt_col = cm['target_field']
                        
                        if src_col.startswith('{') and src_col.endswith('}'):
                            var_name = src_col[1:-1]
                            new_row[tgt_col] = cm.get('default_value', '')
                        elif src_col in row:
                            new_row[tgt_col] = row[src_col]
                        elif cm.get('default_value'):
                            new_row[tgt_col] = cm['default_value']
                    
                    if enable_vmonth and target_month_value and target_month_field and target_month_field not in mapped_fields:
                        new_row[target_month_field] = target_month_value
                        add_log(f"[{source_table}] v_month已添加到行数据: {target_month_field} = {target_month_value}")
                    
                    if add_partition and partition_field and partition_date:
                        new_row[partition_field] = partition_date
                        add_log(f"[{source_table}] 分区字段已添加: {partition_field} = {new_row[partition_field]}")
                    
                    mapped_data.append(new_row)
                batch_data = mapped_data
                add_log(f"[{source_table}] 映射后数据: {len(batch_data)} 条")
                
                if batch_data:
                    add_log(f"[{source_table}] 映射后字段列表: {list(batch_data[0].keys())}")
            
            elif enable_vmonth and target_month_value:
                mapped_data = []
                for row in batch_data:
                    row[target_month_field] = target_month_value
                    mapped_data.append(row)
                batch_data = mapped_data
                add_log(f"[{source_table}] 添加 v_month 字段: {len(batch_data)} 条")

            if batch_data:
                columns = list(batch_data[0].keys())
                placeholders = ', '.join(['%s'] * len(columns))
                columns_str = ', '.join(columns)

                add_log(f"[{source_table}] 插入字段: {columns_str}")

                values = []
                for row in batch_data:
                    row_values = []
                    for col in columns:
                        val = row.get(col)
                        if isinstance(val, datetime.datetime) or isinstance(val, datetime.date):
                            row_values.append(val.strftime(time_format))
                        else:
                            row_values.append(val)
                    values.append(tuple(row_values))
                
                add_log(f"[{source_table}] 准备插入 {len(values)} 条记录")

                insert_query = f"INSERT INTO {target_table} ({columns_str}) VALUES ({placeholders})"
                add_log(f"[{source_table}] 执行插入: {insert_query}")
                
                try:
                    target_conn.cursor.executemany(insert_query, values)
                    target_conn.connection.commit()
                    inserted = len(values)
                    total_inserted += inserted
                    now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    add_log(f"[{now}] [{source_table}] -> [{target_table}] 成功插入 {inserted} 条记录，累计: {total_inserted}")
                except Exception as e:
                    target_conn.connection.rollback()
                    now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    add_log(f"[{now}] [{source_table}] -> [{target_table}] 插入数据时出错: {str(e)}")
                    source_conn.disconnect()
                    target_conn.disconnect()
                    sync_status['running'] = False
                    return

            offset += batch_size
            sync_status['progress'] = int((total_inserted / max(total_records, 1)) * 100)
            sync_status['total_inserted'] = total_inserted
            now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            add_log(f"[{now}] [{source_table}] -> [{target_table}] 进度: {sync_status['progress']}%，已处理 {offset}/{table_count} 条")

        table_end_time = datetime.datetime.now()
        table_duration = (table_end_time - table_start_time).total_seconds()
        now = table_end_time.strftime('%Y-%m-%d %H:%M:%S')
        add_log(f"[{now}] 表同步完成: {source_table} -> {target_table}")
        add_log(f"[{now}] 来源条数: {table_count}, 插入条数: {total_inserted}")
        add_log(f"[{now}] 耗时: {table_duration:.2f} 秒")

    source_conn.disconnect()
    target_conn.disconnect()
    
    end_time = datetime.datetime.now()
    total_duration = (end_time - start_time).total_seconds()
    now = end_time.strftime('%Y-%m-%d %H:%M:%S')
    
    add_log(f"[{now}] ------------------------------")
    add_log(f"[{now}] ====== 数据同步任务完成 ======")
    add_log(f"[{now}] 开始时间: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    add_log(f"[{now}] 结束时间: {now}")
    add_log(f"[{now}] 总耗时: {total_duration:.2f} 秒")
    add_log(f"[{now}] 来源表总条数: {total_records}")
    sync_status['message'] = f"[{now}] 成功插入总条数: {total_inserted}"
    sync_status['progress'] = 100
    sync_status['running'] = False

def create_table_with_columns(target_conn, source_conn, source_table, target_table, column_mappings):
    source_schema = source_conn.get_table_schema(source_table)
    
    create_sql = f"CREATE TABLE IF NOT EXISTS {target_table} ("
    columns = []
    
    for cm in column_mappings:
        src_col = cm['source_field']
        tgt_col = cm['target_field']
        field_type = cm.get('field_type')
        
        if not field_type:
            src_field = next((s for s in source_schema if s['field'] == src_col), None)
            field_type = src_field['type'] if src_field else 'VARCHAR(255)'
        
        col_def = f"{tgt_col} {field_type}"
        
        if cm.get('default_value'):
            col_def += f" DEFAULT '{cm['default_value']}'"
        
        columns.append(col_def)
    
    create_sql += ', '.join(columns)
    create_sql += ')'
    
    target_conn.execute_update(create_sql)

def add_partition_to_table(target_conn, table_name, partition_name, partition_field, partition_date):
    year = partition_date[:4]
    month = partition_date[4:]
    next_month = str(int(month) + 1).zfill(2)
    next_year = year
    
    if next_month == '13':
        next_month = '01'
        next_year = str(int(year) + 1)
    
    next_partition_value = f"{next_year}{next_month}"
    
    try:
        partitions = target_conn.fetch_all(f"SHOW PARTITIONS FROM {table_name}")
        partition_names = [p['Partition'] for p in partitions]
        if partition_name in partition_names:
            add_log(f"分区 {partition_name} 已存在，跳过添加")
            return
    except Exception as e:
        add_log(f"检查分区是否存在时出错: {str(e)}")
    
    partition_sql = f"""
        ALTER TABLE {table_name} 
        ADD PARTITION (
            PARTITION {partition_name} 
            VALUES LESS THAN ('{next_partition_value}')
        )
    """
    target_conn.execute_update(partition_sql)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/migration')
def migration():
    return render_template('migration.html')

@app.route('/api/connect', methods=['POST'])
def connect_db():
    data = request.json
    db_type = data.get('db_type', 'mysql')
    conn = DBConnector(db_type)
    result = conn.connect(
        data['host'],
        data['port'],
        data['database'],
        data['user'],
        data['password'],
        db_type
    )
    
    if result['success']:
        dbs = conn.get_databases()
        conn.disconnect()
        return jsonify({'success': True, 'databases': dbs})
    return jsonify(result)

@app.route('/api/get_tables', methods=['POST'])
def get_tables():
    data = request.json
    db_type = data.get('db_type', 'mysql')
    conn = DBConnector(db_type)
    result = conn.connect(
        data['host'],
        data['port'],
        data['database'],
        data['user'],
        data['password'],
        db_type
    )
    
    if result['success']:
        tables = conn.get_tables()
        conn.disconnect()
        return jsonify({'success': True, 'tables': tables})
    return jsonify(result)

@app.route('/api/get_schema', methods=['POST'])
def get_schema():
    data = request.json
    db_type = data.get('db_type', 'mysql')
    conn = DBConnector(db_type)
    result = conn.connect(
        data['host'],
        data['port'],
        data['database'],
        data['user'],
        data['password'],
        db_type
    )
    
    if result['success']:
        schema = conn.get_table_schema(data['table'])
        conn.disconnect()
        return jsonify({'success': True, 'schema': schema})
    return jsonify(result)

@app.route('/api/sync', methods=['POST'])
def start_sync():
    global sync_status
    if sync_status['running']:
        return jsonify({'success': False, 'message': 'Sync already running'})

    data = request.json
    source_config = data['source']
    target_config = data['target']
    table_mappings = data['tableMappings']
    sync_options = data['options']

    thread = threading.Thread(target=sync_data, args=(source_config, target_config, table_mappings, sync_options))
    thread.start()

    return jsonify({'success': True, 'message': 'Sync started'})

@app.route('/api/sync_status', methods=['GET'])
def get_sync_status():
    return jsonify(sync_status)

@app.route('/api/sync_cancel', methods=['POST'])
def cancel_sync():
    global sync_status
    if sync_status['running']:
        sync_status['running'] = False
        add_log('同步任务已取消')
        return jsonify({'success': True, 'message': 'Sync cancelled'})
    else:
        return jsonify({'success': False, 'message': 'No sync running'})

# ==================== 钉钉推送功能 ====================
import hmac
import hashlib
import base64
import time
import urllib.parse
import requests

@app.route('/api/dingtalk/test', methods=['POST'])
def test_dingtalk():
    """测试钉钉机器人连接"""
    try:
        data = request.json
        webhook = data.get('webhook', '')
        secret = data.get('secret', '')
        
        if not webhook:
            return jsonify({'success': False, 'error': 'Webhook 地址不能为空'})
        
        # 发送测试消息
        message = {
            "msgtype": "text",
            "text": {
                "content": "【数据迁移系统】连接测试成功！\n测试时间: " + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
        }
        
        result = send_dingtalk_message(webhook, secret, message)
        return jsonify(result)
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/dingtalk/push', methods=['POST'])
def push_to_dingtalk():
    """推送消息到钉钉"""
    try:
        config = request.json
        webhook = config.get('webhook', '')
        secret = config.get('secret', '')
        push_method = config.get('pushMethod', 'text')
        template = config.get('template', '')
        data = config.get('data', {})
        
        if not webhook:
            return jsonify({'success': False, 'error': 'Webhook 地址不能为空'})
        
        # 替换模板变量
        content = template
        content = content.replace('{source_table}', str(data.get('source_table', '')))
        content = content.replace('{target_table}', str(data.get('target_table', '')))
        content = content.replace('{count}', str(data.get('count', 0)))
        content = content.replace('{doc_url}', str(data.get('doc_url', '')))
        content = content.replace('{time}', str(data.get('time', '')))
        
        # 根据推送方式构建消息
        if push_method == 'markdown':
            message = {
                "msgtype": "markdown",
                "markdown": {
                    "title": "数据迁移通知",
                    "text": content
                }
            }
        elif push_method == 'link':
            doc_url = data.get('doc_url', '')
            message = {
                "msgtype": "link",
                "link": {
                    "title": "数据迁移完成",
                    "text": content,
                    "messageUrl": doc_url if doc_url else "https://www.dingtalk.com",
                    "picUrl": ""
                }
            }
        else:  # text
            message = {
                "msgtype": "text",
                "text": {
                    "content": content
                }
            }
        
        result = send_dingtalk_message(webhook, secret, message)
        return jsonify(result)
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

def send_dingtalk_message(webhook, secret, message):
    """发送钉钉消息"""
    try:
        url = webhook
        
        # 如果有加签密钥，计算签名
        if secret:
            timestamp = str(round(time.time() * 1000))
            string_to_sign = timestamp + '\n' + secret
            hmac_code = hmac.new(
                secret.encode('utf-8'),
                string_to_sign.encode('utf-8'),
                digestmod=hashlib.sha256
            ).digest()
            sign = urllib.parse.quote_plus(base64.b64encode(hmac_code))
            url = f"{webhook}&timestamp={timestamp}&sign={sign}"
        
        response = requests.post(url, json=message, timeout=10)
        result = response.json()
        
        if result.get('errcode') == 0:
            return {'success': True, 'message': '发送成功'}
        else:
            return {'success': False, 'error': result.get('errmsg', '未知错误')}
    except Exception as e:
        return {'success': False, 'error': str(e)}

@app.route('/api/tencent_doc/upload', methods=['POST'])
def upload_to_tencent_doc():
    """上传数据到腾讯文档（预留接口）"""
    try:
        data = request.json
        app_id = data.get('appId', '')
        app_secret = data.get('appSecret', '')
        content = data.get('content', '')
        
        # TODO: 实现腾讯文档 API 调用
        # 需要申请腾讯文档 API 权限
        
        return jsonify({
            'success': False, 
            'error': '腾讯文档功能暂未实现，请联系管理员开通 API 权限'
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

# ==================== SQL执行API ====================
@app.route('/api/sql/execute', methods=['POST'])
def execute_sql():
    """执行SQL查询"""
    try:
        data = request.json
        sql = data.get('sql', '')
        db_config = data.get('db_config', {})
        
        if not sql:
            return jsonify({'success': False, 'error': 'SQL语句不能为空'})
        
        if not db_config.get('host'):
            return jsonify({'success': False, 'error': '请先配置并连接数据库'})
        
        # 创建数据库连接
        db_type = db_config.get('db_type', 'mysql')
        connector = DBConnector(
            host=db_config.get('host'),
            port=int(db_config.get('port', 3306)),
            user=db_config.get('user'),
            password=db_config.get('password'),
            database=db_config.get('database'),
            db_type=db_type
        )
        
        # 执行查询
        conn = connector.connect()
        if not conn:
            return jsonify({'success': False, 'error': '数据库连接失败'})
        
        cursor = conn.cursor(dictionary=True) if db_type == 'mysql' else conn.cursor()
        cursor.execute(sql)
        
        # 获取结果
        if cursor.description:
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            
            # 格式化数据
            if db_type == 'mysql':
                formatted_rows = rows
            else:
                formatted_rows = [dict(zip(columns, row)) for row in rows]
            
            cursor.close()
            connector.close()
            
            return jsonify({
                'success': True,
                'columns': columns,
                'rows': formatted_rows,
                'count': len(formatted_rows)
            })
        else:
            # 非 SELECT 语句
            affected = cursor.rowcount
            conn.commit()
            cursor.close()
            connector.close()
            
            return jsonify({
                'success': True,
                'affected_rows': affected,
                'message': f'执行成功，影响 {affected} 行'
            })
            
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

# ==================== 定时任务API ====================
# 任务存储（生产环境应使用数据库）
scheduled_tasks = {}
task_scheduler = None

def init_scheduler():
    """初始化定时任务调度器"""
    global task_scheduler
    try:
        from apscheduler.schedulers.background import BackgroundScheduler
        from apscheduler.triggers.cron import CronTrigger
        
        task_scheduler = BackgroundScheduler()
        task_scheduler.start()
        return True
    except ImportError:
        print("警告: APScheduler 未安装，定时任务功能不可用")
        return False

@app.route('/api/task/create', methods=['POST'])
def create_task():
    """创建定时任务"""
    try:
        data = request.json
        task_id = data.get('id')
        name = data.get('name')
        cron = data.get('cron')
        task_type = data.get('type', 'sql_push')
        sql = data.get('sql', '')
        webhook = data.get('webhook')
        secret = data.get('secret', '')
        
        if not all([task_id, name, cron, webhook]):
            return jsonify({'success': False, 'error': '缺少必要参数'})
        
        # 保存任务信息
        scheduled_tasks[task_id] = {
            'id': task_id,
            'name': name,
            'cron': cron,
            'type': task_type,
            'sql': sql,
            'webhook': webhook,
            'secret': secret,
            'status': 'active'
        }
        
        # 如果调度器可用，添加定时任务
        if task_scheduler:
            try:
                from apscheduler.triggers.cron import CronTrigger
                
                trigger = CronTrigger.from_crontab(cron.replace(' ', '')[0:-2], timezone='Asia/Shanghai')
                
                task_scheduler.add_job(
                    execute_scheduled_task,
                    trigger,
                    id=str(task_id),
                    args=[task_id],
                    replace_existing=True
                )
            except Exception as e:
                print(f"添加定时任务失败: {e}")
        
        return jsonify({'success': True, 'task_id': task_id})
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

def execute_scheduled_task(task_id):
    """执行定时任务"""
    task = scheduled_tasks.get(task_id)
    if not task:
        return
    
    try:
        # 执行SQL查询
        if task['type'] == 'sql_push' and task['sql']:
            # TODO: 从配置获取数据库连接
            pass
        
        # 推送到钉钉
        message = {
            "msgtype": "text",
            "text": {
                "content": f"【定时任务执行】\n任务: {task['name']}\n时间: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            }
        }
        send_dingtalk_message(task['webhook'], task.get('secret', ''), message)
        
    except Exception as e:
        print(f"定时任务执行失败: {e}")

@app.route('/api/task/pause', methods=['POST'])
def pause_task():
    """暂停定时任务"""
    try:
        data = request.json
        task_id = data.get('task_id')
        
        if task_id in scheduled_tasks:
            scheduled_tasks[task_id]['status'] = 'paused'
            
            if task_scheduler:
                task_scheduler.pause_job(str(task_id))
        
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/task/resume', methods=['POST'])
def resume_task():
    """恢复定时任务"""
    try:
        data = request.json
        task_id = data.get('task_id')
        
        if task_id in scheduled_tasks:
            scheduled_tasks[task_id]['status'] = 'active'
            
            if task_scheduler:
                task_scheduler.resume_job(str(task_id))
        
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/task/delete', methods=['POST'])
def delete_task():
    """删除定时任务"""
    try:
        data = request.json
        task_id = data.get('task_id')
        
        if task_id in scheduled_tasks:
            del scheduled_tasks[task_id]
            
            if task_scheduler:
                task_scheduler.remove_job(str(task_id))
        
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/cron/validate', methods=['POST'])
def validate_cron():
    """验证Cron表达式"""
    try:
        data = request.json
        cron = data.get('cron', '')
        
        parts = cron.split()
        if len(parts) != 6:
            return jsonify({'success': False, 'error': 'Cron表达式需要6个部分（秒 分 时 日 月 星期）'})
        
        # 解析描述
        sec, minute, hour, day, month, week = parts
        desc = ''
        
        if week != '?' and week != '*':
            week_names = {'MON': '周一', 'TUE': '周二', 'WED': '周三', 'THU': '周四', 
                         'FRI': '周五', 'SAT': '周六', 'SUN': '周日'}
            desc += f"每{week_names.get(week, week)}"
        elif day != '*' and day != '?':
            desc += f"每月{day}号"
        else:
            desc = "每天"
        
        if hour != '*' and hour != '?':
            desc += f" {hour}点"
        if minute != '*' and minute != '?':
            desc += f"{minute}分"
        
        # 计算下次执行时间（简化版）
        next_run = "请参考任务列表中的下次执行时间"
        
        return jsonify({
            'success': True,
            'description': desc + '执行',
            'next_run': next_run
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

if __name__ == '__main__':
    import os
    import datetime
    
    # 初始化定时任务调度器
    init_scheduler()
    
    port = int(os.environ.get('DEPLOY_RUN_PORT', 5000))
    app.run(debug=False, host='0.0.0.0', port=port)