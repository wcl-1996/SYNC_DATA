import datetime
from dateutil import parser

class SyncEngine:
    def __init__(self):
        self.batch_size = 1000
        self.time_format = "%Y-%m-%d %H:%M:%S"
        self.delete_before_insert = False
        self.create_partition = False
        self.add_partition = False
        self.progress_callback = None
        self.status_callback = None

    def set_progress_callback(self, callback):
        self.progress_callback = callback

    def set_status_callback(self, callback):
        self.status_callback = callback

    def log_status(self, message):
        if self.status_callback:
            self.status_callback(message)

    def log_progress(self, current, total):
        if self.progress_callback:
            self.progress_callback(current, total)

    def format_time_value(self, value, target_format=None):
        if value is None:
            return None
        if target_format is None:
            target_format = self.time_format
        try:
            if isinstance(value, datetime.datetime):
                return value.strftime(target_format)
            elif isinstance(value, datetime.date):
                return value.strftime(target_format.split(' ')[0])
            elif isinstance(value, str):
                dt = parser.parse(value)
                return dt.strftime(target_format)
            else:
                return str(value)
        except:
            return str(value)

    def build_query(self, table_name, limit_field=None, limit_value=None, time_format=None):
        query = f"SELECT * FROM {table_name}"
        params = []
        if limit_field and limit_value:
            col_type = None
            try:
                parser.parse(str(limit_value))
                col_type = 'datetime'
            except:
                try:
                    int(limit_value)
                    col_type = 'int'
                except:
                    col_type = 'string'
            
            if col_type == 'datetime':
                formatted_value = self.format_time_value(limit_value, time_format)
                query += f" WHERE {limit_field} >= %s"
                params.append(formatted_value)
            else:
                query += f" WHERE {limit_field} >= %s"
                params.append(limit_value)
        return query, params

    def get_total_count(self, source_conn, table_name, limit_field=None, limit_value=None):
        query = f"SELECT COUNT(*) as count FROM {table_name}"
        params = []
        if limit_field and limit_value:
            query += f" WHERE {limit_field} >= %s"
            params.append(limit_value)
        result = source_conn.fetch_one(query, params)
        return result['count'] if result else 0

    def batch_fetch(self, source_conn, table_name, offset=0, limit=1000, limit_field=None, limit_value=None):
        query = f"SELECT * FROM {table_name}"
        params = []
        if limit_field and limit_value:
            try:
                parser.parse(str(limit_value))
                query += f" WHERE {limit_field} >= %s"
                params.append(limit_value)
            except:
                query += f" WHERE {limit_field} >= %s"
                params.append(limit_value)
        query += " LIMIT %s OFFSET %s"
        params.extend([limit, offset])
        return source_conn.fetch_all(query, params)

    def insert_batch(self, target_conn, table_name, data_list, column_mapping=None):
        if not data_list:
            return 0
        
        first_row = data_list[0]
        source_columns = list(first_row.keys())
        
        if column_mapping:
            target_columns = []
            mapped_data = []
            for row in data_list:
                new_row = {}
                for src_col, tgt_col in column_mapping.items():
                    if src_col in row:
                        new_row[tgt_col] = row[src_col]
                mapped_data.append(new_row)
            if mapped_data:
                target_columns = list(mapped_data[0].keys())
                data_list = mapped_data
        else:
            target_columns = source_columns
        
        placeholders = ', '.join(['%s'] * len(target_columns))
        columns_str = ', '.join(target_columns)
        
        values = []
        for row in data_list:
            row_values = []
            for col in target_columns:
                val = row.get(col)
                if isinstance(val, datetime.datetime) or isinstance(val, datetime.date):
                    row_values.append(val.strftime(self.time_format))
                else:
                    row_values.append(val)
            values.append(tuple(row_values))
        
        query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
        
        try:
            target_conn.cursor.executemany(query, values)
            target_conn.connection.commit()
            return len(values)
        except Exception as e:
            target_conn.connection.rollback()
            return -1

    def sync_table(self, source_conn, target_conn, source_table, target_table, 
                   column_mapping=None, batch_size=1000, limit_field=None, 
                   limit_value=None, delete_before_insert=False, 
                   create_partition=False, add_partition=False,
                   partition_column=None, partition_name=None, 
                   partition_condition=None):
        total_count = self.get_total_count(source_conn, source_table, limit_field, limit_value)
        self.log_status(f"Total records to sync: {total_count}")
        
        if delete_before_insert:
            self.log_status(f"Deleting existing data from {target_table}...")
            target_conn.delete_table_data(target_table)
            self.log_status("Delete completed")
        
        if not target_conn.table_exists(target_table):
            self.log_status(f"Creating table {target_table}...")
            if create_partition and partition_column:
                target_conn.create_partitioned_table(source_conn, source_table, 
                                                    target_table, partition_column)
            else:
                target_conn.create_table(source_conn, source_table, target_table)
            self.log_status("Table created")
        
        if add_partition and partition_name and partition_condition:
            self.log_status(f"Adding partition {partition_name}...")
            target_conn.add_partition(target_table, partition_name, partition_condition)
            self.log_status("Partition added")
        
        offset = 0
        total_inserted = 0
        
        while offset < total_count:
            self.log_progress(offset, total_count)
            self.log_status(f"Fetching batch {offset // batch_size + 1}...")
            
            batch_data = self.batch_fetch(source_conn, source_table, offset, batch_size, 
                                         limit_field, limit_value)
            
            if not batch_data:
                break
            
            self.log_status(f"Inserting {len(batch_data)} records...")
            inserted = self.insert_batch(target_conn, target_table, batch_data, column_mapping)
            
            if inserted < 0:
                self.log_status("Error inserting batch")
                return False
            
            total_inserted += inserted
            offset += batch_size
        
        self.log_progress(total_count, total_count)
        self.log_status(f"Sync completed. Total inserted: {total_inserted}")
        return True

    def sync_databases(self, source_conn, target_conn, table_mappings, sync_options):
        self.batch_size = sync_options.get('batch_size', 1000)
        self.time_format = sync_options.get('time_format', "%Y-%m-%d %H:%M:%S")
        self.delete_before_insert = sync_options.get('delete_before_insert', False)
        self.create_partition = sync_options.get('create_partition', False)
        self.add_partition = sync_options.get('add_partition', False)
        
        for mapping in table_mappings:
            source_table = mapping['source_table']
            target_table = mapping['target_table']
            column_mapping = mapping.get('column_mapping')
            limit_field = mapping.get('limit_field')
            limit_value = mapping.get('limit_value')
            
            self.log_status(f"Syncing {source_table} -> {target_table}")
            
            success = self.sync_table(
                source_conn, target_conn,
                source_table, target_table,
                column_mapping=column_mapping,
                batch_size=self.batch_size,
                limit_field=limit_field,
                limit_value=limit_value,
                delete_before_insert=self.delete_before_insert,
                create_partition=self.create_partition,
                add_partition=self.add_partition,
                partition_column=mapping.get('partition_column'),
                partition_name=mapping.get('partition_name'),
                partition_condition=mapping.get('partition_condition')
            )
            
            if not success:
                self.log_status(f"Failed to sync {source_table}")
                return False
        
        self.log_status("All tables synced successfully")
        return True