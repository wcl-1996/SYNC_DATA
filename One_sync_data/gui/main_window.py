import sys
from PyQt5.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
                             QLabel, QLineEdit, QPushButton, QComboBox, QTableWidget,
                             QTableWidgetItem, QGroupBox, QCheckBox, QProgressBar,
                             QTextEdit, QSplitter, QHeaderView, QMessageBox, QTabWidget,
                             QSpinBox, QDoubleSpinBox)
from PyQt5.QtCore import Qt, QThread, pyqtSignal
from db.connector import DBConnector
from sync.sync_engine import SyncEngine

class SyncThread(QThread):
    progress_signal = pyqtSignal(int, int)
    status_signal = pyqtSignal(str)
    finished_signal = pyqtSignal(bool)
    
    def __init__(self, source_conn, target_conn, table_mappings, sync_options):
        super().__init__()
        self.source_conn = source_conn
        self.target_conn = target_conn
        self.table_mappings = table_mappings
        self.sync_options = sync_options
    
    def run(self):
        engine = SyncEngine()
        engine.set_progress_callback(lambda c, t: self.progress_signal.emit(c, t))
        engine.set_status_callback(lambda m: self.status_signal.emit(m))
        
        try:
            success = engine.sync_databases(self.source_conn, self.target_conn, 
                                           self.table_mappings, self.sync_options)
            self.finished_signal.emit(success)
        except Exception as e:
            self.status_signal.emit(f"Error: {str(e)}")
            self.finished_signal.emit(False)

class DBConfigWidget(QWidget):
    def __init__(self, title):
        super().__init__()
        self.title = title
        self.init_ui()
    
    def init_ui(self):
        layout = QVBoxLayout()
        
        group_box = QGroupBox(self.title)
        group_layout = QVBoxLayout()
        
        h_layout = QHBoxLayout()
        h_layout.addWidget(QLabel("Host:"))
        self.host_edit = QLineEdit()
        self.host_edit.setText("localhost")
        h_layout.addWidget(self.host_edit)
        group_layout.addLayout(h_layout)
        
        h_layout = QHBoxLayout()
        h_layout.addWidget(QLabel("Port:"))
        self.port_edit = QLineEdit()
        self.port_edit.setText("3306")
        h_layout.addWidget(self.port_edit)
        group_layout.addLayout(h_layout)
        
        h_layout = QHBoxLayout()
        h_layout.addWidget(QLabel("Database:"))
        self.db_combo = QComboBox()
        self.db_combo.setEditable(True)
        h_layout.addWidget(self.db_combo)
        group_layout.addLayout(h_layout)
        
        h_layout = QHBoxLayout()
        h_layout.addWidget(QLabel("User:"))
        self.user_edit = QLineEdit()
        h_layout.addWidget(self.user_edit)
        group_layout.addLayout(h_layout)
        
        h_layout = QHBoxLayout()
        h_layout.addWidget(QLabel("Password:"))
        self.pwd_edit = QLineEdit()
        self.pwd_edit.setEchoMode(QLineEdit.Password)
        h_layout.addWidget(self.pwd_edit)
        group_layout.addLayout(h_layout)
        
        self.connect_btn = QPushButton("Connect")
        self.connect_btn.clicked.connect(self.on_connect)
        group_layout.addWidget(self.connect_btn)
        
        self.status_label = QLabel("Status: Disconnected")
        self.status_label.setStyleSheet("color: red")
        group_layout.addWidget(self.status_label)
        
        group_box.setLayout(group_layout)
        layout.addWidget(group_box)
        
        self.setLayout(layout)
        
        self.connector = None
        self.connected = False
    
    def on_connect(self):
        self.connector = DBConnector()
        host = self.host_edit.text()
        port = self.port_edit.text()
        database = self.db_combo.currentText()
        user = self.user_edit.text()
        password = self.pwd_edit.text()
        
        if self.connector.connect(host, port, database, user, password):
            self.status_label.setText("Status: Connected")
            self.status_label.setStyleSheet("color: green")
            self.connected = True
            self.load_databases()
        else:
            self.status_label.setText("Status: Connection failed")
            self.status_label.setStyleSheet("color: red")
            self.connected = False
    
    def load_databases(self):
        if self.connector:
            dbs = self.connector.get_databases()
            self.db_combo.clear()
            self.db_combo.addItems(dbs)
    
    def get_connection(self):
        return self.connector if self.connected else None

class ColumnMappingWidget(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.source_columns = []
        self.target_columns = []
        self.init_ui()
    
    def init_ui(self):
        layout = QVBoxLayout()
        
        h_layout = QHBoxLayout()
        
        self.source_list = QComboBox()
        h_layout.addWidget(QLabel("Source Column:"))
        h_layout.addWidget(self.source_list)
        
        h_layout.addWidget(QLabel("->"))
        
        self.target_list = QComboBox()
        h_layout.addWidget(QLabel("Target Column:"))
        h_layout.addWidget(self.target_list)
        
        self.add_btn = QPushButton("Add Mapping")
        self.add_btn.clicked.connect(self.add_mapping)
        h_layout.addWidget(self.add_btn)
        
        layout.addLayout(h_layout)
        
        self.mapping_table = QTableWidget()
        self.mapping_table.setColumnCount(2)
        self.mapping_table.setHorizontalHeaderLabels(["Source", "Target"])
        self.mapping_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        
        self.remove_btn = QPushButton("Remove Selected")
        self.remove_btn.clicked.connect(self.remove_mapping)
        layout.addWidget(self.remove_btn)
        
        layout.addWidget(self.mapping_table)
        
        self.setLayout(layout)
    
    def set_source_columns(self, columns):
        self.source_columns = columns
        self.source_list.clear()
        self.source_list.addItems(columns)
    
    def set_target_columns(self, columns):
        self.target_columns = columns
        self.target_list.clear()
        self.target_list.addItems(columns)
    
    def add_mapping(self):
        source = self.source_list.currentText()
        target = self.target_list.currentText()
        if source and target:
            row_count = self.mapping_table.rowCount()
            self.mapping_table.insertRow(row_count)
            self.mapping_table.setItem(row_count, 0, QTableWidgetItem(source))
            self.mapping_table.setItem(row_count, 1, QTableWidgetItem(target))
    
    def remove_mapping(self):
        selected_rows = self.mapping_table.selectionModel().selectedRows()
        for row in sorted(selected_rows, key=lambda x: x.row(), reverse=True):
            self.mapping_table.removeRow(row.row())
    
    def get_mappings(self):
        mappings = {}
        for row in range(self.mapping_table.rowCount()):
            source = self.mapping_table.item(row, 0).text()
            target = self.mapping_table.item(row, 1).text()
            mappings[source] = target
        return mappings

class TableMappingWidget(QWidget):
    mapping_added = pyqtSignal(dict)
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.source_conn = None
        self.target_conn = None
        self.init_ui()
    
    def init_ui(self):
        layout = QVBoxLayout()
        
        h_layout = QHBoxLayout()
        
        self.source_table = QComboBox()
        self.source_table.currentTextChanged.connect(self.on_source_table_change)
        h_layout.addWidget(QLabel("Source Table:"))
        h_layout.addWidget(self.source_table)
        
        h_layout.addWidget(QLabel("->"))
        
        self.target_table = QComboBox()
        self.target_table.currentTextChanged.connect(self.on_target_table_change)
        h_layout.addWidget(QLabel("Target Table:"))
        h_layout.addWidget(self.target_table)
        
        self.add_mapping_btn = QPushButton("Add Table Mapping")
        self.add_mapping_btn.clicked.connect(self.add_table_mapping)
        h_layout.addWidget(self.add_mapping_btn)
        
        layout.addLayout(h_layout)
        
        self.column_mapping = ColumnMappingWidget()
        layout.addWidget(self.column_mapping)
        
        self.setLayout(layout)
    
    def set_source_connection(self, conn):
        self.source_conn = conn
        if conn:
            tables = conn.get_tables()
            self.source_table.clear()
            self.source_table.addItems(tables)
    
    def set_target_connection(self, conn):
        self.target_conn = conn
        if conn:
            tables = conn.get_tables()
            self.target_table.clear()
            self.target_table.addItems(tables)
    
    def on_source_table_change(self, table_name):
        if self.source_conn and table_name:
            schema = self.source_conn.get_table_schema(table_name)
            columns = [col['field'] for col in schema]
            self.column_mapping.set_source_columns(columns)
    
    def on_target_table_change(self, table_name):
        if self.target_conn and table_name:
            schema = self.target_conn.get_table_schema(table_name)
            columns = [col['field'] for col in schema]
            self.column_mapping.set_target_columns(columns)
    
    def add_table_mapping(self):
        source_table = self.source_table.currentText()
        target_table = self.target_table.currentText()
        if source_table and target_table:
            mapping = {
                'source_table': source_table,
                'target_table': target_table,
                'column_mapping': self.column_mapping.get_mappings()
            }
            self.mapping_added.emit(mapping)

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("MySQL Data Migration Tool")
        self.setGeometry(100, 100, 1200, 800)
        
        self.table_mappings = []
        self.init_ui()
    
    def init_ui(self):
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QVBoxLayout(central_widget)
        
        tab_widget = QTabWidget()
        
        conn_tab = QWidget()
        conn_layout = QHBoxLayout(conn_tab)
        
        self.source_config = DBConfigWidget("Source Database")
        self.target_config = DBConfigWidget("Target Database")
        
        conn_layout.addWidget(self.source_config)
        conn_layout.addWidget(self.target_config)
        
        tab_widget.addTab(conn_tab, "Connection")
        
        mapping_tab = QWidget()
        mapping_layout = QVBoxLayout(mapping_tab)
        
        self.table_mapping_widget = TableMappingWidget()
        self.table_mapping_widget.mapping_added.connect(self.on_mapping_added)
        mapping_layout.addWidget(self.table_mapping_widget)
        
        self.mapping_list = QTableWidget()
        self.mapping_list.setColumnCount(2)
        self.mapping_list.setHorizontalHeaderLabels(["Source Table", "Target Table"])
        self.mapping_list.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        mapping_layout.addWidget(self.mapping_list)
        
        tab_widget.addTab(mapping_tab, "Table Mapping")
        
        options_tab = QWidget()
        options_layout = QVBoxLayout(options_tab)
        
        h_layout = QHBoxLayout()
        h_layout.addWidget(QLabel("Batch Size:"))
        self.batch_size_spin = QSpinBox()
        self.batch_size_spin.setRange(100, 10000)
        self.batch_size_spin.setValue(1000)
        h_layout.addWidget(self.batch_size_spin)
        options_layout.addLayout(h_layout)
        
        h_layout = QHBoxLayout()
        h_layout.addWidget(QLabel("Limit Field:"))
        self.limit_field_edit = QLineEdit()
        h_layout.addWidget(self.limit_field_edit)
        h_layout.addWidget(QLabel("Limit Value:"))
        self.limit_value_edit = QLineEdit()
        h_layout.addWidget(self.limit_value_edit)
        options_layout.addLayout(h_layout)
        
        h_layout = QHBoxLayout()
        h_layout.addWidget(QLabel("Time Format:"))
        self.time_format_edit = QLineEdit()
        self.time_format_edit.setText("%Y-%m-%d %H:%M:%S")
        h_layout.addWidget(self.time_format_edit)
        options_layout.addLayout(h_layout)
        
        self.delete_check = QCheckBox("Delete Before Insert")
        options_layout.addWidget(self.delete_check)
        
        self.create_partition_check = QCheckBox("Create Partition")
        options_layout.addWidget(self.create_partition_check)
        
        self.add_partition_check = QCheckBox("Add Partition")
        options_layout.addWidget(self.add_partition_check)
        
        h_layout = QHBoxLayout()
        h_layout.addWidget(QLabel("Partition Column:"))
        self.partition_column_edit = QLineEdit()
        h_layout.addWidget(self.partition_column_edit)
        options_layout.addLayout(h_layout)
        
        h_layout = QHBoxLayout()
        h_layout.addWidget(QLabel("Partition Name:"))
        self.partition_name_edit = QLineEdit()
        h_layout.addWidget(self.partition_name_edit)
        h_layout.addWidget(QLabel("Partition Condition:"))
        self.partition_condition_edit = QLineEdit()
        h_layout.addWidget(self.partition_condition_edit)
        options_layout.addLayout(h_layout)
        
        tab_widget.addTab(options_tab, "Sync Options")
        
        main_layout.addWidget(tab_widget)
        
        self.progress_bar = QProgressBar()
        main_layout.addWidget(self.progress_bar)
        
        self.log_text = QTextEdit()
        self.log_text.setReadOnly(True)
        main_layout.addWidget(self.log_text)
        
        btn_layout = QHBoxLayout()
        
        self.sync_btn = QPushButton("Start Sync")
        self.sync_btn.clicked.connect(self.on_sync)
        btn_layout.addWidget(self.sync_btn)
        
        self.clear_btn = QPushButton("Clear Log")
        self.clear_btn.clicked.connect(self.log_text.clear)
        btn_layout.addWidget(self.clear_btn)
        
        main_layout.addLayout(btn_layout)
        
        self.source_config.connect_btn.clicked.connect(self.on_source_connected)
        self.target_config.connect_btn.clicked.connect(self.on_target_connected)
    
    def on_source_connected(self):
        conn = self.source_config.get_connection()
        if conn:
            self.table_mapping_widget.set_source_connection(conn)
            self.log("Source database connected")
    
    def on_target_connected(self):
        conn = self.target_config.get_connection()
        if conn:
            self.table_mapping_widget.set_target_connection(conn)
            self.log("Target database connected")
    
    def on_mapping_added(self, mapping):
        self.table_mappings.append(mapping)
        row_count = self.mapping_list.rowCount()
        self.mapping_list.insertRow(row_count)
        self.mapping_list.setItem(row_count, 0, QTableWidgetItem(mapping['source_table']))
        self.mapping_list.setItem(row_count, 1, QTableWidgetItem(mapping['target_table']))
    
    def log(self, message):
        self.log_text.append(message)
    
    def on_sync(self):
        source_conn = self.source_config.get_connection()
        target_conn = self.target_config.get_connection()
        
        if not source_conn or not target_conn:
            QMessageBox.warning(self, "Warning", "Please connect both databases first")
            return
        
        if not self.table_mappings:
            QMessageBox.warning(self, "Warning", "Please add table mappings")
            return
        
        sync_options = {
            'batch_size': self.batch_size_spin.value(),
            'time_format': self.time_format_edit.text(),
            'delete_before_insert': self.delete_check.isChecked(),
            'create_partition': self.create_partition_check.isChecked(),
            'add_partition': self.add_partition_check.isChecked()
        }
        
        for mapping in self.table_mappings:
            mapping['limit_field'] = self.limit_field_edit.text() if self.limit_field_edit.text() else None
            mapping['limit_value'] = self.limit_value_edit.text() if self.limit_value_edit.text() else None
            mapping['partition_column'] = self.partition_column_edit.text() if self.partition_column_edit.text() else None
            mapping['partition_name'] = self.partition_name_edit.text() if self.partition_name_edit.text() else None
            mapping['partition_condition'] = self.partition_condition_edit.text() if self.partition_condition_edit.text() else None
        
        self.sync_btn.setEnabled(False)
        self.progress_bar.setValue(0)
        
        self.sync_thread = SyncThread(source_conn, target_conn, self.table_mappings, sync_options)
        self.sync_thread.progress_signal.connect(self.on_progress)
        self.sync_thread.status_signal.connect(self.log)
        self.sync_thread.finished_signal.connect(self.on_sync_finished)
        self.sync_thread.start()
    
    def on_progress(self, current, total):
        if total > 0:
            self.progress_bar.setValue(int((current / total) * 100))
    
    def on_sync_finished(self, success):
        self.sync_btn.setEnabled(True)
        if success:
            QMessageBox.information(self, "Success", "Sync completed successfully")
        else:
            QMessageBox.error(self, "Error", "Sync failed")

def main():
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec_())

if __name__ == "__main__":
    main()