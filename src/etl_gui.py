#!/usr/bin/env python3
"""
etl_gui.py - GUI frontend for the ETL pipeline.

This application provides a graphical interface for:
1. Managing data source paths
2. Checking database connection
3. Running the ETL pipeline
4. Configuring output data and filters

It interfaces with the existing scripts:
- generate_config.py
- data_cleaner.py
- load_to_postgres.py
- payment_analysis.sql
"""

import os
import sys
import json
import subprocess
import time
import traceback
import platform
import threading
import pandas as pd
import importlib
from PyQt5.QtGui import QTextCursor
import importlib # Added import
from pathlib import Path
from datetime import datetime
from threading import Thread, Event
from sqlalchemy import text  # Import text for SQL queries
import glob

from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QTabWidget, QWidget, QVBoxLayout, QHBoxLayout,
    QPushButton, QLabel, QLineEdit, QFileDialog, QMessageBox, QProgressBar,
    QTableWidget, QTableWidgetItem, QHeaderView, QComboBox, QCheckBox,
    QGroupBox, QScrollArea, QSplitter, QTextEdit, QListWidget, QListWidgetItem, 
    QDialog, QDialogButtonBox, QFormLayout, QSpinBox, QStatusBar, QAction,
    QToolBar, QFrame, QToolButton, QMenu, QStyle, QSizePolicy, QSystemTrayIcon,
    QTreeWidget, QTreeWidgetItem, QAbstractItemView, QWizard, QWizardPage,
    QCompleter, QGraphicsDropShadowEffect, QDateEdit, QCalendarWidget, QSlider,
    QRadioButton, QGridLayout, QToolTip
)
from PyQt5.QtCore import Qt, pyqtSignal, pyqtSlot, QThread, QSize, QTimer, QEvent, QUrl, QPoint, QRect, QObject
from PyQt5.QtGui import (
    QFont, QIcon, QColor, QPalette, QPixmap, QCursor, QKeySequence, QLinearGradient,
    QFontDatabase, QPainter, QPen, QBrush, QDesktopServices, QImage, QDoubleValidator
)

# Import for database connection check
try:
    from sqlalchemy import create_engine, text
    from dotenv import load_dotenv
except ImportError:
    print("Required packages not installed. Please run:")
    print("pip install sqlalchemy python-dotenv")
    sys.exit(1)

class ThemeManager:
    """Manages application styling and theme settings."""
    
    # Define color palettes
    THEMES = {
        "Light": {
            "primary": "#0078d7",
            "primary_hover": "#0063b1",
            "accent": "#4cc9f0",
            "success": "#4caf50",
            "warning": "#ff9800",
            "error": "#f44336",
            "background": "#f5f5f5",
            "card_background": "#ffffff",
            "text": "#333333",
            "text_secondary": "#777777",
            "border": "#dddddd",
            "input_background": "#ffffff",
            "disabled": "#cccccc",
        },
        "Dark": {
            "primary": "#2185d0",
            "primary_hover": "#1678c2",
            "accent": "#4cc9f0",
            "success": "#21ba45",
            "warning": "#f2711c",
            "error": "#db2828",
            "background": "#2d3436",
            "card_background": "#1e2a31",
            "text": "#f5f5f5",
            "text_secondary": "#b3b3b3",
            "border": "#555555",
            "input_background": "#3f4a52",
            "disabled": "#666666",
        }
    }
    
    @classmethod
    def get_icon_path(cls, icon_name):
        """Get the path for the specified icon."""
        # Default icon folder location
        icons_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "icons")
        os.makedirs(icons_dir, exist_ok=True)
        
        # Return default fallback if icon doesn't exist
        icon_path = os.path.join(icons_dir, f"{icon_name}.png")
        if not os.path.exists(icon_path):
            # Use QT's built-in icons as fallback
            return None
            
        return icon_path
    
    @classmethod
    def get_stylesheet(cls, theme_name="Light"):
        """Get complete stylesheet based on selected theme."""
        theme = cls.THEMES.get(theme_name, cls.THEMES["Light"])
        
        return f"""
            QMainWindow, QDialog {{
                background-color: {theme["background"]};
                color: {theme["text"]};
                font-family: "Segoe UI", Arial, sans-serif;
            }}
            
            QTabWidget::pane {{
                border: 1px solid {theme["border"]};
                background-color: {theme["card_background"]};
                border-radius: 4px;
            }}
            
            QTabBar::tab {{
                background-color: {theme["background"]};
                border: 1px solid {theme["border"]};
                padding: 8px 16px;
                margin-right: 2px;
                border-top-left-radius: 4px;
                border-top-right-radius: 4px;
                color: {theme["text"]};
            }}
            
            QTabBar::tab:selected {{
                background-color: {theme["card_background"]};
                border-bottom-color: {theme["card_background"]};
                color: {theme["primary"]};
                font-weight: bold;
            }}
            
            QPushButton {{
                background-color: {theme["primary"]};
                color: white;
                border: none;
                padding: 8px 16px;
                font-weight: bold;
                border-radius: 4px;
                min-height: 18px;
            }}
            
            QPushButton:hover {{
                background-color: {theme["primary_hover"]};
            }}
            
            QPushButton:pressed {{
                background-color: {theme["primary_hover"]};
                padding-top: 9px;
                padding-bottom: 7px;
            }}
            
            QPushButton:disabled {{
                background-color: {theme["disabled"]};
            }}
            
            QLabel {{
                color: {theme["text"]};
            }}
            
            QGroupBox {{
                font-weight: bold;
                border: 1px solid {theme["border"]};
                border-radius: 4px;
                margin-top: 16px;
                padding-top: 16px;
                color: {theme["text"]};
                background-color: {theme["card_background"]};
            }}
            
            QGroupBox::title {{
                subcontrol-origin: margin;
                left: 10px;
                padding: 0 5px;
                color: {theme["text"]};
            }}
            
            QLineEdit, QSpinBox, QComboBox {{
                border: 1px solid {theme["border"]};
                border-radius: 4px;
                padding: 6px;
                background-color: {theme["input_background"]};
                color: {theme["text"]};
                selection-background-color: {theme["primary"]};
            }}
            
            QTextEdit, QListWidget, QTableWidget, QTreeWidget {{
                border: 1px solid {theme["border"]};
                border-radius: 4px;
                background-color: {theme["card_background"]};
                color: {theme["text"]};
                selection-background-color: {theme["primary"]};
                selection-color: white;
            }}
            
            QProgressBar {{
                border: 1px solid {theme["border"]};
                border-radius: 4px;
                text-align: center;
                height: 20px;
                background-color: {theme["card_background"]};
                color: {theme["text"]};
            }}
            
            QProgressBar::chunk {{
                background-color: {theme["primary"]};
                width: 1px;
            }}
            
            QStatusBar {{
                background-color: {theme["card_background"]};
                color: {theme["text"]};
                border-top: 1px solid {theme["border"]};
            }}
            
            QToolButton {{
                border: none;
                border-radius: 4px;
                background-color: transparent;
                padding: 6px;
            }}
            
            QToolButton:hover {{
                background-color: rgba(200, 200, 200, 30%);
            }}
            
            QToolButton:pressed {{
                background-color: rgba(200, 200, 200, 50%);
            }}
            
            QScrollBar:vertical {{
                border: none;
                background-color: {theme["background"]};
                width: 12px;
                margin: 0px;
            }}
            
            QScrollBar::handle:vertical {{
                background-color: {theme["border"]};
                min-height: 20px;
                border-radius: 6px;
            }}
            
            QScrollBar::add-line:vertical,
            QScrollBar::sub-line:vertical {{
                border: none;
                background: none;
                height: 0px;
            }}
            
            QToolTip {{
                background-color: #FFFFCC;
                color: #000000;
                border: 1px solid #888888;
                padding: 5px;
                border-radius: 3px;
                opacity: 255;
            }}
            
            QMenuBar {{
                background-color: {theme["card_background"]};
            }}
            
            QMenuBar::item {{
                background-color: transparent;
                padding: 6px 10px;
                color: {theme["text"]};
            }}
            
            QMenuBar::item:selected {{
                background-color: {theme["primary"]};
                color: white;
            }}
            
            QMenu {{
                background-color: {theme["card_background"]};
                border: 1px solid {theme["border"]};
                padding: 4px;
            }}
            
            QMenu::item {{
                padding: 6px 20px 6px 20px;
                color: {theme["text"]};
            }}
            
            QMenu::item:selected {{
                background-color: {theme["primary"]};
                color: white;
            }}
            
            QHeaderView::section {{
                background-color: {theme["background"]};
                border: 1px solid {theme["border"]};
                padding: 4px;
                color: {theme["text"]};
                font-weight: bold;
            }}
        """
    
    @classmethod
    def apply_theme(cls, app, theme_name="Light"):
        """Apply theme to the application."""
        # Set stylesheet
        app.setStyleSheet(cls.get_stylesheet(theme_name))
        
        # Load custom fonts if needed
        cls.load_fonts()
    
    @classmethod
    def load_fonts(cls):
        """Load custom fonts into the application."""
        # Example of loading a font
        # font_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fonts", "OpenSans-Regular.ttf")
        # QFontDatabase.addApplicationFont(font_path)
        pass

class LogCaptureThread(QThread):
    """Thread to capture and redirect process output to GUI."""
    log_signal = pyqtSignal(str)
    progress_signal = pyqtSignal(int)
    finished_signal = pyqtSignal(bool, str)  # Success flag and message

    def __init__(self, command, description):
        super().__init__()
        self.command = command
        self.description = description
        self.process = None
        self.success = False
        self.message = ""

    def run(self):
        try:
            self.log_signal.emit(f"Starting: {self.description}\n")
            self.log_signal.emit(f"Command: {self.command}\n")
            
            # Start process and capture output with line buffering
            self.process = subprocess.Popen(
                self.command,
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                universal_newlines=True
            )
            
            # Read and emit output lines
            for line in iter(self.process.stdout.readline, ''):
                # Ensure we always emit the line so GUI can see it
                self.log_signal.emit(line)
                
                # Try to extract progress information
                if "%" in line:
                    try:
                        percent = int(line.split("%")[0].split()[-1])
                        self.progress_signal.emit(percent)
                    except (ValueError, IndexError):
                        pass
                
                # Log user interactions and important messages distinctly
                if "Do you want to proceed with dropping and reloading?" in line:
                    self.log_signal.emit("\n*** WAITING FOR USER INPUT - Please check terminal window! ***\n")
                    
                # Check for common SQL warnings/errors that indicate progress is still happening
                if any(keyword in line for keyword in ["SQL", "table", "dropping", "loading", "processing"]):
                    self.log_signal.emit(f"PROGRESS: {line.strip()}\n")
            
            # Wait for process to complete
            return_code = self.process.wait()
            
            if return_code == 0:
                self.success = True
                self.message = f"{self.description} completed successfully."
            else:
                self.success = False
                self.message = f"{self.description} failed with return code {return_code}."
            
            self.log_signal.emit(f"\n{self.message}\n")
            self.finished_signal.emit(self.success, self.message)
            
        except Exception as e:
            self.success = False
            self.message = f"Error executing {self.description}: {str(e)}"
            self.log_signal.emit(f"\n{self.message}\n")
            self.log_signal.emit(traceback.format_exc())
            self.finished_signal.emit(self.success, self.message)

    def stop(self):
        """Stop the running process."""
        if self.process and self.process.poll() is None:
            self.process.terminate()
            self.log_signal.emit("\nProcess terminated by user.\n")

class DatabaseConnection:
    """Handles database connection configuration and testing."""
    
    def __init__(self):
        """Initialize database connection settings."""
        self.engine = None
        self.connected = False
        self.conn_string = None
        self.connection_error = None
        
    @property
    def connection_info(self):
        """Returns a dictionary with database connection information"""
        if not self.conn_string:
            return {'host': 'Not connected', 'port': 'Not connected', 'database': 'Not connected', 'user': 'Not connected'}
        
        try:
            # Extract connection parts from connection string
            # Format: postgresql://user:password@host:port/database
            parts = self.conn_string.split('@')
            if len(parts) == 2:
                user_part = parts[0].split('://')[1].split(':')[0]
                host_part = parts[1].split('/')[0]
                host = host_part.split(':')[0]
                port = host_part.split(':')[1] if ':' in host_part else '5432'
                database = parts[1].split('/')[1]
                
                return {
                    'host': host,
                    'port': port,
                    'database': database,
                    'user': user_part
                }
            return {'host': 'Unknown', 'port': 'Unknown', 'database': 'Unknown', 'user': 'Unknown'}
        except Exception:
            return {'host': 'Error', 'port': 'Error', 'database': 'Error', 'user': 'Error'}
    
    def connect_from_env(self):
        """Try to connect to the database using environment variables."""
        try:
            # Close any existing connections first
            if self.engine:
                try:
                    self.engine.dispose()
                    print("Disposed existing database engine")
                except:
                    pass
                self.engine = None
            
            # Import modules
            from sqlalchemy import create_engine
            import os
            from dotenv import load_dotenv
            
            # Load environment variables
            env_path = Path(__file__).parent / '.env'
            load_dotenv(env_path, override=True)
            
            # Get database connection parameters
            db_host = os.getenv('DB_HOST', 'localhost')
            db_port = os.getenv('DB_PORT', '5432')
            db_name = os.getenv('DB_NAME', 'etl')
            db_user = os.getenv('DB_USER', 'postgres')
            db_password = os.getenv('DB_PASSWORD', '12345678')
            
            # Create connection string
            conn_string = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
            self.conn_string = conn_string
            
            # Create engine with timeout and pool settings
            self.engine = create_engine(
                conn_string,
                connect_args={"connect_timeout": 10},
                pool_size=5,
                max_overflow=10,
                pool_timeout=30,
                pool_recycle=1800
            )
            
            # Test connection with a simple query
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            
            self.connected = True
            self.connection_error = None
            
            print(f"Connected to database '{db_name}' on {db_host}:{db_port}")
            return True, f"Connected to database '{db_name}' on {db_host}:{db_port}"
            
        except Exception as e:
            # Handle specific database errors with more helpful messages
            error_message = str(e)
            self.connected = False
            self.connection_error = error_message
            
            # Provide more specific error messages based on error type
            if "password authentication failed" in error_message:
                user_message = f"Password authentication failed for user '{db_user}'. Please check your database credentials in the .env file."
            elif "connection refused" in error_message.lower():
                user_message = f"Connection refused to {db_host}:{db_port}. Please make sure the PostgreSQL server is running."
            elif "database" in error_message.lower() and "does not exist" in error_message.lower():
                user_message = f"Database '{db_name}' does not exist. Please create it or use a different database."
            else:
                user_message = f"Database connection error: {error_message}"
            
            print(f"Database connection error: {error_message}")
            return False, user_message
    
    def test_connection(self, host, port, database, user, password):
        """Test database connection with provided credentials."""
        try:
            from sqlalchemy import create_engine
            
            # Create connection string
            conn_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"
            
            # Create temporary engine
            temp_engine = create_engine(
                conn_string,
                connect_args={"connect_timeout": 5}  # Short timeout for testing
            )
            
            # Test connection
            with temp_engine.connect() as conn:
                result = conn.execute(text("SELECT 1"))
                row = result.fetchone()
                if row and row[0] == 1:
                    return True, f"Successfully connected to {database} on {host}:{port}"
                else:
                    return False, "Connection test failed: Unexpected response"
        
        except Exception as e:
            # Format a user-friendly error message
            error_message = str(e)
            if "password authentication failed" in error_message:
                return False, f"Password authentication failed for user '{user}'"
            elif "connection refused" in error_message.lower():
                return False, f"Connection refused to {host}:{port}. Is PostgreSQL running?"
            elif "database" in error_message.lower() and "does not exist" in error_message.lower():
                return False, f"Database '{database}' does not exist"
            else:
                return False, f"Connection error: {error_message}"
    
    def check_tables(self):
        """Check if required tables exist in the database."""
        if not self.connected or not self.engine:
            return False, "Not connected to database.", []
        
        try:
            # Define required tables
            required_tables = [
                'portal', 'metabase', 'checkout_v1', 'checkout_v2', 'payfort', 'tamara', 'bank'
            ]
            
            # Get existing tables
            with self.engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public'
                """))
                
                existing_tables = [row[0] for row in result]
            
            # Find missing tables
            missing_tables = [table for table in required_tables if table not in existing_tables]
            
            if missing_tables:
                return False, f"Missing tables: {', '.join(missing_tables)}", existing_tables
            else:
                return True, f"All required tables exist. Found {len(existing_tables)} tables in total.", existing_tables
                
        except Exception as e:
            return False, f"Error checking tables: {str(e)}", []

class SourceFilesDialog(QDialog):
    """Enhanced dialog for adding/editing source files with drag and drop support."""
    
    def __init__(self, source, file_paths=None, parent=None):
        super().__init__(parent)
        self.source = source
        self.file_paths = file_paths if file_paths else []
        
        self.setup_ui()
        self.setup_connections()
        
    def setup_ui(self):
        """Set up the enhanced dialog UI."""
        self.setWindowTitle(f"Manage {self.source.capitalize()} Files")
        self.setMinimumWidth(700)
        self.setMinimumHeight(500)
        
        # Main layout
        layout = QVBoxLayout()
        layout.setSpacing(10)
        layout.setContentsMargins(12, 12, 12, 12)
        
        # Title section with icon
        title_layout = QHBoxLayout()
        title_icon = QLabel()
        try:
            icon_path = ThemeManager.get_icon_path("file")
            if icon_path:
                pixmap = QPixmap(icon_path).scaled(32, 32, Qt.KeepAspectRatio, Qt.SmoothTransformation)
                title_icon.setPixmap(pixmap)
        except:
            pass  # Skip icon if not available
            
        title_label = QLabel(f"Files for <b>{self.source.capitalize()}</b>")
        title_label.setFont(QFont("Segoe UI", 12))
        title_layout.addWidget(title_icon)
        title_layout.addWidget(title_label)
        title_layout.addStretch()
        
        # Help text
        help_text = QLabel("Drag and drop CSV files here or use the buttons below to add files")
        help_text.setStyleSheet("color: #777777; font-style: italic;")
        
        layout.addLayout(title_layout)
        layout.addWidget(help_text)
        
        # Files list with custom styling
        self.files_list = QListWidget()
        self.files_list.setAlternatingRowColors(True)
        self.files_list.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.files_list.setDragDropMode(QAbstractItemView.DragDrop)
        self.files_list.setAcceptDrops(True)
        self.files_list.setDefaultDropAction(Qt.CopyAction)
        self.files_list.setStyleSheet("""
            QListWidget {
                border: 1px solid #ddd;
                border-radius: 4px;
                padding: 4px;
            }
            QListWidget::item {
                border-bottom: 1px solid #eee;
                padding: 4px;
            }
            QListWidget::item:selected {
                background-color: #0078d7;
                color: white;
            }
            QListWidget::item:alternate {
                background-color: #f9f9f9;
            }
        """)
        
        layout.addWidget(self.files_list, 1)  # 1 = stretch factor
        
        # Buttons section
        btn_layout = QHBoxLayout()
        
        # Add files button with icon
        self.add_btn = QPushButton("Add Files")
        self.add_btn.setIcon(self.style().standardIcon(QStyle.SP_FileDialogNewFolder))
        self.add_btn.setIconSize(QSize(16, 16))
        self.add_btn.setToolTip("Add one or more CSV files")
        
        # Add directory button with icon
        self.add_dir_btn = QPushButton("Add Directory")
        self.add_dir_btn.setIcon(self.style().standardIcon(QStyle.SP_DirOpenIcon))
        self.add_dir_btn.setIconSize(QSize(16, 16))
        self.add_dir_btn.setToolTip("Add all CSV files from a directory")
        
        # Remove selected button with icon
        self.remove_btn = QPushButton("Remove Selected")
        self.remove_btn.setIcon(self.style().standardIcon(QStyle.SP_TrashIcon))
        self.remove_btn.setIconSize(QSize(16, 16))
        self.remove_btn.setToolTip("Remove selected files from the list")
        
        # Clear all button with icon
        self.clear_btn = QPushButton("Clear All")
        self.clear_btn.setIcon(self.style().standardIcon(QStyle.SP_DialogResetButton))
        self.clear_btn.setIconSize(QSize(16, 16))
        self.clear_btn.setToolTip("Remove all files from the list")
        
        btn_layout.addWidget(self.add_btn)
        btn_layout.addWidget(self.add_dir_btn)
        btn_layout.addWidget(self.remove_btn)
        btn_layout.addWidget(self.clear_btn)
        
        layout.addLayout(btn_layout)
        
        # Status section
        self.status_label = QLabel(f"{len(self.file_paths)} files selected")
        self.status_label.setStyleSheet("color: #777777;")
        layout.addWidget(self.status_label)
        
        # Standard dialog buttons
        button_box = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        button_box.accepted.connect(self.accept)
        button_box.rejected.connect(self.reject)
        layout.addWidget(button_box)
        
        self.setLayout(layout)
        
        # Update the files list
        self.update_files_list()
    
    def setup_connections(self):
        """Connect signals to slots."""
        self.add_btn.clicked.connect(self.add_files)
        self.add_dir_btn.clicked.connect(self.add_directory)
        self.remove_btn.clicked.connect(self.remove_selected)
        self.clear_btn.clicked.connect(self.clear_all)
        self.files_list.itemDoubleClicked.connect(self.open_file_location)
        
    def update_files_list(self):
        """Update the files list widget with icons and detailed information."""
        self.files_list.clear()
        
        for file_path in self.file_paths:
            item = QListWidgetItem()
            
            # Set item text and tooltip
            file_name = os.path.basename(file_path)
            item.setText(file_name)
            item.setToolTip(file_path)
            
            # Set icon based on file type
            item.setIcon(self.style().standardIcon(QStyle.SP_FileIcon))
            
            # Add metadata as data
            try:
                file_size = os.path.getsize(file_path)
                size_str = self.format_size(file_size)
                modified_time = os.path.getmtime(file_path)
                modified_str = datetime.fromtimestamp(modified_time).strftime('%Y-%m-%d %H:%M')
                
                # Add full path as data
                item.setData(Qt.UserRole, file_path)
                
                # Add detailed text
                item.setText(f"{file_name} ({size_str}) - Modified: {modified_str}")
            except:
                # If file doesn't exist or can't be accessed
                item.setText(f"{file_name} (path not accessible)")
                item.setForeground(QColor("#ff6b6b"))
            
            self.files_list.addItem(item)
        
        # Update status label
        self.status_label.setText(f"{len(self.file_paths)} files selected")
        
    def add_files(self):
        """Open file dialog to add CSV files."""
        files, _ = QFileDialog.getOpenFileNames(
            self, "Select CSV Files", "", "CSV Files (*.csv)"
        )
        if files:
            # Filter out duplicates
            new_files = [f for f in files if f not in self.file_paths]
            self.file_paths.extend(new_files)
            self.update_files_list()
            if new_files:
                self.status_label.setText(f"Added {len(new_files)} new files")
    
    def add_directory(self):
        """Add all CSV files from a directory."""
        directory = QFileDialog.getExistingDirectory(
            self, "Select Directory", ""
        )
        if directory:
            # Find all CSV files in the directory
            csv_files = []
            for root, _, files in os.walk(directory):
                for file in files:
                    if file.lower().endswith('.csv'):
                        full_path = os.path.join(root, file)
                        if full_path not in self.file_paths:
                            csv_files.append(full_path)
            
            # Add unique files
            self.file_paths.extend(csv_files)
            self.update_files_list()
            
            if csv_files:
                self.status_label.setText(f"Added {len(csv_files)} files from directory")
            else:
                self.status_label.setText("No new CSV files found in directory")
    
    def remove_selected(self):
        """Remove selected files from the list."""
        selected_items = self.files_list.selectedItems()
        if not selected_items:
            return
        
        count = len(selected_items)
        for item in selected_items:
            file_path = item.data(Qt.UserRole)
            if file_path in self.file_paths:
                self.file_paths.remove(file_path)
        
        self.update_files_list()
        self.status_label.setText(f"Removed {count} files")
    
    def clear_all(self):
        """Clear all files from the list."""
        if not self.file_paths:
            return
            
        count = len(self.file_paths)
        self.file_paths.clear()
        self.update_files_list()
        self.status_label.setText(f"Removed all {count} files")
    
    def open_file_location(self, item):
        """Open the file location in explorer when double-clicked."""
        file_path = item.data(Qt.UserRole)
        if os.path.exists(file_path):
            # Open file directory
            directory = os.path.dirname(file_path)
            QDesktopServices.openUrl(QUrl.fromLocalFile(directory))
    
    def format_size(self, size_bytes):
        """Format file size in human-readable format."""
        if size_bytes < 1024:
            return f"{size_bytes} B"
        elif size_bytes < 1024 * 1024:
            return f"{size_bytes / 1024:.1f} KB"
        elif size_bytes < 1024 * 1024 * 1024:
            return f"{size_bytes / (1024 * 1024):.1f} MB"
        else:
            return f"{size_bytes / (1024 * 1024 * 1024):.1f} GB"
    
    def dragEnterEvent(self, event):
        """Handle drag enter events for drag-and-drop operation."""
        if event.mimeData().hasUrls():
            event.acceptProposedAction()
    
    def dropEvent(self, event):
        """Handle drop events for drag-and-drop file operation."""
        if event.mimeData().hasUrls():
            urls = event.mimeData().urls()
            new_files = []
            
            for url in urls:
                file_path = url.toLocalFile()
                if file_path.lower().endswith('.csv') and file_path not in self.file_paths:
                    new_files.append(file_path)
            
            if new_files:
                self.file_paths.extend(new_files)
                self.update_files_list()
                self.status_label.setText(f"Added {len(new_files)} files via drag and drop")
                
            event.acceptProposedAction()

class MainWindow(QMainWindow):
    """Main application window with enhanced UI for a more modern look and feel."""
    
    def __init__(self):
        """Initialize the main application window with all UI components."""
        super().__init__()
        
        self._ensure_env_file_exists() # Ensure .env file exists before anything else
        
        # Initialize database connection
        self.db = DatabaseConnection()
        
        # Set default output directory to root/output instead of using paths.json
        self.output_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "output")
        # Create the output directory if it doesn't exist
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Initialize processing state variables
        self.current_process = None
        self.start_time = None
        self.original_force_overwrite_state = False  # Store original state to restore later
        
        # Initialize query execution
        self.query_thread = None
        self.execute_stop_event = threading.Event()
        self.query_elapsed_timer = QTimer(self)
        self.query_start_time = None
        
        # Set application icon if available
        try:
            icon_path = ThemeManager.get_icon_path("app_icon")
            if icon_path:
                self.setWindowIcon(QIcon(icon_path))
        except:
            pass
            
        self.setup_ui()
        self.check_db_connection()
        self.load_paths_config()
        
        # Start a timer to periodically update UI elements
        self.refresh_timer = QTimer(self)
        self.refresh_timer.timeout.connect(self.refresh_ui)
        self.refresh_timer.start(10000)  # Update every 10 seconds
        
        # Enhance tooltips to make them more visible
        tooltip_font = QFont()
        tooltip_font.setPointSize(10)
        QToolTip.setFont(tooltip_font)
        QApplication.setEffectEnabled(Qt.UI_AnimateTooltip, True)
        # Set a longer display time for tooltips (10 seconds instead of default)
        QApplication.instance().setProperty("tooltipDelay", 500)  # 500ms before showing
        QApplication.instance().setProperty("tooltipTime", 10000)  # 10 seconds display time
    
    def setup_ui(self):
        """Set up the main UI components with modern styling."""
        self.setWindowTitle("The Chefz - Transactions Processor")
        self.setMinimumSize(1000, 700)
        
        # Setup central widget with gradient background
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        
        # Status bar for showing connection status with icons
        self.statusBar = QStatusBar()
        self.statusBar.setContentsMargins(10, 0, 10, 0)
        self.setStatusBar(self.statusBar)
        
        # Status indicators in the status bar
        self.db_status_indicator = QLabel()
        self.db_status_indicator.setPixmap(self.style().standardIcon(QStyle.SP_DialogApplyButton).pixmap(16, 16))
        self.statusBar.addPermanentWidget(self.db_status_indicator)
        
        # Create tab widget with custom styling
        self.tabs = QTabWidget()
        self.tabs.setDocumentMode(True)  # More modern look
        self.tabs.setTabPosition(QTabWidget.North)
        self.tabs.setMovable(True)       # Allow rearranging tabs
        
        # Make tabs wider
        self.tabs.setStyleSheet("""
            QTabBar::tab {
                min-width: 150px;
                padding: 10px 15px;
                margin-right: 2px;
            }
        """)
        
        # Main layout
        main_layout = QVBoxLayout(central_widget)
        main_layout.setContentsMargins(10, 10, 10, 10)
        main_layout.addWidget(self.tabs)
        
        # Create tabs
        self.setup_sources_tab()
        self.setup_processing_tab()
        self.setup_query_generator_tab()  # Moved before output tab
        self.setup_output_tab()
        
        # Set up menu bar and toolbar
        self.setup_menu()
        self.setup_toolbar()
    
    def setup_menu(self):
        """Set up the application menu bar with icons and keyboard shortcuts."""
        menubar = self.menuBar()
        
        # File menu
        file_menu = menubar.addMenu("&File")
        
        # New config action
        new_config_action = QAction(self.style().standardIcon(QStyle.SP_FileIcon), "&New Configuration", self)
        new_config_action.setShortcut(QKeySequence.New)
        new_config_action.triggered.connect(self.new_configuration)
        file_menu.addAction(new_config_action)
        
        # Load config action
        load_config_action = QAction(self.style().standardIcon(QStyle.SP_DialogOpenButton), "&Load Configuration", self)
        load_config_action.setShortcut(QKeySequence.Open)
        load_config_action.triggered.connect(self.load_configuration)
        file_menu.addAction(load_config_action)
        
        # Save config action
        save_config_action = QAction(self.style().standardIcon(QStyle.SP_DialogSaveButton), "&Save Configuration", self)
        save_config_action.setShortcut(QKeySequence.Save)
        save_config_action.triggered.connect(self.save_paths_json)
        file_menu.addAction(save_config_action)
        
        file_menu.addSeparator()
        
        # Load application config action
        load_app_config_action = QAction(self.style().standardIcon(QStyle.SP_DialogOpenButton), "Load &Application Settings", self)
        load_app_config_action.setShortcut("Ctrl+Shift+O")
        load_app_config_action.triggered.connect(self.load_application_config)
        file_menu.addAction(load_app_config_action)
        
        # Save application config action
        save_app_config_action = QAction(self.style().standardIcon(QStyle.SP_DialogSaveButton), "Save A&pplication Settings", self)
        save_app_config_action.setShortcut("Ctrl+Shift+S")
        save_app_config_action.triggered.connect(self.save_application_config)
        file_menu.addAction(save_app_config_action)
        
        file_menu.addSeparator()
        
        # Exit action
        exit_action = QAction(self.style().standardIcon(QStyle.SP_DialogCloseButton), "E&xit", self)
        exit_action.setShortcut(QKeySequence.Quit)
        exit_action.triggered.connect(self.close)
        file_menu.addAction(exit_action)
        
        # Database menu
        db_menu = menubar.addMenu("&Database")
        
        # Check connection action
        check_conn_action = QAction(self.style().standardIcon(QStyle.SP_BrowserReload), "&Check Connection", self)
        check_conn_action.setShortcut("Ctrl+R")
        check_conn_action.triggered.connect(self.check_db_connection)
        db_menu.addAction(check_conn_action)
        
        # Configure database action
        config_db_action = QAction(self.style().standardIcon(QStyle.SP_FileDialogDetailedView), "&Configure Database", self)
        config_db_action.triggered.connect(self.configure_database)
        db_menu.addAction(config_db_action)
        
        # Tools menu
        tools_menu = menubar.addMenu("&Tools")
        
        # Run pipeline action
        run_action = QAction(self.style().standardIcon(QStyle.SP_MediaPlay), "&Run Pipeline", self)
        run_action.setShortcut("F5")
        run_action.triggered.connect(self.start_processing)
        tools_menu.addAction(run_action)
        
        # Stop process action
        stop_action = QAction(self.style().standardIcon(QStyle.SP_MediaStop), "Stop &Process", self)
        stop_action.setShortcut("F6")
        stop_action.setEnabled(False)
        stop_action.triggered.connect(self.stop_processing)
        tools_menu.addAction(stop_action)
        self.stop_action = stop_action  # Store reference for enabling/disabling
        
        tools_menu.addSeparator()
        
        # Settings action
        settings_action = QAction(self.style().standardIcon(QStyle.SP_FileDialogDetailedView), "&Settings", self)
        settings_action.triggered.connect(self.show_settings)
        tools_menu.addAction(settings_action)
        
        # Help menu
        help_menu = menubar.addMenu("&Help")
        
        # Documentation action
        docs_action = QAction(self.style().standardIcon(QStyle.SP_DialogHelpButton), "&Documentation", self)
        docs_action.triggered.connect(self.show_documentation)
        help_menu.addAction(docs_action)
        
        # About action
        about_action = QAction(self.style().standardIcon(QStyle.SP_FileDialogInfoView), "&About", self)
        about_action.triggered.connect(self.show_about)
        help_menu.addAction(about_action)
    
    def setup_toolbar(self):
        """Set up the main toolbar with quick access buttons."""
        toolbar = QToolBar("Main Toolbar")
        toolbar.setMovable(False)
        toolbar.setIconSize(QSize(24, 24))
        toolbar.setToolButtonStyle(Qt.ToolButtonTextBesideIcon)
        
        # Add actions to toolbar
        toolbar.addAction(self.style().standardIcon(QStyle.SP_FileIcon), "New", self.new_configuration)
        toolbar.addAction(self.style().standardIcon(QStyle.SP_DialogSaveButton), "Save", self.save_paths_json)
        toolbar.addSeparator()
        
        # Add app config actions
        save_app_action = toolbar.addAction(self.style().standardIcon(QStyle.SP_DialogSaveButton), "Save Settings", self.save_application_config)
        save_app_action.setToolTip("Save all application settings to a file")
        
        load_app_action = toolbar.addAction(self.style().standardIcon(QStyle.SP_DialogOpenButton), "Load Settings", self.load_application_config)
        load_app_action.setToolTip("Load application settings from a file")
        
        toolbar.addSeparator()
        
        toolbar.addAction(self.style().standardIcon(QStyle.SP_BrowserReload), "Check DB", self.check_db_connection)
        toolbar.addSeparator()
        
        # Run button
        run_action = toolbar.addAction(self.style().standardIcon(QStyle.SP_MediaPlay), "Run", self.start_processing)
        self.run_action = run_action  # Store reference
        
        # Stop button
        stop_action = toolbar.addAction(self.style().standardIcon(QStyle.SP_MediaStop), "Stop", self.stop_processing)
        stop_action.setEnabled(False)
        self.toolbar_stop_action = stop_action  # Store reference
        
        self.addToolBar(toolbar)
    
    def setup_sources_tab(self):
        """Set up the enhanced data sources management tab."""
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setContentsMargins(10, 10, 10, 10)
        layout.setSpacing(10)
        
        # Database connection card with visual status
        db_group = QGroupBox("Database Connection")
        db_layout = QVBoxLayout()
        
        # Connection status with icon
        status_layout = QHBoxLayout()
        self.db_status_icon = QLabel()
        self.db_status_icon.setPixmap(self.style().standardIcon(QStyle.SP_DialogCancelButton).pixmap(16, 16))
        self.db_status_label = QLabel("Connection status: Not connected")
        self.db_status_label.setStyleSheet("font-weight: bold;")
        
        status_layout.addWidget(self.db_status_icon)
        status_layout.addWidget(self.db_status_label)
        status_layout.addStretch()
        
        # Connection details
        self.db_info_label = QLabel("Connection details: None")
        
        # Quick actions
        actions_layout = QHBoxLayout()
        
        check_db_btn = QPushButton("Check Connection")
        check_db_btn.setIcon(self.style().standardIcon(QStyle.SP_BrowserReload))
        check_db_btn.clicked.connect(self.check_db_connection)
        
        config_db_btn = QPushButton("Configure Database")
        config_db_btn.setIcon(self.style().standardIcon(QStyle.SP_FileDialogDetailedView))
        config_db_btn.clicked.connect(self.configure_database)
        
        actions_layout.addWidget(check_db_btn)
        actions_layout.addWidget(config_db_btn)
        actions_layout.addStretch()
        
        db_layout.addLayout(status_layout)
        db_layout.addWidget(self.db_info_label)
        db_layout.addLayout(actions_layout)
        
        db_group.setLayout(db_layout)
        layout.addWidget(db_group)
        
        # Data sources list with interactive features
        sources_group = QGroupBox("Data Sources")
        sources_layout = QVBoxLayout()
        
        # Header section with refresh button
        header_layout = QHBoxLayout()
        sources_title = QLabel("Available Sources")
        sources_title.setStyleSheet("font-weight: bold; font-size: 14px;")
        
        refresh_btn = QToolButton()
        refresh_btn.setIcon(self.style().standardIcon(QStyle.SP_BrowserReload))
        refresh_btn.setToolTip("Refresh source list")
        refresh_btn.clicked.connect(self.load_paths_config)
        
        header_layout.addWidget(sources_title)
        header_layout.addStretch()
        header_layout.addWidget(refresh_btn)
        
        sources_layout.addLayout(header_layout)
        
        # Enhanced table with icons and better formatting
        self.sources_table = QTableWidget(0, 3)  # Added column for actions
        self.sources_table.setHorizontalHeaderLabels(["Source", "Files", "Actions"])
        self.sources_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeToContents)
        self.sources_table.horizontalHeader().setSectionResizeMode(1, QHeaderView.Stretch)
        self.sources_table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeToContents)
        self.sources_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.sources_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.sources_table.verticalHeader().setVisible(False)
        self.sources_table.setAlternatingRowColors(True)
        
        # Connect item double-click to edit function
        self.sources_table.itemDoubleClicked.connect(self.handle_source_double_click)
        
        sources_layout.addWidget(self.sources_table)
        
        # Action buttons with icons
        btn_layout = QHBoxLayout()
        
        edit_source_btn = QPushButton("Edit Source")
        edit_source_btn.setIcon(self.style().standardIcon(QStyle.SP_FileDialogDetailedView))
        edit_source_btn.clicked.connect(self.edit_source)
        
        save_paths_btn = QPushButton("Save Configuration")
        save_paths_btn.setIcon(self.style().standardIcon(QStyle.SP_DialogSaveButton))
        save_paths_btn.clicked.connect(self.save_paths_json)
        
        btn_layout.addWidget(edit_source_btn)
        btn_layout.addWidget(save_paths_btn)
        btn_layout.addStretch()
        
        sources_layout.addLayout(btn_layout)
        sources_group.setLayout(sources_layout)
        
        layout.addWidget(sources_group)
        
        self.tabs.addTab(tab, "Data Sources")
    
    def setup_processing_tab(self):
        """Set up the enhanced processing control tab."""
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setContentsMargins(10, 10, 10, 10)
        layout.setSpacing(15)
        
        # Upper section split into two columns
        upper_layout = QHBoxLayout()
        
        # Left column - Processing controls
        controls_group = QGroupBox("Processing Controls")
        controls_layout = QVBoxLayout()
        
        # Add processing mode selection
        mode_group = QGroupBox("Processing Mode")
        mode_layout = QVBoxLayout()
        
        self.process_new_data_radio = QRadioButton("Process New Data (overwrites existing data)")
        self.process_new_data_radio.setChecked(True)
        
        self.use_existing_data_radio = QRadioButton("Use Existing Data (no tables available)")
        self.use_existing_data_radio.setEnabled(False)
        
        # Add force overwrite checkbox
        self.force_overwrite_check = QCheckBox("Force Overwrite Without Confirmation")
        self.force_overwrite_check.setToolTip("When checked, existing data will be overwritten without asking for confirmation")
        
        # Add parallel processing checkbox
        self.parallel_check = QCheckBox("Use Parallel Processing")
        self.parallel_check.setChecked(True)
        self.parallel_check.setToolTip("When checked, processing will use multiple CPU cores for faster execution")
        
        # Add max workers spinbox
        workers_layout = QHBoxLayout()
        workers_layout.addWidget(QLabel("Max Workers:"))
        self.max_workers_spin = QSpinBox()
        self.max_workers_spin.setRange(1, 16)
        self.max_workers_spin.setValue(min(os.cpu_count() - 1 if os.cpu_count() else 4, 8))
        self.max_workers_spin.setToolTip("Maximum number of worker processes to use")
        workers_layout.addWidget(self.max_workers_spin)
        workers_layout.addStretch()
        
        # Add batch size spinbox
        batch_layout = QHBoxLayout()
        batch_layout.addWidget(QLabel("Batch Size:"))
        self.batch_size_spin = QSpinBox()
        self.batch_size_spin.setRange(10000, 1000000)
        self.batch_size_spin.setSingleStep(10000)
        self.batch_size_spin.setValue(250000)
        self.batch_size_spin.setToolTip("Number of records to process in each batch")
        batch_layout.addWidget(self.batch_size_spin)
        batch_layout.addStretch()
        
        mode_layout.addWidget(self.process_new_data_radio)
        mode_layout.addWidget(self.use_existing_data_radio)
        mode_layout.addWidget(self.force_overwrite_check)
        mode_layout.addWidget(self.parallel_check)
        mode_layout.addLayout(workers_layout)
        mode_layout.addLayout(batch_layout)
        mode_group.setLayout(mode_layout)
        controls_layout.addWidget(mode_group)
        
        # Action buttons with descriptive text
        button_layout = QHBoxLayout()
        
        self.process_btn = QPushButton("Start Processing")
        self.process_btn.setIcon(self.style().standardIcon(QStyle.SP_MediaPlay))
        self.process_btn.setIconSize(QSize(24, 24))
        self.process_btn.clicked.connect(self.start_processing)
        self.process_btn.setMinimumHeight(40)
        self.process_btn.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        
        self.stop_btn = QPushButton("Stop")
        self.stop_btn.setIcon(self.style().standardIcon(QStyle.SP_MediaStop))
        self.stop_btn.setIconSize(QSize(24, 24))
        self.stop_btn.clicked.connect(self.stop_processing)
        self.stop_btn.setEnabled(False)
        self.stop_btn.setMinimumHeight(40)
        self.stop_btn.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        
        button_layout.addWidget(self.process_btn)
        button_layout.addWidget(self.stop_btn)
        
        # Current operation status display
        status_frame = QFrame()
        status_frame.setFrameShape(QFrame.StyledPanel)
        status_frame.setStyleSheet("background-color: #f8f9fa; border-radius: 4px; padding: 8px;")
        status_layout = QVBoxLayout(status_frame)
        
        status_title = QLabel("Current Operation")
        status_title.setStyleSheet("font-weight: bold;")
        
        self.current_operation_label = QLabel("No operation in progress")
        
        status_layout.addWidget(status_title)
        status_layout.addWidget(self.current_operation_label)
        
        controls_layout.addLayout(button_layout)
        controls_layout.addWidget(status_frame)
        
        # Estimated completion section
        time_frame = QFrame()
        time_frame.setFrameShape(QFrame.StyledPanel)
        time_frame.setStyleSheet("background-color: #f8f9fa; border-radius: 4px; padding: 8px;")
        time_layout = QVBoxLayout(time_frame)
        
        time_title = QLabel("Estimated Completion")
        time_title.setStyleSheet("font-weight: bold;")
        
        self.time_estimate_label = QLabel("--:--:--")
        self.time_estimate_label.setStyleSheet("font-size: 16px;")
        
        time_layout.addWidget(time_title)
        time_layout.addWidget(self.time_estimate_label)
        
        controls_layout.addWidget(time_frame)
        controls_group.setLayout(controls_layout)
        
        # Right column - Progress tracking with visual indicators
        progress_group = QGroupBox("Progress")
        progress_layout = QVBoxLayout()
        
        # Step indicators with status icons
        step_frame = QFrame()
        step_frame.setFrameShape(QFrame.StyledPanel)
        step_frame.setStyleSheet("background-color: #f8f9fa; border-radius: 4px; padding: 8px;")
        step_layout = QVBoxLayout(step_frame)
        
        # Step 1: Generate Configuration
        step1_layout = QHBoxLayout()
        self.step1_icon = QLabel()
        self.step1_icon.setPixmap(self.style().standardIcon(QStyle.SP_DialogApplyButton).pixmap(16, 16))
        self.step1_icon.setVisible(False)  # Hide initially
        step1_label = QLabel("1. Generate Configuration")
        step1_layout.addWidget(self.step1_icon)
        step1_layout.addWidget(step1_label)
        step1_layout.addStretch()
        
        # Step 2: Clean and Process Data
        step2_layout = QHBoxLayout()
        self.step2_icon = QLabel()
        self.step2_icon.setPixmap(self.style().standardIcon(QStyle.SP_DialogApplyButton).pixmap(16, 16))
        self.step2_icon.setVisible(False)  # Hide initially
        step2_label = QLabel("2. Clean and Process Data")
        step2_layout.addWidget(self.step2_icon)
        step2_layout.addWidget(step2_label)
        step2_layout.addStretch()
        
        # Step 3: Load Data to PostgreSQL
        step3_layout = QHBoxLayout()
        self.step3_icon = QLabel()
        self.step3_icon.setPixmap(self.style().standardIcon(QStyle.SP_DialogApplyButton).pixmap(16, 16))
        self.step3_icon.setVisible(False)  # Hide initially
        step3_label = QLabel("3. Load Data to PostgreSQL")
        step3_layout.addWidget(self.step3_icon)
        step3_layout.addWidget(step3_label)
        step3_layout.addStretch()
        
        step_layout.addLayout(step1_layout)
        step_layout.addLayout(step2_layout)
        step_layout.addLayout(step3_layout)
        
        # Progress bar with percentage
        progress_bar_layout = QVBoxLayout()
        
        self.progress_bar = QProgressBar()
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(0)
        self.progress_bar.setTextVisible(True)
        self.progress_bar.setFormat("%p% completed")
        self.progress_bar.setMinimumHeight(24)
        
        self.status_label = QLabel("Ready")
        self.status_label.setAlignment(Qt.AlignCenter)
        
        progress_bar_layout.addWidget(self.progress_bar)
        progress_bar_layout.addWidget(self.status_label)
        
        progress_layout.addWidget(step_frame)
        progress_layout.addLayout(progress_bar_layout)
        
        progress_group.setLayout(progress_layout)
        
        # Add both columns to the upper layout
        upper_layout.addWidget(controls_group)
        upper_layout.addWidget(progress_group)
        # Adjust stretch factors for better responsiveness
        upper_layout.setStretchFactor(controls_group, 1) # Controls group takes 1 part of horizontal space
        upper_layout.setStretchFactor(progress_group, 2) # Progress group takes 2 parts, allowing it to expand more
        
        # Lower section - Log output with filter options
        log_group = QGroupBox("Process Log")
        log_layout = QVBoxLayout()
        
        # Log filter options
        filter_layout = QHBoxLayout()
        
        filter_label = QLabel("Filter:")
        self.log_filter = QLineEdit()
        self.log_filter.setPlaceholderText("Filter log entries...")
        self.log_filter.setClearButtonEnabled(True)
        self.log_filter.textChanged.connect(self.filter_log)
        
        self.log_level_combo = QComboBox()
        self.log_level_combo.addItems(["All Levels", "INFO", "WARNING", "ERROR", "DEBUG"])
        self.log_level_combo.currentTextChanged.connect(self.filter_log)
        
        clear_log_btn = QPushButton("Clear Log")
        clear_log_btn.clicked.connect(self.clear_log)
        
        filter_layout.addWidget(filter_label)
        filter_layout.addWidget(self.log_filter, 1)  # Stretch factor
        filter_layout.addWidget(QLabel("Level:"))
        filter_layout.addWidget(self.log_level_combo)
        filter_layout.addWidget(clear_log_btn)
        
        # Enhanced log output with syntax highlighting
        self.log_output = QTextEdit()
        self.log_output.setReadOnly(True)
        self.log_output.setLineWrapMode(QTextEdit.NoWrap)  # Better for log viewing
        self.log_output.setFont(QFont("Consolas", 9))  # Monospaced font
        
        log_layout.addLayout(filter_layout)
        log_layout.addWidget(self.log_output)
        
        log_group.setLayout(log_layout)
        
        # Add sections to main layout
        layout.addLayout(upper_layout)
        layout.addWidget(log_group, 1)  # Give log area more vertical space
        
        tab.setLayout(layout)
        self.tabs.addTab(tab, "Processing")
        
        # Check for existing database tables
        self.check_db_connection()
    
    def setup_output_tab(self):
        """Set up the enhanced output configuration and filter tab."""
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setContentsMargins(10, 10, 10, 10)
        layout.setSpacing(15)
        
        # Create a splitter for resizable sections
        splitter = QSplitter(Qt.Vertical)
        
        # Top section - Configuration and query execution
        top_config_widget = QWidget()
        top_layout = QVBoxLayout(top_config_widget)
        top_layout.setContentsMargins(0, 0, 0, 0)
        
        # Output configuration section
        output_group = QGroupBox("Output Configuration")
        config_layout = QFormLayout()
        
        # Output directory with browse button
        self.output_dir_edit = QLineEdit(self.output_dir)
        self.output_dir_edit.setReadOnly(True)
        
        browse_output_btn = QPushButton("Browse...")
        browse_output_btn.setIcon(self.style().standardIcon(QStyle.SP_DirOpenIcon))
        browse_output_btn.clicked.connect(self.browse_output_dir)
        
        output_dir_layout = QHBoxLayout()
        output_dir_layout.addWidget(self.output_dir_edit)
        output_dir_layout.addWidget(browse_output_btn)
        
        config_layout.addRow("Output Directory:", output_dir_layout)
        
        # SQL Query file selection
        self.query_file_edit = QLineEdit("payment_analysis.sql")
        
        browse_query_btn = QPushButton("Browse...")
        browse_query_btn.setIcon(self.style().standardIcon(QStyle.SP_FileDialogDetailedView))
        browse_query_btn.clicked.connect(self.browse_query_file)
        
        query_file_layout = QHBoxLayout()
        query_file_layout.addWidget(self.query_file_edit)
        query_file_layout.addWidget(browse_query_btn)
        
        config_layout.addRow("SQL Query File:", query_file_layout)
        
        # Add output format selection
        self.output_format = QComboBox()
        self.output_format.addItems(["CSV (.csv)", "CSV (Native PostgreSQL)", "Excel (.xlsx)", "Parquet (.parquet)"])
        config_layout.addRow("Output Format:", self.output_format)
        
        output_group.setLayout(config_layout)
        top_layout.addWidget(output_group)
        
        # Query execution options
        exec_group = QGroupBox("Query Execution Options")
        exec_layout = QGridLayout()
        
        # Batch size adjustment
        self.query_batch_size_spin = QSpinBox()
        self.query_batch_size_spin.setRange(1000, 1000000)
        self.query_batch_size_spin.setSingleStep(10000)
        self.query_batch_size_spin.setValue(250000)
        exec_layout.addWidget(QLabel("Batch Size:"), 0, 0)
        exec_layout.addWidget(self.query_batch_size_spin, 0, 1)
        
        # Chunk size for results
        self.chunk_size_spin = QSpinBox()
        self.chunk_size_spin.setRange(1000, 500000)
        self.chunk_size_spin.setSingleStep(5000)
        self.chunk_size_spin.setValue(100000)
        exec_layout.addWidget(QLabel("Chunk Size:"), 0, 2)
        exec_layout.addWidget(self.chunk_size_spin, 0, 3)
        
        # Maximum workers configuration
        self.query_max_workers_spin = QSpinBox()
        self.query_max_workers_spin.setRange(1, 16)
        self.query_max_workers_spin.setValue(min(os.cpu_count() - 1 if os.cpu_count() else 4, 8))
        self.query_max_workers_spin.setToolTip("Maximum number of worker processes to use")
        exec_layout.addWidget(QLabel("Max Workers:"), 1, 0)
        exec_layout.addWidget(self.query_max_workers_spin, 1, 1)
        
        # Query timeout (seconds)
        self.query_timeout_spin = QSpinBox()
        self.query_timeout_spin.setRange(30, 7200)  # 30 seconds to 2 hours
        self.query_timeout_spin.setSingleStep(30)
        self.query_timeout_spin.setValue(360)  # 6 minutes default
        self.query_timeout_spin.setToolTip("Query timeout in seconds")
        exec_layout.addWidget(QLabel("Timeout (seconds):"), 1, 2)
        exec_layout.addWidget(self.query_timeout_spin, 1, 3)
        
        # Processing options
        options_layout = QHBoxLayout()
        
        self.use_multiprocessing_check = QCheckBox("Use Multiprocessing")
        self.use_multiprocessing_check.setChecked(True)
        self.use_multiprocessing_check.setToolTip("Enable parallel processing for faster query execution")
        
        self.include_view_creation_check = QCheckBox("Include View Creation")
        self.include_view_creation_check.setChecked(True)
        self.include_view_creation_check.setToolTip("Include CREATE VIEW statements in the query")
        
        self.include_index_creation_check = QCheckBox("Include Index Creation")
        self.include_index_creation_check.setChecked(True)
        self.include_index_creation_check.setToolTip("Include CREATE INDEX statements in the query")
        
        options_layout.addWidget(self.use_multiprocessing_check)
        options_layout.addWidget(self.include_view_creation_check)
        options_layout.addWidget(self.include_index_creation_check)
        options_layout.addStretch()
        
        exec_layout.addLayout(options_layout, 2, 0, 1, 4)
        
        # Execution action buttons
        action_layout = QHBoxLayout()
        
        self.execute_query_btn = QPushButton("Execute Query")
        self.execute_query_btn.setIcon(self.style().standardIcon(QStyle.SP_MediaPlay))
        self.execute_query_btn.clicked.connect(self.execute_query)
        self.execute_query_btn.setMinimumHeight(40)
        self.execute_query_btn.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        
        self.stop_query_btn = QPushButton("Stop")
        self.stop_query_btn.setIcon(self.style().standardIcon(QStyle.SP_MediaStop))
        self.stop_query_btn.clicked.connect(self.stop_query_execution)
        self.stop_query_btn.setEnabled(False)
        self.stop_query_btn.setMinimumHeight(40)
        self.stop_query_btn.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        
        self.view_results_btn = QPushButton("View Results")
        self.view_results_btn.setIcon(self.style().standardIcon(QStyle.SP_FileDialogContentsView))
        self.view_results_btn.clicked.connect(self.view_query_results)
        self.view_results_btn.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        
        action_layout.addWidget(self.execute_query_btn)
        action_layout.addWidget(self.stop_query_btn)
        action_layout.addWidget(self.view_results_btn)
        action_layout.addStretch()
        
        exec_layout.addLayout(action_layout, 3, 0, 1, 4)
        
        exec_group.setLayout(exec_layout)
        top_layout.addWidget(exec_group)
        
        # Progress tracking
        progress_group = QGroupBox("Query Progress")
        progress_layout = QVBoxLayout()
        
        self.query_progress_bar = QProgressBar()
        self.query_progress_bar.setRange(0, 100)
        self.query_progress_bar.setValue(0)
        
        self.query_status_label = QLabel("Ready")
        self.query_time_label = QLabel("--:--:--")
        
        progress_layout.addWidget(self.query_progress_bar)
        
        status_layout = QHBoxLayout()
        status_layout.addWidget(QLabel("Status:"))
        status_layout.addWidget(self.query_status_label)
        status_layout.addStretch()
        status_layout.addWidget(QLabel("Time:"))
        status_layout.addWidget(self.query_time_label)
        
        progress_layout.addLayout(status_layout)
        
        progress_group.setLayout(progress_layout)
        top_layout.addWidget(progress_group)
        
        # Output preview section with table
        preview_group = QGroupBox("Output Preview")
        preview_layout = QVBoxLayout()
        
        # Preview table with sample data
        self.preview_table = QTableWidget(0, 5)
        self.preview_table.setHorizontalHeaderLabels(["Order ID", "Date", "Amount", "Method", "Status"])
        self.preview_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        
        # Set minimum height for preview table
        self.preview_table.setMinimumHeight(200)
        
        preview_layout.addWidget(self.preview_table)
        
        # Action buttons for preview
        preview_buttons = QHBoxLayout()
        
        refresh_preview_btn = QPushButton("Refresh Preview")
        refresh_preview_btn.setIcon(self.style().standardIcon(QStyle.SP_BrowserReload))
        refresh_preview_btn.clicked.connect(self.refresh_preview)
        
        export_preview_btn = QPushButton("Export Preview")
        export_preview_btn.setIcon(self.style().standardIcon(QStyle.SP_DialogSaveButton))
        export_preview_btn.clicked.connect(self.export_preview)
        
        preview_buttons.addWidget(refresh_preview_btn)
        preview_buttons.addWidget(export_preview_btn)
        preview_buttons.addStretch()
        
        preview_layout.addLayout(preview_buttons)
        
        preview_group.setLayout(preview_layout)
        
        # Add sections to the splitter
        splitter.addWidget(top_config_widget)
        splitter.addWidget(preview_group)
        
        # Set initial sizes
        splitter.setSizes([600, 400])
        
        layout.addWidget(splitter)
        tab.setLayout(layout)
        self.tabs.addTab(tab, "Output")
        
        # Store query execution state
        self.query_thread = None
        self.query_stop_event = None
    
    def browse_query_file(self):
        """Open file dialog to select SQL query file."""
        file_path, _ = QFileDialog.getOpenFileName(
            self, "Select SQL Query File", "", "SQL Files (*.sql)"
        )
        
        if file_path:
            self.query_file_edit.setText(file_path)
            self.statusBar.showMessage(f"SQL query file set to: {file_path}", 5000)
    
    def execute_query(self):
        """Execute the SQL query with selected options."""
        try:
            # Log start of query execution
            self.log_output.append("<span style='color:#2ecc71'><b>Starting query execution...</b></span>")
            
            # Check database connection first
            if not self.db.connected:
                self.log_output.append("<span style='color:#e74c3c'><b>Database not connected, attempting to connect...</b></span>")
                success, message = self.db.connect_from_env()
                if not success:
                    error_msg = f"Database Connection Error: {message}"
                    self.log_output.append(f"<span style='color:#e74c3c'><b>{error_msg}</b></span>")
                    QMessageBox.warning(self, "Database Connection Error", message)
                    return
                self.log_output.append("<span style='color:#2ecc71'><b>Database connection established</b></span>")
            
            # Get output file path
            output_dir = self.output_dir_edit.text()
            os.makedirs(output_dir, exist_ok=True)
            self.log_output.append(f"Output directory: {output_dir}")
            
            # Determine output file format and extension
            format_text = self.output_format.currentText()
            # Set CSV as default format
            output_format_arg = "csv"
            ext = ".csv"
            
            if "Excel" in format_text:
                ext = ".xlsx"
                output_format_arg = "excel"
            elif "CSV (Native PostgreSQL)" in format_text:
                ext = ".csv"
                output_format_arg = "csv_native"
            elif "CSV" in format_text: # Must be after native CSV check
                ext = ".csv"
                output_format_arg = "csv"
            elif "Parquet" in format_text:
                ext = ".parquet"
                output_format_arg = "parquet"
            
            self.log_output.append(f"Selected output format: {format_text} (arg: {output_format_arg}, ext: {ext})")
            
            # Generate timestamp for filename
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

            # Check if we have a generated query from the Query Generator tab
            use_generated_query = False
            if hasattr(self, 'generated_query') and self.generated_query:
                self.log_output.append("Found generated query from Query Generator tab")
                query_content = self.generated_query
                output_file = os.path.join(output_dir, f"generated_query_results{ext}")
                use_generated_query = True
                
                # Ask user if they want to use the generated query
                reply = QMessageBox.question(
                    self, 
                    "Use Generated Query", 
                    "A query has been generated in the Query Generator tab. Do you want to use this query?\n\n"
                    "Yes: Use the generated query\n"
                    "No: Use the file specified in the SQL Query field",
                    QMessageBox.Yes | QMessageBox.No,
                    QMessageBox.Yes
                )
                
                if reply == QMessageBox.No:
                    use_generated_query = False
                    self.log_output.append("User chose to use SQL query file instead of generated query")
                else:
                    self.log_output.append("User confirmed to use generated query")
            
            # If not using generated query, or if user chose not to, use the specified file
            if not use_generated_query:
                # Get query file path
                query_file = self.query_file_edit.text()
                if not query_file or not os.path.exists(query_file):
                    error_msg = "Invalid Query File: Please select a valid SQL query file."
                    self.log_output.append(f"<span style='color:#e74c3c'><b>{error_msg}</b></span>")
                    QMessageBox.warning(self, "Invalid Query File", "Please select a valid SQL query file.")
                    return
                
                self.log_output.append(f"Using SQL query file: {query_file}")
                # Read the file to get the query content
                try:
                    with open(query_file, 'r') as f:
                        query_content = f.read() # Not strictly needed here if sql_executor handles file path
                    self.log_output.append(f"Successfully read query file ({len(query_content)} bytes)") 
                except Exception as e:
                    error_msg = f"Failed to read query file: {str(e)}"
                    self.log_output.append(f"<span style='color:#e74c3c'><b>{error_msg}</b></span>")
                    QMessageBox.warning(self, "Error", error_msg)
                    return
                
                output_filename_base = Path(query_file).stem
                output_file = os.path.join(output_dir, f"{output_filename_base}_results_{timestamp}{ext}")
            else:
                 # Using generated query from Query Generator tab
                output_file = os.path.join(output_dir, f"generated_query_results_{timestamp}{ext}")
            
            # Update UI state
            self.execute_query_btn.setEnabled(False)
            self.stop_query_btn.setEnabled(True)
            self.query_progress_bar.setValue(0)
            self.query_status_label.setText("Running...")
            self.query_time_label.setText("00:00:00")
            
            # Create a query options dictionary
            query_options = {
                "batch_size": self.query_batch_size_spin.value(),
                "chunk_size": self.chunk_size_spin.value(),
                "max_workers": self.query_max_workers_spin.value() if self.use_multiprocessing_check.isChecked() else 1,
                "timeout": self.query_timeout_spin.value()
            }
            
            # Only add date filter if it's explicitly enabled in the UI
            if hasattr(self, 'enable_date_filter_check') and self.enable_date_filter_check.isChecked():
                query_options["date_filter"] = {
                    "enabled": True,  # Explicitly mark as enabled
                    "start_date": self.query_start_date.date().toString("yyyy-MM-dd"),
                    "end_date": self.query_end_date.date().toString("yyyy-MM-dd")
                }
                self.log_output.append(f"Date filtering enabled: {query_options['date_filter']['start_date']} to {query_options['date_filter']['end_date']}")
            else:
                # Pass empty date_filter with enabled=False to be explicit
                query_options["date_filter"] = {
                    "enabled": False
                }
                self.log_output.append("Date filtering disabled - executing query without date constraints.")
            
            # Log query parameters
            self.log_output.append(f"Query options: batch_size={query_options['batch_size']}, " + 
                                  f"chunk_size={query_options['chunk_size']}, " +
                                  f"max_workers={query_options['max_workers']}")
            
            # Add back the include_view_creation and include_index_creation options
            query_options["include_view_creation"] = self.include_view_creation_check.isChecked()
            query_options["include_index_creation"] = self.include_index_creation_check.isChecked()
            
            # If using generated query, write it to a temporary file
            if use_generated_query:
                try:
                    temp_dir = os.path.join(output_dir, "temp")
                    os.makedirs(temp_dir, exist_ok=True)
                    temp_file = os.path.join(temp_dir, "temp_generated_query.sql")
                    
                    self.log_output.append(f"Writing generated query to temporary file: {temp_file}")
                    
                    with open(temp_file, 'w') as f:
                        f.write(query_content)
                    
                    self.log_output.append(f"Generated query ({len(query_content)} bytes) saved to temporary file")
                    self.statusBar.showMessage(f"Generated query saved to {temp_file}", 3000)
                    query_file = temp_file
                except Exception as e:
                    error_msg = f"Failed to write generated query to file: {str(e)}"
                    self.log_output.append(f"<span style='color:#e74c3c'><b>{error_msg}</b></span>")
                    QMessageBox.warning(self, "Error", error_msg)
                    
                    # Reset UI
                    self.execute_query_btn.setEnabled(True)
                    self.stop_query_btn.setEnabled(False)
                    return
            
            self.log_output.append(f"Output will be saved to: {output_file}")
            
            # Start query execution thread
            self.log_output.append("Creating query execution thread...")
            self.query_stop_event = threading.Event()
            
            try:
                # Pass the connection string directly instead of the engine object
                self.query_thread = QueryExecutionThread(
                    self.db.conn_string, query_file, output_file, query_options, self.query_stop_event,
                    # Pass the determined output_format_arg to the thread
                    # The QueryExecutionThread will then pass it to sql_executor.run_sql_query
                    output_format_for_thread=output_format_arg
                )
                
                self.query_thread.progress_signal.connect(self.update_query_progress)
                self.query_thread.status_signal.connect(self.update_query_status)
                self.query_thread.time_signal.connect(self.update_query_time)
                self.query_thread.finished_signal.connect(self.query_execution_finished)
                self.query_thread.log_signal.connect(self.update_log)  # Connect the log signal
                
                # Start execution timer
                self.query_timer = QTimer(self)
                self.query_start_time = time.time()
                self.query_timer.timeout.connect(self.update_query_elapsed_time)
                self.query_timer.start(1000)  # Update every second
                
                # Start the thread
                self.log_output.append("<span style='color:#2ecc71'><b>Starting query execution thread...</b></span>")
                self.query_thread.start()
                
            except Exception as e:
                error_msg = f"Failed to start query execution thread: {str(e)}"
                self.log_output.append(f"<span style='color:#e74c3c'><b>{error_msg}</b></span>")
                self.log_output.append(traceback.format_exc())
                
                # Reset UI
                self.execute_query_btn.setEnabled(True)
                self.stop_query_btn.setEnabled(False)
                QMessageBox.critical(self, "Error", error_msg)
                
        except Exception as e:
            error_msg = f"Unexpected error in execute_query: {str(e)}"
            self.log_output.append(f"<span style='color:#e74c3c'><b>{error_msg}</b></span>")
            self.log_output.append(traceback.format_exc())
            
            # Reset UI
            self.execute_query_btn.setEnabled(True)
            self.stop_query_btn.setEnabled(False)
            QMessageBox.critical(self, "Error", error_msg)
    
    def stop_query_execution(self):
        """Stop the running query execution."""
        if self.query_thread and self.query_thread.isRunning() and self.query_stop_event:
            # Signal the thread to stop
            self.query_stop_event.set()
            self.query_status_label.setText("Stopping...")
    
    def update_query_progress(self, value):
        """Update query progress bar."""
        self.query_progress_bar.setValue(value)
    
    def update_query_status(self, status):
        """Update query status label."""
        self.query_status_label.setText(status)
    
    def update_query_time(self, time_str):
        """Update query time label."""
        self.query_time_label.setText(time_str)
    
    def update_query_elapsed_time(self):
        """Update elapsed time during query execution."""
        if not hasattr(self, 'query_start_time'):
            return
            
        elapsed = time.time() - self.query_start_time
        hours, remainder = divmod(int(elapsed), 3600)
        minutes, seconds = divmod(remainder, 60)
        self.query_time_label.setText(f"{hours:02}:{minutes:02}:{seconds:02}")
    
    def query_execution_finished(self, success, message, result_file=None):
        """Handle completion of query execution thread."""
        # Stop the timer
        if hasattr(self, 'query_timer'):
            self.query_timer.stop()
        
        # Update UI
        self.execute_query_btn.setEnabled(True)
        self.stop_query_btn.setEnabled(False)
        self.query_status_label.setText("Completed" if success else "Failed")
        
        # Add completion message to log
        if success:
            self.log_output.append(f"<span style='color:#2ecc71'><b>Query execution completed successfully</b></span>")
            if result_file and os.path.exists(result_file):
                self.log_output.append(f"Results saved to: {result_file}")
                self.statusBar.showMessage(f"Query completed. Results saved to {result_file}. Loading preview...", 10000)
                
                # Load result preview in a separate thread
                self.result_loader_thread = ResultLoadingThread(result_file)
                self.result_loader_thread.data_loaded.connect(self.handle_data_loaded_for_preview)
                self.result_loader_thread.loading_failed.connect(self.handle_data_loading_failed)
                self.result_loader_thread.start()
            else:
                self.statusBar.showMessage("Query completed successfully, but no result file was produced.")
        else:
            error_msg = f"Query execution failed: {message}"
            self.log_output.append(f"<span style='color:#e74c3c'><b>{error_msg}</b></span>")
            self.statusBar.showMessage(error_msg)
            
            # Show error dialog
            QMessageBox.critical(self, "Query Execution Failed", error_msg)
        
        # Clean up old connections to avoid leaving orphaned connections
        # This helps prevent the "too many clients already" error on PostgreSQL
        if hasattr(self, 'db') and self.db.connected and self.db.engine:
            try:
                # Dispose the engine to close all connections in the pool
                self.log_output.append("Cleaning up database connections...")
                self.db.engine.dispose()
                
                # Reconnect with a fresh engine
                success, message = self.db.connect_from_env()
                if success:
                    self.log_output.append("<span style='color:#2ecc71'>Database connection refreshed successfully</span>")
                else:
                    self.log_output.append(f"<span style='color:#e74c3c'>Failed to refresh database connection: {message}</span>")
            except Exception as e:
                self.log_output.append(f"<span style='color:#e74c3c'>Error during connection cleanup: {str(e)}</span>")
    
    def handle_data_loaded_for_preview(self, df, result_file):
        """Slot to handle successfully loaded DataFrame for preview."""
        self.update_preview_table(df)
        self.statusBar.showMessage(f"Preview loaded for {os.path.basename(result_file)}. Displaying first {df.shape[0]} rows.", 5000)

    def handle_data_loading_failed(self, error_message, result_file):
        """Slot to handle failures during data loading for preview."""
        self.log_output.append(f"<span style='color:#e74c3c'><b>{error_message}</b></span>")
        self.statusBar.showMessage(f"Failed to load preview for {os.path.basename(result_file)}.", 5000)
        QMessageBox.warning(self, "Preview Loading Failed", error_message)

    def load_result_preview(self, result_file):
        """Load the results into the preview table. (This will now be initiated by a thread)"""
        if not result_file or not os.path.exists(result_file):
            self.statusBar.showMessage("Error: Result file not found for preview.", 5000)
            return
        
        self.statusBar.showMessage(f"Initiating preview load for {os.path.basename(result_file)}...", 5000)
        self.result_loader_thread = ResultLoadingThread(result_file)
        self.result_loader_thread.data_loaded.connect(self.handle_data_loaded_for_preview)
        self.result_loader_thread.loading_failed.connect(self.handle_data_loading_failed)
        self.result_loader_thread.start()
    
    def update_preview_table(self, df):
        """Update the preview table with data from DataFrame."""
        if df is None:
            self.preview_table.setRowCount(0)
            self.preview_table.setColumnCount(0)
            self.preview_table.setHorizontalHeaderLabels([])
            self.statusBar.showMessage("No data to display in preview.", 3000)
            return

        # Clear the table
        self.preview_table.setRowCount(0)
        
        # Set the column headers based on DataFrame columns
        self.preview_table.setColumnCount(len(df.columns))
        self.preview_table.setHorizontalHeaderLabels(df.columns)
        
        # Add data from DataFrame
        for row_idx, row in df.iterrows():
            self.preview_table.insertRow(row_idx)
            for col_idx, value in enumerate(row):
                item = QTableWidgetItem(str(value) if pd.notna(value) else "")
                self.preview_table.setItem(row_idx, col_idx, item)
        
        # Adjust columns to fit content
        self.preview_table.resizeColumnsToContents()
    
    def refresh_preview(self):
        """Refresh the preview table with the latest data."""
        # Determine the output directory
        output_dir = self.output_dir_edit.text()
        query_file = self.query_file_edit.text()
        
        if not output_dir:
            QMessageBox.warning(self, "No Output Directory", "Please specify an output directory first.")
            return
            
        if not os.path.exists(output_dir):
            QMessageBox.warning(self, "Output Directory Not Found", f"The directory {output_dir} does not exist.")
            return
        
        # Get the base name of the query file to help with matching result files
        query_name = ""
        if query_file:
            query_name = Path(query_file).stem
        
        # Find all potential result files in the output directory
        result_files = []
        
        # Check for files with various extensions
        for ext in ['.xlsx', '.csv', '.parquet']:
            # Look for both old format (static) and new format (with timestamp)
            if query_name:
                # Pattern for files associated with specific query
                pattern = f"{query_name}_results*{ext}"
                result_files.extend(glob.glob(os.path.join(output_dir, pattern)))
            
            # Also include "generated_query_results" files
            pattern = f"generated_query_results*{ext}"
            result_files.extend(glob.glob(os.path.join(output_dir, pattern)))
        
        if not result_files:
            QMessageBox.information(self, "No Results", "No result files found in the output directory. Please run a query first.")
            return
        
        # Sort files by modification time (newest first)
        result_files.sort(key=os.path.getmtime, reverse=True)
        
        # Load preview for the most recent file
        most_recent_file = result_files[0]
        self.log_output.append(f"Loading preview for most recent result file: {most_recent_file}")
        self.load_result_preview(most_recent_file)
    
    def export_preview(self):
        """Export the current preview data to a file."""
        if self.preview_table.rowCount() == 0:
            QMessageBox.information(self, "No Data", "No data to export. Please load a preview first.")
            return
            
        file_path, selected_filter = QFileDialog.getSaveFileName(
            self, "Export Preview", "", "CSV Files (*.csv);;Excel Files (*.xlsx)"
        )
        
        if not file_path:
            return
            
        try:
            # Convert table data to DataFrame
            data = []
            headers = []
            
            # Get headers
            for col in range(self.preview_table.columnCount()):
                headers.append(self.preview_table.horizontalHeaderItem(col).text())
            
            # Get data
            for row in range(self.preview_table.rowCount()):
                row_data = []
                for col in range(self.preview_table.columnCount()):
                    item = self.preview_table.item(row, col)
                    row_data.append(item.text() if item else "")
                data.append(row_data)
            
            # Create DataFrame
            import pandas as pd
            df = pd.DataFrame(data, columns=headers)
            
            # Export based on file extension
            if file_path.endswith('.xlsx'):
                try:
                    # Check if openpyxl is available
                    import openpyxl
                    df.to_excel(file_path, index=False, engine='openpyxl')
                except ImportError:
                    QMessageBox.warning(self, "Export Warning", 
                                       "The openpyxl package is not installed. Using xlsxwriter as fallback.")
                    try:
                        # Try xlsxwriter as fallback
                        df.to_excel(file_path, index=False, engine='xlsxwriter')
                    except ImportError:
                        # If both fail, fall back to CSV
                        csv_path = file_path.replace('.xlsx', '.csv')
                        df.to_csv(csv_path, index=False)
                        QMessageBox.warning(self, "Export Changed", 
                                          f"Excel export failed. Data saved as CSV to {csv_path}")
                        return
            else:
                df.to_csv(file_path, index=False)
            
            QMessageBox.information(self, "Export Complete", f"Preview data exported to {file_path}")
            
        except Exception as e:
            QMessageBox.critical(self, "Export Error", f"Error exporting data: {str(e)}")
    
    def view_query_results(self):
        """Open the query results file in the default application."""
        # Determine the output directory
        output_dir = self.output_dir_edit.text()
        query_file = self.query_file_edit.text()
        
        if not output_dir:
            QMessageBox.warning(self, "No Output Directory", "Please specify an output directory first.")
            return
            
        if not os.path.exists(output_dir):
            QMessageBox.warning(self, "Output Directory Not Found", f"The directory {output_dir} does not exist.")
            return
        
        # Get the base name of the query file to help with matching result files
        query_name = ""
        if query_file:
            query_name = Path(query_file).stem
        
        # Find all potential result files in the output directory
        result_files = []
        
        # Check for files with various extensions
        for ext in ['.csv', '.xlsx', '.parquet']:
            # Look for both old format (static) and new format (with timestamp)
            if query_name:
                # Pattern for files associated with specific query
                pattern = f"{query_name}_results*{ext}"
                result_files.extend(glob.glob(os.path.join(output_dir, pattern)))
            
            # Also include "generated_query_results" files
            pattern = f"generated_query_results*{ext}"
            result_files.extend(glob.glob(os.path.join(output_dir, pattern)))
        
        if not result_files:
            QMessageBox.information(self, "No Results", "No result files found in the output directory. Please run a query first.")
            return
        
        # Sort files by modification time (newest first)
        result_files.sort(key=os.path.getmtime, reverse=True)
        
        # Open the most recent file
        most_recent_file = result_files[0]
        try:
            # Open the file with the default application
            self.log_output.append(f"Opening most recent result file: {most_recent_file}")
            if platform.system() == 'Windows':
                os.startfile(most_recent_file)
            elif platform.system() == 'Darwin':  # macOS
                subprocess.call(['open', most_recent_file])
            else:  # Linux
                subprocess.call(['xdg-open', most_recent_file])
            self.statusBar.showMessage(f"Opened result file: {os.path.basename(most_recent_file)}", 5000)
        except Exception as e:
            QMessageBox.warning(self, "Error", f"Could not open file: {str(e)}")
            self.log_output.append(f"<span style='color:#e74c3c'>Error opening result file: {str(e)}</span>")
    
    def update_batch_size(self, value):
        """Update batch size spinbox when slider changes."""
        batch_size = value * 10000
        self.batch_size_spin.setValue(batch_size)
    
    def update_batch_slider(self, value):
        """Update slider when batch size spinbox changes."""
        slider_value = value // 10000
        self.batch_size_slider.setValue(slider_value)
    
    def apply_filters(self):
        """Apply selected filters to the data."""
        QMessageBox.information(self, "Filters Applied", 
                               "Filters will be applied during the next processing run.")
    
    def update_sources_table(self, sources):
        """Update the sources table with interactive buttons and detailed information."""
        self.sources_table.setRowCount(0)
        
        row = 0
        for source, config in sources.items():
            self.sources_table.insertRow(row)
            
            # Source name with icon
            source_item = QTableWidgetItem(source.capitalize())
            source_item.setIcon(self.style().standardIcon(QStyle.SP_FileDialogDetailedView))
            self.sources_table.setItem(row, 0, source_item)
            
            # File count and details
            files = config.get("files", [])
            
            # More descriptive file info
            if not files:
                files_text = "No files configured"
                files_color = QColor("#ff6b6b")  # Red for warning
            else:
                total_size = 0
                try:
                    for file_path in files:
                        if os.path.exists(file_path):
                            total_size += os.path.getsize(file_path)
                except:
                    pass
                
                size_str = self.format_size(total_size)
                files_text = f"{len(files)} file(s) - {size_str}"
                files_color = QColor("#2ecc71") if len(files) > 0 else QColor("#ff6b6b")
                
            files_item = QTableWidgetItem(files_text)
            files_item.setForeground(files_color)
            self.sources_table.setItem(row, 1, files_item)
            
            # Add edit button in the actions column
            actions_widget = QWidget()
            actions_layout = QHBoxLayout(actions_widget)
            actions_layout.setContentsMargins(4, 2, 4, 2)
            
            edit_btn = QPushButton()
            edit_btn.setIcon(self.style().standardIcon(QStyle.SP_FileDialogDetailedView))
            edit_btn.setToolTip(f"Edit {source} files")
            edit_btn.setMaximumWidth(30)
            edit_btn.clicked.connect(lambda checked, s=source: self.edit_source_by_name(s))
            
            view_btn = QPushButton()
            view_btn.setIcon(self.style().standardIcon(QStyle.SP_FileDialogContentsView))
            view_btn.setToolTip(f"View {source} files")
            view_btn.setMaximumWidth(30)
            view_btn.clicked.connect(lambda checked, s=source: self.view_source_files(s))
            
            actions_layout.addWidget(edit_btn)
            actions_layout.addWidget(view_btn)
            actions_layout.addStretch()
            
            self.sources_table.setCellWidget(row, 2, actions_widget)
            row += 1
    
    def format_size(self, size_bytes):
        """Format file size in human-readable format."""
        if size_bytes < 1024:
            return f"{size_bytes} B"
        elif size_bytes < 1024 * 1024:
            return f"{size_bytes / 1024:.1f} KB"
        elif size_bytes < 1024 * 1024 * 1024:
            return f"{size_bytes / (1024 * 1024):.1f} MB"
        else:
            return f"{size_bytes / (1024 * 1024 * 1024):.1f} GB"
    
    def edit_source_by_name(self, source):
        """Edit source files by source name."""
        try:
            path = Path("paths.json")
            with open(path, 'r') as f:
                data = json.load(f)
            
            files = data["sources"].get(source, {}).get("files", [])
            
            # Open dialog to edit files
            try:
                dialog = SourceFilesDialog(source, files, self)
                if dialog.exec_() == QDialog.Accepted:
                    # Update paths.json with new files
                    data["sources"][source]["files"] = dialog.file_paths
                    
                    with open(path, 'w') as f:
                        json.dump(data, f, indent=4)
                    
                    # Update the UI
                    self.update_sources_table(data["sources"])
                    self.statusBar.showMessage(f"Updated files for source: {source}", 5000)
            except Exception as dialog_err:
                QMessageBox.critical(self, "Dialog Error", 
                                    f"Error creating source dialog: {str(dialog_err)}\n\n"
                                    f"Error type: {type(dialog_err).__name__}\n"
                                    f"Traceback: {traceback.format_exc()}")
                self.statusBar.showMessage(f"Dialog error: {str(dialog_err)}", 5000)
            
        except Exception as e:
            QMessageBox.warning(self, "Error", f"Failed to edit source: {str(e)}")
            self.statusBar.showMessage(f"Error editing source: {str(e)}", 5000)
    
    def view_source_files(self, source):
        """View source files in a dialog."""
        try:
            path = Path("paths.json")
            with open(path, 'r') as f:
                data = json.load(f)
            
            files = data["sources"].get(source, {}).get("files", [])
            
            if not files:
                QMessageBox.information(self, "No Files", f"No files configured for source: {source}")
                return
            
            # Create a simple dialog to view files
            dialog = QDialog(self)
            dialog.setWindowTitle(f"Files for {source.capitalize()}")
            dialog.setMinimumWidth(600)
            dialog.setMinimumHeight(400)
            
            layout = QVBoxLayout(dialog)
            
            # Create file list
            list_widget = QListWidget()
            for file_path in files:
                item = QListWidgetItem()
                
                # Set item text and tooltip
                file_name = os.path.basename(file_path)
                item.setText(file_name)
                item.setToolTip(file_path)
                
                # Set icon based on file type
                item.setIcon(self.style().standardIcon(QStyle.SP_FileIcon))
                
                # Add file path as data
                item.setData(Qt.UserRole, file_path)
                
                # Add file stats if available
                try:
                    if os.path.exists(file_path):
                        size = os.path.getsize(file_path)
                        modified = os.path.getmtime(file_path)
                        
                        size_str = self.format_size(size)
                        modified_str = datetime.fromtimestamp(modified).strftime('%Y-%m-%d %H:%M')
                        
                        item.setText(f"{file_name} ({size_str}) - Modified: {modified_str}")
                    else:
                        item.setText(f"{file_name} (File not found)")
                        item.setForeground(QColor("#ff6b6b"))
                except:
                    pass
                
                list_widget.addItem(item)
            
            layout.addWidget(list_widget)
            
            # Add button to open file location
            btn_layout = QHBoxLayout()
            
            open_location_btn = QPushButton("Open File Location")
            open_location_btn.setIcon(self.style().standardIcon(QStyle.SP_DirOpenIcon))
            open_location_btn.clicked.connect(lambda: self.open_selected_file_location(list_widget))
            
            close_btn = QPushButton("Close")
            close_btn.clicked.connect(dialog.accept)
            
            btn_layout.addWidget(open_location_btn)
            btn_layout.addStretch()
            btn_layout.addWidget(close_btn)
            
            layout.addLayout(btn_layout)
            
            dialog.exec_()
            
        except Exception as e:
            QMessageBox.warning(self, "Error", f"Failed to view source files: {str(e)}")
    
    def open_selected_file_location(self, list_widget):
        """Open the location of the selected file in file explorer."""
        selected_items = list_widget.selectedItems()
        if not selected_items:
            return
            
        file_path = selected_items[0].data(Qt.UserRole)
        if os.path.exists(file_path):
            # Open file directory
            directory = os.path.dirname(file_path)
            QDesktopServices.openUrl(QUrl.fromLocalFile(directory))
    
    def handle_source_double_click(self, item):
        """Handle double click on source table item."""
        if item.column() in [0, 1]:  # Source name or Files column
            row = item.row()
            source = self.sources_table.item(row, 0).text().lower()
            self.edit_source_by_name(source)
    
    def check_db_connection(self):
        """Check database connection and update status with visual indicators."""
        self.statusBar.showMessage("Checking database connection...")
        
        was_connected = self.db.connected
        success, message = self.db.connect_from_env()
        
        if success:
            # Update status indicator
            self.db_status_label.setText("Connection status: Connected")
            self.db_status_label.setStyleSheet("color: #2ecc71; font-weight: bold;")
            self.db_status_icon.setPixmap(self.style().standardIcon(QStyle.SP_DialogApplyButton).pixmap(16, 16))
            
            # Update status bar indicator
            self.db_status_indicator.setPixmap(self.style().standardIcon(QStyle.SP_DialogApplyButton).pixmap(16, 16))
            
            # Update connection details
            info = self.db.connection_info
            self.db_info_label.setText(
                f"Connected to: <b>{info['database']}</b> on {info['host']}:{info['port']} as {info['user']}"
            )
            
            # Check for existing tables with data
            success, tables_message, tables = self.db.check_tables()
            self.tables_with_data = tables  # Store for later use
            
            # Show message - different if already connected versus new connection
            if was_connected:
                self.statusBar.showMessage(f"Connection verified: {tables_message}")
            else:
                self.statusBar.showMessage(f"Successfully connected: {tables_message}")
            
            # If tables exist, enable the "Use Existing Data" option in processing tab
            if hasattr(self, 'use_existing_data_radio') and tables:
                self.use_existing_data_radio.setEnabled(True)
                self.use_existing_data_radio.setText(f"Use Existing Data ({len(tables)} tables available)")
            elif hasattr(self, 'use_existing_data_radio'):
                self.use_existing_data_radio.setEnabled(False)
                self.use_existing_data_radio.setText("Use Existing Data (No tables available)")
            
        else:
            # Update status indicator
            self.db_status_label.setText("Connection status: Not connected")
            self.db_status_label.setStyleSheet("color: #e74c3c; font-weight: bold;")
            self.db_status_icon.setPixmap(self.style().standardIcon(QStyle.SP_DialogCancelButton).pixmap(16, 16))
            
            # Update status bar indicator
            self.db_status_indicator.setPixmap(self.style().standardIcon(QStyle.SP_DialogCancelButton).pixmap(16, 16))
            
            # Update connection details
            self.db_info_label.setText("Connection details: None")
            
            # Show error in status bar
            self.statusBar.showMessage(message)
            
            # If processing tab has radio buttons, disable "Use Existing Data"
            if hasattr(self, 'use_existing_data_radio'):
                self.use_existing_data_radio.setEnabled(False)
                self.use_existing_data_radio.setText("Use Existing Data (Not connected)")

        return success
    
    def configure_database(self):
        """Open dialog to configure database connection."""
        try:
            # Create and show the database configuration dialog
            dialog = DatabaseConfigDialog(self)
            if dialog.exec_() == QDialog.Accepted:
                # Check the connection after configuration has been updated
                self.check_db_connection()
        except Exception as e:
            QMessageBox.warning(self, "Error", f"Failed to configure database: {str(e)}")

    def new_configuration(self):
        """Create a new configuration."""
        reply = QMessageBox.question(
            self, 
            "Create New Configuration", 
            "This will reset all source configurations. Continue?",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No
        )
        
        if reply == QMessageBox.Yes:
            # Create default paths.json with empty sources
            default_data = {"sources": {
                "portal": {"files": []},
                "metabase": {"files": []},
                "checkout_v1": {"files": []},
                "checkout_v2": {"files": []},
                "payfort": {"files": []},
                "tamara": {"files": []},
                "bank": {"files": []}
            }}
            
            try:
                with open("paths.json", 'w') as f:
                    json.dump(default_data, f, indent=4)
                
                self.update_sources_table(default_data["sources"])
                self.statusBar.showMessage("Created new configuration with empty sources", 5000)
            except Exception as e:
                QMessageBox.warning(self, "Error", f"Failed to create new configuration: {str(e)}")
    
    def load_configuration(self):
        """Load configuration from a different paths.json file."""
        file_path, _ = QFileDialog.getOpenFileName(
            self, "Load Configuration", "", "JSON Files (*.json)"
        )
        
        if file_path:
            try:
                with open(file_path, 'r') as f:
                    data = json.load(f)
                
                if "sources" in data:
                    # Save to current paths.json
                    with open("paths.json", 'w') as f:
                        json.dump(data, f, indent=4)
                    
                    self.update_sources_table(data["sources"])
                    self.statusBar.showMessage(f"Loaded configuration from {file_path}", 5000)
                else:
                    QMessageBox.warning(self, "Invalid Format", "The selected file does not contain a valid 'sources' section.")
            except Exception as e:
                QMessageBox.warning(self, "Error", f"Failed to load configuration: {str(e)}")
    
    def filter_log(self):
        """Filter log output based on text and level filters."""
        # This is a stub for future implementation
        filter_text = self.log_filter.text().lower()
        selected_level = self.log_level_combo.currentText()
        
        # Future: Implement actual filtering logic
        self.statusBar.showMessage(f"Log filtered with text: '{filter_text}' and level: {selected_level}", 3000)
    
    def clear_log(self):
        """Clear the log output."""
        self.log_output.clear()
        self.statusBar.showMessage("Log cleared", 3000)
    
    def show_settings(self):
        """Show settings dialog."""
        QMessageBox.information(self, "Settings", "Settings functionality will be implemented in a future version.")
    
    def show_documentation(self):
        """Show documentation."""
        QMessageBox.information(self, "Documentation", "Documentation is available in the project repository.")
    
    def show_about(self):
        """Show about dialog."""
        about_text = """
        <h2>ETL Pipeline Manager</h2>
        <p>Version 1.0</p>
        <p>A graphical interface for managing the ETL pipeline.</p>
        <p>This application provides a user-friendly way to:</p>
        <ul>
            <li>Manage data source paths</li>
            <li>Check database connection</li>
            <li>Run the ETL pipeline</li>
            <li>Configure output data and filters</li>
        </ul>
        """
        QMessageBox.about(self, "About ETL Pipeline Manager", about_text)
    
    def refresh_ui(self):
        """Periodically refresh UI elements."""
        # Check connection status
        if self.db.connected:
            try:
                with self.db.engine.connect() as conn:
                    conn.execute(text("SELECT 1"))
            except:
                # Connection lost
                self.db.connected = False
                self.db_status_label.setText("Connection status: Lost connection")
                self.db_status_label.setStyleSheet("color: #e74c3c; font-weight: bold;")
                self.db_status_icon.setPixmap(self.style().standardIcon(QStyle.SP_DialogCancelButton).pixmap(16, 16))
                self.db_status_indicator.setPixmap(self.style().standardIcon(QStyle.SP_DialogCancelButton).pixmap(16, 16))
        
        # Update timestamps or other dynamic content here
        
    def start_processing(self):
        """Start the ETL processing pipeline with enhanced user feedback."""
        # Reset step indicators
        self.step1_icon.setVisible(False)
        self.step2_icon.setVisible(False)
        self.step3_icon.setVisible(False)
        
        # Check database connection first
        if not self.db.connected:
            success, message = self.db.connect_from_env()
            if not success:
                QMessageBox.warning(self, "Database Connection Error", message)
                return
        
        # Check if database has existing data
        success, message, tables_with_data = self.db.check_tables()
        
        # Determine the processing mode
        process_new_data = self.process_new_data_radio.isChecked()
        
        proceed = True
        # Store original force_overwrite state to restore it later if needed
        self.original_force_overwrite_state = self.force_overwrite_check.isChecked()
        
        if process_new_data and tables_with_data and not self.force_overwrite_check.isChecked():
            # Ask user whether to proceed with overwriting data
            reply = QMessageBox.question(
                self, "Existing Data Found",
                f"Found {len(tables_with_data)} tables with data in the database.\n"
                "Do you want to proceed and overwrite this data?",
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.No
            )
            proceed = (reply == QMessageBox.Yes)
            
            # If user clicked Yes, set force_overwrite to True temporarily
            # This ensures --yes is passed to load_to_postgres.py
            if proceed:
                self.force_overwrite_check.setChecked(True)
                self.log_output.append("User confirmed overwrite via dialog. Setting force_overwrite=True for this run.")
        
        if proceed:
            # Update UI state
            self.process_btn.setEnabled(False)
            self.stop_btn.setEnabled(True)
            self.run_action.setEnabled(False)
            self.stop_action.setEnabled(True)
            self.toolbar_stop_action.setEnabled(True)
            
            self.log_output.clear()
            self.progress_bar.setValue(0)
            self.status_label.setText("Processing...")
            self.current_operation_label.setText("Initializing...")
            
            # Set estimated completion time
            self.time_estimate_label.setText("Calculating...")
            
            # Store start time for estimations
            self.start_time = time.time()
            
            # Get output directory and other options
            output_dir = self.output_dir_edit.text()
            batch_size = self.batch_size_spin.value()
            max_workers = self.max_workers_spin.value() if self.parallel_check.isChecked() else 1
            
            # Set options string for command
            parallel_option = "--parallel" if self.parallel_check.isChecked() else ""
            
            # Run the pipeline based on selected mode
            if process_new_data:
                self.run_pipeline(output_dir, batch_size, max_workers, parallel_option)
            else:
                # Skip data processing, run only SQL analysis
                self.run_sql_analysis()
            
            # Switch to processing tab for better visibility
            self.tabs.setCurrentIndex(1)
        else:
            # If user clicked No, restore original force_overwrite state
            self.force_overwrite_check.setChecked(self.original_force_overwrite_state)
    
    def run_pipeline(self, output_dir, batch_size, max_workers, parallel_option):
        """Run the ETL pipeline steps with enhanced parameters."""
        # Step 1: Generate configuration
        self.current_operation_label.setText("Generate Configuration")
        command = f"python generate_config.py"
        self.run_process(command, "Generate Configuration")
    
    def run_sql_analysis(self):
        """Run only the SQL analysis part of the pipeline."""
        # Execute SQL analysis directly
        self.current_operation_label.setText("Execute SQL Analysis")
        
        # Make output directory if it doesn't exist
        output_dir = self.output_dir_edit.text()
        os.makedirs(output_dir, exist_ok=True)
        os.makedirs("./output", exist_ok=True)
        
        # Set up the SQL analysis command
        command = f"python sql_executor.py --query payment_analysis.sql --output ./output/analysis_results.xlsx"
        self.run_process(command, "Execute SQL Analysis")
    
    def run_process(self, command, description):
        """Run a subprocess command with logging and tracking."""
        # Create and start thread for process
        self.current_process = LogCaptureThread(command, description)
        
        # Connect signals
        self.current_process.log_signal.connect(self.update_log)
        self.current_process.progress_signal.connect(self.update_progress)
        self.current_process.finished_signal.connect(
            lambda success, message: self.process_finished(success, message, description)
        )
        
        # Start the process
        self.current_process.start()
    
    def update_log(self, text):
        """Update log output with colorized text."""
        # Add color based on log level or important messages
        if "ERROR" in text or "Error" in text or "failed" in text.lower():
            text = f'<span style="color: #e74c3c;"><b>{text}</b></span>'
        elif "WARNING" in text or "Warning" in text:
            text = f'<span style="color: #f39c12;">{text}</span>'
        elif "SUCCESS" in text or "Success" in text or "successfully" in text.lower():
            text = f'<span style="color: #2ecc71;"><b>{text}</b></span>'
        elif "PROGRESS" in text:
            text = f'<span style="color: #3498db;"><b>{text}</b></span>'
        elif "***" in text:  # Special attention messages
            text = f'<span style="color: #e67e22; background-color: #f9ebc7; font-size: 14px;"><b>{text}</b></span>'
        
        # Append to log
        self.log_output.append(text)
        
        # Keep only the last 5000 lines to prevent memory issues
        document = self.log_output.document()
        if document.blockCount() > 5000:
            cursor = QTextCursor(document)
            cursor.movePosition(QTextCursor.Start)
            cursor.movePosition(QTextCursor.Down, QTextCursor.KeepAnchor, document.blockCount() - 5000)
            cursor.removeSelectedText()
        
        # Scroll to bottom
        cursor = self.log_output.textCursor()
        cursor.movePosition(cursor.End)
        self.log_output.setTextCursor(cursor)
        
        # Process events to ensure UI updates
        QApplication.processEvents()
    
    def update_progress(self, value):
        """Update progress bar with current value."""
        self.progress_bar.setValue(value)
        
        # Update estimated completion time
        if 0 < value < 100:
            # Simple linear estimation
            elapsed_time = time.time() - self.start_time if hasattr(self, 'start_time') else 0
            if elapsed_time > 0 and value > 0:
                total_estimated = elapsed_time * 100 / value
                remaining = total_estimated - elapsed_time
                
                # Format remaining time
                remaining_str = self.format_time(remaining)
                self.time_estimate_label.setText(remaining_str)
        
    def format_time(self, seconds):
        """Format seconds into a readable time string."""
        if seconds < 60:
            return f"{int(seconds)} seconds"
        elif seconds < 3600:
            minutes = int(seconds / 60)
            seconds = int(seconds % 60)
            return f"{minutes}:{seconds:02d} minutes"
        else:
            hours = int(seconds / 3600)
            minutes = int((seconds % 3600) / 60)
            return f"{hours}:{minutes:02d} hours"
            
    def stop_processing(self):
        """Stop current processing."""
        if self.current_process and self.current_process.isRunning():
            # Stop the process
            self.current_process.stop()
            
            # Update UI
            self.status_label.setText("Stopped by user")
            self.current_operation_label.setText("Operation cancelled")
            
            # Reset controls
            self.process_btn.setEnabled(True)
            self.stop_btn.setEnabled(False)
            self.run_action.setEnabled(True)
            self.stop_action.setEnabled(False)
            self.toolbar_stop_action.setEnabled(False)
            
            # Restore original force_overwrite state if it was temporarily changed
            if hasattr(self, 'original_force_overwrite_state'):
                self.force_overwrite_check.setChecked(self.original_force_overwrite_state)
                self.log_output.append("Restored original force_overwrite setting after user cancellation.")
            
            # Show message
            self.statusBar.showMessage("Processing stopped by user", 5000)
    
    @pyqtSlot(bool, str)
    def process_finished(self, success, message, description, result_file=None):
        """Handle process completion with improved visual indicators and next step logic."""
        if success:
            self.status_label.setText(message)
            
            # Handle result file preview if available
            if description == "Execute Generated Query" and result_file and os.path.exists(result_file):
                self.log_output.append(f"Query results saved to: {result_file}")
                try:
                    self.load_result_preview(result_file)
                except Exception as e:
                    self.log_output.append(f"Error loading results preview: {str(e)}")
                    traceback.print_exc()
            
            # Update step indicators based on completed step
            if description == "Generate Configuration":
                self.step1_icon.setVisible(True)
                self.step1_icon.setPixmap(self.style().standardIcon(QStyle.SP_DialogApplyButton).pixmap(16, 16))
                
                # Next: Run data_cleaner.py
                output_dir = self.output_dir_edit.text()
                batch_size = self.batch_size_spin.value()
                max_workers = self.max_workers_spin.value() if self.parallel_check.isChecked() else 1
                
                parallel_option = "--parallel" if self.parallel_check.isChecked() else ""
                max_workers_option = f"--max_workers {max_workers}" if self.parallel_check.isChecked() else ""
                
                command = f"python data_cleaner.py --output_dir {output_dir} --batch_size {batch_size} {parallel_option} {max_workers_option}"
                self.current_operation_label.setText("Clean and Process Data")
                self.run_process(command, "Clean and Process Data")
                
            elif description == "Clean and Process Data":
                self.step2_icon.setVisible(True)
                self.step2_icon.setPixmap(self.style().standardIcon(QStyle.SP_DialogApplyButton).pixmap(16, 16))
                
                # Next: Run load_to_postgres.py
                output_dir = self.output_dir_edit.text()
                
                # Pass --yes flag when force_overwrite_check is checked
                auto_yes = "--yes" if self.force_overwrite_check.isChecked() else ""
                
                # Ensure the --yes flag is properly passed with space
                if auto_yes:
                    command = f"python load_to_postgres.py --parquet-dir {output_dir} {auto_yes}"
                    self.log_output.append(f"Passing automatic confirmation flag: {auto_yes}")
                else:
                    command = f"python load_to_postgres.py --parquet-dir {output_dir}"
                
                self.current_operation_label.setText("Load Data to PostgreSQL")
                self.log_output.append("About to run: " + command)
                self.run_process(command, "Load Data to PostgreSQL")
                
            elif description == "Load Data to PostgreSQL":
                self.step3_icon.setVisible(True)
                self.step3_icon.setPixmap(self.style().standardIcon(QStyle.SP_DialogApplyButton).pixmap(16, 16))
                
                # Final step - all completed
                self.current_operation_label.setText("Processing Completed")
                self.time_estimate_label.setText("Completed")
                
                QMessageBox.information(self, "Processing Complete", 
                                        "Data has been processed and saved to the database successfully!")
                self.status_label.setText("Completed")
                
                # Reset UI controls
                self.process_btn.setEnabled(True)
                self.stop_btn.setEnabled(False)
                self.run_action.setEnabled(True)
                self.stop_action.setEnabled(False)
                self.toolbar_stop_action.setEnabled(False)
                
                # Restore original force_overwrite state if it was temporarily changed
                if hasattr(self, 'original_force_overwrite_state'):
                    self.force_overwrite_check.setChecked(self.original_force_overwrite_state)
                    self.log_output.append("Restored original force_overwrite setting.")
            elif description == "Execute SQL Analysis":
                # This block is being removed as SQL Analysis isn't part of this process
                pass
            elif description == "Execute Generated Query":
                # Generated query execution completed
                self.current_operation_label.setText("Query Execution Completed")
                self.time_estimate_label.setText("Completed")
                
                QMessageBox.information(self, "Query Execution Complete", 
                                       "The generated SQL query has executed successfully!")
                self.status_label.setText("Completed")
                
                # Reset UI controls
                self.process_btn.setEnabled(True)
                self.stop_btn.setEnabled(False)
                self.run_action.setEnabled(True)
                self.stop_action.setEnabled(False)
                self.toolbar_stop_action.setEnabled(False)
        else:
            # Process failed - update UI with error indicators
            self.status_label.setText(f"Error: {message}")
            self.current_operation_label.setText("Error Occurred")
            self.time_estimate_label.setText("Failed")
            
            # Update icon for the failed step
            if description == "Generate Configuration":
                self.step1_icon.setVisible(True)
                self.step1_icon.setPixmap(self.style().standardIcon(QStyle.SP_DialogCancelButton).pixmap(16, 16))
            elif description == "Clean and Process Data":
                self.step2_icon.setVisible(True)
                self.step2_icon.setPixmap(self.style().standardIcon(QStyle.SP_DialogCancelButton).pixmap(16, 16))
            elif description == "Load Data to PostgreSQL":
                self.step3_icon.setVisible(True)
                self.step3_icon.setPixmap(self.style().standardIcon(QStyle.SP_DialogCancelButton).pixmap(16, 16))
            elif description == "Execute SQL Analysis":
                # This condition is being removed as SQL Analysis isn't part of this process
                pass
            
            QMessageBox.critical(self, "Processing Error", 
                                f"Error during {description}:\n{message}")
            
            # Reset UI controls
            self.process_btn.setEnabled(True)
            self.stop_btn.setEnabled(False)
            self.run_action.setEnabled(True)
            self.stop_action.setEnabled(False)
            self.toolbar_stop_action.setEnabled(False)
            
            # Restore original force_overwrite state if it was temporarily changed
            if hasattr(self, 'original_force_overwrite_state'):
                self.force_overwrite_check.setChecked(self.original_force_overwrite_state)
                self.log_output.append("Restored original force_overwrite setting after error.")

    def load_paths_config(self):
        """Load paths.json configuration file."""
        try:
            path = Path("paths.json")
            if path.exists():
                with open(path, 'r') as f:
                    data = json.load(f)
                
                if "sources" in data:
                    sources = data["sources"]
                    self.update_sources_table(sources)
                    self.statusBar.showMessage(f"Loaded {len(sources)} sources from paths.json", 5000)
                    
                # Load configuration if available
                if "configuration" in data:
                    config = data["configuration"]
                    
                    # Skip loading output directory from paths.json as we're using root/output
                    # if "output_dir" in config:
                    #     self.output_dir = config["output_dir"]
                    #     self.output_dir_edit.setText(self.output_dir)
                    
                    # Always update the text in case it was changed
                    self.output_dir_edit.setText(self.output_dir)
                    
                    # Load batch size
                    if "batch_size" in config:
                        self.batch_size_spin.setValue(config["batch_size"])
                    
                    # Load worker settings
                    if "max_workers" in config:
                        self.max_workers_spin.setValue(config["max_workers"])
                    
                    if "use_parallel" in config:
                        self.parallel_check.setChecked(config["use_parallel"])
                    
                    if "force_overwrite" in config:
                        self.force_overwrite_check.setChecked(config["force_overwrite"])
                    
                    # Load filters if available
                    if "filters" in config:
                        filters = config["filters"]
                        
                        # Apply date filters
                        if "start_date" in filters and hasattr(self, 'start_date_edit'):
                            try:
                                self.start_date_edit.setDate(datetime.strptime(filters["start_date"], "%Y-%m-%d").date())
                            except:
                                pass
                        
                        if "end_date" in filters and hasattr(self, 'end_date_edit'):
                            try:
                                self.end_date_edit.setDate(datetime.strptime(filters["end_date"], "%Y-%m-%d").date())
                            except:
                                pass
                        
                        # Apply amount filters
                        if "min_amount" in filters and hasattr(self, 'min_amount_edit'):
                            self.min_amount_edit.setText(filters["min_amount"])
                        
                        if "max_amount" in filters and hasattr(self, 'max_amount_edit'):
                            self.max_amount_edit.setText(filters["max_amount"])
                        
                        # Apply other filters
                        if "payment_method" in filters and hasattr(self, 'payment_method_combo'):
                            index = self.payment_method_combo.findText(filters["payment_method"])
                            if index >= 0:
                                self.payment_method_combo.setCurrentIndex(index)
                        
                        if "status" in filters and hasattr(self, 'status_combo'):
                            index = self.status_combo.findText(filters["status"])
                            if index >= 0:
                                self.status_combo.setCurrentIndex(index)
                        
                        if "include_duplicates" in filters and hasattr(self, 'include_duplicates_check'):
                            self.include_duplicates_check.setChecked(filters["include_duplicates"])
                else:
                    self.statusBar.showMessage("Invalid paths.json format: 'sources' key missing", 5000)
            else:
                # Create default paths.json with empty sources
                default_data = {"sources": {
                    "portal": {"files": []},
                    "metabase": {"files": []},
                    "checkout_v1": {"files": []},
                    "checkout_v2": {"files": []},
                    "payfort": {"files": []},
                    "tamara": {"files": []},
                    "bank": {"files": []}
                }}
                
                with open(path, 'w') as f:
                    json.dump(default_data, f, indent=4)
                
                self.update_sources_table(default_data["sources"])
                self.statusBar.showMessage("Created default paths.json with empty sources", 5000)
        
        except Exception as e:
            QMessageBox.warning(self, "Error", f"Failed to load paths.json: {str(e)}")
            self.statusBar.showMessage(f"Error loading paths.json: {str(e)}", 5000)
            
    def save_paths_json(self):
        """Save current configuration to paths.json file."""
        try:
            path = Path("paths.json")
            
            # If exists, load current data first to update just the sources
            if path.exists():
                with open(path, 'r') as f:
                    data = json.load(f)
            else:
                data = {"sources": {}}
            
            # Add output configuration
            if "configuration" not in data:
                data["configuration"] = {}
                
            # Save output directory, batch size, and worker settings
            data["configuration"]["output_dir"] = self.output_dir
            data["configuration"]["batch_size"] = self.batch_size_spin.value()
            data["configuration"]["max_workers"] = self.max_workers_spin.value()
            data["configuration"]["use_parallel"] = self.parallel_check.isChecked()
            data["configuration"]["force_overwrite"] = self.force_overwrite_check.isChecked()
            
            # Save filters if available
            filters = {}
            
            # Date filters
            if hasattr(self, 'start_date_edit') and hasattr(self, 'end_date_edit'):
                filters["start_date"] = self.start_date_edit.date().toString("yyyy-MM-dd")
                filters["end_date"] = self.end_date_edit.date().toString("yyyy-MM-dd")
            
            # Amount filters
            if hasattr(self, 'min_amount_edit') and hasattr(self, 'max_amount_edit'):
                filters["min_amount"] = self.min_amount_edit.text() or ""
                filters["max_amount"] = self.max_amount_edit.text() or ""
            
            # Save payment method and status if available
            if hasattr(self, 'payment_method_combo'):
                filters["payment_method"] = self.payment_method_combo.currentText()
            
            if hasattr(self, 'status_combo'):
                filters["status"] = self.status_combo.currentText()
            
            if hasattr(self, 'include_duplicates_check'):
                filters["include_duplicates"] = self.include_duplicates_check.isChecked()
                
            data["configuration"]["filters"] = filters
            
            # Write updated config to paths.json
            with open(path, 'w') as f:
                json.dump(data, f, indent=4)
                
            self.statusBar.showMessage("Configuration saved to paths.json", 5000)
            return True
            
        except Exception as e:
            QMessageBox.warning(self, "Error", f"Failed to save paths.json: {str(e)}")
            self.statusBar.showMessage(f"Error saving paths.json: {str(e)}", 5000)
            return False

    def edit_source(self):
        """Edit the currently selected source in the sources table."""
        selected_rows = self.sources_table.selectionModel().selectedRows()
        if not selected_rows:
            QMessageBox.information(self, "No Selection", "Please select a source to edit.")
            return
            
        # Get the source name from the first column of the selected row
        row = selected_rows[0].row()
        source = self.sources_table.item(row, 0).text().lower()
        
        # Use the existing method to edit this source
        self.edit_source_by_name(source)

    def browse_output_dir(self):
        """Open file dialog to select output directory."""
        directory = QFileDialog.getExistingDirectory(
            self, "Select Output Directory", self.output_dir
        )
        
        if directory:
            self.output_dir = directory
            self.output_dir_edit.setText(directory)
            self.statusBar.showMessage(f"Output directory set to: {directory}", 5000)

    def setup_query_generator_tab(self):
        """Set up the query generator tab with dynamic field selection."""
        # Create main tab widget
        self.query_generator_tab = QWidget()
        
        # Create a scroll area to handle potentially large content
        scroll_area = QScrollArea()
        scroll_area.setWidgetResizable(True)
        scroll_area.setFrameShape(QFrame.NoFrame)
        
        # Widget that will contain all our content
        content_widget = QWidget()
        main_layout = QVBoxLayout(content_widget)
        main_layout.setContentsMargins(10, 10, 10, 10)
        main_layout.setSpacing(15)
        
        # Add tooltip help button at the top
        tooltip_help_layout = QHBoxLayout()
        tooltip_help_btn = QPushButton("Hover over section headers for detailed help")
        tooltip_help_btn.setIcon(self.style().standardIcon(QStyle.SP_MessageBoxInformation))
        tooltip_help_btn.setStyleSheet("background-color: #E3F2FD; color: #0D47A1; padding: 5px;")
        tooltip_help_btn.setCursor(QCursor(Qt.WhatsThisCursor))
        tooltip_help_btn.clicked.connect(lambda: QMessageBox.information(
            self, 
            "Tooltip Help", 
            "Hover your mouse over section headers (like 'Output Data Filter' or 'Matching Criteria with Bank Data') to see detailed help information.\n\n"
            "When you see the cursor change to a question mark, it means there's additional help available as a tooltip.\n\n"
            "The border will also highlight in blue to indicate where tooltips are available."
        ))
        tooltip_help_layout.addWidget(tooltip_help_btn)
        tooltip_help_layout.addStretch()
        main_layout.addLayout(tooltip_help_layout)

        # Initialize field map and checkboxes dictionary
        self.field_map = {}
        self.field_checkboxes = {} # Stores tab_name_field_name: checkbox pairs

        # Load field definitions dynamically
        self.load_field_definitions() # This will now populate self.field_map

        # Field selection area
        field_selection_group = QGroupBox("Select Fields for Query")
        field_selection_layout = QVBoxLayout(field_selection_group)
        
        # Create tabs widget for field selection from different sources
        self.field_tabs = QTabWidget()
        self.field_tabs.setTabPosition(QTabWidget.North) # Tabs on top - standard UI/UX
        self.field_tabs.setMinimumHeight(300)  # Ensure minimum height
        
        # Apply style to make tabs more visible and follow standard UI patterns
        self.field_tabs.setStyleSheet("""
            QTabWidget::pane { 
                border: 1px solid #ddd;
                background-color: white;
            }
            QTabBar::tab {
                min-width: 120px;
                padding: 8px 12px;
                margin: 1px 2px 0px 0px;
                border-top-left-radius: 4px;
                border-top-right-radius: 4px;
                border: 1px solid #ddd;
                border-bottom: none;
            }
            QTabBar::tab:selected {
                background-color: #0078d7;
                color: white;
                font-weight: bold;
            }
            QTabBar::tab:!selected {
                background-color: #f0f0f0;
                margin-top: 2px;
            }
        """)

        # Create tabs for each source based on self.field_map
        tab_order = ["Portal", "Metabase", "Checkout V1", "Checkout V2", "Payfort", "Tamara", "Bank", "Analysis"]
        
        for tab_name_display in tab_order:
            source_key = tab_name_display.lower().replace(" ", "_") # e.g., "checkout_v1"
            if source_key in self.field_map and self.field_map[source_key]:
                self.setup_field_selection_tab(self.field_tabs, tab_name_display, self.field_map[source_key])
            elif source_key == "analysis" and "analysis" in self.field_map and self.field_map["analysis"]: # Special case for analysis
                 self.setup_field_selection_tab(self.field_tabs, "Analysis", self.field_map["analysis"])

        # Add the field tabs widget to the layout
        field_selection_layout.addWidget(self.field_tabs, 1)  # With stretch factor
        
        # Add "Select All" buttons directly under field tabs in the field selection group
        select_all_layout = QHBoxLayout()
        
        select_all_tabs_btn = QPushButton("Select All Fields (All Tabs)")
        select_all_tabs_btn.setIcon(self.style().standardIcon(QStyle.SP_DialogApplyButton))
        select_all_tabs_btn.clicked.connect(self.select_all_fields)
        
        deselect_all_tabs_btn = QPushButton("Deselect All Fields (All Tabs)")
        deselect_all_tabs_btn.setIcon(self.style().standardIcon(QStyle.SP_DialogCancelButton))
        deselect_all_tabs_btn.clicked.connect(self.deselect_all_fields)
        
        load_defaults_btn = QPushButton("Load Defaults")
        load_defaults_btn.setIcon(self.style().standardIcon(QStyle.SP_DialogResetButton))
        load_defaults_btn.clicked.connect(self.load_default_fields)
        
        select_all_layout.addWidget(select_all_tabs_btn)
        select_all_layout.addWidget(deselect_all_tabs_btn)
        select_all_layout.addWidget(load_defaults_btn)
        
        field_selection_layout.addLayout(select_all_layout)
        
        # Add field selection to main layout
        main_layout.addWidget(field_selection_group)
        
        # Create tabs for query options
        options_tabs = QTabWidget()
        
        # Data Processing tab
        data_proc_tab = QWidget()
        data_proc_layout = QVBoxLayout(data_proc_tab)
        
        # Transaction merging section
        merge_group = QGroupBox("Transaction Merging")
        merge_group.setToolTip("<b>Transaction Merging</b><br>Controls how related authorization and capture transactions are combined in the query results.<br><br>When enabled, authorization transactions are merged with their related capture transactions.")
        merge_layout = QVBoxLayout(merge_group)
        self.enhance_tooltip_widget(merge_group)
        
        self.merge_auth_capture_check = QCheckBox("Merge Auth and Capture Transactions")
        self.merge_auth_capture_check.setChecked(True)
        self.merge_auth_capture_check.setToolTip("Merge authorization and capture transactions (removes auth row and keeps capture with auth details)")
        
        self.use_auth_rrn_check = QCheckBox("Use Auth RRN for Capture Transactions")
        self.use_auth_rrn_check.setChecked(True)
        self.use_auth_rrn_check.setToolTip("Use RRN from authorization transaction for corresponding capture transactions")
        
        self.use_auth_code_check = QCheckBox("Use Auth Code for Capture Transactions")
        self.use_auth_code_check.setChecked(True)
        self.use_auth_code_check.setToolTip("Use authorization code from auth transaction for corresponding capture transactions")
        
        merge_layout.addWidget(self.merge_auth_capture_check)
        merge_layout.addWidget(self.use_auth_rrn_check)
        merge_layout.addWidget(self.use_auth_code_check)
        
        # Transaction filtering - RENAME TO OUTPUT DATA FILTER
        filter_group = QGroupBox("Output Data Filter")
        filter_group.setToolTip("<b>Output Data Filter</b><br>Controls which transactions appear in your query results based on status and transaction types.<br><br>Use these options to filter the data that will be included in the final query output.")
        filter_layout = QVBoxLayout(filter_group)
        self.enhance_tooltip_widget(filter_group)
        
        self.success_only_check = QCheckBox("Show Successful Transactions Only")
        self.success_only_check.setChecked(True)
        self.success_only_check.setToolTip("Only show transactions with a successful outcome")
        
        transaction_types_layout = QHBoxLayout()
        transaction_types_layout.addWidget(QLabel("Include Transaction Types:"))
        
        self.include_auth_check = QCheckBox("Auth")
        self.include_auth_check.setChecked(True)
        self.include_auth_check.setToolTip("Include Authorization transactions in the analysis.")
        
        self.include_capture_check = QCheckBox("Capture")
        self.include_capture_check.setChecked(True)
        self.include_capture_check.setToolTip("Include Capture transactions in the analysis.")
        
        self.include_refund_check = QCheckBox("Refund")
        self.include_refund_check.setChecked(True)
        self.include_refund_check.setToolTip("Include Refund transactions in the analysis.")
        
        self.include_void_check = QCheckBox("Void")
        self.include_void_check.setChecked(True)
        self.include_void_check.setToolTip("Include Void transactions in the analysis.")
        
        transaction_types_layout.addWidget(self.include_auth_check)
        transaction_types_layout.addWidget(self.include_capture_check)
        transaction_types_layout.addWidget(self.include_refund_check)
        transaction_types_layout.addWidget(self.include_void_check)
        transaction_types_layout.addStretch()
        
        # Additional transaction processing options
        self.handle_partial_refunds_check = QCheckBox("Process Partial Refunds")
        self.handle_partial_refunds_check.setChecked(True)
        self.handle_partial_refunds_check.setToolTip("Include special handling for partial refund transactions")
        
        self.simplify_response_codes_check = QCheckBox("Simplify Response Codes")
        self.simplify_response_codes_check.setChecked(True)
        self.simplify_response_codes_check.setToolTip("Simplify response codes to standardized categories")
        
        filter_layout.addWidget(self.success_only_check)
        filter_layout.addLayout(transaction_types_layout)
        filter_layout.addWidget(self.handle_partial_refunds_check)
        filter_layout.addWidget(self.simplify_response_codes_check)
        
        data_proc_layout.addWidget(merge_group)
        data_proc_layout.addWidget(filter_group)
        data_proc_layout.addStretch()
        
        # Reconciliation tab
        reconciliation_tab = QWidget()
        reconciliation_layout = QVBoxLayout(reconciliation_tab)
        
        # Primary option - Enable/disable reconciliation
        self.reconciliation_check = QCheckBox("Include Bank Reconciliation")
        self.reconciliation_check.setChecked(True)
        self.reconciliation_check.setToolTip("<b>Bank Reconciliation</b><br>Include bank matching and reconciliation logic to compare gateway transactions with bank records.<br><br>This allows you to identify matched, partially matched, and unmatched transactions between payment gateways and bank records.")
        self.reconciliation_check.stateChanged.connect(self.toggle_reconciliation_options)
        self.enhance_tooltip_widget(self.reconciliation_check)
        
        # Advanced matching options
        matching_group = QGroupBox("Matching Options")
        matching_group.setToolTip("<b>Matching Options</b><br>Defines how transaction matching works when reconciling with bank data.<br><br>These options control amount tolerances and date criteria used for matching transactions with bank records.")
        matching_layout = QVBoxLayout(matching_group)
        self.enhance_tooltip_widget(matching_group)
        
        self.exact_match_check = QCheckBox("Exact Amount Matching")
        self.exact_match_check.setChecked(True)
        self.exact_match_check.setToolTip("Match amounts exactly (within 0.01 tolerance)")
        
        self.approx_match_check = QCheckBox("Approximate Amount Matching")
        self.approx_match_check.setChecked(True)
        self.approx_match_check.setToolTip("Allow approximate amount matching (within 1.0 tolerance)")
        
        self.date_match_check = QCheckBox("Date-based Matching")
        self.date_match_check.setChecked(True)
        self.date_match_check.setToolTip("Include matches based on same transaction date")
        
        matching_layout.addWidget(self.exact_match_check)
        matching_layout.addWidget(self.approx_match_check)
        matching_layout.addWidget(self.date_match_check)
        
        # Match types - RENAME TO MATCHING CRITERIA WITH BANK DATA
        match_types_group = QGroupBox("Matching Criteria with Bank Data")
        match_types_group.setToolTip("<b>Matching Criteria with Bank Data</b><br>Defines the specific fields used to match transactions with bank records.<br><br>Select which combinations of fields (Auth Code, RRN) should be used for matching.")
        match_types_layout = QVBoxLayout(match_types_group)
        self.enhance_tooltip_widget(match_types_group)
        
        self.auth_rrn_check = QCheckBox("Auth+RRN")
        self.auth_rrn_check.setChecked(True)
        self.auth_rrn_check.setToolTip("Match by both Auth Code and RRN")
        
        self.rrn_only_check = QCheckBox("RRN Only")
        self.rrn_only_check.setChecked(True)
        self.rrn_only_check.setToolTip("Match by RRN only")
        
        self.auth_only_check = QCheckBox("Auth Only")
        self.auth_only_check.setChecked(True)
        self.auth_only_check.setToolTip("Match by Auth Code only")
        
        match_types_layout.addWidget(self.auth_rrn_check)
        match_types_layout.addWidget(self.rrn_only_check)
        match_types_layout.addWidget(self.auth_only_check)
        
        # Add new option for distinct orders
        distinct_orders_group = QGroupBox("Result Options")
        distinct_orders_group.setToolTip("<b>Result Options</b><br>Options that affect the final structure of query results.<br><br>These settings control the format and organization of the data returned by the query.")
        distinct_orders_layout = QVBoxLayout(distinct_orders_group)
        self.enhance_tooltip_widget(distinct_orders_group)
        
        self.return_distinct_orders_check = QCheckBox("Return Distinct Orders")
        self.return_distinct_orders_check.setChecked(False)
        self.return_distinct_orders_check.setToolTip("<b>Return Distinct Orders</b><br>Return only unique gateway_order_id records.<br><br>This eliminates duplicate orders and only returns one transaction per order.")
        self.enhance_tooltip_widget(self.return_distinct_orders_check)
        
        distinct_orders_layout.addWidget(self.return_distinct_orders_check)
        
        reconciliation_layout.addWidget(self.reconciliation_check)
        reconciliation_layout.addWidget(matching_group)
        reconciliation_layout.addWidget(match_types_group)
        reconciliation_layout.addWidget(distinct_orders_group)  # Add the new group
        reconciliation_layout.addStretch()
        
        # Output Options tab
        output_options_tab = QWidget()
        output_options_layout = QVBoxLayout(output_options_tab)
        
        # Filtering and Output Options group
        filter_output_group = QGroupBox("Filtering and Output Options")
        filter_output_form_layout = QFormLayout(filter_output_group)

        # Date Filter
        self.enable_date_filter_check = QCheckBox("Enable Date Filter (Portal Data)")
        self.enable_date_filter_check.setChecked(False) 
        self.enable_date_filter_check.setToolTip("Enable to filter portal transactions by date range.")
        
        date_range_controls_layout = QHBoxLayout()
        self.query_start_date = QDateEdit()
        self.query_start_date.setCalendarPopup(True)
        self.query_start_date.setDate(datetime.now().replace(day=1).date())
        self.query_start_date.setEnabled(False) 
        self.query_start_date.setToolTip("Select the start date for filtering portal transactions.")

        self.query_end_date = QDateEdit()
        self.query_end_date.setCalendarPopup(True)
        self.query_end_date.setDate(datetime.now().date())
        self.query_end_date.setEnabled(False) 
        self.query_end_date.setToolTip("Select the end date for filtering portal transactions.")
        
        self.enable_date_filter_check.stateChanged.connect(
            lambda state: (
                self.query_start_date.setEnabled(state == Qt.Checked),
                self.query_end_date.setEnabled(state == Qt.Checked)
            )
        )
        date_range_controls_layout.addWidget(QLabel("From:"))
        date_range_controls_layout.addWidget(self.query_start_date)
        date_range_controls_layout.addWidget(QLabel("To:"))
        date_range_controls_layout.addWidget(self.query_end_date)
        date_range_controls_layout.addStretch()
        
        filter_output_form_layout.addRow(self.enable_date_filter_check)
        filter_output_form_layout.addRow(date_range_controls_layout)

        # Row Limit
        limit_row_layout = QHBoxLayout()
        self.enable_limit_check = QCheckBox("Limit number of rows returned")
        self.enable_limit_check.setChecked(False)
        self.enable_limit_check.setToolTip("Enable to limit the number of rows in the final query result.")
        
        self.limit_spinbox = QSpinBox()
        self.limit_spinbox.setRange(1, 1000000)
        self.limit_spinbox.setValue(1000)
        self.limit_spinbox.setEnabled(False)
        self.limit_spinbox.setToolTip("Set the maximum number of rows to return.")

        self.enable_limit_check.stateChanged.connect(lambda state: self.limit_spinbox.setEnabled(state == Qt.Checked))
        
        limit_row_layout.addWidget(self.enable_limit_check)
        limit_row_layout.addWidget(self.limit_spinbox)
        limit_row_layout.addStretch()
        filter_output_form_layout.addRow("Row Limit:", limit_row_layout)
        
        output_options_layout.addWidget(filter_output_group)
        output_options_layout.addStretch()
        
        # Add tabs to options tabwidget
        options_tabs.addTab(data_proc_tab, "Data Processing")
        options_tabs.addTab(reconciliation_tab, "Reconciliation")
        options_tabs.addTab(output_options_tab, "Output Options") 
        
        main_layout.addWidget(options_tabs)
        
        # Button layout with Generate and View buttons
        button_layout = QHBoxLayout()
        
        generate_btn = QPushButton("Generate Query")
        generate_btn.setIcon(self.style().standardIcon(QStyle.SP_BrowserReload))
        generate_btn.clicked.connect(self.generate_query)
        
        view_query_btn = QPushButton("View Generated Query")
        view_query_btn.setIcon(self.style().standardIcon(QStyle.SP_FileDialogContentsView))
        view_query_btn.clicked.connect(self.show_query_dialog)
        
        save_query_btn = QPushButton("Save Query")
        save_query_btn.setIcon(self.style().standardIcon(QStyle.SP_DialogSaveButton))
        save_query_btn.clicked.connect(self.save_generated_query)
        
        save_config_btn = QPushButton("Save Configuration")
        save_config_btn.setIcon(self.style().standardIcon(QStyle.SP_DialogSaveButton))
        save_config_btn.clicked.connect(self.save_query_config)
        
        load_config_btn = QPushButton("Load Configuration")
        load_config_btn.setIcon(self.style().standardIcon(QStyle.SP_DialogOpenButton))
        load_config_btn.clicked.connect(self.load_query_config)
        
        button_layout.addWidget(generate_btn)
        button_layout.addWidget(view_query_btn)
        button_layout.addWidget(save_query_btn)
        button_layout.addWidget(save_config_btn)
        button_layout.addWidget(load_config_btn)
        button_layout.addStretch()
        
        main_layout.addLayout(button_layout)
        
        # Set content widget as scroll area's widget
        scroll_area.setWidget(content_widget)
        
        # Add scroll area to the main tab layout
        tab_layout = QVBoxLayout(self.query_generator_tab)
        tab_layout.setContentsMargins(0, 0, 0, 0)
        tab_layout.addWidget(scroll_area)
        
        self.tabs.addTab(self.query_generator_tab, "Query Generator")
        
        # Initialize reconciliation options state
        self.toggle_reconciliation_options()
        
        # Setup dependencies between options
        self.setup_option_dependencies()
        
        # Store the generated query text
        self.generated_query = ""
        
        # Generate initial query
        self.generate_query()
    
    def show_query_dialog(self):
        """Show the generated query in a dialog window."""
        if not self.generated_query:
            QMessageBox.warning(self, "No Query", "No query has been generated yet. Please generate a query first.")
            return
        
        dialog = QDialog(self)
        dialog.setWindowTitle("Generated SQL Query")
        dialog.setMinimumSize(800, 600)
        
        layout = QVBoxLayout(dialog)
        
        # Query display area
        query_text = QTextEdit()
        query_text.setReadOnly(True)
        query_text.setFont(QFont("Consolas", 10))
        query_text.setText(self.generated_query)
        
        # Button for copying to clipboard
        button_layout = QHBoxLayout()
        
        copy_btn = QPushButton("Copy to Clipboard")
        copy_btn.setIcon(self.style().standardIcon(QStyle.SP_DialogSaveButton))
        copy_btn.clicked.connect(lambda: self.copy_query_to_clipboard_from_dialog(query_text))
        
        close_btn = QPushButton("Close")
        close_btn.setIcon(self.style().standardIcon(QStyle.SP_DialogCloseButton))
        close_btn.clicked.connect(dialog.accept)
        
        button_layout.addWidget(copy_btn)
        button_layout.addStretch()
        button_layout.addWidget(close_btn)
        
        layout.addWidget(query_text)
        layout.addLayout(button_layout)
        
        dialog.exec_()
    
    def copy_query_to_clipboard_from_dialog(self, text_edit):
        """Copy query from dialog text edit to clipboard."""
        clipboard = QApplication.clipboard()
        clipboard.setText(text_edit.toPlainText())
        QMessageBox.information(self, "Copied", "Query copied to clipboard.")

    def generate_query(self):
        """Collects selected fields and options, then calls the query generator."""
        selected_fields = {}
        
        # Process field_checkboxes which uses keys like "portal_order_id", "checkout_v1_amount"
        for key, checkbox in self.field_checkboxes.items():
            if checkbox.isChecked():
                # Parse the key to extract the tab name and field name
                # Key format is "tab_name_field_name" (e.g., "portal_order_id")
                parts = key.split("_", 1)  # Split at first underscore only
                if len(parts) == 2:
                    raw_tab_name, field_name = parts
                    # Sanitize tab_name to match query_generator.py expectations (e.g., "checkout v1" -> "checkout_v1")
                    tab_name = raw_tab_name.replace(" ", "_")
                    
                    # Initialize the list for this tab if it doesn't exist
                    if tab_name not in selected_fields:
                        selected_fields[tab_name] = []
                    # Add the field name to the list for this tab
                    selected_fields[tab_name].append(field_name)

        # Date filter
        date_filter = None
        if self.enable_date_filter_check.isChecked():
            date_filter = {
                "enabled": True,  # Explicitly set the enabled flag
                "start_date": self.query_start_date.date().toString("yyyy-MM-dd"),
                "end_date": self.query_end_date.date().toString("yyyy-MM-dd")
            }
            self.statusBar.showMessage(f"Filtering for dates {date_filter['start_date']} to {date_filter['end_date']}", 3000)
        else:
            date_filter = {"enabled": False}  # Explicitly set enabled = False
            self.statusBar.showMessage("Date filter not enabled.", 3000)
            
        # Get limit option
        limit = None
        if self.enable_limit_check.isChecked():
            limit = self.limit_spinbox.value()
            self.statusBar.showMessage(f"Applying row limit: {limit}", 3000)
        
        # Get reconciliation option
        include_reconciliation = self.reconciliation_check.isChecked()
        
        # Get distinct orders option
        return_distinct_orders = self.return_distinct_orders_check.isChecked()
        
        # Generate the query using the direct signature
        query = ""  # Initialize query
        try:
            # Import the generate_payment_analysis_query function
            from query_generator import generate_payment_analysis_query
            
            query = generate_payment_analysis_query(
                selected_fields=selected_fields,
                include_reconciliation=include_reconciliation,
                date_filter=date_filter,
                limit=limit,
                return_distinct_orders=return_distinct_orders
            )
        except Exception as e:
            error_msg = f"Error generating query: {str(e)}\n\n{traceback.format_exc()}"
            self.log_output.append(f"<span style='color:#e74c3c'><b>{error_msg}</b></span>")
            QMessageBox.critical(self, "Query Generation Error", error_msg)
            self.generated_query = "" # Ensure it's cleared on error
            self.statusBar.showMessage("Query generation failed.", 5000)
            return # Stop further execution if query generation fails
            
        # Store the generated query for later use
        self.generated_query = query
            
        # Confirmation message
        status_msg = "Query generated successfully"
        if limit is not None:
            status_msg += f" (with {limit} row limit)"
        if date_filter and date_filter.get("enabled", False):
            status_msg += f" (filtered from {date_filter['start_date']} to {date_filter['end_date']})"
        if not include_reconciliation:
            status_msg += " (without bank reconciliation)"
        if return_distinct_orders:
            status_msg += " (with distinct orders)"
        status_msg += ". Click 'View Generated Query' to see it."
        self.statusBar.showMessage(status_msg, 5000)
    
    def save_generated_query(self):
        """Save the generated SQL query to a file."""
        if not self.query_preview.toPlainText():
            QMessageBox.warning(self, "Empty Query", "Please generate a query first")
            return
            
        file_path, _ = QFileDialog.getSaveFileName(
            self, "Save SQL Query", "", "SQL Files (*.sql);;All Files (*)"
        )
        
        if file_path:
            try:
                with open(file_path, 'w') as f:
                    f.write(self.query_preview.toPlainText())
                self.statusBar.showMessage(f"Query saved to {file_path}", 5000)
            except Exception as e:
                QMessageBox.warning(self, "Error", f"Failed to save query: {str(e)}")
    
    def copy_query_to_clipboard(self):
        """Copy the generated SQL query to clipboard."""
        if not self.query_preview.toPlainText():
            QMessageBox.warning(self, "Empty Query", "Please generate a query first")
            return
            
        clipboard = QApplication.clipboard()
        clipboard.setText(self.query_preview.toPlainText())
        self.statusBar.showMessage("Query copied to clipboard", 3000)
    
    def execute_generated_query(self):
        """Execute the generated SQL query with optimization."""
        if not self.query_preview.toPlainText():
            QMessageBox.warning(self, "Empty Query", "Please generate a query first")
            return
            
        # Check database connection first
        if not self.db.connected:
            success, message = self.db.connect_from_env()
            if not success:
                QMessageBox.critical(self, "Database Connection Error", 
                                  f"Cannot connect to database: {message}\n\n"
                                  f"Please configure your database connection by clicking on "
                                  f"the 'Configure Database' button.")
                # Show the database configuration dialog
                self.configure_database()
                return
        
        # Verify connection with a simple query
        try:
            with self.db.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            self.log_output.append("Database connection verified successfully.")
        except Exception as e:
            QMessageBox.critical(self, "Database Connection Error", 
                              f"Database connection test failed: {str(e)}\n\n"
                              f"Please check your database credentials and configuration.")
            self.configure_database()
            return
            
        # Get output file path and create directory if needed
        output_dir = os.path.join("./output", "queries")
        os.makedirs(output_dir, exist_ok=True)
        output_file = os.path.join(output_dir, f"generated_query_results.xlsx")
                
        # Save the query to a temporary file
        temp_file = os.path.join(output_dir, "temp_generated_query.sql")
        try:
            with open(temp_file, 'w') as f:
                f.write(self.query_preview.toPlainText())
            
            # Create options for optimized execution
            batch_size = self.query_batch_size_spin.value() if hasattr(self, 'query_batch_size_spin') else 250000
            max_workers = self.query_max_workers_spin.value() if hasattr(self, 'query_max_workers_spin') else 4
            timeout = self.query_timeout_spin.value() if hasattr(self, 'query_timeout_spin') else 360
            
            options = {
                "batch_size": batch_size,
                "max_workers": max_workers,
                "timeout": timeout
            }
            
            # Add date filter from query generator
            if hasattr(self, 'query_start_date') and hasattr(self, 'query_end_date'):
                options["date_filter"] = {
                    "start_date": self.query_start_date.date().toString("yyyy-MM-dd"),
                    "end_date": self.query_end_date.date().toString("yyyy-MM-dd")
                }
                
            # Update UI state
            self.process_btn.setEnabled(False)
            self.stop_btn.setEnabled(True)
            self.run_action.setEnabled(False)
            self.stop_action.setEnabled(True)
            self.toolbar_stop_action.setEnabled(True)
                
            self.log_output.clear()
            self.progress_bar.setValue(0)
            self.status_label.setText("Executing generated query...")
            self.current_operation_label.setText("SQL Query Execution")
            
            # Store output file for later reference
            self.current_result_file = output_file
            
            # Start query execution thread
            self.query_stop_event = threading.Event()
            
            self.query_thread = QueryExecutionThread(
                self.db.engine, temp_file, output_file, options, self.query_stop_event
            )
            
            # Connect signals
            self.query_thread.progress_signal.connect(self.update_progress)
            self.query_thread.status_signal.connect(lambda status: self.status_label.setText(status))
            self.query_thread.log_signal.connect(self.update_log)
            
            # Modified to properly pass the result file to the process_finished method
            self.query_thread.finished_signal.connect(
                lambda success, message, result_file: self.process_finished(success, message, "Execute Generated Query", result_file)
            )
                
            # Start the thread
            self.query_thread.start()
                
            # Switch to processing tab
            self.tabs.setCurrentIndex(1)
                
        except Exception as e:
            QMessageBox.warning(self, "Error", f"Failed to execute query: {str(e)}")
            self.process_btn.setEnabled(True)
            self.stop_btn.setEnabled(False)
            self.run_action.setEnabled(True)
            self.stop_action.setEnabled(False)
            self.toolbar_stop_action.setEnabled(False)

    def load_field_definitions(self):
        """
        Load field definitions.
        Primary source for table columns: generated_config.py.
        Source for analysis/derived fields: query_generator.py.
        Populates self.field_map = {
            'portal': ['order_id', 'transaction_amount', ...],
            'metabase': ['portal_order_id', ...],
            'checkout_v1': ['amount', 'status', ...], (these are canonical names from generated_config)
            'bank': ['auth_code', 'rrn', ...], (canonical names from generated_config)
            'analysis': ['gateway_source', 'transaction_analysis', ...] (from query_generator.py)
        }
        """
        self.field_map = {}
        query_generator_module = None
        generated_config_module = None

        try:
            # Dynamically import query_generator
            module_path = Path(os.getcwd()) / "query_generator.py"
            spec = importlib.util.spec_from_file_location("query_generator_module", module_path)
            if spec and spec.loader:
                query_generator_module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(query_generator_module)
            else:
                self.log_output.append("Could not load query_generator.py for field definitions.")
                QMessageBox.warning(self, "Load Error", "Could not load query_generator.py.")
                return
        except Exception as e:
            self.log_output.append(f"Error importing query_generator.py: {e}")
            QMessageBox.warning(self, "Import Error", f"Error loading query_generator.py: {e}")
            return

        try:
            # Dynamically import generated_config
            gc_module_path = Path(os.getcwd()) / "generated_config.py"
            gc_spec = importlib.util.spec_from_file_location("generated_config_module", gc_module_path)
            if gc_spec and gc_spec.loader:
                generated_config_module = importlib.util.module_from_spec(gc_spec)
                gc_spec.loader.exec_module(generated_config_module)
            else:
                self.log_output.append("Could not load generated_config.py for field definitions.")
                QMessageBox.warning(self, "Load Error", "Could not load generated_config.py.")
                return
        except Exception as e:
            self.log_output.append(f"Error importing generated_config.py: {e}")
            QMessageBox.warning(self, "Import Error", f"Error loading generated_config.py: {e}")
            return

        if not hasattr(generated_config_module, 'CONFIG'):
            self.log_output.append("CONFIG dictionary not found in generated_config.py.")
            QMessageBox.warning(self, "Config Error", "CONFIG not found in generated_config.py.")
            return
            
        # Populate from generated_config.CONFIG for table columns
        # Example sources: "portal", "metabase", "checkout_v1", "checkout_v2", "payfort", "tamara", "bank"
        # These keys in generated_config.CONFIG should align with what query_generator.py expects
        # for its table aliases (p, m, c1, c2, pf, t, b).
        
        # Define mapping from generated_config source keys to field_map keys (which are also used by query_generator)
        # Generally, they are the same (lowercase).
        source_keys_from_config = ["portal", "metabase", "checkout_v1", "checkout_v2", "payfort", "tamara", "bank"]

        for source_name_cfg in source_keys_from_config:
            if source_name_cfg in generated_config_module.CONFIG:
                columns_data = generated_config_module.CONFIG[source_name_cfg].get("columns", {})
                field_names = []
                for canonical_name, col_detail in columns_data.items():
                    # The key 'canonical_name' (e.g., 'order_id', 'status') is what we want.
                    # col_detail['map_to'] should be the same as canonical_name here.
                    field_names.append(canonical_name)
                
                if field_names:
                    self.field_map[source_name_cfg] = sorted(list(set(field_names)))


        # Populate "analysis" fields from query_generator.py
        # These are derived fields, not direct table columns.
        if hasattr(query_generator_module, 'FIELD_MAPPINGS'):
            analysis_field_names = []
            for key in query_generator_module.FIELD_MAPPINGS.keys():
                if key.startswith("analysis_"):
                    # Extract short name, e.g., "gateway_source" from "analysis_gateway_source"
                    short_name = key.replace("analysis_", "", 1)
                    analysis_field_names.append(short_name)
            
            # Also, some analysis fields might be directly listed (like bank_match_type)
            # For now, let's assume bank_match_type is special.
            # If it's defined in generated_config.py for bank, it would be picked up there.
            # If it's purely an analysis field from query_generator, it needs to be added here.
            # Let's assume for now "bank_match_type" is still handled as an "analysis" field for selection.
            # The query_generator.py itself calculates it if 'bank' fields are selected.
            # This part needs to be robust: what defines an "analysis" field for selection purposes?

            # A more direct way for analysis fields if query_generator has a dedicated list:
            if hasattr(query_generator_module, 'ANALYSIS_FIELDS_FOR_SELECTION'): # Assuming such a list exists
                 analysis_field_names.extend(list(query_generator_module.ANALYSIS_FIELDS_FOR_SELECTION))
            else: # Fallback to inspecting FIELD_MAPPINGS for "analysis_" prefix
                # This is what we did above. Ensure no duplicates.
                pass # analysis_field_names already populated

            if "bank_match_type" not in analysis_field_names: # Example of adding a specific one if not covered
                 #This is tricky: bank_match_type is derived in query_gen if bank reconciliation is on
                 #but selected under "analysis" tab in GUI.
                 #For selection, let's ensure it's available if not already.
                 #The actual query_generator.py logic will produce it if bank fields are selected.
                 analysis_field_names.append("bank_match_type")


            if analysis_field_names:
                 self.field_map["analysis"] = sorted(list(set(analysis_field_names)))
        
        # Log the loaded field map for debugging
        # self.log_output.append(f"Field map loaded: {json.dumps(self.field_map, indent=2)}")
        #print(f"DEBUG: Field map loaded: {json.dumps(self.field_map, indent=2)}")


    def setup_field_selection_tab(self, tabs_widget, tab_label_display, field_names_list):
        """Set up a tab with checkboxes for field selection."""
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setSpacing(10)
        
        # Add a status header
        header_label = QLabel(f"{len(field_names_list)} fields available")
        header_label.setStyleSheet("font-weight: bold; color: #0078d7; margin-bottom: 5px;")
        layout.addWidget(header_label)
        
        # Create a scroll area for checkboxes to handle large number of fields
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameStyle(QFrame.NoFrame)
        
        # Container widget for checkboxes
        container = QWidget()
        container.setStyleSheet("background-color: white;")
        grid_layout = QGridLayout(container)
        grid_layout.setContentsMargins(5, 5, 5, 5)
        grid_layout.setSpacing(8)
        grid_layout.setHorizontalSpacing(25)  # More space between columns
        
        # Define important field categories with colors
        important_fields = {
            "identifiers": ["id", "transaction_id", "order_id", "reference", "rrn", "authorization_code"],
            "financials": ["amount", "currency", "fee", "total", "commission"],
            "status": ["status", "response", "success", "failed", "error"],
            "timing": ["date", "time", "timestamp", "created", "updated"],
            "methods": ["payment_method", "card", "bank", "gateway"]
        }
        
        # Color schemes for important fields
        category_colors = {
            "identifiers": "#4285F4",  # Blue
            "financials": "#0F9D58",   # Green
            "status": "#F4B400",       # Yellow
            "timing": "#DB4437",       # Red
            "methods": "#9C27B0"       # Purple
        }
        
        # Add checkboxes in a grid layout (3 columns)
        columns = 3
        sorted_fields = sorted(field_names_list)
        
        for i, field in enumerate(sorted_fields):
            row = i // columns
            col = i % columns
            
            checkbox = QCheckBox(field)
            checkbox.setMinimumWidth(130)
            
            # Determine if this is an important field and set styling
            is_important = False
            for category, patterns in important_fields.items():
                if any(pattern in field.lower() for pattern in patterns):
                    is_important = True
                    if category in category_colors:
                        color = category_colors[category]
                        checkbox.setStyleSheet(f"color: {color}; font-weight: bold;")
                    break
            
            # Pre-check important fields
            if is_important:
                checkbox.setChecked(True)
            
            # Store reference to the checkbox
            key = f"{tab_label_display.lower()}_{field}"
            self.field_checkboxes[key] = checkbox
            
            grid_layout.addWidget(checkbox, row, col)
        
        scroll.setWidget(container)
        layout.addWidget(scroll, 1)  # Give it stretch factor
        
        # Quick selection buttons specific to this tab
        selection_layout = QHBoxLayout()
        
        select_tab_btn = QPushButton(f"Select All in {tab_label_display}")
        select_tab_btn.setIcon(self.style().standardIcon(QStyle.SP_DialogApplyButton))
        select_tab_btn.clicked.connect(lambda: self.select_tab_fields(tab_label_display.lower()))
        
        deselect_tab_btn = QPushButton(f"Deselect All in {tab_label_display}")
        deselect_tab_btn.setIcon(self.style().standardIcon(QStyle.SP_DialogCancelButton))
        deselect_tab_btn.clicked.connect(lambda: self.deselect_tab_fields(tab_label_display.lower()))
        
        selection_layout.addWidget(select_tab_btn)
        selection_layout.addWidget(deselect_tab_btn)
        selection_layout.addStretch()
        
        layout.addLayout(selection_layout)
        tab.setLayout(layout)
        
        # Add tab to parent tabs widget
        tabs_widget.addTab(tab, f"{tab_label_display}")
    
    def select_tab_fields(self, tab_name):
        """Select all fields in a specific tab."""
        tab_prefix = tab_name.lower() + "_"
        count = 0
        for key, checkbox in self.field_checkboxes.items():
            if key.startswith(tab_prefix):
                checkbox.setChecked(True)
                count += 1
        self.statusBar.showMessage(f"Selected {count} fields in {tab_name} tab", 3000)
    
    def deselect_tab_fields(self, tab_name):
        """Deselect all fields in a specific tab."""
        tab_prefix = tab_name.lower() + "_"
        count = 0
        for key, checkbox in self.field_checkboxes.items():
            if key.startswith(tab_prefix):
                checkbox.setChecked(False)
                count += 1
        self.statusBar.showMessage(f"Deselected {count} fields in {tab_name} tab", 3000)
    
    def load_default_fields(self):
        """Load default field selection based on common fields."""
        default_patterns = [
            "id", "transaction_id", "amount", "status", "date", "time", 
            "payment_method", "order", "customer", "currency",
            "reference", "authorization_code", "rrn", "source",
            "response", "gateway", "merchant"
        ]
        
        # First deselect all
        self.deselect_all_fields()
        
        # Then select defaults
        for key, checkbox in self.field_checkboxes.items():
            for pattern in default_patterns:
                if pattern in key.lower():
                    checkbox.setChecked(True)
                    break
        
        self.statusBar.showMessage("Default fields loaded", 3000)
    
    def save_generated_query(self):
        """Save the generated query to a file."""
        if not self.generated_query:
            QMessageBox.warning(self, "No Query", "No query has been generated yet.")
            return
        
        file_path, _ = QFileDialog.getSaveFileName(
            self, "Save SQL Query", "", "SQL Files (*.sql)"
        )
        
        if file_path:
            try:
                with open(file_path, 'w') as f:
                    f.write(self.generated_query)
                self.statusBar.showMessage(f"Query saved to {file_path}", 5000)
            except Exception as e:
                QMessageBox.critical(self, "Error", f"Error saving query: {str(e)}")

    def setup_option_dependencies(self):
        """Setup dependencies between different options."""
        # Auth/Capture merging dependency
        self.merge_auth_capture_check.stateChanged.connect(self.toggle_merge_options)
        
        # Success only dependency
        self.success_only_check.stateChanged.connect(self.toggle_success_options)
        
        # Initialize states
        self.toggle_merge_options()
        self.toggle_success_options()

    def save_query_config(self):
        """Save the current query generator configuration to a JSON file."""
        file_path, _ = QFileDialog.getSaveFileName(
            self, "Save Query Configuration", "", "JSON Files (*.json)"
        )
        
        if not file_path:
            return
            
        try:
            # Collect all field selections
            field_selections = {}
            for key, checkbox in self.field_checkboxes.items():
                field_selections[key] = checkbox.isChecked()
            
            # Collect all option settings
            options = {
                "data_processing": {
                    "merge_auth_capture": self.merge_auth_capture_check.isChecked(),
                    "use_auth_rrn": self.use_auth_rrn_check.isChecked(),
                    "use_auth_code": self.use_auth_code_check.isChecked(),
                    "success_only": self.success_only_check.isChecked(),
                    "include_auth": self.include_auth_check.isChecked(),
                    "include_capture": self.include_capture_check.isChecked(),
                    "include_refund": self.include_refund_check.isChecked(),
                    "include_void": self.include_void_check.isChecked(),
                    "handle_partial_refunds": self.handle_partial_refunds_check.isChecked(),
                    "simplify_response_codes": self.simplify_response_codes_check.isChecked()
                },
                "reconciliation": {
                    "include_reconciliation": self.reconciliation_check.isChecked(),
                    "exact_match": self.exact_match_check.isChecked(),
                    "approx_match": self.approx_match_check.isChecked(),
                    "date_match": self.date_match_check.isChecked(),
                    "auth_rrn_match": self.auth_rrn_check.isChecked(),
                    "rrn_only_match": self.rrn_only_check.isChecked(),
                    "auth_only_match": self.auth_only_check.isChecked(),
                    "return_distinct_orders": self.return_distinct_orders_check.isChecked()
                },
                "output_options": { # Add new section for output options
                    "enable_limit": self.enable_limit_check.isChecked(),
                    "limit_value": self.limit_spinbox.value()
                },
                "date_filter_options": { # Add new section for date filter config
                    "enable_date_filter": self.enable_date_filter_check.isChecked(),
                    "start_date_text": self.query_start_date.date().toString("yyyy-MM-dd"), # Save the date text
                    "end_date_text": self.query_end_date.date().toString("yyyy-MM-dd") # Save the date text
                }
            }
            
            # Create the configuration dictionary
            config = {
                "field_selections": field_selections,
                "options": options
            }
            
            # Save to file
            with open(file_path, 'w') as f:
                json.dump(config, f, indent=2)
                
            self.statusBar.showMessage(f"Query configuration saved to {file_path}", 5000)
            
        except Exception as e:
            QMessageBox.critical(self, "Save Error", f"Error saving configuration: {str(e)}")
    
    def load_query_config(self):
        """Load query generator configuration from a JSON file."""
        file_path, _ = QFileDialog.getOpenFileName(
            self, "Load Query Configuration", "", "JSON Files (*.json)"
        )
        
        if not file_path:
            return
            
        try:
            # Load configuration from file
            with open(file_path, 'r') as f:
                config = json.load(f)
                
            # Apply field selections
            if "field_selections" in config:
                field_selections = config["field_selections"]
                missing_fields = []
                
                for key, is_checked in field_selections.items():
                    if key in self.field_checkboxes:
                        self.field_checkboxes[key].setChecked(is_checked)
                    else:
                        missing_fields.append(key)
                
                if missing_fields:
                    print(f"Warning: {len(missing_fields)} fields from the configuration were not found in the current UI")
            
            # Apply options
            if "options" in config:
                options = config["options"]
                
                # Apply data processing options
                if "data_processing" in options:
                    dp = options["data_processing"]
                    
                    if "merge_auth_capture" in dp:
                        self.merge_auth_capture_check.setChecked(dp["merge_auth_capture"])
                    if "use_auth_rrn" in dp:
                        self.use_auth_rrn_check.setChecked(dp["use_auth_rrn"])
                    if "use_auth_code" in dp:
                        self.use_auth_code_check.setChecked(dp["use_auth_code"])
                    if "success_only" in dp:
                        self.success_only_check.setChecked(dp["success_only"])
                    if "include_auth" in dp:
                        self.include_auth_check.setChecked(dp["include_auth"])
                    if "include_capture" in dp:
                        self.include_capture_check.setChecked(dp["include_capture"])
                    if "include_refund" in dp:
                        self.include_refund_check.setChecked(dp["include_refund"])
                    if "include_void" in dp:
                        self.include_void_check.setChecked(dp["include_void"])
                    if "handle_partial_refunds" in dp:
                        self.handle_partial_refunds_check.setChecked(dp["handle_partial_refunds"])
                    if "simplify_response_codes" in dp:
                        self.simplify_response_codes_check.setChecked(dp["simplify_response_codes"])
                
                # Apply reconciliation options
                if "reconciliation" in options:
                    rc = options["reconciliation"]
                    
                    if "include_reconciliation" in rc:
                        self.reconciliation_check.setChecked(rc["include_reconciliation"])
                    if "exact_match" in rc:
                        self.exact_match_check.setChecked(rc["exact_match"])
                    if "approx_match" in rc:
                        self.approx_match_check.setChecked(rc["approx_match"])
                    if "date_match" in rc:
                        self.date_match_check.setChecked(rc["date_match"])
                    if "auth_rrn_match" in rc:
                        self.auth_rrn_check.setChecked(rc["auth_rrn_match"])
                    if "rrn_only_match" in rc:
                        self.rrn_only_check.setChecked(rc["rrn_only_match"])
                    if "auth_only_match" in rc:
                        self.auth_only_check.setChecked(rc["auth_only_match"])
                    if "return_distinct_orders" in rc:
                        self.return_distinct_orders_check.setChecked(rc["return_distinct_orders"])
            
            # Apply output options
            if "output_options" in options: # Check for the new section
                oo = options["output_options"]
                if "enable_limit" in oo:
                    self.enable_limit_check.setChecked(oo["enable_limit"])
                if "limit_value" in oo:
                    self.limit_spinbox.setValue(oo["limit_value"])
                    self.limit_spinbox.setEnabled(self.enable_limit_check.isChecked()) # Ensure spinbox state matches checkbox
            
            # Apply date filter options
            if "date_filter_options" in options:
                dfo = options["date_filter_options"]
                if "enable_date_filter" in dfo:
                    self.enable_date_filter_check.setChecked(dfo["enable_date_filter"])
                
                # Enable/disable date edits based on the loaded checkbox state
                date_edits_enabled = self.enable_date_filter_check.isChecked()
                self.query_start_date.setEnabled(date_edits_enabled)
                self.query_end_date.setEnabled(date_edits_enabled)

                if "start_date_text" in dfo and dfo["start_date_text"]:
                    try:
                        self.query_start_date.setDate(datetime.strptime(dfo["start_date_text"], "%Y-%m-%d").date())
                    except (ValueError, TypeError):
                        pass # ignore if invalid
                if "end_date_text" in dfo and dfo["end_date_text"]:
                    try:
                        self.query_end_date.setDate(datetime.strptime(dfo["end_date_text"], "%Y-%m-%d").date())
                    except (ValueError, TypeError):
                        pass # ignore if invalid
            
            # Apply date filter to Query Generator tab if it exists in config (legacy handling for older config)
            if "query_generator" in config and "filtering_and_output" in config["query_generator"]:
                fo_config = config["query_generator"]["filtering_and_output"]
                if "date_range" in fo_config:
                    qg_date_range = fo_config["date_range"]
                    if "start_date" in qg_date_range and qg_date_range["start_date"] and hasattr(self, 'query_start_date'):
                        self.enable_date_filter_check.setChecked(True) # Assume enabled if present in old config
                        try:
                            self.query_start_date.setDate(datetime.strptime(qg_date_range["start_date"], "%Y-%m-%d").date())
                        except (ValueError, TypeError):
                            pass
                    if "end_date" in qg_date_range and qg_date_range["end_date"] and hasattr(self, 'query_end_date'):
                        try:
                            self.query_end_date.setDate(datetime.strptime(qg_date_range["end_date"], "%Y-%m-%d").date())
                        except (ValueError, TypeError):
                            pass
                if "row_limit" in fo_config:
                    qg_row_limit = fo_config["row_limit"]
                    if "enable_limit" in qg_row_limit and hasattr(self, 'enable_limit_check'):
                        self.enable_limit_check.setChecked(qg_row_limit["enable_limit"])
                    if "limit_value" in qg_row_limit and hasattr(self, 'limit_spinbox'):
                        self.limit_spinbox.setValue(qg_row_limit["limit_value"])
                
                # Update enabled state of date/limit controls after loading legacy config
                if hasattr(self, 'enable_date_filter_check'):
                    date_edits_enabled = self.enable_date_filter_check.isChecked()
                    if hasattr(self, 'query_start_date'): self.query_start_date.setEnabled(date_edits_enabled)
                    if hasattr(self, 'query_end_date'): self.query_end_date.setEnabled(date_edits_enabled)
                if hasattr(self, 'enable_limit_check') and hasattr(self, 'limit_spinbox'):
                    self.limit_spinbox.setEnabled(self.enable_limit_check.isChecked())

            # Generate a new query with the loaded settings
            if hasattr(self, 'generate_query'):
                self.generate_query()
            
            self.statusBar.showMessage(f"Query configuration loaded from {file_path}", 5000)
            
        except Exception as e:
            QMessageBox.critical(self, "Load Error", f"Error loading configuration: {str(e)}")
            print(f"Error details: {traceback.format_exc()}")

    def save_application_config(self):
        """Save all application settings to a JSON file."""
        file_path, _ = QFileDialog.getSaveFileName(
            self, "Save Application Configuration", "", "JSON Files (*.json)"
        )
        
        if not file_path:
            return
            
        try:
            # Create a comprehensive configuration dictionary
            config = {
                "app_version": "1.0.0",
                "saved_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "tabs": {
                    "data_sources": self.get_data_sources_config(),
                    "processing": self.get_processing_config(),
                    "query_generator": self.get_query_generator_config(),
                    "output": self.get_output_config()
                }
            }
            
            # Save to file
            with open(file_path, 'w') as f:
                json.dump(config, f, indent=2)
                
            self.statusBar.showMessage(f"Application configuration saved to {file_path}", 5000)
            
        except Exception as e:
            QMessageBox.critical(self, "Save Error", f"Error saving application configuration: {str(e)}")
            print(f"Error details: {traceback.format_exc()}")
    
    def load_application_config(self):
        """Load application settings from a JSON file."""
        file_path, _ = QFileDialog.getOpenFileName(
            self, "Load Application Configuration", "", "JSON Files (*.json)"
        )
        
        if not file_path:
            return
            
        try:
            # Load configuration from file
            with open(file_path, 'r') as f:
                config = json.load(f)
            
            if "tabs" not in config:
                raise ValueError("Invalid configuration file: 'tabs' section not found")
                
            tabs = config["tabs"]
            
            # Apply configuration to each tab
            if "data_sources" in tabs:
                self.apply_data_sources_config(tabs["data_sources"])
                
            if "processing" in tabs:
                self.apply_processing_config(tabs["processing"])
                
            if "query_generator" in tabs:
                self.apply_query_generator_config(tabs["query_generator"])
                
            if "output" in tabs:
                self.apply_output_config(tabs["output"])
                
            self.statusBar.showMessage(f"Application configuration loaded from {file_path}", 5000)
            
        except Exception as e:
            QMessageBox.critical(self, "Load Error", f"Error loading application configuration: {str(e)}")
            print(f"Error details: {traceback.format_exc()}")
    
    def get_data_sources_config(self):
        """Get configuration from the Data Sources tab."""
        try:
            # Load paths.json if it exists
            path = Path("paths.json")
            if path.exists():
                with open(path, 'r') as f:
                    data = json.load(f)
                return data
            else:
                return {}
        except Exception as e:
            print(f"Error getting data sources config: {str(e)}")
            return {}
    
    def get_processing_config(self):
        """Get configuration from the Processing tab."""
        return {
            "process_new_data": self.process_new_data_radio.isChecked(),
            "parallel_processing": self.parallel_check.isChecked() if hasattr(self, 'parallel_check') else True,
            "max_workers": self.max_workers_spin.value() if hasattr(self, 'max_workers_spin') else 4,
            "force_overwrite": self.force_overwrite_check.isChecked() if hasattr(self, 'force_overwrite_check') else False,
            "batch_size": self.batch_size_spin.value() if hasattr(self, 'batch_size_spin') else 250000
        }
    
    def get_query_generator_config(self):
        """Get configuration from the Query Generator tab."""
        # Field selections
        field_selections = {}
        for key, checkbox in self.field_checkboxes.items():
            field_selections[key] = checkbox.isChecked()
        
        # Option settings
        options = {
            "data_processing": {
                "merge_auth_capture": self.merge_auth_capture_check.isChecked(),
                "use_auth_rrn": self.use_auth_rrn_check.isChecked(),
                "use_auth_code": self.use_auth_code_check.isChecked(),
                "success_only": self.success_only_check.isChecked(),
                "include_auth": self.include_auth_check.isChecked(),
                "include_capture": self.include_capture_check.isChecked(),
                "include_refund": self.include_refund_check.isChecked(),
                "include_void": self.include_void_check.isChecked(),
                "handle_partial_refunds": self.handle_partial_refunds_check.isChecked(),
                "simplify_response_codes": self.simplify_response_codes_check.isChecked()
            },
            "reconciliation": {
                "include_reconciliation": self.reconciliation_check.isChecked(),
                "exact_match": self.exact_match_check.isChecked(),
                "approx_match": self.approx_match_check.isChecked(),
                "date_match": self.date_match_check.isChecked(),
                "auth_rrn_match": self.auth_rrn_check.isChecked(),
                "rrn_only_match": self.rrn_only_check.isChecked(),
                "auth_only_match": self.auth_only_check.isChecked(),
                "return_distinct_orders": self.return_distinct_orders_check.isChecked()
            },
            "output_options": { # Add new section for output options
                "enable_limit": self.enable_limit_check.isChecked(),
                "limit_value": self.limit_spinbox.value()
            }
        }
        
        return {
            "field_selections": field_selections,
            "options": options
        }
    
    def get_output_config(self):
        """Get configuration from the Output tab."""
        return {
            "output_directory": self.output_dir_edit.text(),
            "query_file": self.query_file_edit.text() if hasattr(self, 'query_file_edit') else "payment_analysis.sql",
            "date_range": {
                "start_date": self.query_start_date.date().toString("yyyy-MM-dd") if hasattr(self, 'query_start_date') else None,
                "end_date": self.query_end_date.date().toString("yyyy-MM-dd") if hasattr(self, 'query_end_date') else None
            },
            "output_format": self.output_format.currentText() if hasattr(self, 'output_format') else "Excel (.xlsx)",
            "batch_size": self.query_batch_size_spin.value() if hasattr(self, 'query_batch_size_spin') else 250000,
            "chunk_size": self.chunk_size_spin.value() if hasattr(self, 'chunk_size_spin') else 100000,
            "max_workers": self.query_max_workers_spin.value() if hasattr(self, 'query_max_workers_spin') else 4,
            "timeout": self.query_timeout_spin.value() if hasattr(self, 'query_timeout_spin') else 360,
            "use_multiprocessing": self.use_multiprocessing_check.isChecked() if hasattr(self, 'use_multiprocessing_check') else True,
            "include_view_creation": self.include_view_creation_check.isChecked() if hasattr(self, 'include_view_creation_check') else True,
            "include_index_creation": self.include_index_creation_check.isChecked() if hasattr(self, 'include_index_creation_check') else True
        }
    
    def apply_data_sources_config(self, config):
        """Apply configuration to the Data Sources tab."""
        try:
            # Save to paths.json
            path = Path("paths.json")
            with open(path, 'w') as f:
                json.dump(config, f, indent=4)
            
            # Update the sources table if it exists
            if hasattr(self, 'update_sources_table') and "sources" in config:
                self.update_sources_table(config["sources"])
        except Exception as e:
            print(f"Error applying data sources config: {str(e)}")
    
    def apply_processing_config(self, config):
        """Apply configuration to the Processing tab."""
        try:
            # Apply process mode
            if "process_new_data" in config and hasattr(self, 'process_new_data_radio'):
                self.process_new_data_radio.setChecked(config["process_new_data"])
                if hasattr(self, 'use_existing_data_radio'):
                    self.use_existing_data_radio.setChecked(not config["process_new_data"])
            
            # Apply parallel processing
            if "parallel_processing" in config and hasattr(self, 'parallel_check'):
                self.parallel_check.setChecked(config["parallel_processing"])
            
            # Apply max workers
            if "max_workers" in config and hasattr(self, 'max_workers_spin'):
                self.max_workers_spin.setValue(config["max_workers"])
            
            # Apply force overwrite
            if "force_overwrite" in config and hasattr(self, 'force_overwrite_check'):
                self.force_overwrite_check.setChecked(config["force_overwrite"])
            
            # Apply batch size
            if "batch_size" in config and hasattr(self, 'batch_size_spin'):
                self.batch_size_spin.setValue(config["batch_size"])
                if hasattr(self, 'update_batch_slider'):
                    self.update_batch_slider(config["batch_size"])
        except Exception as e:
            print(f"Error applying processing config: {str(e)}")
    
    def apply_query_generator_config(self, config):
        """Apply configuration to the Query Generator tab."""
        try:
            # Apply field selections
            if "field_selections" in config:
                field_selections = config["field_selections"]
                for key, is_checked in field_selections.items():
                    if key in self.field_checkboxes:
                        self.field_checkboxes[key].setChecked(is_checked)
            
            # Apply options
            if "options" in config:
                options = config["options"]
                
                # Apply data processing options
                if "data_processing" in options:
                    dp = options["data_processing"]
                    
                    if "merge_auth_capture" in dp:
                        self.merge_auth_capture_check.setChecked(dp["merge_auth_capture"])
                    if "use_auth_rrn" in dp:
                        self.use_auth_rrn_check.setChecked(dp["use_auth_rrn"])
                    if "use_auth_code" in dp:
                        self.use_auth_code_check.setChecked(dp["use_auth_code"])
                    if "success_only" in dp:
                        self.success_only_check.setChecked(dp["success_only"])
                    if "include_auth" in dp:
                        self.include_auth_check.setChecked(dp["include_auth"])
                    if "include_capture" in dp:
                        self.include_capture_check.setChecked(dp["include_capture"])
                    if "include_refund" in dp:
                        self.include_refund_check.setChecked(dp["include_refund"])
                    if "include_void" in dp:
                        self.include_void_check.setChecked(dp["include_void"])
                    if "handle_partial_refunds" in dp:
                        self.handle_partial_refunds_check.setChecked(dp["handle_partial_refunds"])
                    if "simplify_response_codes" in dp:
                        self.simplify_response_codes_check.setChecked(dp["simplify_response_codes"])
                
                # Apply reconciliation options
                if "reconciliation" in options:
                    rc = options["reconciliation"]
                    
                    if "include_reconciliation" in rc:
                        self.reconciliation_check.setChecked(rc["include_reconciliation"])
                    if "exact_match" in rc:
                        self.exact_match_check.setChecked(rc["exact_match"])
                    if "approx_match" in rc:
                        self.approx_match_check.setChecked(rc["approx_match"])
                    if "date_match" in rc:
                        self.date_match_check.setChecked(rc["date_match"])
                    if "auth_rrn_match" in rc:
                        self.auth_rrn_check.setChecked(rc["auth_rrn_match"])
                    if "rrn_only_match" in rc:
                        self.rrn_only_check.setChecked(rc["rrn_only_match"])
                    if "auth_only_match" in rc:
                        self.auth_only_check.setChecked(rc["auth_only_match"])
                    if "return_distinct_orders" in rc:
                        self.return_distinct_orders_check.setChecked(rc["return_distinct_orders"])
            
            # Apply output options
            if "output_options" in options: # Check for the new section
                oo = options["output_options"]
                if "enable_limit" in oo:
                    self.enable_limit_check.setChecked(oo["enable_limit"])
                if "limit_value" in oo:
                    self.limit_spinbox.setValue(oo["limit_value"])
                    self.limit_spinbox.setEnabled(self.enable_limit_check.isChecked()) # Ensure spinbox state matches checkbox
            
            # Generate a new query with the loaded settings
            self.generate_query()
        except Exception as e:
            print(f"Error applying query generator config: {str(e)}")
    
    def apply_output_config(self, config):
        """Apply configuration to the Output tab."""
        try:
            # Apply output directory
            if "output_directory" in config and hasattr(self, 'output_dir_edit'):
                self.output_dir_edit.setText(config["output_directory"])
                self.output_dir = config["output_directory"]
            
            # Apply query file
            if "query_file" in config and hasattr(self, 'query_file_edit'):
                self.query_file_edit.setText(config["query_file"])
            
            # Apply date range
            if "date_range" in config:
                date_range = config["date_range"]
                
                if "start_date" in date_range and date_range["start_date"] and hasattr(self, 'query_start_date'):
                    try:
                        self.query_start_date.setDate(datetime.strptime(date_range["start_date"], "%Y-%m-%d").date())
                    except:
                        pass
                
                if "end_date" in date_range and date_range["end_date"] and hasattr(self, 'query_end_date'):
                    try:
                        self.query_end_date.setDate(datetime.strptime(date_range["end_date"], "%Y-%m-%d").date())
                    except:
                        pass
            
            # Apply output format
            if "output_format" in config and hasattr(self, 'output_format'):
                index = self.output_format.findText(config["output_format"])
                if index >= 0:
                    self.output_format.setCurrentIndex(index)
            
            # Apply batch size
            if "batch_size" in config and hasattr(self, 'query_batch_size_spin'):
                self.query_batch_size_spin.setValue(config["batch_size"])
            
            # Apply chunk size
            if "chunk_size" in config and hasattr(self, 'chunk_size_spin'):
                self.chunk_size_spin.setValue(config["chunk_size"])
            
            # Apply max workers
            if "max_workers" in config and hasattr(self, 'query_max_workers_spin'):
                self.query_max_workers_spin.setValue(config["max_workers"])
            
            # Apply timeout
            if "timeout" in config and hasattr(self, 'query_timeout_spin'):
                self.query_timeout_spin.setValue(config["timeout"])
            
            # Apply multiprocessing
            if "use_multiprocessing" in config and hasattr(self, 'use_multiprocessing_check'):
                self.use_multiprocessing_check.setChecked(config["use_multiprocessing"])
            
            # Apply view creation
            if "include_view_creation" in config and hasattr(self, 'include_view_creation_check'):
                self.include_view_creation_check.setChecked(config["include_view_creation"])
            
            # Apply index creation
            if "include_index_creation" in config and hasattr(self, 'include_index_creation_check'):
                self.include_index_creation_check.setChecked(config["include_index_creation"])
        except Exception as e:
            print(f"Error applying output config: {str(e)}")

    def find_matching_brace(self, text):
        """Find the position of the matching closing brace in text."""
        count = 0
        for i, char in enumerate(text):
            if char == '{':
                count += 1
            elif char == '}':
                count -= 1
                if count == -1:
                    return i
        return -1

    def toggle_reconciliation_options(self):
        """Enable/disable reconciliation options based on main checkbox."""
        enabled = self.reconciliation_check.isChecked()
        
        # Toggle all sub-option widgets
        self.exact_match_check.setEnabled(enabled)
        self.approx_match_check.setEnabled(enabled)
        self.date_match_check.setEnabled(enabled)
        self.auth_rrn_check.setEnabled(enabled)
        self.rrn_only_check.setEnabled(enabled)
        self.auth_only_check.setEnabled(enabled)
        # Return distinct orders is always enabled as it's a result option, not a matching option
        # Don't add: self.return_distinct_orders_check.setEnabled(enabled)
    
    def toggle_merge_options(self):
        """Toggle auth/capture merge sub-options based on main checkbox."""
        enabled = self.merge_auth_capture_check.isChecked()
        self.use_auth_rrn_check.setEnabled(enabled)
        self.use_auth_code_check.setEnabled(enabled)
    
    def toggle_success_options(self):
        """Toggle options that are dependent on the success-only checkbox."""
        # Could be expanded if needed
        pass
    
    def select_all_fields(self):
        """Select all checkboxes in the field selection tabs."""
        for checkbox in self.field_checkboxes.values():
            checkbox.setChecked(True)
        self.statusBar.showMessage("All fields selected", 3000)
    
    def deselect_all_fields(self):
        """Deselect all checkboxes in the field selection tabs except primary keys and join keys."""
        # Define fields that should never be deselected based on payment_analysis.sql output
        critical_field_patterns = [
            "gateway_source", "gateway_order_id", "gateway_transaction_id", 
            "amount", "status", "transaction_date", "authorization_code", "rrn", 
            "payment_method", "response_code", "final_rrn", "final_authorization_code",
            "transaction_analysis", "transaction_outcome", "bank_match_type",
            "order_id", "transaction_id"  # Keep backward compatibility with older patterns
        ]
        
        for key, checkbox in self.field_checkboxes.items():
            # Check if the field is a critical field that should be preserved
            should_preserve = any(pattern in key.lower() for pattern in critical_field_patterns)
            
            if not should_preserve:
                checkbox.setChecked(False)
                
        self.statusBar.showMessage("Fields deselected (except key output fields)", 3000)

    def enhance_tooltip_widget(self, widget):
        """Make tooltips more noticeable by showing a 'tooltip available' cursor and changing border on hover."""
        if widget.toolTip():
            # Create the event filter only for widgets with tooltips
            class HoverEventFilter(QObject):
                def eventFilter(self, obj, event):
                    if event.type() == QEvent.Enter:
                        # Change cursor to indicate tooltip is available
                        QApplication.setOverrideCursor(QCursor(Qt.WhatsThisCursor))
                        # Optional: can also change widget style temporarily
                        if isinstance(obj, QGroupBox):
                            obj.setStyleSheet("QGroupBox { border: 2px solid #0078D7; }")
                    elif event.type() == QEvent.Leave:
                        # Restore cursor
                        QApplication.restoreOverrideCursor()
                        # Restore widget style
                        if isinstance(obj, QGroupBox):
                            obj.setStyleSheet("")
                    return False
            
            # Apply the event filter
            hover_filter = HoverEventFilter(widget)
            widget.installEventFilter(hover_filter)
            # Store reference to prevent garbage collection
            if not hasattr(self, '_event_filters'):
                self._event_filters = []
            self._event_filters.append(hover_filter)

    def _ensure_env_file_exists(self):
        """Checks if 'src/.env' exists and creates it with defaults if not."""
        env_path = Path(__file__).parent / '.env'
        if not env_path.exists():
            try:
                with open(env_path, 'w') as f:
                    f.write("# PostgreSQL Connection Details\\n")
                    f.write("DB_HOST=localhost\\n")
                    f.write("DB_PORT=5432\\n")
                    f.write("DB_NAME=your_database_name\\n")
                    f.write("DB_USER=your_username\\n")
                    f.write("DB_PASSWORD=your_password\\n")
                
                # Optionally inform the user
                if hasattr(self, 'statusBar') and self.statusBar:
                     self.statusBar.showMessage("Created default 'src/.env' file. Please configure your database details.", 10000)
                else: # Fallback if statusbar not yet initialized
                    print("Created default 'src/.env' file. Please configure your database details.")
                
                # Display a message box to the user as well for more prominence
                QMessageBox.information(
                    None, # Parent can be None if called before main window is fully up
                    ".env File Created",
                    f"A default .env file has been created at: {env_path}\\n\\n"
                    "Please open this file and update it with your actual PostgreSQL database credentials "
                    "before attempting to connect to the database."
                )
            except Exception as e:
                error_msg = f"Failed to create default .env file: {str(e)}"
                if hasattr(self, 'log_output') and self.log_output: # Check if log_output exists
                    self.log_output.append(f"<span style='color:#e74c3c'><b>{error_msg}</b></span>")
                else:
                    print(error_msg) # Fallback to console print

                QMessageBox.warning(
                    None,
                    "Error Creating .env",
                    error_msg
                )

class DatabaseConfigDialog(QDialog):
    """Dialog to configure database connection settings."""
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.db = parent.db if parent else DatabaseConnection()
        self.setup_ui()
        self.load_env_config()
    
    def setup_ui(self):
        """Set up the dialog UI."""
        self.setWindowTitle("Database Configuration")
        self.setMinimumWidth(450)
        
        # Create form layout
        layout = QFormLayout()
        
        # Database host
        self.host_edit = QLineEdit()
        self.host_edit.setPlaceholderText("e.g. localhost, 127.0.0.1")
        layout.addRow("Host:", self.host_edit)
        
        # Database port
        self.port_edit = QLineEdit()
        self.port_edit.setPlaceholderText("5432")
        layout.addRow("Port:", self.port_edit)
        
        # Database name
        self.database_edit = QLineEdit()
        self.database_edit.setPlaceholderText("postgres")
        layout.addRow("Database:", self.database_edit)
        
        # Username
        self.username_edit = QLineEdit()
        self.username_edit.setPlaceholderText("postgres")
        layout.addRow("Username:", self.username_edit)
        
        # Password
        self.password_edit = QLineEdit()
        self.password_edit.setEchoMode(QLineEdit.Password)
        layout.addRow("Password:", self.password_edit)
        
        # Add status display
        self.status_label = QLabel("Enter database connection details and test connection.")
        self.status_label.setWordWrap(True)
        self.status_label.setStyleSheet("color: #666;")
        layout.addRow(self.status_label)
        
        # Add test connection button
        self.test_btn = QPushButton("Test Connection")
        self.test_btn.clicked.connect(self.test_connection)
        layout.addRow("", self.test_btn)
        
        # Add help text
        help_text = ("These settings will be saved to a .env file and used for database connections. "
                    "The password will be stored in plain text, so ensure your .env file is secure.")
        help_label = QLabel(help_text)
        help_label.setWordWrap(True)
        help_label.setStyleSheet("color: #666; font-style: italic;")
        layout.addRow(help_label)
        
        # Add dialog buttons
        self.button_box = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        self.button_box.accepted.connect(self.save_config)
        self.button_box.rejected.connect(self.reject)
        layout.addRow(self.button_box)
        
        # Set layout
        self.setLayout(layout)
    
    def load_env_config(self):
        """Load configuration from .env file if available."""
        from dotenv import load_dotenv
        import os
        
        load_dotenv()
        
        # Set values from environment variables or defaults
        self.host_edit.setText(os.getenv('DB_HOST', 'localhost'))
        self.port_edit.setText(os.getenv('DB_PORT', '5432'))
        self.database_edit.setText(os.getenv('DB_NAME', 'postgres'))
        self.username_edit.setText(os.getenv('DB_USER', 'postgres'))
        self.password_edit.setText(os.getenv('DB_PASSWORD', ''))
    
    def save_config(self):
        """Save database configuration to .env file."""
        try:
            # Create or update .env file
            with open('.env', 'w') as f:
                f.write(f"DB_HOST={self.host_edit.text().strip()}\n")
                f.write(f"DB_PORT={self.port_edit.text().strip()}\n")
                f.write(f"DB_NAME={self.database_edit.text().strip()}\n")
                f.write(f"DB_USER={self.username_edit.text().strip()}\n")
                f.write(f"DB_PASSWORD={self.password_edit.text()}\n")
            
            # Show success message
            QMessageBox.information(self, "Configuration Saved", "Database configuration saved successfully.")
            
            # Try connecting with new parameters
            success, message = self.db.connect_from_env()
            if success:
                self.accept()  # Close dialog on successful connection
            else:
                # Ask if they want to continue anyway
                reply = QMessageBox.question(
                    self, "Connection Failed", 
                    f"{message}\n\nWould you like to save these settings anyway?",
                    QMessageBox.Yes | QMessageBox.No, 
                    QMessageBox.No
                )
                if reply == QMessageBox.Yes:
                    self.accept()
        
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to save configuration: {str(e)}")
    
    def test_connection(self):
        """Test database connection with current settings."""
        try:
            # Update status
            self.status_label.setText("Testing connection...")
            self.status_label.setStyleSheet("color: #0000FF;")
            
            # Get values from form
            host = self.host_edit.text().strip()
            port = self.port_edit.text().strip()
            database = self.database_edit.text().strip()
            username = self.username_edit.text().strip()
            password = self.password_edit.text()
            
            # Validate inputs
            if not host or not port or not database or not username:
                self.status_label.setText("Please fill in all required fields.")
                self.status_label.setStyleSheet("color: #FF0000;")
                return
            
            # Test connection using the db connection object
            success, message = self.db.test_connection(
                host, port, database, username, password
            )
            
            if success:
                self.status_label.setText(f"✅ {message}")
                self.status_label.setStyleSheet("color: #008800; font-weight: bold;")
                self.test_btn.setStyleSheet("background-color: #8FBC8F;")
            else:
                self.status_label.setText(f"❌ {message}")
                self.status_label.setStyleSheet("color: #FF0000;")
                self.test_btn.setStyleSheet("")
                
                # Show detailed error dialog
                QMessageBox.warning(self, "Connection Test Failed", 
                                    f"{message}\n\nPlease check your database settings and try again.")
            
        except Exception as e:
            self.status_label.setText(f"❌ Error: {str(e)}")
            self.status_label.setStyleSheet("color: #FF0000;")
            self.test_btn.setStyleSheet("")
            
            QMessageBox.critical(self, "Connection Test Error", 
                                f"An error occurred during the connection test: {str(e)}")

class QueryExecutionThread(QThread):
    """Thread to handle SQL query execution with progress reporting."""
    progress_signal = pyqtSignal(int)
    status_signal = pyqtSignal(str)
    time_signal = pyqtSignal(str)
    finished_signal = pyqtSignal(bool, str, str)  # Success flag, message, result file path
    log_signal = pyqtSignal(str)  # For detailed logs

    def __init__(self, engine_or_conn_string, query_file, output_file, options, stop_event, output_format_for_thread="excel"):
        super().__init__()
        if isinstance(engine_or_conn_string, str):
            self.connection_url = engine_or_conn_string
            self.log_signal.emit("Received connection string directly")
        elif hasattr(engine_or_conn_string, 'url'):
            self.connection_url = str(engine_or_conn_string.url)
            self.log_signal.emit("Extracted connection URL from engine")
        else:
            self.connection_url = str(engine_or_conn_string)
            self.log_signal.emit("Using engine object as string")
        
        self.query_file = query_file
        self.output_file = output_file
        self.options = options
        self.stop_event = stop_event
        self.output_format = output_format_for_thread # Store the format
        
    def run(self):
        """Execute the SQL query and handle result processing."""
        engine = None
        try:
            # Send initial signals
            self.status_signal.emit("Starting query execution...")
            self.progress_signal.emit(0)
            self.log_signal.emit("QueryExecutionThread: Starting execution")
            
            # Import sql_executor if available, otherwise use direct approach
            try:
                self.log_signal.emit("Attempting to import sql_executor module...")
                from sql_executor import run_sql_query, create_engine, text
                self.log_signal.emit("Successfully imported sql_executor module")
                
                # Use the dedicated executor
                self.status_signal.emit("Executing query using optimized executor...")
                
                # Load query file content to pass directly
                try:
                    with open(self.query_file, 'r') as f:
                        query_content = f.read()
                    self.log_signal.emit(f"Read query file: {len(query_content)} bytes")
                except Exception as e:
                    self.log_signal.emit(f"Error reading query file: {str(e)}")
                    raise
                
                # Execute query directly without using multiprocessing
                self.log_signal.emit("Executing query directly...")
                try:
                    # Create a new engine with the connection URL
                    from sqlalchemy import create_engine
                    
                    # Log info about the connection string (hiding password)
                    conn_url_parts = self.connection_url.split('@')
                    if len(conn_url_parts) >= 2:
                        masked_url = conn_url_parts[0].split(':')[0] + ':***@' + conn_url_parts[1]
                        self.log_signal.emit(f"Creating engine with masked connection URL: {masked_url}")
                    else:
                        self.log_signal.emit("Creating database engine from connection URL")
                    
                    # Create engine with appropriate settings
                    engine = create_engine(
                        self.connection_url,
                        connect_args={
                            "connect_timeout": 10,
                            "options": "-c statement_timeout=120000"  # Default 120 second statement timeout
                        },
                        pool_size=5,
                        max_overflow=10,
                        pool_timeout=30,
                        pool_recycle=1800
                    )
                    
                    # Test connection
                    with engine.connect() as conn:
                        conn.execute(text("SELECT 1"))
                        self.log_signal.emit("Database connection verified successfully")
                    
                    # Define progress callback for sql_executor
                    start_time = time.time()
                    def progress_callback(progress, status):
                        self.progress_signal.emit(progress)
                        self.status_signal.emit(status)
                        self.log_signal.emit(f"Progress: {progress}%, Status: {status}")
                        
                        # Update time
                        elapsed = time.time() - start_time
                        hours, remainder = divmod(int(elapsed), 3600)
                        minutes, seconds = divmod(remainder, 60)
                        time_str = f"{hours:02}:{minutes:02}:{seconds:02}"
                        self.time_signal.emit(time_str)
                    
                    # Call run_sql_query directly with env_path instead of engine
                    result = run_sql_query(
                        query_file=self.query_file,
                        output_file=self.output_file,
                        timeout=self.options.get('timeout', 360),
                        max_workers=self.options.get('max_workers', 4),
                        batch_size=self.options.get('batch_size', 250000),
                        progress_callback=progress_callback,
                        date_filter=self.options.get('date_filter', None),
                        env_path=Path('.env'),  # Use default .env file
                        output_format=self.output_format # Pass stored output_format
                    )
                    
                    if result.get('success', False):
                        self.status_signal.emit("Query completed successfully")
                        self.progress_signal.emit(100)
                        self.finished_signal.emit(True, "Query executed successfully", self.output_file)
                        self.log_signal.emit(f"Query execution successful, output saved to {self.output_file}")
                    else:
                        error_msg = result.get('message', "Unknown error")
                        self.status_signal.emit("Query execution failed")
                        self.log_signal.emit(f"Query execution failed: {error_msg}")
                        self.finished_signal.emit(False, error_msg, None)
                        
                except Exception as exec_error:
                    self.log_signal.emit(f"Error in executor: {str(exec_error)}")
                    self.log_signal.emit(traceback.format_exc())
                    self.status_signal.emit(f"Error: {str(exec_error)}")
                    self.finished_signal.emit(False, f"Query execution error: {str(exec_error)}", None)
                
            except ImportError as ie:
                self.log_signal.emit(f"ImportError: {str(ie)}")
                self.log_signal.emit("Could not import sql_executor, falling back to direct execution")
                # Fallback to direct SQL execution
                self.status_signal.emit("Executing query directly...")
                self._execute_query_directly()
            except Exception as exec_error:
                self.log_signal.emit(f"Error during execution: {str(exec_error)}")
                self.status_signal.emit(f"Error: {str(exec_error)}")
                self.finished_signal.emit(False, f"Query execution error: {str(exec_error)}", None)
        
        except Exception as e:
            self.log_signal.emit(f"Unhandled exception in QueryExecutionThread: {str(e)}")
            self.log_signal.emit(traceback.format_exc())
            self.status_signal.emit(f"Error: {str(e)}")
            self.finished_signal.emit(False, f"Query execution error: {str(e)}", None)
        
        finally:
            # Clean up database connections no matter what happened
            try:
                if engine:
                    self.log_signal.emit("Disposing engine and closing all database connections")
                    engine.dispose()
            except Exception as cleanup_error:
                self.log_signal.emit(f"Error during engine cleanup: {str(cleanup_error)}")
    
    def _execute_query_directly(self):
        """Execute SQL query directly when sql_executor is not available."""
        try:
            if not hasattr(self, 'engine') or self.engine is None:
                from sqlalchemy import create_engine
                self.engine = create_engine(
                    self.connection_url,
                    connect_args={
                        "connect_timeout": 10,
                        "options": "-c statement_timeout=120000"
                    },
                    pool_size=5,
                    max_overflow=10,
                    pool_timeout=30,
                    pool_recycle=1800
            )
            
            # Read the query file
            self.log_signal.emit(f"Reading query file: {self.query_file}")
            with open(self.query_file, 'r') as f:
                query = f.read()
            
            self.log_signal.emit(f"Query file read successfully: {len(query)} bytes")
            
            # Apply date filter if specified
            if 'date_filter' in self.options:
                start_date = self.options['date_filter'].get('start_date')
                end_date = self.options['date_filter'].get('end_date')
                
                if start_date and end_date:
                    # Replace date parameters in query
                    self.log_signal.emit(f"Applying date filter: {start_date} to {end_date}")
                    query = query.replace("{{start_date}}", start_date)
                    query = query.replace("{{end_date}}", end_date)
            
            # Execute the query with SQLAlchemy
            with self.engine.connect() as conn:
                # Begin a transaction - will use this for non-DDL statements
                trans = conn.begin()
                
                self.status_signal.emit("Executing query...")
                self.log_signal.emit("Connected to database, starting query execution")
                self.progress_signal.emit(10)
                
                # Split the query into multiple statements if needed
                self.log_signal.emit("Splitting query into statements")
                statements = self._split_sql_statements(query)
                self.log_signal.emit(f"Found {len(statements)} SQL statements")
                
                # Track progress through statements
                total_statements = len(statements)
                for i, stmt in enumerate(statements):
                    # Check if we should stop
                    if self.stop_event.is_set():
                        self.log_signal.emit("Stop event detected - terminating execution")
                        self.status_signal.emit("Query execution terminated by user")
                        trans.rollback()  # Make sure to rollback if stopping
                        self.finished_signal.emit(False, "Query execution terminated by user", None)
                        return
                    
                    # Skip empty statements
                    if not stmt.strip():
                        continue
                    
                    # Update progress
                    progress = int(10 + ((i / total_statements) * 80))
                    self.progress_signal.emit(progress)
                    self.status_signal.emit(f"Executing statement {i+1} of {total_statements}...")
                    self.log_signal.emit(f"Executing statement {i+1}/{total_statements}: {stmt[:100]}...")
                    
                    # Execute the statement
                    try:
                        # Check if this is a DDL statement that requires special handling
                        is_ddl = any(keyword in stmt.upper() for keyword in [
                            "CREATE VIEW", "DROP VIEW", "CREATE TABLE", "DROP TABLE", 
                            "CREATE INDEX", "DROP INDEX", "ALTER TABLE", "DO $$"
                        ])
                        
                        if is_ddl:
                            # Commit any pending transaction before DDL
                            try:
                                trans.commit()
                                self.log_signal.emit("Committed previous transaction before DDL")
                            except Exception:
                                # If there wasn't an active transaction, that's ok
                                self.log_signal.emit("No active transaction to commit before DDL")
                                
                            self.log_signal.emit(f"Statement {i+1} is a DDL operation, using AUTOCOMMIT")
                            # Use a separate connection with autocommit for DDL statements
                            with self.engine.connect() as ddl_conn:
                                # Set autocommit isolation level
                                ddl_conn_with_options = ddl_conn.execution_options(isolation_level="AUTOCOMMIT")
                                result = ddl_conn_with_options.execute(text(stmt))
                                self.log_signal.emit(f"DDL Statement {i+1} executed successfully")
                                
                                # For VIEW operations, verify the view exists after creation
                                if "CREATE VIEW" in stmt.upper() or "CREATE OR REPLACE VIEW" in stmt.upper():
                                    try:
                                        # Extract view name using regex
                                        import re
                                        view_match = re.search(r'CREATE\s+(OR\s+REPLACE\s+)?VIEW\s+([^\s(]+)', stmt, re.IGNORECASE)
                                        if view_match:
                                            view_name = view_match.group(2).strip()
                                            self.log_signal.emit(f"Verifying view {view_name} was created...")
                                            # Check if view exists in catalog
                                            verify_result = ddl_conn_with_options.execute(
                                                text("SELECT EXISTS(SELECT 1 FROM information_schema.views WHERE table_name = :view_name)"),
                                                {"view_name": view_name.lower()}
                                            )
                                            view_exists = verify_result.scalar()
                                            if view_exists:
                                                self.log_signal.emit(f"View {view_name} was created successfully")
                                            else:
                                                self.log_signal.emit(f"Warning: View {view_name} may not have been created properly")
                                    except Exception as verify_error:
                                        self.log_signal.emit(f"Error verifying view: {str(verify_error)}")
                            
                            # Start a new transaction for subsequent non-DDL statements
                            trans = conn.begin()
                        else:
                            # For regular statements, use the existing connection
                            result = conn.execute(text(stmt))
                            self.log_signal.emit(f"Statement {i+1} executed successfully")
                        
                        # If the statement is a SELECT, we might want to capture the result
                        if stmt.strip().upper().startswith('SELECT'):
                            try:
                                # Only fetch a limited number of rows to avoid memory issues
                                self.log_signal.emit("Statement is a SELECT, fetching limited rows")
                                rows = result.fetchmany(10000)  # Limit to 10K rows
                                
                                if rows:
                                    # Get the result as a DataFrame
                                    import pandas as pd
                                    df = pd.DataFrame(rows)
                                    if len(df) > 0:
                                        df.columns = result.keys()
                                        self.log_signal.emit(f"Retrieved {len(df)} rows, {len(df.columns)} columns")
                            except Exception as fetch_error:
                                self.log_signal.emit(f"Error fetching result: {str(fetch_error)}")
                    except Exception as e:
                        # Log the error but continue with other statements
                        error_msg = f"Error executing statement {i+1}: {str(e)}"
                        self.log_signal.emit(error_msg)
                        self.log_signal.emit(f"Statement: {stmt[:500]}...")
                        
                        # Rollback the transaction to reset error state
                        try:
                            trans.rollback()
                            self.log_signal.emit("Rolled back transaction due to error")
                            # Start a new transaction for the next statements
                            trans = conn.begin()
                        except Exception as rollback_error:
                            self.log_signal.emit(f"Error during rollback: {str(rollback_error)}")
                
                # Ensure transaction is committed
                try:
                    trans.commit()
                    self.log_signal.emit("Final transaction committed")
                except Exception as commit_error:
                    self.log_signal.emit(f"Error during final commit: {str(commit_error)}")
                    # Try to rollback if commit fails
                    try:
                        trans.rollback()
                    except:
                        pass
                
                # Get final result for output
                self.status_signal.emit("Fetching query results...")
                self.progress_signal.emit(90)
                self.log_signal.emit("Preparing final results")
                
                # Execute the final SELECT statement to get results
                # Note: We need to extract the last SELECT statement
                select_statements = [stmt for stmt in statements if stmt.strip().upper().startswith('SELECT')]
                if select_statements:
                    final_query = select_statements[-1]
                    self.log_signal.emit(f"Executing final SELECT statement: {final_query[:100]}...")
                    
                    try:
                        # Use a new transaction for the final query
                        with conn.begin():
                            result = conn.execute(text(final_query))
                            
                            # Convert to DataFrame and save to file
                            self.log_signal.emit("Converting result to DataFrame")
                            import pandas as pd
                            
                            # Use a chunked approach to handle potentially large result sets
                            chunk_size = min(self.options.get('chunk_size', 100000), 100000)
                            self.log_signal.emit(f"Using chunk size of {chunk_size} rows")
                            
                            # Initialize an empty DataFrame for results
                            full_df = None
                            row_count = 0
                            
                            # Process in chunks
                            while True:
                                chunk = result.fetchmany(chunk_size)
                                if not chunk:
                                    break
                                    
                                # Convert chunk to DataFrame
                                df_chunk = pd.DataFrame(chunk)
                                if len(df_chunk) > 0:
                                    df_chunk.columns = result.keys()
                                    row_count += len(df_chunk)
                                    
                                    # If this is the first chunk, initialize the full DataFrame
                                    if full_df is None:
                                        full_df = df_chunk
                                    else:
                                        # Otherwise append to the full DataFrame
                                        full_df = pd.concat([full_df, df_chunk], ignore_index=True)
                                    
                                    self.log_signal.emit(f"Processed {row_count} rows so far")
                                    self.status_signal.emit(f"Processing results: {row_count} rows")
                            
                            # Save the final DataFrame to file
                            if full_df is not None and len(full_df) > 0:
                                self.status_signal.emit(f"Saving {len(full_df)} rows to {self.output_file}...")
                                self.log_signal.emit(f"Saving {len(full_df)} rows to {self.output_file}")
                                
                                # Save to appropriate format
                                if self.output_file.endswith('.xlsx'):
                                    full_df.to_excel(self.output_file, index=False)
                                elif self.output_file.endswith('.csv'):
                                    full_df.to_csv(self.output_file, index=False)
                                elif self.output_file.endswith('.parquet'):
                                    full_df.to_parquet(self.output_file, index=False)
                                
                                self.log_signal.emit(f"Results saved successfully to {self.output_file}")
                            else:
                                self.log_signal.emit("Query returned no results")
                    except Exception as result_error:
                        error_msg = f"Error processing results: {str(result_error)}"
                        self.log_signal.emit(error_msg)
                        self.finished_signal.emit(False, error_msg, None)
                        return
                else:
                    self.log_signal.emit("No SELECT statements found for retrieving results")
                
                self.progress_signal.emit(100)
                self.status_signal.emit("Query execution completed successfully")
                self.log_signal.emit("Query execution complete")
                self.finished_signal.emit(True, f"Query executed successfully. Results saved to {self.output_file}", self.output_file)
                
        except Exception as e:
            error_msg = f"Error in _execute_query_directly: {str(e)}"
            self.log_signal.emit(error_msg)
            self.log_signal.emit(traceback.format_exc())
            self.status_signal.emit(f"Error: {str(e)}")
            self.finished_signal.emit(False, error_msg, None)
    
    def _split_sql_statements(self, sql):
        """Split a SQL script into individual statements."""
        # Use sqlparse to split statements properly
        try:
            import sqlparse
            self.log_signal.emit("Using sqlparse to split SQL statements")
            statements = sqlparse.split(sql)
            
            # Special handling for DO blocks and CREATE VIEW statements
            processed_statements = []
            
            # Group DO blocks and CREATE VIEW statements that should be executed together
            i = 0
            while i < len(statements):
                stmt = statements[i].strip()
                
                # Check if this is a DO block or CREATE/DROP VIEW
                if stmt.upper().startswith('DO '):
                    # Add the DO block as a single statement
                    processed_statements.append(stmt)
                elif stmt.upper().startswith('DROP VIEW IF EXISTS PAYMENT_ANALYSIS_VIEW'):
                    # Look ahead to combine the DROP VIEW with CREATE VIEW
                    combined_stmt = stmt
                    j = i + 1
                    while j < len(statements) and statements[j].upper().startswith('CREATE OR REPLACE VIEW'):
                        combined_stmt += "\n" + statements[j]
                        j += 1
                    processed_statements.append(combined_stmt)
                    i = j - 1  # Skip the statements we just combined
                else:
                    processed_statements.append(stmt)
                
                i += 1
            
            self.log_signal.emit(f"Split SQL into {len(processed_statements)} statements")
            return processed_statements
        except ImportError:
            self.log_signal.emit("sqlparse not available, falling back to simple splitting")
            # Fallback to the simple approach if sqlparse is not available
            statements = []
            current_statement = ""
            in_single_quote = False
            in_double_quote = False
            in_do_block = False
            
            for char in sql:
                current_statement += char
                
                # Track if we're inside a DO block
                if not in_single_quote and not in_double_quote:
                    if current_statement.strip().upper().startswith('DO '):
                        in_do_block = True
                    elif in_do_block and '$$;' in current_statement:
                        in_do_block = False
                
                if char == "'":
                    # Toggle single quote state if not escaped
                    if not in_double_quote:
                        in_single_quote = not in_single_quote
                elif char == '"':
                    # Toggle double quote state if not escaped
                    if not in_single_quote:
                        in_double_quote = not in_double_quote
                elif char == ';' and not in_single_quote and not in_double_quote and not in_do_block:
                    # Statement end found outside of quotes and not in DO block
                    statements.append(current_statement)
                    current_statement = ""
            
            # Add the last statement if there's no trailing semicolon
            if current_statement.strip():
                statements.append(current_statement)
            
            self.log_signal.emit(f"Split SQL into {len(statements)} statements (simple method)")
            return statements

class ResultLoadingThread(QThread):
    """Thread to load query results from a file using pandas."""
    data_loaded = pyqtSignal(object, str)  # Emits DataFrame and result_file path
    loading_failed = pyqtSignal(str, str) # Emits error message and result_file path

    def __init__(self, result_file, parent=None):
        super().__init__(parent)
        self.result_file = result_file
        self.preview_row_limit = 1000 # Max rows to load for preview

    def run(self):
        try:
            df = None
            if not self.result_file or not os.path.exists(self.result_file):
                self.loading_failed.emit("Result file not found.", self.result_file)
                return

            if self.result_file.endswith(('.xlsx', '.xls')):
                xl = pd.ExcelFile(self.result_file)
                sheets = xl.sheet_names
                data_sheets = [s for s in sheets if s != 'Summary' and not s.startswith('Error_')]
                sheet_to_load = data_sheets[0] if data_sheets else sheets[0]
                # Load only a limited number of rows for preview for Excel
                df = pd.read_excel(self.result_file, sheet_name=sheet_to_load, nrows=self.preview_row_limit)
            elif self.result_file.endswith('.csv'):
                df = pd.read_csv(self.result_file, nrows=self.preview_row_limit)
            elif self.result_file.endswith('.parquet'):
                # Parquet might not support nrows directly in all engines, read full then slice
                temp_df = pd.read_parquet(self.result_file)
                df = temp_df.head(self.preview_row_limit)
            else:
                self.loading_failed.emit(f"Unsupported file format: {self.result_file}", self.result_file)
                return
            
            self.data_loaded.emit(df, self.result_file)

        except Exception as e:
            error_message = f"Error loading result preview from {self.result_file}: {str(e)}\\n{traceback.format_exc()}"
            self.loading_failed.emit(error_message, self.result_file)


# Entry point of the application
if __name__ == "__main__":
    app = QApplication(sys.argv)
    
    # Apply modern styling from ThemeManager
    ThemeManager.apply_theme(app, "Light")
    
    window = MainWindow()
    window.show()
    sys.exit(app.exec_()) 