# ETL Pipeline Manager for PostgreSQL

## 1. Overview

The ETL Pipeline Manager is a Python-based application designed to streamline the process of executing and managing SQL queries against a PostgreSQL database. It provides a user-friendly graphical interface (GUI) for interacting with the database, managing SQL script files, configuring execution parameters, and handling query outputs. This tool is particularly useful for data analysts, engineers, and anyone needing to run complex SQL tasks, manage payment data, or perform transaction analysis.

The application supports direct SQL execution, batch processing, robust error handling, and flexible output options, including CSV, Excel, and Parquet formats.

## 2. Key Features

*   **Graphical User Interface (GUI)**: Built with PyQt5 for intuitive management of:
    *   Database connections (configurable via `.env` file or GUI dialog).
    *   SQL query files (loading, browsing).
    *   Output directory selection and management (defaults to `[PROJECT_ROOT]/output/`).
    *   Query execution parameters (batch size, chunk size, timeouts, workers).
*   **SQL Query Execution**:
    *   Execute individual SQL files or ad-hoc queries.
    *   Support for multi-statement SQL scripts.
    *   Handles DDL (Data Definition Language) statements (e.g., `CREATE VIEW`, `CREATE TABLE`) with appropriate transaction control (AUTOCOMMIT).
    *   Manages transactions effectively for DML (Data Manipulation Language) statements, including `ROLLBACK` on error to prevent `InFailedSqlTransaction` issues.
*   **Query Output Management**:
    *   Save query results to various formats: CSV, Excel (.xlsx), Parquet.
    *   Option for native PostgreSQL CSV export (`COPY TO STDOUT`).
    *   Dynamic filename generation for results, often including timestamps.
    *   Preview query results directly within the GUI.
    *   "View Results" button intelligently opens the most recently generated output file.
*   **Performance and Robustness**:
    *   Query execution with configurable timeouts to prevent indefinite hanging.
    *   Attempt to cancel timed-out queries on the PostgreSQL server.
    *   Detailed logging of operations and errors in the GUI.
    *   Connection pooling and recycling for efficient database interaction.
*   **Query Generation (Optional)**:
    *   Includes a `query_generator.py` module for dynamically creating complex payment analysis SQL queries based on user-selected fields and options.
*   **Easy Startup**:
    *   `run.bat` script for Windows users, which intelligently searches for Python installations (including standard PATH, Py launcher, Conda environments, and common installation directories), sets up a virtual environment, installs dependencies, and launches the GUI.

## 3. Project Structure

```
etl-pgsql/
├── run.bat                     # Main startup script for Windows
├── output/                     # Default directory for query results (created automatically)
├── src/                        # Source code directory
│   ├── etl_gui.py              # Main application: GUI, logic, and orchestration
│   ├── sql_executor.py         # Core SQL execution engine, transaction management, timeout handling
│   ├── query_generator.py      # Module for dynamic SQL query generation (payment analysis)
│   ├── db_connection.py        # (Logic likely integrated within etl_gui.py or sql_executor.py for DB connections)
│   ├── requirements.txt        # Python package dependencies
│   ├── paths.json              # Configuration file (though some settings like output_dir are now overridden)
│   ├── payment_analysis.sql    # Example/default SQL analysis script
│   ├── generate_config.py      # Utility script (part of a broader ETL process)
│   ├── data_cleaner.py         # Utility script (part of a broader ETL process)
│   ├── load_to_postgres.py     # Utility script (part of a broader ETL process)
│   ├── run.py                  # A script for running a pipeline (distinct from run.bat)
│   ├── icons/                  # Directory for UI icons
│   └── .env                    # Environment file for database credentials (create this file)
└── venv/                       # Virtual environment directory (created by run.bat)
```

## 4. Setup and Installation

### Prerequisites

*   **Python**: Version 3.7 or newer.
*   **PostgreSQL**: A running PostgreSQL server instance.
*   **Conda (Optional)**: If you use Conda for Python environment management, `run.bat` can detect and use Conda environments.

### Installation Steps

1.  **Clone the Repository (if applicable)**:
    ```bash
    git clone <repository_url>
    cd etl-pgsql
    ```
2.  **Create `.env` File for Database Credentials**:
    In the `src/` directory, create a file named `.env` with your PostgreSQL connection details:
    ```env
    DB_HOST=your_db_host
    DB_PORT=your_db_port (e.g., 5432)
    DB_NAME=your_db_name
    DB_USER=your_db_user
    DB_PASSWORD=your_db_password
    ```
    *   The GUI also provides a dialog to configure and save these settings to the `.env` file.

3.  **Install Dependencies**:
    *   **Using `run.bat` (Recommended for Windows)**:
        Simply double-click `run.bat`. It will:
        1.  Detect your Python installation.
        2.  Create a virtual environment in a folder named `venv`.
        3.  Activate the virtual environment.
        4.  Install all required packages from `src/requirements.txt`.
        5.  Launch the ETL Pipeline Manager GUI.
        *   You can force a re-installation of requirements by running: `run.bat --reinstall`
    *   **Manual Installation (for other OS or advanced users)**:
        ```bash
        python -m venv venv
        source venv/bin/activate  # On Linux/macOS
        # venv\\Scripts\\activate    # On Windows
        pip install -r src/requirements.txt
        ```

### Dependencies

Key dependencies are listed in `src/requirements.txt` and include:
*   `PyQt5`: For the graphical user interface.
*   `SQLAlchemy`: For database interaction and ORM capabilities.
*   `psycopg2-binary`: PostgreSQL adapter for Python.
*   `pandas`: For data manipulation and handling query results.
*   `python-dotenv`: For managing environment variables (database credentials).
*   `sqlparse`: For parsing SQL scripts.
*   `openpyxl`: For Excel file support.

## 5. How to Run

*   **Windows**: Double-click `run.bat`.
*   **Other OS / Manual**:
    1.  Activate the virtual environment (e.g., `source venv/bin/activate`).
    2.  Navigate to the `src` directory: `cd src`
    3.  Run the GUI: `python etl_gui.py`

## 6. Usage Guide

The ETL Pipeline Manager GUI is organized into several tabs:

### Sources Tab
*   **Database Connection**:
    *   Displays the current connection status to your PostgreSQL database.
    *   "Check Connection" button: Attempts to connect/reconnect using credentials from `.env`.
    *   "Configure Database" button: Opens a dialog to input and save database credentials (host, port, database name, user, password) to the `src/.env` file.
*   **(Other source management features may exist here for a broader ETL pipeline)**

### Output Tab
This is the primary tab for executing SQL queries.
*   **Output Configuration**:
    *   **Output Directory**: Shows the current directory where results will be saved. Defaults to `[PROJECT_ROOT]/output/`. Click "Browse..." to change it for the current session.
    *   **SQL Query File**: Select the `.sql` file you want to execute. Click "Browse..." to choose a file.
    *   **Output Format**: Choose the format for saving query results (CSV, CSV Native PostgreSQL, Excel, Parquet).
*   **Query Execution Options**:
    *   **Batch Size, Chunk Size, Max Workers, Timeout**: Configure parameters for query execution, affecting performance and resource usage.
*   **Actions**:
    *   **Execute Query**: Runs the selected SQL query file with the specified options.
    *   **Stop**: Attempts to stop an ongoing query execution.
    *   **View Results**: Opens the most recently generated result file from the output directory using the system's default application.
*   **Query Progress**:
    *   Displays a progress bar, status messages, and elapsed time for the current query execution.
*   **Output Preview**:
    *   Shows a table preview of the query results (a subset of rows).
    *   "Refresh Preview": Reloads the preview, ensuring it shows data from the most recent result file.
    *   "Export Preview": Saves the currently displayed preview data.
*   **Log Output**:
    *   A detailed log of all operations, messages, and errors encountered during query execution.

### Query Generator Tab (Optional)
*   Provides a UI to dynamically build payment analysis SQL queries.
*   Select fields from different data sources.
*   Configure options like bank reconciliation.
*   "Generate Query": Creates the SQL query based on selections.
*   "View Generated Query", "Save Query", "Execute Generated Query".

## 7. Configuration

*   **Database Connection**: Primarily configured via the `src/.env` file or the "Configure Database" dialog in the GUI.
    ```env
    DB_HOST=your_host
    DB_PORT=5432
    DB_NAME=your_database
    DB_USER=your_username
    DB_PASSWORD=your_password
    ```
*   **Output Directory**:
    *   The application defaults to creating and using an `output/` folder in the project's root directory.
    *   This can be changed temporarily via the "Browse..." button in the "Output" tab of the GUI.
*   **`paths.json`**:
    *   Located in `src/paths.json`.
    *   Historically used for various path configurations, including `output_dir`.
    *   While `output_dir` from this file is now overridden by the `[PROJECT_ROOT]/output/` default, other configurations within it might still be used by different parts of the broader ETL system if applicable (e.g., source file paths for `data_cleaner.py`).

## 8. Core Components in Depth

### `src/etl_gui.py`
*   **Role**: The central nervous system of the application. It builds and manages the entire PyQt5 GUI, handles user interactions, and orchestrates the calls to backend modules like `sql_executor.py`.
*   **Key Responsibilities**:
    *   Initializing and managing UI elements (tabs, buttons, input fields, tables, log views).
    *   Handling database connection logic via the `DatabaseConnection` class (reading `.env`, testing connections, updating UI with status).
    *   Managing output directory settings.
    *   Launching query execution in a separate thread (`QueryExecutionThread`) to keep the GUI responsive.
    *   Processing signals from the execution thread for progress updates, logs, and completion status.
    *   Dynamically finding and loading the most recent result files for the "View Results" and "Refresh Preview" features.
    *   Interfacing with `query_generator.py` if the Query Generator tab is used.

### `src/sql_executor.py`
*   **Role**: Provides the core functionality for executing SQL queries robustly against PostgreSQL. It is designed to be callable both from the GUI (`QueryExecutionThread`) and potentially as a standalone script.
*   **Key Responsibilities**:
    *   **Database Engine Creation**: Sets up SQLAlchemy engine with optimized settings (connection pooling, statement timeouts, application name).
    *   **SQL Parsing**: Uses `sqlparse` to split multi-statement SQL scripts into individual, executable statements.
    *   **Statement Execution**:
        *   Executes statements one by one.
        *   **Transaction Management**:
            *   Uses `AUTOCOMMIT` for DDL statements (`CREATE VIEW`, `CREATE TABLE`, etc.) to ensure they are executed outside of explicit transaction blocks where required.
            *   For DML and `SELECT` statements, it attempts to manage transactions, including `ROLLBACK` on error. This was a key fix to prevent `psycopg2.errors.InFailedSqlTransaction` (current transaction is aborted, commands ignored until end of transaction block) by ensuring that if one statement in a script fails, the transaction is rolled back before attempting subsequent statements.
        *   **Timeout Control**: Implements query execution with a configurable timeout (`execute_query_with_timeout`). If a query exceeds its timeout, it attempts to cancel the query on the PostgreSQL server using `pg_cancel_backend` and `pg_terminate_backend`.
        *   **Error Handling**: Catches exceptions during query execution, logs them, and reports them back.
    *   **Result Handling**:
        *   Fetches results from `SELECT` statements, often using `pandas` to convert them into DataFrames.
        *   Supports chunked fetching for large result sets to manage memory.
        *   Can write results directly to specified output files (CSV, Excel, Parquet).
        *   Supports native PostgreSQL `COPY TO STDOUT WITH CSV HEADER` for efficient CSV export.
    *   **Progress Reporting**: Includes a mechanism for a progress callback, used by the GUI to update the user.

### `run.bat` (Windows Startup Script)
*   **Role**: Simplifies the startup process for Windows users.
*   **Key Functionality**:
    *   **Python Detection**:
        1.  Checks for `python` and `py` (Python launcher) commands in PATH.
        2.  Iterates through specific Python versions using `py -3.12`, `py -3.11`, etc.
        3.  Checks for Conda installations (base and active environments).
        4.  Scans a list of common Python installation directories (e.g., `C:\PythonXX`, `%LOCALAPPDATA%\Programs\Python`).
        5.  Searches the system `PATH` environment variable.
    *   **Version Check**: Ensures the detected Python is 3.7 or newer.
    *   **Virtual Environment**:
        *   Creates a virtual environment named `venv` if it doesn't exist.
        *   Activates the `venv`.
    *   **Dependency Installation**:
        *   Installs/updates packages from `src/requirements.txt` into the `venv`.
        *   Supports a `--reinstall` flag to force reinstallation.
    *   **Application Launch**: Navigates to the `src` directory and runs `python etl_gui.py`.
    *   **Error Handling**: Provides informative messages if Python is not found or if steps fail. Attempts to reinstall requirements if the app fails due to `ModuleNotFoundError`.

## 9. Troubleshooting

*   **`psycopg2.errors.InFailedSqlTransaction`**: This error ("current transaction is aborted, commands ignored until end of transaction block") typically occurs if a prior statement in a transaction failed, and the transaction was not rolled back. Recent changes in `sql_executor.py` and `etl_gui.py` aim to handle this by rolling back transactions upon statement failure. Ensure you are using the latest version of the code.
*   **Python Not Found (`run.bat`)**: If `run.bat` cannot find Python, ensure Python 3.7+ is installed and accessible. Consider adding Python to your system PATH or installing it in a standard location.
*   **Database Connection Issues**:
    *   Verify credentials in `src/.env` are correct.
    *   Ensure the PostgreSQL server is running and accessible from your machine.
    *   Check firewall settings.
    *   Use the "Configure Database" and "Test Connection" features in the GUI.
*   **ModuleNotFoundError**: If `run.bat` is used, it should handle dependency installation. If running manually, ensure the virtual environment is activated and `pip install -r src/requirements.txt` has been successfully executed. `run.bat` also includes a feature to try reinstalling requirements if this error is detected on app startup.

## 10. License

(Specify your project's license here, e.g., MIT License)

---

This README provides a comprehensive guide to the ETL Pipeline Manager. For specific code-level details, refer to the source files and their inline comments. 