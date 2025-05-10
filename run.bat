@echo off
setlocal enabledelayedexpansion

echo ====================================================
echo ETL Pipeline Manager - Startup Script
echo ====================================================

:: Check for force reinstall parameter
set FORCE_REINSTALL=0
if "%1"=="--reinstall" (
    set FORCE_REINSTALL=1
    echo Force reinstall requirements requested.
)

:: Set directory constants
set SRC_DIR=src
set VENV_DIR=venv

:: Check if src directory exists
if not exist %SRC_DIR% (
    echo ERROR: Source directory '%SRC_DIR%' not found.
    echo Please make sure all application files are in the '%SRC_DIR%' folder.
    pause
    exit /b 1
)

:: Check if requirements.txt exists
if not exist %SRC_DIR%\requirements.txt (
    echo ERROR: Requirements file '%SRC_DIR%\requirements.txt' not found.
    echo Please make sure all application files are properly installed.
    pause
    exit /b 1
)

:: Check if Python is installed using multiple methods
echo Checking Python installation...

set PYTHON_CMD=
set PYTHON_VERSION=

:: Try python command
python --version >nul 2>&1
if %ERRORLEVEL% EQU 0 (
    set PYTHON_CMD=python
    for /f "tokens=2" %%i in ('python --version 2^>^&1') do set PYTHON_VERSION=%%i
    echo Found Python !PYTHON_VERSION! using 'python' command
    goto :python_found
)

:: Try py command (Windows Python launcher)
py --version >nul 2>&1
if %ERRORLEVEL% EQU 0 (
    set PYTHON_CMD=py
    for /f "tokens=2" %%i in ('py --version 2^>^&1') do set PYTHON_VERSION=%%i
    echo Found Python !PYTHON_VERSION! using 'py' command
    goto :python_found
)

:: Try specific Python versions via launcher
for %%v in (3.12 3.11 3.10 3.9 3.8 3.7) do (
    py -%%v --version >nul 2>&1
    if !ERRORLEVEL! EQU 0 (
        set PYTHON_CMD=py -%%v
        for /f "tokens=2" %%i in ('py -%%v --version 2^>^&1') do set PYTHON_VERSION=%%i
        echo Found Python !PYTHON_VERSION! using 'py -%%v' command
        goto :python_found
    )
)

:: If we reach here, Python was not found
echo ERROR: Python not found. Please install Python 3.7 or newer.
echo You can download Python from https://www.python.org/downloads/
pause
exit /b 1

:python_found

:: Parse version to check if it's 3.7+
for /f "tokens=1,2 delims=." %%a in ("!PYTHON_VERSION!") do (
    set MAJOR_VERSION=%%a
    set MINOR_VERSION=%%b
)

if !MAJOR_VERSION! LSS 3 (
    echo ERROR: Python 3.7+ is required, but found Python !PYTHON_VERSION!
    echo Please install Python 3.7 or newer.
    pause
    exit /b 1
)

if !MAJOR_VERSION! EQU 3 if !MINOR_VERSION! LSS 7 (
    echo ERROR: Python 3.7+ is required, but found Python !PYTHON_VERSION!
    echo Please install Python 3.7 or newer.
    pause
    exit /b 1
)

echo Python check passed: Using Python !PYTHON_VERSION!

:: Check if virtual environment exists
if exist %VENV_DIR%\Scripts\python.exe (
    echo Virtual environment already exists.
    
    :: Check if force reinstall was requested
    if !FORCE_REINSTALL! EQU 1 (
        echo Force reinstall requested. Will reinstall all requirements.
        set INSTALL_REQUIREMENTS=1
    )
) else (
    echo Creating virtual environment...
    %PYTHON_CMD% -m venv %VENV_DIR%
    if !ERRORLEVEL! NEQ 0 (
        echo ERROR: Failed to create virtual environment.
        pause
        exit /b 1
    )
    echo Virtual environment created successfully.
    set INSTALL_REQUIREMENTS=1
)

:: Activate the virtual environment
echo Activating virtual environment...
call %VENV_DIR%\Scripts\activate.bat
if %ERRORLEVEL% NEQ 0 (
    echo ERROR: Failed to activate virtual environment.
    pause
    exit /b 1
)

:: Install requirements if needed
if defined INSTALL_REQUIREMENTS (
    echo Installing requirements...
    pip install -r %SRC_DIR%/requirements.txt
    if !ERRORLEVEL! NEQ 0 (
        echo ERROR: Failed to install requirements.
        echo Make sure %SRC_DIR%/requirements.txt exists.
        pause
        exit /b 1
    )
    echo Requirements installed successfully.
)

:: Set working directory to src
cd %SRC_DIR%

:: Run the application
echo Starting ETL Pipeline Manager...
python etl_gui.py
set APP_EXIT_CODE=%ERRORLEVEL%

:: Check if application failed due to missing module
if !APP_EXIT_CODE! NEQ 0 (
    type etl_gui_error.log 2>nul | findstr /i "ModuleNotFoundError: No module named" > nul
    if !ERRORLEVEL! EQU 0 (
        echo.
        echo Application failed due to missing module. Attempting to reinstall requirements...
        cd ..
        pip install -r %SRC_DIR%/requirements.txt
        if !ERRORLEVEL! EQU 0 (
            echo Requirements reinstalled successfully. Restarting application...
            cd %SRC_DIR%
            python etl_gui.py
            set APP_EXIT_CODE=!ERRORLEVEL!
        ) else (
            echo Failed to reinstall requirements.
        )
    ) else (
        echo Application exited with error code !APP_EXIT_CODE!
    )
)

:: Display hint for reinstalling requirements if the app still failed
if !APP_EXIT_CODE! NEQ 0 (
    echo.
    echo If you're experiencing module import errors, try running:
    echo   %~nx0 --reinstall
    echo to force reinstall all requirements.
    pause
)

:: Return to original directory and deactivate virtual environment
cd ..
call %VENV_DIR%\Scripts\deactivate.bat

exit /b !APP_EXIT_CODE! 