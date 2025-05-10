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

:: Try to find Conda Python
echo Checking for Conda installations...
:: Check if conda is in PATH
conda --version >nul 2>&1
if %ERRORLEVEL% EQU 0 (
    :: Get conda python path
    for /f "tokens=*" %%i in ('conda info --base') do set CONDA_BASE=%%i
    if exist "!CONDA_BASE!\python.exe" (
        set PYTHON_CMD="!CONDA_BASE!\python.exe"
        for /f "tokens=2" %%i in ('"!PYTHON_CMD!" --version 2^>^&1') do set PYTHON_VERSION=%%i
        echo Found Python !PYTHON_VERSION! from Conda base environment
        goto :python_found
    )
    
    :: Try current conda environment if active
    if defined CONDA_PREFIX (
        if exist "!CONDA_PREFIX!\python.exe" (
            set PYTHON_CMD="!CONDA_PREFIX!\python.exe"
            for /f "tokens=2" %%i in ('"!PYTHON_CMD!" --version 2^>^&1') do set PYTHON_VERSION=%%i
            echo Found Python !PYTHON_VERSION! from active Conda environment
            goto :python_found
        )
    )
)

:: Check common installation paths
echo Checking common installation paths...
set COMMON_PATHS=^
C:\Python312;^
C:\Python311;^
C:\Python310;^
C:\Python39;^
C:\Python38;^
C:\Python37;^
C:\Program Files\Python312;^
C:\Program Files\Python311;^
C:\Program Files\Python310;^
C:\Program Files\Python39;^
C:\Program Files\Python38;^
C:\Program Files\Python37;^
C:\Program Files (x86)\Python312;^
C:\Program Files (x86)\Python311;^
C:\Program Files (x86)\Python310;^
C:\Program Files (x86)\Python39;^
C:\Program Files (x86)\Python38;^
C:\Program Files (x86)\Python37;^
%LOCALAPPDATA%\Programs\Python\Python312;^
%LOCALAPPDATA%\Programs\Python\Python311;^
%LOCALAPPDATA%\Programs\Python\Python310;^
%LOCALAPPDATA%\Programs\Python\Python39;^
%LOCALAPPDATA%\Programs\Python\Python38;^
%LOCALAPPDATA%\Programs\Python\Python37

for %%p in (%COMMON_PATHS%) do (
    if exist "%%p\python.exe" (
        set PYTHON_CMD="%%p\python.exe"
        for /f "tokens=2" %%i in ('"!PYTHON_CMD!" --version 2^>^&1') do set PYTHON_VERSION=%%i
        echo Found Python !PYTHON_VERSION! at %%p
        goto :python_found
    )
)

:: Check for Anaconda/Miniconda in common locations
set CONDA_PATHS=^
C:\ProgramData\Anaconda3;^
C:\ProgramData\Miniconda3;^
%USERPROFILE%\Anaconda3;^
%USERPROFILE%\Miniconda3;^
%LOCALAPPDATA%\Continuum\anaconda3;^
%LOCALAPPDATA%\Continuum\miniconda3

for %%p in (%CONDA_PATHS%) do (
    if exist "%%p\python.exe" (
        set PYTHON_CMD="%%p\python.exe"
        for /f "tokens=2" %%i in ('"!PYTHON_CMD!" --version 2^>^&1') do set PYTHON_VERSION=%%i
        echo Found Python !PYTHON_VERSION! from Conda at %%p
        goto :python_found
    )
)

:: Search PATH for python.exe
echo Searching PATH for Python installations...
for %%p in (python.exe) do (
    set FOUND_PATH=%%~$PATH:p
    if defined FOUND_PATH (
        for /f "delims=" %%i in ("!FOUND_PATH!") do set PYTHON_PATH=%%~dpi
        set PYTHON_CMD="!FOUND_PATH!"
        for /f "tokens=2" %%i in ('"!PYTHON_CMD!" --version 2^>^&1') do set PYTHON_VERSION=%%i
        echo Found Python !PYTHON_VERSION! in PATH at !FOUND_PATH!
        goto :python_found
    )
)

:: If we reach here, Python was not found
echo ERROR: Python not found. Please install Python 3.7 or newer.
echo You can download Python from https://www.python.org/downloads/
echo or install Anaconda/Miniconda from https://www.anaconda.com/products/distribution
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