@echo off
REM setup.bat — one-shot environment setup for a fresh clone on Windows.
REM
REM Creates .venv, installs pinned deps. Idempotent — safe to re-run.
REM
REM After running this, activate with:  .venv\Scripts\activate.bat

setlocal enabledelayedexpansion

REM Allow overriding the Python binary, default to "python" on PATH.
if "%PYTHON_BIN%"=="" set PYTHON_BIN=python

REM Sanity-check that Python is on PATH before we do anything.
where %PYTHON_BIN% >nul 2>nul
if errorlevel 1 (
    echo [setup] ERROR: '%PYTHON_BIN%' not found on PATH.
    echo [setup] Install Python 3.11 or 3.12 from python.org, or set PYTHON_BIN.
    exit /b 1
)

if not exist ".venv" (
    echo [setup] Creating virtualenv in .venv ...
    %PYTHON_BIN% -m venv .venv
    if errorlevel 1 (
        echo [setup] ERROR: failed to create .venv
        exit /b 1
    )
) else (
    echo [setup] Reusing existing .venv
)

echo [setup] Upgrading pip ...
.venv\Scripts\python.exe -m pip install --upgrade pip --quiet
if errorlevel 1 (
    echo [setup] ERROR: pip upgrade failed
    exit /b 1
)

echo [setup] Installing dependencies from requirements.txt ...
.venv\Scripts\python.exe -m pip install -r requirements.txt --quiet
if errorlevel 1 (
    echo [setup] ERROR: dependency install failed
    exit /b 1
)

echo [setup] Done.
echo.
echo Activate with:  .venv\Scripts\activate.bat
echo Then run:       python src\daily_metrics.py --no-db
echo Or with DB:     python scripts\migrate.py ^&^& python src\daily_metrics.py

endlocal