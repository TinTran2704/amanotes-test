@echo off
REM prepare.bat — activate venv + load .env + init DB + run migrations.
REM
REM Intended flow:
REM   First time:  setup.bat  →  prepare.bat
REM   After that:  prepare.bat  (idempotent, safe to re-run)
REM
REM Note: intentionally does NOT use setlocal/endlocal so that env vars
REM loaded from .env persist in the calling terminal after this script exits.

REM ── 1. Guard: venv must exist ─────────────────────────────────────────────
if not exist ".venv\Scripts\activate.bat" (
    echo [prepare] .venv not found. Run setup.bat first.
    exit /b 1
)

REM ── 2. Activate venv ──────────────────────────────────────────────────────
call .venv\Scripts\activate.bat
echo [prepare] venv activated.

REM ── 3. Load .env ──────────────────────────────────────────────────────────
if not exist ".env" (
    echo [prepare] No .env found — skipping. Copy .env.example to .env if needed.
    goto :skip_db
)

echo [prepare] Loading .env ...
for /f "usebackq eol=# tokens=1,* delims==" %%A in (".env") do (
    if not "%%A"=="" (
        set "%%A=%%B"
        echo [prepare]   SET %%A
    )
)

REM ── 4. Init database (idempotent) ─────────────────────────────────────────
REM psql must be on PATH — comes with any Postgres installation.
where psql >nul 2>nul
if errorlevel 1 (
    echo [prepare] WARNING: psql not found on PATH — skipping DB init.
    echo [prepare] Either add Postgres bin dir to PATH, or create the DB manually:
    echo [prepare]   psql -U %PGUSER% -c "CREATE DATABASE %PGDATABASE%;"
    goto :skip_db
)

echo [prepare] Ensuring database '%PGDATABASE%' exists ...
psql -h %PGHOST% -p %PGPORT% -U %PGUSER% -d postgres -f db\init.sql
if errorlevel 1 (
    echo [prepare] ERROR: could not run db\init.sql. Check PGHOST/PGUSER/PGPASSWORD in .env.
    exit /b 1
)

REM ── 5. Run migrations ─────────────────────────────────────────────────────
echo [prepare] Running migrations ...
python scripts\migrate.py
if errorlevel 1 (
    echo [prepare] ERROR: migrations failed. Check output above.
    exit /b 1
)

:skip_db

REM ── 6. Done ───────────────────────────────────────────────────────────────
echo.
echo [prepare] Ready.
python --version
echo.
echo Next steps:
echo   python src\daily_metrics.py --no-db        (CSV only)
echo   python src\daily_metrics.py                (CSV + metadata DB)