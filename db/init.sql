-- db/init.sql
-- Creates the application database and user if they don't exist yet.
-- Run once before migrate.py. Safe to re-run (all statements are idempotent).
--
-- Connect as a superuser (postgres) to run this:
--   psql -U postgres -f db\init.sql

-- Create database only if it doesn't exist.
-- Postgres has no "CREATE DATABASE IF NOT EXISTS", so we check pg_database first.
SELECT 'CREATE DATABASE amanotes'
WHERE NOT EXISTS (
    SELECT FROM pg_database WHERE datname = 'amanotes'
)\gexec