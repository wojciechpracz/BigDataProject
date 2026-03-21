-- =============================================================
-- 01_setup.sql  –  Create database schemas
-- Idempotent: safe to run multiple times
-- =============================================================

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS cleaned;
CREATE SCHEMA IF NOT EXISTS gold;
