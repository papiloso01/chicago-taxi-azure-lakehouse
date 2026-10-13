-- Runs only on the first initialization of a fresh local Docker volume.
CREATE DATABASE IF NOT EXISTS taxi_reporting CHARACTER SET utf8mb4;
CREATE DATABASE IF NOT EXISTS pipeline_control CHARACTER SET utf8mb4;

CREATE TABLE IF NOT EXISTS pipeline_control.pipeline_run (
    run_id VARCHAR(64) PRIMARY KEY,
    pipeline_name VARCHAR(100) NOT NULL,
    source_name VARCHAR(100) NOT NULL,
    start_date DATE NOT NULL,
    end_date_exclusive DATE NOT NULL,
    status ENUM('running', 'succeeded', 'failed') NOT NULL,
    extracted_rows BIGINT UNSIGNED NOT NULL DEFAULT 0,
    accepted_rows BIGINT UNSIGNED NOT NULL DEFAULT 0,
    rejected_rows BIGINT UNSIGNED NOT NULL DEFAULT 0,
    started_at_utc DATETIME(6) NOT NULL,
    completed_at_utc DATETIME(6),
    error_message TEXT,
    CONSTRAINT valid_run_window CHECK (start_date < end_date_exclusive)
);
