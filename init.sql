CREATE TABLE IF NOT EXISTS orders (
    city_id INT,
    order_id VARCHAR(50),
    tender_id VARCHAR(50),
    user_id VARCHAR(50),
    driver_id VARCHAR(50),
    offset_hours INT,
    status_order VARCHAR(20),
    status_tender VARCHAR(20),
    order_timestamp TIMESTAMP,
    tender_timestamp TIMESTAMP,
    driveraccept_timestamp TIMESTAMP,
    driverarrived_timestamp TIMESTAMP,
    driverstarttheride_timestamp TIMESTAMP,
    driverdone_timestamp TIMESTAMP,
    clientcancel_timestamp TIMESTAMP,
    drivercancel_timestamp TIMESTAMP,
    order_modified_local TIMESTAMP,
    cancel_before_accept_local TIMESTAMP,
    distance_in_meters FLOAT,
    duration_in_seconds FLOAT,
    price_order_local FLOAT,
    price_tender_local FLOAT,
    price_start_local FLOAT
);

-- Readonly user for LLM querying
CREATE USER readonly_user WITH PASSWORD 'readonly_password';
GRANT CONNECT ON DATABASE drivee_nl2sql TO readonly_user;
GRANT USAGE ON SCHEMA public TO readonly_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO readonly_user;

-- Allow readonly_user to read future tables as well
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO readonly_user;

-- Secure Postgres instance: Setup statement timeout against heavy hallucinated queries (15 seconds)
ALTER ROLE readonly_user SET statement_timeout = '15000';
