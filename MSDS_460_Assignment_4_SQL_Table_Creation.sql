CREATE EXTENSION IF NOT EXISTS postgis;

CREATE TABLE IF NOT EXISTS employees (
    employee_id SERIAL PRIMARY KEY NOT NULL,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    age INTEGER,
    rating FLOAT,
    json_contact_info JSON,
    bjson_contact_info JSONB,
    address GEOMETRY(Point)
);
