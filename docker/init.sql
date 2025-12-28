-- Create roles
CREATE ROLE app WITH LOGIN PASSWORD 'app_password';
CREATE ROLE admin WITH LOGIN PASSWORD 'admin_password';
CREATE ROLE developer WITH LOGIN PASSWORD 'developer_password';

-- Create databases
CREATE DATABASE uno;
CREATE DATABASE dos;

-- Grant connect privileges
GRANT CONNECT ON DATABASE uno TO app, admin, developer;
GRANT CONNECT ON DATABASE dos TO app, admin, developer;

-- Set up uno database
\connect uno

CREATE SCHEMA schema1;
CREATE SCHEMA schema2;

-- Grant CREATE permission for benchmark table creation
GRANT CREATE ON SCHEMA schema1 TO admin;
GRANT CREATE ON SCHEMA schema2 TO admin;

-- Grant app CREATE on schema1 for pgbench table creation
-- pgbench will use schema1 via search_path in connection string
GRANT CREATE ON SCHEMA schema1 TO app;
ALTER DEFAULT PRIVILEGES IN SCHEMA schema1 GRANT ALL ON TABLES TO app;
ALTER DEFAULT PRIVILEGES IN SCHEMA schema1 GRANT ALL ON SEQUENCES TO app;

CREATE TABLE schema1.example (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    value INTEGER,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE schema2.example (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    value INTEGER,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Grant schema usage and table privileges for uno
GRANT USAGE ON SCHEMA schema1, schema2 TO app, admin, developer;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA schema1 TO app;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA schema2 TO app;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA schema1 TO admin, developer;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA schema2 TO admin, developer;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA schema1 TO app, admin, developer;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA schema2 TO app, admin, developer;

-- Insert sample data
INSERT INTO schema1.example (name, value) VALUES ('alpha-uno-s1', 1), ('beta-uno-s1', 2);
INSERT INTO schema2.example (name, value) VALUES ('alpha-uno-s2', 10), ('beta-uno-s2', 20);

-- Set up dos database
\connect dos

CREATE SCHEMA schema1;
CREATE SCHEMA schema2;

-- Grant CREATE permission for benchmark table creation
GRANT CREATE ON SCHEMA schema1 TO admin;
GRANT CREATE ON SCHEMA schema2 TO admin;

CREATE TABLE schema1.example (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    value INTEGER,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE schema2.example (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    value INTEGER,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Grant schema usage and table privileges for dos
GRANT USAGE ON SCHEMA schema1, schema2 TO app, admin, developer;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA schema1 TO app;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA schema2 TO app;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA schema1 TO admin, developer;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA schema2 TO admin, developer;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA schema1 TO app, admin, developer;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA schema2 TO app, admin, developer;

-- Insert sample data
INSERT INTO schema1.example (name, value) VALUES ('alpha-dos-s1', 100), ('beta-dos-s1', 200);
INSERT INTO schema2.example (name, value) VALUES ('alpha-dos-s2', 1000), ('beta-dos-s2', 2000);
