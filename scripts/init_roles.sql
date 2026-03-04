-- 1. Create Roles
CREATE ROLE role_aegis_runtime;
CREATE ROLE role_aegis_registry_runtime;
CREATE ROLE role_aegis_steward;
CREATE ROLE role_aegis_registry_admin;
CREATE ROLE role_aegis_data_owner;
CREATE ROLE role_aegis_meta_owner;

-- 2. Create Users (1 user per role)
CREATE USER user_aegis_runtime WITH PASSWORD 'runtime_pass';
CREATE USER user_aegis_registry_runtime WITH PASSWORD 'registry_pass';
CREATE USER user_aegis_steward WITH PASSWORD 'steward_pass';
CREATE USER user_aegis_registry_admin WITH PASSWORD 'admin_pass';
CREATE USER user_aegis_data_owner WITH PASSWORD 'data_owner_pass';
CREATE USER user_aegis_meta_owner WITH PASSWORD 'meta_owner_pass';

-- 3. Assign Roles to Users
GRANT role_aegis_runtime TO user_aegis_runtime;
GRANT role_aegis_registry_runtime TO user_aegis_registry_runtime;
GRANT role_aegis_steward TO user_aegis_steward;
GRANT role_aegis_registry_admin TO user_aegis_registry_admin;
GRANT role_aegis_data_owner TO user_aegis_data_owner;
GRANT role_aegis_meta_owner TO user_aegis_meta_owner;

-- 4. Revoke default public schema access to ensure clean slate
REVOKE ALL ON SCHEMA public FROM PUBLIC;
REVOKE ALL ON SCHEMA aegis_meta FROM PUBLIC;

--------------------------------------------------------------------------------
-- 1. aegis_runtime (FastAPI / Aegis proxy during request execution)
--------------------------------------------------------------------------------
GRANT USAGE ON SCHEMA public TO role_aegis_runtime;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO role_aegis_runtime;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO role_aegis_runtime;

GRANT USAGE ON SCHEMA aegis_meta TO role_aegis_runtime;
GRANT SELECT ON aegis_meta.compiled_registry_artifacts TO role_aegis_runtime;
GRANT SELECT, INSERT, UPDATE ON aegis_meta.chat_sessions TO role_aegis_runtime;
GRANT SELECT, INSERT ON aegis_meta.chat_messages TO role_aegis_runtime;
-- Explicit denys are handled inherently by PostgreSQL default deny posture.

--------------------------------------------------------------------------------
-- 2. aegis_registry_runtime (Aegis compiler loader at startup / reload)
--------------------------------------------------------------------------------
GRANT USAGE ON SCHEMA aegis_meta TO role_aegis_registry_runtime;
GRANT SELECT ON aegis_meta.compiled_registry_artifacts TO role_aegis_registry_runtime;
GRANT SELECT ON aegis_meta.metadata_versions TO role_aegis_registry_runtime;

--------------------------------------------------------------------------------
-- 3. aegis_steward (Steward UI / metadata editors)
--------------------------------------------------------------------------------
GRANT USAGE ON SCHEMA aegis_meta TO role_aegis_steward;
GRANT SELECT ON ALL TABLES IN SCHEMA aegis_meta TO role_aegis_steward;
ALTER DEFAULT PRIVILEGES IN SCHEMA aegis_meta GRANT SELECT ON TABLES TO role_aegis_steward;

GRANT INSERT, UPDATE ON aegis_meta.metadata_tables TO role_aegis_steward;
GRANT INSERT, UPDATE ON aegis_meta.metadata_columns TO role_aegis_steward;
GRANT INSERT, UPDATE ON aegis_meta.metadata_relationships TO role_aegis_steward;
GRANT INSERT ON aegis_meta.metadata_versions TO role_aegis_steward;
GRANT INSERT ON aegis_meta.metadata_audit TO role_aegis_steward;
-- Cannot activate versions: omitted UPDATE on metadata_versions
-- WORM compliance: omitted UPDATE/DELETE on metadata_audit

--------------------------------------------------------------------------------
-- 4. aegis_registry_admin (Controlled deployment pipeline / senior operator)
--------------------------------------------------------------------------------
GRANT USAGE ON SCHEMA aegis_meta TO role_aegis_registry_admin;
GRANT SELECT ON ALL TABLES IN SCHEMA aegis_meta TO role_aegis_registry_admin;
GRANT UPDATE (status, registry_hash, approved_by, approved_at) ON aegis_meta.metadata_versions TO role_aegis_registry_admin;
GRANT INSERT ON aegis_meta.compiled_registry_artifacts TO role_aegis_registry_admin;
GRANT INSERT ON aegis_meta.metadata_audit TO role_aegis_registry_admin;
-- No access to business data (schema public omitted)

--------------------------------------------------------------------------------
-- 5. aegis_data_owner (DBA / migration jobs for business schema)
--------------------------------------------------------------------------------
GRANT USAGE, CREATE ON SCHEMA public TO role_aegis_data_owner;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO role_aegis_data_owner;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL PRIVILEGES ON TABLES TO role_aegis_data_owner;
-- No access to metadata schema omitted

--------------------------------------------------------------------------------
-- 6. aegis_meta_owner (Migration tooling - Alembic)
--------------------------------------------------------------------------------
GRANT USAGE, CREATE ON SCHEMA aegis_meta TO role_aegis_meta_owner;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA aegis_meta TO role_aegis_meta_owner;
ALTER DEFAULT PRIVILEGES IN SCHEMA aegis_meta GRANT ALL PRIVILEGES ON TABLES TO role_aegis_meta_owner;
-- Full control over metadata schema
