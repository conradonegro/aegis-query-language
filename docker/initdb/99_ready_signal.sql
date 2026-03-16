-- Sentinel table created last in initdb sequence (alphabetically after 01_BIRD_dev.sql).
-- Its existence proves all preceding initdb scripts have fully committed.
-- Queried by the db healthcheck before migrate/discover/aegis are allowed to start.
CREATE TABLE public._aegis_docker_ready (id INT);
