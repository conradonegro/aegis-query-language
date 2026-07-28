-- ============================================================================
-- 02_restore_fks.sql — restore FKs declared in the original BIRD SQLite DDL
-- that the official SQLite→PostgreSQL transpile dropped.
--
-- Verified against data/minidev/MINIDEV/dev_databases/*/*.sqlite
-- (PRAGMA foreign_key_list) vs the FK constraints present in
-- 01_BIRD_dev.sql. Every constraint below exists in the canonical BIRD
-- dataset; none is invented. Deliberately NOT restored (absent from the
-- SQLite DDL too): card_games cards.setcode -> sets.code.
--
-- NOT VALID: BIRD data contains orphan rows that fail FK validation
-- (the likely reason the official transpile dropped these). NOT VALID
-- registers the constraint in pg_constraint — which is all metadata
-- discovery reads — without validating existing rows.
--
-- Statements are independent; a failure in one (e.g. a unique-index
-- collision on dirty data) must not block the rest, so this file runs
-- without ON_ERROR_STOP.
--
-- Known expected failure: team_attributes.team_fifa_api_id -> team —
-- BIRD's team table contains duplicate team_fifa_api_id values, so the
-- prerequisite unique index cannot be created. 16 of 17 restore.
-- ============================================================================

-- card_games ----------------------------------------------------------------
ALTER TABLE ONLY foreign_data
    ADD CONSTRAINT foreign_data_uuid_fkey FOREIGN KEY (uuid)
    REFERENCES cards(uuid) NOT VALID;
ALTER TABLE ONLY legalities
    ADD CONSTRAINT legalities_uuid_fkey FOREIGN KEY (uuid)
    REFERENCES cards(uuid) NOT VALID;

-- codebase_community --------------------------------------------------------
ALTER TABLE ONLY comments
    ADD CONSTRAINT comments_postid_fkey FOREIGN KEY (postid)
    REFERENCES posts(id) NOT VALID;
ALTER TABLE ONLY posthistory
    ADD CONSTRAINT posthistory_postid_fkey FOREIGN KEY (postid)
    REFERENCES posts(id) NOT VALID;
ALTER TABLE ONLY postlinks
    ADD CONSTRAINT postlinks_postid_fkey FOREIGN KEY (postid)
    REFERENCES posts(id) NOT VALID;
ALTER TABLE ONLY postlinks
    ADD CONSTRAINT postlinks_relatedpostid_fkey FOREIGN KEY (relatedpostid)
    REFERENCES posts(id) NOT VALID;
ALTER TABLE ONLY posts
    ADD CONSTRAINT posts_parentid_fkey FOREIGN KEY (parentid)
    REFERENCES posts(id) NOT VALID;
ALTER TABLE ONLY tags
    ADD CONSTRAINT tags_excerptpostid_fkey FOREIGN KEY (excerptpostid)
    REFERENCES posts(id) NOT VALID;

-- debit_card_specializing ---------------------------------------------------
ALTER TABLE ONLY yearmonth
    ADD CONSTRAINT yearmonth_customerid_fkey FOREIGN KEY (customerid)
    REFERENCES customers(customerid) NOT VALID;

-- european_football_2 -------------------------------------------------------
ALTER TABLE ONLY league
    ADD CONSTRAINT league_country_id_fkey FOREIGN KEY (country_id)
    REFERENCES country(id) NOT VALID;
ALTER TABLE ONLY match
    ADD CONSTRAINT match_league_id_fkey FOREIGN KEY (league_id)
    REFERENCES league(id) NOT VALID;
ALTER TABLE ONLY match
    ADD CONSTRAINT match_country_id_fkey FOREIGN KEY (country_id)
    REFERENCES country(id) NOT VALID;
-- The fifa-id FKs reference non-key columns; PostgreSQL requires a unique
-- index on the referenced column before the FK can be registered.
CREATE UNIQUE INDEX IF NOT EXISTS player_player_fifa_api_id_key
    ON player(player_fifa_api_id);
ALTER TABLE ONLY player_attributes
    ADD CONSTRAINT player_attributes_player_fifa_api_id_fkey
    FOREIGN KEY (player_fifa_api_id)
    REFERENCES player(player_fifa_api_id) NOT VALID;
CREATE UNIQUE INDEX IF NOT EXISTS team_team_fifa_api_id_key
    ON team(team_fifa_api_id);
ALTER TABLE ONLY team_attributes
    ADD CONSTRAINT team_attributes_team_fifa_api_id_fkey
    FOREIGN KEY (team_fifa_api_id)
    REFERENCES team(team_fifa_api_id) NOT VALID;

-- formula_1 -----------------------------------------------------------------
ALTER TABLE ONLY constructorresults
    ADD CONSTRAINT constructorresults_raceid_fkey FOREIGN KEY (raceid)
    REFERENCES races(raceid) NOT VALID;
ALTER TABLE ONLY constructorstandings
    ADD CONSTRAINT constructorstandings_raceid_fkey FOREIGN KEY (raceid)
    REFERENCES races(raceid) NOT VALID;
ALTER TABLE ONLY driverstandings
    ADD CONSTRAINT driverstandings_raceid_fkey FOREIGN KEY (raceid)
    REFERENCES races(raceid) NOT VALID;
