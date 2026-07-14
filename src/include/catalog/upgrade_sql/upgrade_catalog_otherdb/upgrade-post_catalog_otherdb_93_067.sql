DROP FUNCTION IF EXISTS pg_catalog.timezone(text, timestamp without time zone) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.timezone(interval, timestamp without time zone) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 2069;
CREATE FUNCTION pg_catalog.timezone(text, timestamp without time zone)
RETURNS timestamp without time zone
AS 'timestamp_zone'
LANGUAGE INTERNAL
IMMUTABLE STRICT COST 1;
COMMENT ON FUNCTION pg_catalog.timezone(text, timestamp without time zone) IS 'adjust timestamp to new time zone';
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 2070;
CREATE FUNCTION pg_catalog.timezone(interval, timestamp without time zone)
RETURNS timestamp without time zone
AS 'timestamp_izone'
LANGUAGE INTERNAL
IMMUTABLE STRICT COST 1;
COMMENT ON FUNCTION pg_catalog.timezone(interval, timestamp without time zone) IS 'adjust timestamp to new time zone';
