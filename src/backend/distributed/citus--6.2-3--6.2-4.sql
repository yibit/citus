/* citus--6.2-3--6.2-4.sql */

CREATE FUNCTION pg_catalog.qualified_shard_name(object_name regclass, shard_id bigint)
    RETURNS text
    LANGUAGE C STABLE STRICT
    AS 'MODULE_PATHNAME', $$qualified_shard_name$$;
COMMENT ON FUNCTION pg_catalog.qualified_shard_name(object_name regclass, shard_id bigint)
    IS 'returns schema-qualified, shard-extended identifier of object name';

/* redefine shard_name as STRICT */
CREATE OR REPLACE FUNCTION pg_catalog.shard_name(object_name regclass, shard_id bigint)
    RETURNS text
    LANGUAGE C STABLE STRICT
    AS 'MODULE_PATHNAME', $$shard_name$$;
COMMENT ON FUNCTION pg_catalog.shard_name(object_name regclass, shard_id bigint)
    IS 'returns shard-extended identifier of object name';
