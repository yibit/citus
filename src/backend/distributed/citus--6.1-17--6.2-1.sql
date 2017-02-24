/* citus--6.1-17--6.2-1.sql */

SET search_path = 'pg_catalog';

CREATE FUNCTION citus_table_size(table_name regclass)
	RETURNS bigint
    LANGUAGE C VOLATILE STRICT
    AS 'MODULE_PATHNAME', $$citus_table_size$$;
COMMENT ON FUNCTION citus_table_size(table_name regclass)
    IS 'computes the size of both and local distributed tables';

RESET search_path;
