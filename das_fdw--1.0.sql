/*-------------------------------------------------------------------------
 *
 * das_fdw--1.2.sql
 * 			Foreign-data wrapper for remote DAS servers
 *
 * Portions Copyright (c) 2022-2024, EnterpriseDB Corporation.
 *
 * IDENTIFICATION
 * 			das_fdw--1.2.sql
 *
 *-------------------------------------------------------------------------
 */


CREATE FUNCTION das_fdw_handler()
RETURNS fdw_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION das_fdw_validator(text[], oid)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER das_fdw
  HANDLER das_fdw_handler
  VALIDATOR das_fdw_validator;

CREATE OR REPLACE FUNCTION das_fdw_version()
  RETURNS pg_catalog.int4 STRICT
  AS 'MODULE_PATHNAME' LANGUAGE C;
