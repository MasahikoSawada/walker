/* garbagemap--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION garbagemap" to load this file. \quit

CREATE FUNCTION garbage_summary(
       IN relname regclass,
       OUT rangeno INT,
       OUT freespace INT,
       OUT n_tuples INT,
       OUT n_dead_tuples INT,
       OUT n_all_visible INT)
AS 'MODULE_PATHNAME', 'garbage_summary'
LANGUAGE C STRICT PARALLEL UNSAFE;

CREATE FUNCTION pg_gmap_from_file(relname regclass) RETURNS text
AS 'MODULE_PATHNAME', 'pg_gmap_from_file'
LANGUAGE C STRICT PARALLEL UNSAFE;

-- garbage_summary  + dead_tuple_ratio
CREATE OR REPLACE FUNCTION gs(
       rel regclass,
       OUT rangeno INT,
       OUT freespace INT,
       OUT n_tuples INT,
       OUT n_dead_tuples INT,
       OUT n_all_visible INT,
       OUT dead_tuple_ratio NUMERIC(15,4))
RETURNS SETOF RECORD
AS $$
SELECT
	rangeno,
	freespace,
	n_tuples,
	n_dead_tuples,
	n_all_visible,
	(n_dead_tuples::numeric(15,4) / n_tuples)::numeric(15,4) as dead_tuple_ratio
FROM garbage_summary(rel);
$$
LANGUAGE sql;

-- Ranking of ranges having dead tuples */
CREATE OR REPLACE FUNCTION gs_rank(
       rel regclass,
       OUT rownum INT,
       OUT percent_blocks NUMERIC(15,4),
       OUT rangeno INT,
       OUT freespace INT,
       OUT n_tuples INT,
       OUT n_dead_tuples INT,
       OUT n_all_visible INT,
       OUT dead_tuple_ratio NUMERIC(15,4),
       OUT percent NUMERIC(15,4))
RETURNS SETOF RECORD
AS $$
SELECT
       (row_number() over())::INT as rownum,
       foo.*
       FROM
       (
              SELECT
              (percent_rank() over(order by n_dead_tuples desc) * 100)::numeric(15,4) as percent_blocks,
              *,
              round((sum(n_dead_tuples) over (order by n_dead_tuples desc range between unbounded preceding and current row))::numeric(15,4) / nullif(sum(n_dead_tuples) over (), 0)::int * 100, 2) as ratio
              FROM gs(rel)
              ORDER BY n_dead_tuples DESC, rangeno ASC
      ) as foo
$$
LANGUAGE sql;
