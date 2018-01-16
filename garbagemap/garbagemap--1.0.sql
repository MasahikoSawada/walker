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

-- garbage_summary  + dead_tuple_ratio
CREATE OR REPLACE FUNCTION gs(
       rel regclass,
       OUT rangeno INT,
       OUT freespace INT,
       OUT n_tuples INT,
       OUT n_dead_tuples INT,
       OUT n_all_visible INT,
       OUT dead_tuple_ratio NUMERIC(10,2))
RETURNS SETOF RECORD
AS $$
SELECT
	rangeno,
	freespace,
	n_tuples,
	n_dead_tuples,
	n_all_visible,
	(n_dead_tuples::numeric(10,2) / n_tuples)::numeric(10,2) as dead_tuple_ratio
FROM garbage_summary(rel);
$$
LANGUAGE sql;

-- Ranking of ranges having dead tuples */
CREATE OR REPLACE FUNCTION gs_rank(
       rel regclass,
       OUT rangeno INT,
       OUT freespace INT,
       OUT n_tuples INT,
       OUT n_dead_tuples INT,
       OUT n_all_visible INT,
       OUT dead_tuple_ratio NUMERIC(10,2),
       OUT precent NUMERIC(10,2))
RETURNS SETOF RECORD
AS $$
SELECT *,
       round((sum(n_dead_tuples) over (order by n_dead_tuples desc range between unbounded preceding and current row))::numeric(10,2) / sum(n_dead_tuples) over () * 100, 2)
       FROM gs(rel)
       ORDER BY n_dead_tuples DESC, rangeno ASC;
$$
LANGUAGE sql;
