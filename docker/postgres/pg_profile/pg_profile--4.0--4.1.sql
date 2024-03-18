\echo Use "ALTER EXTENSION pg_profile UPDATE" to load this file. \quit
DROP FUNCTION drop_server;
DROP FUNCTION delete_samples(integer, integer, integer);
DROP FUNCTION collect_obj_stats;
DROP FUNCTION collect_pg_stat_statements_stats;
DROP FUNCTION create_server(name, text, boolean, integer, text);
DROP FUNCTION export_data;
DROP FUNCTION import_data;
DROP FUNCTION take_sample(integer, boolean);
DROP FUNCTION get_report_context;
DROP FUNCTION save_pg_stat_statements;
DROP FUNCTION statements_stats;
DROP FUNCTION top_statements;
DROP FUNCTION top_elapsed_htbl;
DROP FUNCTION top_elapsed_diff_htbl;
DROP FUNCTION top_exec_time_diff_htbl;
DROP FUNCTION top_exec_time_htbl;
DROP FUNCTION top_plan_time_diff_htbl;
DROP FUNCTION top_plan_time_htbl;
CREATE FUNCTION create_server(IN server name, IN server_connstr text, IN server_enabled boolean = TRUE,
IN max_sample_age integer = NULL, IN description text = NULL) RETURNS integer SET search_path=@extschema@ AS $$
DECLARE
    server_exists     integer;
    sserver_id        integer;
BEGIN

    SELECT count(*) INTO server_exists FROM servers WHERE server_name=server;
    IF server_exists > 0 THEN
        RAISE 'Server already exists.';
    END IF;

    INSERT INTO servers(server_name,server_description,connstr,enabled,max_sample_age)
    VALUES (server,description,server_connstr,server_enabled,max_sample_age)
    RETURNING server_id INTO sserver_id;

    /*
    * We might create server sections to avoid concurrency on tables
    */
    PERFORM create_server_partitions(sserver_id);

    RETURN sserver_id;
END;
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION create_server(IN server name, IN server_connstr text, IN server_enabled boolean,
IN max_sample_age integer, IN description text) IS 'Create a new server';
CREATE FUNCTION create_server_partitions(IN sserver_id integer) RETURNS integer SET search_path=@extschema@ AS $$
DECLARE
    in_extension      boolean;
BEGIN
    -- Create last_stat_statements table partition
    EXECUTE format(
      'CREATE TABLE last_stat_statements_srv%1$s PARTITION OF last_stat_statements '
      'FOR VALUES IN (%1$s)',
      sserver_id);
    -- PK constraint for new partition
    EXECUTE format(
      'ALTER TABLE last_stat_statements_srv%1$s '
      'ADD CONSTRAINT pk_last_stat_satements_srv%1$s PRIMARY KEY (server_id, sample_id, userid, datid, queryid, toplevel)',
      sserver_id);
    /*
    * Check if partition is already in our extension. This happens when function
    * is called during CREATE EXTENSION script execution
    */
    EXECUTE format('SELECT count(*) = 1 '
      'FROM pg_depend dep '
        'JOIN pg_extension ext ON (dep.refobjid = ext.oid) '
        'JOIN pg_class rel ON (rel.oid = dep.objid AND rel.relkind= ''r'') '
      'WHERE ext.extname= ''pg_profile'' AND rel.relname = ''last_stat_statements_srv%1$s''',
      sserver_id) INTO in_extension;
    -- Add partition to extension
    IF NOT in_extension THEN
      EXECUTE format('ALTER EXTENSION pg_profile ADD TABLE last_stat_statements_srv%1$s',
        sserver_id);
    END IF;
    -- Create last_stat_kcache table partition
    EXECUTE format(
      'CREATE TABLE last_stat_kcache_srv%1$s PARTITION OF last_stat_kcache '
      'FOR VALUES IN (%1$s)',
      sserver_id);
    EXECUTE format(
      'ALTER TABLE last_stat_kcache_srv%1$s '
      'ADD CONSTRAINT pk_last_stat_kcache_srv%1$s PRIMARY KEY (server_id, sample_id, datid, userid, queryid, toplevel), '
      'ADD CONSTRAINT fk_last_kcache_stmts_srv%1$s FOREIGN KEY '
        '(server_id, sample_id, datid, userid, queryid, toplevel) REFERENCES '
        'last_stat_statements_srv%1$s(server_id, sample_id, datid, userid, queryid, toplevel) '
        'ON DELETE CASCADE',
      sserver_id);
    IF NOT in_extension THEN
      EXECUTE format('ALTER EXTENSION pg_profile ADD TABLE last_stat_kcache_srv%1$s',
        sserver_id);
    END IF;

    -- Create last_stat_database table partition
    EXECUTE format(
      'CREATE TABLE last_stat_database_srv%1$s PARTITION OF last_stat_database '
      'FOR VALUES IN (%1$s)',
      sserver_id);
    EXECUTE format(
      'ALTER TABLE last_stat_database_srv%1$s '
        'ADD CONSTRAINT pk_last_stat_database_srv%1$s PRIMARY KEY (server_id, sample_id, datid), '
        'ADD CONSTRAINT fk_last_stat_database_samples_srv%1$s '
          'FOREIGN KEY (server_id, sample_id) '
          'REFERENCES samples(server_id, sample_id) ON DELETE RESTRICT',
        sserver_id);
    IF NOT in_extension THEN
      EXECUTE format('ALTER EXTENSION pg_profile ADD TABLE last_stat_database_srv%1$s',
        sserver_id);
    END IF;

    -- Create last_stat_tablespaces table partition
    EXECUTE format(
      'CREATE TABLE last_stat_tablespaces_srv%1$s PARTITION OF last_stat_tablespaces '
      'FOR VALUES IN (%1$s)',
      sserver_id);
    EXECUTE format(
      'ALTER TABLE last_stat_tablespaces_srv%1$s '
        'ADD CONSTRAINT pk_last_stat_tablespaces_srv%1$s PRIMARY KEY (server_id, sample_id, tablespaceid), '
        'ADD CONSTRAINT fk_last_stat_tablespaces_samples_srv%1$s '
          'FOREIGN KEY (server_id, sample_id) REFERENCES samples(server_id, sample_id) '
          'ON DELETE RESTRICT',
        sserver_id);
    IF NOT in_extension THEN
      EXECUTE format('ALTER EXTENSION pg_profile ADD TABLE last_stat_tablespaces_srv%1$s',
        sserver_id);
    END IF;

    -- Create last_stat_tables table partition
    EXECUTE format(
      'CREATE TABLE last_stat_tables_srv%1$s PARTITION OF last_stat_tables '
      'FOR VALUES IN (%1$s)',
      sserver_id);
    EXECUTE format(
      'ALTER TABLE last_stat_tables_srv%1$s '
        'ADD CONSTRAINT pk_last_stat_tables_srv%1$s '
          'PRIMARY KEY (server_id, sample_id, datid, relid), '
        'ADD CONSTRAINT fk_last_stat_tables_dat_srv%1$s '
          'FOREIGN KEY (server_id, sample_id, datid) '
          'REFERENCES sample_stat_database(server_id, sample_id, datid) ON DELETE RESTRICT',
        sserver_id);
    IF NOT in_extension THEN
      EXECUTE format('ALTER EXTENSION pg_profile ADD TABLE last_stat_tables_srv%1$s',
        sserver_id);
    END IF;

    -- Create last_stat_indexes table partition
    EXECUTE format(
      'CREATE TABLE last_stat_indexes_srv%1$s PARTITION OF last_stat_indexes '
      'FOR VALUES IN (%1$s)',
      sserver_id);
    EXECUTE format(
      'ALTER TABLE last_stat_indexes_srv%1$s '
        'ADD CONSTRAINT pk_last_stat_indexes_srv%1$s '
          'PRIMARY KEY (server_id, sample_id, datid, indexrelid), '
        'ADD CONSTRAINT fk_last_stat_indexes_dat_srv%1$s '
        'FOREIGN KEY (server_id, sample_id, datid) '
          'REFERENCES sample_stat_database(server_id, sample_id, datid) ON DELETE RESTRICT',
        sserver_id);
    IF NOT in_extension THEN
      EXECUTE format('ALTER EXTENSION pg_profile ADD TABLE last_stat_indexes_srv%1$s',
        sserver_id);
    END IF;

    -- Create last_stat_user_functions table partition
    EXECUTE format(
      'CREATE TABLE last_stat_user_functions_srv%1$s PARTITION OF last_stat_user_functions '
      'FOR VALUES IN (%1$s)',
      sserver_id);
    EXECUTE format(
      'ALTER TABLE last_stat_user_functions_srv%1$s '
      'ADD CONSTRAINT pk_last_stat_user_functions_srv%1$s '
      'PRIMARY KEY (server_id, sample_id, datid, funcid), '
      'ADD CONSTRAINT fk_last_stat_user_functions_dat_srv%1$s '
        'FOREIGN KEY (server_id, sample_id, datid) '
        'REFERENCES sample_stat_database(server_id, sample_id, datid) ON DELETE RESTRICT',
        sserver_id);
    IF NOT in_extension THEN
      EXECUTE format('ALTER EXTENSION pg_profile ADD TABLE last_stat_user_functions_srv%1$s',
        sserver_id);
    END IF;

    RETURN sserver_id;
END;
$$ LANGUAGE plpgsql;
CREATE FUNCTION drop_server(IN server name) RETURNS integer SET search_path=@extschema@ AS $$
DECLARE
    del_rows    integer;
    dserver_id  integer;
BEGIN
    SELECT server_id INTO STRICT dserver_id FROM servers WHERE server_name = server;
    DELETE FROM bl_samples WHERE server_id = dserver_id;
    EXECUTE format('ALTER EXTENSION pg_profile DROP TABLE last_stat_kcache_srv%1$s',
      dserver_id);
    EXECUTE format(
      'DROP TABLE last_stat_kcache_srv%1$s',
      dserver_id);
    EXECUTE format('ALTER EXTENSION pg_profile DROP TABLE last_stat_statements_srv%1$s',
      dserver_id);
    EXECUTE format(
      'DROP TABLE last_stat_statements_srv%1$s',
      dserver_id);
    EXECUTE format('ALTER EXTENSION pg_profile DROP TABLE last_stat_database_srv%1$s',
      dserver_id);
    EXECUTE format(
      'DROP TABLE last_stat_database_srv%1$s',
      dserver_id);
    EXECUTE format('ALTER EXTENSION pg_profile DROP TABLE last_stat_tables_srv%1$s',
      dserver_id);
    EXECUTE format(
      'DROP TABLE last_stat_tables_srv%1$s',
      dserver_id);
    EXECUTE format('ALTER EXTENSION pg_profile DROP TABLE last_stat_indexes_srv%1$s',
      dserver_id);
    EXECUTE format(
      'DROP TABLE last_stat_indexes_srv%1$s',
      dserver_id);
    EXECUTE format('ALTER EXTENSION pg_profile DROP TABLE last_stat_tablespaces_srv%1$s',
      dserver_id);
    EXECUTE format(
      'DROP TABLE last_stat_tablespaces_srv%1$s',
      dserver_id);
    EXECUTE format('ALTER EXTENSION pg_profile DROP TABLE last_stat_user_functions_srv%1$s',
      dserver_id);
    EXECUTE format(
      'DROP TABLE last_stat_user_functions_srv%1$s',
      dserver_id);
    DELETE FROM last_stat_cluster WHERE server_id = dserver_id;
    DELETE FROM last_stat_wal WHERE server_id = dserver_id;
    DELETE FROM last_stat_archiver WHERE server_id = dserver_id;
    DELETE FROM sample_stat_tablespaces WHERE server_id = dserver_id;
    DELETE FROM tablespaces_list WHERE server_id = dserver_id;
    /*
     * We have several constraints that should be deferred to avoid
     * violation due to several cascade deletion branches
     */
    SET CONSTRAINTS
        fk_stat_indexes_indexes,
        fk_toast_table,
        fk_st_tablespaces_tablespaces,
        fk_st_tables_tables,
        fk_indexes_tables,
        fk_user_functions_functions,
        fk_stmt_list,
        fk_kcache_stmt_list,
        fk_statements_roles
      DEFERRED;
    DELETE FROM samples WHERE server_id = dserver_id;
    SET CONSTRAINTS
        fk_stat_indexes_indexes,
        fk_toast_table,
        fk_st_tablespaces_tablespaces,
        fk_st_tables_tables,
        fk_indexes_tables,
        fk_user_functions_functions,
        fk_stmt_list,
        fk_kcache_stmt_list,
        fk_statements_roles
      IMMEDIATE;
    DELETE FROM servers WHERE server_id = dserver_id;
    GET DIAGNOSTICS del_rows = ROW_COUNT;
    RETURN del_rows;
END;
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION drop_server(IN server name) IS 'Drop a server';
CREATE FUNCTION delete_samples(IN server_id integer, IN start_id integer = NULL, IN end_id integer = NULL)
RETURNS integer
SET search_path=@extschema@ AS $$
DECLARE
  smp_delcount  integer;
BEGIN
  /*
  * There could exist sample before deletion interval using
  * dictionary values having last_sample_id value in deletion
  * interval. So we need to move such last_sample_id values
  * to the past
  * We need to do so only if there is at last one sample before
  * deletion interval. Usually there won't any, because this
  * could happen only when there is a baseline in use or manual
  * deletion is performed.
  */
  IF (SELECT count(*) > 0 FROM samples s
    WHERE s.server_id = delete_samples.server_id AND sample_id < start_id) OR
    (SELECT count(*) > 0 FROM bl_samples bs
    WHERE bs.server_id = delete_samples.server_id
      AND bs.sample_id BETWEEN start_id AND end_id)
  THEN
    -- Statements list
    UPDATE stmt_list uls
    SET last_sample_id = new_lastids.last_sample_id
    FROM (
      SELECT queryid_md5, max(rf.sample_id) AS last_sample_id
      FROM
        sample_statements rf JOIN stmt_list lst USING (server_id, queryid_md5)
        LEFT JOIN bl_samples bl ON
          (bl.server_id, bl.sample_id) = (rf.server_id, rf.sample_id) AND bl.sample_id BETWEEN start_id AND end_id
      WHERE
        rf.server_id = delete_samples.server_id
        AND lst.last_sample_id BETWEEN start_id AND end_id
        AND (rf.sample_id < start_id OR bl.sample_id IS NOT NULL)
      GROUP BY queryid_md5
      ) new_lastids
    WHERE
      (uls.server_id, uls.queryid_md5) = (delete_samples.server_id, new_lastids.queryid_md5)
      AND uls.last_sample_id BETWEEN start_id AND end_id;

    UPDATE tablespaces_list uls
    SET last_sample_id = new_lastids.last_sample_id
    FROM (
      SELECT tablespaceid, max(rf.sample_id) AS last_sample_id
      FROM
        sample_stat_tablespaces rf JOIN tablespaces_list lst
          USING (server_id, tablespaceid)
        LEFT JOIN bl_samples bl ON
          (bl.server_id, bl.sample_id) = (rf.server_id, rf.sample_id) AND bl.sample_id BETWEEN start_id AND end_id
      WHERE
        rf.server_id = delete_samples.server_id
        AND lst.last_sample_id BETWEEN start_id AND end_id
        AND (rf.sample_id < start_id OR bl.sample_id IS NOT NULL)
      GROUP BY tablespaceid
      ) new_lastids
    WHERE
      (uls.server_id, uls.tablespaceid) =
      (delete_samples.server_id, new_lastids.tablespaceid)
      AND uls.last_sample_id BETWEEN start_id AND end_id;

    -- Roles
    UPDATE roles_list uls
    SET last_sample_id = new_lastids.last_sample_id
    FROM (
      SELECT userid, max(rf.sample_id) AS last_sample_id
      FROM
        sample_statements rf JOIN roles_list lst
          USING (server_id, userid)
        LEFT JOIN bl_samples bl ON
          (bl.server_id, bl.sample_id) = (rf.server_id, rf.sample_id) AND bl.sample_id BETWEEN start_id AND end_id
      WHERE
        rf.server_id = delete_samples.server_id
        AND lst.last_sample_id BETWEEN start_id AND end_id
        AND (rf.sample_id < start_id OR bl.sample_id IS NOT NULL)
      GROUP BY userid
      ) new_lastids
    WHERE
      (uls.server_id, uls.userid) =
      (delete_samples.server_id, new_lastids.userid)
      AND uls.last_sample_id BETWEEN start_id AND end_id;

    -- Indexes
    UPDATE indexes_list uls
    SET last_sample_id = new_lastids.last_sample_id
    FROM (
      SELECT indexrelid, max(rf.sample_id) AS last_sample_id
      FROM
        sample_stat_indexes rf JOIN indexes_list lst
          USING (server_id, indexrelid)
        LEFT JOIN bl_samples bl ON
          (bl.server_id, bl.sample_id) = (rf.server_id, rf.sample_id) AND bl.sample_id BETWEEN start_id AND end_id
      WHERE
        rf.server_id = delete_samples.server_id
        AND lst.last_sample_id BETWEEN start_id AND end_id
        AND (rf.sample_id < start_id OR bl.sample_id IS NOT NULL)
      GROUP BY indexrelid
      ) new_lastids
    WHERE
      (uls.server_id, uls.indexrelid) =
      (delete_samples.server_id, new_lastids.indexrelid)
      AND uls.last_sample_id BETWEEN start_id AND end_id;

    -- Tables
    UPDATE tables_list uls
    SET last_sample_id = new_lastids.last_sample_id
    FROM (
      SELECT relid, max(rf.sample_id) AS last_sample_id
      FROM
        sample_stat_tables rf JOIN tables_list lst
          USING (server_id, relid)
        LEFT JOIN bl_samples bl ON
          (bl.server_id, bl.sample_id) = (rf.server_id, rf.sample_id) AND bl.sample_id BETWEEN start_id AND end_id
      WHERE
        rf.server_id = delete_samples.server_id
        AND lst.last_sample_id BETWEEN start_id AND end_id
        AND (rf.sample_id < start_id OR bl.sample_id IS NOT NULL)
      GROUP BY relid
      ) new_lastids
    WHERE
      (uls.server_id, uls.relid) =
      (delete_samples.server_id, new_lastids.relid)
      AND uls.last_sample_id BETWEEN start_id AND end_id;

    -- Functions
    UPDATE funcs_list uls
    SET last_sample_id = new_lastids.last_sample_id
    FROM (
      SELECT funcid, max(rf.sample_id) AS last_sample_id
      FROM
        sample_stat_user_functions rf JOIN funcs_list lst
          USING (server_id, funcid)
        LEFT JOIN bl_samples bl ON
          (bl.server_id, bl.sample_id) = (rf.server_id, rf.sample_id) AND bl.sample_id BETWEEN start_id AND end_id
      WHERE
        rf.server_id = delete_samples.server_id
        AND lst.last_sample_id BETWEEN start_id AND end_id
        AND (rf.sample_id < start_id OR bl.sample_id IS NOT NULL)
      GROUP BY funcid
      ) new_lastids
    WHERE
      (uls.server_id, uls.funcid) =
      (delete_samples.server_id, new_lastids.funcid)
      AND uls.last_sample_id BETWEEN start_id AND end_id;
  END IF;

  -- Delete specified samples without baseline samples
  SET CONSTRAINTS
      fk_stat_indexes_indexes,
      fk_toast_table,
      fk_st_tablespaces_tablespaces,
      fk_st_tables_tables,
      fk_indexes_tables,
      fk_user_functions_functions,
      fk_stmt_list,
      fk_kcache_stmt_list,
      fk_statements_roles
    DEFERRED;
  DELETE FROM samples dsmp
  USING
    servers srv
    JOIN samples smp USING (server_id)
    LEFT JOIN bl_samples bls USING (server_id, sample_id)
  WHERE
    (dsmp.server_id, dsmp.sample_id) =
    (smp.server_id, smp.sample_id) AND
    smp.sample_id != srv.last_sample_id AND
    srv.server_id = delete_samples.server_id AND
    bls.sample_id IS NULL AND (
      (start_id IS NULL AND end_id IS NULL) OR
      smp.sample_id BETWEEN delete_samples.start_id AND delete_samples.end_id
    )
  ;
  GET DIAGNOSTICS smp_delcount := ROW_COUNT;
  SET CONSTRAINTS
      fk_stat_indexes_indexes,
      fk_toast_table,
      fk_st_tablespaces_tablespaces,
      fk_st_tables_tables,
      fk_indexes_tables,
      fk_user_functions_functions,
      fk_stmt_list,
      fk_kcache_stmt_list,
      fk_statements_roles
    IMMEDIATE;

  IF smp_delcount > 0 THEN
    -- Delete obsolete values of postgres parameters
    DELETE FROM sample_settings ss
    USING (
      SELECT ss.server_id, max(first_seen) AS first_seen, setting_scope, name
      FROM sample_settings ss
      WHERE ss.server_id = delete_samples.server_id AND first_seen <=
        (SELECT min(sample_time) FROM samples s WHERE s.server_id = delete_samples.server_id)
      GROUP BY ss.server_id, setting_scope, name) AS ss_ref
    WHERE ss.server_id = ss_ref.server_id AND
      ss.setting_scope = ss_ref.setting_scope AND
      ss.name = ss_ref.name AND
      ss.first_seen < ss_ref.first_seen;
    -- Delete obsolete values of postgres parameters from previous versions of postgres on server
    DELETE FROM sample_settings ss
    WHERE ss.server_id = delete_samples.server_id AND first_seen <
      (SELECT min(first_seen) FROM sample_settings mss WHERE mss.server_id = delete_samples.server_id AND name = 'version' AND setting_scope = 2);
  END IF;

  RETURN smp_delcount;
END;
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION delete_samples(integer, integer, integer) IS
  'Manually deletes server samples for provided server identifier. By default deletes all samples';
CREATE FUNCTION export_data(IN server_name name = NULL, IN min_sample_id integer = NULL,
  IN max_sample_id integer = NULL, IN obfuscate_queries boolean = FALSE)
RETURNS TABLE(
    section_id  bigint,
    row_data    json
) SET search_path=@extschema@ AS $$
DECLARE
  section_counter   bigint = 0;
  ext_version       text = NULL;
  tables_list       json = NULL;
  sserver_id        integer = NULL;
  r_result          RECORD;
BEGIN
  /*
    Exported table will contain rows of extension tables, packed in JSON
    Each row will have a section ID, defining a table in most cases
    First sections contains metadata - extension name and version, tables list
  */
  -- Extension info
  IF (SELECT count(*) = 1 FROM pg_catalog.pg_extension WHERE extname = 'pg_profile') THEN
    SELECT extversion INTO STRICT r_result FROM pg_catalog.pg_extension WHERE extname = 'pg_profile';
    ext_version := r_result.extversion;
  ELSE
    RAISE 'Export is not supported for manual installed version';
  END IF;
  RETURN QUERY EXECUTE $q$SELECT $3, row_to_json(s)
    FROM (SELECT $1 AS extension,
              $2 AS version,
              $3 + 1 AS tab_list_section
    ) s$q$
    USING 'pg_profile', ext_version, section_counter;
  section_counter := section_counter + 1;
  -- tables list
  EXECUTE $q$
    WITH RECURSIVE exp_tables (reloid, relname, inc_rels) AS (
      -- start with all independent tables
        SELECT rel.oid, rel.relname, array_agg(rel.oid) OVER()
          FROM pg_depend dep
            JOIN pg_extension ext ON (dep.refobjid = ext.oid)
            JOIN pg_class rel ON (rel.oid = dep.objid AND rel.relkind IN ('r','p'))
            LEFT OUTER JOIN fkdeps con ON (con.reloid = dep.objid)
          WHERE ext.extname = $1 AND rel.relname NOT IN
              ('import_queries', 'import_queries_version_order',
              'report', 'report_static', 'report_struct')
            AND NOT rel.relispartition
            AND con.reloid IS NULL
      UNION
      -- and add all tables that have resolved dependencies by previously added tables
          SELECT con.reloid as reloid, con.relname, recurse.inc_rels||array_agg(con.reloid) OVER()
          FROM
            fkdeps con JOIN
            exp_tables recurse ON
              (array_append(recurse.inc_rels,con.reloid) @> con.reldeps AND
              NOT ARRAY[con.reloid] <@ recurse.inc_rels)
    ),
    fkdeps (reloid, relname, reldeps) AS (
      -- tables with their foreign key dependencies
      SELECT rel.oid as reloid, rel.relname, array_agg(con.confrelid), array_agg(rel.oid) OVER()
      FROM pg_depend dep
        JOIN pg_extension ext ON (dep.refobjid = ext.oid)
        JOIN pg_class rel ON (rel.oid = dep.objid AND rel.relkind IN ('r','p'))
        JOIN pg_constraint con ON (con.conrelid = dep.objid AND con.contype = 'f')
      WHERE ext.extname = $1 AND rel.relname NOT IN
        ('import_queries', 'import_queries_version_order',
        'report', 'report_static', 'report_struct')
        AND NOT rel.relispartition
      GROUP BY rel.oid, rel.relname
    )
    SELECT json_agg(row_to_json(tl)) FROM
    (SELECT row_number() OVER() + $2 AS section_id, relname FROM exp_tables) tl ;
  $q$ INTO tables_list
  USING 'pg_profile', section_counter;
  section_id := section_counter;
  row_data := tables_list;
  RETURN NEXT;
  section_counter := section_counter + 1;
  -- Server selection
  IF export_data.server_name IS NOT NULL THEN
    sserver_id := get_server_by_name(export_data.server_name);
  END IF;
  -- Tables data
  FOR r_result IN
    SELECT json_array_elements(tables_list)->>'relname' as relname
  LOOP
    -- Tables select conditions
    CASE
      WHEN r_result.relname != 'sample_settings'
        AND (r_result.relname LIKE 'sample%' OR r_result.relname LIKE 'last%') THEN
        RETURN QUERY EXECUTE format(
            $q$SELECT $1,row_to_json(dt) FROM
              (SELECT * FROM %I WHERE ($2 IS NULL OR $2 = server_id) AND
                ($3 IS NULL OR sample_id >= $3) AND
                ($4 IS NULL OR sample_id <= $4)) dt$q$,
            r_result.relname
          )
        USING
          section_counter,
          sserver_id,
          min_sample_id,
          max_sample_id;
      WHEN r_result.relname = 'bl_samples' THEN
        RETURN QUERY EXECUTE format(
            $q$
            SELECT $1,row_to_json(dt) FROM (
              SELECT *
              FROM %I b
                JOIN (
                  SELECT bl_id
                  FROM bl_samples
                    WHERE ($2 IS NULL OR $2 = server_id)
                  GROUP BY bl_id
                  HAVING
                    ($3 IS NULL OR min(sample_id) >= $3) AND
                    ($4 IS NULL OR max(sample_id) <= $4)
                ) bl_smp USING (bl_id)
              WHERE ($2 IS NULL OR $2 = server_id)
              ) dt$q$,
            r_result.relname
          )
        USING
          section_counter,
          sserver_id,
          min_sample_id,
          max_sample_id;
      WHEN r_result.relname = 'baselines' THEN
        RETURN QUERY EXECUTE format(
            $q$
            SELECT $1,row_to_json(dt) FROM (
              SELECT b.*
              FROM %I b
              JOIN bl_samples bs USING(server_id, bl_id)
                WHERE ($2 IS NULL OR $2 = server_id)
              GROUP BY b.server_id, b.bl_id, b.bl_name, b.keep_until
              HAVING
                ($3 IS NULL OR min(sample_id) >= $3) AND
                ($4 IS NULL OR max(sample_id) <= $4)
              ) dt$q$,
            r_result.relname
          )
        USING
          section_counter,
          sserver_id,
          min_sample_id,
          max_sample_id;
      WHEN r_result.relname = 'stmt_list' THEN
        RETURN QUERY EXECUTE format(
            $sql$SELECT $1,row_to_json(dt) FROM
              (SELECT rows.server_id, rows.queryid_md5,
                CASE $5
                  WHEN TRUE THEN pg_catalog.md5(rows.query)
                  ELSE rows.query
                END AS query,
                last_sample_id
               FROM %I AS rows WHERE (server_id,queryid_md5) IN
                (SELECT server_id, queryid_md5 FROM sample_statements WHERE
                  ($2 IS NULL OR $2 = server_id) AND
                ($3 IS NULL OR sample_id >= $3) AND
                ($4 IS NULL OR sample_id <= $4))) dt$sql$,
            r_result.relname
          )
        USING
          section_counter,
          sserver_id,
          min_sample_id,
          max_sample_id,
          obfuscate_queries;
      ELSE
        RETURN QUERY EXECUTE format(
            $q$SELECT $1,row_to_json(dt) FROM (SELECT * FROM %I WHERE $2 IS NULL OR $2 = server_id) dt$q$,
            r_result.relname
          )
        USING section_counter, sserver_id;
    END CASE;
    section_counter := section_counter + 1;
  END LOOP;
  RETURN;
END;
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION export_data(IN server_name name, IN min_sample_id integer,
  IN max_sample_id integer, IN obfuscate_queries boolean) IS 'Export collected data as a table';
CREATE FUNCTION import_data(data regclass, server_name_prefix text = NULL) RETURNS bigint
SET search_path=@extschema@ AS $$
DECLARE
  import_meta     jsonb;
  tables_list     jsonb;
  servers_list    jsonb; -- import servers list

  row_proc        bigint;
  rows_processed  bigint = 0;
  new_server_id   integer = null;
  import_stage    integer = 0;

  r_result        RECORD;
BEGIN
  -- Get import metadata
  EXECUTE format('SELECT row_data::jsonb FROM %s WHERE section_id = 0',data)
  INTO STRICT import_meta;

  -- Check dump compatibility
  IF (SELECT count(*) < 1 FROM import_queries_version_order
      WHERE extension = import_meta ->> 'extension'
        AND version = import_meta ->> 'version')
  THEN
    RAISE 'Unsupported extension version: %', (import_meta ->> 'extension')||' '||(import_meta ->> 'version');
  END IF;

  -- Get import tables list
  EXECUTE format('SELECT row_data::jsonb FROM %s WHERE section_id = $1',data)
  USING (import_meta ->> 'tab_list_section')::integer
  INTO STRICT tables_list;
  -- Servers processing
  -- Get import servers list
  EXECUTE format($q$SELECT
      jsonb_agg(srvjs.row_data::jsonb)
    FROM
      jsonb_to_recordset($1) as tbllist(section_id integer, relname text),
      %1$s srvjs
    WHERE
      tbllist.relname = 'servers'
      AND srvjs.section_id = tbllist.section_id$q$,
    data)
  USING tables_list
  INTO STRICT servers_list;

  CREATE TEMPORARY TABLE IF NOT EXISTS tmp_srv_map (
    imp_srv_id bigint PRIMARY KEY,
    local_srv_id bigint
  );

  TRUNCATE tmp_srv_map;

  /*
   * Performing importing to local servers matching. We need to consider several cases:
   * - creation dates and system identifiers matched - we have a match
   * - creation dates and system identifiers don't match, but names matched - conflict as we can't create a new server
   * - nothing matched - a new local server is to be created
   * By the way, we'll populate tmp_srv_map table, containing
   * a mapping between local and importing servers to use on data load.
   */
  FOR r_result IN EXECUTE format($q$SELECT
      COALESCE($3,'')||
        imp_srv.server_name       imp_server_name,
      ls.server_name              local_server_name,
      imp_srv.server_created      imp_server_created,
      ls.server_created           local_server_created,
      d.row_data->>'reset_val'    imp_system_identifier,
      ls.system_identifier        local_system_identifier,
      imp_srv.server_id           imp_server_id,
      ls.server_id                local_server_id,
      imp_srv.server_description  imp_server_description,
      imp_srv.db_exclude          imp_server_db_exclude,
      imp_srv.connstr             imp_server_connstr,
      imp_srv.max_sample_age      imp_server_max_sample_age,
      imp_srv.last_sample_id      imp_server_last_sample_id,
      imp_srv.size_smp_wnd_start  imp_size_smp_wnd_start,
      imp_srv.size_smp_wnd_dur    imp_size_smp_wnd_dur,
      imp_srv.size_smp_interval   imp_size_smp_interval
    FROM
      jsonb_to_recordset($1) as
        imp_srv(
          server_id           integer,
          server_name         name,
          server_description  text,
          server_created      timestamp with time zone,
          db_exclude          name[],
          enabled             boolean,
          connstr             text,
          max_sample_age      integer,
          last_sample_id      integer,
          size_smp_wnd_start  time with time zone,
          size_smp_wnd_dur    interval hour to second,
          size_smp_interval   interval day to minute
        )
      JOIN jsonb_to_recordset($2) AS tbllist(section_id integer, relname text)
        ON (tbllist.relname = 'sample_settings')
      JOIN %s d ON
        (d.section_id = tbllist.section_id AND d.row_data->>'name' = 'system_identifier'
          AND (d.row_data->>'server_id')::integer = imp_srv.server_id)
      LEFT OUTER JOIN (
        SELECT
          srv.server_id,
          srv.server_name,
          srv.server_created,
          set.reset_val as system_identifier
        FROM servers srv
          LEFT OUTER JOIN sample_settings set ON (set.server_id = srv.server_id AND set.name = 'system_identifier')
        ) ls ON
        ((imp_srv.server_created = ls.server_created AND d.row_data->>'reset_val' = ls.system_identifier)
          OR COALESCE($3,'')||imp_srv.server_name = ls.server_name)
    $q$,
    data)
  USING
    servers_list,
    tables_list,
    server_name_prefix
  LOOP
    IF r_result.imp_server_created = r_result.local_server_created AND
      r_result.imp_system_identifier = r_result.local_system_identifier
    THEN
      /* use this local server when matched by server creation time and system identifier */
      INSERT INTO tmp_srv_map (imp_srv_id,local_srv_id) VALUES
        (r_result.imp_server_id,r_result.local_server_id);
      /* Update local server if new last_sample_id is greatest*/
      UPDATE servers
      SET
        (
          db_exclude,
          connstr,
          max_sample_age,
          last_sample_id,
          size_smp_wnd_start,
          size_smp_wnd_dur,
          size_smp_interval
        ) = (
          r_result.imp_server_db_exclude,
          r_result.imp_server_connstr,
          r_result.imp_server_max_sample_age,
          r_result.imp_server_last_sample_id,
          r_result.imp_size_smp_wnd_start,
          r_result.imp_size_smp_wnd_dur,
          r_result.imp_size_smp_interval
        )
      WHERE server_id = r_result.local_server_id
        AND last_sample_id < r_result.imp_server_last_sample_id;
    ELSIF r_result.imp_server_name = r_result.local_server_name
    THEN
      /* Names matched, but identifiers does not - we have a conflict */
      RAISE 'Local server "%" creation date or system identifier does not match imported one (try renaming local server)',
        r_result.local_server_name;
    ELSIF r_result.local_server_name IS NULL
    THEN
      /* No match at all - we are creating a new server */
      INSERT INTO servers AS srv (
        server_name,
        server_description,
        server_created,
        db_exclude,
        enabled,
        connstr,
        max_sample_age,
        last_sample_id,
        size_smp_wnd_start,
        size_smp_wnd_dur,
        size_smp_interval)
      VALUES (
        r_result.imp_server_name,
        r_result.imp_server_description,
        r_result.imp_server_created,
        r_result.imp_server_db_exclude,
        FALSE,
        r_result.imp_server_connstr,
        r_result.imp_server_max_sample_age,
        r_result.imp_server_last_sample_id,
        r_result.imp_size_smp_wnd_start,
        r_result.imp_size_smp_wnd_dur,
        r_result.imp_size_smp_interval
      )
      RETURNING server_id INTO new_server_id;
      INSERT INTO tmp_srv_map (imp_srv_id,local_srv_id) VALUES
        (r_result.imp_server_id,new_server_id);
      PERFORM create_server_partitions(new_server_id);
    ELSE
      /* This shouldn't ever happen */
      RAISE 'Import and local servers matching exception';
    END IF;
  END LOOP;
  ANALYZE tmp_srv_map;

  /* Import tables data
  * We have three stages here:
  * 1) Common stage for non-partitioned tables
  * 2) Import independent last_* tables data
  * 3) Import last_stat_kcache data as it depends on last_stat_statements
  */
  import_stage = 0;
  WHILE import_stage < 3 LOOP
    FOR r_result IN (
      -- get most recent versions of queries for importing tables
      WITH RECURSIVE ver_order (extension,version,level) AS (
        SELECT
          extension,
          version,
          1 as level
        FROM import_queries_version_order
        WHERE extension = import_meta ->> 'extension'
          AND version = import_meta ->> 'version'
        UNION ALL
        SELECT
          vo.parent_extension,
          vo.parent_version,
          vor.level + 1 as level
        FROM import_queries_version_order vo
          JOIN ver_order vor ON
            ((vo.extension, vo.version) = (vor.extension, vor.version))
        WHERE vo.parent_version IS NOT NULL
      )
      SELECT
        q.query,
        q.exec_order,
        tbllist.section_id as section_id,
        tbllist.relname
      FROM
        ver_order vo JOIN
        (SELECT min(o.level) as level,vq.extension, vq.relname FROM ver_order o
        JOIN import_queries vq ON (o.extension, o.version) = (vq.extension, vq.from_version)
        GROUP BY vq.extension, vq.relname) as min_level ON
          (vo.extension,vo.level) = (min_level.extension,min_level.level)
        JOIN import_queries q ON
          (q.extension,q.from_version,q.relname) = (vo.extension,vo.version,min_level.relname)
        RIGHT OUTER JOIN jsonb_to_recordset(tables_list) as tbllist(section_id integer, relname text) ON
          (tbllist.relname = q.relname)
      WHERE tbllist.relname NOT IN ('servers')
      ORDER BY tbllist.section_id ASC, q.exec_order ASC
    )
    LOOP
      CASE import_stage
        WHEN 0 THEN CONTINUE WHEN r_result.relname LIKE 'last_%';
        WHEN 1 THEN CONTINUE WHEN r_result.relname NOT LIKE 'last_%' OR
          r_result.relname = 'last_stat_kcache';
        WHEN 2 THEN CONTINUE WHEN r_result.relname != 'last_stat_kcache';
      END CASE;
      -- Forgotten query for table check
      IF r_result.query IS NULL THEN
        RAISE 'There is no import query for relation %', r_result.relname;
      END IF;
      -- execute load query for each import relation
      EXECUTE
        format(r_result.query,
          data)
      USING
        r_result.section_id;
      GET DIAGNOSTICS row_proc = ROW_COUNT;
      rows_processed := rows_processed + row_proc;
    END LOOP; -- over importing tables
    import_stage := import_stage + 1; -- next import stage
  END LOOP; -- over import_stages

  RETURN rows_processed;
END;
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION import_data(regclass, text) IS
  'Import sample data from table, exported by export_data function';
CREATE FUNCTION collect_pg_stat_statements_stats(IN properties jsonb, IN sserver_id integer, IN s_id integer, IN topn integer) RETURNS void SET search_path=@extschema@ AS $$
DECLARE
  qres              record;
  st_query          text;
BEGIN
    -- Adding dblink extension schema to search_path if it does not already there
    SELECT extnamespace::regnamespace AS dblink_schema INTO STRICT qres FROM pg_catalog.pg_extension WHERE extname = 'dblink';
    IF NOT string_to_array(current_setting('search_path'),',') @> ARRAY[qres.dblink_schema::text] THEN
      EXECUTE 'SET LOCAL search_path TO ' || current_setting('search_path')||','|| qres.dblink_schema;
    END IF;

    -- Check if mandatory extensions exists
    IF NOT
      (
        SELECT count(*) = 1
        FROM jsonb_to_recordset(properties #> '{extensions}') AS ext(extname text)
        WHERE extname = 'pg_stat_statements'
      )
    THEN
      RETURN;
    END IF;

    -- Dynamic statements query
    st_query := format(
      'SELECT '
        'st.userid,'
        'st.userid::regrole AS username,'
        'st.dbid,'
        'st.queryid,'
        '{statements_fields} '
      'FROM '
        '{statements_view} st '
    );

    st_query := replace(st_query, '{statements_view}',
      format('%1$I.pg_stat_statements(false)',
        (
          SELECT extnamespace FROM jsonb_to_recordset(properties #> '{extensions}')
            AS x(extname text, extnamespace text)
          WHERE extname = 'pg_stat_statements'
        )
      )
    );

    -- pg_stat_statements versions
    CASE (
        SELECT extversion
        FROM jsonb_to_recordset(properties #> '{extensions}')
          AS ext(extname text, extversion text)
        WHERE extname = 'pg_stat_statements'
      )
      WHEN '1.3','1.4','1.5','1.6','1.7' THEN
        st_query := replace(st_query, '{statements_fields}',
          'true as toplevel,'
          'NULL as plans,'
          'NULL as total_plan_time,'
          'NULL as min_plan_time,'
          'NULL as max_plan_time,'
          'NULL as mean_plan_time,'
          'NULL as stddev_plan_time,'
          'st.calls,'
          'st.total_time as total_exec_time,'
          'st.min_time as min_exec_time,'
          'st.max_time as max_exec_time,'
          'st.mean_time as mean_exec_time,'
          'st.stddev_time as stddev_exec_time,'
          'st.rows,'
          'st.shared_blks_hit,'
          'st.shared_blks_read,'
          'st.shared_blks_dirtied,'
          'st.shared_blks_written,'
          'st.local_blks_hit,'
          'st.local_blks_read,'
          'st.local_blks_dirtied,'
          'st.local_blks_written,'
          'st.temp_blks_read,'
          'st.temp_blks_written,'
          'st.blk_read_time,'
          'st.blk_write_time,'
          'NULL as wal_records,'
          'NULL as wal_fpi,'
          'NULL as wal_bytes, '
          'NULL as jit_functions, '
          'NULL as jit_generation_time, '
          'NULL as jit_inlining_count, '
          'NULL as jit_inlining_time, '
          'NULL as jit_optimization_count, '
          'NULL as jit_optimization_time, '
          'NULL as jit_emission_count, '
          'NULL as jit_emission_time '
        );
      WHEN '1.8' THEN
        st_query := replace(st_query, '{statements_fields}',
          'true as toplevel,'
          'st.plans,'
          'st.total_plan_time,'
          'st.min_plan_time,'
          'st.max_plan_time,'
          'st.mean_plan_time,'
          'st.stddev_plan_time,'
          'st.calls,'
          'st.total_exec_time,'
          'st.min_exec_time,'
          'st.max_exec_time,'
          'st.mean_exec_time,'
          'st.stddev_exec_time,'
          'st.rows,'
          'st.shared_blks_hit,'
          'st.shared_blks_read,'
          'st.shared_blks_dirtied,'
          'st.shared_blks_written,'
          'st.local_blks_hit,'
          'st.local_blks_read,'
          'st.local_blks_dirtied,'
          'st.local_blks_written,'
          'st.temp_blks_read,'
          'st.temp_blks_written,'
          'st.blk_read_time,'
          'st.blk_write_time,'
          'st.wal_records,'
          'st.wal_fpi,'
          'st.wal_bytes, '
          'NULL as jit_functions, '
          'NULL as jit_generation_time, '
          'NULL as jit_inlining_count, '
          'NULL as jit_inlining_time, '
          'NULL as jit_optimization_count, '
          'NULL as jit_optimization_time, '
          'NULL as jit_emission_count, '
          'NULL as jit_emission_time '
        );
      WHEN '1.9' THEN
        st_query := replace(st_query, '{statements_fields}',
          'st.toplevel,'
          'st.plans,'
          'st.total_plan_time,'
          'st.min_plan_time,'
          'st.max_plan_time,'
          'st.mean_plan_time,'
          'st.stddev_plan_time,'
          'st.calls,'
          'st.total_exec_time,'
          'st.min_exec_time,'
          'st.max_exec_time,'
          'st.mean_exec_time,'
          'st.stddev_exec_time,'
          'st.rows,'
          'st.shared_blks_hit,'
          'st.shared_blks_read,'
          'st.shared_blks_dirtied,'
          'st.shared_blks_written,'
          'st.local_blks_hit,'
          'st.local_blks_read,'
          'st.local_blks_dirtied,'
          'st.local_blks_written,'
          'st.temp_blks_read,'
          'st.temp_blks_written,'
          'st.blk_read_time,'
          'st.blk_write_time,'
          'st.wal_records,'
          'st.wal_fpi,'
          'st.wal_bytes, '
          'NULL as jit_functions, '
          'NULL as jit_generation_time, '
          'NULL as jit_inlining_count, '
          'NULL as jit_inlining_time, '
          'NULL as jit_optimization_count, '
          'NULL as jit_optimization_time, '
          'NULL as jit_emission_count, '
          'NULL as jit_emission_time '
        );
      WHEN '1.10' THEN
        st_query := replace(st_query, '{statements_fields}',
          'st.toplevel,'
          'st.plans,'
          'st.total_plan_time,'
          'st.min_plan_time,'
          'st.max_plan_time,'
          'st.mean_plan_time,'
          'st.stddev_plan_time,'
          'st.calls,'
          'st.total_exec_time,'
          'st.min_exec_time,'
          'st.max_exec_time,'
          'st.mean_exec_time,'
          'st.stddev_exec_time,'
          'st.rows,'
          'st.shared_blks_hit,'
          'st.shared_blks_read,'
          'st.shared_blks_dirtied,'
          'st.shared_blks_written,'
          'st.local_blks_hit,'
          'st.local_blks_read,'
          'st.local_blks_dirtied,'
          'st.local_blks_written,'
          'st.temp_blks_read,'
          'st.temp_blks_written,'
          'st.blk_read_time,'
          'st.blk_write_time,'
          'st.wal_records,'
          'st.wal_fpi,'
          'st.wal_bytes, '
          'st.jit_functions, '
          'st.jit_generation_time, '
          'st.jit_inlining_count, '
          'st.jit_inlining_time, '
          'st.jit_optimization_count, '
          'st.jit_optimization_time, '
          'st.jit_emission_count, '
          'st.jit_emission_time '
        );
      ELSE
        RAISE 'Unsupported pg_stat_statements extension version.';
    END CASE; -- pg_stat_statememts versions

    -- Get statements data
    INSERT INTO last_stat_statements (
        server_id,
        sample_id,
        userid,
        username,
        datid,
        queryid,
        plans,
        total_plan_time,
        min_plan_time,
        max_plan_time,
        mean_plan_time,
        stddev_plan_time,
        calls,
        total_exec_time,
        min_exec_time,
        max_exec_time,
        mean_exec_time,
        stddev_exec_time,
        rows,
        shared_blks_hit,
        shared_blks_read,
        shared_blks_dirtied,
        shared_blks_written,
        local_blks_hit,
        local_blks_read,
        local_blks_dirtied,
        local_blks_written,
        temp_blks_read,
        temp_blks_written,
        blk_read_time,
        blk_write_time,
        wal_records,
        wal_fpi,
        wal_bytes,
        toplevel,
        in_sample,
        jit_functions,
        jit_generation_time,
        jit_inlining_count,
        jit_inlining_time,
        jit_optimization_count,
        jit_optimization_time,
        jit_emission_count,
        jit_emission_time
      )
    SELECT
      sserver_id,
      s_id,
      dbl.userid,
      dbl.username,
      dbl.datid,
      dbl.queryid,
      dbl.plans,
      dbl.total_plan_time,
      dbl.min_plan_time,
      dbl.max_plan_time,
      dbl.mean_plan_time,
      dbl.stddev_plan_time,
      dbl.calls,
      dbl.total_exec_time,
      dbl.min_exec_time,
      dbl.max_exec_time,
      dbl.mean_exec_time,
      dbl.stddev_exec_time,
      dbl.rows,
      dbl.shared_blks_hit,
      dbl.shared_blks_read,
      dbl.shared_blks_dirtied,
      dbl.shared_blks_written,
      dbl.local_blks_hit,
      dbl.local_blks_read,
      dbl.local_blks_dirtied,
      dbl.local_blks_written,
      dbl.temp_blks_read,
      dbl.temp_blks_written,
      dbl.blk_read_time,
      dbl.blk_write_time,
      dbl.wal_records,
      dbl.wal_fpi,
      dbl.wal_bytes,
      dbl.toplevel,
      false,
      dbl.jit_functions,
      dbl.jit_generation_time,
      dbl.jit_inlining_count,
      dbl.jit_inlining_time,
      dbl.jit_optimization_count,
      dbl.jit_optimization_time,
      dbl.jit_emission_count,
      dbl.jit_emission_time
    FROM dblink('server_connection',st_query)
    AS dbl (
      -- pg_stat_statements fields
        userid              oid,
        username            name,
        datid               oid,
        queryid             bigint,
        toplevel            boolean,
        plans               bigint,
        total_plan_time     double precision,
        min_plan_time       double precision,
        max_plan_time       double precision,
        mean_plan_time      double precision,
        stddev_plan_time    double precision,
        calls               bigint,
        total_exec_time     double precision,
        min_exec_time       double precision,
        max_exec_time       double precision,
        mean_exec_time      double precision,
        stddev_exec_time    double precision,
        rows                bigint,
        shared_blks_hit     bigint,
        shared_blks_read    bigint,
        shared_blks_dirtied bigint,
        shared_blks_written bigint,
        local_blks_hit      bigint,
        local_blks_read     bigint,
        local_blks_dirtied  bigint,
        local_blks_written  bigint,
        temp_blks_read      bigint,
        temp_blks_written   bigint,
        blk_read_time       double precision,
        blk_write_time      double precision,
        wal_records         bigint,
        wal_fpi             bigint,
        wal_bytes           numeric,
        jit_functions       bigint,
        jit_generation_time double precision,
        jit_inlining_count  bigint,
        jit_inlining_time   double precision,
        jit_optimization_count  bigint,
        jit_optimization_time   double precision,
        jit_emission_count  bigint,
        jit_emission_time   double precision
      );
    EXECUTE format('ANALYZE last_stat_statements_srv%1$s',
      sserver_id);

    -- Rusage data collection when available
    IF
      (
        SELECT count(*) = 1
        FROM jsonb_to_recordset(properties #> '{extensions}') AS ext(extname text)
        WHERE extname = 'pg_stat_kcache'
      )
    THEN
      -- Dynamic rusage query
      st_query := format(
        'SELECT '
          'kc.userid,'
          'kc.dbid,'
          'kc.queryid,'
          '{kcache_fields} '
        'FROM '
          '{kcache_view} kc '
      );

      st_query := replace(st_query, '{kcache_view}',
        format('%1$I.pg_stat_kcache()',
          (
            SELECT extnamespace FROM jsonb_to_recordset(properties #> '{extensions}')
              AS x(extname text, extnamespace text)
            WHERE extname = 'pg_stat_kcache'
          )
        )
      );

      CASE -- pg_stat_kcache versions
        (
          SELECT extversion
          FROM jsonb_to_recordset(properties #> '{extensions}')
            AS x(extname text, extversion text)
          WHERE extname = 'pg_stat_kcache'
        )
        -- pg_stat_kcache v.2.1.0 - 2.1.3
        WHEN '2.1.0','2.1.1','2.1.2','2.1.3' THEN
          st_query := replace(st_query, '{kcache_fields}',
            'true as toplevel,'
            'NULL as plan_user_time,'
            'NULL as plan_system_time,'
            'NULL as plan_minflts,'
            'NULL as plan_majflts,'
            'NULL as plan_nswaps,'
            'NULL as plan_reads,'
            'NULL as plan_writes,'
            'NULL as plan_msgsnds,'
            'NULL as plan_msgrcvs,'
            'NULL as plan_nsignals,'
            'NULL as plan_nvcsws,'
            'NULL as plan_nivcsws,'
            'kc.user_time as exec_user_time,'
            'kc.system_time as exec_system_time,'
            'kc.minflts as exec_minflts,'
            'kc.majflts as exec_majflts,'
            'kc.nswaps as exec_nswaps,'
            'kc.reads as exec_reads,'
            'kc.writes as exec_writes,'
            'kc.msgsnds as exec_msgsnds,'
            'kc.msgrcvs as exec_msgrcvs,'
            'kc.nsignals as exec_nsignals,'
            'kc.nvcsws as exec_nvcsws,'
            'kc.nivcsws as exec_nivcsws '
          );
        -- pg_stat_kcache v.2.2.0, 2.2.1
        WHEN '2.2.0', '2.2.1' THEN
          st_query := replace(st_query, '{kcache_fields}',
            'kc.top as toplevel,'
            'kc.plan_user_time as plan_user_time,'
            'kc.plan_system_time as plan_system_time,'
            'kc.plan_minflts as plan_minflts,'
            'kc.plan_majflts as plan_majflts,'
            'kc.plan_nswaps as plan_nswaps,'
            'kc.plan_reads as plan_reads,'
            'kc.plan_writes as plan_writes,'
            'kc.plan_msgsnds as plan_msgsnds,'
            'kc.plan_msgrcvs as plan_msgrcvs,'
            'kc.plan_nsignals as plan_nsignals,'
            'kc.plan_nvcsws as plan_nvcsws,'
            'kc.plan_nivcsws as plan_nivcsws,'
            'kc.exec_user_time as exec_user_time,'
            'kc.exec_system_time as exec_system_time,'
            'kc.exec_minflts as exec_minflts,'
            'kc.exec_majflts as exec_majflts,'
            'kc.exec_nswaps as exec_nswaps,'
            'kc.exec_reads as exec_reads,'
            'kc.exec_writes as exec_writes,'
            'kc.exec_msgsnds as exec_msgsnds,'
            'kc.exec_msgrcvs as exec_msgrcvs,'
            'kc.exec_nsignals as exec_nsignals,'
            'kc.exec_nvcsws as exec_nvcsws,'
            'kc.exec_nivcsws as exec_nivcsws '
          );
        ELSE
          st_query := NULL;
      END CASE; -- pg_stat_kcache versions

      IF st_query IS NOT NULL THEN
        INSERT INTO last_stat_kcache(
          server_id,
          sample_id,
          userid,
          datid,
          toplevel,
          queryid,
          plan_user_time,
          plan_system_time,
          plan_minflts,
          plan_majflts,
          plan_nswaps,
          plan_reads,
          plan_writes,
          plan_msgsnds,
          plan_msgrcvs,
          plan_nsignals,
          plan_nvcsws,
          plan_nivcsws,
          exec_user_time,
          exec_system_time,
          exec_minflts,
          exec_majflts,
          exec_nswaps,
          exec_reads,
          exec_writes,
          exec_msgsnds,
          exec_msgrcvs,
          exec_nsignals,
          exec_nvcsws,
          exec_nivcsws
        )
        SELECT
          sserver_id,
          s_id,
          dbl.userid,
          dbl.datid,
          dbl.toplevel,
          dbl.queryid,
          dbl.plan_user_time  AS plan_user_time,
          dbl.plan_system_time  AS plan_system_time,
          dbl.plan_minflts  AS plan_minflts,
          dbl.plan_majflts  AS plan_majflts,
          dbl.plan_nswaps  AS plan_nswaps,
          dbl.plan_reads  AS plan_reads,
          dbl.plan_writes  AS plan_writes,
          dbl.plan_msgsnds  AS plan_msgsnds,
          dbl.plan_msgrcvs  AS plan_msgrcvs,
          dbl.plan_nsignals  AS plan_nsignals,
          dbl.plan_nvcsws  AS plan_nvcsws,
          dbl.plan_nivcsws  AS plan_nivcsws,
          dbl.exec_user_time  AS exec_user_time,
          dbl.exec_system_time  AS exec_system_time,
          dbl.exec_minflts  AS exec_minflts,
          dbl.exec_majflts  AS exec_majflts,
          dbl.exec_nswaps  AS exec_nswaps,
          dbl.exec_reads  AS exec_reads,
          dbl.exec_writes  AS exec_writes,
          dbl.exec_msgsnds  AS exec_msgsnds,
          dbl.exec_msgrcvs  AS exec_msgrcvs,
          dbl.exec_nsignals  AS exec_nsignals,
          dbl.exec_nvcsws  AS exec_nvcsws,
          dbl.exec_nivcsws  AS exec_nivcsws
        FROM dblink('server_connection',st_query)
        AS dbl (
          userid            oid,
          datid             oid,
          queryid           bigint,
          toplevel          boolean,
          plan_user_time    double precision,
          plan_system_time  double precision,
          plan_minflts      bigint,
          plan_majflts      bigint,
          plan_nswaps       bigint,
          plan_reads        bigint,
          plan_writes       bigint,
          plan_msgsnds      bigint,
          plan_msgrcvs      bigint,
          plan_nsignals     bigint,
          plan_nvcsws       bigint,
          plan_nivcsws      bigint,
          exec_user_time    double precision,
          exec_system_time  double precision,
          exec_minflts      bigint,
          exec_majflts      bigint,
          exec_nswaps       bigint,
          exec_reads        bigint,
          exec_writes       bigint,
          exec_msgsnds      bigint,
          exec_msgrcvs      bigint,
          exec_nsignals     bigint,
          exec_nvcsws       bigint,
          exec_nivcsws      bigint
        ) JOIN last_stat_statements lss USING (userid, datid, queryid, toplevel)
        WHERE
          (lss.server_id, lss.sample_id) = (sserver_id, s_id);
        EXECUTE format('ANALYZE last_stat_kcache_srv%1$s',
          sserver_id);
      END IF; -- st_query is not null
    END IF; -- pg_stat_kcache extension is available

    PERFORM mark_pg_stat_statements(sserver_id, s_id, topn);

    -- Get queries texts
    CASE (
        SELECT extversion
        FROM jsonb_to_recordset(properties #> '{extensions}')
          AS ext(extname text, extversion text)
        WHERE extname = 'pg_stat_statements'
      )
      WHEN '1.3','1.4','1.5','1.6','1.7','1.8' THEN
        st_query :=
          'SELECT userid, dbid, true AS toplevel, queryid, '||
          $o$regexp_replace(query,$i$\s+$i$,$i$ $i$,$i$g$i$) AS query $o$ ||
          'FROM %1$I.pg_stat_statements(true) '
          'WHERE queryid IN (%s)';
      WHEN '1.9', '1.10' THEN
        st_query :=
          'SELECT userid, dbid, toplevel, queryid, '||
          $o$regexp_replace(query,$i$\s+$i$,$i$ $i$,$i$g$i$) AS query $o$ ||
          'FROM %1$I.pg_stat_statements(true) '
          'WHERE queryid IN (%s)';
      ELSE
        RAISE 'Unsupported pg_stat_statements extension version.';
    END CASE;

    -- Substitute pg_stat_statements extension schema and queries list
    st_query := format(st_query,
        (
          SELECT extnamespace FROM jsonb_to_recordset(properties #> '{extensions}')
            AS x(extname text, extnamespace text)
          WHERE extname = 'pg_stat_statements'
        ),
        (
          SELECT string_agg(queryid::text,',')
          FROM last_stat_statements
          WHERE
            (server_id, sample_id, in_sample) =
            (sserver_id, s_id, true)
        )
    );

    -- Now we can save statement
    FOR qres IN (
      SELECT
        userid,
        datid,
        toplevel,
        queryid,
        query
      FROM dblink('server_connection',st_query) AS
        dbl(
            userid    oid,
            datid     oid,
            toplevel  boolean,
            queryid   bigint,
            query     text
          )
        JOIN last_stat_statements lst USING (userid, datid, toplevel, queryid)
      WHERE
        (lst.server_id, lst.sample_id, lst.in_sample) =
        (sserver_id, s_id, true)
    )
    LOOP
      -- statement texts
      INSERT INTO stmt_list AS isl (
          server_id,
          last_sample_id,
          queryid_md5,
          query
        )
      VALUES (
          sserver_id,
          NULL,
          md5(qres.query),
          qres.query
        )
      ON CONFLICT ON CONSTRAINT pk_stmt_list
      DO UPDATE SET last_sample_id = NULL
      WHERE
        isl.last_sample_id IS NOT NULL;

      -- bind queryid to queryid_md5 for this sample
      -- different text queries can have the same queryid
      -- between samples
      UPDATE last_stat_statements SET queryid_md5 = md5(qres.query)
      WHERE (server_id, sample_id, userid, datid, toplevel, queryid) =
        (sserver_id, s_id, qres.userid, qres.datid, qres.toplevel, qres.queryid);
    END LOOP; -- over sample statements

    -- Flushing pg_stat_kcache
    CASE (
        SELECT extversion FROM jsonb_to_recordset(properties #> '{extensions}')
          AS x(extname text, extversion text)
        WHERE extname = 'pg_stat_kcache'
    )
      WHEN '2.1.0','2.1.1','2.1.2','2.1.3','2.2.0','2.2.1' THEN
        SELECT * INTO qres FROM dblink('server_connection',
          format('SELECT %1$I.pg_stat_kcache_reset()',
            (
              SELECT extnamespace FROM jsonb_to_recordset(properties #> '{extensions}')
                AS x(extname text, extnamespace text)
              WHERE extname = 'pg_stat_kcache'
            )
          )
        ) AS t(res char(1));
      ELSE
        NULL;
    END CASE;

    -- Flushing statements
    CASE (
        SELECT extversion
        FROM jsonb_to_recordset(properties #> '{extensions}')
          AS ext(extname text, extversion text)
        WHERE extname = 'pg_stat_statements'
      )
      -- pg_stat_statements v 1.3-1.8
      WHEN '1.3','1.4','1.5','1.6','1.7','1.8','1.9','1.10' THEN
        SELECT * INTO qres FROM dblink('server_connection',
          format('SELECT %1$I.pg_stat_statements_reset()',
            (
              SELECT extnamespace FROM jsonb_to_recordset(properties #> '{extensions}')
                AS x(extname text, extnamespace text)
              WHERE extname = 'pg_stat_statements'
            )
          )
        ) AS t(res char(1));
      ELSE
        RAISE 'Unsupported pg_stat_statements version.';
    END CASE;

    -- Save the diffs in a sample
    PERFORM save_pg_stat_statements(sserver_id, s_id);
    -- Delete obsolete last_* data
    DELETE FROM last_stat_kcache WHERE server_id = sserver_id AND sample_id < s_id;
    DELETE FROM last_stat_statements WHERE server_id = sserver_id AND sample_id < s_id;
END;
$$ LANGUAGE plpgsql;
CREATE FUNCTION save_pg_stat_statements(IN sserver_id integer, IN s_id integer)
RETURNS void
SET search_path=@extschema@ AS $$
  -- This function performs save marked statements data in sample tables
  -- User names
  INSERT INTO roles_list AS irl (
    server_id,
    last_sample_id,
    userid,
    username
  )
  SELECT DISTINCT
    sserver_id,
    NULL::integer,
    st.userid,
    COALESCE(st.username, '_unknown_')
  FROM
    last_stat_statements st
  WHERE (st.server_id, st.sample_id, in_sample) = (sserver_id, s_id, true)
  ON CONFLICT ON CONSTRAINT pk_roles_list
  DO UPDATE SET
    (last_sample_id, username) =
    (EXCLUDED.last_sample_id, EXCLUDED.username)
  WHERE
    (irl.last_sample_id, irl.username) IS DISTINCT FROM
    (EXCLUDED.last_sample_id, EXCLUDED.username)
  ;

  -- Statement stats
  INSERT INTO sample_statements(
    server_id,
    sample_id,
    userid,
    datid,
    toplevel,
    queryid,
    queryid_md5,
    plans,
    total_plan_time,
    min_plan_time,
    max_plan_time,
    mean_plan_time,
    stddev_plan_time,
    calls,
    total_exec_time,
    min_exec_time,
    max_exec_time,
    mean_exec_time,
    stddev_exec_time,
    rows,
    shared_blks_hit,
    shared_blks_read,
    shared_blks_dirtied,
    shared_blks_written,
    local_blks_hit,
    local_blks_read,
    local_blks_dirtied,
    local_blks_written,
    temp_blks_read,
    temp_blks_written,
    blk_read_time,
    blk_write_time,
    wal_records,
    wal_fpi,
    wal_bytes,
    jit_functions,
    jit_generation_time,
    jit_inlining_count,
    jit_inlining_time,
    jit_optimization_count,
    jit_optimization_time,
    jit_emission_count,
    jit_emission_time
  )
  SELECT
    sserver_id,
    s_id,
    userid,
    datid,
    toplevel,
    queryid,
    queryid_md5,
    plans,
    total_plan_time,
    min_plan_time,
    max_plan_time,
    mean_plan_time,
    stddev_plan_time,
    calls,
    total_exec_time,
    min_exec_time,
    max_exec_time,
    mean_exec_time,
    stddev_exec_time,
    rows,
    shared_blks_hit,
    shared_blks_read,
    shared_blks_dirtied,
    shared_blks_written,
    local_blks_hit,
    local_blks_read,
    local_blks_dirtied,
    local_blks_written,
    temp_blks_read,
    temp_blks_written,
    blk_read_time,
    blk_write_time,
    wal_records,
    wal_fpi,
    wal_bytes,
    jit_functions,
    jit_generation_time,
    jit_inlining_count,
    jit_inlining_time,
    jit_optimization_count,
    jit_optimization_time,
    jit_emission_count,
    jit_emission_time
  FROM
    last_stat_statements JOIN stmt_list USING (server_id, queryid_md5)
  WHERE
    (server_id, sample_id, in_sample) = (sserver_id, s_id, true);

  /*
  * Aggregated statements stats
  */
  INSERT INTO sample_statements_total(
    server_id,
    sample_id,
    datid,
    plans,
    total_plan_time,
    calls,
    total_exec_time,
    rows,
    shared_blks_hit,
    shared_blks_read,
    shared_blks_dirtied,
    shared_blks_written,
    local_blks_hit,
    local_blks_read,
    local_blks_dirtied,
    local_blks_written,
    temp_blks_read,
    temp_blks_written,
    blk_read_time,
    blk_write_time,
    wal_records,
    wal_fpi,
    wal_bytes,
    statements,
    jit_functions,
    jit_generation_time,
    jit_inlining_count,
    jit_inlining_time,
    jit_optimization_count,
    jit_optimization_time,
    jit_emission_count,
    jit_emission_time
  )
  SELECT
    server_id,
    sample_id,
    datid,
    sum(lss.plans),
    sum(lss.total_plan_time),
    sum(lss.calls),
    sum(lss.total_exec_time),
    sum(lss.rows),
    sum(lss.shared_blks_hit),
    sum(lss.shared_blks_read),
    sum(lss.shared_blks_dirtied),
    sum(lss.shared_blks_written),
    sum(lss.local_blks_hit),
    sum(lss.local_blks_read),
    sum(lss.local_blks_dirtied),
    sum(lss.local_blks_written),
    sum(lss.temp_blks_read),
    sum(lss.temp_blks_written),
    sum(lss.blk_read_time),
    sum(lss.blk_write_time),
    sum(lss.wal_records),
    sum(lss.wal_fpi),
    sum(lss.wal_bytes),
    count(*),
    sum(lss.jit_functions),
    sum(lss.jit_generation_time),
    sum(lss.jit_inlining_count),
    sum(lss.jit_inlining_time),
    sum(lss.jit_optimization_count),
    sum(lss.jit_optimization_time),
    sum(lss.jit_emission_count),
    sum(lss.jit_emission_time)
  FROM
    last_stat_statements lss
    -- In case of already dropped database
    JOIN sample_stat_database ssd USING (server_id, sample_id, datid)
  WHERE
    (server_id, sample_id) = (sserver_id, s_id)
  GROUP BY
    server_id,
    sample_id,
    datid
  ;

  /*
  * If rusage data is available we should just save it in sample for saved
  * statements
  */
  INSERT INTO sample_kcache (
      server_id,
      sample_id,
      userid,
      datid,
      queryid,
      queryid_md5,
      plan_user_time,
      plan_system_time,
      plan_minflts,
      plan_majflts,
      plan_nswaps,
      plan_reads,
      plan_writes,
      plan_msgsnds,
      plan_msgrcvs,
      plan_nsignals,
      plan_nvcsws,
      plan_nivcsws,
      exec_user_time,
      exec_system_time,
      exec_minflts,
      exec_majflts,
      exec_nswaps,
      exec_reads,
      exec_writes,
      exec_msgsnds,
      exec_msgrcvs,
      exec_nsignals,
      exec_nvcsws,
      exec_nivcsws,
      toplevel
  )
  SELECT
    cur.server_id,
    cur.sample_id,
    cur.userid,
    cur.datid,
    cur.queryid,
    sst.queryid_md5,
    cur.plan_user_time,
    cur.plan_system_time,
    cur.plan_minflts,
    cur.plan_majflts,
    cur.plan_nswaps,
    cur.plan_reads,
    cur.plan_writes,
    cur.plan_msgsnds,
    cur.plan_msgrcvs,
    cur.plan_nsignals,
    cur.plan_nvcsws,
    cur.plan_nivcsws,
    cur.exec_user_time,
    cur.exec_system_time,
    cur.exec_minflts,
    cur.exec_majflts,
    cur.exec_nswaps,
    cur.exec_reads,
    cur.exec_writes,
    cur.exec_msgsnds,
    cur.exec_msgrcvs,
    cur.exec_nsignals,
    cur.exec_nvcsws,
    cur.exec_nivcsws,
    cur.toplevel
  FROM
    last_stat_kcache cur JOIN last_stat_statements sst ON
      (sst.server_id, sst.sample_id, sst.userid, sst.datid, sst.queryid, sst.toplevel) =
      (cur.server_id, cur.sample_id, cur.userid, cur.datid, cur.queryid, cur.toplevel)
  WHERE
    (cur.server_id, cur.sample_id, sst.in_sample) = (sserver_id, s_id, true);

  -- Aggregated pg_stat_kcache data
  INSERT INTO sample_kcache_total(
    server_id,
    sample_id,
    datid,
    plan_user_time,
    plan_system_time,
    plan_minflts,
    plan_majflts,
    plan_nswaps,
    plan_reads,
    plan_writes,
    plan_msgsnds,
    plan_msgrcvs,
    plan_nsignals,
    plan_nvcsws,
    plan_nivcsws,
    exec_user_time,
    exec_system_time,
    exec_minflts,
    exec_majflts,
    exec_nswaps,
    exec_reads,
    exec_writes,
    exec_msgsnds,
    exec_msgrcvs,
    exec_nsignals,
    exec_nvcsws,
    exec_nivcsws,
    statements
  )
  SELECT
    cur.server_id,
    cur.sample_id,
    cur.datid,
    sum(plan_user_time),
    sum(plan_system_time),
    sum(plan_minflts),
    sum(plan_majflts),
    sum(plan_nswaps),
    sum(plan_reads),
    sum(plan_writes),
    sum(plan_msgsnds),
    sum(plan_msgrcvs),
    sum(plan_nsignals),
    sum(plan_nvcsws),
    sum(plan_nivcsws),
    sum(exec_user_time),
    sum(exec_system_time),
    sum(exec_minflts),
    sum(exec_majflts),
    sum(exec_nswaps),
    sum(exec_reads),
    sum(exec_writes),
    sum(exec_msgsnds),
    sum(exec_msgrcvs),
    sum(exec_nsignals),
    sum(exec_nvcsws),
    sum(exec_nivcsws),
    count(*)
  FROM
    last_stat_kcache cur
    -- In case of already dropped database
    JOIN sample_stat_database db USING (server_id, sample_id, datid)
  WHERE
    (cur.server_id, cur.sample_id) = (sserver_id, s_id) AND
    toplevel
  GROUP BY
    server_id,
    sample_id,
    datid
  ;
$$ LANGUAGE sql;
CREATE FUNCTION take_sample(IN sserver_id integer, IN skip_sizes boolean
) RETURNS integer SET search_path=@extschema@ AS $$
DECLARE
    s_id              integer;
    topn              integer;
    ret               integer;
    server_properties jsonb = '{"extensions":[],"settings":[],"timings":{},"properties":{}}'; -- version, extensions, etc.
    qres              record;
    settings_refresh  boolean = true;
    collect_timings   boolean = false;

    server_query      text;
BEGIN
    -- Get server connstr
    SELECT properties INTO server_properties FROM get_connstr(sserver_id, server_properties);

    -- Getting timing collection setting
    BEGIN
        collect_timings := current_setting('pg_profile.track_sample_timings')::boolean;
    EXCEPTION
        WHEN OTHERS THEN collect_timings := false;
    END;

    server_properties := jsonb_set(server_properties,'{collect_timings}',to_jsonb(collect_timings));

    -- Getting TopN setting
    BEGIN
        topn := current_setting('pg_profile.topn')::integer;
    EXCEPTION
        WHEN OTHERS THEN topn := 20;
    END;


    -- Adding dblink extension schema to search_path if it does not already there
    IF (SELECT count(*) = 0 FROM pg_catalog.pg_extension WHERE extname = 'dblink') THEN
      RAISE 'dblink extension must be installed';
    END IF;
    SELECT extnamespace::regnamespace AS dblink_schema INTO STRICT qres FROM pg_catalog.pg_extension WHERE extname = 'dblink';
    IF NOT string_to_array(current_setting('search_path'),',') @> ARRAY[qres.dblink_schema::text] THEN
      EXECUTE 'SET LOCAL search_path TO ' || current_setting('search_path')||','|| qres.dblink_schema;
    END IF;

    IF dblink_get_connections() @> ARRAY['server_connection'] THEN
        PERFORM dblink_disconnect('server_connection');
    END IF;

    -- Only one running take_sample() function allowed per server!
    -- Explicitly lock server in servers table
    BEGIN
        SELECT * INTO qres FROM servers WHERE server_id = sserver_id FOR UPDATE NOWAIT;
    EXCEPTION
        WHEN OTHERS THEN RAISE 'Can''t get lock on server. Is there another take_sample() function running on this server?';
    END;

    -- Creating a new sample record
    UPDATE servers SET last_sample_id = last_sample_id + 1 WHERE server_id = sserver_id
      RETURNING last_sample_id INTO s_id;
    INSERT INTO samples(sample_time,server_id,sample_id)
      VALUES (now(),sserver_id,s_id);

    -- Getting max_sample_age setting
    BEGIN
        ret := COALESCE(current_setting('pg_profile.max_sample_age')::integer);
    EXCEPTION
        WHEN OTHERS THEN ret := 7;
    END;
    -- Applying skip sizes policy
    IF skip_sizes IS NULL THEN
      IF num_nulls(qres.size_smp_wnd_start, qres.size_smp_wnd_dur, qres.size_smp_interval) > 0 THEN
        skip_sizes := false;
      ELSE
        /*
        Skip sizes collection if there was a sample with sizes recently
        or if we are not in size collection time window
        */
        SELECT
          count(*) > 0 OR
          NOT
          CASE WHEN timezone('UTC',current_time) > timezone('UTC',qres.size_smp_wnd_start) THEN
            timezone('UTC',now()) <=
            timezone('UTC',now())::date +
            timezone('UTC',qres.size_smp_wnd_start) +
            qres.size_smp_wnd_dur
          ELSE
            timezone('UTC',now()) <=
            timezone('UTC',now() - interval '1 day')::date +
            timezone('UTC',qres.size_smp_wnd_start) +
            qres.size_smp_wnd_dur
          END
            INTO STRICT skip_sizes
        FROM
          sample_stat_tables_total st
          JOIN samples s USING (server_id, sample_id)
        WHERE
          server_id = sserver_id
          AND st.relsize_diff IS NOT NULL
          AND sample_time > now() - qres.size_smp_interval;
      END IF;
    END IF;

    IF (server_properties #>> '{collect_timings}')::boolean THEN
      server_properties := jsonb_set(server_properties,'{timings,connect}',jsonb_build_object('start',clock_timestamp()));
      server_properties := jsonb_set(server_properties,'{timings,total}',jsonb_build_object('start',clock_timestamp()));
    END IF;

    -- Server connection
    PERFORM dblink_connect('server_connection', server_properties #>> '{properties,server_connstr}');
    -- Transaction
    PERFORM dblink('server_connection','BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY');
    -- Setting application name
    PERFORM dblink('server_connection','SET application_name=''pg_profile''');
    -- Setting lock_timout prevents hanging of take_sample() call due to DDL in long transaction
    PERFORM dblink('server_connection','SET lock_timeout=3000');
    -- Reset search_path for security reasons
    PERFORM dblink('server_connection','SET search_path=''''');

    IF (server_properties #>> '{collect_timings}')::boolean THEN
      server_properties := jsonb_set(server_properties,'{timings,connect,end}',to_jsonb(clock_timestamp()));
      server_properties := jsonb_set(server_properties,'{timings,get server environment}',jsonb_build_object('start',clock_timestamp()));
    END IF;
    -- Get settings values for the server
    FOR qres IN
      SELECT * FROM dblink('server_connection',
          'SELECT name, '
          'reset_val, '
          'unit, '
          'pending_restart '
          'FROM pg_catalog.pg_settings '
          'WHERE name IN ('
            '''server_version_num'''
          ')')
        AS dbl(name text, reset_val text, unit text, pending_restart boolean)
    LOOP
      server_properties := jsonb_insert(server_properties,'{"settings",0}',to_jsonb(qres));
    END LOOP;

    -- Get extensions, that we need to perform statements stats collection
    FOR qres IN
      SELECT * FROM dblink('server_connection',
          'SELECT extname, '
          'extnamespace::regnamespace::name AS extnamespace, '
          'extversion '
          'FROM pg_catalog.pg_extension '
          'WHERE extname IN ('
            '''pg_stat_statements'','
            '''pg_wait_sampling'','
            '''pg_stat_kcache'''
          ')')
        AS dbl(extname name, extnamespace name, extversion text)
    LOOP
      server_properties := jsonb_insert(server_properties,'{"extensions",0}',to_jsonb(qres));
    END LOOP;

    -- Collecting postgres parameters
    /* We might refresh all parameters if version() was changed
    * This is needed for deleting obsolete parameters, not appearing in new
    * Postgres version.
    */
    SELECT ss.setting != dblver.version INTO settings_refresh
    FROM v_sample_settings ss, dblink('server_connection','SELECT version() as version') AS dblver (version text)
    WHERE ss.server_id = sserver_id AND ss.sample_id = s_id AND ss.name='version' AND ss.setting_scope = 2;
    settings_refresh := COALESCE(settings_refresh,true);

    -- Constructing server sql query for settings
    server_query := 'SELECT 1 as setting_scope,name,setting,reset_val,boot_val,unit,sourcefile,sourceline,pending_restart '
      'FROM pg_catalog.pg_settings '
      'UNION ALL SELECT 2 as setting_scope,''version'',version(),version(),NULL,NULL,NULL,NULL,False '
      'UNION ALL SELECT 2 as setting_scope,''pg_postmaster_start_time'','
      'pg_catalog.pg_postmaster_start_time()::text,'
      'pg_catalog.pg_postmaster_start_time()::text,NULL,NULL,NULL,NULL,False '
      'UNION ALL SELECT 2 as setting_scope,''pg_conf_load_time'','
      'pg_catalog.pg_conf_load_time()::text,pg_catalog.pg_conf_load_time()::text,NULL,NULL,NULL,NULL,False '
      'UNION ALL SELECT 2 as setting_scope,''system_identifier'','
      'system_identifier::text,system_identifier::text,system_identifier::text,'
      'NULL,NULL,NULL,False FROM pg_catalog.pg_control_system()';

    INSERT INTO sample_settings(
      server_id,
      first_seen,
      setting_scope,
      name,
      setting,
      reset_val,
      boot_val,
      unit,
      sourcefile,
      sourceline,
      pending_restart
    )
    SELECT
      s.server_id as server_id,
      s.sample_time as first_seen,
      cur.setting_scope,
      cur.name,
      cur.setting,
      cur.reset_val,
      cur.boot_val,
      cur.unit,
      cur.sourcefile,
      cur.sourceline,
      cur.pending_restart
    FROM
      sample_settings lst JOIN (
        -- Getting last versions of settings
        SELECT server_id, name, max(first_seen) as first_seen
        FROM sample_settings
        WHERE server_id = sserver_id AND (
          NOT settings_refresh
          -- system identifier shouldn't have a duplicate in case of version change
          -- this breaks export/import procedures, as those are related to this ID
          OR name = 'system_identifier'
        )
        GROUP BY server_id, name
      ) lst_times
      USING (server_id, name, first_seen)
      -- Getting current settings values
      RIGHT OUTER JOIN dblink('server_connection',server_query
          ) AS cur (
            setting_scope smallint,
            name text,
            setting text,
            reset_val text,
            boot_val text,
            unit text,
            sourcefile text,
            sourceline integer,
            pending_restart boolean
          )
        USING (setting_scope, name)
      JOIN samples s ON (s.server_id = sserver_id AND s.sample_id = s_id)
    WHERE
      cur.reset_val IS NOT NULL AND (
        lst.name IS NULL
        OR cur.reset_val != lst.reset_val
        OR cur.pending_restart != lst.pending_restart
        OR lst.sourcefile != cur.sourcefile
        OR lst.sourceline != cur.sourceline
        OR lst.unit != cur.unit
      );

    -- Check system identifier change
    SELECT min(reset_val::bigint) != max(reset_val::bigint) AS sysid_changed INTO STRICT qres
    FROM sample_settings
    WHERE server_id = sserver_id AND name = 'system_identifier';
    IF qres.sysid_changed THEN
      RAISE 'Server system_identifier has changed! Ensure server connection string is correct. Consider creating a new server for this cluster.';
    END IF;

    -- for server named 'local' check system identifier match
    IF (SELECT
      count(*) > 0
    FROM servers srv
      JOIN sample_settings ss USING (server_id)
      CROSS JOIN pg_catalog.pg_control_system() cs
    WHERE server_id = sserver_id AND ss.name = 'system_identifier'
      AND srv.server_name = 'local' AND reset_val::bigint != system_identifier)
    THEN
      RAISE 'Local system_identifier does not match with server specified by connection string of "local" server';
    END IF;

    INSERT INTO sample_settings(
      server_id,
      first_seen,
      setting_scope,
      name,
      setting,
      reset_val,
      boot_val,
      unit,
      sourcefile,
      sourceline,
      pending_restart
    )
    SELECT
      s.server_id,
      s.sample_time,
      1 as setting_scope,
      'pg_profile.topn',
      topn,
      topn,
      topn,
      null,
      null,
      null,
      false
    FROM samples s LEFT OUTER JOIN  v_sample_settings prm ON
      (s.server_id = prm.server_id AND s.sample_id = prm.sample_id AND prm.name = 'pg_profile.topn' AND prm.setting_scope = 1)
    WHERE s.server_id = sserver_id AND s.sample_id = s_id AND (prm.setting IS NULL OR prm.setting::integer != topn);

    IF (server_properties #>> '{collect_timings}')::boolean THEN
      server_properties := jsonb_set(server_properties,'{timings,get server environment,end}',to_jsonb(clock_timestamp()));
      server_properties := jsonb_set(server_properties,'{timings,collect database stats}',jsonb_build_object('start',clock_timestamp()));
    END IF;

    -- Construct pg_stat_database query
    CASE
      WHEN (
        SELECT count(*) = 1 FROM jsonb_to_recordset(server_properties #> '{settings}')
          AS x(name text, reset_val text)
        WHERE name = 'server_version_num'
          AND reset_val::integer >= 140000
      )
      THEN
        server_query := 'SELECT '
            'dbs.datid, '
            'dbs.datname, '
            'dbs.xact_commit, '
            'dbs.xact_rollback, '
            'dbs.blks_read, '
            'dbs.blks_hit, '
            'dbs.tup_returned, '
            'dbs.tup_fetched, '
            'dbs.tup_inserted, '
            'dbs.tup_updated, '
            'dbs.tup_deleted, '
            'dbs.conflicts, '
            'dbs.temp_files, '
            'dbs.temp_bytes, '
            'dbs.deadlocks, '
            'dbs.checksum_failures, '
            'dbs.checksum_last_failure, '
            'dbs.blk_read_time, '
            'dbs.blk_write_time, '
            'dbs.session_time, '
            'dbs.active_time, '
            'dbs.idle_in_transaction_time, '
            'dbs.sessions, '
            'dbs.sessions_abandoned, '
            'dbs.sessions_fatal, '
            'dbs.sessions_killed, '
            'dbs.stats_reset, '
            'pg_database_size(dbs.datid) as datsize, '
            '0 as datsize_delta, '
            'db.datistemplate '
          'FROM pg_catalog.pg_stat_database dbs '
          'JOIN pg_catalog.pg_database db ON (dbs.datid = db.oid) '
          'WHERE dbs.datname IS NOT NULL';
      WHEN (
        SELECT count(*) = 1 FROM jsonb_to_recordset(server_properties #> '{settings}')
          AS x(name text, reset_val text)
        WHERE name = 'server_version_num'
          AND reset_val::integer >= 120000
      )
      THEN
        server_query := 'SELECT '
            'dbs.datid, '
            'dbs.datname, '
            'dbs.xact_commit, '
            'dbs.xact_rollback, '
            'dbs.blks_read, '
            'dbs.blks_hit, '
            'dbs.tup_returned, '
            'dbs.tup_fetched, '
            'dbs.tup_inserted, '
            'dbs.tup_updated, '
            'dbs.tup_deleted, '
            'dbs.conflicts, '
            'dbs.temp_files, '
            'dbs.temp_bytes, '
            'dbs.deadlocks, '
            'dbs.checksum_failures, '
            'dbs.checksum_last_failure, '
            'dbs.blk_read_time, '
            'dbs.blk_write_time, '
            'NULL as session_time, '
            'NULL as active_time, '
            'NULL as idle_in_transaction_time, '
            'NULL as sessions, '
            'NULL as sessions_abandoned, '
            'NULL as sessions_fatal, '
            'NULL as sessions_killed, '
            'dbs.stats_reset, '
            'pg_database_size(dbs.datid) as datsize, '
            '0 as datsize_delta, '
            'db.datistemplate '
          'FROM pg_catalog.pg_stat_database dbs '
          'JOIN pg_catalog.pg_database db ON (dbs.datid = db.oid) '
          'WHERE dbs.datname IS NOT NULL';
      WHEN (
        SELECT count(*) = 1 FROM jsonb_to_recordset(server_properties #> '{settings}')
          AS x(name text, reset_val text)
        WHERE name = 'server_version_num'
          AND reset_val::integer < 120000
      )
      THEN
        server_query := 'SELECT '
            'dbs.datid, '
            'dbs.datname, '
            'dbs.xact_commit, '
            'dbs.xact_rollback, '
            'dbs.blks_read, '
            'dbs.blks_hit, '
            'dbs.tup_returned, '
            'dbs.tup_fetched, '
            'dbs.tup_inserted, '
            'dbs.tup_updated, '
            'dbs.tup_deleted, '
            'dbs.conflicts, '
            'dbs.temp_files, '
            'dbs.temp_bytes, '
            'dbs.deadlocks, '
            'NULL as checksum_failures, '
            'NULL as checksum_last_failure, '
            'dbs.blk_read_time, '
            'dbs.blk_write_time, '
            'NULL as session_time, '
            'NULL as active_time, '
            'NULL as idle_in_transaction_time, '
            'NULL as sessions, '
            'NULL as sessions_abandoned, '
            'NULL as sessions_fatal, '
            'NULL as sessions_killed, '
            'dbs.stats_reset, '
            'pg_database_size(dbs.datid) as datsize, '
            '0 as datsize_delta, '
            'db.datistemplate '
          'FROM pg_catalog.pg_stat_database dbs '
          'JOIN pg_catalog.pg_database db ON (dbs.datid = db.oid) '
          'WHERE dbs.datname IS NOT NULL';
    END CASE;

    -- pg_stat_database data
    INSERT INTO last_stat_database (
        server_id,
        sample_id,
        datid,
        datname,
        xact_commit,
        xact_rollback,
        blks_read,
        blks_hit,
        tup_returned,
        tup_fetched,
        tup_inserted,
        tup_updated,
        tup_deleted,
        conflicts,
        temp_files,
        temp_bytes,
        deadlocks,
        checksum_failures,
        checksum_last_failure,
        blk_read_time,
        blk_write_time,
        session_time,
        active_time,
        idle_in_transaction_time,
        sessions,
        sessions_abandoned,
        sessions_fatal,
        sessions_killed,
        stats_reset,
        datsize,
        datsize_delta,
        datistemplate)
    SELECT
        sserver_id,
        s_id,
        datid,
        datname,
        xact_commit AS xact_commit,
        xact_rollback AS xact_rollback,
        blks_read AS blks_read,
        blks_hit AS blks_hit,
        tup_returned AS tup_returned,
        tup_fetched AS tup_fetched,
        tup_inserted AS tup_inserted,
        tup_updated AS tup_updated,
        tup_deleted AS tup_deleted,
        conflicts AS conflicts,
        temp_files AS temp_files,
        temp_bytes AS temp_bytes,
        deadlocks AS deadlocks,
        checksum_failures as checksum_failures,
        checksum_last_failure as checksum_failures,
        blk_read_time AS blk_read_time,
        blk_write_time AS blk_write_time,
        session_time AS session_time,
        active_time AS active_time,
        idle_in_transaction_time AS idle_in_transaction_time,
        sessions AS sessions,
        sessions_abandoned AS sessions_abandoned,
        sessions_fatal AS sessions_fatal,
        sessions_killed AS sessions_killed,
        stats_reset,
        datsize AS datsize,
        datsize_delta AS datsize_delta,
        datistemplate AS datistemplate
    FROM dblink('server_connection',server_query) AS rs (
        datid oid,
        datname name,
        xact_commit bigint,
        xact_rollback bigint,
        blks_read bigint,
        blks_hit bigint,
        tup_returned bigint,
        tup_fetched bigint,
        tup_inserted bigint,
        tup_updated bigint,
        tup_deleted bigint,
        conflicts bigint,
        temp_files bigint,
        temp_bytes bigint,
        deadlocks bigint,
        checksum_failures bigint,
        checksum_last_failure timestamp with time zone,
        blk_read_time double precision,
        blk_write_time double precision,
        session_time double precision,
        active_time double precision,
        idle_in_transaction_time double precision,
        sessions bigint,
        sessions_abandoned bigint,
        sessions_fatal bigint,
        sessions_killed bigint,
        stats_reset timestamp with time zone,
        datsize bigint,
        datsize_delta bigint,
        datistemplate boolean
        );

    EXECUTE format('ANALYZE last_stat_database_srv%1$s',
      sserver_id);

    IF (server_properties #>> '{collect_timings}')::boolean THEN
      server_properties := jsonb_set(server_properties,'{timings,collect database stats,end}',to_jsonb(clock_timestamp()));
      server_properties := jsonb_set(server_properties,'{timings,calculate database stats}',jsonb_build_object('start',clock_timestamp()));
    END IF;
    -- Calc stat_database diff
    INSERT INTO sample_stat_database(
      server_id,
      sample_id,
      datid,
      datname,
      xact_commit,
      xact_rollback,
      blks_read,
      blks_hit,
      tup_returned,
      tup_fetched,
      tup_inserted,
      tup_updated,
      tup_deleted,
      conflicts,
      temp_files,
      temp_bytes,
      deadlocks,
      checksum_failures,
      checksum_last_failure,
      blk_read_time,
      blk_write_time,
      session_time,
      active_time,
      idle_in_transaction_time,
      sessions,
      sessions_abandoned,
      sessions_fatal,
      sessions_killed,
      stats_reset,
      datsize,
      datsize_delta,
      datistemplate
    )
    SELECT
        cur.server_id,
        cur.sample_id,
        cur.datid,
        cur.datname,
        cur.xact_commit - COALESCE(lst.xact_commit,0),
        cur.xact_rollback - COALESCE(lst.xact_rollback,0),
        cur.blks_read - COALESCE(lst.blks_read,0),
        cur.blks_hit - COALESCE(lst.blks_hit,0),
        cur.tup_returned - COALESCE(lst.tup_returned,0),
        cur.tup_fetched - COALESCE(lst.tup_fetched,0),
        cur.tup_inserted - COALESCE(lst.tup_inserted,0),
        cur.tup_updated - COALESCE(lst.tup_updated,0),
        cur.tup_deleted - COALESCE(lst.tup_deleted,0),
        cur.conflicts - COALESCE(lst.conflicts,0),
        cur.temp_files - COALESCE(lst.temp_files,0),
        cur.temp_bytes - COALESCE(lst.temp_bytes,0),
        cur.deadlocks - COALESCE(lst.deadlocks,0),
        cur.checksum_failures - COALESCE(lst.checksum_failures,0),
        cur.checksum_last_failure,
        cur.blk_read_time - COALESCE(lst.blk_read_time,0),
        cur.blk_write_time - COALESCE(lst.blk_write_time,0),
        cur.session_time - COALESCE(lst.session_time,0),
        cur.active_time - COALESCE(lst.active_time,0),
        cur.idle_in_transaction_time - COALESCE(lst.idle_in_transaction_time,0),
        cur.sessions - COALESCE(lst.sessions,0),
        cur.sessions_abandoned - COALESCE(lst.sessions_abandoned,0),
        cur.sessions_fatal - COALESCE(lst.sessions_fatal,0),
        cur.sessions_killed - COALESCE(lst.sessions_killed,0),
        cur.stats_reset,
        cur.datsize as datsize,
        cur.datsize - COALESCE(lst.datsize,0) as datsize_delta,
        cur.datistemplate
    FROM last_stat_database cur
      LEFT OUTER JOIN last_stat_database lst ON
        (lst.server_id, lst.sample_id, lst.datid, lst.datname, lst.stats_reset) =
        (cur.server_id, cur.sample_id - 1, cur.datid, cur.datname, cur.stats_reset)
    WHERE cur.sample_id = s_id AND cur.server_id = sserver_id;

    /*
    * In case of statistics reset full database size, and checksum checksum_failures
    * is incorrectly considered as increment by previous query.
    * So, we need to update it with correct value
    */
    UPDATE sample_stat_database sdb
    SET
      datsize_delta = cur.datsize - lst.datsize,
      checksum_failures = cur.checksum_failures - lst.checksum_failures,
      checksum_last_failure = cur.checksum_last_failure
    FROM
      last_stat_database cur
      JOIN last_stat_database lst ON
        (lst.server_id, lst.sample_id, lst.datid, lst.datname) =
        (cur.server_id, cur.sample_id - 1, cur.datid, cur.datname)
    WHERE cur.stats_reset != lst.stats_reset AND
      cur.sample_id = s_id AND cur.server_id = sserver_id AND
      (sdb.server_id, sdb.sample_id, sdb.datid, sdb.datname) =
      (cur.server_id, cur.sample_id, cur.datid, cur.datname);

    IF (server_properties #>> '{collect_timings}')::boolean THEN
      server_properties := jsonb_set(server_properties,'{timings,calculate database stats,end}',to_jsonb(clock_timestamp()));
      server_properties := jsonb_set(server_properties,'{timings,collect tablespace stats}',jsonb_build_object('start',clock_timestamp()));
    END IF;

    -- Construct tablespace stats query
    server_query := 'SELECT '
        'oid as tablespaceid,'
        'spcname as tablespacename,'
        'pg_catalog.pg_tablespace_location(oid) as tablespacepath,'
        'pg_catalog.pg_tablespace_size(oid) as size,'
        '0 as size_delta '
        'FROM pg_catalog.pg_tablespace ';

    -- Get tablespace stats
    INSERT INTO last_stat_tablespaces(
      server_id,
      sample_id,
      tablespaceid,
      tablespacename,
      tablespacepath,
      size,
      size_delta
    )
    SELECT
      sserver_id,
      s_id,
      dbl.tablespaceid,
      dbl.tablespacename,
      dbl.tablespacepath,
      dbl.size AS size,
      dbl.size_delta AS size_delta
    FROM dblink('server_connection', server_query)
    AS dbl (
        tablespaceid            oid,
        tablespacename          name,
        tablespacepath          text,
        size                    bigint,
        size_delta              bigint
    );

    EXECUTE format('ANALYZE last_stat_tablespaces_srv%1$s',
      sserver_id);

    IF (server_properties #>> '{collect_timings}')::boolean THEN
      server_properties := jsonb_set(server_properties,'{timings,collect tablespace stats,end}',to_jsonb(clock_timestamp()));
      server_properties := jsonb_set(server_properties,'{timings,collect statement stats}',jsonb_build_object('start',clock_timestamp()));
    END IF;

    -- Search for statements statistics extension
    CASE
      -- pg_stat_statements statistics collection
      WHEN (
        SELECT count(*) = 1
        FROM jsonb_to_recordset(server_properties #> '{extensions}') AS ext(extname text)
        WHERE extname = 'pg_stat_statements'
      ) THEN
        PERFORM collect_pg_stat_statements_stats(server_properties, sserver_id, s_id, topn);
      ELSE
        NULL;
    END CASE;

    IF (server_properties #>> '{collect_timings}')::boolean THEN
      server_properties := jsonb_set(server_properties,'{timings,collect statement stats,end}',to_jsonb(clock_timestamp()));
      server_properties := jsonb_set(server_properties,'{timings,collect wait sampling stats}',jsonb_build_object('start',clock_timestamp()));
    END IF;

    -- Search for wait sampling extension
    CASE
      -- pg_wait_sampling statistics collection
      WHEN (
        SELECT count(*) = 1
        FROM jsonb_to_recordset(server_properties #> '{extensions}') AS ext(extname text)
        WHERE extname = 'pg_wait_sampling'
      ) THEN
        PERFORM collect_pg_wait_sampling_stats(server_properties, sserver_id, s_id, topn);
      ELSE
        NULL;
    END CASE;

    IF (server_properties #>> '{collect_timings}')::boolean THEN
      server_properties := jsonb_set(server_properties,'{timings,collect wait sampling stats,end}',to_jsonb(clock_timestamp()));
      server_properties := jsonb_set(server_properties,'{timings,query pg_stat_bgwriter}',jsonb_build_object('start',clock_timestamp()));
    END IF;

    -- pg_stat_bgwriter data
    CASE
      WHEN (
        SELECT count(*) = 1 FROM jsonb_to_recordset(server_properties #> '{settings}')
          AS x(name text, reset_val text)
        WHERE name = 'server_version_num'
          AND reset_val::integer < 100000
      )
      THEN
        server_query := 'SELECT '
          'checkpoints_timed,'
          'checkpoints_req,'
          'checkpoint_write_time,'
          'checkpoint_sync_time,'
          'buffers_checkpoint,'
          'buffers_clean,'
          'maxwritten_clean,'
          'buffers_backend,'
          'buffers_backend_fsync,'
          'buffers_alloc,'
          'stats_reset,'
          'CASE WHEN pg_catalog.pg_is_in_recovery() THEN 0 '
            'ELSE pg_catalog.pg_xlog_location_diff(pg_catalog.pg_current_xlog_location(),''0/00000000'') '
          'END AS wal_size '
          'FROM pg_catalog.pg_stat_bgwriter';
      WHEN (
        SELECT count(*) = 1 FROM jsonb_to_recordset(server_properties #> '{settings}')
          AS x(name text, reset_val text)
        WHERE name = 'server_version_num'
          AND reset_val::integer >= 100000
      )
      THEN
        server_query := 'SELECT '
          'checkpoints_timed,'
          'checkpoints_req,'
          'checkpoint_write_time,'
          'checkpoint_sync_time,'
          'buffers_checkpoint,'
          'buffers_clean,'
          'maxwritten_clean,'
          'buffers_backend,'
          'buffers_backend_fsync,'
          'buffers_alloc,'
          'stats_reset,'
          'CASE WHEN pg_catalog.pg_is_in_recovery() THEN 0 '
              'ELSE pg_catalog.pg_wal_lsn_diff(pg_catalog.pg_current_wal_lsn(),''0/00000000'') '
          'END AS wal_size '
        'FROM pg_catalog.pg_stat_bgwriter';
    END CASE;

    IF server_query IS NOT NULL THEN
      INSERT INTO last_stat_cluster (
        server_id,
        sample_id,
        checkpoints_timed,
        checkpoints_req,
        checkpoint_write_time,
        checkpoint_sync_time,
        buffers_checkpoint,
        buffers_clean,
        maxwritten_clean,
        buffers_backend,
        buffers_backend_fsync,
        buffers_alloc,
        stats_reset,
        wal_size)
      SELECT
        sserver_id,
        s_id,
        checkpoints_timed AS checkpoints_timed,
        checkpoints_req AS checkpoints_req,
        checkpoint_write_time AS checkpoint_write_time,
        checkpoint_sync_time AS checkpoint_sync_time,
        buffers_checkpoint AS buffers_checkpoint,
        buffers_clean AS buffers_clean,
        maxwritten_clean AS maxwritten_clean,
        buffers_backend AS buffers_backend,
        buffers_backend_fsync AS buffers_backend_fsync,
        buffers_alloc AS buffers_alloc,
        stats_reset,
        wal_size AS wal_size
      FROM dblink('server_connection',server_query) AS rs (
        checkpoints_timed bigint,
        checkpoints_req bigint,
        checkpoint_write_time double precision,
        checkpoint_sync_time double precision,
        buffers_checkpoint bigint,
        buffers_clean bigint,
        maxwritten_clean bigint,
        buffers_backend bigint,
        buffers_backend_fsync bigint,
        buffers_alloc bigint,
        stats_reset timestamp with time zone,
        wal_size bigint);
    END IF;

    IF (server_properties #>> '{collect_timings}')::boolean THEN
      server_properties := jsonb_set(server_properties,'{timings,query pg_stat_bgwriter,end}',to_jsonb(clock_timestamp()));
      server_properties := jsonb_set(server_properties,'{timings,query pg_stat_wal}',jsonb_build_object('start',clock_timestamp()));
    END IF;

    -- pg_stat_wal data
    CASE
      WHEN (
        SELECT count(*) = 1 FROM jsonb_to_recordset(server_properties #> '{settings}')
          AS x(name text, reset_val text)
        WHERE name = 'server_version_num'
          AND reset_val::integer >= 140000
      )
      THEN
        server_query := 'SELECT '
          'wal_records,'
          'wal_fpi,'
          'wal_bytes,'
          'wal_buffers_full,'
          'wal_write,'
          'wal_sync,'
          'wal_write_time,'
          'wal_sync_time,'
          'stats_reset '
          'FROM pg_catalog.pg_stat_wal';
      ELSE
        server_query := NULL;
    END CASE;

    IF server_query IS NOT NULL THEN
      INSERT INTO last_stat_wal (
        server_id,
        sample_id,
        wal_records,
        wal_fpi,
        wal_bytes,
        wal_buffers_full,
        wal_write,
        wal_sync,
        wal_write_time,
        wal_sync_time,
        stats_reset
      )
      SELECT
        sserver_id,
        s_id,
        wal_records,
        wal_fpi,
        wal_bytes,
        wal_buffers_full,
        wal_write,
        wal_sync,
        wal_write_time,
        wal_sync_time,
        stats_reset
      FROM dblink('server_connection',server_query) AS rs (
        wal_records         bigint,
        wal_fpi             bigint,
        wal_bytes           numeric,
        wal_buffers_full    bigint,
        wal_write           bigint,
        wal_sync            bigint,
        wal_write_time      double precision,
        wal_sync_time       double precision,
        stats_reset         timestamp with time zone);
    END IF;

    IF (server_properties #>> '{collect_timings}')::boolean THEN
      server_properties := jsonb_set(server_properties,'{timings,query pg_stat_wal,end}',to_jsonb(clock_timestamp()));
      server_properties := jsonb_set(server_properties,'{timings,query pg_stat_archiver}',jsonb_build_object('start',clock_timestamp()));
    END IF;

    -- pg_stat_archiver data
    CASE
      WHEN (
        SELECT count(*) = 1 FROM jsonb_to_recordset(server_properties #> '{settings}')
          AS x(name text, reset_val text)
        WHERE name = 'server_version_num'
          AND reset_val::integer > 90500
      )
      THEN
        server_query := 'SELECT '
          'archived_count,'
          'last_archived_wal,'
          'last_archived_time,'
          'failed_count,'
          'last_failed_wal,'
          'last_failed_time,'
          'stats_reset '
          'FROM pg_catalog.pg_stat_archiver';
    END CASE;

    IF server_query IS NOT NULL THEN
      INSERT INTO last_stat_archiver (
        server_id,
        sample_id,
        archived_count,
        last_archived_wal,
        last_archived_time,
        failed_count,
        last_failed_wal,
        last_failed_time,
        stats_reset)
      SELECT
        sserver_id,
        s_id,
        archived_count as archived_count,
        last_archived_wal as last_archived_wal,
        last_archived_time as last_archived_time,
        failed_count as failed_count,
        last_failed_wal as last_failed_wal,
        last_failed_time as last_failed_time,
        stats_reset as stats_reset
      FROM dblink('server_connection',server_query) AS rs (
        archived_count              bigint,
        last_archived_wal           text,
        last_archived_time          timestamp with time zone,
        failed_count                bigint,
        last_failed_wal             text,
        last_failed_time            timestamp with time zone,
        stats_reset                 timestamp with time zone
      );
    END IF;

    IF (server_properties #>> '{collect_timings}')::boolean THEN
      server_properties := jsonb_set(server_properties,'{timings,query pg_stat_archiver,end}',to_jsonb(clock_timestamp()));
      server_properties := jsonb_set(server_properties,'{timings,collect object stats}',jsonb_build_object('start',clock_timestamp()));
    END IF;

    -- Collecting stat info for objects of all databases
    server_properties := collect_obj_stats(server_properties, sserver_id, s_id, skip_sizes);
    ASSERT server_properties IS NOT NULL, 'lost properties';

    IF (server_properties #>> '{collect_timings}')::boolean THEN
      server_properties := jsonb_set(server_properties,'{timings,collect object stats,end}',to_jsonb(clock_timestamp()));
      server_properties := jsonb_set(server_properties,'{timings,disconnect}',jsonb_build_object('start',clock_timestamp()));
    END IF;

    PERFORM dblink('server_connection', 'COMMIT');
    PERFORM dblink_disconnect('server_connection');

    IF (server_properties #>> '{collect_timings}')::boolean THEN
      server_properties := jsonb_set(server_properties,'{timings,disconnect,end}',to_jsonb(clock_timestamp()));
      server_properties := jsonb_set(server_properties,'{timings,maintain repository}',jsonb_build_object('start',clock_timestamp()));
    END IF;

    -- Updating dictionary table in case of object renaming:
    -- Databases
    UPDATE sample_stat_database AS db
    SET datname = lst.datname
    FROM last_stat_database AS lst
    WHERE db.server_id = lst.server_id AND db.datid = lst.datid
      AND db.datname != lst.datname
      AND lst.sample_id = s_id;
    -- Tables
    UPDATE tables_list AS tl
    SET (schemaname, relname) = (lst.schemaname, lst.relname)
    FROM last_stat_tables AS lst
    WHERE (tl.server_id, tl.datid, tl.relid, tl.relkind) =
        (lst.server_id, lst.datid, lst.relid, lst.relkind)
      AND (tl.schemaname, tl.relname) != (lst.schemaname, lst.relname)
      AND lst.sample_id = s_id;
    -- Functions
    UPDATE funcs_list AS fl
    SET (schemaname, funcname, funcargs) =
      (lst.schemaname, lst.funcname, lst.funcargs)
    FROM last_stat_user_functions AS lst
    WHERE (fl.server_id, fl.datid, fl.funcid) =
        (lst.server_id, lst.datid, lst.funcid)
      AND (fl.schemaname, fl.funcname, fl.funcargs) !=
        (lst.schemaname, lst.funcname, lst.funcargs)
      AND lst.sample_id = s_id;

    IF (server_properties #>> '{collect_timings}')::boolean THEN
      server_properties := jsonb_set(server_properties,'{timings,maintain repository,end}',to_jsonb(clock_timestamp()));
      server_properties := jsonb_set(server_properties,'{timings,calculate tablespace stats}',jsonb_build_object('start',clock_timestamp()));
    END IF;

    INSERT INTO tablespaces_list AS itl (
        server_id,
        last_sample_id,
        tablespaceid,
        tablespacename,
        tablespacepath
      )
    SELECT
      cur.server_id,
      NULL,
      cur.tablespaceid,
      cur.tablespacename,
      cur.tablespacepath
    FROM
      last_stat_tablespaces cur
    WHERE
      (cur.server_id, cur.sample_id) = (sserver_id, s_id)
    ON CONFLICT ON CONSTRAINT pk_tablespace_list DO
    UPDATE SET
        (last_sample_id, tablespacename, tablespacepath) =
        (EXCLUDED.last_sample_id, EXCLUDED.tablespacename, EXCLUDED.tablespacepath)
      WHERE
        (itl.last_sample_id, itl.tablespacename, itl.tablespacepath) IS DISTINCT FROM
        (EXCLUDED.last_sample_id, EXCLUDED.tablespacename, EXCLUDED.tablespacepath);

    -- Calculate diffs for tablespaces
    INSERT INTO sample_stat_tablespaces(
      server_id,
      sample_id,
      tablespaceid,
      size,
      size_delta
    )
    SELECT
      cur.server_id as server_id,
      cur.sample_id as sample_id,
      cur.tablespaceid as tablespaceid,
      cur.size as size,
      cur.size - COALESCE(lst.size, 0) AS size_delta
    FROM last_stat_tablespaces cur
      LEFT OUTER JOIN last_stat_tablespaces lst ON
        (cur.server_id, cur.sample_id - 1, cur.tablespaceid) =
        (lst.server_id, lst.sample_id, lst.tablespaceid)
    WHERE (cur.sample_id, cur.server_id) = (s_id, sserver_id);

    IF (server_properties #>> '{collect_timings}')::boolean THEN
      server_properties := jsonb_set(server_properties,'{timings,calculate tablespace stats,end}',to_jsonb(clock_timestamp()));
      server_properties := jsonb_set(server_properties,'{timings,calculate object stats}',jsonb_build_object('start',clock_timestamp()));
    END IF;

    -- collect databases objects stats
    server_properties := sample_dbobj_delta(server_properties,sserver_id,s_id,topn,skip_sizes);
    ASSERT server_properties IS NOT NULL, 'lost properties';

    DELETE FROM last_stat_tablespaces WHERE server_id = sserver_id AND sample_id != s_id;

    DELETE FROM last_stat_database WHERE server_id = sserver_id AND sample_id != s_id;

    IF (server_properties #>> '{collect_timings}')::boolean THEN
      server_properties := jsonb_set(server_properties,'{timings,calculate object stats,end}',to_jsonb(clock_timestamp()));
      server_properties := jsonb_set(server_properties,'{timings,calculate cluster stats}',jsonb_build_object('start',clock_timestamp()));
    END IF;

    -- Calc stat cluster diff
    INSERT INTO sample_stat_cluster(
      server_id,
      sample_id,
      checkpoints_timed,
      checkpoints_req,
      checkpoint_write_time,
      checkpoint_sync_time,
      buffers_checkpoint,
      buffers_clean,
      maxwritten_clean,
      buffers_backend,
      buffers_backend_fsync,
      buffers_alloc,
      stats_reset,
      wal_size
    )
    SELECT
        cur.server_id,
        cur.sample_id,
        cur.checkpoints_timed - COALESCE(lst.checkpoints_timed,0),
        cur.checkpoints_req - COALESCE(lst.checkpoints_req,0),
        cur.checkpoint_write_time - COALESCE(lst.checkpoint_write_time,0),
        cur.checkpoint_sync_time - COALESCE(lst.checkpoint_sync_time,0),
        cur.buffers_checkpoint - COALESCE(lst.buffers_checkpoint,0),
        cur.buffers_clean - COALESCE(lst.buffers_clean,0),
        cur.maxwritten_clean - COALESCE(lst.maxwritten_clean,0),
        cur.buffers_backend - COALESCE(lst.buffers_backend,0),
        cur.buffers_backend_fsync - COALESCE(lst.buffers_backend_fsync,0),
        cur.buffers_alloc - COALESCE(lst.buffers_alloc,0),
        cur.stats_reset,
        cur.wal_size - COALESCE(lst.wal_size,0)
        /* We will overwrite this value in case of stats reset
         * (see below)
         */
    FROM last_stat_cluster cur
      LEFT OUTER JOIN last_stat_cluster lst ON
        (cur.stats_reset, cur.server_id, cur.sample_id) =
        (lst.stats_reset, lst.server_id, lst.sample_id + 1)
    WHERE cur.sample_id = s_id AND cur.server_id = sserver_id;

    /* wal_size is calculated since 0 to current value when stats reset happened
     * so, we need to update it
     */
    UPDATE sample_stat_cluster ssc
    SET wal_size = cur.wal_size - lst.wal_size
    FROM last_stat_cluster cur
      JOIN last_stat_cluster lst ON
        (cur.server_id, cur.sample_id) =
        (lst.server_id, lst.sample_id + 1)
    WHERE
      (ssc.server_id, ssc.sample_id) =
      (cur.server_id, cur.sample_id) AND
      cur.sample_id = s_id AND
      cur.server_id = sserver_id AND
      cur.stats_reset != lst.stats_reset;

    DELETE FROM last_stat_cluster WHERE server_id = sserver_id AND sample_id != s_id;

    IF (server_properties #>> '{collect_timings}')::boolean THEN
      server_properties := jsonb_set(server_properties,'{timings,calculate cluster stats,end}',to_jsonb(clock_timestamp()));
      server_properties := jsonb_set(server_properties,'{timings,calculate WAL stats}',jsonb_build_object('start',clock_timestamp()));
    END IF;

    -- Calc WAL stat diff
    INSERT INTO sample_stat_wal(
      server_id,
      sample_id,
      wal_records,
      wal_fpi,
      wal_bytes,
      wal_buffers_full,
      wal_write,
      wal_sync,
      wal_write_time,
      wal_sync_time,
      stats_reset
    )
    SELECT
        cur.server_id,
        cur.sample_id,
        cur.wal_records - COALESCE(lst.wal_records,0),
        cur.wal_fpi - COALESCE(lst.wal_fpi,0),
        cur.wal_bytes - COALESCE(lst.wal_bytes,0),
        cur.wal_buffers_full - COALESCE(lst.wal_buffers_full,0),
        cur.wal_write - COALESCE(lst.wal_write,0),
        cur.wal_sync - COALESCE(lst.wal_sync,0),
        cur.wal_write_time - COALESCE(lst.wal_write_time,0),
        cur.wal_sync_time - COALESCE(lst.wal_sync_time,0),
        cur.stats_reset
    FROM last_stat_wal cur
    LEFT OUTER JOIN last_stat_wal lst ON
      (cur.stats_reset = lst.stats_reset AND cur.server_id = lst.server_id AND lst.sample_id = cur.sample_id - 1)
    WHERE cur.sample_id = s_id AND cur.server_id = sserver_id;

    DELETE FROM last_stat_wal WHERE server_id = sserver_id AND sample_id != s_id;

    IF (server_properties #>> '{collect_timings}')::boolean THEN
      server_properties := jsonb_set(server_properties,'{timings,calculate WAL stats,end}',to_jsonb(clock_timestamp()));
      server_properties := jsonb_set(server_properties,'{timings,calculate archiver stats}',jsonb_build_object('start',clock_timestamp()));
    END IF;

    -- Calc stat archiver diff
    INSERT INTO sample_stat_archiver(
      server_id,
      sample_id,
      archived_count,
      last_archived_wal,
      last_archived_time,
      failed_count,
      last_failed_wal,
      last_failed_time,
      stats_reset
    )
    SELECT
        cur.server_id,
        cur.sample_id,
        cur.archived_count - COALESCE(lst.archived_count,0),
        cur.last_archived_wal,
        cur.last_archived_time,
        cur.failed_count - COALESCE(lst.failed_count,0),
        cur.last_failed_wal,
        cur.last_failed_time,
        cur.stats_reset
    FROM last_stat_archiver cur
    LEFT OUTER JOIN last_stat_archiver lst ON
      (cur.stats_reset = lst.stats_reset AND cur.server_id = lst.server_id AND lst.sample_id = cur.sample_id - 1)
    WHERE cur.sample_id = s_id AND cur.server_id = sserver_id;

    DELETE FROM last_stat_archiver WHERE server_id = sserver_id AND sample_id != s_id;

    IF (server_properties #>> '{collect_timings}')::boolean THEN
      server_properties := jsonb_set(server_properties,'{timings,calculate archiver stats,end}',to_jsonb(clock_timestamp()));
      server_properties := jsonb_set(server_properties,'{timings,delete obsolete samples}',jsonb_build_object('start',clock_timestamp()));
    END IF;

    -- Updating dictionary tables setting last_sample_id
    UPDATE tablespaces_list utl SET last_sample_id = s_id - 1
    FROM tablespaces_list tl LEFT JOIN sample_stat_tablespaces cur
      ON (cur.server_id, cur.sample_id, cur.tablespaceid) =
        (tl.server_id, s_id, tl.tablespaceid)
    WHERE
      tl.last_sample_id IS NULL AND
      (utl.server_id, utl.tablespaceid) = (tl.server_id, tl.tablespaceid) AND
      tl.server_id = sserver_id AND cur.server_id IS NULL;

    UPDATE funcs_list ufl SET last_sample_id = s_id - 1
    FROM funcs_list fl LEFT JOIN sample_stat_user_functions cur
      ON (cur.server_id, cur.sample_id, cur.datid, cur.funcid) =
        (fl.server_id, s_id, fl.datid, fl.funcid)
    WHERE
      fl.last_sample_id IS NULL AND
      fl.server_id = sserver_id AND cur.server_id IS NULL AND
      (ufl.server_id, ufl.datid, ufl.funcid) =
      (fl.server_id, fl.datid, fl.funcid);

    UPDATE indexes_list uil SET last_sample_id = s_id - 1
    FROM indexes_list il LEFT JOIN sample_stat_indexes cur
      ON (cur.server_id, cur.sample_id, cur.datid, cur.indexrelid) =
        (il.server_id, s_id, il.datid, il.indexrelid)
    WHERE
      il.last_sample_id IS NULL AND
      il.server_id = sserver_id AND cur.server_id IS NULL AND
      (uil.server_id, uil.datid, uil.indexrelid) =
      (il.server_id, il.datid, il.indexrelid);

    UPDATE tables_list utl SET last_sample_id = s_id - 1
    FROM tables_list tl LEFT JOIN sample_stat_tables cur
      ON (cur.server_id, cur.sample_id, cur.datid, cur.relid) =
        (tl.server_id, s_id, tl.datid, tl.relid)
    WHERE
      tl.last_sample_id IS NULL AND
      tl.server_id = sserver_id AND cur.server_id IS NULL AND
      (utl.server_id, utl.datid, utl.relid) =
      (tl.server_id, tl.datid, tl.relid);

    UPDATE stmt_list slu SET last_sample_id = s_id - 1
    FROM sample_statements ss RIGHT JOIN stmt_list sl
      ON (ss.server_id, ss.sample_id, ss.queryid_md5) =
        (sl.server_id, s_id, sl.queryid_md5)
    WHERE
      sl.server_id = sserver_id AND
      sl.last_sample_id IS NULL AND
      ss.server_id IS NULL AND
      (slu.server_id, slu.queryid_md5) = (sl.server_id, sl.queryid_md5);

    UPDATE roles_list rlu SET last_sample_id = s_id - 1
    FROM
        sample_statements ss
      RIGHT JOIN roles_list rl
      ON (ss.server_id, ss.sample_id, ss.userid) =
        (rl.server_id, s_id, rl.userid)
    WHERE
      rl.server_id = sserver_id AND
      rl.last_sample_id IS NULL AND
      ss.server_id IS NULL AND
      (rlu.server_id, rlu.userid) = (rl.server_id, rl.userid);

    -- Deleting obsolete baselines
    DELETE FROM baselines
    WHERE keep_until < now()
      AND server_id = sserver_id;

    -- Deleting obsolete samples
    PERFORM num_nulls(min(s.sample_id),max(s.sample_id)) > 0 OR
      delete_samples(sserver_id, min(s.sample_id), max(s.sample_id)) > 0
    FROM samples s JOIN
      servers n USING (server_id)
    WHERE s.server_id = sserver_id
        AND s.sample_time < now() - (COALESCE(n.max_sample_age,ret) || ' days')::interval
        AND (s.server_id,s.sample_id) NOT IN (SELECT server_id,sample_id FROM bl_samples WHERE server_id = sserver_id);

    IF (server_properties #>> '{collect_timings}')::boolean THEN
      server_properties := jsonb_set(server_properties,'{timings,delete obsolete samples,end}',to_jsonb(clock_timestamp()));
      server_properties := jsonb_set(server_properties,'{timings,total,end}',to_jsonb(clock_timestamp()));
      -- Save timing statistics of sample
      INSERT INTO sample_timings
      SELECT sserver_id, s_id, key,(value::jsonb #>> '{end}')::timestamp with time zone - (value::jsonb #>> '{start}')::timestamp with time zone as time_spent
      FROM jsonb_each_text(server_properties #> '{timings}');
    END IF;
    ASSERT server_properties IS NOT NULL, 'lost properties';

    RETURN 0;
END;
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION take_sample(IN sserver_id integer, IN skip_sizes boolean) IS
  'Statistics sample creation function (by server_id)';
CREATE FUNCTION collect_obj_stats(IN properties jsonb, IN sserver_id integer, IN s_id integer,
  IN skip_sizes boolean
) RETURNS jsonb SET search_path=@extschema@ AS $$
DECLARE
    --Cursor for db stats
    c_dblist CURSOR FOR
    SELECT datid,datname,tablespaceid FROM dblink('server_connection',
    'select dbs.oid,dbs.datname,dbs.dattablespace from pg_catalog.pg_database dbs '
    'where not dbs.datistemplate and dbs.datallowconn') AS dbl (
        datid oid,
        datname name,
        tablespaceid oid
    ) JOIN servers n ON (n.server_id = sserver_id AND array_position(n.db_exclude,dbl.datname) IS NULL);

    qres        record;
    db_connstr  text;
    t_query     text;
    result      jsonb := collect_obj_stats.properties;
BEGIN
    -- Adding dblink extension schema to search_path if it does not already there
    IF (SELECT count(*) = 0 FROM pg_catalog.pg_extension WHERE extname = 'dblink') THEN
      RAISE 'dblink extension must be installed';
    END IF;
    SELECT extnamespace::regnamespace AS dblink_schema INTO STRICT qres FROM pg_catalog.pg_extension WHERE extname = 'dblink';
    IF NOT string_to_array(current_setting('search_path'),',') @> ARRAY[qres.dblink_schema::text] THEN
      EXECUTE 'SET LOCAL search_path TO ' || current_setting('search_path')||','|| qres.dblink_schema;
    END IF;

    -- Disconnecting existing connection
    IF dblink_get_connections() @> ARRAY['server_db_connection'] THEN
        PERFORM dblink_disconnect('server_db_connection');
    END IF;

    -- Load new data from statistic views of all cluster databases
    FOR qres IN c_dblist LOOP
      db_connstr := concat_ws(' ',properties #>> '{properties,server_connstr}',
        format($o$dbname='%s'$o$,replace(qres.datname,$o$'$o$,$o$\'$o$))
      );
      PERFORM dblink_connect('server_db_connection',db_connstr);
      -- Transaction
      PERFORM dblink('server_db_connection','BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY');
      -- Setting application name
      PERFORM dblink('server_db_connection','SET application_name=''pg_profile''');
      -- Setting lock_timout prevents hanging of take_sample() call due to DDL in long transaction
      PERFORM dblink('server_db_connection','SET lock_timeout=3000');
      -- Reset search_path for security reasons
      PERFORM dblink('server_db_connection','SET search_path=''''');

      IF (properties #>> '{collect_timings}')::boolean THEN
        result := jsonb_set(result,ARRAY['timings',format('db:%s collect tables stats',qres.datname)],jsonb_build_object('start',clock_timestamp()));
      END IF;

      -- Generate Table stats query
      CASE
        WHEN (
          SELECT count(*) = 1 FROM jsonb_to_recordset(properties #> '{settings}')
            AS x(name text, reset_val text)
          WHERE name = 'server_version_num'
            AND reset_val::integer < 130000
        )
        THEN
          t_query := 'SELECT '
            'st.relid,'
            'st.schemaname,'
            'st.relname,'
            'st.seq_scan,'
            'st.seq_tup_read,'
            'st.idx_scan,'
            'st.idx_tup_fetch,'
            'st.n_tup_ins,'
            'st.n_tup_upd,'
            'st.n_tup_del,'
            'st.n_tup_hot_upd,'
            'st.n_live_tup,'
            'st.n_dead_tup,'
            'st.n_mod_since_analyze,'
            'NULL as n_ins_since_vacuum,'
            'st.last_vacuum,'
            'st.last_autovacuum,'
            'st.last_analyze,'
            'st.last_autoanalyze,'
            'st.vacuum_count,'
            'st.autovacuum_count,'
            'st.analyze_count,'
            'st.autoanalyze_count,'
            'stio.heap_blks_read,'
            'stio.heap_blks_hit,'
            'stio.idx_blks_read,'
            'stio.idx_blks_hit,'
            'stio.toast_blks_read,'
            'stio.toast_blks_hit,'
            'stio.tidx_blks_read,'
            'stio.tidx_blks_hit,'
            -- Size of all forks without TOAST
            '{relation_size} relsize,'
            '0 relsize_diff,'
            'class.reltablespace AS tablespaceid,'
            'class.reltoastrelid,'
            'class.relkind,'
            'class.relpages::bigint * current_setting(''block_size'')::bigint AS relpages_bytes,'
            '0 AS relpages_bytes_diff '
          'FROM pg_catalog.pg_stat_all_tables st '
          'JOIN pg_catalog.pg_statio_all_tables stio USING (relid, schemaname, relname) '
          'JOIN pg_catalog.pg_class class ON (st.relid = class.oid) '
          -- is relation or its dependant is locked
          '{lock_join}'
          ;

        WHEN (
          SELECT count(*) = 1 FROM jsonb_to_recordset(properties #> '{settings}')
            AS x(name text, reset_val text)
          WHERE name = 'server_version_num'
            AND reset_val::integer >= 130000
        )
        THEN
          t_query := 'SELECT '
            'st.relid,'
            'st.schemaname,'
            'st.relname,'
            'st.seq_scan,'
            'st.seq_tup_read,'
            'st.idx_scan,'
            'st.idx_tup_fetch,'
            'st.n_tup_ins,'
            'st.n_tup_upd,'
            'st.n_tup_del,'
            'st.n_tup_hot_upd,'
            'st.n_live_tup,'
            'st.n_dead_tup,'
            'st.n_mod_since_analyze,'
            'st.n_ins_since_vacuum,'
            'st.last_vacuum,'
            'st.last_autovacuum,'
            'st.last_analyze,'
            'st.last_autoanalyze,'
            'st.vacuum_count,'
            'st.autovacuum_count,'
            'st.analyze_count,'
            'st.autoanalyze_count,'
            'stio.heap_blks_read,'
            'stio.heap_blks_hit,'
            'stio.idx_blks_read,'
            'stio.idx_blks_hit,'
            'stio.toast_blks_read,'
            'stio.toast_blks_hit,'
            'stio.tidx_blks_read,'
            'stio.tidx_blks_hit,'
            -- Size of all forks without TOAST
            '{relation_size} relsize,'
            '0 relsize_diff,'
            'class.reltablespace AS tablespaceid,'
            'class.reltoastrelid,'
            'class.relkind,'
            'class.relpages::bigint * current_setting(''block_size'')::bigint AS relpages_bytes,'
            '0 AS relpages_bytes_diff '
          'FROM pg_catalog.pg_stat_all_tables st '
          'JOIN pg_catalog.pg_statio_all_tables stio USING (relid, schemaname, relname) '
          'JOIN pg_catalog.pg_class class ON (st.relid = class.oid) '
          -- is relation or its dependant is locked
          '{lock_join}'
          ;
        ELSE
          RAISE 'Unsupported server version.';
      END CASE;

      IF skip_sizes THEN
        t_query := replace(t_query,'{relation_size}','NULL');
        t_query := replace(t_query,'{lock_join}','');
      ELSE
        t_query := replace(t_query,'{relation_size}','CASE locked.objid WHEN st.relid THEN NULL ELSE '
          'pg_catalog.pg_table_size(st.relid) - '
          'coalesce(pg_catalog.pg_relation_size(class.reltoastrelid),0) END');
        t_query := replace(t_query,'{lock_join}',
          'LEFT OUTER JOIN LATERAL '
            '(WITH RECURSIVE deps (objid) AS ('
              'SELECT relation FROM pg_catalog.pg_locks WHERE granted AND locktype = ''relation'' AND mode=''AccessExclusiveLock'' '
              'UNION '
              'SELECT refobjid FROM pg_catalog.pg_depend d JOIN deps dd ON (d.objid = dd.objid)'
            ') '
            'SELECT objid FROM deps) AS locked ON (st.relid = locked.objid)');
      END IF;

      INSERT INTO last_stat_tables(
        server_id,
        sample_id,
        datid,
        relid,
        schemaname,
        relname,
        seq_scan,
        seq_tup_read,
        idx_scan,
        idx_tup_fetch,
        n_tup_ins,
        n_tup_upd,
        n_tup_del,
        n_tup_hot_upd,
        n_live_tup,
        n_dead_tup,
        n_mod_since_analyze,
        n_ins_since_vacuum,
        last_vacuum,
        last_autovacuum,
        last_analyze,
        last_autoanalyze,
        vacuum_count,
        autovacuum_count,
        analyze_count,
        autoanalyze_count,
        heap_blks_read,
        heap_blks_hit,
        idx_blks_read,
        idx_blks_hit,
        toast_blks_read,
        toast_blks_hit,
        tidx_blks_read,
        tidx_blks_hit,
        relsize,
        relsize_diff,
        tablespaceid,
        reltoastrelid,
        relkind,
        in_sample,
        relpages_bytes,
        relpages_bytes_diff
      )
      SELECT
        sserver_id,
        s_id,
        qres.datid,
        dbl.relid,
        dbl.schemaname,
        dbl.relname,
        dbl.seq_scan AS seq_scan,
        dbl.seq_tup_read AS seq_tup_read,
        dbl.idx_scan AS idx_scan,
        dbl.idx_tup_fetch AS idx_tup_fetch,
        dbl.n_tup_ins AS n_tup_ins,
        dbl.n_tup_upd AS n_tup_upd,
        dbl.n_tup_del AS n_tup_del,
        dbl.n_tup_hot_upd AS n_tup_hot_upd,
        dbl.n_live_tup AS n_live_tup,
        dbl.n_dead_tup AS n_dead_tup,
        dbl.n_mod_since_analyze AS n_mod_since_analyze,
        dbl.n_ins_since_vacuum AS n_ins_since_vacuum,
        dbl.last_vacuum,
        dbl.last_autovacuum,
        dbl.last_analyze,
        dbl.last_autoanalyze,
        dbl.vacuum_count AS vacuum_count,
        dbl.autovacuum_count AS autovacuum_count,
        dbl.analyze_count AS analyze_count,
        dbl.autoanalyze_count AS autoanalyze_count,
        dbl.heap_blks_read AS heap_blks_read,
        dbl.heap_blks_hit AS heap_blks_hit,
        dbl.idx_blks_read AS idx_blks_read,
        dbl.idx_blks_hit AS idx_blks_hit,
        dbl.toast_blks_read AS toast_blks_read,
        dbl.toast_blks_hit AS toast_blks_hit,
        dbl.tidx_blks_read AS tidx_blks_read,
        dbl.tidx_blks_hit AS tidx_blks_hit,
        dbl.relsize AS relsize,
        dbl.relsize_diff AS relsize_diff,
        CASE WHEN dbl.tablespaceid=0 THEN qres.tablespaceid ELSE dbl.tablespaceid END AS tablespaceid,
        dbl.reltoastrelid,
        dbl.relkind,
        false,
        dbl.relpages_bytes,
        dbl.relpages_bytes_diff
      FROM dblink('server_db_connection', t_query)
      AS dbl (
          relid                 oid,
          schemaname            name,
          relname               name,
          seq_scan              bigint,
          seq_tup_read          bigint,
          idx_scan              bigint,
          idx_tup_fetch         bigint,
          n_tup_ins             bigint,
          n_tup_upd             bigint,
          n_tup_del             bigint,
          n_tup_hot_upd         bigint,
          n_live_tup            bigint,
          n_dead_tup            bigint,
          n_mod_since_analyze   bigint,
          n_ins_since_vacuum    bigint,
          last_vacuum           timestamp with time zone,
          last_autovacuum       timestamp with time zone,
          last_analyze          timestamp with time zone,
          last_autoanalyze      timestamp with time zone,
          vacuum_count          bigint,
          autovacuum_count      bigint,
          analyze_count         bigint,
          autoanalyze_count     bigint,
          heap_blks_read        bigint,
          heap_blks_hit         bigint,
          idx_blks_read         bigint,
          idx_blks_hit          bigint,
          toast_blks_read       bigint,
          toast_blks_hit        bigint,
          tidx_blks_read        bigint,
          tidx_blks_hit         bigint,
          relsize               bigint,
          relsize_diff          bigint,
          tablespaceid          oid,
          reltoastrelid         oid,
          relkind               char,
          relpages_bytes        bigint,
          relpages_bytes_diff   bigint
      );

      EXECUTE format('ANALYZE last_stat_tables_srv%1$s',
        sserver_id);

      IF (result #>> '{collect_timings}')::boolean THEN
        result := jsonb_set(result,ARRAY['timings',format('db:%s collect tables stats',qres.datname),'end'],to_jsonb(clock_timestamp()));
        result := jsonb_set(result,ARRAY['timings',format('db:%s collect indexes stats',qres.datname)],jsonb_build_object('start',clock_timestamp()));
      END IF;

      -- Generate index stats query
      t_query := 'SELECT st.*,'
        'stio.idx_blks_read,'
        'stio.idx_blks_hit,'
        '{relation_size} relsize,'
        '0,'
        'pg_class.reltablespace as tablespaceid,'
        '(ix.indisunique OR con.conindid IS NOT NULL) AS indisunique,'
        'pg_class.relpages::bigint * current_setting(''block_size'')::bigint AS relpages_bytes,'
        '0 AS relpages_bytes_diff '
      'FROM pg_catalog.pg_stat_all_indexes st '
        'JOIN pg_catalog.pg_statio_all_indexes stio USING (relid, indexrelid, schemaname, relname, indexrelname) '
        'JOIN pg_catalog.pg_index ix ON (ix.indexrelid = st.indexrelid) '
        'JOIN pg_catalog.pg_class ON (pg_class.oid = st.indexrelid) '
        'LEFT OUTER JOIN pg_catalog.pg_constraint con ON (con.conindid = ix.indexrelid AND con.contype in (''p'',''u'')) '
        '{lock_join}'
        ;

      IF skip_sizes THEN
        t_query := replace(t_query,'{relation_size}','NULL');
        t_query := replace(t_query,'{lock_join}','');
      ELSE
        t_query := replace(t_query,'{relation_size}',
          'CASE l.relation WHEN st.indexrelid THEN NULL ELSE pg_relation_size(st.indexrelid) END');
        t_query := replace(t_query,'{lock_join}',
          'LEFT OUTER JOIN LATERAL ('
            'SELECT relation '
            'FROM pg_catalog.pg_locks '
            'WHERE '
            '(relation = st.indexrelid AND granted AND locktype = ''relation'' AND mode=''AccessExclusiveLock'')'
          ') l ON (l.relation = st.indexrelid)');
      END IF;

      INSERT INTO last_stat_indexes(
        server_id,
        sample_id,
        datid,
        relid,
        indexrelid,
        schemaname,
        relname,
        indexrelname,
        idx_scan,
        idx_tup_read,
        idx_tup_fetch,
        idx_blks_read,
        idx_blks_hit,
        relsize,
        relsize_diff,
        tablespaceid,
        indisunique,
        in_sample,
        relpages_bytes,
        relpages_bytes_diff
      )
      SELECT
        sserver_id,
        s_id,
        qres.datid,
        relid,
        indexrelid,
        schemaname,
        relname,
        indexrelname,
        dbl.idx_scan AS idx_scan,
        dbl.idx_tup_read AS idx_tup_read,
        dbl.idx_tup_fetch AS idx_tup_fetch,
        dbl.idx_blks_read AS idx_blks_read,
        dbl.idx_blks_hit AS idx_blks_hit,
        dbl.relsize AS relsize,
        dbl.relsize_diff AS relsize_diff,
        CASE WHEN tablespaceid=0 THEN qres.tablespaceid ELSE tablespaceid END tablespaceid,
        indisunique,
        false,
        dbl.relpages_bytes,
        dbl.relpages_bytes_diff
      FROM dblink('server_db_connection', t_query)
      AS dbl (
         relid          oid,
         indexrelid     oid,
         schemaname     name,
         relname        name,
         indexrelname   name,
         idx_scan       bigint,
         idx_tup_read   bigint,
         idx_tup_fetch  bigint,
         idx_blks_read  bigint,
         idx_blks_hit   bigint,
         relsize        bigint,
         relsize_diff   bigint,
         tablespaceid   oid,
         indisunique    bool,
         relpages_bytes bigint,
         relpages_bytes_diff  bigint
      );

      EXECUTE format('ANALYZE last_stat_indexes_srv%1$s',
        sserver_id);

      IF (result #>> '{collect_timings}')::boolean THEN
        result := jsonb_set(result,ARRAY['timings',format('db:%s collect indexes stats',qres.datname),'end'],to_jsonb(clock_timestamp()));
        result := jsonb_set(result,ARRAY['timings',format('db:%s collect functions stats',qres.datname)],jsonb_build_object('start',clock_timestamp()));
      END IF;

      -- Generate Function stats query
      t_query := 'SELECT f.funcid,'
        'f.schemaname,'
        'f.funcname,'
        'pg_get_function_arguments(f.funcid) AS funcargs,'
        'f.calls,'
        'f.total_time,'
        'f.self_time,'
        'p.prorettype::regtype::text =''trigger'' AS trg_fn '
      'FROM pg_catalog.pg_stat_user_functions f '
        'JOIN pg_catalog.pg_proc p ON (f.funcid = p.oid) '
      'WHERE pg_get_function_arguments(f.funcid) IS NOT NULL';

      INSERT INTO last_stat_user_functions(
        server_id,
        sample_id,
        datid,
        funcid,
        schemaname,
        funcname,
        funcargs,
        calls,
        total_time,
        self_time,
        trg_fn
      )
      SELECT
        sserver_id,
        s_id,
        qres.datid,
        funcid,
        schemaname,
        funcname,
        funcargs,
        dbl.calls AS calls,
        dbl.total_time AS total_time,
        dbl.self_time AS self_time,
        dbl.trg_fn
      FROM dblink('server_db_connection', t_query)
      AS dbl (
         funcid       oid,
         schemaname   name,
         funcname     name,
         funcargs     text,
         calls        bigint,
         total_time   double precision,
         self_time    double precision,
         trg_fn       boolean
      );

      EXECUTE format('ANALYZE last_stat_user_functions_srv%1$s',
        sserver_id);

      PERFORM dblink('server_db_connection', 'COMMIT');
      PERFORM dblink_disconnect('server_db_connection');
      IF (result #>> '{collect_timings}')::boolean THEN
        result := jsonb_set(result,ARRAY['timings',format('db:%s collect functions stats',qres.datname),'end'],to_jsonb(clock_timestamp()));
      END IF;
    END LOOP;
   RETURN result;
END;
$$ LANGUAGE plpgsql;
CREATE FUNCTION profile_checkavail_statements_jit_stats(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS BOOLEAN
SET search_path=@extschema@ AS $$
    SELECT COALESCE(sum(jit_functions + jit_inlining_count + jit_optimization_count + jit_emission_count), 0) > 0
    FROM sample_statements_total
    WHERE server_id = sserver_id AND sample_id BETWEEN start_id + 1 AND end_id
$$ LANGUAGE sql;
CREATE FUNCTION statements_stats(IN sserver_id integer, IN start_id integer, IN end_id integer, IN topn integer)
RETURNS TABLE(
        dbname              name,
        datid               oid,
        calls               bigint,
        plans               bigint,
        total_exec_time     double precision,
        total_plan_time     double precision,
        blk_read_time       double precision,
        blk_write_time      double precision,
        trg_fn_total_time   double precision,
        shared_gets         bigint,
        local_gets          bigint,
        shared_blks_dirtied bigint,
        local_blks_dirtied  bigint,
        temp_blks_read      bigint,
        temp_blks_written   bigint,
        local_blks_read     bigint,
        local_blks_written  bigint,
        statements          bigint,
        wal_bytes           bigint,
        jit_functions       bigint,
        jit_generation_time double precision,
        jit_inlining_count  bigint,
        jit_inlining_time   double precision,
        jit_optimization_count  bigint,
        jit_optimization_time   double precision,
        jit_emission_count  bigint,
        jit_emission_time   double precision
)
SET search_path=@extschema@ AS $$
    SELECT
        sample_db.datname AS dbname,
        sample_db.datid AS datid,
        sum(st.calls)::bigint AS calls,
        sum(st.plans)::bigint AS plans,
        sum(st.total_exec_time)/1000::double precision AS total_exec_time,
        sum(st.total_plan_time)/1000::double precision AS total_plan_time,
        sum(st.blk_read_time)/1000::double precision AS blk_read_time,
        sum(st.blk_write_time)/1000::double precision AS blk_write_time,
        (sum(trg.total_time)/1000)::double precision AS trg_fn_total_time,
        sum(st.shared_blks_hit)::bigint + sum(st.shared_blks_read)::bigint AS shared_gets,
        sum(st.local_blks_hit)::bigint + sum(st.local_blks_read)::bigint AS local_gets,
        sum(st.shared_blks_dirtied)::bigint AS shared_blks_dirtied,
        sum(st.local_blks_dirtied)::bigint AS local_blks_dirtied,
        sum(st.temp_blks_read)::bigint AS temp_blks_read,
        sum(st.temp_blks_written)::bigint AS temp_blks_written,
        sum(st.local_blks_read)::bigint AS local_blks_read,
        sum(st.local_blks_written)::bigint AS local_blks_written,
        sum(st.statements)::bigint AS statements,
        sum(st.wal_bytes)::bigint AS wal_bytes,
        sum(st.jit_functions)::bigint AS jit_functions,
        sum(st.jit_generation_time)/1000::double precision AS jit_generation_time,
        sum(st.jit_inlining_count)::bigint AS jit_inlining_count,
        sum(st.jit_inlining_time)/1000::double precision AS jit_inlining_time,
        sum(st.jit_optimization_count)::bigint AS jit_optimization_count,
        sum(st.jit_optimization_time)/1000::double precision AS jit_optimization_time,
        sum(st.jit_emission_count)::bigint AS jit_emission_count,
        sum(st.jit_emission_time)/1000::double precision AS jit_emission_time
    FROM sample_statements_total st
        LEFT OUTER JOIN sample_stat_user_func_total trg
          ON (st.server_id = trg.server_id AND st.sample_id = trg.sample_id AND st.datid = trg.datid AND trg.trg_fn)
        -- Database name
        JOIN sample_stat_database sample_db
        ON (st.server_id=sample_db.server_id AND st.sample_id=sample_db.sample_id AND st.datid=sample_db.datid)
    WHERE st.server_id = sserver_id AND st.sample_id BETWEEN start_id + 1 AND end_id
    GROUP BY sample_db.datname, sample_db.datid;
$$ LANGUAGE sql;
CREATE FUNCTION dbagg_jit_stats_htbl(IN report_context jsonb, IN sserver_id integer)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    --Cursor for db stats
    c_dbstats CURSOR(start1_id integer, end1_id integer, topn integer) FOR
    SELECT
        COALESCE(dbname,'Total') as dbname_t,
        NULLIF(sum(calls), 0) as calls,
        NULLIF(sum(total_exec_time), 0.0) as total_exec_time,
        NULLIF(sum(total_plan_time), 0.0) as total_plan_time,
        NULLIF(sum(jit_functions), 0) as jit_functions,
        NULLIF(sum(jit_generation_time), 0.0) as jit_generation_time,
        NULLIF(sum(jit_inlining_count), 0) as jit_inlining_count,
        NULLIF(sum(jit_inlining_time), 0.0) as jit_inlining_time,
        NULLIF(sum(jit_optimization_count), 0) as jit_optimization_count,
        NULLIF(sum(jit_optimization_time), 0.0) as jit_optimization_time,
        NULLIF(sum(jit_emission_count), 0) as jit_emission_count,
        NULLIF(sum(jit_emission_time), 0.0) as jit_emission_time
    FROM statements_stats(sserver_id,start1_id,end1_id,topn)
    WHERE
        jit_functions +
        jit_inlining_count +
        jit_optimization_count +
        jit_emission_count > 0
    GROUP BY ROLLUP(dbname)
    ORDER BY dbname NULLS LAST;

    r_result RECORD;
BEGIN
    -- Database stats TPLs
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {stattbl}>'
          '<tr>'
            '<th rowspan="2">Database</th>'
            '<th rowspan="2"title="Number of query executions">Calls</th>'
            '{planning_times?time_hdr}'
            '<th colspan="2">Generation</th>'
            '<th colspan="2">Inlining</th>'
            '<th colspan="2">Optimization</th>'
            '<th colspan="2">Emission</th>'
          '</tr>'
          '<tr>'
            '{planning_times?plan_time_hdr}'
            '<th title="Time spent executing queries">Exec</th>'
            '<th title="Total number of functions JIT-compiled by the statements">Count</th>'
            '<th title="Total time spent by the statements on generating JIT code">Gen. time</th>'
            '<th title="Number of times functions have been inlined">Count</th>'
            '<th title="Total time spent by statements on inlining functions">Time</th>'
            '<th title="Number of times statements has been optimized">Count</th>'
            '<th title="Total time spent by statements on optimizing">Time</th>'
            '<th title="Number of times code has been emitted">Count</th>'
            '<th title="Total time spent by statements on emitting code">Time</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'stdb_tpl',
        '<tr>'
          '<td>%1$s</td>'
          '<td {value}>%2$s</td>'
          '{planning_times?plan_time_cell}'
          '<td {value}>%4$s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>',
      '!planning_times?time_hdr', -- Time header for stat_statements less then v1.8
        '<th colspan="1">Time (s)</th>',
      'planning_times?time_hdr', -- Time header for stat_statements v1.8 - added plan time field
        '<th colspan="2">Time (s)</th>',
      'planning_times?plan_time_hdr',
        '<th title="Time spent planning queries">Plan</th>',
      'planning_times?plan_time_cell',
        '<td {value}>%3$s</td>'
    );

    -- apply settings to templates
    jtab_tpl := jsonb_replace(report_context, jtab_tpl);

    -- Reporting summary databases stats
    FOR r_result IN c_dbstats(
        (report_context #>> '{report_properties,start1_id}')::integer,
        (report_context #>> '{report_properties,end1_id}')::integer,
        (report_context #>> '{report_properties,topn}')::integer
      )
    LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['stdb_tpl'],
            r_result.dbname_t,
            r_result.calls,
            round(CAST(r_result.total_plan_time AS numeric),2),
            round(CAST(r_result.total_exec_time AS numeric),2),
            r_result.jit_functions,
            round(CAST(r_result.jit_generation_time AS numeric),2),
            r_result.jit_inlining_count,
            round(CAST(r_result.jit_inlining_time AS numeric),2),
            r_result.jit_optimization_count,
            round(CAST(r_result.jit_optimization_time AS numeric),2),
            r_result.jit_emission_count,
            round(CAST(r_result.jit_emission_time AS numeric),2)
        );
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    END IF;

    RETURN report;
END;
$$ LANGUAGE plpgsql;
CREATE FUNCTION dbagg_jit_stats_diff_htbl(IN report_context jsonb, IN sserver_id integer)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    --Cursor for db stats
    c_dbstats CURSOR(start1_id integer, end1_id integer,
      start2_id integer, end2_id integer, topn integer)
    FOR
    SELECT
        COALESCE(COALESCE(st1.dbname,st2.dbname),'Total') as dbname,
        NULLIF(sum(st1.calls), 0) as calls1,
        NULLIF(sum(st1.total_exec_time), 0.0) as total_exec_time1,
        NULLIF(sum(st1.total_plan_time), 0.0) as total_plan_time1,
        NULLIF(sum(st1.jit_functions), 0) as jit_functions1,
        NULLIF(sum(st1.jit_generation_time), 0.0) as jit_generation_time1,
        NULLIF(sum(st1.jit_inlining_count), 0) as jit_inlining_count1,
        NULLIF(sum(st1.jit_inlining_time), 0.0) as jit_inlining_time1,
        NULLIF(sum(st1.jit_optimization_count), 0) as jit_optimization_count1,
        NULLIF(sum(st1.jit_optimization_time), 0.0) as jit_optimization_time1,
        NULLIF(sum(st1.jit_emission_count), 0) as jit_emission_count1,
        NULLIF(sum(st1.jit_emission_time), 0.0) as jit_emission_time1,
        NULLIF(sum(st2.calls), 0) as calls2,
        NULLIF(sum(st2.total_exec_time), 0.0) as total_exec_time2,
        NULLIF(sum(st2.total_plan_time), 0.0) as total_plan_time2,
        NULLIF(sum(st2.jit_functions), 0) as jit_functions2,
        NULLIF(sum(st2.jit_generation_time), 0.0) as jit_generation_time2,
        NULLIF(sum(st2.jit_inlining_count), 0) as jit_inlining_count2,
        NULLIF(sum(st2.jit_inlining_time), 0.0) as jit_inlining_time2,
        NULLIF(sum(st2.jit_optimization_count), 0) as jit_optimization_count2,
        NULLIF(sum(st2.jit_optimization_time), 0.0) as jit_optimization_time2,
        NULLIF(sum(st2.jit_emission_count), 0) as jit_emission_count2,
        NULLIF(sum(st2.jit_emission_time), 0.0) as jit_emission_time2
    FROM statements_stats(sserver_id,start1_id,end1_id,topn) st1
        FULL OUTER JOIN statements_stats(sserver_id,start2_id,end2_id,topn) st2 USING (datid)
    WHERE
        COALESCE(st1.jit_functions +
        st1.jit_inlining_count +
        st1.jit_optimization_count +
        st1.jit_emission_count, 0) +
        COALESCE(st2.jit_functions +
        st2.jit_inlining_count +
        st2.jit_optimization_count +
        st2.jit_emission_count, 0) > 0
    GROUP BY ROLLUP(COALESCE(st1.dbname,st2.dbname))
    ORDER BY COALESCE(st1.dbname,st2.dbname) NULLS LAST;

    r_result RECORD;
BEGIN
    -- Statements stats per database TPLs
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {difftbl}>'
          '<tr>'
            '<th rowspan="2">Database</th>'
            '<th rowspan="2">I</th>'
            '<th rowspan="2" title="Number of query executions">Calls</th>'
            '{planning_times?time_hdr}'
            '<th colspan="2">Generation</th>'
            '<th colspan="2">Inlining</th>'
            '<th colspan="2">Optimization</th>'
            '<th colspan="2">Emission</th>'
          '</tr>'
          '<tr>'
            '{planning_times?plan_time_hdr}'
            '<th title="Time spent executing queries">Exec</th>'
            '<th title="Total number of functions JIT-compiled by the statements">Count</th>'
            '<th title="Total time spent by the statements on generating JIT code">Gen. time</th>'
            '<th title="Number of times functions have been inlined">Count</th>'
            '<th title="Total time spent by statements on inlining functions">Time</th>'
            '<th title="Number of times statements has been optimized">Count</th>'
            '<th title="Total time spent by statements on optimizing">Time</th>'
            '<th title="Number of times code has been emitted">Count</th>'
            '<th title="Total time spent by statements on emitting code">Time</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'stdb_tpl',
        '<tr {interval1}>'
          '<td {rowtdspanhdr}>%1$s</td>'
          '<td {label} {title1}>1</td>'
          '<td {value}>%2$s</td>'
          '{planning_times?plan_time_cell1}'
          '<td {value}>%4$s</td>'
          '<td {value}>%5$s</td>'
          '<td {value}>%6$s</td>'
          '<td {value}>%7$s</td>'
          '<td {value}>%8$s</td>'
          '<td {value}>%9$s</td>'
          '<td {value}>%10$s</td>'
          '<td {value}>%11$s</td>'
          '<td {value}>%12$s</td>'
        '</tr>'
        '<tr {interval2}>'
          '<td {label} {title2}>2</td>'
          '<td {value}>%13$s</td>'
          '{planning_times?plan_time_cell2}'
          '<td {value}>%15$s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>'
        '<tr style="visibility:collapse"></tr>',
      '!planning_times?time_hdr', -- Time header for stat_statements less then v1.8
        '<th colspan="1">Time (s)</th>',
      'planning_times?time_hdr', -- Time header for stat_statements v1.8 - added plan time field
        '<th colspan="2">Time (s)</th>',
      'planning_times?plan_time_hdr',
        '<th title="Time spent planning queries">Plan</th>',
      'planning_times?plan_time_cell1',
        '<td {value}>%3$s</td>',
      'planning_times?plan_time_cell2',
        '<td {value}>%14$s</td>'
    );
    -- apply settings to templates
    jtab_tpl := jsonb_replace(report_context, jtab_tpl);

    -- Reporting summary databases stats
    FOR r_result IN c_dbstats(
        (report_context #>> '{report_properties,start1_id}')::integer,
        (report_context #>> '{report_properties,end1_id}')::integer,
        (report_context #>> '{report_properties,start2_id}')::integer,
        (report_context #>> '{report_properties,end2_id}')::integer,
        (report_context #>> '{report_properties,topn}')::integer
      )
    LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['stdb_tpl'],
            r_result.dbname,
            r_result.calls1,
            round(CAST(r_result.total_plan_time1 AS numeric),2),
            round(CAST(r_result.total_exec_time1 AS numeric),2),
            r_result.jit_functions1,
            round(CAST(r_result.jit_generation_time1 AS numeric),2),
            r_result.jit_inlining_count1,
            round(CAST(r_result.jit_inlining_time1 AS numeric),2),
            r_result.jit_optimization_count1,
            round(CAST(r_result.jit_optimization_time1 AS numeric),2),
            r_result.jit_emission_count1,
            round(CAST(r_result.jit_emission_time1 AS numeric),2),
            r_result.calls2,
            round(CAST(r_result.total_plan_time2 AS numeric),2),
            round(CAST(r_result.total_exec_time2 AS numeric),2),
            r_result.jit_functions2,
            round(CAST(r_result.jit_generation_time2 AS numeric),2),
            r_result.jit_inlining_count2,
            round(CAST(r_result.jit_inlining_time2 AS numeric),2),
            r_result.jit_optimization_count2,
            round(CAST(r_result.jit_optimization_time2 AS numeric),2),
            r_result.jit_emission_count2,
            round(CAST(r_result.jit_emission_time2 AS numeric),2)
        );
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    END IF;

    RETURN report;
END;
$$ LANGUAGE plpgsql;
CREATE FUNCTION top_statements(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS TABLE(
    server_id               integer,
    datid                   oid,
    dbname                  name,
    userid                  oid,
    username                name,
    queryid                 bigint,
    toplevel                boolean,
    plans                   bigint,
    plans_pct               float,
    calls                   bigint,
    calls_pct               float,
    total_time              double precision,
    total_time_pct          double precision,
    total_plan_time         double precision,
    plan_time_pct           float,
    total_exec_time         double precision,
    total_exec_time_pct     float,
    exec_time_pct           float,
    min_exec_time           double precision,
    max_exec_time           double precision,
    mean_exec_time          double precision,
    stddev_exec_time        double precision,
    min_plan_time           double precision,
    max_plan_time           double precision,
    mean_plan_time          double precision,
    stddev_plan_time        double precision,
    rows                    bigint,
    shared_blks_hit         bigint,
    shared_hit_pct          float,
    shared_blks_read        bigint,
    read_pct                float,
    shared_blks_fetched     bigint,
    shared_blks_fetched_pct float,
    shared_blks_dirtied     bigint,
    dirtied_pct             float,
    shared_blks_written     bigint,
    tot_written_pct         float,
    backend_written_pct     float,
    local_blks_hit          bigint,
    local_hit_pct           float,
    local_blks_read         bigint,
    local_blks_fetched      bigint,
    local_blks_dirtied      bigint,
    local_blks_written      bigint,
    temp_blks_read          bigint,
    temp_blks_written       bigint,
    blk_read_time           double precision,
    blk_write_time          double precision,
    io_time                 double precision,
    io_time_pct             float,
    temp_read_total_pct     float,
    temp_write_total_pct    float,
    local_read_total_pct    float,
    local_write_total_pct   float,
    wal_records             bigint,
    wal_fpi                 bigint,
    wal_bytes               numeric,
    wal_bytes_pct           float,
    user_time               double precision,
    system_time             double precision,
    reads                   bigint,
    writes                  bigint,
    jit_functions           bigint,
    jit_generation_time     double precision,
    jit_inlining_count      bigint,
    jit_inlining_time       double precision,
    jit_optimization_count  bigint,
    jit_optimization_time   double precision,
    jit_emission_count      bigint,
    jit_emission_time       double precision
) SET search_path=@extschema@ AS $$
    WITH
      tot AS (
        SELECT
            COALESCE(sum(total_plan_time), 0.0) + sum(total_exec_time) AS total_time,
            sum(blk_read_time) AS blk_read_time,
            sum(blk_write_time) AS blk_write_time,
            sum(shared_blks_hit) AS shared_blks_hit,
            sum(shared_blks_read) AS shared_blks_read,
            sum(shared_blks_dirtied) AS shared_blks_dirtied,
            sum(temp_blks_read) AS temp_blks_read,
            sum(temp_blks_written) AS temp_blks_written,
            sum(local_blks_read) AS local_blks_read,
            sum(local_blks_written) AS local_blks_written,
            sum(calls) AS calls,
            sum(plans) AS plans
        FROM sample_statements_total st
        WHERE st.server_id = sserver_id AND st.sample_id BETWEEN start_id + 1 AND end_id
      ),
      totbgwr AS (
        SELECT
          sum(buffers_checkpoint) + sum(buffers_clean) + sum(buffers_backend) AS written,
          sum(buffers_backend) AS buffers_backend,
          sum(wal_size) AS wal_size
        FROM sample_stat_cluster
        WHERE server_id = sserver_id AND sample_id BETWEEN start_id + 1 AND end_id
      )
    SELECT
        st.server_id as server_id,
        st.datid as datid,
        sample_db.datname as dbname,
        st.userid as userid,
        rl.username as username,
        st.queryid as queryid,
        st.toplevel as toplevel,
        sum(st.plans)::bigint as plans,
        (sum(st.plans)*100/NULLIF(min(tot.plans), 0))::float as plans_pct,
        sum(st.calls)::bigint as calls,
        (sum(st.calls)*100/NULLIF(min(tot.calls), 0))::float as calls_pct,
        (sum(st.total_exec_time) + COALESCE(sum(st.total_plan_time), 0.0))/1000 as total_time,
        (sum(st.total_exec_time) + COALESCE(sum(st.total_plan_time), 0.0))*100/NULLIF(min(tot.total_time), 0) as total_time_pct,
        sum(st.total_plan_time)/1000::double precision as total_plan_time,
        sum(st.total_plan_time)*100/NULLIF(sum(st.total_exec_time) + COALESCE(sum(st.total_plan_time), 0.0), 0) as plan_time_pct,
        sum(st.total_exec_time)/1000::double precision as total_exec_time,
        sum(st.total_exec_time)*100/NULLIF(min(tot.total_time), 0) as total_exec_time_pct,
        sum(st.total_exec_time)*100/NULLIF(sum(st.total_exec_time) + COALESCE(sum(st.total_plan_time), 0.0), 0) as exec_time_pct,
        min(st.min_exec_time) as min_exec_time,
        max(st.max_exec_time) as max_exec_time,
        sum(st.mean_exec_time*st.calls)/NULLIF(sum(st.calls), 0) as mean_exec_time,
        sqrt(sum((power(st.stddev_exec_time,2)+power(st.mean_exec_time,2))*st.calls)/NULLIF(sum(st.calls),0)-power(sum(st.mean_exec_time*st.calls)/NULLIF(sum(st.calls),0),2)) as stddev_exec_time,
        min(st.min_plan_time) as min_plan_time,
        max(st.max_plan_time) as max_plan_time,
        sum(st.mean_plan_time*st.plans)/NULLIF(sum(st.plans),0) as mean_plan_time,
        sqrt(sum((power(st.stddev_plan_time,2)+power(st.mean_plan_time,2))*st.plans)/NULLIF(sum(st.plans),0)-power(sum(st.mean_plan_time*st.plans)/NULLIF(sum(st.plans),0),2)) as stddev_plan_time,
        sum(st.rows)::bigint as rows,
        sum(st.shared_blks_hit)::bigint as shared_blks_hit,
        (sum(st.shared_blks_hit) * 100 / NULLIF(sum(st.shared_blks_hit) + sum(st.shared_blks_read), 0))::float as shared_hit_pct,
        sum(st.shared_blks_read)::bigint as shared_blks_read,
        (sum(st.shared_blks_read) * 100 / NULLIF(min(tot.shared_blks_read), 0))::float as read_pct,
        (sum(st.shared_blks_hit) + sum(st.shared_blks_read))::bigint as shared_blks_fetched,
        ((sum(st.shared_blks_hit) + sum(st.shared_blks_read)) * 100 / NULLIF(min(tot.shared_blks_hit) + min(tot.shared_blks_read), 0))::float as shared_blks_fetched_pct,
        sum(st.shared_blks_dirtied)::bigint as shared_blks_dirtied,
        (sum(st.shared_blks_dirtied) * 100 / NULLIF(min(tot.shared_blks_dirtied), 0))::float as dirtied_pct,
        sum(st.shared_blks_written)::bigint as shared_blks_written,
        (sum(st.shared_blks_written) * 100 / NULLIF(min(totbgwr.written), 0))::float as tot_written_pct,
        (sum(st.shared_blks_written) * 100 / NULLIF(min(totbgwr.buffers_backend), 0))::float as backend_written_pct,
        sum(st.local_blks_hit)::bigint as local_blks_hit,
        (sum(st.local_blks_hit) * 100 / NULLIF(sum(st.local_blks_hit) + sum(st.local_blks_read),0))::float as local_hit_pct,
        sum(st.local_blks_read)::bigint as local_blks_read,
        (sum(st.local_blks_hit) + sum(st.local_blks_read))::bigint as local_blks_fetched,
        sum(st.local_blks_dirtied)::bigint as local_blks_dirtied,
        sum(st.local_blks_written)::bigint as local_blks_written,
        sum(st.temp_blks_read)::bigint as temp_blks_read,
        sum(st.temp_blks_written)::bigint as temp_blks_written,
        sum(st.blk_read_time)/1000::double precision as blk_read_time,
        sum(st.blk_write_time)/1000::double precision as blk_write_time,
        (sum(st.blk_read_time) + sum(st.blk_write_time))/1000::double precision as io_time,
        (sum(st.blk_read_time) + sum(st.blk_write_time)) * 100 / NULLIF(min(tot.blk_read_time) + min(tot.blk_write_time),0) as io_time_pct,
        (sum(st.temp_blks_read) * 100 / NULLIF(min(tot.temp_blks_read), 0))::float as temp_read_total_pct,
        (sum(st.temp_blks_written) * 100 / NULLIF(min(tot.temp_blks_written), 0))::float as temp_write_total_pct,
        (sum(st.local_blks_read) * 100 / NULLIF(min(tot.local_blks_read), 0))::float as local_read_total_pct,
        (sum(st.local_blks_written) * 100 / NULLIF(min(tot.local_blks_written), 0))::float as local_write_total_pct,
        sum(st.wal_records)::bigint as wal_records,
        sum(st.wal_fpi)::bigint as wal_fpi,
        sum(st.wal_bytes) as wal_bytes,
        (sum(st.wal_bytes) * 100 / NULLIF(min(totbgwr.wal_size), 0))::float wal_bytes_pct,
        -- kcache stats
        COALESCE(sum(kc.exec_user_time), 0.0) + COALESCE(sum(kc.plan_user_time), 0.0) as user_time,
        COALESCE(sum(kc.exec_system_time), 0.0) + COALESCE(sum(kc.plan_system_time), 0.0) as system_time,
        (COALESCE(sum(kc.exec_reads), 0) + COALESCE(sum(kc.plan_reads), 0))::bigint as reads,
        (COALESCE(sum(kc.exec_writes), 0) + COALESCE(sum(kc.plan_writes), 0))::bigint as writes,
        sum(st.jit_functions)::bigint AS jit_functions,
        sum(st.jit_generation_time)/1000::double precision AS jit_generation_time,
        sum(st.jit_inlining_count)::bigint AS jit_inlining_count,
        sum(st.jit_inlining_time)/1000::double precision AS jit_inlining_time,
        sum(st.jit_optimization_count)::bigint AS jit_optimization_count,
        sum(st.jit_optimization_time)/1000::double precision AS jit_optimization_time,
        sum(st.jit_emission_count)::bigint AS jit_emission_count,
        sum(st.jit_emission_time)/1000::double precision AS jit_emission_time
    FROM sample_statements st
        -- User name
        JOIN roles_list rl USING (server_id, userid)
        -- Database name
        JOIN sample_stat_database sample_db
        USING (server_id, sample_id, datid)
        -- kcache join
        LEFT OUTER JOIN sample_kcache kc USING(server_id, sample_id, userid, datid, queryid, toplevel)
        -- Total stats
        CROSS JOIN tot CROSS JOIN totbgwr
    WHERE st.server_id = sserver_id AND st.sample_id BETWEEN start_id + 1 AND end_id
    GROUP BY
      st.server_id,
      st.datid,
      sample_db.datname,
      st.userid,
      rl.username,
      st.queryid,
      st.toplevel
$$ LANGUAGE sql;
CREATE FUNCTION top_jit_htbl(IN report_context jsonb, IN sserver_id integer)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report      text := '';
    tab_row     text := '';
    jtab_tpl    jsonb;
    r_result RECORD;

    --Cursor for top(cnt) queries ordered by JIT total time
    c_jit_time CURSOR(topn integer) FOR
    SELECT * FROM (SELECT
        st.datid,
        st.dbname,
        st.userid,
        st.username,
        st.queryid,
        st.toplevel,
        NULLIF(st.total_plan_time, 0) as total_plan_time,
        NULLIF(st.total_exec_time, 0) as total_exec_time,
        NULLIF(st.io_time, 0) as io_time,
        NULLIF(st.blk_read_time, 0.0) as blk_read_time,
        NULLIF(st.blk_write_time, 0.0) as blk_write_time,
        NULLIF(st.jit_functions, 0) as jit_functions,
        NULLIF(st.jit_generation_time, 0) as jit_generation_time,
        NULLIF(st.jit_inlining_count, 0) as jit_inlining_count,
        NULLIF(st.jit_inlining_time, 0) as jit_inlining_time,
        NULLIF(st.jit_optimization_count, 0) as jit_optimization_count,
        NULLIF(st.jit_optimization_time, 0) as jit_optimization_time,
        NULLIF(st.jit_emission_count, 0) as jit_emission_count,
        NULLIF(st.jit_emission_time, 0) as jit_emission_time,
        st.jit_generation_time + st.jit_inlining_time + st.jit_optimization_time + st.jit_emission_time as jit_total_time,
        row_number() over(order by st.total_exec_time DESC NULLS LAST) as num_exec_time,
        row_number() over(order by st.total_time DESC NULLS LAST) as num_total_time
    FROM top_statements1 st
    ORDER BY (st.jit_generation_time + st.jit_inlining_time + st.jit_optimization_time + st.jit_emission_time) DESC NULLS LAST,
      st.queryid ASC,
      st.toplevel ASC,
      st.datid ASC,
      st.userid ASC,
      st.dbname ASC,
      st.username ASC
      ) t1
    WHERE jit_functions + jit_inlining_count + jit_optimization_count + jit_emission_count > 0
      AND least(
          num_exec_time,
          num_total_time
          ) <= topn;
BEGIN
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {stattbl}>'
          '<tr>'
            '<th rowspan="2">Query ID</th>'
            '<th rowspan="2">Database</th>'
            '<th rowspan="2">User</th>'
            '<th rowspan="2" title="Total time spent on JIT in seconds">JIT total (s)</th>'
            '<th colspan="2">Generation</th>'
            '<th colspan="2">Inlining</th>'
            '<th colspan="2">Optimization</th>'
            '<th colspan="2">Emission</th>'
            '<th colspan="{planning_times?planning_colspan}">Time (s)</th>'
            '{io_times?iotime_hdr1}'
          '</tr>'
          '<tr>'
            '<th title="Total number of functions JIT-compiled by the statement.">Count</th>'
            '<th title="Total time spent by the statement on generating JIT code, in seconds.">Time (s)</th>'
            '<th title="Number of times functions have been inlined.">Count</th>'
            '<th title="Total time spent by the statement on inlining functions, in seconds.">Time (s)</th>'
            '<th title="Number of times the statement has been optimized.">Count</th>'
            '<th title="Total time spent by the statement on optimizing, in seconds.">Time (s)</th>'
            '<th title="Number of times code has been emitted.">Count</th>'
            '<th title="Total time spent by the statement on emitting code, in seconds.">Time (s)</th>'
            '{planning_times?planning_hdr}'
            '<th title="Time spent executing statement">Exec</th>'
            '{io_times?iotime_hdr2}'
          '</tr>'
          '{rows}'
        '</table>',
      'stmt_tpl',
        '<tr>'
          '<td class="mono hdr" id="%20$s"><p><a HREF="#%2$s">%2$s</a></p>'
          '<p><small>[%3$s]</small>%1$s</p></td>'
          '<td>%4$s</td>'
          '<td>%5$s</td>'
          '<td {value}>%6$s</td>'
          '<td {value}>%12$s</td>'
          '<td {value}>%13$s</td>'
          '<td {value}>%14$s</td>'
          '<td {value}>%15$s</td>'
          '<td {value}>%16$s</td>'
          '<td {value}>%17$s</td>'
          '<td {value}>%18$s</td>'
          '<td {value}>%19$s</td>'
          '{planning_times?planning_row}'
          '<td {value}>%9$s</td>'
          '{io_times?iotime_row}'
        '</tr>',
      'nested_tpl',
        ' <small title="Nested level">(N)</small>',
      'planning_times?planning_colspan','2',
      'planning_times?planning_hdr',
        '<th title="Time spent planning statement">Plan</th>',
      'planning_times?planning_row',
        '<td {value}>%8$s</td>',
      'io_times?iotime_hdr1',
        '<th colspan="2">I/O time (s)</th>',
      'io_times?iotime_hdr2',
        '<th title="Time spent reading blocks by statement">Read</th>'
        '<th title="Time spent writing blocks by statement">Write</th>',
      'io_times?iotime_row',
        '<td {value}>%10$s</td>'
        '<td {value}>%11$s</td>'
      );

    -- apply settings to templates
    jtab_tpl := jsonb_replace(report_context, jtab_tpl);

    -- Reporting on top queries
    FOR r_result IN c_jit_time(
        (report_context #>> '{report_properties,topn}')::integer
      )
    LOOP
        tab_row := format(
            jtab_tpl #>> ARRAY['stmt_tpl'],
            CASE WHEN NOT r_result.toplevel THEN jtab_tpl #>> ARRAY['nested_tpl'] ELSE '' END,  -- 1
            to_hex(r_result.queryid), -- 2
            left(md5(r_result.userid::text || r_result.datid::text || r_result.queryid::text), 10), -- 3
            r_result.dbname, -- 4
            r_result.username, -- 5
            round(CAST(r_result.jit_total_time AS numeric), 2), -- 6
            round(CAST(r_result.total_plan_time + r_result.total_exec_time AS numeric),2), -- 7
            round(CAST(r_result.total_plan_time AS numeric),2),  -- 8
            round(CAST(r_result.total_exec_time AS numeric),2),  -- 9
            round(CAST(r_result.blk_read_time AS numeric),2), -- 10
            round(CAST(r_result.blk_write_time AS numeric),2), -- 11
            r_result.jit_functions, -- 12
            round(CAST(r_result.jit_generation_time AS numeric),2), -- 13
            r_result.jit_inlining_count, -- 14
            round(CAST(r_result.jit_inlining_time AS numeric),2), -- 15
            r_result.jit_optimization_count, -- 16
            round(CAST(r_result.jit_optimization_time AS numeric),2), -- 17
            r_result.jit_emission_count, -- 18
            round(CAST(r_result.jit_emission_time AS numeric),2), -- 19
            format(
                'jit_%s_%s_%s_%s',
                to_hex(r_result.queryid),
                r_result.datid::text,
                r_result.userid::text,
                r_result.toplevel::text)  -- 20
        );
        report := report || tab_row;
        PERFORM collect_queries(
            r_result.userid,r_result.datid,r_result.queryid
        );
    END LOOP;

    IF report != '' THEN
        RETURN replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    ELSE
        RETURN '';
    END IF;
END;
$$ LANGUAGE plpgsql;
CREATE FUNCTION top_jit_diff_htbl(IN report_context jsonb, IN sserver_id integer)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report      text := '';
    tab_row     text := '';
    jtab_tpl    jsonb;

    --Cursor for top(cnt) queries ordered by JIT total time
    c_jit_time CURSOR(topn integer) FOR
    SELECT * FROM (SELECT
        COALESCE(st1.datid,st2.datid) as datid,
        COALESCE(st1.dbname,st2.dbname) as dbname,
        COALESCE(st1.userid,st2.userid) as userid,
        COALESCE(st1.username,st2.username) as username,
        COALESCE(st1.queryid,st2.queryid) as queryid,
        COALESCE(st1.toplevel,st2.toplevel) as toplevel,

        -- top_statements1
        NULLIF(st1.total_plan_time, 0.0) as total_plan_time1,
        NULLIF(st1.total_exec_time, 0.0) as total_exec_time1,
        NULLIF(st1.jit_generation_time + st1.jit_inlining_time +
          st1.jit_optimization_time + st1.jit_emission_time, 0) as total_jit_time1,
        NULLIF(st1.jit_functions, 0) as jit_functions1,
        NULLIF(st1.jit_generation_time, 0) as jit_generation_time1,
        NULLIF(st1.jit_inlining_count, 0) as jit_inlining_count1,
        NULLIF(st1.jit_inlining_time, 0) as jit_inlining_time1,
        NULLIF(st1.jit_optimization_count, 0) as jit_optimization_count1,
        NULLIF(st1.jit_optimization_time, 0) as jit_optimization_time1,
        NULLIF(st1.jit_emission_count, 0) as jit_emission_count1,
        NULLIF(st1.jit_emission_time, 0) as jit_emission_time1,
        NULLIF(st1.blk_read_time, 0.0) as blk_read_time1,
        NULLIF(st1.blk_write_time, 0.0) as blk_write_time1,

        -- top_statements2
        NULLIF(st2.total_time, 0.0) as total_time2,
        NULLIF(st2.total_time_pct, 0.0) as total_time_pct2,
        NULLIF(st2.total_plan_time, 0.0) as total_plan_time2,
        NULLIF(st2.total_exec_time, 0.0) as total_exec_time2,
        NULLIF(st2.jit_generation_time + st2.jit_inlining_time +
          st2.jit_optimization_time + st2.jit_emission_time, 0) as total_jit_time2,
        NULLIF(st2.jit_functions, 0) as jit_functions2,
        NULLIF(st2.jit_generation_time, 0) as jit_generation_time2,
        NULLIF(st2.jit_inlining_count, 0) as jit_inlining_count2,
        NULLIF(st2.jit_inlining_time, 0) as jit_inlining_time2,
        NULLIF(st2.jit_optimization_count, 0) as jit_optimization_count2,
        NULLIF(st2.jit_optimization_time, 0) as jit_optimization_time2,
        NULLIF(st2.jit_emission_count, 0) as jit_emission_count2,
        NULLIF(st2.jit_emission_time, 0) as jit_emission_time2,
        NULLIF(st2.blk_read_time, 0.0) as blk_read_time2,
        NULLIF(st2.blk_write_time, 0.0) as blk_write_time2,

        -- other
        row_number() over (ORDER BY st1.total_exec_time DESC NULLS LAST) as num_exec_time1,
        row_number() over (ORDER BY st2.total_exec_time DESC NULLS LAST) as num_exec_time2,
        row_number() over (ORDER BY st1.total_time DESC NULLS LAST) as num_total_time1,
        row_number() over (ORDER BY st2.total_time DESC NULLS LAST) as num_total_time2

    FROM top_statements1 st1
        FULL OUTER JOIN top_statements2 st2 USING (server_id, datid, userid, queryid, toplevel)
    ORDER BY
        COALESCE(st1.jit_generation_time + st1.jit_inlining_time + st1.jit_optimization_time + st1.jit_emission_time, 0) +
        COALESCE(st2.jit_generation_time + st2.jit_inlining_time + st2.jit_optimization_time + st2.jit_emission_time, 0) DESC,
        COALESCE(st1.queryid,st2.queryid) ASC,
        COALESCE(st1.datid,st2.datid) ASC,
        COALESCE(st1.userid,st2.userid) ASC,
        COALESCE(st1.toplevel,st2.toplevel) ASC
    ) t1
    WHERE
        COALESCE(jit_functions1 + jit_inlining_count1 + jit_optimization_count1 + jit_emission_count1, 0) +
        COALESCE(jit_functions2 + jit_inlining_count2 + jit_optimization_count2 + jit_emission_count2, 0) > 0
      AND least(
          num_exec_time1,
          num_exec_time1,
          num_total_time1,
          num_total_time2
          ) <= topn;

BEGIN
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {difftbl}>'
          '<tr>'
            '<th rowspan="2">Query ID</th>'
            '<th rowspan="2">Database</th>'
            '<th rowspan="2">User</th>'
            '<th rowspan="2">I</th>'
            '<th rowspan="2" title="Total time spent on JIT in seconds">JIT total (s)</th>'
            '<th colspan="2">Generation</th>'
            '<th colspan="2">Inlining</th>'
            '<th colspan="2">Optimization</th>'
            '<th colspan="2">Emission</th>'
           '<th colspan="{planning_times?planning_colspan}">Time (s)</th>'
            '{io_times?iotime_hdr1}'
          '</tr>'
          '<tr>'
            '<th title="Total number of functions JIT-compiled by the statement.">Count</th>'
            '<th title="Total time spent by the statement on generating JIT code, in seconds.">Time (s)</th>'
            '<th title="Number of times functions have been inlined.">Count</th>'
            '<th title="Total time spent by the statement on inlining functions, in seconds.">Time (s)</th>'
            '<th title="Number of times the statement has been optimized.">Count</th>'
            '<th title="Total time spent by the statement on optimizing, in seconds.">Time (s)</th>'
            '<th title="Number of times code has been emitted.">Count</th>'
            '<th title="Total time spent by the statement on emitting code, in seconds.">Time (s)</th>'
            '{planning_times?planning_hdr}'
            '<th title="Time spent executing statement">Exec</th>'
            '{io_times?iotime_hdr2}'
          '</tr>'
          '{rows}'
        '</table>',
      'stmt_tpl',
          '<tr {interval1}>'
          '<td {rowtdspanhdr_mono} id="%34$s"><p><a HREF="#%2$s">%2$s</a></p>'
          '<p><small>[%3$s]</small>%1$s</p></td>'
          '<td {rowtdspanhdr}>%4$s</td>'
          '<td {rowtdspanhdr}>%5$s</td>'
          '<td {label} {title1}>1</td>'
          '<td {value}>%6$s</td>'
          '<td {value}>%7$s</td>'
          '<td {value}>%8$s</td>'
          '<td {value}>%9$s</td>'
          '<td {value}>%10$s</td>'
          '<td {value}>%11$s</td>'
          '<td {value}>%12$s</td>'
          '<td {value}>%13$s</td>'
          '<td {value}>%14$s</td>'
          '{planning_times?planning_row1}'
          '<td {value}>%17$s</td>'
          '{io_times?iotime_row1}'
        '</tr>'
        '<tr {interval2}>'
          '<td {label} {title2}>2</td>'
          '<td {value}>%20$s</td>'
          '<td {value}>%21$s</td>'
          '<td {value}>%22$s</td>'
          '<td {value}>%23$s</td>'
          '<td {value}>%24$s</td>'
          '<td {value}>%25$s</td>'
          '<td {value}>%26$s</td>'
          '<td {value}>%27$s</td>'
          '<td {value}>%28$s</td>'
          '{planning_times?planning_row2}'
          '<td {value}>%31$s</td>'
          '{io_times?iotime_row2}'
        '</tr>'
        '<tr style="visibility:collapse"></tr>',
      'nested_tpl',
        '<small title="Nested level">(N)</small>',
      'planning_times?planning_colspan', '2',
      'planning_times?planning_hdr',
        '<th title="Time spent planning statement">Plan</th>',
      'planning_times?planning_row1',
        '<td {value}>%16$s</td>',
      'planning_times?planning_row2',
        '<td {value}>%30$s</td>',
      'io_times?iotime_hdr1',
        '<th colspan="2">I/O time (s)</th>',
      'io_times?iotime_hdr2',
        '<th title="Time spent reading blocks by statement">Read</th>'
        '<th title="Time spent writing blocks by statement">Write</th>',
      'io_times?iotime_row1',
        '<td {value}>%18$s</td>'
        '<td {value}>%19$s</td>',
      'io_times?iotime_row2',
        '<td {value}>%32$s</td>'
        '<td {value}>%33$s</td>'
    );

    -- apply settings to templates
    jtab_tpl := jsonb_replace(report_context, jtab_tpl);

    -- Reporting on top queries
    FOR r_result IN c_jit_time(
        (report_context #>> '{report_properties,topn}')::integer
      )
    LOOP
        tab_row := format(
            jtab_tpl #>> ARRAY['stmt_tpl'],
            CASE WHEN NOT r_result.toplevel THEN jtab_tpl #>> ARRAY['nested_tpl'] ELSE '' END, -- 1
            to_hex(r_result.queryid), -- 2
            left(md5(r_result.userid::text || r_result.datid::text || r_result.queryid::text), 10), -- 3
            r_result.dbname, -- 4
            r_result.username, -- 5

            -- Sample 1
            -- JIT statistics
            round(CAST(r_result.total_jit_time1 AS numeric),2), -- 6
            r_result.jit_functions1, -- 7
            round(CAST(r_result.jit_generation_time1 AS numeric),2), -- 8
            r_result.jit_inlining_count1, -- 9
            round(CAST(r_result.jit_inlining_time1 AS numeric),2), -- 10
            r_result.jit_optimization_count1, -- 11
            round(CAST(r_result.jit_optimization_time1 AS numeric),2), -- 12
            r_result.jit_emission_count1, -- 13
            round(CAST(r_result.jit_emission_time1 AS numeric),2), -- 14

            -- Time
            round(CAST(r_result.total_plan_time1 + r_result.total_exec_time1 AS numeric),2), -- 15
            round(CAST(r_result.total_plan_time1 AS numeric),2),  -- 16
            round(CAST(r_result.total_exec_time1 AS numeric),2),  -- 17

            -- IO Time
            round(CAST(r_result.blk_read_time1 AS numeric),2), -- 18
            round(CAST(r_result.blk_write_time1 AS numeric),2), -- 19

            -- Sample 2
            -- JIT statistics
            round(CAST(r_result.total_jit_time2 AS numeric),2), -- 20
            r_result.jit_functions2, -- 21
            round(CAST(r_result.jit_generation_time2 AS numeric),2), -- 22
            r_result.jit_inlining_count2, -- 23
            round(CAST(r_result.jit_inlining_time2 AS numeric),2), -- 24
            r_result.jit_optimization_count2, -- 25
            round(CAST(r_result.jit_optimization_time2 AS numeric),2), -- 26
            r_result.jit_emission_count2, -- 27
            round(CAST(r_result.jit_emission_time2 AS numeric),2), -- 28

            -- Time
            round(CAST(r_result.total_plan_time2 + r_result.total_exec_time1 AS numeric),2), -- 29
            round(CAST(r_result.total_plan_time2 AS numeric),2),  -- 30
            round(CAST(r_result.total_exec_time2 AS numeric),2),  -- 31

            -- IO Time
            round(CAST(r_result.blk_read_time2 AS numeric),2), -- 32
            round(CAST(r_result.blk_write_time2 AS numeric),2), -- 33

            -- JIT ID
            format(
                'jit_%s_%s_%s_%s',
                to_hex(r_result.queryid),
                r_result.datid::text,
                r_result.userid::text,
                r_result.toplevel::text)  -- 34
        );

        report := report || tab_row;
        PERFORM collect_queries(
            r_result.userid,r_result.datid,r_result.queryid
        );
    END LOOP;

    IF report != '' THEN
        RETURN replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    ELSE
        RETURN '';
    END IF;

END;
$$ LANGUAGE plpgsql;
CREATE FUNCTION top_elapsed_htbl(IN report_context jsonb, IN sserver_id integer)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report      text := '';
    tab_row     text := '';
    jtab_tpl    jsonb;

    --Cursor for top(cnt) queries ordered by elapsed time
    c_elapsed_time CURSOR(topn integer) FOR
    SELECT
        st.datid,
        st.dbname,
        st.userid,
        st.username,
        st.queryid,
        st.toplevel,
        NULLIF(st.total_time_pct, 0) as total_time_pct,
        NULLIF(st.total_time, 0) as total_time,
        NULLIF(st.total_plan_time, 0) as total_plan_time,
        NULLIF(st.total_exec_time, 0) as total_exec_time,
        NULLIF(st.jit_generation_time + st.jit_inlining_time +
          st.jit_optimization_time + st.jit_emission_time, 0) as total_jit_time,
        st.jit_functions + st.jit_inlining_count + st.jit_optimization_count + st.jit_emission_count > 0 as jit_avail,
        NULLIF(st.blk_read_time, 0.0) as blk_read_time,
        NULLIF(st.blk_write_time, 0.0) as blk_write_time,
        NULLIF(st.user_time, 0.0) as user_time,
        NULLIF(st.system_time, 0.0) as system_time,
        NULLIF(st.calls, 0) as calls,
        NULLIF(st.plans, 0) as plans

    FROM top_statements1 st
    ORDER BY st.total_time DESC,
      st.queryid ASC,
      st.toplevel ASC,
      st.datid ASC,
      st.userid ASC,
      st.dbname ASC,
      st.username ASC
    LIMIT topn;

    r_result RECORD;
BEGIN
    -- This report section is meaningful only when planning timing is available
    IF NOT jsonb_extract_path_text(report_context, 'report_features', 'planning_times')::boolean THEN
      RETURN '';
    END IF;

    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {stattbl}>'
          '<tr>'
            '<th rowspan="2">Query ID</th>'
            '<th rowspan="2">Database</th>'
            '<th rowspan="2">User</th>'
            '<th rowspan="2" title="Elapsed time as a percentage of total cluster elapsed time">%Total</th>'
            '<th colspan="3">Time (s)</th>'
            '{statements_jit_stats?jit_time_hdr}'
            '{io_times?iotime_hdr1}'
            '{kcachestatements?kcache_hdr1}'
            '<th rowspan="2" title="Number of times the statement was planned">Plans</th>'
            '<th rowspan="2" title="Number of times the statement was executed">Executions</th>'
          '</tr>'
          '<tr>'
            '<th title="Time spent by the statement">Elapsed</th>'
            '<th title="Time spent planning statement">Plan</th>'
            '<th title="Time spent executing statement">Exec</th>'
            '{io_times?iotime_hdr2}'
            '{kcachestatements?kcache_hdr2}'
          '</tr>'
          '{rows}'
        '</table>',
      'stmt_tpl',
        '<tr>'
          '<td {mono}><p><a HREF="#%2$s">%2$s</a></p>'
          '<p><small>[%3$s]</small>%1$s</p></td>'
          '<td>%4$s</td>'
          '<td>%5$s</td>'
          '<td {value}>%6$s</td>'
          '<td {value}>%7$s</td>'
          '<td {value}>%8$s</td>'
          '<td {value}>%9$s</td>'
          '{statements_jit_stats?jit_time_row}'
          '{io_times?iotime_row}'
          '{kcachestatements?kcache_row}'
          '<td {value}>%14$s</td>'
          '<td {value}>%15$s</td>'
        '</tr>',
      'nested_tpl',
        ' <small title="Nested level">(N)</small>',
      'statements_jit_stats?jit_time_hdr',
        '<th rowspan="2">JIT<br>time (s)</th>',
      'statements_jit_stats?jit_time_row',
        '<td {value}>%16$s</td>',
      'io_times?iotime_hdr1',
        '<th colspan="2">I/O time (s)</th>',
      'io_times?iotime_hdr2',
        '<th title="Time spent reading blocks by statement">Read</th>'
        '<th title="Time spent writing blocks by statement">Write</th>',
      'io_times?iotime_row',
        '<td {value}>%10$s</td>'
        '<td {value}>%11$s</td>',
      'kcachestatements?kcache_hdr1',
        '<th colspan="2">CPU time (s)</th>',
      'kcachestatements?kcache_hdr2',
        '<th>Usr</th>'
        '<th>Sys</th>',
      'kcachestatements?kcache_row',
        '<td {value}>%12$s</td>'
        '<td {value}>%13$s</td>'
      );
    -- apply settings to templates
    jtab_tpl := jsonb_replace(report_context, jtab_tpl);
    -- Reporting on top queries by elapsed time
    FOR r_result IN c_elapsed_time(
        (report_context #>> '{report_properties,topn}')::integer
      )
    LOOP
        tab_row := format(
            jtab_tpl #>> ARRAY['stmt_tpl'],
            CASE WHEN NOT r_result.toplevel THEN jtab_tpl #>> ARRAY['nested_tpl'] ELSE '' END,
            to_hex(r_result.queryid),
            left(md5(r_result.userid::text || r_result.datid::text || r_result.queryid::text), 10),
            r_result.dbname,
            r_result.username,
            round(CAST(r_result.total_time_pct AS numeric),2),
            round(CAST(r_result.total_time AS numeric),2),
            round(CAST(r_result.total_plan_time AS numeric),2),
            round(CAST(r_result.total_exec_time AS numeric),2),
            round(CAST(r_result.blk_read_time AS numeric),2),
            round(CAST(r_result.blk_write_time AS numeric),2),
            round(CAST(r_result.user_time AS numeric),2),
            round(CAST(r_result.system_time AS numeric),2),
            r_result.plans,
            r_result.calls,
            CASE WHEN r_result.jit_avail
                THEN format(
                '<a HREF="#jit_%s_%s_%s_%s">%s</a>',
                to_hex(r_result.queryid),
                r_result.datid::text,
                r_result.userid::text,
                r_result.toplevel::text,
                round(CAST(r_result.total_jit_time AS numeric),2)::text)
                ELSE ''
            END  -- 16
        );

        report := report || tab_row;
        PERFORM collect_queries(
            r_result.userid,r_result.datid,r_result.queryid
        );
    END LOOP;

    IF report != '' THEN
        RETURN replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    ELSE
        RETURN '';
    END IF;
END;
$$ LANGUAGE plpgsql;
CREATE FUNCTION top_elapsed_diff_htbl(IN report_context jsonb, IN sserver_id integer)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report      text := '';
    tab_row     text := '';
    jtab_tpl    jsonb;

    --Cursor for top(cnt) queries ordered by elapsed time
    c_elapsed_time CURSOR(topn integer) FOR
    SELECT * FROM (SELECT
        COALESCE(st1.datid,st2.datid) as datid,
        COALESCE(st1.dbname,st2.dbname) as dbname,
        COALESCE(st1.userid,st2.userid) as userid,
        COALESCE(st1.username,st2.username) as username,
        COALESCE(st1.queryid,st2.queryid) as queryid,
        COALESCE(st1.toplevel,st2.toplevel) as toplevel,
        NULLIF(st1.total_time, 0.0) as total_time1,
        NULLIF(st1.total_time_pct, 0.0) as total_time_pct1,
        NULLIF(st1.total_plan_time, 0.0) as total_plan_time1,
        NULLIF(st1.total_exec_time, 0.0) as total_exec_time1,
        NULLIF(st1.jit_generation_time + st1.jit_inlining_time +
          st1.jit_optimization_time + st1.jit_emission_time, 0) as total_jit_time1,
        NULLIF(st1.blk_read_time, 0.0) as blk_read_time1,
        NULLIF(st1.blk_write_time, 0.0) as blk_write_time1,
        NULLIF(st1.user_time, 0.0) as user_time1,
        NULLIF(st1.system_time, 0.0) as system_time1,
        NULLIF(st1.calls, 0) as calls1,
        NULLIF(st1.plans, 0) as plans1,
        NULLIF(st2.total_time, 0.0) as total_time2,
        NULLIF(st2.total_time_pct, 0.0) as total_time_pct2,
        NULLIF(st2.total_plan_time, 0.0) as total_plan_time2,
        NULLIF(st2.total_exec_time, 0.0) as total_exec_time2,
        NULLIF(st2.jit_generation_time + st2.jit_inlining_time +
          st2.jit_optimization_time + st2.jit_emission_time, 0) as total_jit_time2,
        NULLIF(st2.blk_read_time, 0.0) as blk_read_time2,
        NULLIF(st2.blk_write_time, 0.0) as blk_write_time2,
        NULLIF(st2.user_time, 0.0) as user_time2,
        NULLIF(st2.system_time, 0.0) as system_time2,
        NULLIF(st2.calls, 0) as calls2,
        NULLIF(st2.plans, 0) as plans2,
        st1.jit_functions + st1.jit_inlining_count + st1.jit_optimization_count + st1.jit_emission_count > 0 OR
        st2.jit_functions + st2.jit_inlining_count + st2.jit_optimization_count + st2.jit_emission_count > 0 as jit_avail,
        row_number() over (ORDER BY st1.total_time DESC NULLS LAST) as rn_time1,
        row_number() over (ORDER BY st2.total_time DESC NULLS LAST) as rn_time2,
        left(md5(COALESCE(st1.userid,st2.userid)::text || COALESCE(st1.datid,st2.datid)::text || COALESCE(st1.queryid,st2.queryid)::text), 10) as hashed_ids
    FROM top_statements1 st1
        FULL OUTER JOIN top_statements2 st2 USING (server_id, datid, userid, queryid, toplevel)
    ORDER BY COALESCE(st1.total_time,0) + COALESCE(st2.total_time,0) DESC,
      COALESCE(st1.queryid,st2.queryid) ASC,
      COALESCE(st1.datid,st2.datid) ASC,
      COALESCE(st1.userid,st2.userid) ASC,
      COALESCE(st1.toplevel,st2.toplevel) ASC
    ) t1
    WHERE least(
        rn_time1,
        rn_time2
      ) <= topn;

    r_result RECORD;
BEGIN
    -- This report section is meaningful only when planning timing is available
    IF NOT jsonb_extract_path_text(report_context, 'report_features', 'planning_times')::boolean THEN
      RETURN '';
    END IF;

    -- Elapsed time sorted list TPLs
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {difftbl}>'
          '<tr>'
            '<th rowspan="2">Query ID</th>'
            '<th rowspan="2">Database</th>'
            '<th rowspan="2">User</th>'
            '<th rowspan="2">I</th>'
            '<th rowspan="2" title="Elapsed time as a percentage of total cluster elapsed time">%Total</th>'
            '<th colspan="3">Time (s)</th>'
            '{statements_jit_stats?jit_time_hdr}'
            '{io_times?iotime_hdr1}'
            '{kcachestatements?kcache_hdr1}'
            '<th rowspan="2" title="Number of times the statement was planned">Plans</th>'
            '<th rowspan="2" title="Number of times the statement was executed">Executions</th>'
          '</tr>'
          '<tr>'
            '<th title="Time spent by the statement">Elapsed</th>'
            '<th title="Time spent planning statement">Plan</th>'
            '<th title="Time spent executing statement">Exec</th>'
            '{io_times?iotime_hdr2}'
            '{kcachestatements?kcache_hdr2}'
          '</tr>'
          '{rows}'
        '</table>',
      'stmt_tpl',
        '<tr {interval1}>'
          '<td {rowtdspanhdr_mono}><p><a HREF="#%2$s">%2$s</a></p>'
          '<p><small>[%3$s]</small>%1$s</p></td>'
          '<td {rowtdspanhdr}>%4$s</td>'
          '<td {rowtdspanhdr}>%5$s</td>'
          '<td {label} {title1}>1</td>'
          '<td {value}>%6$s</td>'
          '<td {value}>%7$s</td>'
          '<td {value}>%8$s</td>'
          '<td {value}>%9$s</td>'
          '{statements_jit_stats?jit_time_row1}'
          '{io_times?iotime_row1}'
          '{kcachestatements?kcache_row1}'
          '<td {value}>%14$s</td>'
          '<td {value}>%15$s</td>'
        '</tr>'
        '<tr {interval2}>'
          '<td {label} {title2}>2</td>'
          '<td {value}>%16$s</td>'
          '<td {value}>%17$s</td>'
          '<td {value}>%18$s</td>'
          '<td {value}>%19$s</td>'
          '{statements_jit_stats?jit_time_row2}'
          '{io_times?iotime_row2}'
          '{kcachestatements?kcache_row2}'
          '<td {value}>%24$s</td>'
          '<td {value}>%25$s</td>'
        '</tr>'
        '<tr style="visibility:collapse"></tr>',
      'nested_tpl',
        ' <small title="Nested level">(N)</small>',
      'statements_jit_stats?jit_time_hdr',
        '<th rowspan="2">JIT<br>time (s)</th>',
      'statements_jit_stats?jit_time_row1',
        '<td {value}>%26$s</td>',
      'statements_jit_stats?jit_time_row2',
        '<td {value}>%27$s</td>',
      'io_times?iotime_hdr1',
        '<th colspan="2">I/O time (s)</th>',
      'io_times?iotime_hdr2',
        '<th title="Time spent reading blocks by statement">Read</th>'
        '<th title="Time spent writing blocks by statement">Write</th>',
      'io_times?iotime_row1',
        '<td {value}>%10$s</td>'
        '<td {value}>%11$s</td>',
      'io_times?iotime_row2',
        '<td {value}>%20$s</td>'
        '<td {value}>%21$s</td>',
      'kcachestatements?kcache_hdr1',
        '<th colspan="2">CPU time (s)</th>',
      'kcachestatements?kcache_hdr2',
        '<th>Usr</th>'
        '<th>Sys</th>',
      'kcachestatements?kcache_row1',
        '<td {value}>%12$s</td>'
        '<td {value}>%13$s</td>',
      'kcachestatements?kcache_row2',
        '<td {value}>%22$s</td>'
        '<td {value}>%23$s</td>'
      );
    -- apply settings to templates
    jtab_tpl := jsonb_replace(report_context, jtab_tpl);

    -- Reporting on top queries by elapsed time
    FOR r_result IN c_elapsed_time(
        (report_context #>> '{report_properties,topn}')::integer
      )
    LOOP
        tab_row := format(
            jtab_tpl #>> ARRAY['stmt_tpl'],
            CASE WHEN NOT r_result.toplevel THEN jtab_tpl #>> ARRAY['nested_tpl'] ELSE '' END,  -- 1
            to_hex(r_result.queryid),  -- 2
            r_result.hashed_ids,  -- 3
            r_result.dbname,  -- 4
            r_result.username,  -- 5
            round(CAST(r_result.total_time_pct1 AS numeric),2),  -- 6
            round(CAST(r_result.total_time1 AS numeric),2),  -- 7
            round(CAST(r_result.total_plan_time1 AS numeric),2),  -- 8
            round(CAST(r_result.total_exec_time1 AS numeric),2),  -- 9
            round(CAST(r_result.blk_read_time1 AS numeric),2),  -- 10
            round(CAST(r_result.blk_write_time1 AS numeric),2),  -- 11
            round(CAST(r_result.user_time1 AS numeric),2),  -- 12
            round(CAST(r_result.system_time1 AS numeric),2),  -- 13
            r_result.plans1,  -- 14
            r_result.calls1,  -- 18
            round(CAST(r_result.total_time_pct2 AS numeric),2),  -- 16
            round(CAST(r_result.total_time2 AS numeric),2),  -- 17
            round(CAST(r_result.total_plan_time2 AS numeric),2),  -- 18
            round(CAST(r_result.total_exec_time2 AS numeric),2),  -- 19
            round(CAST(r_result.blk_read_time2 AS numeric),2),  -- 20
            round(CAST(r_result.blk_write_time2 AS numeric),2),  -- 21
            round(CAST(r_result.user_time2 AS numeric),2),  -- 22
            round(CAST(r_result.system_time2 AS numeric),2),  -- 23
            r_result.plans2,  -- 24
            r_result.calls2,  -- 25
            CASE WHEN r_result.jit_avail
                THEN format(
                '<a HREF="#jit_%s_%s_%s_%s">%s</a>',
                to_hex(r_result.queryid),
                r_result.datid::text,
                r_result.userid::text,
                r_result.toplevel::text,
                round(CAST(r_result.total_jit_time1 AS numeric),2)::text)
                ELSE ''
            END,  -- 26
            CASE WHEN r_result.jit_avail
                THEN format(
                '<a HREF="#jit_%s_%s_%s_%s">%s</a>',
                to_hex(r_result.queryid),
                r_result.datid::text,
                r_result.userid::text,
                r_result.toplevel::text,
                round(CAST(r_result.total_jit_time2 AS numeric),2)::text)
                ELSE ''
            END -- 27
        );

        report := report || tab_row;
        PERFORM collect_queries(
            r_result.userid,r_result.datid,r_result.queryid
        );
    END LOOP;

    IF report != '' THEN
        RETURN replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    ELSE
        RETURN '';
    END IF;
END;
$$ LANGUAGE plpgsql;
CREATE FUNCTION top_plan_time_htbl(IN report_context jsonb, IN sserver_id integer)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report      text := '';
    tab_row     text := '';
    jtab_tpl    jsonb;

    --Cursor for queries ordered by planning time
    c_plan_time CURSOR(topn integer) FOR
    SELECT
        st.datid,
        st.dbname,
        st.userid,
        st.username,
        st.queryid,
        st.toplevel,
        NULLIF(st.plans, 0) as plans,
        NULLIF(st.calls, 0) as calls,
        NULLIF(st.total_plan_time, 0.0) as total_plan_time,
        NULLIF(st.plan_time_pct, 0.0) as plan_time_pct,
        NULLIF(st.min_plan_time, 0.0) as min_plan_time,
        NULLIF(st.max_plan_time, 0.0) as max_plan_time,
        NULLIF(st.mean_plan_time, 0.0) as mean_plan_time,
        NULLIF(st.stddev_plan_time, 0.0) as stddev_plan_time
    FROM top_statements1 st
    ORDER BY st.total_plan_time DESC,
      st.total_exec_time DESC,
      st.queryid ASC,
      st.toplevel ASC,
      st.datid ASC,
      st.userid ASC,
      st.dbname ASC,
      st.username ASC
    LIMIT topn;

    r_result RECORD;
BEGIN
    -- This report section is meaningful only when planning timing is available
    IF NOT jsonb_extract_path_text(report_context, 'report_features', 'planning_times')::boolean THEN
      RETURN '';
    END IF;

    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {stattbl}>'
          '<tr>'
            '<th rowspan="2">Query ID</th>'
            '<th rowspan="2">Database</th>'
            '<th rowspan="2">User</th>'
            '<th rowspan="2" title="Time spent planning statement">Plan elapsed (s)</th>'
            '<th rowspan="2" title="Plan elapsed as a percentage of statement elapsed time">%Elapsed</th>'
            '<th colspan="4" title="Planning time statistics">Plan times (ms)</th>'
            '<th rowspan="2" title="Number of times the statement was planned">Plans</th>'
            '<th rowspan="2" title="Number of times the statement was executed">Executions</th>'
          '</tr>'
          '<tr>'
            '<th>Mean</th>'
            '<th>Min</th>'
            '<th>Max</th>'
            '<th>StdErr</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'stmt_tpl',
        '<tr>'
          '<td {mono}><p><a HREF="#%2$s">%2$s</a></p>'
          '<p><small>[%3$s]</small>%1$s</p></td>'
          '<td>%4$s</td>'
          '<td>%5$s</td>'
          '<td {value}>%6$s</td>'
          '<td {value}>%7$s</td>'
          '<td {value}>%8$s</td>'
          '<td {value}>%9$s</td>'
          '<td {value}>%10$s</td>'
          '<td {value}>%11$s</td>'
          '<td {value}>%12$s</td>'
          '<td {value}>%13$s</td>'
        '</tr>',
      'nested_tpl',
        ' <small title="Nested level">(N)</small>'
      );
    -- apply settings to templates
    jtab_tpl := jsonb_replace(report_context, jtab_tpl);
    -- Reporting on top queries by elapsed time
    FOR r_result IN c_plan_time(
        (report_context #>> '{report_properties,topn}')::integer
      )
    LOOP
        tab_row := format(
            jtab_tpl #>> ARRAY['stmt_tpl'],
            CASE WHEN NOT r_result.toplevel THEN jtab_tpl #>> ARRAY['nested_tpl'] ELSE '' END,
            to_hex(r_result.queryid),
            left(md5(r_result.userid::text || r_result.datid::text || r_result.queryid::text), 10),
            r_result.dbname,
            r_result.username,
            round(CAST(r_result.total_plan_time AS numeric),2),
            round(CAST(r_result.plan_time_pct AS numeric),2),
            round(CAST(r_result.mean_plan_time AS numeric),3),
            round(CAST(r_result.min_plan_time AS numeric),3),
            round(CAST(r_result.max_plan_time AS numeric),3),
            round(CAST(r_result.stddev_plan_time AS numeric),3),
            r_result.plans,
            r_result.calls
        );

        report := report || tab_row;
        PERFORM collect_queries(
            r_result.userid,r_result.datid,r_result.queryid
        );
    END LOOP;

    IF report != '' THEN
        RETURN replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    ELSE
        RETURN '';
    END IF;
END;
$$ LANGUAGE plpgsql;
CREATE FUNCTION top_plan_time_diff_htbl(IN report_context jsonb, IN sserver_id integer)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report      text := '';
    tab_row     text := '';
    jtab_tpl    jsonb;

    --Cursor for top(cnt) queries ordered by elapsed time
    c_plan_time CURSOR(topn integer) FOR
    SELECT * FROM (SELECT
        COALESCE(st1.datid,st2.datid) as datid,
        COALESCE(st1.dbname,st2.dbname) as dbname,
        COALESCE(st1.userid,st2.userid) as userid,
        COALESCE(st1.username,st2.username) as username,
        COALESCE(st1.queryid,st2.queryid) as queryid,
        COALESCE(st1.toplevel,st2.toplevel) as toplevel,
        NULLIF(st1.plans, 0) as plans1,
        NULLIF(st1.calls, 0) as calls1,
        NULLIF(st1.total_plan_time, 0.0) as total_plan_time1,
        NULLIF(st1.plan_time_pct, 0.0) as plan_time_pct1,
        NULLIF(st1.min_plan_time, 0.0) as min_plan_time1,
        NULLIF(st1.max_plan_time, 0.0) as max_plan_time1,
        NULLIF(st1.mean_plan_time, 0.0) as mean_plan_time1,
        NULLIF(st1.stddev_plan_time, 0.0) as stddev_plan_time1,
        NULLIF(st2.plans, 0) as plans2,
        NULLIF(st2.calls, 0) as calls2,
        NULLIF(st2.total_plan_time, 0.0) as total_plan_time2,
        NULLIF(st2.plan_time_pct, 0.0) as plan_time_pct2,
        NULLIF(st2.min_plan_time, 0.0) as min_plan_time2,
        NULLIF(st2.max_plan_time, 0.0) as max_plan_time2,
        NULLIF(st2.mean_plan_time, 0.0) as mean_plan_time2,
        NULLIF(st2.stddev_plan_time, 0.0) as stddev_plan_time2,
        row_number() over (ORDER BY st1.total_plan_time DESC NULLS LAST) as rn_time1,
        row_number() over (ORDER BY st2.total_plan_time DESC NULLS LAST) as rn_time2
    FROM top_statements1 st1
        FULL OUTER JOIN top_statements2 st2 USING (server_id, datid, userid, queryid, toplevel)
    ORDER BY COALESCE(st1.total_plan_time,0) + COALESCE(st2.total_plan_time,0) DESC,
      COALESCE(st1.total_exec_time,0) + COALESCE(st2.total_exec_time,0) DESC,
      COALESCE(st1.queryid,st2.queryid) ASC,
      COALESCE(st1.datid,st2.datid) ASC,
      COALESCE(st1.userid,st2.userid) ASC,
      COALESCE(st1.toplevel,st2.toplevel) ASC
    ) t1
    WHERE least(
        rn_time1,
        rn_time2
      ) <= topn;

    r_result RECORD;
BEGIN
    -- Elapsed time sorted list TPLs
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {difftbl}>'
          '<tr>'
            '<th rowspan="2">Query ID</th>'
            '<th rowspan="2">Database</th>'
            '<th rowspan="2">User</th>'
            '<th rowspan="2">I</th>'
            '<th rowspan="2" title="Time spent planning statement">Plan elapsed (s)</th>'
            '<th rowspan="2" title="Plan elapsed as a percentage of statement elapsed time">%Elapsed</th>'
            '<th colspan="4" title="Planning time statistics">Plan times (ms)</th>'
            '<th rowspan="2" title="Number of times the statement was planned">Plans</th>'
            '<th rowspan="2" title="Number of times the statement was executed">Executions</th>'
          '</tr>'
          '<tr>'
            '<th>Mean</th>'
            '<th>Min</th>'
            '<th>Max</th>'
            '<th>StdErr</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'stmt_tpl',
        '<tr {interval1}>'
          '<td {rowtdspanhdr_mono}><p><a HREF="#%2$s">%2$s</a></p>'
          '<p><small>[%3$s]</small>%1$s</p></td>'
          '<td {rowtdspanhdr}>%4$s</td>'
          '<td {rowtdspanhdr}>%5$s</td>'
          '<td {label} {title1}>1</td>'
          '<td {value}>%6$s</td>'
          '<td {value}>%7$s</td>'
          '<td {value}>%8$s</td>'
          '<td {value}>%9$s</td>'
          '<td {value}>%10$s</td>'
          '<td {value}>%11$s</td>'
          '<td {value}>%12$s</td>'
          '<td {value}>%13$s</td>'
        '</tr>'
        '<tr {interval2}>'
          '<td {label} {title2}>2</td>'
          '<td {value}>%14$s</td>'
          '<td {value}>%15$s</td>'
          '<td {value}>%16$s</td>'
          '<td {value}>%17$s</td>'
          '<td {value}>%18$s</td>'
          '<td {value}>%19$s</td>'
          '<td {value}>%20$s</td>'
          '<td {value}>%21$s</td>'
        '</tr>'
        '<tr style="visibility:collapse"></tr>',
      'nested_tpl',
        ' <small title="Nested level">(N)</small>'
      );
    -- apply settings to templates
    jtab_tpl := jsonb_replace(report_context, jtab_tpl);

    -- Reporting on top queries by elapsed time
    FOR r_result IN c_plan_time(
        (report_context #>> '{report_properties,topn}')::integer
      )
    LOOP
        tab_row := format(
            jtab_tpl #>> ARRAY['stmt_tpl'],
            CASE WHEN NOT r_result.toplevel THEN jtab_tpl #>> ARRAY['nested_tpl'] ELSE '' END,
            to_hex(r_result.queryid),
            left(md5(r_result.userid::text || r_result.datid::text || r_result.queryid::text), 10),
            r_result.dbname,
            r_result.username,
            round(CAST(r_result.total_plan_time1 AS numeric),2),
            round(CAST(r_result.plan_time_pct1 AS numeric),2),
            round(CAST(r_result.mean_plan_time1 AS numeric),3),
            round(CAST(r_result.min_plan_time1 AS numeric),3),
            round(CAST(r_result.max_plan_time1 AS numeric),3),
            round(CAST(r_result.stddev_plan_time1 AS numeric),3),
            r_result.plans1,
            r_result.calls1,
            round(CAST(r_result.total_plan_time2 AS numeric),2),
            round(CAST(r_result.plan_time_pct2 AS numeric),2),
            round(CAST(r_result.mean_plan_time2 AS numeric),3),
            round(CAST(r_result.min_plan_time2 AS numeric),3),
            round(CAST(r_result.max_plan_time2 AS numeric),3),
            round(CAST(r_result.stddev_plan_time2 AS numeric),3),
            r_result.plans2,
            r_result.calls2
        );

        report := report || tab_row;
        PERFORM collect_queries(
            r_result.userid,r_result.datid,r_result.queryid
        );
    END LOOP;

    IF report != '' THEN
        RETURN replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    ELSE
        RETURN '';
    END IF;
END;
$$ LANGUAGE plpgsql;
CREATE FUNCTION top_exec_time_htbl(IN report_context jsonb, IN sserver_id integer)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report      text := '';
    tab_row     text := '';
    jtab_tpl    jsonb;

    --Cursor for queries ordered by execution time
    c_exec_time CURSOR(topn integer) FOR
    SELECT
        st.datid,
        st.dbname,
        st.userid,
        st.username,
        st.queryid,
        st.toplevel,
        NULLIF(st.calls, 0) as calls,
        NULLIF(st.total_exec_time, 0.0) as total_exec_time,
        NULLIF(st.total_exec_time_pct, 0.0) as total_exec_time_pct,
        NULLIF(st.exec_time_pct, 0.0) as exec_time_pct,
        NULLIF(st.jit_generation_time + st.jit_inlining_time +
          st.jit_optimization_time + st.jit_emission_time, 0) as total_jit_time,
        st.jit_functions + st.jit_inlining_count + st.jit_optimization_count + st.jit_emission_count > 0 as jit_avail,
        NULLIF(st.blk_read_time, 0.0) as blk_read_time,
        NULLIF(st.blk_write_time, 0.0) as blk_write_time,
        NULLIF(st.min_exec_time, 0.0) as min_exec_time,
        NULLIF(st.max_exec_time, 0.0) as max_exec_time,
        NULLIF(st.mean_exec_time, 0.0) as mean_exec_time,
        NULLIF(st.stddev_exec_time, 0.0) as stddev_exec_time,
        NULLIF(st.rows, 0) as rows,
        NULLIF(st.user_time, 0.0) as user_time,
        NULLIF(st.system_time, 0.0) as system_time
    FROM top_statements1 st
    ORDER BY st.total_exec_time DESC,
      st.total_time DESC,
      st.queryid ASC,
      st.toplevel ASC,
      st.datid ASC,
      st.userid ASC,
      st.dbname ASC,
      st.username ASC
    LIMIT topn;

    r_result RECORD;
BEGIN
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {stattbl}>'
          '<tr>'
            '<th rowspan="2">Query ID</th>'
            '<th rowspan="2">Database</th>'
            '<th rowspan="2">User</th>'
            '<th rowspan="2" title="Time spent executing statement">Exec (s)</th>'
            '{planning_times?elapsed_pct_hdr}'
            '<th rowspan="2" title="Exec time as a percentage of total cluster elapsed time">%Total</th>'
            '{statements_jit_stats?jit_time_hdr}'
            '{io_times?iotime_hdr1}'
            '{kcachestatements?kcache_hdr1}'
            '<th rowspan="2" title="Total number of rows retrieved or affected by the statement">Rows</th>'
            '<th colspan="4" title="Execution time statistics">Execution times (ms)</th>'
            '<th rowspan="2" title="Number of times the statement was executed">Executions</th>'
          '</tr>'
          '<tr>'
            '{io_times?iotime_hdr2}'
            '{kcachestatements?kcache_hdr2}'
            '<th>Mean</th>'
            '<th>Min</th>'
            '<th>Max</th>'
            '<th>StdErr</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'stmt_tpl',
        '<tr>'
          '<td {mono}><p><a HREF="#%2$s">%2$s</a></p>'
          '<p><small>[%3$s]</small>%1$s</p></td>'
          '<td>%4$s</td>'
          '<td>%5$s</td>'
          '<td {value}>%6$s</td>'
          '{planning_times?elapsed_pct_row}'
          '<td {value}>%7$s</td>'
          '{statements_jit_stats?jit_time_row}'
          '{io_times?iotime_row}'
          '{kcachestatements?kcache_row}'
          '<td {value}>%12$s</td>'
          '<td {value}>%13$s</td>'
          '<td {value}>%14$s</td>'
          '<td {value}>%15$s</td>'
          '<td {value}>%16$s</td>'
          '<td {value}>%17$s</td>'
        '</tr>',
      'nested_tpl',
        ' <small title="Nested level">(N)</small>',
      'io_times?iotime_hdr1',
        '<th colspan="2">I/O time (s)</th>',
      'io_times?iotime_hdr2',
        '<th title="Time spent reading blocks by statement">Read</th>'
        '<th title="Time spent writing blocks by statement">Write</th>',
      'io_times?iotime_row',
        '<td {value}>%8$s</td>'
        '<td {value}>%9$s</td>',
      'planning_times?elapsed_pct_hdr',
        '<th rowspan="2" title="Exec time as a percentage of statement elapsed time">%Elapsed</th>',
      'planning_times?elapsed_pct_row',
        '<td {value}>%18$s</td>',
      'statements_jit_stats?jit_time_hdr',
        '<th rowspan="2">JIT<br>time (s)</th>',
      'statements_jit_stats?jit_time_row',
        '<td {value}>%19$s</td>',
      'kcachestatements?kcache_hdr1',
        '<th colspan="2">CPU time (s)</th>',
      'kcachestatements?kcache_hdr2',
        '<th>Usr</th>'
        '<th>Sys</th>',
      'kcachestatements?kcache_row',
        '<td {value}>%10$s</td>'
        '<td {value}>%11$s</td>'
      );
    -- apply settings to templates
    jtab_tpl := jsonb_replace(report_context, jtab_tpl);
    -- Reporting on top queries by elapsed time
    FOR r_result IN c_exec_time(
        (report_context #>> '{report_properties,topn}')::integer
      )
    LOOP
        tab_row := format(
            jtab_tpl #>> ARRAY['stmt_tpl'],
            CASE WHEN NOT r_result.toplevel THEN jtab_tpl #>> ARRAY['nested_tpl'] ELSE '' END,  -- 1
            to_hex(r_result.queryid),  -- 2
            left(md5(r_result.userid::text || r_result.datid::text || r_result.queryid::text), 10),  -- 3
            r_result.dbname,  -- 4
            r_result.username,  -- 5
            round(CAST(r_result.total_exec_time AS numeric),2),  -- 6
            round(CAST(r_result.total_exec_time_pct AS numeric),2),  -- 7
            round(CAST(r_result.blk_read_time AS numeric),2),  -- 8
            round(CAST(r_result.blk_write_time AS numeric),2),  -- 9
            round(CAST(r_result.user_time AS numeric),2),  -- 10
            round(CAST(r_result.system_time AS numeric),2),  -- 11
            r_result.rows,  -- 12
            round(CAST(r_result.mean_exec_time AS numeric),3),  -- 13
            round(CAST(r_result.min_exec_time AS numeric),3),  -- 14
            round(CAST(r_result.max_exec_time AS numeric),3),  -- 15
            round(CAST(r_result.stddev_exec_time AS numeric),3),  -- 16
            r_result.calls,  -- 17
            round(CAST(r_result.exec_time_pct AS numeric),2),  -- 18
            CASE WHEN r_result.jit_avail
                THEN format(
                '<a HREF="#jit_%s_%s_%s_%s">%s</a>',
                to_hex(r_result.queryid),
                r_result.datid::text,
                r_result.userid::text,
                r_result.toplevel::text,
                round(CAST(r_result.total_jit_time AS numeric),2)::text)
                ELSE ''
            END  -- 19
        );

        report := report || tab_row;
        PERFORM collect_queries(
            r_result.userid,r_result.datid,r_result.queryid
        );
    END LOOP;

    IF report != '' THEN
        RETURN replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    ELSE
        RETURN '';
    END IF;
END;
$$ LANGUAGE plpgsql;
CREATE FUNCTION top_exec_time_diff_htbl(IN report_context jsonb, IN sserver_id integer)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report      text := '';
    tab_row     text := '';
    jtab_tpl    jsonb;

    --Cursor for top(cnt) queries ordered by elapsed time
    c_exec_time CURSOR(topn integer) FOR
    SELECT * FROM (SELECT
        COALESCE(st1.datid,st2.datid) as datid,
        COALESCE(st1.dbname,st2.dbname) as dbname,
        COALESCE(st1.userid,st2.userid) as userid,
        COALESCE(st1.username,st2.username) as username,
        COALESCE(st1.queryid,st2.queryid) as queryid,
        COALESCE(st1.toplevel,st2.toplevel) as toplevel,
        NULLIF(st1.calls, 0) as calls1,
        NULLIF(st1.total_exec_time, 0.0) as total_exec_time1,
        NULLIF(st1.total_exec_time_pct, 0.0) as total_exec_time_pct1,
        NULLIF(st1.exec_time_pct, 0.0) as exec_time_pct1,
        NULLIF(st1.jit_generation_time + st1.jit_inlining_time +
          st1.jit_optimization_time + st1.jit_emission_time, 0) as total_jit_time1,
        NULLIF(st1.blk_read_time, 0.0) as blk_read_time1,
        NULLIF(st1.blk_write_time, 0.0) as blk_write_time1,
        NULLIF(st1.min_exec_time, 0.0) as min_exec_time1,
        NULLIF(st1.max_exec_time, 0.0) as max_exec_time1,
        NULLIF(st1.mean_exec_time, 0.0) as mean_exec_time1,
        NULLIF(st1.stddev_exec_time, 0.0) as stddev_exec_time1,
        NULLIF(st1.rows, 0) as rows1,
        NULLIF(st1.user_time, 0.0) as user_time1,
        NULLIF(st1.system_time, 0.0) as system_time1,
        NULLIF(st2.calls, 0) as calls2,
        NULLIF(st2.total_exec_time, 0.0) as total_exec_time2,
        NULLIF(st2.total_exec_time_pct, 0.0) as total_exec_time_pct2,
        NULLIF(st2.exec_time_pct, 0.0) as exec_time_pct2,
        NULLIF(st2.jit_generation_time + st2.jit_inlining_time +
          st2.jit_optimization_time + st2.jit_emission_time, 0) as total_jit_time2,
        NULLIF(st2.blk_read_time, 0.0) as blk_read_time2,
        NULLIF(st2.blk_write_time, 0.0) as blk_write_time2,
        NULLIF(st2.min_exec_time, 0.0) as min_exec_time2,
        NULLIF(st2.max_exec_time, 0.0) as max_exec_time2,
        NULLIF(st2.mean_exec_time, 0.0) as mean_exec_time2,
        NULLIF(st2.stddev_exec_time, 0.0) as stddev_exec_time2,
        NULLIF(st2.rows, 0) as rows2,
        NULLIF(st2.user_time, 0.0) as user_time2,
        NULLIF(st2.system_time, 0.0) as system_time2,
        st1.jit_functions + st1.jit_inlining_count + st1.jit_optimization_count + st1.jit_emission_count > 0 OR
        st2.jit_functions + st2.jit_inlining_count + st2.jit_optimization_count + st2.jit_emission_count > 0 as jit_avail,
        row_number() over (ORDER BY st1.total_exec_time DESC NULLS LAST) as rn_time1,
        row_number() over (ORDER BY st2.total_exec_time DESC NULLS LAST) as rn_time2
    FROM top_statements1 st1
        FULL OUTER JOIN top_statements2 st2 USING (server_id, datid, userid, queryid, toplevel)
    ORDER BY COALESCE(st1.total_exec_time,0) + COALESCE(st2.total_exec_time,0) DESC,
      COALESCE(st1.total_time,0) + COALESCE(st2.total_time,0) DESC,
      COALESCE(st1.queryid,st2.queryid) ASC,
      COALESCE(st1.datid,st2.datid) ASC,
      COALESCE(st1.userid,st2.userid) ASC,
      COALESCE(st1.toplevel,st2.toplevel) ASC
    ) t1
    WHERE least(
        rn_time1,
        rn_time2
      ) <= topn;

    r_result RECORD;
BEGIN
    -- Elapsed time sorted list TPLs
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {difftbl}>'
          '<tr>'
            '<th rowspan="2">Query ID</th>'
            '<th rowspan="2">Database</th>'
            '<th rowspan="2">User</th>'
            '<th rowspan="2">I</th>'
            '<th rowspan="2" title="Time spent executing statement">Exec (s)</th>'
            '{planning_times?elapsed_pct_hdr}'
            '<th rowspan="2" title="Exec time as a percentage of total cluster elapsed time">%Total</th>'
            '{statements_jit_stats?jit_time_hdr}'
            '{io_times?iotime_hdr1}'
            '{kcachestatements?kcache_hdr1}'
            '<th rowspan="2" title="Total number of rows retrieved or affected by the statement">Rows</th>'
            '<th colspan="4" title="Execution time statistics">Execution times (ms)</th>'
            '<th rowspan="2" title="Number of times the statement was executed">Executions</th>'
          '</tr>'
          '<tr>'
            '{io_times?iotime_hdr2}'
            '{kcachestatements?kcache_hdr2}'
            '<th>Mean</th>'
            '<th>Min</th>'
            '<th>Max</th>'
            '<th>StdErr</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'stmt_tpl',
        '<tr {interval1}>'
          '<td {rowtdspanhdr_mono}><p><a HREF="#%2$s">%2$s</a></p>'
          '<p><small>[%3$s]</small>%1$s</p></td>'
          '<td {rowtdspanhdr}>%4$s</td>'
          '<td {rowtdspanhdr}>%5$s</td>'
          '<td {label} {title1}>1</td>'
          '<td {value}>%6$s</td>'
          '{planning_times?elapsed_pct_row1}'
          '<td {value}>%7$s</td>'
          '{statements_jit_stats?jit_time_row1}'
          '{io_times?iotime_row1}'
          '{kcachestatements?kcache_row1}'
          '<td {value}>%12$s</td>'
          '<td {value}>%13$s</td>'
          '<td {value}>%14$s</td>'
          '<td {value}>%15$s</td>'
          '<td {value}>%16$s</td>'
          '<td {value}>%17$s</td>'
        '</tr>'
        '<tr {interval2}>'
          '<td {label} {title2}>2</td>'
          '<td {value}>%18$s</td>'
          '{planning_times?elapsed_pct_row2}'
          '<td {value}>%19$s</td>'
          '{statements_jit_stats?jit_time_row2}'
          '{io_times?iotime_row2}'
          '{kcachestatements?kcache_row2}'
          '<td {value}>%24$s</td>'
          '<td {value}>%25$s</td>'
          '<td {value}>%26$s</td>'
          '<td {value}>%27$s</td>'
          '<td {value}>%28$s</td>'
          '<td {value}>%29$s</td>'
        '</tr>'
        '<tr style="visibility:collapse"></tr>',
      'nested_tpl',
        ' <small title="Nested level">(N)</small>',
      'io_times?iotime_hdr1',
        '<th colspan="2">I/O time (s)</th>',
      'io_times?iotime_hdr2',
        '<th title="Time spent reading blocks by statement">Read</th>'
        '<th title="Time spent writing blocks by statement">Write</th>',
      'io_times?iotime_row1',
        '<td {value}>%8$s</td>'
        '<td {value}>%9$s</td>',
      'io_times?iotime_row2',
        '<td {value}>%20$s</td>'
        '<td {value}>%21$s</td>',
      'planning_times?elapsed_pct_hdr',
        '<th rowspan="2" title="Exec time as a percentage of statement elapsed time">%Elapsed</th>',
      'planning_times?elapsed_pct_row1',
        '<td {value}>%30$s</td>',
      'planning_times?elapsed_pct_row2',
        '<td {value}>%31$s</td>',
      'statements_jit_stats?jit_time_hdr',
        '<th rowspan="2">JIT<br>time (s)</th>',
      'statements_jit_stats?jit_time_row1',
        '<td {value}>%32$s</td>',
      'statements_jit_stats?jit_time_row2',
        '<td {value}>%33$s</td>',
      'kcachestatements?kcache_hdr1',
        '<th colspan="2">CPU time (s)</th>',
      'kcachestatements?kcache_hdr2',
        '<th>Usr</th>'
        '<th>Sys</th>',
      'kcachestatements?kcache_row1',
        '<td {value}>%10$s</td>'
        '<td {value}>%11$s</td>',
      'kcachestatements?kcache_row2',
        '<td {value}>%22$s</td>'
        '<td {value}>%23$s</td>'
      );
    -- apply settings to templates
    jtab_tpl := jsonb_replace(report_context, jtab_tpl);

    -- Reporting on top queries by elapsed time
    FOR r_result IN c_exec_time(
        (report_context #>> '{report_properties,topn}')::integer
      )
    LOOP
        tab_row := format(
            jtab_tpl #>> ARRAY['stmt_tpl'],
            CASE WHEN NOT r_result.toplevel THEN jtab_tpl #>> ARRAY['nested_tpl'] ELSE '' END,  -- 1
            to_hex(r_result.queryid),  -- 2
            left(md5(r_result.userid::text || r_result.datid::text || r_result.queryid::text), 10),  -- 3
            r_result.dbname,  -- 4
            r_result.username,  -- 5
            round(CAST(r_result.total_exec_time1 AS numeric),2),  -- 6
            round(CAST(r_result.total_exec_time_pct1 AS numeric),2),  -- 7
            round(CAST(r_result.blk_read_time1 AS numeric),2),  -- 8
            round(CAST(r_result.blk_write_time1 AS numeric),2),  -- 9
            round(CAST(r_result.user_time1 AS numeric),2),  -- 10
            round(CAST(r_result.system_time1 AS numeric),2),  -- 11
            r_result.rows1,  -- 12
            round(CAST(r_result.mean_exec_time1 AS numeric),3),  -- 13
            round(CAST(r_result.min_exec_time1 AS numeric),3),  -- 14
            round(CAST(r_result.max_exec_time1 AS numeric),3),  -- 15
            round(CAST(r_result.stddev_exec_time1 AS numeric),3),  -- 16
            r_result.calls1,  -- 17
            round(CAST(r_result.total_exec_time2 AS numeric),2),  -- 18
            round(CAST(r_result.total_exec_time_pct2 AS numeric),2),  -- 19
            round(CAST(r_result.blk_read_time2 AS numeric),2),  -- 20
            round(CAST(r_result.blk_write_time2 AS numeric),2),  -- 21
            round(CAST(r_result.user_time2 AS numeric),2),  -- 22
            round(CAST(r_result.system_time2 AS numeric),2),  -- 23
            r_result.rows2,  -- 24
            round(CAST(r_result.mean_exec_time2 AS numeric),3),  -- 25
            round(CAST(r_result.min_exec_time2 AS numeric),3),  -- 26
            round(CAST(r_result.max_exec_time2 AS numeric),3),  -- 27
            round(CAST(r_result.stddev_exec_time2 AS numeric),3),  -- 28
            r_result.calls2,  -- 29
            round(CAST(r_result.exec_time_pct1 AS numeric),2),  -- 30
            round(CAST(r_result.exec_time_pct2 AS numeric),2),  -- 31
            CASE WHEN r_result.jit_avail
                THEN format(
                '<a HREF="#jit_%s_%s_%s_%s">%s</a>',
                to_hex(r_result.queryid),
                r_result.datid::text,
                r_result.userid::text,
                r_result.toplevel::text,
                round(CAST(r_result.total_jit_time1 AS numeric),2)::text)
                ELSE ''
            END,  -- 34
            CASE WHEN r_result.jit_avail
                THEN format(
                '<a HREF="#jit_%s_%s_%s_%s">%s</a>',
                to_hex(r_result.queryid),
                r_result.datid::text,
                r_result.userid::text,
                r_result.toplevel::text,
                round(CAST(r_result.total_jit_time2 AS numeric),2)::text)
                ELSE ''
            END  -- 35
        );

        report := report || tab_row;
        PERFORM collect_queries(
            r_result.userid,r_result.datid,r_result.queryid
        );
    END LOOP;

    IF report != '' THEN
        RETURN replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    ELSE
        RETURN '';
    END IF;
END;
$$ LANGUAGE plpgsql;
CREATE FUNCTION get_report_context(IN sserver_id integer, IN start1_id integer, IN end1_id integer,
  IN description text = NULL,
  IN start2_id integer = NULL, IN end2_id integer = NULL)
RETURNS jsonb SET search_path=@extschema@ AS $$
DECLARE
  report_context  jsonb;
  r_result    RECORD;

  qlen_limit  integer;
  topn        integer;

  start1_time text;
  end1_time   text;
  start2_time text;
  end2_time   text;
BEGIN
    ASSERT num_nulls(start1_id, end1_id) = 0, 'At least first interval bounds is necessary';

    -- Getting query length limit setting
    BEGIN
        qlen_limit := current_setting('pg_profile.max_query_length')::integer;
    EXCEPTION
        WHEN OTHERS THEN qlen_limit := 20000;
    END;

    -- Getting TopN setting
    BEGIN
        topn := current_setting('pg_profile.topn')::integer;
    EXCEPTION
        WHEN OTHERS THEN topn := 20;
    END;

    -- Populate report settings
    -- Check if all samples of requested interval are available
    IF (
      SELECT count(*) != end1_id - start1_id + 1 FROM samples
      WHERE server_id = sserver_id AND sample_id BETWEEN start1_id AND end1_id
    ) THEN
      RAISE 'Not enough samples between %',
        format('%s AND %s', start1_id, end1_id);
    END IF;

    -- Get report times
    SELECT sample_time::text INTO STRICT start1_time FROM samples
    WHERE (server_id,sample_id) = (sserver_id,start1_id);
    SELECT sample_time::text INTO STRICT end1_time FROM samples
    WHERE (server_id,sample_id) = (sserver_id,end1_id);

    IF num_nulls(start2_id, end2_id) = 2 THEN
      report_context := jsonb_build_object(
      'htbl',jsonb_build_object(
        'reltr','class="parent"',
        'toasttr','class="child"',
        'reltdhdr','class="hdr"',
        'stattbl','class="stat"',
        'value','class="value"',
        'mono','class="mono"',
        'reltdspanhdr','rowspan="2" class="hdr"'
      ),
      'report_features',jsonb_build_object(
        'statstatements',profile_checkavail_statstatements(sserver_id, start1_id, end1_id),
        'planning_times',profile_checkavail_planning_times(sserver_id, start1_id, end1_id),
        'wait_sampling_tot',profile_checkavail_wait_sampling_total(sserver_id, start1_id, end1_id),
        'io_times',profile_checkavail_io_times(sserver_id, start1_id, end1_id),
        'statement_wal_bytes',profile_checkavail_stmt_wal_bytes(sserver_id, start1_id, end1_id),
        'wal_stats',profile_checkavail_walstats(sserver_id, start1_id, end1_id),
        'sess_stats',profile_checkavail_sessionstats(sserver_id, start1_id, end1_id),
        'function_stats',profile_checkavail_functions(sserver_id, start1_id, end1_id),
        'trigger_function_stats',profile_checkavail_trg_functions(sserver_id, start1_id, end1_id),
        'kcachestatements',profile_checkavail_rusage(sserver_id,start1_id,end1_id),
        'rusage_planstats',profile_checkavail_rusage_planstats(sserver_id,start1_id,end1_id),
        'statements_jit_stats',profile_checkavail_statements_jit_stats(sserver_id, start1_id, end1_id) OR
          profile_checkavail_statements_jit_stats(sserver_id, start2_id, end2_id)
        ),
      'report_properties',jsonb_build_object(
        'interval_duration_sec',
          (SELECT extract(epoch FROM e.sample_time - s.sample_time)
          FROM samples s JOIN samples e USING (server_id)
          WHERE e.sample_id=end1_id and s.sample_id=start1_id
            AND server_id = sserver_id),
        'checksum_fail_detected', COALESCE((
          SELECT sum(checksum_failures) > 0
          FROM sample_stat_database
          WHERE server_id = sserver_id AND sample_id BETWEEN start1_id + 1 AND end1_id
          ), false),
        'topn', topn,
        'max_query_length', qlen_limit,
        'start1_id', start1_id,
        'end1_id', end1_id,
        'report_start1', start1_time,
        'report_end1', end1_time
        )
      );
    ELSIF num_nulls(start2_id, end2_id) = 0 THEN
      -- Get report times
      SELECT sample_time::text INTO STRICT start2_time FROM samples
      WHERE (server_id,sample_id) = (sserver_id,start2_id);
      SELECT sample_time::text INTO STRICT end2_time FROM samples
      WHERE (server_id,sample_id) = (sserver_id,end2_id);
      -- Check if all samples of requested interval are available
      IF (
        SELECT count(*) != end2_id - start2_id + 1 FROM samples
        WHERE server_id = sserver_id AND sample_id BETWEEN start2_id AND end2_id
      ) THEN
        RAISE 'Not enough samples between %',
          format('%s AND %s', start2_id, end2_id);
      END IF;
      report_context := jsonb_build_object(
      'htbl',jsonb_build_object(
        'value','class="value"',
        'interval1','class="int1"',
        'interval2','class="int2"',
        'label','class="label"',
        'stattbl','class="stat"',
        'difftbl','class="stat diff"',
        'rowtdspanhdr','rowspan="2" class="hdr"',
        'rowtdspanhdr_mono','rowspan="2" class="hdr mono"',
        'mono','class="mono"',
        'title1',format('title="(%s - %s)"',start1_time, end1_time),
        'title2',format('title="(%s - %s)"',start2_time, end2_time)
        ),
      'report_features',jsonb_build_object(
        'statstatements',profile_checkavail_statstatements(sserver_id, start1_id, end1_id) OR
          profile_checkavail_statstatements(sserver_id, start2_id, end2_id),
        'planning_times',profile_checkavail_planning_times(sserver_id, start1_id, end1_id) OR
          profile_checkavail_planning_times(sserver_id, start2_id, end2_id),
        'wait_sampling_tot',profile_checkavail_wait_sampling_total(sserver_id, start1_id, end1_id) OR
          profile_checkavail_wait_sampling_total(sserver_id, start2_id, end2_id),
        'io_times',profile_checkavail_io_times(sserver_id, start1_id, end1_id) OR
          profile_checkavail_io_times(sserver_id, start2_id, end2_id),
        'statement_wal_bytes',profile_checkavail_stmt_wal_bytes(sserver_id, start1_id, end1_id) OR
          profile_checkavail_stmt_wal_bytes(sserver_id, start2_id, end2_id),
        'wal_stats',profile_checkavail_walstats(sserver_id, start1_id, end1_id) OR
          profile_checkavail_walstats(sserver_id, start2_id, end2_id),
        'sess_stats',profile_checkavail_sessionstats(sserver_id, start1_id, end1_id) OR
          profile_checkavail_sessionstats(sserver_id, start2_id, end2_id),
        'function_stats',profile_checkavail_functions(sserver_id, start1_id, end1_id) OR
          profile_checkavail_functions(sserver_id, start2_id, end2_id),
        'trigger_function_stats',profile_checkavail_trg_functions(sserver_id, start1_id, end1_id) OR
          profile_checkavail_trg_functions(sserver_id, start2_id, end2_id),
        'kcachestatements',profile_checkavail_rusage(sserver_id, start1_id, end1_id) OR
          profile_checkavail_rusage(sserver_id, start2_id, end2_id),
        'rusage_planstats',profile_checkavail_rusage_planstats(sserver_id, start1_id, end1_id) OR
          profile_checkavail_rusage_planstats(sserver_id, start2_id, end2_id),
        'statements_jit_stats',profile_checkavail_statements_jit_stats(sserver_id, start1_id, end1_id) OR
          profile_checkavail_statements_jit_stats(sserver_id, start2_id, end2_id)
        ),
      'report_properties',jsonb_build_object(
        'interval1_duration_sec',
          (SELECT extract(epoch FROM e.sample_time - s.sample_time)
          FROM samples s JOIN samples e USING (server_id)
          WHERE e.sample_id=end1_id and s.sample_id=start1_id
            AND server_id = sserver_id),
        'interval2_duration_sec',
          (SELECT extract(epoch FROM e.sample_time - s.sample_time)
          FROM samples s JOIN samples e USING (server_id)
          WHERE e.sample_id=end2_id and s.sample_id=start2_id
            AND server_id = sserver_id),
        'checksum_fail_detected', COALESCE((
          SELECT sum(checksum_failures) > 0
          FROM sample_stat_database
          WHERE server_id = sserver_id AND
            (sample_id BETWEEN start1_id + 1 AND end1_id OR
            sample_id BETWEEN start2_id + 1 AND end2_id)
          ), false),

        'topn', topn,
        'max_query_length', qlen_limit,

        'start1_id', start1_id,
        'end1_id', end1_id,
        'report_start1', start1_time,
        'report_end1', end1_time,

        'start2_id', start2_id,
        'end2_id', end2_id,
        'report_start2', start2_time,
        'report_end2', end2_time
        )
      );
    ELSE
      RAISE 'Two bounds must be specified for second interval';
    END IF;

    -- Server name and description
    SELECT server_name, server_description INTO STRICT r_result
    FROM servers WHERE server_id = sserver_id;
    report_context := jsonb_set(report_context, '{report_properties,server_name}',
      to_jsonb(r_result.server_name)
    );
    IF r_result.server_description IS NOT NULL AND r_result.server_description != ''
    THEN
      report_context := jsonb_set(report_context, '{report_properties,server_description}',
        to_jsonb(format(
          '<p>%s</p>',
          r_result.server_description
        ))
      );
    ELSE
      report_context := jsonb_set(report_context, '{report_properties,server_description}',to_jsonb(''::text));
    END IF;
    -- Report description
    IF description IS NOT NULL AND description != '' THEN
      report_context := jsonb_set(report_context, '{report_properties,description}',
        to_jsonb(format(
          '<h2>Report description</h2><p>%s</p>',
          description
        ))
      );
    ELSE
      report_context := jsonb_set(report_context, '{report_properties,description}',to_jsonb(''::text));
    END IF;
    -- Version substitution
    IF (SELECT count(*) = 1 FROM pg_catalog.pg_extension WHERE extname = 'pg_profile') THEN
      SELECT extversion INTO STRICT r_result FROM pg_catalog.pg_extension WHERE extname = 'pg_profile';
      report_context := jsonb_set(report_context, '{report_properties,pgprofile_version}',
        to_jsonb(r_result.extversion)
      );
    END IF;
  RETURN report_context;
END;
$$ LANGUAGE plpgsql;
ALTER TABLE sample_statements
  DROP CONSTRAINT fk_statements_roles,
  ADD CONSTRAINT fk_statements_roles FOREIGN KEY (server_id, userid)
    REFERENCES roles_list (server_id, userid)
    ON DELETE NO ACTION ON UPDATE CASCADE
    DEFERRABLE INITIALLY IMMEDIATE;

ALTER TABLE indexes_list
  DROP CONSTRAINT fk_indexes_tables,
  ADD CONSTRAINT fk_indexes_tables FOREIGN KEY (server_id, datid, relid)
    REFERENCES tables_list(server_id, datid, relid)
      ON DELETE NO ACTION ON UPDATE CASCADE
      DEFERRABLE INITIALLY IMMEDIATE;

ALTER TABLE sample_kcache
  DROP CONSTRAINT fk_kcache_stmt_list,
  ADD CONSTRAINT fk_kcache_stmt_list FOREIGN KEY (server_id,queryid_md5)
    REFERENCES stmt_list (server_id,queryid_md5)
      ON DELETE NO ACTION ON UPDATE CASCADE
      DEFERRABLE INITIALLY IMMEDIATE;

INSERT INTO import_queries_version_order VALUES
('pg_profile','4.1','pg_profile','4.0')
;

-- last_* tables partitioning. Rename first
ALTER TABLE last_stat_database RENAME TO old_last_stat_database;
ALTER TABLE last_stat_tablespaces RENAME TO old_last_stat_tablespaces;
ALTER TABLE last_stat_tables RENAME TO old_last_stat_tables;
ALTER TABLE last_stat_indexes RENAME TO old_last_stat_indexes;
ALTER TABLE last_stat_user_functions RENAME TO old_last_stat_user_functions;
ALTER TABLE last_stat_statements RENAME TO old_last_stat_statements;
ALTER TABLE last_stat_kcache RENAME TO old_last_stat_kcache;

-- Create partitioned tables
CREATE TABLE last_stat_database (LIKE sample_stat_database)
PARTITION BY LIST (server_id);
COMMENT ON TABLE last_stat_database IS 'Last sample data for calculating diffs in next sample';

CREATE TABLE last_stat_tablespaces (LIKE v_sample_stat_tablespaces)
PARTITION BY LIST (server_id);
COMMENT ON TABLE last_stat_tablespaces IS 'Last sample data for calculating diffs in next sample';

CREATE TABLE last_stat_tables(
    server_id           integer,
    sample_id           integer,
    datid               oid,
    relid               oid,
    schemaname          name,
    relname             name,
    seq_scan            bigint,
    seq_tup_read        bigint,
    idx_scan            bigint,
    idx_tup_fetch       bigint,
    n_tup_ins           bigint,
    n_tup_upd           bigint,
    n_tup_del           bigint,
    n_tup_hot_upd       bigint,
    n_live_tup          bigint,
    n_dead_tup          bigint,
    n_mod_since_analyze bigint,
    n_ins_since_vacuum  bigint,
    last_vacuum         timestamp with time zone,
    last_autovacuum     timestamp with time zone,
    last_analyze        timestamp with time zone,
    last_autoanalyze    timestamp with time zone,
    vacuum_count        bigint,
    autovacuum_count    bigint,
    analyze_count       bigint,
    autoanalyze_count   bigint,
    heap_blks_read      bigint,
    heap_blks_hit       bigint,
    idx_blks_read       bigint,
    idx_blks_hit        bigint,
    toast_blks_read     bigint,
    toast_blks_hit      bigint,
    tidx_blks_read      bigint,
    tidx_blks_hit       bigint,
    relsize             bigint,
    relsize_diff        bigint,
    tablespaceid        oid,
    reltoastrelid       oid,
    relkind             char(1),
    in_sample           boolean NOT NULL DEFAULT false,
    relpages_bytes      bigint,
    relpages_bytes_diff bigint
)
PARTITION BY LIST (server_id);
COMMENT ON TABLE last_stat_tables IS 'Last sample data for calculating diffs in next sample';

CREATE TABLE last_stat_indexes (
    server_id           integer,
    sample_id           integer,
    datid               oid,
    relid               oid NOT NULL,
    indexrelid          oid,
    schemaname          name,
    relname             name,
    indexrelname        name,
    idx_scan            bigint,
    idx_tup_read        bigint,
    idx_tup_fetch       bigint,
    idx_blks_read       bigint,
    idx_blks_hit        bigint,
    relsize             bigint,
    relsize_diff        bigint,
    tablespaceid        oid NOT NULL,
    indisunique         bool,
    in_sample           boolean NOT NULL DEFAULT false,
    relpages_bytes      bigint,
    relpages_bytes_diff bigint
)
PARTITION BY LIST (server_id);
COMMENT ON TABLE last_stat_indexes IS 'Last sample data for calculating diffs in next sample';

CREATE TABLE last_stat_user_functions (LIKE v_sample_stat_user_functions, in_sample boolean NOT NULL DEFAULT false)
PARTITION BY LIST (server_id);
COMMENT ON TABLE last_stat_user_functions IS 'Last sample data for calculating diffs in next sample';

CREATE TABLE last_stat_statements (
    server_id           integer,
    sample_id           integer,
    userid              oid,
    username            name,
    datid               oid,
    queryid             bigint,
    queryid_md5         char(32),
    plans               bigint,
    total_plan_time     double precision,
    min_plan_time       double precision,
    max_plan_time       double precision,
    mean_plan_time      double precision,
    stddev_plan_time    double precision,
    calls               bigint,
    total_exec_time     double precision,
    min_exec_time       double precision,
    max_exec_time       double precision,
    mean_exec_time      double precision,
    stddev_exec_time    double precision,
    rows                bigint,
    shared_blks_hit     bigint,
    shared_blks_read    bigint,
    shared_blks_dirtied bigint,
    shared_blks_written bigint,
    local_blks_hit      bigint,
    local_blks_read     bigint,
    local_blks_dirtied  bigint,
    local_blks_written  bigint,
    temp_blks_read      bigint,
    temp_blks_written   bigint,
    blk_read_time       double precision,
    blk_write_time      double precision,
    wal_records         bigint,
    wal_fpi             bigint,
    wal_bytes           numeric,
    toplevel            boolean,
    in_sample           boolean DEFAULT false,
    jit_functions       bigint,
    jit_generation_time double precision,
    jit_inlining_count  bigint,
    jit_inlining_time   double precision,
    jit_optimization_count  bigint,
    jit_optimization_time   double precision,
    jit_emission_count  bigint,
    jit_emission_time   double precision
)
PARTITION BY LIST (server_id);

CREATE TABLE last_stat_kcache (
    server_id           integer,
    sample_id           integer,
    userid              oid,
    datid               oid,
    toplevel            boolean DEFAULT true,
    queryid             bigint,
    plan_user_time      double precision, --  User CPU time used
    plan_system_time    double precision, --  System CPU time used
    plan_minflts         bigint, -- Number of page reclaims (soft page faults)
    plan_majflts         bigint, -- Number of page faults (hard page faults)
    plan_nswaps         bigint, -- Number of swaps
    plan_reads          bigint, -- Number of bytes read by the filesystem layer
    plan_writes         bigint, -- Number of bytes written by the filesystem layer
    plan_msgsnds        bigint, -- Number of IPC messages sent
    plan_msgrcvs        bigint, -- Number of IPC messages received
    plan_nsignals       bigint, -- Number of signals received
    plan_nvcsws         bigint, -- Number of voluntary context switches
    plan_nivcsws        bigint,
    exec_user_time      double precision, --  User CPU time used
    exec_system_time    double precision, --  System CPU time used
    exec_minflts         bigint, -- Number of page reclaims (soft page faults)
    exec_majflts         bigint, -- Number of page faults (hard page faults)
    exec_nswaps         bigint, -- Number of swaps
    exec_reads          bigint, -- Number of bytes read by the filesystem layer
    exec_writes         bigint, -- Number of bytes written by the filesystem layer
    exec_msgsnds        bigint, -- Number of IPC messages sent
    exec_msgrcvs        bigint, -- Number of IPC messages received
    exec_nsignals       bigint, -- Number of signals received
    exec_nvcsws         bigint, -- Number of voluntary context switches
    exec_nivcsws        bigint
)
PARTITION BY LIST (server_id);

-- Create sections for servers
SELECT create_server_partitions(server_id)
FROM servers;

-- Relload the contents of last_* tables
INSERT INTO last_stat_database
SELECT lst.*
FROM
  old_last_stat_database lst
  JOIN servers srv ON (lst.server_id, lst.sample_id) = (srv.server_id, srv.last_sample_id);

INSERT INTO last_stat_tablespaces
SELECT lst.*
FROM
  old_last_stat_tablespaces lst
  JOIN servers srv ON (lst.server_id, lst.sample_id) = (srv.server_id, srv.last_sample_id);

INSERT INTO last_stat_tables
SELECT lst.*
FROM
  old_last_stat_tables lst
  JOIN servers srv ON (lst.server_id, lst.sample_id) = (srv.server_id, srv.last_sample_id);

INSERT INTO last_stat_indexes
SELECT lst.*
FROM
  old_last_stat_indexes lst
  JOIN servers srv ON (lst.server_id, lst.sample_id) = (srv.server_id, srv.last_sample_id);

INSERT INTO last_stat_user_functions
SELECT lst.*
FROM
  old_last_stat_user_functions lst
  JOIN servers srv ON (lst.server_id, lst.sample_id) = (srv.server_id, srv.last_sample_id);

INSERT INTO last_stat_statements
SELECT lst.*, 0, 0, 0, 0, 0, 0, 0, 0
FROM
  old_last_stat_statements lst
  JOIN servers srv ON (lst.server_id, lst.sample_id) = (srv.server_id, srv.last_sample_id);

INSERT INTO last_stat_kcache
SELECT lst.*
FROM
  old_last_stat_kcache lst
  JOIN servers srv ON (lst.server_id, lst.sample_id) = (srv.server_id, srv.last_sample_id);

-- Remove old tables
DROP TABLE old_last_stat_database CASCADE;
DROP TABLE old_last_stat_tablespaces CASCADE;
DROP TABLE old_last_stat_tables CASCADE;
DROP TABLE old_last_stat_indexes CASCADE;
DROP TABLE old_last_stat_user_functions CASCADE;
DROP TABLE old_last_stat_statements CASCADE;
DROP TABLE old_last_stat_kcache CASCADE;

ALTER TABLE sample_statements
  ADD COLUMN jit_functions       bigint,
  ADD COLUMN jit_generation_time double precision,
  ADD COLUMN jit_inlining_count  bigint,
  ADD COLUMN jit_inlining_time   double precision,
  ADD COLUMN jit_optimization_count  bigint,
  ADD COLUMN jit_optimization_time   double precision,
  ADD COLUMN jit_emission_count  bigint,
  ADD COLUMN jit_emission_time   double precision
;

ALTER TABLE sample_statements_total
  ADD COLUMN jit_functions       bigint,
  ADD COLUMN jit_generation_time double precision,
  ADD COLUMN jit_inlining_count  bigint,
  ADD COLUMN jit_inlining_time   double precision,
  ADD COLUMN jit_optimization_count  bigint,
  ADD COLUMN jit_optimization_time   double precision,
  ADD COLUMN jit_emission_count  bigint,
  ADD COLUMN jit_emission_time   double precision
;

-- Import queries update
UPDATE import_queries SET
  query = 'INSERT INTO sample_statements_total (server_id,sample_id,datid,plans,total_plan_time,'
    'calls,total_exec_time,rows,shared_blks_hit,shared_blks_read,'
    'shared_blks_dirtied,shared_blks_written,local_blks_hit,local_blks_read,'
    'local_blks_dirtied,local_blks_written,temp_blks_read,temp_blks_written,'
    'blk_read_time,blk_write_time,wal_records,wal_fpi,wal_bytes,statements'
    ',jit_functions,jit_generation_time,jit_inlining_count,jit_inlining_time,'
    'jit_optimization_count,jit_optimization_time,jit_emission_count,jit_emission_time'
    ')'
  'SELECT '
    'srv_map.local_srv_id, '
    'dt.sample_id, '
    'dt.datid, '
    'dt.plans, '
    'dt.total_plan_time, '
    'dt.calls, '
    'dt.total_exec_time, '
    'dt.rows, '
    'dt.shared_blks_hit, '
    'dt.shared_blks_read, '
    'dt.shared_blks_dirtied, '
    'dt.shared_blks_written, '
    'dt.local_blks_hit, '
    'dt.local_blks_read, '
    'dt.local_blks_dirtied, '
    'dt.local_blks_written, '
    'dt.temp_blks_read, '
    'dt.temp_blks_written, '
    'dt.blk_read_time, '
    'dt.blk_write_time, '
    'dt.wal_records, '
    'dt.wal_fpi, '
    'dt.wal_bytes, '
    'dt.statements, '
    'dt.jit_functions, '
    'dt.jit_generation_time, '
    'dt.jit_inlining_count, '
    'dt.jit_inlining_time, '
    'dt.jit_optimization_count, '
    'dt.jit_optimization_time, '
    'dt.jit_emission_count, '
    'dt.jit_emission_time '
  'FROM %1$s imp '
    'CROSS JOIN json_to_record(imp.row_data) AS '
      'dt ( '
        'server_id            integer, '
        'sample_id            integer, '
        'datid                oid, '
        'plans                bigint, '
        'total_plan_time      double precision, '
        'calls                bigint, '
        'total_exec_time      double precision, '
        'rows                 bigint, '
        'shared_blks_hit      bigint, '
        'shared_blks_read     bigint, '
        'shared_blks_dirtied  bigint, '
        'shared_blks_written  bigint, '
        'local_blks_hit       bigint, '
        'local_blks_read      bigint, '
        'local_blks_dirtied   bigint, '
        'local_blks_written   bigint, '
        'temp_blks_read       bigint, '
        'temp_blks_written    bigint, '
        'blk_read_time        double precision, '
        'blk_write_time       double precision, '
        'wal_records          bigint, '
        'wal_fpi              bigint, '
        'wal_bytes            numeric, '
        'statements           bigint, '
        'jit_functions        bigint, '
        'jit_generation_time  double precision, '
        'jit_inlining_count   bigint, '
        'jit_inlining_time    double precision, '
        'jit_optimization_count  bigint, '
        'jit_optimization_time   double precision, '
        'jit_emission_count   bigint, '
        'jit_emission_time    double precision'
      ') '
    'JOIN tmp_srv_map srv_map ON '
      '(srv_map.imp_srv_id = dt.server_id) '
    'LEFT OUTER JOIN sample_statements_total ld ON '
      '(ld.server_id = srv_map.local_srv_id AND ld.sample_id = dt.sample_id AND ld.datid = dt.datid) '
  'WHERE ld.server_id IS NULL AND imp.section_id = $1 '
WHERE
  (from_version, exec_order, relname) = ('0.3.1', 1, 'sample_statements_total');

UPDATE import_queries SET
  query = 'INSERT INTO last_stat_statements (server_id,sample_id,userid,username,datid,queryid,queryid_md5,'
    'plans,total_plan_time,min_plan_time,max_plan_time,mean_plan_time,'
    'stddev_plan_time,calls,total_exec_time,min_exec_time,max_exec_time,mean_exec_time,'
    'stddev_exec_time,rows,shared_blks_hit,shared_blks_read,shared_blks_dirtied,'
    'shared_blks_written,local_blks_hit,local_blks_read,local_blks_dirtied,'
    'local_blks_written,temp_blks_read,temp_blks_written,blk_read_time,blk_write_time,'
    'wal_records,wal_fpi,wal_bytes,toplevel,in_sample'
    ',jit_functions,jit_generation_time,jit_inlining_count,jit_inlining_time,'
    'jit_optimization_count,jit_optimization_time,jit_emission_count,jit_emission_time'
    ')'
  'SELECT '
    'srv_map.local_srv_id, '
    'dt.sample_id, '
    'dt.userid, '
    'dt.username, '
    'dt.datid, '
    'dt.queryid, '
    'dt.queryid_md5, '
    'dt.plans, '
    'dt.total_plan_time, '
    'dt.min_plan_time, '
    'dt.max_plan_time, '
    'dt.mean_plan_time, '
    'dt.stddev_plan_time, '
    'dt.calls, '
    'dt.total_exec_time, '
    'dt.min_exec_time, '
    'dt.max_exec_time, '
    'dt.mean_exec_time, '
    'dt.stddev_exec_time, '
    'dt.rows, '
    'dt.shared_blks_hit, '
    'dt.shared_blks_read, '
    'dt.shared_blks_dirtied, '
    'dt.shared_blks_written, '
    'dt.local_blks_hit, '
    'dt.local_blks_read, '
    'dt.local_blks_dirtied, '
    'dt.local_blks_written, '
    'dt.temp_blks_read, '
    'dt.temp_blks_written, '
    'dt.blk_read_time, '
    'dt.blk_write_time, '
    'dt.wal_records, '
    'dt.wal_fpi, '
    'dt.wal_bytes, '
    'dt.toplevel, '
    'dt.in_sample, '
    'dt.jit_functions, '
    'dt.jit_generation_time, '
    'dt.jit_inlining_count, '
    'dt.jit_inlining_time, '
    'dt.jit_optimization_count, '
    'dt.jit_optimization_time, '
    'dt.jit_emission_count, '
    'dt.jit_emission_time '
  'FROM %1$s imp '
    'CROSS JOIN json_to_record(imp.row_data) AS '
      'dt ( '
        'server_id            integer, '
        'sample_id            integer, '
        'userid               oid, '
        'username             name, '
        'datid                oid, '
        'queryid              bigint, '
        'queryid_md5          character(32), '
        'plans                bigint, '
        'total_plan_time      double precision, '
        'min_plan_time        double precision, '
        'max_plan_time        double precision, '
        'mean_plan_time       double precision, '
        'stddev_plan_time     double precision, '
        'calls                bigint, '
        'total_exec_time      double precision, '
        'min_exec_time        double precision, '
        'max_exec_time        double precision, '
        'mean_exec_time       double precision, '
        'stddev_exec_time     double precision, '
        'rows                 bigint, '
        'shared_blks_hit      bigint, '
        'shared_blks_read     bigint, '
        'shared_blks_dirtied  bigint, '
        'shared_blks_written  bigint, '
        'local_blks_hit       bigint, '
        'local_blks_read      bigint, '
        'local_blks_dirtied   bigint, '
        'local_blks_written   bigint, '
        'temp_blks_read       bigint, '
        'temp_blks_written    bigint, '
        'blk_read_time        double precision, '
        'blk_write_time       double precision, '
        'wal_records          bigint, '
        'wal_fpi              bigint, '
        'wal_bytes            numeric, '
        'toplevel             boolean, '
        'in_sample            boolean, '
        'jit_functions        bigint, '
        'jit_generation_time  double precision, '
        'jit_inlining_count   bigint, '
        'jit_inlining_time    double precision, '
        'jit_optimization_count  bigint, '
        'jit_optimization_time   double precision, '
        'jit_emission_count   bigint, '
        'jit_emission_time    double precision'
      ') '
    'JOIN tmp_srv_map srv_map ON '
      '(srv_map.imp_srv_id = dt.server_id) '
    'LEFT OUTER JOIN last_stat_statements ld ON '
      '(ld.server_id, ld.sample_id, ld.datid, ld.userid, ld.queryid, ld.toplevel) = '
      '(srv_map.local_srv_id, dt.sample_id, dt.datid, dt.userid, dt.queryid, dt.toplevel) '
  'WHERE ld.server_id IS NULL AND imp.section_id = $1 '
WHERE
  (from_version, exec_order, relname) = ('4.0', 1, 'last_stat_statements');

UPDATE import_queries SET
  query = 'INSERT INTO sample_statements (server_id,sample_id,userid,datid,queryid,queryid_md5,'
    'plans,total_plan_time,min_plan_time,max_plan_time,mean_plan_time,'
    'stddev_plan_time,calls,total_exec_time,min_exec_time,max_exec_time,mean_exec_time,'
    'stddev_exec_time,rows,shared_blks_hit,shared_blks_read,shared_blks_dirtied,'
    'shared_blks_written,local_blks_hit,local_blks_read,local_blks_dirtied,'
    'local_blks_written,temp_blks_read,temp_blks_written,blk_read_time,blk_write_time,'
    'wal_records,wal_fpi,wal_bytes,toplevel'
    ',jit_functions,jit_generation_time,jit_inlining_count,jit_inlining_time,'
    'jit_optimization_count,jit_optimization_time,jit_emission_count,jit_emission_time'
    ')'
  'SELECT '
    'srv_map.local_srv_id, '
    'dt.sample_id, '
    'dt.userid, '
    'dt.datid, '
    'dt.queryid, '
    'dt.queryid_md5, '
    'dt.plans, '
    'dt.total_plan_time, '
    'dt.min_plan_time, '
    'dt.max_plan_time, '
    'dt.mean_plan_time, '
    'dt.stddev_plan_time, '
    'dt.calls, '
    'dt.total_exec_time, '
    'dt.min_exec_time, '
    'dt.max_exec_time, '
    'dt.mean_exec_time, '
    'dt.stddev_exec_time, '
    'dt.rows, '
    'dt.shared_blks_hit, '
    'dt.shared_blks_read, '
    'dt.shared_blks_dirtied, '
    'dt.shared_blks_written, '
    'dt.local_blks_hit, '
    'dt.local_blks_read, '
    'dt.local_blks_dirtied, '
    'dt.local_blks_written, '
    'dt.temp_blks_read, '
    'dt.temp_blks_written, '
    'dt.blk_read_time, '
    'dt.blk_write_time, '
    'dt.wal_records, '
    'dt.wal_fpi, '
    'dt.wal_bytes, '
    'dt.toplevel, '
    'dt.jit_functions, '
    'dt.jit_generation_time, '
    'dt.jit_inlining_count, '
    'dt.jit_inlining_time, '
    'dt.jit_optimization_count, '
    'dt.jit_optimization_time, '
    'dt.jit_emission_count, '
    'dt.jit_emission_time '
  'FROM %1$s imp '
    'CROSS JOIN json_to_record(imp.row_data) AS '
      'dt ( '
        'server_id            integer, '
        'sample_id            integer, '
        'userid               oid, '
        'datid                oid, '
        'queryid              bigint, '
        'queryid_md5          character(32), '
        'plans                bigint, '
        'total_plan_time      double precision, '
        'min_plan_time        double precision, '
        'max_plan_time        double precision, '
        'mean_plan_time       double precision, '
        'stddev_plan_time     double precision, '
        'calls                bigint, '
        'total_exec_time      double precision, '
        'min_exec_time        double precision, '
        'max_exec_time        double precision, '
        'mean_exec_time       double precision, '
        'stddev_exec_time     double precision, '
        'rows                 bigint, '
        'shared_blks_hit      bigint, '
        'shared_blks_read     bigint, '
        'shared_blks_dirtied  bigint, '
        'shared_blks_written  bigint, '
        'local_blks_hit       bigint, '
        'local_blks_read      bigint, '
        'local_blks_dirtied   bigint, '
        'local_blks_written   bigint, '
        'temp_blks_read       bigint, '
        'temp_blks_written    bigint, '
        'blk_read_time        double precision, '
        'blk_write_time       double precision, '
        'wal_records          bigint, '
        'wal_fpi              bigint, '
        'wal_bytes            numeric, '
        'toplevel             boolean, '
        'jit_functions        bigint, '
        'jit_generation_time  double precision, '
        'jit_inlining_count   bigint, '
        'jit_inlining_time    double precision, '
        'jit_optimization_count  bigint, '
        'jit_optimization_time   double precision, '
        'jit_emission_count   bigint, '
        'jit_emission_time    double precision'
      ') '
    'JOIN tmp_srv_map srv_map ON '
      '(srv_map.imp_srv_id = dt.server_id) '
    'LEFT OUTER JOIN sample_statements ld ON '
      '(ld.server_id, ld.sample_id, ld.datid, ld.userid, ld.queryid, ld.toplevel) = '
      '(srv_map.local_srv_id, dt.sample_id, dt.datid, dt.userid, dt.queryid, dt.toplevel) '
  'WHERE ld.server_id IS NULL AND imp.section_id = $1 '
WHERE
  (from_version, exec_order, relname) = ('4.0', 1, 'sample_statements');

INSERT INTO report_struct (
  report_id, sect_id, parent_sect_id, s_ord, toc_cap, tbl_cap, feature, function_name, href, content)
VALUES
(1, 'dbjitstat', 'srvstat', 550, 'JIT statistics by database', 'JIT statistics by database', 'statements_jit_stats', 'dbagg_jit_stats_htbl', 'dbagg_jit_stat', NULL),
(2, 'dbjitstat', 'srvstat', 550, 'JIT statistics by database', 'JIT statistics by database', 'statements_jit_stats', 'dbagg_jit_stats_diff_htbl', 'dbagg_jit_stat', NULL),
(1, 'sqljit', 'sqlsthdr', 1150, 'Top SQL by JIT elapsed time', 'Top SQL by JIT elapsed time', 'statements_jit_stats', 'top_jit_htbl', 'top_jit', NULL),
(2, 'sqljit', 'sqlsthdr', 1150, 'Top SQL by JIT elapsed time', 'Top SQL by JIT elapsed time', 'statements_jit_stats', 'top_jit_diff_htbl', 'top_jit', NULL)
;
